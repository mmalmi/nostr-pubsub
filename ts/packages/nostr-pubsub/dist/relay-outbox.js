import { RelayRateLimitError } from './relay-rate-limit.js';
/** Use the same local transaction as the event. A completed enqueue never waits for a socket. */
export async function enqueueRelayPublication(event, relays, put, options = {}) {
    for (const relay of new Set(relays)) {
        await put(options.broadcast
            ? { id: event.id, relay, broadcast: true, created_at: event.created_at }
            : { id: event.id, relay });
    }
}
/** Drain one destination independently. Unconfirmed records survive interruption and restart. */
export async function drainRelayOutbox(relay, rows, options) {
    const progress = { sent: 0, present: 0, failed: 0 };
    const pending = rows.filter(row => row.relay === relay);
    const automatic = pending.filter(row => !row.broadcast).slice(0, 100);
    const broadcast = pending.filter(row => row.broadcast)
        .sort((a, b) => (a.created_at ?? 0) - (b.created_at ?? 0));
    async function send(batch) {
        for (const row of batch) {
            if (!await options.enabled(row))
                continue;
            try {
                await options.send(row);
            }
            catch (error) {
                if (error instanceof RelayRateLimitError)
                    throw error;
                progress.failed++;
                progress.error = error instanceof Error ? error.message : String(error);
                options.onProgress?.({ ...progress });
                if (!options.connected() || progress.error.endsWith('timed out'))
                    return false;
                continue;
            }
            await options.acknowledge([row.id]);
            progress.sent++;
            options.onProgress?.({ ...progress });
        }
        return true;
    }
    if (!await send(automatic))
        return progress;
    for (let offset = 0; offset < broadcast.length; offset += 512) {
        let batch = broadcast.slice(offset, offset + 512);
        if (!await options.enabled(batch[0]))
            break;
        const missing = await options.missing?.(batch);
        if (missing) {
            const present = batch.filter(row => !missing.has(row.id)).map(row => row.id);
            if (present.length) {
                await options.acknowledge(present);
                progress.present += present.length;
                options.onProgress?.({ ...progress });
            }
            batch = batch.filter(row => missing.has(row.id));
        }
        if (!await send(batch))
            break;
    }
    return progress;
}
export function relayRetryDeadline(error, now = Date.now()) {
    return Math.min(now + (error.retryAfterMs ?? 60000), 8640000000000000);
}
/** Run independent destination passes and persist backoff. The host supplies the wake-up mechanism. */
export async function retryRelayOutbox(relays, store, run, onDeferred, onError) {
    await Promise.all([...new Set(relays)].map(async (relay) => {
        try {
            const until = await store.deadline(relay);
            if (until && until > Date.now()) {
                onDeferred(relay, until);
                return;
            }
            await run(relay);
        }
        catch (error) {
            if (!(error instanceof RelayRateLimitError)) {
                onError(relay, error);
                return;
            }
            const until = relayRetryDeadline(error);
            await store.defer(relay, until);
            onDeferred(relay, until);
        }
    }));
}
//# sourceMappingURL=relay-outbox.js.map