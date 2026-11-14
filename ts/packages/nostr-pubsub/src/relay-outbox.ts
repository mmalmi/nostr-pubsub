import { RelayRateLimitError } from './relay-rate-limit.js';

export interface RelayDelivery {
  id: string;
  relay: string;
  created_at?: number;
  broadcast?: boolean;
}

/** Use the same local transaction as the event. A completed enqueue never waits for a socket. */
export async function enqueueRelayPublication(
  event: { id: string; created_at: number },
  relays: readonly string[],
  put: (row: RelayDelivery) => Promise<unknown>,
  options: { broadcast?: boolean } = {},
): Promise<void> {
  for (const relay of new Set(relays)) {
    await put(options.broadcast
      ? { id: event.id, relay, broadcast: true, created_at: event.created_at }
      : { id: event.id, relay });
  }
}

export interface RelayDeliveryProgress {
  sent: number;
  present: number;
  failed: number;
  error?: string;
}

export interface RelayDrainOptions {
  /** Called before each send so paused/removed destinations can keep their outstanding rows. */
  enabled(row: RelayDelivery): Promise<boolean>;
  /** Resolve on a positive remote receipt. Throw on rejection or disconnect. */
  send(row: RelayDelivery): Promise<void>;
  acknowledge(ids: string[]): Promise<void>;
  connected(): boolean;
  /** Return IDs missing at this relay, or undefined to publish the batch directly. */
  missing?(batch: RelayDelivery[]): Promise<ReadonlySet<string> | undefined>;
  onProgress?(progress: RelayDeliveryProgress): void;
}

/** Drain one destination independently. Unconfirmed records survive interruption and restart. */
export async function drainRelayOutbox(
  relay: string, rows: readonly RelayDelivery[], options: RelayDrainOptions,
): Promise<RelayDeliveryProgress> {
  const progress: RelayDeliveryProgress = { sent: 0, present: 0, failed: 0 };
  const pending = rows.filter(row => row.relay === relay);
  const automatic = pending.filter(row => !row.broadcast);
  const broadcast = pending.filter(row => row.broadcast)
    .sort((a, b) => (a.created_at ?? 0) - (b.created_at ?? 0));
  async function send(batch: readonly RelayDelivery[], limit = Infinity) {
    let attempted = 0;
    for (const row of batch) {
      if (attempted >= limit) break;
      if (!await options.enabled(row)) continue;
      attempted++;
      try {
        await options.send(row);
      } catch (error) {
        if (error instanceof RelayRateLimitError) throw error;
        progress.failed++;
        progress.error = error instanceof Error ? error.message : String(error);
        options.onProgress?.({ ...progress });
        if (!options.connected() || progress.error.endsWith('timed out')) return false;
        continue;
      }
      await options.acknowledge([row.id]);
      progress.sent++;
      options.onProgress?.({ ...progress });
    }
    return true;
  }
  if (!await send(automatic, 100)) return progress;
  for (let offset = 0; offset < broadcast.length; offset += 512) {
    let batch = broadcast.slice(offset, offset + 512);
    if (options.missing) {
      const enabled: RelayDelivery[] = [];
      for (const row of batch) {
        if (await options.enabled(row)) enabled.push(row);
      }
      if (!enabled.length) continue;
      batch = enabled;
      const missing = await options.missing(batch);
      if (missing) {
        const present: string[] = [];
        for (const row of batch) {
          if (!missing.has(row.id) && await options.enabled(row)) present.push(row.id);
        }
        if (present.length) {
          await options.acknowledge(present);
          progress.present += present.length;
          options.onProgress?.({ ...progress });
        }
        batch = batch.filter(row => missing.has(row.id));
      }
    }
    if (!await send(batch)) break;
  }
  return progress;
}

export function relayRetryDeadline(error: { retryAfterMs?: number }, now = Date.now()): number {
  return Math.min(now + (error.retryAfterMs ?? 60000), 8640000000000000);
}

export interface RelayRetryStore {
  deadline(relay: string): Promise<number | undefined>;
  defer(relay: string, until: number): Promise<void>;
}

/** Run independent destination passes and persist backoff. The host supplies the wake-up mechanism. */
export async function retryRelayOutbox(
  relays: readonly string[], store: RelayRetryStore, run: (relay: string) => Promise<void>,
  onDeferred: (relay: string, until: number) => void, onError: (relay: string, error: unknown) => void,
): Promise<void> {
  await Promise.all([...new Set(relays)].map(async relay => {
    try {
      const until = await store.deadline(relay);
      if (until && until > Date.now()) { onDeferred(relay, until); return; }
      await run(relay);
    } catch (error) {
      if (!(error instanceof RelayRateLimitError)) { onError(relay, error); return; }
      const until = relayRetryDeadline(error);
      await store.defer(relay, until);
      onDeferred(relay, until);
    }
  }));
}
