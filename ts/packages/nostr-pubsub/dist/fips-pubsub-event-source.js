import { SOURCE_PRIORITY_FIPS_ENDPOINT, fipsEndpointSource, } from './source.js';
import { validateQueryOptions, verifyNostrEvent, } from './types.js';
export const DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS = 1_000;
/** Router adapter for the FIPS-TCP REQ/INV/WANT/EVENT subscription protocol. */
export class FipsNostrPubsubEventSource {
    client;
    queryWindowMs;
    id;
    publishAcceptance = 'queued';
    live = new Map();
    constructor(client, queryWindowMs = DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS, id = 'fips') {
        this.client = client;
        this.queryWindowMs = queryWindowMs;
        this.id = id;
        if (!Number.isSafeInteger(queryWindowMs) || queryWindowMs <= 0) {
            throw new RangeError('FIPS pubsub query window must be a positive safe integer');
        }
    }
    async publish(event, _source) {
        await this.client.publish(event);
        return { accepted: true, priority: SOURCE_PRIORITY_FIPS_ENDPOINT };
    }
    subscribe(filters, handler) {
        // A runtime opens live delivery before its history query. Both must share
        // the same wire interest instead of consuming two bounded peer slots.
        const key = JSON.stringify(filters.map((filter) => Object.fromEntries(Object.entries(filter).sort(([a], [b]) => a.localeCompare(b)))));
        let shared = this.live.get(key);
        if (shared) {
            shared.handlers.add(handler);
            for (const incoming of shared.recent.values())
                handler(incoming);
        }
        else {
            shared = { handlers: new Set([handler]), recent: new Map() };
            this.live.set(key, shared);
            const entry = shared;
            try {
                entry.subscription = this.client.subscribe(filters, (event, peerId) => {
                    const incoming = { event, source: fipsEndpointSource(peerId), priority: SOURCE_PRIORITY_FIPS_ENDPOINT };
                    entry.recent.set(event.id, incoming);
                    if (entry.recent.size > this.client.limits.maxReplayEvents)
                        entry.recent.delete(entry.recent.keys().next().value);
                    let failure;
                    for (const receive of [...entry.handlers]) {
                        try {
                            receive(incoming);
                        }
                        catch (error) {
                            failure = error;
                        }
                    }
                    if (failure)
                        throw failure;
                });
            }
            catch (error) {
                this.live.delete(key);
                throw error;
            }
        }
        const entry = shared;
        return { close: () => {
                entry.handlers.delete(handler);
                if (!entry.handlers.size && this.live.get(key) === entry) {
                    this.live.delete(key);
                    entry.subscription?.close();
                }
            } };
    }
    query(filters, options = {}) {
        validateQueryOptions(options);
        if (options.signal?.aborted)
            return Promise.reject(abortError(options.signal.reason));
        const deadline = options.deadline ?? Date.now() + this.queryWindowMs;
        if (deadline <= Date.now()) {
            return Promise.reject(new DOMException('FIPS pubsub query deadline exceeded', 'TimeoutError'));
        }
        return new Promise((resolve, reject) => {
            const events = new Map();
            let settled = false;
            let closeOnAssign = false;
            let timer;
            let subscription;
            const finish = (complete, error) => {
                if (settled)
                    return;
                settled = true;
                if (timer !== undefined)
                    clearTimeout(timer);
                options.signal?.removeEventListener('abort', cancel);
                if (subscription === undefined)
                    closeOnAssign = true;
                else
                    subscription.close();
                if (error !== undefined)
                    reject(error);
                else
                    resolve({ events: ordered(events.values(), options.limit), complete });
            };
            const cancel = () => finish(false, abortError(options.signal?.reason));
            options.signal?.addEventListener('abort', cancel, { once: true });
            subscription = this.subscribe(filters, (incoming) => {
                const event = verifyNostrEvent(incoming.event);
                if (!events.has(event.id))
                    events.set(event.id, { ...incoming, event });
                if (options.limit !== undefined && events.size >= options.limit)
                    finish(true);
            });
            if (closeOnAssign)
                subscription.close();
            if (!settled)
                timer = setTimeout(() => finish(false), Math.max(0, deadline - Date.now()));
        });
    }
}
function ordered(events, limit) {
    const sorted = [...events].sort((left, right) => right.event.created_at - left.event.created_at ||
        (left.event.id < right.event.id ? -1 : left.event.id > right.event.id ? 1 : 0));
    return limit === undefined ? sorted : sorted.slice(0, limit);
}
function abortError(reason) {
    return new DOMException(typeof reason === 'string' ? reason : 'FIPS pubsub query cancelled', 'AbortError');
}
//# sourceMappingURL=fips-pubsub-event-source.js.map