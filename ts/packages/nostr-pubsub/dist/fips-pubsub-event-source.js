import { SOURCE_PRIORITY_FIPS_ENDPOINT, fipsEndpointSource, } from './source.js';
import { validateQueryOptions, verifyNostrEvent, } from './types.js';
export const DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS = 1_000;
/** Router adapter for the FIPS-TCP REQ/INV/WANT/EVENT subscription protocol. */
export class FipsNostrPubsubEventSource {
    client;
    queryWindowMs;
    constructor(client, queryWindowMs = DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS) {
        this.client = client;
        this.queryWindowMs = queryWindowMs;
        if (!Number.isSafeInteger(queryWindowMs) || queryWindowMs <= 0) {
            throw new RangeError('FIPS pubsub query window must be a positive safe integer');
        }
    }
    async publish(event, _source) {
        await this.client.publish(event);
        return { accepted: true, priority: SOURCE_PRIORITY_FIPS_ENDPOINT };
    }
    subscribe(filters, handler) {
        const subscription = this.client.subscribe(filters, (event, peerId) => handler({
            event,
            source: fipsEndpointSource(peerId),
            priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
        }));
        return { close: () => subscription.close() };
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