import { FipsSourceSubscriptions } from './fips-pubsub-source-subscriptions.js';
import { SOURCE_PRIORITY_FIPS_ENDPOINT, } from './source.js';
import { validateQueryOptions, verifyNostrEvent, } from './types.js';
export const DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS = 1_000;
/** Router adapter for the FIPS-TCP REQ/INV/WANT/EVENT subscription protocol. */
export class FipsNostrPubsubEventSource {
    client;
    queryWindowMs;
    id;
    publishAcceptance = 'queued';
    subscriptions;
    constructor(client, queryWindowMs = DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS, id = 'fips') {
        this.client = client;
        this.queryWindowMs = queryWindowMs;
        this.id = id;
        if (!Number.isSafeInteger(queryWindowMs) || queryWindowMs <= 0) {
            throw new RangeError('FIPS pubsub query window must be a positive safe integer');
        }
        this.subscriptions = new FipsSourceSubscriptions(client);
    }
    async publish(event, _source) {
        await this.client.publish(event);
        return { accepted: true, priority: SOURCE_PRIORITY_FIPS_ENDPOINT };
    }
    subscribe(filters, handler) {
        return this.subscriptions.subscribe(filters, handler);
    }
    query(filters, options = {}) {
        validateQueryOptions(options);
        if (options.signal?.aborted)
            return Promise.reject(abortError(options.signal.reason));
        const deadline = Math.min(options.deadline ?? Infinity, Date.now() + this.queryWindowMs);
        if (deadline <= Date.now()) {
            return Promise.reject(new DOMException('FIPS pubsub query deadline exceeded', 'TimeoutError'));
        }
        return new Promise((resolve, reject) => {
            const events = new Map();
            let settled = false;
            let outcome;
            let timer;
            let subscription;
            const finish = (complete, error) => {
                if (settled)
                    return;
                settled = true;
                if (timer !== undefined)
                    clearTimeout(timer);
                options.signal?.removeEventListener('abort', cancel);
                outcome = { complete, error };
                deliverOutcome();
            };
            const deliverOutcome = () => {
                if (!outcome || !subscription)
                    return;
                subscription.close();
                if (outcome.error !== undefined)
                    reject(outcome.error);
                else
                    resolve({ events: ordered(events.values(), options.limit), complete: outcome.complete });
            };
            const cancel = () => finish(false, abortError(options.signal?.reason));
            options.signal?.addEventListener('abort', cancel, { once: true });
            void this.subscribe(filters, (incoming) => {
                if (settled)
                    return;
                const event = verifyNostrEvent(incoming.event);
                if (!events.has(event.id))
                    events.set(event.id, { ...incoming, event });
                if (options.limit !== undefined && events.size >= options.limit)
                    finish(true);
            }).then((assigned) => {
                subscription = assigned;
                deliverOutcome();
            }, (error) => {
                // Validation/startup failure has no subscription handle to release.
                subscription = { close() { } };
                if (settled)
                    deliverOutcome();
                else
                    finish(false, error);
            });
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