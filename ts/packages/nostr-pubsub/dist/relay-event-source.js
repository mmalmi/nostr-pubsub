import { SOURCE_PRIORITY_RELAY, relaySource, } from './source.js';
import { validateQueryOptions, verifyNostrEvent, } from './types.js';
/** Traditional Nostr relay adapter for the shared reader/publisher/live router. */
export class NostrRelayEventSource {
    url;
    transport;
    source;
    constructor(url, transport) {
        this.url = url;
        this.transport = transport;
        this.source = relaySource(url);
    }
    async publish(event, _source) {
        const verified = verifyNostrEvent(event);
        await this.transport.publish(verified);
        return { accepted: true, priority: SOURCE_PRIORITY_RELAY };
    }
    subscribe(filters, handler) {
        const subscription = this.transport.subscribe(filters, {
            onEvent: (event) => {
                try {
                    handler({
                        event: verifyNostrEvent(event),
                        source: this.source,
                        priority: SOURCE_PRIORITY_RELAY,
                    });
                }
                catch {
                    // Invalid relay events never cross the reader boundary.
                }
            },
        });
        return { close: () => subscription.close('nostr-pubsub subscription closed') };
    }
    query(filters, options = {}) {
        validateQueryOptions(options);
        if (options.signal?.aborted)
            return Promise.reject(abortError(options.signal.reason));
        if (options.deadline !== undefined && Date.now() >= options.deadline) {
            return Promise.reject(new DOMException('Nostr relay query deadline exceeded', 'TimeoutError'));
        }
        return new Promise((resolve, reject) => {
            const events = new Map();
            let settled = false;
            let closeOnAssign = false;
            let timer;
            let subscription;
            const finish = (error) => {
                if (settled)
                    return;
                settled = true;
                if (timer !== undefined)
                    clearTimeout(timer);
                options.signal?.removeEventListener('abort', cancel);
                if (subscription === undefined)
                    closeOnAssign = true;
                else
                    subscription.close('nostr-pubsub query complete');
                if (error !== undefined)
                    reject(error);
                else
                    resolve({ events: ordered(events.values(), options.limit) });
            };
            const cancel = () => finish(abortError(options.signal?.reason));
            options.signal?.addEventListener('abort', cancel, { once: true });
            timer = options.deadline === undefined
                ? undefined
                : setTimeout(() => finish(new DOMException('Nostr relay query deadline exceeded', 'TimeoutError')), Math.max(0, options.deadline - Date.now()));
            subscription = this.transport.subscribe(filters, {
                onEvent: (event) => {
                    try {
                        const verified = verifyNostrEvent(event);
                        if (!events.has(verified.id)) {
                            events.set(verified.id, {
                                event: verified,
                                source: this.source,
                                priority: SOURCE_PRIORITY_RELAY,
                            });
                        }
                        if (options.limit !== undefined && events.size >= options.limit)
                            finish();
                    }
                    catch {
                        // Invalid relay events never enter a query result.
                    }
                },
                onClose: () => finish(),
            }, { closeOnEose: true });
            if (closeOnAssign)
                subscription.close('nostr-pubsub query complete');
        });
    }
}
function ordered(events, limit) {
    const sorted = [...events].sort((left, right) => right.event.created_at - left.event.created_at ||
        (left.event.id < right.event.id ? -1 : left.event.id > right.event.id ? 1 : 0));
    return limit === undefined ? sorted : sorted.slice(0, limit);
}
function abortError(reason) {
    return new DOMException(typeof reason === 'string' ? reason : 'Nostr relay query cancelled', 'AbortError');
}
//# sourceMappingURL=relay-event-source.js.map