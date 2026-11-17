import { withPublishRateLimit } from './relay-rate-limit.js';
import { SimplePool } from 'nostr-tools';
import { copyVerifiedNostrEvent, PubsubError, verifyNostrEvent, } from './types.js';
const verificationBoundaries = new WeakSet();
/**
 * Couple SimplePool's synchronous verification with this adapter's canonical
 * admission without a duplicate signature check or a spoofable public marker.
 */
export function createSimplePoolNostrRelayVerificationBoundary(verifier) {
    const admitted = new WeakMap();
    const boundary = Object.freeze({
        verifyEvent(event) {
            admitted.delete(event);
            try {
                admitted.set(event, verifyNostrEvent(event, verifier));
                return true;
            }
            catch {
                return false;
            }
        },
        admitEvent(event) {
            const verified = admitted.get(event);
            if (!verified) {
                throw PubsubError.validation('relay event was not admitted by the configured SimplePool boundary');
            }
            return copyVerifiedNostrEvent(verified);
        },
    });
    verificationBoundaries.add(boundary);
    return boundary;
}
/** Browser/WebSocket Nostr relay carrier backed by nostr-tools SimplePool. */
export class SimplePoolNostrRelayTransport {
    getRelays;
    pool;
    verificationBoundary;
    queryQuietWindowMs;
    publishTimeoutMs;
    constructor(options) {
        if (options.verificationBoundary !== undefined
            && !verificationBoundaries.has(options.verificationBoundary)) {
            throw new TypeError('invalid SimplePool Nostr relay verification boundary');
        }
        if (options.verificationBoundary !== undefined && options.pool === undefined) {
            throw new TypeError('SimplePool verification boundary requires its paired shared pool');
        }
        this.getRelays = options.getRelays;
        this.pool = options.pool ?? new SimplePool();
        this.verificationBoundary = options.verificationBoundary;
        this.queryQuietWindowMs = positiveMilliseconds(options.queryQuietWindowMs, 600);
        this.publishTimeoutMs = positiveMilliseconds(options.publishTimeoutMs, 4_500);
    }
    subscribe(filters, handlers, options = {}) {
        if (filters.length === 0) {
            throw new Error('Nostr relay subscriptions require at least one filter');
        }
        const relays = uniqueRelays(this.getRelays());
        if (relays.length === 0) {
            handlers.onClose?.(['No Nostr relays configured'], false);
            return { close: () => undefined };
        }
        const historical = options.closeOnEose === true;
        let closed = false;
        let quietTimer;
        let subscriptions = [];
        let remaining = historical ? filters.length * relays.length : filters.length;
        let allEosed = true;
        const closeReasons = [];
        const finish = (reasons, reason, complete = false) => {
            if (closed)
                return;
            closed = true;
            if (quietTimer !== undefined)
                clearTimeout(quietTimer);
            quietTimer = undefined;
            for (const subscription of subscriptions)
                void subscription.close(reason);
            handlers.onClose?.(reasons, complete);
        };
        const armQuietWindow = () => {
            if (!historical || closed)
                return;
            if (quietTimer !== undefined)
                clearTimeout(quietTimer);
            quietTimer = setTimeout(() => {
                const reason = 'Nostr relay query quiet window elapsed';
                finish([reason], reason);
            }, this.queryQuietWindowMs);
        };
        const onEvent = (event) => {
            if (closed)
                return false;
            let verified;
            try {
                verified = this.verificationBoundary?.admitEvent(event) ?? verifyNostrEvent(event);
            }
            catch {
                return false;
            }
            handlers.onEvent(verified);
            armQuietWindow();
            return true;
        };
        const ended = (eosed, reasons) => {
            allEosed &&= eosed;
            closeReasons.push(...reasons);
            if (--remaining > 0)
                return;
            const reason = allEosed ? 'Nostr relay query reached EOSE' : 'Nostr relay subscription closed';
            finish(allEosed ? [reason] : closeReasons, reason, allEosed);
        };
        subscriptions = historical
            ? filters.flatMap(filter => {
                const seen = new Set();
                return relays.map(url => this.subscribeHistory(url, filter, event => {
                    if (seen.has(event.id))
                        return;
                    if (onEvent(event))
                        seen.add(event.id);
                }, ended));
            })
            : filters.map(filter => this.pool.subscribeMany(relays, filter, {
                onevent: onEvent, onclose: reasons => ended(false, reasons),
            }));
        if (closed) {
            for (const subscription of subscriptions) {
                void subscription.close('Nostr relay subscription already complete');
            }
        }
        else {
            armQuietWindow();
        }
        return {
            close: (reason) => {
                const closeReason = reason ?? 'Nostr relay subscription closed';
                finish(reason === undefined ? [] : [reason], closeReason);
            },
        };
    }
    subscribeHistory(url, filter, onEvent, ended) {
        let done = false;
        const end = (complete, reasons) => {
            if (!done) {
                done = true;
                ended(complete, reasons);
            }
        };
        if (!this.pool.ensureRelay) {
            // A pool facade can emit EOSE during close or from its own wait timer.
            const maxWait = Math.max(this.queryQuietWindowMs, 4500);
            const deadline = Date.now() + maxWait;
            return this.pool.subscribeMany([url], filter, {
                maxWait, onevent: onEvent,
                oneose: () => queueMicrotask(() => end(Date.now() < deadline, [])),
                onclose: reasons => end(false, reasons),
            });
        }
        // A concrete pool's aggregate EOSE includes failures and expired timers. Observe the wire EOSE.
        let close;
        const start = async () => {
            if (this.pool.allowConnectingToRelay?.(url, ['read', [filter]]) === false) {
                end(false, ['Relay connection declined']);
                return;
            }
            const relay = await this.pool.ensureRelay(url, { connectionTimeout: this.pool.maxWaitForConnection }).catch(error => {
                this.pool.onRelayConnectionFailure?.(url);
                throw error;
            });
            this.pool.onRelayConnectionSuccess?.(url);
            if (done)
                return;
            const subscription = relay.prepareSubscription([filter], {
                onevent: onEvent, oneose: () => end(true, []), onclose: reason => end(false, [reason]),
                receivedEvent: this.pool.trackRelays ? (source, id) => {
                    const seen = this.pool.seenOn;
                    if (seen && !seen.has(id))
                        seen.set(id, new Set());
                    seen?.get(id)?.add(source);
                } : undefined,
            });
            relay.ongoingOperations++;
            relay.idleSince = undefined;
            close = reason => { if (relay.openSubs.has(subscription.id))
                subscription.close(reason); };
            await relay.send(JSON.stringify(['REQ', subscription.id, filter]));
        };
        void start().catch(error => end(false, [String(error)]));
        return { close: reason => { done = true; close?.(reason); } };
    }
    async publish(event) {
        const verified = verifyNostrEvent(event);
        const relays = uniqueRelays(this.getRelays());
        if (relays.length === 0)
            throw new Error('No Nostr relays configured');
        // Pool.publish can resolve a connection-failure message instead of rejecting.
        // A concrete relay publish resolves only after its positive OK response.
        const attempts = this.pool.ensureRelay
            ? relays.map(async (url) => {
                const relay = await this.pool.ensureRelay(url, { connectionTimeout: this.publishTimeoutMs });
                relay.publishTimeout = this.publishTimeoutMs;
                return relay.publish(verified);
            })
            : this.pool.publish(relays, verified, { maxWait: this.publishTimeoutMs }).map(async (attempt) => {
                const result = await attempt;
                if (result.startsWith('connection failure:'))
                    throw new Error(result);
                return result;
            });
        try {
            await Promise.any(attempts.map((attempt, index) => withPublishRateLimit(this.pool, relays[index], verified.id, () => withTimeout(attempt, this.publishTimeoutMs))));
        }
        catch (error) {
            if (!(error instanceof AggregateError))
                throw error;
            const first = error.errors[0];
            if (first instanceof Error)
                throw first;
            throw new Error(first === undefined ? 'Nostr relay publish failed' : String(first));
        }
    }
}
function uniqueRelays(relays) {
    return [...new Set(relays)];
}
function positiveMilliseconds(value, fallback) {
    if (value === undefined)
        return fallback;
    if (!Number.isFinite(value) || value <= 0) {
        throw new RangeError('Nostr relay timeouts must be positive finite milliseconds');
    }
    return Math.max(1, Math.trunc(value));
}
function withTimeout(promise, timeoutMs) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Nostr relay publish timed out')), timeoutMs);
        promise.then((value) => {
            clearTimeout(timer);
            resolve(value);
        }, (error) => {
            clearTimeout(timer);
            reject(error);
        });
    });
}
//# sourceMappingURL=simple-pool-relay-transport.js.map