import { SimplePool } from 'nostr-tools';
import { verifyNostrEvent, } from './types.js';
/** Browser/WebSocket Nostr relay carrier backed by nostr-tools SimplePool. */
export class SimplePoolNostrRelayTransport {
    getRelays;
    pool;
    queryQuietWindowMs;
    publishTimeoutMs;
    constructor(options) {
        this.getRelays = options.getRelays;
        this.pool = options.pool ?? new SimplePool();
        this.queryQuietWindowMs = positiveMilliseconds(options.queryQuietWindowMs, 600);
        this.publishTimeoutMs = positiveMilliseconds(options.publishTimeoutMs, 4_500);
    }
    subscribe(filters, handlers, options = {}) {
        if (filters.length === 0) {
            throw new Error('Nostr relay subscriptions require at least one filter');
        }
        const relays = uniqueRelays(this.getRelays());
        if (relays.length === 0) {
            handlers.onClose?.(['No Nostr relays configured']);
            return { close: () => undefined };
        }
        const historical = options.closeOnEose === true;
        let closed = false;
        let quietTimer;
        let subscriptions = [];
        let remainingEose = filters.length;
        let remainingClose = filters.length;
        const closeReasons = [];
        const finish = (reasons, reason) => {
            if (closed)
                return;
            closed = true;
            if (quietTimer !== undefined)
                clearTimeout(quietTimer);
            quietTimer = undefined;
            for (const subscription of subscriptions)
                void subscription.close(reason);
            handlers.onClose?.(reasons);
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
        subscriptions = filters.map((filter) => this.pool.subscribeMany(relays, filter, {
            onevent: (event) => {
                let verified;
                try {
                    verified = verifyNostrEvent(event);
                }
                catch {
                    return;
                }
                handlers.onEvent(verified);
                armQuietWindow();
            },
            oneose: historical
                ? () => {
                    remainingEose -= 1;
                    if (remainingEose === 0) {
                        const reason = 'Nostr relay query reached EOSE';
                        finish([reason], reason);
                    }
                }
                : undefined,
            onclose: (reasons) => {
                closeReasons.push(...reasons);
                remainingClose -= 1;
                if (remainingClose === 0)
                    finish(closeReasons, 'Nostr relay subscription closed');
            },
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
    async publish(event) {
        const verified = verifyNostrEvent(event);
        const relays = uniqueRelays(this.getRelays());
        if (relays.length === 0)
            throw new Error('No Nostr relays configured');
        const attempts = this.pool.publish(relays, verified, { maxWait: this.publishTimeoutMs });
        const results = await Promise.allSettled(attempts.map((attempt) => withTimeout(attempt, this.publishTimeoutMs)));
        if (results.some((result) => result.status === 'fulfilled'))
            return;
        const failure = results.find((result) => result.status === 'rejected');
        throw new Error(rejectionMessage(failure));
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
function rejectionMessage(failure) {
    if (failure?.status !== 'rejected')
        return 'Nostr relay publish failed';
    return failure.reason instanceof Error ? failure.reason.message : String(failure.reason);
}
//# sourceMappingURL=simple-pool-relay-transport.js.map