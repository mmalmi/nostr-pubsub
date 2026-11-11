import { AbstractSimplePool } from 'nostr-tools/abstract-pool';
import { verifyEvent } from 'nostr-tools/pure';
import { normalizeURL } from 'nostr-tools/utils';
/** The relay may ask for backoff without specifying a duration. */
export class RelayRateLimitError extends Error {
    relay;
    retryAfterMs;
    name = 'RelayRateLimitError';
    constructor(relay, reason, retryAfterMs) {
        super(reason);
        this.relay = relay;
        this.retryAfterMs = retryAfterMs;
    }
}
export function parseRelayRateLimit(relay, reason, hint) {
    if (reason instanceof RelayRateLimitError)
        return reason;
    const message = reason instanceof Error ? reason.message : reason;
    if (typeof message !== 'string' || !/^rate-limited:/i.test(message.trim()))
        return;
    // Prefer the NIP-01 millisecond parameter. Older relays sometimes give a unit in the reason.
    const legacy = /\bretry (?:in|after) (\d+)\s*(ms|milliseconds?|s|seconds?)\b/i.exec(message);
    const delay = typeof hint === 'string' && /^[1-9]\d*$/.test(hint) ? Number(hint)
        : legacy ? Number(legacy[1]) * (/^m/i.test(legacy[2]) ? 1 : 1000) : 0;
    return new RelayRateLimitError(relay, message, delay > 0 ? Math.min(delay, Number.MAX_SAFE_INTEGER) : undefined);
}
const replyKey = (url, type, id) => JSON.stringify([normalizeURL(url), type, id]);
/** A shared nostr-tools pool that retains optional wire parameters for in-flight replies. */
export class RateLimitAwarePool extends AbstractSimplePool {
    observers;
    constructor(options = {}) {
        const observers = new Map();
        const Socket = options.websocketImplementation ?? globalThis.WebSocket;
        super({
            maxWaitForConnection: 3000, verifyEvent, ...options,
            websocketImplementation: Socket && class extends Socket {
                constructor(url, protocols) {
                    super(url, protocols);
                    this.addEventListener?.('message', ({ data }) => {
                        if (!observers.size || typeof data !== 'string' || !/^\s*\[\s*"(OK|CLOSED)"/.test(data))
                            return;
                        let frame;
                        try {
                            frame = JSON.parse(data);
                        }
                        catch {
                            return;
                        }
                        const [type, id] = frame;
                        if (typeof id !== 'string' || (type === 'OK' && frame[2] !== false))
                            return;
                        const waiting = observers.get(replyKey(String(url), type, id));
                        if (!waiting)
                            return;
                        const error = type === 'CLOSED' ? parseRelayRateLimit(String(url), frame[2], frame[3])
                            : parseRelayRateLimit(String(url), frame[3], frame[4]);
                        if (error)
                            for (const notify of waiting)
                                notify(error);
                    });
                }
            },
        });
        this.observers = observers;
    }
    /** Register before sending a raw operation. Call the returned function on every exit. */
    watchRateLimit(url, type, id) {
        const key = replyKey(url, type, id);
        const waiting = this.observers.get(key) ?? new Set();
        this.observers.set(key, waiting);
        let error;
        const notify = (reply) => { error = reply; };
        waiting.add(notify);
        return (reason) => {
            waiting.delete(notify);
            if (!waiting.size && this.observers.get(key) === waiting)
                this.observers.delete(key);
            return error ?? parseRelayRateLimit(url, reason);
        };
    }
}
/** Release the wire observer on success, rejection and caller-supplied timeout. */
export async function withPublishRateLimit(pool, url, id, publish) {
    const finish = pool instanceof RateLimitAwarePool ? pool.watchRateLimit(url, 'OK', id)
        : (reason) => parseRelayRateLimit(url, reason);
    try {
        const result = await publish();
        finish();
        return result;
    }
    catch (error) {
        throw finish(error) ?? error;
    }
}
//# sourceMappingURL=relay-rate-limit.js.map