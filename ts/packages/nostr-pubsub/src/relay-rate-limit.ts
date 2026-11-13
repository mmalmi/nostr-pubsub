import { AbstractSimplePool, type AbstractPoolConstructorOptions } from 'nostr-tools/abstract-pool';
import { verifyEvent } from 'nostr-tools/pure';
import { normalizeURL } from 'nostr-tools/utils';

/** The relay may ask for backoff without specifying a duration. */
export class RelayRateLimitError extends Error {
  readonly name = 'RelayRateLimitError';
  constructor(readonly relay: string, reason: string, readonly retryAfterMs?: number) {
    super(reason);
  }
}

export function parseRelayRateLimit(relay: string, reason: unknown, hint?: unknown): RelayRateLimitError | undefined {
  if (reason instanceof RelayRateLimitError) return reason;
  const message = reason instanceof Error ? reason.message : reason;
  if (typeof message !== 'string' || !/^rate-limited:/i.test(message.trim())) return;
  // Prefer the NIP-01 millisecond parameter. Older relays sometimes give a unit in the reason.
  const legacy = /\bretry (?:in|after) (\d+)\s*(ms|milliseconds?|s|seconds?)\b/i.exec(message);
  const delay = typeof hint === 'string' && /^[1-9]\d*$/.test(hint) ? Number(hint)
    : legacy ? Number(legacy[1]) * (/^m/i.test(legacy[2]) ? 1 : 1000) : 0;
  return new RelayRateLimitError(relay, message, delay > 0 ? Math.min(delay, Number.MAX_SAFE_INTEGER) : undefined);
}

type Reply = 'OK' | 'CLOSED';
type Observer = (error: RelayRateLimitError) => void;
const replyKey = (url: string, type: Reply, id: string) => JSON.stringify([normalizeURL(url), type, id]);

/** A shared nostr-tools pool that retains optional wire parameters for in-flight replies. */
export class RateLimitAwarePool extends AbstractSimplePool {
  private readonly observers: Map<string, Set<Observer>>;

  constructor(options: Partial<AbstractPoolConstructorOptions> = {}) {
    const observers = new Map<string, Set<Observer>>();
    const Socket = options.websocketImplementation ?? globalThis.WebSocket;
    super({
      maxWaitForConnection: 3000, verifyEvent, ...options,
      websocketImplementation: Socket && class extends Socket {
        constructor(url: string | URL, protocols?: string | string[]) {
          super(url, protocols);
          this.addEventListener?.('message', ({ data }) => {
            if (!observers.size || typeof data !== 'string' || !/^\s*\[\s*"(OK|CLOSED)"/.test(data)) return;
            let frame;
            try { frame = JSON.parse(data); } catch { return; }
            const [type, id] = frame;
            if (typeof id !== 'string' || (type === 'OK' && frame[2] !== false)) return;
            const waiting = observers.get(replyKey(String(url), type, id));
            if (!waiting) return;
            const error = type === 'CLOSED' ? parseRelayRateLimit(String(url), frame[2], frame[3])
              : parseRelayRateLimit(String(url), frame[3], frame[4]);
            if (error) for (const notify of waiting) notify(error);
          });
        }
      },
    });
    this.observers = observers;
  }

  /** Register before sending a raw operation. Call the returned function on every exit. */
  watchRateLimit(url: string, type: Reply, id: string): (reason?: unknown) => RelayRateLimitError | undefined {
    const key = replyKey(url, type, id);
    const waiting = this.observers.get(key) ?? new Set<Observer>();
    this.observers.set(key, waiting);
    let error: RelayRateLimitError | undefined;
    const notify = (reply: RelayRateLimitError) => { error = reply; };
    waiting.add(notify);
    return (reason) => {
      waiting.delete(notify);
      if (!waiting.size && this.observers.get(key) === waiting) this.observers.delete(key);
      return error ?? parseRelayRateLimit(url, reason);
    };
  }
}

/** Release the wire observer on success, rejection and caller-supplied timeout. */
export async function withPublishRateLimit<T>(pool: object, url: string, id: string, publish: () => Promise<T>): Promise<T> {
  const finish = pool instanceof RateLimitAwarePool ? pool.watchRateLimit(url, 'OK', id)
    : (reason?: unknown) => parseRelayRateLimit(url, reason);
  try {
    const result = await publish();
    finish();
    return result;
  } catch (error) {
    throw finish(error) ?? error;
  }
}
