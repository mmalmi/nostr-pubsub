import { AbstractSimplePool, type AbstractPoolConstructorOptions } from 'nostr-tools/abstract-pool';
/** The relay may ask for backoff without specifying a duration. */
export declare class RelayRateLimitError extends Error {
    readonly relay: string;
    readonly retryAfterMs?: number | undefined;
    readonly name = "RelayRateLimitError";
    constructor(relay: string, reason: string, retryAfterMs?: number | undefined);
}
export declare function parseRelayRateLimit(relay: string, reason: unknown, hint?: unknown): RelayRateLimitError | undefined;
type Reply = 'OK' | 'CLOSED';
/** A shared nostr-tools pool that retains optional wire parameters for in-flight replies. */
export declare class RateLimitAwarePool extends AbstractSimplePool {
    private readonly observers;
    constructor(options?: Partial<AbstractPoolConstructorOptions>);
    /** Register before sending a raw operation. Call the returned function on every exit. */
    watchRateLimit(url: string, type: Reply, id: string): (reason?: unknown) => RelayRateLimitError | undefined;
}
/** Release the wire observer on success, rejection and caller-supplied timeout. */
export declare function withPublishRateLimit<T>(pool: object, url: string, id: string, publish: () => Promise<T>): Promise<T>;
export {};
//# sourceMappingURL=relay-rate-limit.d.ts.map