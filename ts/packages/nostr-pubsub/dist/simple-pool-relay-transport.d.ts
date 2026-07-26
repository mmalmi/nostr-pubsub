import { SimplePool } from 'nostr-tools';
import { type NostrEvent, type NostrEventVerifier, type NostrFilter, type NostrVerifiedEvent } from './types.js';
import type { NostrRelaySubscription, NostrRelayTransport, NostrRelayTransportHandlers, NostrRelayTransportSubscribeOptions } from './relay-event-source.js';
type RelayPool = Pick<SimplePool, 'publish' | 'subscribeMany'>;
export interface SimplePoolNostrRelayVerificationBoundary {
    /** Pass this exact function to the shared SimplePool constructor. */
    readonly verifyEvent: NostrEventVerifier;
    /** Canonically admits only the exact object accepted by that function. */
    readonly admitEvent: (event: NostrEvent) => NostrVerifiedEvent;
}
/**
 * Couple SimplePool's synchronous verification with this adapter's canonical
 * admission without a duplicate signature check or a spoofable public marker.
 */
export declare function createSimplePoolNostrRelayVerificationBoundary(verifier?: NostrEventVerifier): SimplePoolNostrRelayVerificationBoundary;
export interface SimplePoolNostrRelayTransportOptions {
    /** Read the application's configured relay URLs when each operation starts. */
    getRelays(): readonly string[];
    /** Supply a shared pool when the application already owns one. */
    pool?: RelayPool;
    /**
     * Opaque proof paired with `new SimplePool({ verifyEvent: boundary.verifyEvent })`.
     * This prevents the adapter from repeating the pool's signature verification.
     */
    verificationBoundary?: SimplePoolNostrRelayVerificationBoundary;
    /** Inactivity bound for historical queries; live subscriptions do not use it. */
    queryQuietWindowMs?: number;
    /** Per-relay publication bound. */
    publishTimeoutMs?: number;
}
/** Browser/WebSocket Nostr relay carrier backed by nostr-tools SimplePool. */
export declare class SimplePoolNostrRelayTransport implements NostrRelayTransport {
    private readonly getRelays;
    private readonly pool;
    private readonly verificationBoundary?;
    private readonly queryQuietWindowMs;
    private readonly publishTimeoutMs;
    constructor(options: SimplePoolNostrRelayTransportOptions);
    subscribe(filters: NostrFilter[], handlers: NostrRelayTransportHandlers, options?: NostrRelayTransportSubscribeOptions): NostrRelaySubscription;
    publish(event: NostrVerifiedEvent): Promise<void>;
}
export {};
//# sourceMappingURL=simple-pool-relay-transport.d.ts.map