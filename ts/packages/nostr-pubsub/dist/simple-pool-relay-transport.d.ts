import { SimplePool } from 'nostr-tools';
import { type NostrFilter, type NostrVerifiedEvent } from './types.js';
import type { NostrRelaySubscription, NostrRelayTransport, NostrRelayTransportHandlers, NostrRelayTransportSubscribeOptions } from './relay-event-source.js';
type RelayPool = Pick<SimplePool, 'publish' | 'subscribeMany'>;
export interface SimplePoolNostrRelayTransportOptions {
    /** Read the application's configured relay URLs when each operation starts. */
    getRelays(): readonly string[];
    /** Supply a shared pool when the application already owns one. */
    pool?: RelayPool;
    /** Inactivity bound for historical queries; live subscriptions do not use it. */
    queryQuietWindowMs?: number;
    /** Per-relay publication bound. */
    publishTimeoutMs?: number;
}
/** Browser/WebSocket Nostr relay carrier backed by nostr-tools SimplePool. */
export declare class SimplePoolNostrRelayTransport implements NostrRelayTransport {
    private readonly getRelays;
    private readonly pool;
    private readonly queryQuietWindowMs;
    private readonly publishTimeoutMs;
    constructor(options: SimplePoolNostrRelayTransportOptions);
    subscribe(filters: NostrFilter[], handlers: NostrRelayTransportHandlers, options?: NostrRelayTransportSubscribeOptions): NostrRelaySubscription;
    publish(event: NostrVerifiedEvent): Promise<void>;
}
export {};
//# sourceMappingURL=simple-pool-relay-transport.d.ts.map