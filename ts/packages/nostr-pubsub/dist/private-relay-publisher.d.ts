import { NostrRuntime } from './runtime.js';
import { type NostrEvent } from './types.js';
import type { NostrRuntimeOptions, RuntimeEventStore, RuntimeSource } from './runtime-types.js';
/** Classify before validation so malformed private traffic cannot fall back to public publication. */
export declare function isPrivateRelayEvent(event: Pick<NostrEvent, 'kind'>): boolean;
/**
 * Keep retry queues in their intended connection lane, retaining quarantined
 * records in storage. Historical received events remain locally readable.
 * Supply separate underlying stores for public reception and private sending.
 */
export declare function publicationLaneStore(store: RuntimeEventStore, lane: 'public' | 'private'): RuntimeEventStore;
export type PrivatePublicationSource = Pick<RuntimeSource, 'id' | 'publishAcceptance'> & Required<Pick<RuntimeSource, 'publish'>>;
export type PrivateRelayPublisherOptions = Pick<NostrRuntimeOptions, 'relays' | 'store' | 'websocketImplementation' | 'publishTimeoutMs' | 'historyTimeoutMs' | 'onError'> & {
    sources?: readonly PrivatePublicationSource[];
};
export type PrivateRelayPublisher = Pick<NostrRuntime, 'publish' | 'retryPending' | 'setRelays' | 'getRelayStats' | 'removeSource' | 'close'> & {
    addSource(source: PrivatePublicationSource): void;
};
/**
 * An EVENT-only relay connection owner with a fresh NIP-42 signer per attempt.
 * It cannot expose an account signer or issue subscriptions, queries or inventories.
 * Supplied storage remains caller-owned. This isolates protocol identities; it
 * does not hide network addresses or timing.
 */
export declare function createPrivateRelayPublisher(options?: PrivateRelayPublisherOptions): PrivateRelayPublisher;
//# sourceMappingURL=private-relay-publisher.d.ts.map