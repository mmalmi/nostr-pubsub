import { NostrRuntime } from './runtime.js';
import { type NostrEvent } from './types.js';
import type { NostrRuntimeOptions, RuntimeEventStore, RuntimeSource } from './runtime-types.js';
/** Classify before validation so malformed private traffic cannot fall back to public publication. */
export declare function isPrivateRelayEvent(event: Pick<NostrEvent, 'kind'>): boolean;
/**
 * Shape check for private sends, including queued retries. Invite responses
 * (1059) and wakes (21111) carry exactly one `p`. A kind 1060 message may carry
 * one `p` or none: without it, the envelope needs its `header` tag, and the
 * recipient finds it through the sender key it already follows. Whether a
 * message should name its recipient is decided by the protocol library, not here.
 */
export declare function privateEnvelopeAllowed(event: NostrEvent): boolean;
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