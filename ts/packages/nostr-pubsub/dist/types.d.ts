import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent;
export type SourceId = string;
export type NostrEventVerifier = (event: NostrEvent) => boolean;
export interface NostrEventBatchVerificationOptions {
    signal?: AbortSignal;
}
export type NostrEventBatchVerifier = (events: readonly NostrEvent[], options: Readonly<NostrEventBatchVerificationOptions>) => PromiseLike<readonly boolean[]>;
export interface QueryOptions {
    limit?: number;
    /** Cancels work which is no longer useful to the caller. */
    signal?: AbortSignal;
    /** Absolute Unix timestamp in milliseconds shared by every queried source. */
    deadline?: number;
}
export declare function validateQueryOptions(options: QueryOptions): void;
export declare class PubsubError extends Error {
    readonly kind: 'validation' | 'storage';
    private constructor();
    static validation(message: string): PubsubError;
    static storage(message: string): PubsubError;
}
export declare function verifyNostrEvent(event: NostrEvent, verifier?: NostrEventVerifier): NostrVerifiedEvent;
/**
 * Admit events checked by an asynchronous trust boundary such as a Web Worker.
 *
 * The verifier receives immutable defensive clones. No event is admitted unless
 * the verifier returns one `true` boolean per event.
 */
export declare function verifyNostrEventsWith(events: readonly NostrEvent[], verifier: NostrEventBatchVerifier, options?: NostrEventBatchVerificationOptions): Promise<NostrVerifiedEvent[]>;
/** Defensive immutable copy for an event already checked at a trust boundary. */
export declare function copyVerifiedNostrEvent(event: NostrVerifiedEvent): NostrVerifiedEvent;
//# sourceMappingURL=types.d.ts.map