import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent;
export type SourceId = string;
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
export declare function verifyNostrEvent(event: NostrEvent): NostrVerifiedEvent;
/** Defensive immutable copy for an event already checked at a trust boundary. */
export declare function copyVerifiedNostrEvent(event: NostrVerifiedEvent): NostrVerifiedEvent;
//# sourceMappingURL=types.d.ts.map