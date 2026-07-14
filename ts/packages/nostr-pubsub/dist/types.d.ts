import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent;
export type SourceId = string;
export interface QueryOptions {
    limit?: number;
}
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