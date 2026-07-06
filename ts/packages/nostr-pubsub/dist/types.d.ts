import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent | Event;
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
//# sourceMappingURL=types.d.ts.map