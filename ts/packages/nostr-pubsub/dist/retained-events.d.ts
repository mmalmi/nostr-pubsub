import { type EventRetentionPolicy } from './filter.js';
import { type NostrEvent, type NostrFilter, type NostrVerifiedEvent, type QueryOptions } from './types.js';
export declare const MAX_RETAINED_EVENT_BYTES: number;
export declare const MAX_RETAINED_SNAPSHOT_BYTES: number;
export declare const MAX_RETAINED_EVENTS = 4096;
/** A local snapshot of explicitly selected public events; never peer authorization. */
export declare class RetainedEvents {
    private readonly policy;
    private events;
    constructor(policy: EventRetentionPolicy);
    restore(events: readonly NostrEvent[]): void;
    insert(input: NostrEvent): boolean;
    snapshot(): NostrEvent[];
    query(filters: NostrFilter[], options?: QueryOptions): NostrVerifiedEvent[];
}
export declare function retainedEventIsCurrent(event: NostrEvent): boolean;
//# sourceMappingURL=retained-events.d.ts.map