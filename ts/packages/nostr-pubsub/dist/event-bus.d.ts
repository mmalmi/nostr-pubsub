import { type PubsubPolicy } from './policy.js';
import type { EventSource } from './source.js';
import { type NostrEvent, type NostrFilter, type NostrVerifiedEvent, type QueryOptions } from './types.js';
export interface PublishReport {
    accepted: boolean;
    priority: number;
    reason?: string;
}
export interface QueryEvent {
    event: NostrVerifiedEvent;
    source: EventSource;
    priority: number;
}
export interface QueryReport {
    events: QueryEvent[];
    /** False means the reader returned useful partial results. Omitted means true. */
    complete?: boolean;
}
export interface NostrEventReader {
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}
export interface NostrEventPublisher {
    publish(event: NostrEvent, source: EventSource): Promise<PublishReport>;
}
/** Backwards-compatible combined read/write event bus. */
export interface EventBus extends NostrEventReader, NostrEventPublisher {
}
export declare class InMemoryEventBus implements EventBus {
    private readonly policy?;
    private readonly events;
    constructor(policy?: PubsubPolicy | undefined);
    publish(event: NostrEvent, source: EventSource): Promise<PublishReport>;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}
//# sourceMappingURL=event-bus.d.ts.map