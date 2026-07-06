import { type PubsubPolicy } from './policy.js';
import type { EventSource } from './source.js';
import type { NostrFilter, NostrVerifiedEvent, QueryOptions } from './types.js';
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
}
export interface EventBus {
    publish(event: NostrVerifiedEvent, source: EventSource): Promise<PublishReport>;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}
export declare class InMemoryEventBus implements EventBus {
    private readonly policy?;
    private readonly events;
    constructor(policy?: PubsubPolicy | undefined);
    publish(event: NostrVerifiedEvent, source: EventSource): Promise<PublishReport>;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}
//# sourceMappingURL=event-bus.d.ts.map