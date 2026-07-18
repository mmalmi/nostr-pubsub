import type { NostrEventPublisher, NostrEventReader, NostrEventSubscriber, NostrEventSubscription, PublishReport, QueryEvent, QueryReport } from './event-bus.js';
import { type EventSource } from './source.js';
import { type NostrEvent, type NostrFilter, type NostrVerifiedEvent, type QueryOptions } from './types.js';
export interface NostrRelaySubscription {
    close(reason?: string): void;
}
export interface NostrRelayTransportHandlers {
    onEvent(event: NostrEvent): void;
    /** Called after a terminal subscription close. */
    onClose?(reasons?: readonly string[]): void;
}
export interface NostrRelayTransportSubscribeOptions {
    /** Historical queries may close on aggregate EOSE or a transport-owned bound. */
    closeOnEose?: boolean;
}
export interface NostrRelayTransport {
    subscribe(filters: NostrFilter[], handlers: NostrRelayTransportHandlers, options?: NostrRelayTransportSubscribeOptions): NostrRelaySubscription;
    publish(event: NostrVerifiedEvent): Promise<void> | void;
}
/** Traditional Nostr relay adapter for the shared reader/publisher/live router. */
export declare class NostrRelayEventSource implements NostrEventReader, NostrEventPublisher, NostrEventSubscriber {
    readonly url: string;
    private readonly transport;
    readonly source: EventSource;
    constructor(url: string, transport: NostrRelayTransport);
    publish(event: NostrEvent, _source?: EventSource): Promise<PublishReport>;
    subscribe(filters: NostrFilter[], handler: (event: QueryEvent) => void): NostrEventSubscription;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}
//# sourceMappingURL=relay-event-source.d.ts.map