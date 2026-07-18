import type { NostrEventPublisher, NostrEventReader, NostrEventSubscriber, NostrEventSubscription, PublishReport, QueryEvent, QueryReport } from './event-bus.js';
import { FipsNostrPubsubClient } from './fips-pubsub-client.js';
import { type EventSource } from './source.js';
import { type NostrEvent, type NostrFilter, type QueryOptions } from './types.js';
export declare const DEFAULT_FIPS_PUBSUB_QUERY_WINDOW_MS = 1000;
/** Router adapter for the FIPS-TCP REQ/INV/WANT/EVENT subscription protocol. */
export declare class FipsNostrPubsubEventSource implements NostrEventReader, NostrEventPublisher, NostrEventSubscriber {
    readonly client: FipsNostrPubsubClient;
    readonly queryWindowMs: number;
    constructor(client: FipsNostrPubsubClient, queryWindowMs?: number);
    publish(event: NostrEvent, _source: EventSource): Promise<PublishReport>;
    subscribe(filters: NostrFilter[], handler: (event: QueryEvent) => void): NostrEventSubscription;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}
//# sourceMappingURL=fips-pubsub-event-source.d.ts.map