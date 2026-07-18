import type { EventBus, NostrEventPublisher, NostrEventSubscriber, NostrEventSubscription, PublishReport, QueryEvent, QueryReport } from './event-bus.js';
import { type LiveRouteSource, type RoutedLiveEvent, type RoutedLiveSubscription } from './live-routing.js';
import type { PubsubPolicy } from './policy.js';
import { type RouteQuerySource, type RoutedQueryOptions, type RoutedQueryReport, type SourceRoute } from './routing.js';
import type { EventSource } from './source.js';
import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';
export interface RouterPublishSource {
    route: SourceRoute;
    publisher: NostrEventPublisher;
}
export interface NostrPubsubRouterOptions {
    policy: PubsubPolicy;
    querySources?: RouteQuerySource[];
    publishSources?: RouterPublishSource[];
    liveSources?: LiveRouteSource[];
}
/** Owned transport-neutral router for indexes, FIPS peers, and Nostr relays. */
export declare class NostrPubsubRouter implements EventBus, NostrEventSubscriber {
    private readonly policy;
    private readonly querySources;
    private readonly publishSources;
    private readonly liveSources;
    constructor(options: NostrPubsubRouterOptions);
    queryWithContext(filters: NostrFilter[], options?: RoutedQueryOptions, authorPubkey?: string, capabilities?: string[]): Promise<RoutedQueryReport>;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
    publish(event: NostrEvent, source: EventSource): Promise<PublishReport>;
    subscribeWithOptions(filters: NostrFilter[], handler: (event: RoutedLiveEvent) => void, options?: {
        authorPubkey?: string;
        capabilities?: string[];
        maxSeenEvents?: number;
    }): Promise<RoutedLiveSubscription>;
    subscribe(filters: NostrFilter[], handler: (event: QueryEvent) => void): Promise<NostrEventSubscription>;
}
//# sourceMappingURL=router.d.ts.map