import type { NostrEventSubscriber, NostrEventSubscription, QueryEvent } from './event-bus.js';
import { type PubsubPolicy } from './policy.js';
import type { SourceRoute } from './routing.js';
import { type NostrFilter } from './types.js';
export interface LiveRouteSource {
    route: SourceRoute;
    subscriber: NostrEventSubscriber;
}
export interface RoutedLiveEvent extends QueryEvent {
    route: SourceRoute;
}
export interface RoutedLiveSubscription extends NostrEventSubscription {
    readonly routeIds: readonly string[];
}
/** Opens every allowed live route and globally deduplicates noisy mesh/relay delivery. */
export declare function subscribeRoutesWithPolicy(routes: LiveRouteSource[], filters: NostrFilter[], policy: PubsubPolicy, handler: (event: RoutedLiveEvent) => void, options?: {
    authorPubkey?: string;
    capabilities?: string[];
    maxSeenEvents?: number;
}): Promise<RoutedLiveSubscription>;
export declare const allowAllLiveRoutes: PubsubPolicy;
//# sourceMappingURL=live-routing.d.ts.map