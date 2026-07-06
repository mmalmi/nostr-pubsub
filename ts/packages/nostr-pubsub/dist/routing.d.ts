import type { EventBus, QueryEvent } from './event-bus.js';
import { type PolicyDecision, type PubsubPolicy } from './policy.js';
import { type EventSource } from './source.js';
import type { NostrFilter, QueryOptions } from './types.js';
export interface SourceRoute {
    id: string;
    source: EventSource;
    priority: number;
    reason?: string;
    capabilities: string[];
}
export interface RouteQuerySource {
    route: SourceRoute;
    bus: EventBus;
}
export interface RoutedQueryOptions {
    query?: QueryOptions;
}
export interface RouteAttempt {
    route: SourceRoute;
    decision: PolicyDecision;
}
export interface RoutedQueryReport {
    events: QueryEvent[];
    attempts: RouteAttempt[];
}
export declare function sourceRouteFromSource(source: EventSource): SourceRoute;
export declare function localIndexRoute(id: string): SourceRoute;
export declare function peerRoute(id: string): SourceRoute;
export declare function fipsPeerDefaultRoute(id: string): SourceRoute;
export declare function fipsPeerRoute(id: string, priority: number): SourceRoute;
export declare function relayRoute(url: string): SourceRoute;
export declare function withRoutePriority(route: SourceRoute, priority: number): SourceRoute;
export declare function withRouteReason(route: SourceRoute, reason: string): SourceRoute;
export declare function withRouteCapability(route: SourceRoute, capability: string): SourceRoute;
export declare function withRouteCapabilities(route: SourceRoute, capabilities: string[]): SourceRoute;
export declare function queryRoutesWithPolicy(routes: RouteQuerySource[], filters: NostrFilter[], options: RoutedQueryOptions, policy: PubsubPolicy, authorPubkey?: string, capabilities?: string[]): Promise<RoutedQueryReport>;
//# sourceMappingURL=routing.d.ts.map