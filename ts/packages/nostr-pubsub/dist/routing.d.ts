import { type NostrFilter, type QueryOptions } from './types.js';
import type { NostrEventReader, QueryEvent } from './event-bus.js';
import { type PolicyDecision, type PubsubPolicy } from './policy.js';
import { type EventSource } from './source.js';
export declare const DEFAULT_ROUTE_DATASET_ID = "default";
export interface SourceRoute {
    id: string;
    /** Routes with the same dataset id are replicas; different ids are additive. */
    datasetId: string;
    source: EventSource;
    priority: number;
    reason?: string;
    capabilities: string[];
}
export interface RouteQuerySource {
    route: SourceRoute;
    reader: NostrEventReader;
}
export interface RoutedQueryOptions {
    query?: QueryOptions;
}
export interface RouteFailure {
    name: string;
    message: string;
}
export type RouteAttemptOutcome = {
    type: 'success' | 'partial';
    eventCount: number;
    durationMs: number;
} | {
    type: 'failure';
    eventCount: 0;
    durationMs: number;
    error: RouteFailure;
} | {
    type: 'cancelled' | 'deadline-exceeded';
    eventCount: 0;
    durationMs: number;
};
export interface RouteAttempt {
    route: SourceRoute;
    datasetId: string;
    decision: PolicyDecision;
    outcome: RouteAttemptOutcome;
}
export interface RoutedEventProvenance {
    routeId: string;
    datasetId: string;
    source: EventSource;
    priority: number;
}
export interface RoutedQueryEvent extends QueryEvent {
    provenance: RoutedEventProvenance[];
}
export interface RoutedDatasetReport {
    datasetId: string;
    complete: boolean;
    eventCount: number;
}
export interface RoutedQueryReport {
    events: RoutedQueryEvent[];
    attempts: RouteAttempt[];
    datasets: RoutedDatasetReport[];
    complete: boolean;
}
export declare function sourceRouteFromSource(source: EventSource): SourceRoute;
export declare function localIndexRoute(id: string): SourceRoute;
export declare function peerRoute(id: string): SourceRoute;
export declare function fipsPeerDefaultRoute(id: string): SourceRoute;
export declare function fipsPeerRoute(id: string, priority: number): SourceRoute;
export declare function relayRoute(url: string): SourceRoute;
export declare function withRouteDataset(route: SourceRoute, datasetId: string): SourceRoute;
export declare function withRoutePriority(route: SourceRoute, priority: number): SourceRoute;
export declare function withRouteReason(route: SourceRoute, reason: string): SourceRoute;
export declare function withRouteCapability(route: SourceRoute, capability: string): SourceRoute;
export declare function withRouteCapabilities(route: SourceRoute, capabilities: string[]): SourceRoute;
export declare function queryRoutesWithPolicy(routes: RouteQuerySource[], filters: NostrFilter[], options: RoutedQueryOptions, policy: PubsubPolicy, authorPubkey?: string, capabilities?: string[]): Promise<RoutedQueryReport>;
//# sourceMappingURL=routing.d.ts.map