import type { EventBus, QueryEvent } from './event-bus.js';
import {
  decisionPriority,
  type PolicyDecision,
  type PubsubPolicy,
  type SourceCandidate,
} from './policy.js';
import {
  fipsEndpointSource,
  localIndexSource,
  peerSource,
  relaySource,
  sourceKindDefaultPriority,
  type EventSource,
} from './source.js';
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

export function sourceRouteFromSource(source: EventSource): SourceRoute {
  return {
    id: source.id,
    source,
    priority: sourceKindDefaultPriority(source.kind),
    capabilities: [],
  };
}

export function localIndexRoute(id: string): SourceRoute {
  return sourceRouteFromSource(localIndexSource(id));
}

export function peerRoute(id: string): SourceRoute {
  return sourceRouteFromSource(peerSource(id));
}

export function fipsPeerDefaultRoute(id: string): SourceRoute {
  return sourceRouteFromSource(fipsEndpointSource(id));
}

export function fipsPeerRoute(id: string, priority: number): SourceRoute {
  return withRoutePriority(fipsPeerDefaultRoute(id), priority);
}

export function relayRoute(url: string): SourceRoute {
  return sourceRouteFromSource(relaySource(url));
}

export function withRoutePriority(route: SourceRoute, priority: number): SourceRoute {
  return { ...route, priority };
}

export function withRouteReason(route: SourceRoute, reason: string): SourceRoute {
  return { ...route, reason };
}

export function withRouteCapability(route: SourceRoute, capability: string): SourceRoute {
  return { ...route, capabilities: [...route.capabilities, capability] };
}

export function withRouteCapabilities(route: SourceRoute, capabilities: string[]): SourceRoute {
  return { ...route, capabilities: [...route.capabilities, ...capabilities] };
}

export async function queryRoutesWithPolicy(
  routes: RouteQuerySource[],
  filters: NostrFilter[],
  options: RoutedQueryOptions,
  policy: PubsubPolicy,
  authorPubkey?: string,
  capabilities?: string[],
): Promise<RoutedQueryReport> {
  const candidates: Array<{
    effectivePriority: number;
    routeSource: RouteQuerySource;
    decision: PolicyDecision;
  }> = [];

  for (const routeSource of routes) {
    const route = routeSource.route;
    const routeCapabilities = capabilities ?? route.capabilities;
    const candidate: SourceCandidate = {
      source: route.source,
      priority: route.priority,
      reason: route.reason,
      health: {},
    };
    const decision = await policy.checkSource({
      candidate,
      authorPubkey,
      capabilities: routeCapabilities,
    });
    if (decision.type === 'drop') continue;
    candidates.push({
      effectivePriority: saturatingAddI32(route.priority, decisionPriority(decision)),
      routeSource,
      decision,
    });
  }

  candidates.sort((left, right) => right.effectivePriority - left.effectivePriority);

  const report: RoutedQueryReport = { events: [], attempts: [] };
  const queryOptions = options.query ?? {};
  const limit = queryOptions.limit ?? Number.POSITIVE_INFINITY;

  for (const { routeSource, decision } of candidates) {
    if (report.events.length >= limit) break;
    report.attempts.push({ route: routeSource.route, decision });
    const remaining = limit - report.events.length;
    const routeLimit = Number.isFinite(remaining)
      ? Math.min(queryOptions.limit ?? remaining, remaining)
      : queryOptions.limit;
    const routeReport = await routeSource.bus.query(filters, { ...queryOptions, limit: routeLimit });
    report.events.push(...routeReport.events);
  }

  return report;
}

function saturatingAddI32(left: number, right: number): number {
  const sum = left + right;
  if (sum > 2_147_483_647) return 2_147_483_647;
  if (sum < -2_147_483_648) return -2_147_483_648;
  return sum;
}
