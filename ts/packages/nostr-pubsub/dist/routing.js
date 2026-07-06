import { decisionPriority, } from './policy.js';
import { fipsEndpointSource, localIndexSource, peerSource, relaySource, sourceKindDefaultPriority, } from './source.js';
export function sourceRouteFromSource(source) {
    return {
        id: source.id,
        source,
        priority: sourceKindDefaultPriority(source.kind),
        capabilities: [],
    };
}
export function localIndexRoute(id) {
    return sourceRouteFromSource(localIndexSource(id));
}
export function peerRoute(id) {
    return sourceRouteFromSource(peerSource(id));
}
export function fipsPeerDefaultRoute(id) {
    return sourceRouteFromSource(fipsEndpointSource(id));
}
export function fipsPeerRoute(id, priority) {
    return withRoutePriority(fipsPeerDefaultRoute(id), priority);
}
export function relayRoute(url) {
    return sourceRouteFromSource(relaySource(url));
}
export function withRoutePriority(route, priority) {
    return { ...route, priority };
}
export function withRouteReason(route, reason) {
    return { ...route, reason };
}
export function withRouteCapability(route, capability) {
    return { ...route, capabilities: [...route.capabilities, capability] };
}
export function withRouteCapabilities(route, capabilities) {
    return { ...route, capabilities: [...route.capabilities, ...capabilities] };
}
export async function queryRoutesWithPolicy(routes, filters, options, policy, authorPubkey, capabilities) {
    const candidates = [];
    for (const routeSource of routes) {
        const route = routeSource.route;
        const routeCapabilities = capabilities ?? route.capabilities;
        const candidate = {
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
        if (decision.type === 'drop')
            continue;
        candidates.push({
            effectivePriority: saturatingAddI32(route.priority, decisionPriority(decision)),
            routeSource,
            decision,
        });
    }
    candidates.sort((left, right) => right.effectivePriority - left.effectivePriority);
    const report = { events: [], attempts: [] };
    const queryOptions = options.query ?? {};
    const limit = queryOptions.limit ?? Number.POSITIVE_INFINITY;
    for (const { routeSource, decision } of candidates) {
        if (report.events.length >= limit)
            break;
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
function saturatingAddI32(left, right) {
    const sum = left + right;
    if (sum > 2_147_483_647)
        return 2_147_483_647;
    if (sum < -2_147_483_648)
        return -2_147_483_648;
    return sum;
}
//# sourceMappingURL=routing.js.map