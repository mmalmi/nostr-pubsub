import { copyVerifiedNostrEvent, validateQueryOptions, verifyNostrEvent, } from './types.js';
import { decisionPriority, } from './policy.js';
import { fipsEndpointSource, localIndexSource, peerSource, relaySource, sourceKindDefaultPriority, } from './source.js';
export const DEFAULT_ROUTE_DATASET_ID = 'default';
export function sourceRouteFromSource(source) {
    return {
        id: source.id,
        datasetId: DEFAULT_ROUTE_DATASET_ID,
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
export function withRouteDataset(route, datasetId) {
    if (datasetId.length === 0)
        throw new Error('Route dataset identity must not be empty');
    return { ...route, datasetId };
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
    const queryOptions = options.query ?? {};
    validateQueryOptions(queryOptions);
    const candidates = await allowedCandidates(routes, policy, authorPubkey, capabilities);
    const groups = groupByDataset(candidates);
    const globalLimit = queryOptions.limit;
    if (globalLimit !== undefined && globalLimit <= 0) {
        return {
            events: [],
            attempts: [],
            datasets: groups.map(([datasetId]) => ({ datasetId, complete: true, eventCount: 0 })),
            complete: true,
        };
    }
    const results = await Promise.all(groups.map(([datasetId, replicas]) => queryDataset(datasetId, replicas, filters, queryOptions, globalLimit)));
    const merged = mergeRoutedEvents(results.flatMap(({ events }) => events));
    const events = globalLimit === undefined ? merged : merged.slice(0, globalLimit);
    return {
        events,
        attempts: results.flatMap(({ attempts }) => attempts),
        datasets: results.map(({ report }) => report),
        complete: results.every(({ report }) => report.complete),
    };
}
async function allowedCandidates(routes, policy, authorPubkey, capabilities) {
    const candidates = [];
    for (const [ordinal, routeSource] of routes.entries()) {
        const route = routeSource.route;
        const candidate = {
            source: route.source,
            priority: route.priority,
            reason: route.reason,
            health: {},
        };
        const decision = await policy.checkSource({
            candidate,
            authorPubkey,
            capabilities: capabilities ?? route.capabilities,
        });
        if (decision.type === 'drop')
            continue;
        candidates.push({
            effectivePriority: saturatingAddI32(route.priority, decisionPriority(decision)),
            ordinal,
            routeSource,
            decision,
        });
    }
    return candidates.sort((left, right) => right.effectivePriority - left.effectivePriority || left.ordinal - right.ordinal);
}
function groupByDataset(candidates) {
    const groups = new Map();
    for (const candidate of candidates) {
        const datasetId = candidate.routeSource.route.datasetId;
        const replicas = groups.get(datasetId);
        if (replicas === undefined)
            groups.set(datasetId, [candidate]);
        else
            replicas.push(candidate);
    }
    return [...groups];
}
async function queryDataset(datasetId, replicas, filters, queryOptions, globalLimit) {
    const events = [];
    const attempts = [];
    let complete = false;
    for (const replica of replicas) {
        const started = Date.now();
        const result = await runReader(replica.routeSource, filters, {
            ...queryOptions,
            limit: globalLimit,
        });
        const durationMs = Date.now() - started;
        if (result.type === 'report') {
            try {
                const routed = routeEvents(result.report, replica, datasetId);
                events.push(...routed);
                const reportComplete = result.report.complete !== false;
                attempts.push(attempt(replica, datasetId, {
                    type: reportComplete ? 'success' : 'partial',
                    eventCount: routed.length,
                    durationMs,
                }));
                if (reportComplete) {
                    complete = true;
                    break;
                }
            }
            catch (error) {
                attempts.push(failedAttempt(replica, datasetId, durationMs, error));
            }
            continue;
        }
        if (result.type === 'failure') {
            attempts.push(failedAttempt(replica, datasetId, durationMs, result.error));
            continue;
        }
        attempts.push(attempt(replica, datasetId, {
            type: result.type,
            eventCount: 0,
            durationMs,
        }));
        if (result.stop)
            break;
    }
    const deduplicated = mergeRoutedEvents(events);
    return {
        events: deduplicated,
        attempts,
        report: { datasetId, complete, eventCount: deduplicated.length },
    };
}
function routeEvents(report, candidate, datasetId) {
    return report.events.map((queryEvent) => ({
        ...queryEvent,
        event: verifiedReaderEvent(queryEvent.event),
        provenance: [{
                routeId: candidate.routeSource.route.id,
                datasetId,
                source: queryEvent.source,
                priority: queryEvent.priority,
            }],
    }));
}
function mergeRoutedEvents(events) {
    const byId = new Map();
    for (const event of events) {
        const existing = byId.get(event.event.id);
        if (existing === undefined) {
            byId.set(event.event.id, { ...event, provenance: [...event.provenance] });
        }
        else {
            existing.provenance.push(...event.provenance);
        }
    }
    for (const event of byId.values()) {
        event.provenance = event.provenance
            .sort(compareProvenance)
            .filter((item, index, all) => index === 0 || compareProvenance(all[index - 1], item) !== 0);
    }
    return [...byId.values()].sort((left, right) => right.event.created_at - left.event.created_at || compareText(left.event.id, right.event.id));
}
function runReader(routeSource, filters, options) {
    const reader = routeSource.reader ?? routeSource.bus;
    if (reader === undefined) {
        return Promise.resolve({
            type: 'failure',
            error: new Error(`Route ${routeSource.route.id} has no event reader`),
        });
    }
    if (options.signal?.aborted)
        return Promise.resolve({ type: 'cancelled', stop: true });
    if (options.deadline !== undefined && Date.now() >= options.deadline) {
        return Promise.resolve({ type: 'deadline-exceeded', stop: true });
    }
    return new Promise((resolve) => {
        let settled = false;
        const controller = new AbortController();
        let timeout;
        const finish = (result) => {
            if (settled)
                return;
            settled = true;
            if (timeout !== undefined)
                clearTimeout(timeout);
            options.signal?.removeEventListener('abort', cancel);
            resolve(result);
        };
        const cancel = () => {
            controller.abort(options.signal?.reason);
            finish({ type: 'cancelled', stop: true });
        };
        options.signal?.addEventListener('abort', cancel, { once: true });
        if (options.signal?.aborted) {
            cancel();
            return;
        }
        timeout = options.deadline === undefined
            ? undefined
            : setTimeout(() => {
                controller.abort(new DOMException('Nostr event query deadline exceeded', 'TimeoutError'));
                finish({ type: 'deadline-exceeded', stop: true });
            }, Math.max(0, options.deadline - Date.now()));
        Promise.resolve()
            .then(() => reader.query(filters, { ...options, signal: controller.signal }))
            .then((report) => finish({ type: 'report', report }), (error) => finish(readerErrorResult(error)));
    });
}
function attempt(candidate, datasetId, outcome) {
    return { route: candidate.routeSource.route, datasetId, decision: candidate.decision, outcome };
}
function failedAttempt(candidate, datasetId, durationMs, error) {
    return attempt(candidate, datasetId, {
        type: 'failure',
        eventCount: 0,
        durationMs,
        error: errorParts(error),
    });
}
function errorParts(error) {
    if (error instanceof Error)
        return { name: error.name, message: error.message };
    return { name: 'Error', message: String(error) };
}
function verifiedReaderEvent(event) {
    try {
        return copyVerifiedNostrEvent(event);
    }
    catch {
        return verifyNostrEvent(event);
    }
}
function readerErrorResult(error) {
    const name = typeof error === 'object' && error !== null && 'name' in error
        ? String(error.name)
        : '';
    if (name === 'AbortError')
        return { type: 'cancelled', stop: false };
    if (name === 'TimeoutError')
        return { type: 'deadline-exceeded', stop: false };
    return { type: 'failure', error };
}
function compareProvenance(left, right) {
    return compareText(left.datasetId, right.datasetId)
        || compareText(left.routeId, right.routeId)
        || compareText(left.source.kind, right.source.kind)
        || compareText(left.source.id, right.source.id)
        || left.priority - right.priority;
}
function compareText(left, right) {
    return left < right ? -1 : left > right ? 1 : 0;
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