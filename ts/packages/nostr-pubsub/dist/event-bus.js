import { matchFilter } from 'nostr-tools/filter';
import { allowWithPriority, reportParts } from './policy.js';
import { verifyNostrEvent, validateQueryOptions, } from './types.js';
export class InMemoryEventBus {
    policy;
    events = [];
    constructor(policy) {
        this.policy = policy;
    }
    async publish(event, source) {
        const verifiedEvent = verifyNostrEvent(event);
        const decision = this.policy === undefined
            ? allowWithPriority(0)
            : await this.policy.checkEvent({ event: verifiedEvent, source });
        const report = reportParts(decision);
        if (report.accepted) {
            this.events.push({ event: verifiedEvent, source, priority: report.priority });
        }
        return report;
    }
    async query(filters, options = {}) {
        validateQueryOptions(options);
        throwIfQueryStopped(options);
        const ordered = [...this.events].sort((left, right) => right.event.created_at - left.event.created_at || compareText(left.event.id, right.event.id));
        const byId = new Map();
        const effectiveFilters = filters.length === 0 ? [{}] : filters;
        for (const filter of effectiveFilters) {
            let matched = 0;
            const filterResultLimit = typeof filter.limit === 'number' ? filter.limit : undefined;
            for (const stored of ordered) {
                if (filterResultLimit !== undefined && matched >= filterResultLimit)
                    break;
                if (!matchFilter(filter, stored.event))
                    continue;
                matched += 1;
                if (!byId.has(stored.event.id))
                    byId.set(stored.event.id, { ...stored });
            }
            throwIfQueryStopped(options);
        }
        const events = [...byId.values()].sort((left, right) => right.event.created_at - left.event.created_at || compareText(left.event.id, right.event.id));
        return { events: options.limit === undefined ? events : events.slice(0, options.limit) };
    }
}
function throwIfQueryStopped(options) {
    if (options.signal?.aborted)
        throw abortError(options.signal.reason);
    if (options.deadline !== undefined && Date.now() >= options.deadline) {
        throw new DOMException('Nostr event query deadline exceeded', 'TimeoutError');
    }
}
function abortError(reason) {
    const message = typeof reason === 'string' ? reason : 'Nostr event query cancelled';
    return new DOMException(message, 'AbortError');
}
function compareText(left, right) {
    return left < right ? -1 : left > right ? 1 : 0;
}
//# sourceMappingURL=event-bus.js.map