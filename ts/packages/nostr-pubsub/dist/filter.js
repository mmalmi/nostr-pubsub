import { matchFilters } from 'nostr-tools/filter';
export function createEventRetentionPolicy(maxEvents, filters) {
    return { maxEvents, filters: filters.map(cloneFilter) };
}
export function retentionAcceptsEvent(policy, event) {
    return policy.maxEvents > 0 && filtersMatch(policy.filters, event);
}
export function filtersMatch(filters, event) {
    return filters.length === 0 || subscriptionFiltersMatch(filters, event);
}
export function subscriptionFiltersMatch(filters, event) {
    return filters.length > 0 && matchFilters(filters, event);
}
export function filterLimit(filters) {
    let limit;
    for (const filter of filters) {
        if (typeof filter.limit !== 'number')
            continue;
        limit = limit === undefined ? filter.limit : Math.min(limit, filter.limit);
    }
    return limit;
}
export function cloneFilter(filter) {
    const cloned = { ...filter };
    for (const [key, value] of Object.entries(filter)) {
        if (Array.isArray(value)) {
            cloned[key] = [...value];
        }
    }
    return cloned;
}
//# sourceMappingURL=filter.js.map