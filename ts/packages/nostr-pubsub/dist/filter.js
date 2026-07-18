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
    // There is no correct aggregate for an OR of multiple NIP-01 filters: each
    // filter owns its own initial-result limit. Keep the helper safe for legacy
    // single-filter callers instead of silently under-returning multi-filter REQs.
    return filters.length === 1 && typeof filters[0].limit === 'number'
        ? filters[0].limit
        : undefined;
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