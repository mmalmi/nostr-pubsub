import type { NostrEvent, NostrFilter, NostrVerifiedEvent } from './types.js';
export interface EventRetentionPolicy {
    filters: NostrFilter[];
    maxEvents: number;
}
export declare function createEventRetentionPolicy(maxEvents: number, filters: NostrFilter[]): EventRetentionPolicy;
export declare function retentionAcceptsEvent(policy: EventRetentionPolicy, event: NostrEvent | NostrVerifiedEvent): boolean;
export declare function filtersMatch(filters: NostrFilter[], event: NostrEvent | NostrVerifiedEvent): boolean;
export declare function subscriptionFiltersMatch(filters: NostrFilter[], event: NostrEvent | NostrVerifiedEvent): boolean;
export declare function filterLimit(filters: NostrFilter[]): number | undefined;
export declare function cloneFilter(filter: NostrFilter): NostrFilter;
//# sourceMappingURL=filter.d.ts.map