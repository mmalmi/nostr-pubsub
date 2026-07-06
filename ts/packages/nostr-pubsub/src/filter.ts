import { matchFilters } from 'nostr-tools/filter';
import type { NostrEvent, NostrFilter, NostrVerifiedEvent } from './types.js';

export interface EventRetentionPolicy {
  filters: NostrFilter[];
  maxEvents: number;
}

export function createEventRetentionPolicy(
  maxEvents: number,
  filters: NostrFilter[],
): EventRetentionPolicy {
  return { maxEvents, filters: filters.map(cloneFilter) };
}

export function retentionAcceptsEvent(
  policy: EventRetentionPolicy,
  event: NostrEvent | NostrVerifiedEvent,
): boolean {
  return policy.maxEvents > 0 && filtersMatch(policy.filters, event);
}

export function filtersMatch(filters: NostrFilter[], event: NostrEvent | NostrVerifiedEvent): boolean {
  return filters.length === 0 || subscriptionFiltersMatch(filters, event);
}

export function subscriptionFiltersMatch(
  filters: NostrFilter[],
  event: NostrEvent | NostrVerifiedEvent,
): boolean {
  return filters.length > 0 && matchFilters(filters, event);
}

export function filterLimit(filters: NostrFilter[]): number | undefined {
  let limit: number | undefined;
  for (const filter of filters) {
    if (typeof filter.limit !== 'number') continue;
    limit = limit === undefined ? filter.limit : Math.min(limit, filter.limit);
  }
  return limit;
}

export function cloneFilter(filter: NostrFilter): NostrFilter {
  const cloned: NostrFilter = { ...filter };
  for (const [key, value] of Object.entries(filter)) {
    if (Array.isArray(value)) {
      (cloned as Record<string, unknown>)[key] = [...value];
    }
  }
  return cloned;
}
