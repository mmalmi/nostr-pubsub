import type { NostrFilter } from './types.js';
/** Identify targeted public lookups before validating their scope. */
export declare function isRelayDiscoveryFilter(filter: NostrFilter): boolean;
/** Preserve OR semantics while keeping every lookup key in a separate scope. */
export declare function splitRelayDiscoveryFilters(filters: NostrFilter[]): {
    receiver: NostrFilter[];
    discovery: NostrFilter[][];
};
export declare function discoveryScope(filters: NostrFilter[]): string;
//# sourceMappingURL=discovery-scope.d.ts.map