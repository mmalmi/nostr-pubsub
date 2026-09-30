import type { NostrEvent, NostrFilter } from './types.js';
/** Candidate dispatch index only; the caller still applies every original filter. */
export declare class RuntimeInterests<T> {
    private readonly buckets;
    private readonly memberships;
    add(value: T, filters: NostrFilter[]): void;
    remove(value: T): void;
    candidates(event: NostrEvent): Set<T>;
}
//# sourceMappingURL=runtime-interests.d.ts.map