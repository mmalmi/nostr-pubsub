import type { QueryEvent } from './event-bus.js';
import { type NostrFilter, type SourceId } from './types.js';
declare const historyPageToken: unique symbol;
/** Opaque, cursor-bound callback token; do not serialize or reconstruct it. */
export interface HistoryPageToken {
    readonly [historyPageToken]: true;
}
export interface HistoryPage {
    readonly token: HistoryPageToken;
    readonly source: SourceId;
    readonly filter: NostrFilter;
}
export type HistoryPageCompletion = 'complete' | 'partial';
export type HistoryPageStatus = 'advanced' | 'retry' | 'exhausted' | 'limited' | 'stale';
export interface HistoryPageOutcome {
    status: HistoryPageStatus;
    oldest?: number;
    uniqueEvents: number;
}
/** One source/filter cursor. Observe verified wire events before app filtering
 * or cross-source deduplication. The caller owns transport cancellation, query
 * branches, time budgets, and the decision to load more. */
export declare class HistoryCursor {
    private readonly source;
    private readonly maxLimit;
    private filter;
    private initialLimit;
    private active?;
    private stopped;
    constructor(source: SourceId, filter: NostrFilter, now: number, maxLimit: number);
    /** Undefined while a page is active, cancelled, or finished. */
    beginPage(): HistoryPage | undefined;
    /** Duplicates from another source still need this source's observation. */
    observe(token: HistoryPageToken, delivery: QueryEvent): boolean;
    finishPage(token: HistoryPageToken, completion: HistoryPageCompletion): HistoryPageOutcome;
    /** Invalidates outstanding callbacks; construct a new cursor for a new query. */
    cancel(): void;
}
export {};
//# sourceMappingURL=pagination.d.ts.map