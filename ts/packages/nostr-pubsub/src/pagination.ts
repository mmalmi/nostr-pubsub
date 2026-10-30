import { cloneFilter, filtersMatch } from './filter.js';
import type { QueryEvent } from './event-bus.js';
import { PubsubError, type NostrFilter, type SourceId } from './types.js';

declare const historyPageToken: unique symbol;
/** Opaque, cursor-bound callback token; do not serialize or reconstruct it. */
export interface HistoryPageToken { readonly [historyPageToken]: true; }

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
interface ActivePage {
  token: HistoryPageToken;
  seen: Set<string>;
  oldest?: number;
  overflow: boolean;
}

/** One source/filter cursor. Observe verified wire events before app filtering
 * or cross-source deduplication. The caller owns transport cancellation, query
 * branches, time budgets, and the decision to load more. */
export class HistoryCursor {
  private filter: NostrFilter;
  private initialLimit: number;
  private active?: ActivePage;
  private stopped = false;

  constructor(
    private readonly source: SourceId,
    filter: NostrFilter,
    now: number,
    private readonly maxLimit: number,
  ) {
    const limit = filter.limit ?? 100;
    const until = filter.until ?? now;
    if (!Number.isSafeInteger(limit) || !Number.isSafeInteger(maxLimit) ||
      limit < 1 || limit > maxLimit || maxLimit > 65536) {
      throw PubsubError.validation('invalid history page limits');
    }
    if (!Number.isSafeInteger(until) || until < 0 ||
      (filter.since !== undefined && (!Number.isSafeInteger(filter.since) || filter.since < 0 || filter.since > until))) {
      throw PubsubError.validation('invalid history time bounds');
    }
    this.initialLimit = limit;
    this.filter = { ...cloneFilter(filter), limit, until };
  }

  /** Undefined while a page is active, cancelled, or finished. */
  beginPage(): HistoryPage | undefined {
    if (this.stopped || this.active) return;
    const token = Object.freeze({}) as HistoryPageToken;
    this.active = { token, seen: new Set(), overflow: false };
    return { token, source: this.source, filter: cloneFilter(this.filter) };
  }

  /** Duplicates from another source still need this source's observation. */
  observe(token: HistoryPageToken, delivery: QueryEvent): boolean {
    const page = this.active;
    const event = delivery.event;
    if (!page || page.token !== token || delivery.source.id !== this.source ||
      !filtersMatch([this.filter], event) || page.seen.has(event.id)) return false;
    if (page.seen.size === this.maxLimit) { page.overflow = true; return false; }
    page.seen.add(event.id);
    page.oldest = page.oldest === undefined ? event.created_at : Math.min(page.oldest, event.created_at);
    return true;
  }

  finishPage(token: HistoryPageToken, completion: HistoryPageCompletion): HistoryPageOutcome {
    if (completion !== 'complete' && completion !== 'partial') throw PubsubError.validation('invalid history page completion');
    const page = this.active;
    if (!page || page.token !== token) return { status: 'stale', uniqueEvents: 0 };
    this.active = undefined;
    let status: HistoryPageStatus;
    if (page.overflow) { this.stopped = true; status = 'limited'; }
    else if (completion === 'partial') {
      // A timeout cannot prove that newer unseen events are absent.
      status = 'retry';
    } else if (page.oldest !== undefined) {
      const until = this.filter.until!;
      const limit = this.filter.limit!;
      if (page.oldest < until) {
        this.filter.until = page.oldest; // NIP-01 boundaries are inclusive.
        this.filter.limit = this.initialLimit;
        status = 'advanced';
      } else if (page.seen.size >= limit) {
        if (limit === this.maxLimit) { this.stopped = true; status = 'limited'; }
        else { this.filter.limit = Math.min(limit * 2, this.maxLimit); status = 'advanced'; }
      } else if (until === 0 || (this.filter.since !== undefined && this.filter.since >= until)) {
        this.stopped = true; status = 'exhausted';
      } else {
        this.filter.until = until - 1;
        this.filter.limit = this.initialLimit;
        status = 'advanced';
      }
    } else { this.stopped = true; status = 'exhausted'; }
    return { status, oldest: page.oldest, uniqueEvents: page.seen.size };
  }

  /** Invalidates outstanding callbacks; construct a new cursor for a new query. */
  cancel(): void { this.active = undefined; this.stopped = true; }
}
