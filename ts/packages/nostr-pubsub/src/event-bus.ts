import { matchFilter } from 'nostr-tools/filter';
import { allowWithPriority, reportParts, type PubsubPolicy } from './policy.js';
import type { EventSource } from './source.js';
import {
  verifyNostrEvent,
  validateQueryOptions,
  type NostrEvent,
  type NostrFilter,
  type NostrVerifiedEvent,
  type QueryOptions,
} from './types.js';

export interface PublishReport {
  accepted: boolean;
  priority: number;
  reason?: string;
}

export interface QueryEvent {
  event: NostrVerifiedEvent;
  source: EventSource;
  priority: number;
}

export interface QueryReport {
  events: QueryEvent[];
  /** False means the reader returned useful partial results. Omitted means true. */
  complete?: boolean;
}

export interface NostrEventReader {
  query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}

export interface NostrEventPublisher {
  publish(event: NostrEvent, source: EventSource): Promise<PublishReport>;
}

/** Backwards-compatible combined read/write event bus. */
export interface EventBus extends NostrEventReader, NostrEventPublisher {}

interface StoredEvent {
  event: NostrVerifiedEvent;
  source: EventSource;
  priority: number;
}

export class InMemoryEventBus implements EventBus {
  private readonly events: StoredEvent[] = [];

  constructor(private readonly policy?: PubsubPolicy) {}

  async publish(event: NostrEvent, source: EventSource): Promise<PublishReport> {
    const verifiedEvent = verifyNostrEvent(event);
    const decision =
      this.policy === undefined
        ? allowWithPriority(0)
        : await this.policy.checkEvent({ event: verifiedEvent, source });
    const report = reportParts(decision);
    if (report.accepted) {
      this.events.push({ event: verifiedEvent, source, priority: report.priority });
    }
    return report;
  }

  async query(filters: NostrFilter[], options: QueryOptions = {}): Promise<QueryReport> {
    validateQueryOptions(options);
    throwIfQueryStopped(options);
    const ordered = [...this.events].sort((left, right) =>
      right.event.created_at - left.event.created_at || compareText(left.event.id, right.event.id)
    );
    const byId = new Map<string, QueryEvent>();
    const effectiveFilters = filters.length === 0 ? [{}] : filters;
    for (const filter of effectiveFilters) {
      let matched = 0;
      const filterResultLimit = typeof filter.limit === 'number' ? filter.limit : undefined;
      for (const stored of ordered) {
        if (filterResultLimit !== undefined && matched >= filterResultLimit) break;
        if (!matchFilter(filter, stored.event)) continue;
        matched += 1;
        if (!byId.has(stored.event.id)) byId.set(stored.event.id, { ...stored });
      }
      throwIfQueryStopped(options);
    }
    const events = [...byId.values()].sort((left, right) =>
      right.event.created_at - left.event.created_at || compareText(left.event.id, right.event.id)
    );
    return { events: options.limit === undefined ? events : events.slice(0, options.limit) };
  }
}

function throwIfQueryStopped(options: QueryOptions): void {
  if (options.signal?.aborted) throw abortError(options.signal.reason);
  if (options.deadline !== undefined && Date.now() >= options.deadline) {
    throw new DOMException('Nostr event query deadline exceeded', 'TimeoutError');
  }
}

function abortError(reason: unknown): DOMException {
  const message = typeof reason === 'string' ? reason : 'Nostr event query cancelled';
  return new DOMException(message, 'AbortError');
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}
