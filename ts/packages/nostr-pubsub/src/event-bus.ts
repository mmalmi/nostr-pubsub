import { filterLimit, filtersMatch } from './filter.js';
import { allowWithPriority, reportParts, type PubsubPolicy } from './policy.js';
import type { EventSource } from './source.js';
import {
  verifyNostrEvent,
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
}

export interface EventBus {
  publish(event: NostrEvent, source: EventSource): Promise<PublishReport>;
  query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
}

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
    const limit = options.limit ?? filterLimit(filters);
    const events: QueryEvent[] = [];
    for (const stored of this.events) {
      if (limit !== undefined && events.length >= limit) break;
      if (filtersMatch(filters, stored.event)) {
        events.push({ ...stored });
      }
    }
    return { events };
  }
}
