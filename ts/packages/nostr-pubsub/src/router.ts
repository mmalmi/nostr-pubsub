import type {
  EventBus,
  NostrEventPublisher,
  NostrEventSubscriber,
  NostrEventSubscription,
  PublishReport,
  QueryEvent,
  QueryReport,
} from './event-bus.js';
import {
  subscribeRoutesWithPolicy,
  type LiveRouteSource,
  type RoutedLiveEvent,
  type RoutedLiveSubscription,
} from './live-routing.js';
import type { PubsubPolicy, SourceCandidate } from './policy.js';
import {
  queryRoutesWithPolicy,
  type RouteQuerySource,
  type RoutedQueryOptions,
  type RoutedQueryReport,
  type SourceRoute,
} from './routing.js';
import type { EventSource } from './source.js';
import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';

export interface RouterPublishSource {
  route: SourceRoute;
  publisher: NostrEventPublisher;
}

export interface NostrPubsubRouterOptions {
  policy: PubsubPolicy;
  querySources?: RouteQuerySource[];
  publishSources?: RouterPublishSource[];
  liveSources?: LiveRouteSource[];
}

/** Owned transport-neutral router for indexes, FIPS peers, and Nostr relays. */
export class NostrPubsubRouter implements EventBus, NostrEventSubscriber {
  private readonly policy: PubsubPolicy;
  private readonly querySources: RouteQuerySource[];
  private readonly publishSources: RouterPublishSource[];
  private readonly liveSources: LiveRouteSource[];

  constructor(options: NostrPubsubRouterOptions) {
    this.policy = options.policy;
    this.querySources = [...(options.querySources ?? [])];
    this.publishSources = [...(options.publishSources ?? [])];
    this.liveSources = [...(options.liveSources ?? [])];
  }

  queryWithContext(
    filters: NostrFilter[],
    options: RoutedQueryOptions = {},
    authorPubkey?: string,
    capabilities?: string[],
  ): Promise<RoutedQueryReport> {
    return queryRoutesWithPolicy(
      this.querySources,
      filters,
      options,
      this.policy,
      authorPubkey,
      capabilities,
    );
  }

  async query(filters: NostrFilter[], options: QueryOptions = {}): Promise<QueryReport> {
    const report = await this.queryWithContext(filters, { query: options });
    return {
      events: report.events.map(({ event, source, priority }) => ({ event, source, priority })),
      complete: report.complete,
    };
  }

  async publish(event: NostrEvent, source: EventSource): Promise<PublishReport> {
    const selected: RouterPublishSource[] = [];
    for (const target of this.publishSources) {
      const candidate: SourceCandidate = {
        source: target.route.source,
        priority: target.route.priority,
        reason: target.route.reason,
        health: {},
      };
      const decision = await this.policy.checkSource({
        candidate,
        capabilities: target.route.capabilities,
      });
      if (decision.type !== 'drop') selected.push(target);
    }
    const reports = await Promise.all(selected.map(async (target) => {
      try {
        return { routeId: target.route.id, report: await target.publisher.publish(event, source) };
      } catch (error) {
        return { routeId: target.route.id, error };
      }
    }));
    const accepted = reports.filter((result) => result.report?.accepted === true);
    const failures = reports.flatMap((result) => {
      if (result.report?.accepted === true) return [];
      if (result.error !== undefined) return [`${result.routeId}: ${errorMessage(result.error)}`];
      return [`${result.routeId}: ${result.report?.reason ?? 'rejected'}`];
    });
    return {
      accepted: accepted.length > 0,
      priority: accepted.reduce(
        (maximum, result) => Math.max(maximum, result.report!.priority),
        0,
      ),
      reason: failures.length > 0
        ? failures.join('; ')
        : selected.length === 0 ? 'no publish route was selected' : undefined,
    };
  }

  subscribeWithOptions(
    filters: NostrFilter[],
    handler: (event: RoutedLiveEvent) => void,
    options: {
      authorPubkey?: string;
      capabilities?: string[];
      maxSeenEvents?: number;
    } = {},
  ): Promise<RoutedLiveSubscription> {
    return subscribeRoutesWithPolicy(
      this.liveSources,
      filters,
      this.policy,
      handler,
      options,
    );
  }

  async subscribe(
    filters: NostrFilter[],
    handler: (event: QueryEvent) => void,
  ): Promise<NostrEventSubscription> {
    return this.subscribeWithOptions(filters, ({ event, source, priority }) => {
      handler({ event, source, priority });
    });
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
