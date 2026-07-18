import type {
  NostrEventSubscriber,
  NostrEventSubscription,
  QueryEvent,
} from './event-bus.js';
import {
  allowWithPriority,
  type PubsubPolicy,
  type SourceCandidate,
} from './policy.js';
import type { SourceRoute } from './routing.js';
import { verifyNostrEvent, type NostrFilter } from './types.js';

const DEFAULT_LIVE_DEDUP_EVENTS = 4_096;

export interface LiveRouteSource {
  route: SourceRoute;
  subscriber: NostrEventSubscriber;
}

export interface RoutedLiveEvent extends QueryEvent {
  route: SourceRoute;
}

export interface RoutedLiveSubscription extends NostrEventSubscription {
  readonly routeIds: readonly string[];
}

/** Opens every allowed live route and globally deduplicates noisy mesh/relay delivery. */
export async function subscribeRoutesWithPolicy(
  routes: LiveRouteSource[],
  filters: NostrFilter[],
  policy: PubsubPolicy,
  handler: (event: RoutedLiveEvent) => void,
  options: {
    authorPubkey?: string;
    capabilities?: string[];
    maxSeenEvents?: number;
  } = {},
): Promise<RoutedLiveSubscription> {
  const maximum = options.maxSeenEvents ?? DEFAULT_LIVE_DEDUP_EVENTS;
  if (!Number.isSafeInteger(maximum) || maximum <= 0) {
    throw new RangeError('Live route deduplication limit must be a positive safe integer');
  }
  const seen = new Set<string>();
  const order: string[] = [];
  const active: Array<{ route: SourceRoute; subscription: NostrEventSubscription }> = [];
  for (const source of routes) {
    const candidate: SourceCandidate = {
      source: source.route.source,
      priority: source.route.priority,
      reason: source.route.reason,
      health: {},
    };
    const decision = await policy.checkSource({
      candidate,
      authorPubkey: options.authorPubkey,
      capabilities: options.capabilities ?? source.route.capabilities,
    });
    if (decision.type === 'drop') continue;
    const subscription = await source.subscriber.subscribe(filters, (incoming) => {
      const event = verifyNostrEvent(incoming.event);
      if (seen.has(event.id)) return;
      seen.add(event.id);
      order.push(event.id);
      while (order.length > maximum) {
        const removed = order.shift();
        if (removed !== undefined) seen.delete(removed);
      }
      handler({ ...incoming, event, route: source.route });
    });
    active.push({ route: source.route, subscription });
  }
  return {
    routeIds: active.map(({ route }) => route.id),
    close: () => {
      for (const { subscription } of active.splice(0)) subscription.close();
      seen.clear();
      order.length = 0;
    },
  };
}

export const allowAllLiveRoutes: PubsubPolicy = {
  checkEvent: () => allowWithPriority(0),
  checkSource: () => allowWithPriority(0),
};
