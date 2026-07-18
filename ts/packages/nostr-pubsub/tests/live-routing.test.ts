import { describe, expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import {
  InMemoryEventBus,
  NostrPubsubRouter,
  allowAllLiveRoutes,
  fipsPeerDefaultRoute,
  localIndexRoute,
  localIndexSource,
  relayRoute,
  subscribeRoutesWithPolicy,
  withRouteDataset,
} from '../src/index.js';

describe('live source router', () => {
  it('globally deduplicates one event received from many live subscriptions', async () => {
    const index = new InMemoryEventBus();
    const fips = new InMemoryEventBus();
    const handler = vi.fn();
    const subscription = await subscribeRoutesWithPolicy([
      { route: localIndexRoute('hashtree:notes'), subscriber: index },
      { route: fipsPeerDefaultRoute('browser-fips-peer'), subscriber: fips },
    ], [{ kinds: [1] }], allowAllLiveRoutes, handler);
    const event = finalizeEvent({
      kind: 1,
      created_at: 10,
      tags: [],
      content: 'same signed event over index and mesh',
    }, generateSecretKey());

    await Promise.all([
      index.publish(event, localIndexSource('hashtree:notes')),
      fips.publish(event, localIndexSource('fips-cache')),
    ]);
    expect(handler).toHaveBeenCalledOnce();
    expect(subscription.routeIds).toEqual(['hashtree:notes', 'browser-fips-peer']);

    subscription.close();
    await index.publish(finalizeEvent({
      kind: 1,
      created_at: 11,
      tags: [],
      content: 'after close',
    }, generateSecretKey()), localIndexSource('hashtree:notes'));
    expect(handler).toHaveBeenCalledOnce();
  });
});

describe('owned Nostr pubsub router', () => {
  it('preserves relay-only publish priority', async () => {
    const route = relayRoute('wss://relay.example');
    const router = new NostrPubsubRouter({
      policy: allowAllLiveRoutes,
      publishSources: [{
        route,
        publisher: {
          publish: async () => ({ accepted: true, priority: -100 }),
        },
      }],
    });
    const event = finalizeEvent({
      kind: 1,
      created_at: 11,
      tags: [],
      content: 'relay only',
    }, generateSecretKey());

    await expect(router.publish(event, localIndexSource('producer'))).resolves.toEqual({
      accepted: true,
      priority: -100,
      reason: undefined,
    });
  });

  it('combines explicit query, publish, and live index/relay routes', async () => {
    const index = new InMemoryEventBus();
    const relay = new InMemoryEventBus();
    const indexRoute = withRouteDataset(localIndexRoute('hashtree:notes'), 'local');
    const remoteRoute = withRouteDataset(relayRoute('wss://relay.example'), 'relay');
    const router = new NostrPubsubRouter({
      policy: allowAllLiveRoutes,
      querySources: [
        { route: indexRoute, reader: index },
        { route: remoteRoute, reader: relay },
      ],
      publishSources: [
        { route: indexRoute, publisher: index },
        { route: remoteRoute, publisher: relay },
      ],
      liveSources: [
        { route: indexRoute, subscriber: index },
        { route: remoteRoute, subscriber: relay },
      ],
    });
    const handler = vi.fn();
    const subscription = await router.subscribe([{ kinds: [1] }], handler);
    const event = finalizeEvent({
      kind: 1,
      created_at: 12,
      tags: [],
      content: 'owned router event',
    }, generateSecretKey());

    await expect(router.publish(event, localIndexSource('producer')))
      .resolves.toMatchObject({ accepted: true });
    expect(handler).toHaveBeenCalledOnce();
    const queried = await router.queryWithContext([{ kinds: [1] }]);
    expect(queried.events).toHaveLength(1);
    expect(queried.events[0]?.event.id).toBe(event.id);
    expect(queried.events[0]?.provenance).toHaveLength(2);
    subscription.close();
  });
});
