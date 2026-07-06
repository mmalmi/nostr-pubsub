import { describe, expect, it } from 'vitest';
import type { Event } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
import vectors from '../test-data/interop-vectors.json';
import {
  DEFAULT_INV_WANT_HOP_LIMIT,
  InMemoryEventBus,
  PubsubPeerSubscriptionStore,
  allowWithPriority,
  createContentKey,
  createFrame,
  createEventRetentionPolicy,
  deliveryActionForEvent,
  fipsPeerDefaultRoute,
  fipsPeerRoute,
  inventoryFromFrame,
  inventoryToPeersDeliveryPolicy,
  localIndexRoute,
  peerRoute,
  queryRoutesWithPolicy,
  relayRoute,
  retentionAcceptsEvent,
  wantFromInventory,
  type EventSource,
  type PolicyDecision,
  type PubsubPolicy,
  type SourceRoute,
} from '../src/index.js';

const events = vectors.events as Record<string, Event>;

describe('Rust/TypeScript interop vectors', () => {
  it('keeps source route priorities and relay-last ordering compatible', () => {
    const local = localIndexRoute('hashtree:events');
    const fips = fipsPeerDefaultRoute('npub1fips');
    const peer = peerRoute('npub1peer');
    const relay = relayRoute('wss://relay.example');

    expect(local.priority).toBe(vectors.routeDefaults.expectedPriorities.localIndex);
    expect(fips.priority).toBe(vectors.routeDefaults.expectedPriorities.fipsEndpoint);
    expect(peer.priority).toBe(vectors.routeDefaults.expectedPriorities.peer);
    expect(relay.priority).toBe(vectors.routeDefaults.expectedPriorities.relay);

    const attempted = [relay, peer, local, fips]
      .sort((left, right) => right.priority - left.priority)
      .map((route) => route.id);
    expect(attempted).toEqual(vectors.routeDefaults.expectedOrder);
  });

  it('matches Rust retention and Nostr generic-tag filter behavior', () => {
    for (const testCase of vectors.retentionCases) {
      const policy = createEventRetentionPolicy(
        testCase.policy.maxEvents,
        testCase.policy.filters as Filter[],
      );
      expect(retentionAcceptsEvent(policy, events[testCase.event]), testCase.name).toBe(
        testCase.accepts,
      );
    }
  });

  it('matches peer subscription interest and inventory delivery behavior', () => {
    const testCase = vectors.peerSubscriptionCase;
    const store = new PubsubPeerSubscriptionStore(testCase.limits);
    for (const operation of testCase.operations) {
      store.upsertFilters(operation.peerId, operation.subscriptionId, operation.filters as Filter[]);
    }

    expect(store.peerCount()).toBe(testCase.expectedPeerCount);
    expect(store.subscriptionCount()).toBe(testCase.expectedSubscriptionCount);

    for (const interest of testCase.interests) {
      expect(store.peerInterest(interest.peerId, events[interest.event])).toBe(interest.interest);
    }
    for (const expected of testCase.interestedPeers) {
      expect(store.interestedPeers(events[expected.event])).toEqual(expected.peers);
    }

    const delivery = inventoryToPeersDeliveryPolicy();
    for (const expected of testCase.deliveryActions) {
      expect(deliveryActionForEvent(delivery, store, expected.peerId, events[expected.event])).toBe(
        expected.action,
      );
    }
  });

  it('keeps inv/want frames keyed the same way', () => {
    const testCase = vectors.invWantCase;
    const key = createContentKey(testCase.key.streamId, testCase.key.origin, testCase.key.seq);
    const frame = createFrame(key, testCase.payload, testCase.hopLimit);
    const inventory = inventoryFromFrame(frame);
    const want = wantFromInventory(inventory);

    expect(DEFAULT_INV_WANT_HOP_LIMIT).toBe(16);
    expect(inventory.key).toEqual(key);
    expect(inventory.payloadBytes).toBe(testCase.expectedPayloadBytes);
    expect(inventory.hopLimit).toBe(testCase.hopLimit);
    expect(want.key).toEqual(key);
  });

  it('orders routed queries by FIPS and policy priority before relay fallback', async () => {
    const testCase = vectors.routedQueryCase;
    const policy = new VectorPolicy(
      new Map(testCase.routes.map((route) => [route.route.id, route.policyDecision])),
    );
    const routeSources = [];
    for (const route of testCase.routes) {
      const sourceRoute = sourceRouteFromVector(route.route);
      const bus = new InMemoryEventBus();
      for (const eventName of route.events) {
        await bus.publish(events[eventName], sourceRoute.source);
      }
      routeSources.push({ route: sourceRoute, bus });
    }

    const report = await queryRoutesWithPolicy(
      routeSources,
      testCase.filters as Filter[],
      { query: { limit: testCase.limit } },
      policy,
    );

    expect(report.attempts.map((attempt) => attempt.route.id)).toEqual(testCase.expectedAttempts);
    expect(report.events.map((event) => event.event.id)).toEqual(testCase.expectedEvents);
  });
});

class VectorPolicy implements PubsubPolicy {
  constructor(private readonly decisions: Map<string, PolicyDecision>) {}

  checkEvent(): PolicyDecision {
    return allowWithPriority(0);
  }

  checkSource(context: { candidate: { source: EventSource } }): PolicyDecision {
    return this.decisions.get(context.candidate.source.id) ?? allowWithPriority(0);
  }
}

function sourceRouteFromVector(route: {
  kind: string;
  id: string;
  priority?: number;
}): SourceRoute {
  switch (route.kind) {
    case 'relay':
      return relayRoute(route.id);
    case 'fips':
      return route.priority === undefined
        ? fipsPeerDefaultRoute(route.id)
        : fipsPeerRoute(route.id, route.priority);
    case 'peer':
      return peerRoute(route.id);
    case 'local':
      return localIndexRoute(route.id);
    default:
      throw new Error(`unknown route kind: ${route.kind}`);
  }
}
