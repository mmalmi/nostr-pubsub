import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Event } from 'nostr-tools/core';
import { finalizeEvent } from 'nostr-tools/pure';
import vectors from '../test-data/interop-vectors.json';
import {
  FIPS_NOSTR_PUBSUB_SERVICE_PORT,
  FipsNostrRelayService,
  FipsPubsubWireAdapter,
  type FipsPubsubServiceContext,
  type FipsPubsubServiceHandler,
  type FipsPubsubServiceNode,
  type NostrRelaySubscription,
  type NostrRelayTransport,
  type NostrRelayTransportHandlers,
} from '../src/index.js';

const events = vectors.events as Record<string, Event>;
const PEER_A = `02${events.fipsAdvert.pubkey}`;
const PEER_B = `03${events.fipsAdvert.pubkey}`;

class MemoryFipsNode implements FipsPubsubServiceNode {
  private readonly services = new Map<number, FipsPubsubServiceHandler>();
  private readonly sessionListeners = new Set<(event: unknown) => void>();

  registerService(port: number, handler: FipsPubsubServiceHandler): () => void {
    this.services.set(port, handler);
    return () => {
      if (this.services.get(port) === handler) this.services.delete(port);
    };
  }

  on(event: 'session', listener: (event: unknown) => void): () => void {
    if (event !== 'session') throw new Error(`unsupported event ${event}`);
    this.sessionListeners.add(listener);
    return () => this.sessionListeners.delete(listener);
  }

  receive(context: FipsPubsubServiceContext): Promise<void> {
    const handler = this.services.get(context.dstPort);
    if (handler === undefined) throw new Error(`no service on port ${context.dstPort}`);
    return Promise.resolve(handler(context));
  }

  closeSession(peerId: string): void {
    for (const listener of this.sessionListeners) {
      listener({ remotePubkey: peerId, state: 'closed' });
    }
  }

  serviceCount(): number {
    return this.services.size;
  }
}

interface MemoryRelayRecord {
  filters: Parameters<NostrRelayTransport['subscribe']>[0];
  handlers: NostrRelayTransportHandlers;
  closed: boolean;
}

class MemoryRelay implements NostrRelayTransport {
  readonly published: Event[] = [];
  readonly subscriptions: MemoryRelayRecord[] = [];

  subscribe(
    filters: Parameters<NostrRelayTransport['subscribe']>[0],
    handlers: NostrRelayTransportHandlers,
  ): NostrRelaySubscription {
    const record: MemoryRelayRecord = { filters, handlers, closed: false };
    this.subscriptions.push(record);
    return {
      close: () => {
        record.closed = true;
      },
    };
  }

  async publish(event: Event): Promise<void> {
    this.published.push(event);
  }

  emit(index: number, event: Event): void {
    const record = this.subscriptions[index];
    if (record !== undefined && !record.closed) record.handlers.onEvent(event);
  }

  eose(index: number): void {
    const record = this.subscriptions[index];
    if (record !== undefined && !record.closed) record.handlers.onEose?.();
  }
}

interface ClientContext {
  context: FipsPubsubServiceContext;
  replies: Uint8Array[];
}

function clientContext(peerId = PEER_A): ClientContext {
  const replies: Uint8Array[] = [];
  return {
    replies,
    context: {
      src: peerId,
      srcPort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      dstPort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      payload: new Uint8Array(),
      async reply(payload, destinationPort) {
        expect(destinationPort).toBe(FIPS_NOSTR_PUBSUB_SERVICE_PORT);
        replies.push(new Uint8Array(payload));
      },
    },
  };
}

function blockedClientContext(peerId = PEER_A): ClientContext & { release: () => void } {
  const replies: Uint8Array[] = [];
  let release = () => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  return {
    replies,
    release,
    context: {
      src: peerId,
      srcPort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      dstPort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      payload: new Uint8Array(),
      async reply(payload, destinationPort) {
        expect(destinationPort).toBe(FIPS_NOSTR_PUBSUB_SERVICE_PORT);
        replies.push(new Uint8Array(payload));
        await gate;
      },
    },
  };
}

async function flushTasks(): Promise<void> {
  for (let index = 0; index < 16; index += 1) await Promise.resolve();
}

describe('FipsNostrRelayService', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('bridges bounded relay replay and sends only addressed matching events', async () => {
    const node = new MemoryFipsNode();
    const relay = new MemoryRelay();
    const adapter = new FipsPubsubWireAdapter();
    const service = new FipsNostrRelayService({
      node,
      relay,
      limits: { subscriptionTtlMs: 1_000 },
    });
    service.start();
    const client = clientContext();
    client.context.payload = adapter.encodeOutbound({
      type: 'req',
      subscriptionId: 'approval',
      filters: [{ kinds: [events.fipsAdvert.kind], limit: 99 }],
    });

    await node.receive(client.context);

    expect(relay.subscriptions).toHaveLength(1);
    expect(relay.subscriptions[0].filters).toEqual([
      { kinds: [events.fipsAdvert.kind], limit: 8 },
    ]);
    relay.emit(0, events.hashtreeRoot);
    relay.emit(0, events.fipsAdvert);
    relay.eose(0);
    await flushTasks();

    expect(client.replies).toHaveLength(2);
    const outbound = adapter.codec.decodeFrame(client.replies[0]);
    expect(outbound.type).toBe('event');
    if (outbound.type !== 'event') throw new Error('expected EVENT');
    expect(outbound.subscriptionId).toBe('approval');
    expect(outbound.event.id).toBe(events.fipsAdvert.id);
    expect(adapter.codec.decodeFrame(client.replies[1])).toEqual({
      type: 'eose',
      subscriptionId: 'approval',
      eventCount: 1,
    });

    await vi.advanceTimersByTimeAsync(1_000);
    expect(relay.subscriptions[0].closed).toBe(true);
    expect(service.activePeerCount()).toBe(0);
    expect(service.activeSubscriptionCount()).toBe(0);
    relay.emit(0, events.fipsAdvert);
    await flushTasks();
    expect(client.replies).toHaveLength(2);

    await service.stop();
    expect(node.serviceCount()).toBe(0);
  });

  it('publishes only unaddressed signature-verified EVENT frames', async () => {
    const node = new MemoryFipsNode();
    const relay = new MemoryRelay();
    const adapter = new FipsPubsubWireAdapter();
    const service = new FipsNostrRelayService({ node, relay });
    service.start();
    const client = clientContext();

    client.context.payload = adapter.encodeOutbound({
      type: 'event',
      event: events.fipsAdvert as never,
    });
    await node.receive(client.context);
    expect(relay.published.map((event) => event.id)).toEqual([events.fipsAdvert.id]);

    client.context.payload = adapter.encodeOutbound({
      type: 'event',
      subscriptionId: 'server-only',
      event: events.fipsAdvert as never,
    });
    await expect(node.receive(client.context)).rejects.toThrow(/subscription-addressed EVENT/);

    const tampered = JSON.stringify(['EVENT', { ...events.fipsAdvert, content: 'tampered' }]);
    client.context.payload = new TextEncoder().encode(tampered);
    await expect(node.receive(client.context)).rejects.toThrow(/invalid Nostr event/);
    expect(relay.published).toHaveLength(1);

    client.context.payload = new Uint8Array(64 * 1024 + 1);
    await expect(node.receive(client.context)).rejects.toThrow(/limit is 65536/);
    await service.stop();
  });

  it('bounds peers, filters, and concurrent subscriptions and cleans CLOSE/session state', async () => {
    const node = new MemoryFipsNode();
    const relay = new MemoryRelay();
    const adapter = new FipsPubsubWireAdapter();
    const service = new FipsNostrRelayService({
      node,
      relay,
      limits: {
        maxPeers: 1,
        maxSubscriptionsPerPeer: 2,
        maxFiltersPerSubscription: 1,
      },
    });
    service.start();
    const first = clientContext(PEER_A);

    for (const subscriptionId of ['one', 'two']) {
      first.context.payload = adapter.encodeOutbound({
        type: 'req',
        subscriptionId,
        filters: [{ kinds: [1] }],
      });
      await node.receive(first.context);
    }
    expect(service.activeSubscriptionCount()).toBe(2);

    first.context.payload = adapter.encodeOutbound({
      type: 'req',
      subscriptionId: 'three',
      filters: [{ kinds: [1] }],
    });
    await expect(node.receive(first.context)).rejects.toThrow(/subscription limit/);

    first.context.payload = adapter.encodeOutbound({
      type: 'req',
      subscriptionId: 'too-many-filters',
      filters: [{ kinds: [1] }, { kinds: [2] }],
    });
    await expect(node.receive(first.context)).rejects.toThrow(/filter limit/);

    const second = clientContext(PEER_B);
    second.context.payload = adapter.encodeOutbound({
      type: 'req',
      subscriptionId: 'other-peer',
      filters: [{ kinds: [1] }],
    });
    await expect(node.receive(second.context)).rejects.toThrow(/peer limit/);

    first.context.payload = adapter.encodeOutbound({ type: 'close', subscriptionId: 'one' });
    await node.receive(first.context);
    expect(relay.subscriptions[0].closed).toBe(true);
    expect(service.activeSubscriptionCount()).toBe(1);

    node.closeSession(PEER_A);
    expect(relay.subscriptions[1].closed).toBe(true);
    expect(service.activePeerCount()).toBe(0);
    expect(service.activeSubscriptionCount()).toBe(0);

    await service.stop();
  });

  it('requires an authenticated FIPS pubkey and service port in both directions', async () => {
    const node = new MemoryFipsNode();
    const relay = new MemoryRelay();
    const adapter = new FipsPubsubWireAdapter();
    const service = new FipsNostrRelayService({ node, relay });
    service.start();
    const client = clientContext('not-an-authenticated-pubkey');
    client.context.payload = adapter.encodeOutbound({
      type: 'req',
      subscriptionId: 'request',
      filters: [{ kinds: [1] }],
    });
    await expect(node.receive(client.context)).rejects.toThrow(/authenticated FIPS peer/);

    const wrongSourcePort = clientContext();
    wrongSourcePort.context.srcPort = 1;
    wrongSourcePort.context.payload = client.context.payload;
    await expect(node.receive(wrongSourcePort.context)).rejects.toThrow(/source port 7368/);
    await service.stop();
  });

  it('deduplicates events and bounds pending replies under relay flooding', async () => {
    const node = new MemoryFipsNode();
    const relay = new MemoryRelay();
    const adapter = new FipsPubsubWireAdapter();
    const errors: Error[] = [];
    const service = new FipsNostrRelayService({
      node,
      relay,
      onError: (error) => errors.push(error),
    });
    service.start();
    const client = blockedClientContext();
    client.context.payload = adapter.encodeOutbound({
      type: 'req',
      subscriptionId: 'bounded',
      filters: [{ kinds: [events.fipsAdvert.kind] }],
    });
    await node.receive(client.context);

    relay.emit(0, events.fipsAdvert);
    relay.emit(0, events.fipsAdvert);
    for (let index = 0; index < 16; index += 1) {
      relay.emit(
        0,
        finalizeEvent(
          {
            kind: events.fipsAdvert.kind,
            created_at: 1_700_000_000 + index,
            tags: [],
            content: `flood-${index}`,
          },
          Uint8Array.from({ length: 32 }, (_, byte) => byte + 1),
        ),
      );
    }
    await flushTasks();

    expect(client.replies).toHaveLength(1);
    expect(errors.filter((error) => /reply queue is full/.test(error.message))).toHaveLength(1);
    client.release();
    await service.stop();
    expect(client.replies.length).toBeLessThanOrEqual(8);
  });
});
