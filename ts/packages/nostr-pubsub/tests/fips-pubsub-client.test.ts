import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createInterface, type Interface } from 'node:readline';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey, getPublicKey } from 'nostr-tools/pure';
import type { FipsServiceContext } from '@fips/tcp';
import {
  FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES,
  FIPS_NOSTR_PUBSUB_SERVICE_PORT,
  FipsNostrPubsubClient,
  FipsNostrPubsubEventSource,
  FipsPubsubWireCodec,
  localIndexSource,
  type FipsPubsubClientNode,
} from '../src/index.js';

type FipsPubsubServiceHandler = (context: FipsServiceContext) => Promise<void> | void;

const ALICE = `02${'11'.repeat(32)}`;
const BOB = `03${'22'.repeat(32)}`;
const CHARLIE = `02${'33'.repeat(32)}`;

interface RustWireResponse {
  ok: boolean;
  frame?: string;
  error?: string;
}

class RustWireFixture {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly lines: Interface;
  private readonly iterator: AsyncIterableIterator<string>;
  private stderr = '';

  constructor() {
    this.child = spawn('cargo', [
      'run',
      '--quiet',
      '--package',
      'nostr-pubsub-fips',
      '--example',
      'fips-pubsub-wire-stdio',
    ], {
      cwd: fileURLToPath(new URL('../../../../', import.meta.url)),
      stdio: 'pipe',
    });
    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', (chunk: string) => { this.stderr += chunk; });
    this.lines = createInterface({ input: this.child.stdout });
    this.iterator = this.lines[Symbol.asyncIterator]();
  }

  async roundtrip(frame: Uint8Array): Promise<RustWireResponse> {
    this.child.stdin.write(`${JSON.stringify({ frame: Buffer.from(frame).toString('hex') })}\n`);
    const line = await this.iterator.next();
    if (line.done) throw new Error(`Rust wire fixture exited early: ${this.stderr}`);
    return JSON.parse(line.value) as RustWireResponse;
  }

  async close(): Promise<void> {
    this.child.stdin.end();
    if (this.child.exitCode === null) await once(this.child, 'exit');
    this.lines.close();
    if (this.child.exitCode !== 0) throw new Error(`Rust wire fixture failed: ${this.stderr}`);
  }
}

const rustFixtures: RustWireFixture[] = [];
afterEach(async () => {
  await Promise.all(rustFixtures.splice(0).map((fixture) => fixture.close()));
});

class MemoryFipsNetwork {
  private readonly nodes = new Map<string, MemoryFipsNode>();

  node(peerId: string): MemoryFipsNode {
    const node = new MemoryFipsNode(peerId, this);
    this.nodes.set(peerId, node);
    return node;
  }

  get(peerId: string): MemoryFipsNode | undefined {
    return this.nodes.get(peerId);
  }
}

class MemoryFipsNode implements FipsPubsubClientNode {
  private readonly services = new Map<number, FipsPubsubServiceHandler>();
  private readonly listeners = new Map<string, Set<(event: unknown) => void>>();

  constructor(readonly id: string, private readonly network: MemoryFipsNetwork) {}

  registerService(port: number, handler: FipsPubsubServiceHandler): () => void {
    this.services.set(port, handler);
    return () => {
      if (this.services.get(port) === handler) this.services.delete(port);
    };
  }

  on(event: 'peer' | 'session', listener: (event: unknown) => void): () => void {
    let listeners = this.listeners.get(event);
    if (listeners === undefined) {
      listeners = new Set();
      this.listeners.set(event, listeners);
    }
    listeners.add(listener);
    return () => listeners?.delete(listener);
  }

  async sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void> {
    const target = this.network.get(args.dst);
    if (target === undefined) throw new Error(`unroutable FIPS peer ${args.dst}`);
    queueMicrotask(() => void target.receive({
        src: this.id,
        srcPort: args.srcPort ?? 0,
        dstPort: args.dstPort,
        payload: new Uint8Array(args.payload),
        reply: async (payload, destinationPort) => target.sendDatagram({
          dst: this.id,
          srcPort: args.dstPort,
          dstPort: destinationPort ?? args.srcPort ?? 0,
          payload,
        }),
      }).catch(() => undefined));
  }

  emit(event: 'peer' | 'session', value: unknown): void {
    for (const listener of this.listeners.get(event) ?? []) listener(value);
  }

  private async receive(context: FipsServiceContext): Promise<void> {
    const handler = this.services.get(context.dstPort);
    if (handler === undefined) throw new Error(`no FIPS service on ${context.dstPort}`);
    await handler(context);
  }
}

async function settle(...clients: FipsNostrPubsubClient[]): Promise<void> {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    await Promise.all(clients.map((client) => client.idle()));
  }
}

function chatEvent(createdAt: number, content: string) {
  return finalizeEvent({
    kind: 1060,
    created_at: createdAt,
    tags: [['p', 'b'.repeat(64)]],
    content,
  }, generateSecretKey());
}

describe('FipsNostrPubsubClient', () => {
  it('uses the shared TCP/FIPS record maximum exactly', () => {
    expect(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES).toBe(65_525);
    const codec = new FipsPubsubWireCodec();
    const base = codec.encodeFrame({
      type: 'req',
      subscriptionId: 'boundary',
      filters: [{ search: '' }],
    });
    const exact = codec.encodeFrame({
      type: 'req',
      subscriptionId: 'boundary',
      filters: [{ search: 'x'.repeat(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES - base.length) }],
    });

    expect(exact).toHaveLength(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES);
    expect(codec.decodeFrame(exact).type).toBe('req');
    expect(() => codec.encodeFrame({
      type: 'req',
      subscriptionId: 'boundary',
      filters: [{ search: `${'x'.repeat(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES - base.length)}x` }],
    })).toThrow(/limit is 65525/);
  });

  it('roundtrips the exact boundary and signed messages through a Rust process', async () => {
    const rust = new RustWireFixture();
    rustFixtures.push(rust);
    const codec = new FipsPubsubWireCodec();
    const base = codec.encodeFrame({
      type: 'req',
      subscriptionId: 'rust-boundary',
      filters: [{ search: '' }],
    });
    const exact = codec.encodeFrame({
      type: 'req',
      subscriptionId: 'rust-boundary',
      filters: [{ search: 'x'.repeat(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES - base.length) }],
    });
    const exactResponse = await rust.roundtrip(exact);
    expect(exactResponse.ok).toBe(true);
    expect(codec.decodeFrame(Uint8Array.from(Buffer.from(exactResponse.frame!, 'hex'))).type)
      .toBe('req');

    const event = chatEvent(1_700_000_099, 'Rust process boundary');
    const eventResponse = await rust.roundtrip(codec.encodeFrame({ type: 'event', event }));
    expect(eventResponse.ok).toBe(true);
    const decoded = codec.decodeFrame(Uint8Array.from(Buffer.from(eventResponse.frame!, 'hex')));
    expect(decoded.type).toBe('event');
    if (decoded.type === 'event') expect(decoded.event.id).toBe(event.id);

    const inventory = {
      type: 'inv',
      subscriptionIds: ['rust-history', 'rust-live'],
      eventId: event.id,
      eventKind: event.kind,
      payloadBytes: new TextEncoder().encode(JSON.stringify(event)).byteLength,
      hopLimit: 4,
    } as const;
    const inventoryResponse = await rust.roundtrip(codec.encodeFrame(inventory));
    expect(inventoryResponse.ok).toBe(true);
    expect(codec.decodeFrame(Uint8Array.from(Buffer.from(inventoryResponse.frame!, 'hex'))))
      .toEqual(inventory);

    const want = { type: 'want', eventId: event.id } as const;
    const wantResponse = await rust.roundtrip(codec.encodeFrame(want));
    expect(wantResponse.ok).toBe(true);
    expect(codec.decodeFrame(Uint8Array.from(Buffer.from(wantResponse.frame!, 'hex'))))
      .toEqual(want);

    const oversized = new Uint8Array(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES + 1);
    const oversizedResponse = await rust.roundtrip(oversized);
    expect(oversizedResponse.ok).toBe(false);
    expect(oversizedResponse.error).toMatch(/limit is 65525/);
  }, 60_000);

  it('carries signed REQ EVENT CLOSE traffic over admitted FIPS peers', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: network.node(ALICE),
      peers: () => [BOB],
      allowedKinds: [1060],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: network.node(BOB),
      peers: () => [ALICE],
      allowedKinds: [1060],
    }).start();
    const received = vi.fn();
    const subscription = bob.subscribe([{ kinds: [1060], '#p': ['b'.repeat(64)] }], received);
    await settle(alice, bob);
    expect(alice.peerSubscriptionCount(BOB)).toBe(1);

    const event = chatEvent(1_700_000_000, 'shared carrier');
    await alice.publish(event);
    await alice.publish(event);
    await settle(alice, bob);
    expect(received).toHaveBeenCalledTimes(1);
    expect(received).toHaveBeenCalledWith(expect.objectContaining({ id: event.id }), ALICE);

    subscription.close();
    await settle(alice, bob);
    expect(alice.peerSubscriptionCount(BOB)).toBe(0);
    await alice.publish(chatEvent(1_700_000_001, 'after close'));
    await settle(alice, bob);
    expect(received).toHaveBeenCalledTimes(1);

    await alice.stop();
    await bob.stop();
  });

  it('shares one TCP connection attempt for many simultaneous subscriptions', async () => {
    const network = new MemoryFipsNetwork();
    const errors: Error[] = [];
    const options = { allowedKinds: [1060], limits: { maxActiveSubscriptions: 64, maxSubscriptionsPerPeer: 64 }, onError: (error: Error) => errors.push(error) };
    const alice = new FipsNostrPubsubClient({ ...options, localPeerId: ALICE, node: network.node(ALICE), peers: () => [BOB] }).start();
    const bob = new FipsNostrPubsubClient({ ...options, localPeerId: BOB, node: network.node(BOB), peers: () => [ALICE] }).start();
    const received = new Uint16Array(40);
    try {
      for (let i = 0; i < received.length; i++) bob.subscribe([{ kinds: [1060] }], () => { received[i]++; });
      // Both ends connect concurrently while subscription setup is still in flight.
      alice.subscribe([{ kinds: [1060] }], () => {});
      await settle(alice, bob);
      expect(errors.map((error) => error.message)).toEqual([]);
      expect(alice.peerSubscriptionCount(BOB)).toBe(received.length);
      await alice.publish(chatEvent(1_700_000_020, 'shared transport'));
      await settle(alice, bob);
      expect([...received]).toEqual(Array(40).fill(1));
    } finally { await alice.stop(); await bob.stop(); }
  });

  it('shares live and history interests and replays recent peer events to a later query', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({ localPeerId: ALICE, node: network.node(ALICE), peers: () => [BOB] }).start();
    const bob = new FipsNostrPubsubClient({ localPeerId: BOB, node: network.node(BOB), peers: () => [ALICE] }).start();
    const source = new FipsNostrPubsubEventSource(bob, 30);
    try {
      const interests = Array.from({ length: 40 }, (_, since) => [{ kinds: [1060], since }]);
      const subscriptions = await Promise.all(interests.map((filters) => source.subscribe(filters, () => {})));
      const queries = interests.map((filters) => source.query(filters));
      expect(bob.activeSubscriptionCount()).toBe(10);
      await settle(alice, bob);
      const event = chatEvent(1_700_000_030, 'shared history');
      await alice.publish(event); await settle(alice, bob);
      expect((await Promise.all(queries)).every((report) => report.events[0]?.event.id === event.id)).toBe(true);
      expect(bob.activeSubscriptionCount()).toBe(10);
      expect((await source.query(interests[0]!, { limit: 1 })).events[0]?.event.id).toBe(event.id);
      for (const subscription of subscriptions) subscription.close();
      expect(bob.activeSubscriptionCount()).toBe(0);
    } finally { await alice.stop(); await bob.stop(); }
  });

  it('batches 512 exact peer interests below the carrier cap and cancels without leaking recipients', async () => {
    const network = new MemoryFipsNetwork();
    const limits = { maxActiveSubscriptions: 128, maxSubscriptionsPerPeer: 128, maxFiltersPerSubscription: 32, maxCachedEvents: 1024, maxReplayEvents: 512 };
    const errors: string[] = [];
    const alice = new FipsNostrPubsubClient({ localPeerId: ALICE, node: network.node(ALICE), peers: () => [BOB], limits, onError: error => errors.push(error.message) }).start();
    const bob = new FipsNostrPubsubClient({ localPeerId: BOB, node: network.node(BOB), peers: () => [ALICE], limits, onError: error => errors.push(error.message) }).start();
    const requests = vi.spyOn(bob, 'subscribe');
    const source = new FipsNostrPubsubEventSource(bob, 1000);
    const keys = [generateSecretKey(), generateSecretKey()];
    const counts = new Uint16Array(512);
    const recipient = (index: number) => index.toString(16).padStart(64, '0');
    const filters = Array.from(counts, (_, index) => [{ kinds: [1060], authors: [getPublicKey(keys[index % 2]!)], '#p': [recipient(index)] }]);
    const eventFor = (index: number, content = 'exact recipient') => finalizeEvent({ kind: 1060, created_at: 1700000000, tags: [['p', recipient(index)]], content }, keys[index % 2]!);
    const start = performance.now();
    const cpu = process.cpuUsage();
    try {
      const subscriptions = await Promise.all(filters.map((interest, index) => source.subscribe(interest, () => { counts[index]++; })));
      await settle(alice, bob);
      expect(bob.activeSubscriptionCount()).toBe(16);
      expect(alice.peerSubscriptionCount(BOB)).toBe(16);
      const events = Array.from(counts, (_, index) => eventFor(index));
      for (let index = 0; index < events.length; index++) {
        await alice.publish(events[index]!);
        if (index % 16 === 15) await settle(alice, bob);
      }
      await settle(alice, bob);
      expect([...counts]).toEqual(Array(512).fill(1));
      const crossed = finalizeEvent({ kind: 1060, created_at: 1700000000, tags: [['p', recipient(1)]], content: 'must not widen author/recipient pairs' }, keys[0]!);
      await alice.publish(crossed); await settle(alice, bob);
      expect([...counts]).toEqual(Array(512).fill(1));
      expect((await source.query(filters[0]!, { limit: 1 })).events[0]?.event.id).toBe(events[0]!.id);
      // Closing many subscriptions from separate worker tasks must not reopen each time.
      for (let index = 0; index < 256; index++) {
        subscriptions[index]!.close();
        await new Promise(resolve => setTimeout(resolve, 0));
      }
      await alice.publish(eventFor(0, 'closed'));
      await alice.publish(eventFor(511, 'still live'));
      await settle(alice, bob);
      expect(counts[0]).toBe(1);
      expect(counts[511]).toBe(2);
      expect(requests.mock.calls.length).toBeLessThan(64);
      for (const subscription of subscriptions) subscription.close();
      expect(bob.activeSubscriptionCount()).toBe(0);
      await settle(alice, bob);
      expect(alice.peerSubscriptionCount(BOB)).toBe(0);
      expect(errors).toEqual([]);
      const usage = process.cpuUsage(cpu);
      process.stdout.write(`fips stress ${JSON.stringify({ fipsInterests: 512, events: 512, reqs: requests.mock.calls.length, wallMs: Math.round(performance.now() - start), cpuMs: Math.round((usage.user + usage.system) / 1000) })}\n`);
    } finally { await alice.stop(); await bob.stop(); }
  });

  it('bounds batch frames, reports capacity failures, and releases aborted pending queries', async () => {
    const network = new MemoryFipsNetwork();
    const client = new FipsNostrPubsubClient({ localPeerId: ALICE, node: network.node(ALICE), peers: () => [],
      limits: { maxActiveSubscriptions: 2, maxFiltersPerSubscription: 4, maxFrameBytes: 256 } }).start();
    const source = new FipsNostrPubsubEventSource(client, 25);
    try {
      // Each fits independently, but two cannot share a bounded frame.
      const filters = [{ search: 'a'.repeat(210) }];
      const first = await source.subscribe(filters, () => {});
      const second = await source.subscribe([{ search: 'b'.repeat(210) }], () => {});
      expect(client.activeSubscriptionCount()).toBe(2);
      await expect(source.query([{ kinds: [1] }])).rejects.toThrow(/batch limit/);
      await expect(source.query([{ search: 'c'.repeat(256) }])).rejects.toThrow(/bytes, limit/);
      first.close(); second.close();
      const controller = new AbortController();
      const query = source.query([{ kinds: [1060] }], { signal: controller.signal });
      controller.abort();
      await expect(query).rejects.toMatchObject({ name: 'AbortError' });
      expect(client.activeSubscriptionCount()).toBe(0);
      await client.stop();
      await expect(source.query([{ kinds: [1060] }])).rejects.toThrow(/started/);
    } finally { await client.stop(); }
  });

  it('rejects unilateral peer connections before allocating TCP state and closes cleanly', async () => {
    const network = new MemoryFipsNetwork();
    const bobNode = network.node(BOB);
    const sends = vi.spyOn(bobNode, 'sendDatagram');
    const alice = new FipsNostrPubsubClient({ localPeerId: ALICE, node: network.node(ALICE), peers: () => [BOB] }).start();
    const bob = new FipsNostrPubsubClient({ localPeerId: BOB, node: bobNode, peers: () => [ALICE] }).start();
    const charlie = new FipsNostrPubsubClient({ localPeerId: CHARLIE, node: network.node(CHARLIE), peers: () => [BOB] }).start();
    const admitted = vi.fn(); const stranger = vi.fn();
    try {
      await Promise.all([
        new FipsNostrPubsubEventSource(alice).subscribe([{ kinds: [1060] }], admitted),
        new FipsNostrPubsubEventSource(charlie).subscribe([{ kinds: [1060] }], stranger),
      ]);
      await settle(alice, bob, charlie);
      await bob.publish(chatEvent(1700000000, 'account history'));
      await settle(alice, bob, charlie);
      expect(admitted).toHaveBeenCalledTimes(1);
      expect(stranger).not.toHaveBeenCalled();
      expect(bob.peerSubscriptionCount(CHARLIE)).toBe(0);
      expect(sends.mock.calls.some(([args]) => args.dst === CHARLIE)).toBe(false);
    } finally { await Promise.all([alice.stop(), bob.stop(), charlie.stop()]); }
  });

  it('serves admitted persistent history without prewarming the mesh cache', async () => {
    const network = new MemoryFipsNetwork();
    const stored = chatEvent(1_700_000_010, 'persistent only');
    const query = vi.fn(async () => ({ events: [{ event: stored, source: localIndexSource('retained'), priority: 0 }] }));
    const alice = new FipsNostrPubsubClient({ localPeerId: ALICE, node: network.node(ALICE), peers: () => [BOB], allowedKinds: [1060], retainedEventReader: { query } }).start();
    const bob = new FipsNostrPubsubClient({ localPeerId: BOB, node: network.node(BOB), peers: () => [ALICE], allowedKinds: [1060] }).start();
    const received = vi.fn();
    try {
      bob.subscribe([{ kinds: [1060] }], received);
      await settle(alice, bob);
      expect(query).toHaveBeenCalledWith([{ kinds: [1060] }], expect.objectContaining({ limit: 8, signal: expect.any(AbortSignal) }));
      expect(received.mock.calls.map((args) => args[0].id)).toEqual([stored.id]);
    } finally { await alice.stop(); await bob.stop(); }
  });

  it('replays bounded signed events and drops traffic outside admission policy', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: network.node(ALICE),
      peers: () => [BOB],
      allowedKinds: [1060],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: network.node(BOB),
      peers: () => [ALICE],
      allowedKinds: [1060],
    }).start();
    const charlie = new FipsNostrPubsubClient({
      localPeerId: CHARLIE,
      node: network.node(CHARLIE),
      peers: () => [BOB],
      allowedKinds: [1060],
    }).start();
    const cached = chatEvent(1_700_000_010, 'cached before REQ');
    await alice.publish(cached);
    await settle(alice, bob);

    const received = vi.fn();
    bob.subscribe([{ kinds: [1060] }], received);
    await settle(alice, bob);
    expect(received).toHaveBeenCalledTimes(1);
    expect(received.mock.calls[0]?.[0].id).toBe(cached.id);

    await charlie.publish(chatEvent(1_700_000_011, 'not admitted by bob'));
    await settle(bob, charlie);
    expect(received).toHaveBeenCalledTimes(1);

    const profile = finalizeEvent({
      kind: 0,
      created_at: 1_700_000_012,
      tags: [],
      content: '{}',
    }, generateSecretKey());
    await expect(alice.publish(profile)).rejects.toThrow(/event kind 0 is not admitted/);

    await alice.stop();
    await bob.stop();
    await charlie.stop();
  });

  it('adapts the same historical INV/WANT flow to the generic event router', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: network.node(ALICE),
      peers: () => [BOB],
      allowedKinds: [1060],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: network.node(BOB),
      peers: () => [ALICE],
      allowedKinds: [1060],
    }).start();
    const event = chatEvent(1_700_000_013, 'router history');
    await alice.publish(event);
    await settle(alice, bob);

    const source = new FipsNostrPubsubEventSource(bob, 200);
    const pending = source.query([{ kinds: [1060] }], {
      limit: 1,
      deadline: Date.now() + 1_000,
    });
    await settle(alice, bob);
    const report = await pending;
    expect(report.complete).toBe(true);
    expect(report.events.map(({ event: received }) => received.id)).toEqual([event.id]);
    expect(report.events[0]?.source).toEqual({ id: ALICE, kind: 'fips-endpoint' });

    await alice.stop();
    await bob.stop();
  });

  it('bounds empty peer history by its observation window even with a later caller deadline', async () => {
    const network = new MemoryFipsNetwork();
    const client = new FipsNostrPubsubClient({ localPeerId: ALICE, node: network.node(ALICE), peers: () => [] }).start();
    try {
      const source = new FipsNostrPubsubEventSource(client, 25);
      const start = Date.now();
      const report = await source.query([{ kinds: [1060] }], { deadline: Date.now() + 5000 });
      expect(report).toEqual({ events: [], complete: false });
      expect(Date.now() - start).toBeLessThan(500);
      expect(client.activeSubscriptionCount()).toBe(0);
    } finally { await client.stop(); }
  });

  it('refreshes subscriptions when an admitted standalone link appears', async () => {
    const network = new MemoryFipsNetwork();
    const aliceNode = network.node(ALICE);
    const bobNode = network.node(BOB);
    const alicePeers = new Set<string>();
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: aliceNode,
      peers: () => [...alicePeers],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: bobNode,
      peers: () => [ALICE],
    }).start();
    alice.subscribe([{ kinds: [1060] }], vi.fn());
    await settle(alice, bob);
    expect(bob.peerSubscriptionCount(ALICE)).toBe(0);

    alicePeers.add(BOB);
    aliceNode.emit('peer', { remotePubkey: BOB, state: 'connected' });
    await settle(alice, bob);
    expect(bob.peerSubscriptionCount(ALICE)).toBe(1);

    await alice.stop();
    await bob.stop();
  });

  it('retries a subscription request after the route recovers', async () => {
    const peerListeners = new Set<(event: unknown) => void>();
    const errors: string[] = [];
    const sendDatagram = vi.fn()
      .mockRejectedValueOnce(new Error('temporary route failure'))
      .mockResolvedValue(undefined);
    const node: FipsPubsubClientNode = {
      registerService: () => () => {},
      sendDatagram,
      on: (event, listener) => {
        if (event === 'peer') peerListeners.add(listener);
        return () => peerListeners.delete(listener);
      },
    };
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node,
      peers: () => [BOB],
      onError: (error) => errors.push(error.message),
    }).start();
    alice.subscribe([{ kinds: [1060] }], vi.fn());

    await alice.idle();
    expect(sendDatagram).toHaveBeenCalledTimes(1);
    expect(errors).toEqual([
      'connect TCP/FIPS Nostr pubsub peer: temporary route failure',
    ]);

    for (const listener of peerListeners) {
      listener({ remotePubkey: BOB, state: 'connected' });
    }
    await alice.idle();

    expect(sendDatagram).toHaveBeenCalledTimes(2);
    expect(errors).toHaveLength(1);
    await alice.stop();
  });

  it('reconnects the reliable stream after the admitted FIPS route returns', async () => {
    const network = new MemoryFipsNetwork();
    const aliceNode = network.node(ALICE);
    const bobNode = network.node(BOB);
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: aliceNode,
      peers: () => [BOB],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: bobNode,
      peers: () => [ALICE],
    }).start();
    const received = vi.fn();
    bob.subscribe([{ kinds: [1060] }], received);
    await settle(alice, bob);

    aliceNode.emit('peer', { remotePubkey: BOB, state: 'disconnected' });
    bobNode.emit('peer', { remotePubkey: ALICE, state: 'disconnected' });
    await settle(alice, bob);
    aliceNode.emit('peer', { remotePubkey: BOB, state: 'connected' });
    bobNode.emit('peer', { remotePubkey: ALICE, state: 'connected' });
    await settle(alice, bob);

    const event = chatEvent(1_700_000_018, 'after reliable reconnect');
    await alice.publish(event);
    await settle(alice, bob);
    expect(received).toHaveBeenCalledWith(expect.objectContaining({ id: event.id }), ALICE);
    await alice.stop();
    await bob.stop();
  });

  it('forwards a new signed event across explicit admitted peer links', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: network.node(ALICE),
      peers: () => [BOB],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: network.node(BOB),
      peers: () => [ALICE, CHARLIE],
    }).start();
    const charlie = new FipsNostrPubsubClient({
      localPeerId: CHARLIE,
      node: network.node(CHARLIE),
      peers: () => [BOB],
    }).start();
    const received = vi.fn();
    bob.subscribe([{ kinds: [1060] }], vi.fn());
    charlie.subscribe([{ kinds: [1060] }], received);
    await settle(alice, bob, charlie);

    const event = chatEvent(1_700_000_019, 'one explicit hop');
    await alice.publish(event);
    await settle(alice, bob, charlie);
    expect(received).toHaveBeenCalledOnce();
    expect(received.mock.calls[0]?.[0].id).toBe(event.id);

    await alice.stop();
    await bob.stop();
    await charlie.stop();
  });

  it('caches a publication until a peer opens a matching historical REQ', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      localPeerId: ALICE,
      node: network.node(ALICE),
      peers: () => [BOB],
    }).start();
    const bob = new FipsNostrPubsubClient({
      localPeerId: BOB,
      node: network.node(BOB),
      peers: () => [ALICE],
    }).start();
    const event = chatEvent(1_700_000_020, 'retry me');

    await alice.publish(event);
    const received = vi.fn();
    bob.subscribe([{ kinds: [1060] }], received);
    await settle(alice, bob);
    expect(received).toHaveBeenCalledWith(expect.objectContaining({ id: event.id }), ALICE);

    await alice.stop();
    await bob.stop();
  });
});
