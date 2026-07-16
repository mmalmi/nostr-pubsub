import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createInterface, type Interface } from 'node:readline';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import {
  FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES,
  FIPS_NOSTR_PUBSUB_SERVICE_PORT,
  FipsNostrPubsubClient,
  FipsPubsubWireCodec,
  type FipsPubsubClientNode,
  type FipsPubsubServiceContext,
  type FipsPubsubServiceHandler,
} from '../src/index.js';

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
    await target.receive({
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
    });
  }

  emit(event: 'peer' | 'session', value: unknown): void {
    for (const listener of this.listeners.get(event) ?? []) listener(value);
  }

  private async receive(context: FipsPubsubServiceContext): Promise<void> {
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
  it('uses the native FSP datagram maximum exactly', () => {
    expect(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES).toBe(65_525);
    const codec = new FipsPubsubWireCodec();
    const base = codec.encodeFrame({
      type: 'req',
      subscriptionId: 'boundary',
      filters: [{ search: '' }],
    });
    const exact = codec.encodeFrame({
      type: 'req',
      subscriptionId: 'boundary',
      filters: [{ search: 'x'.repeat(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES - base.length) }],
    });

    expect(exact).toHaveLength(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES);
    expect(codec.decodeFrame(exact).type).toBe('req');
    expect(() => codec.encodeFrame({
      type: 'req',
      subscriptionId: 'boundary',
      filters: [{ search: `${'x'.repeat(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES - base.length)}x` }],
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
      filters: [{ search: 'x'.repeat(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES - base.length) }],
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

    const oversized = new Uint8Array(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES + 1);
    const oversizedResponse = await rust.roundtrip(oversized);
    expect(oversizedResponse.ok).toBe(false);
    expect(oversizedResponse.error).toMatch(/limit is 65525/);
  }, 60_000);

  it('carries signed REQ EVENT CLOSE traffic over admitted FIPS peers', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      node: network.node(ALICE),
      peers: () => [BOB],
      allowedKinds: [1060],
    }).start();
    const bob = new FipsNostrPubsubClient({
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

  it('replays bounded signed events and drops traffic outside admission policy', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      node: network.node(ALICE),
      peers: () => [BOB],
      allowedKinds: [1060],
    }).start();
    const bob = new FipsNostrPubsubClient({
      node: network.node(BOB),
      peers: () => [ALICE],
      allowedKinds: [1060],
    }).start();
    const charlie = new FipsNostrPubsubClient({
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

  it('refreshes subscriptions when an admitted standalone link appears', async () => {
    const network = new MemoryFipsNetwork();
    const aliceNode = network.node(ALICE);
    const bobNode = network.node(BOB);
    const alicePeers = new Set<string>();
    const alice = new FipsNostrPubsubClient({
      node: aliceNode,
      peers: () => [...alicePeers],
    }).start();
    const bob = new FipsNostrPubsubClient({
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
      node,
      peers: () => [BOB],
      onError: (error) => errors.push(error.message),
    }).start();
    alice.subscribe([{ kinds: [1060] }], vi.fn());

    await alice.idle();
    expect(sendDatagram).toHaveBeenCalledTimes(1);
    expect(errors).toEqual(['temporary route failure']);

    for (const listener of peerListeners) {
      listener({ remotePubkey: BOB, state: 'connected' });
    }
    await alice.idle();

    expect(sendDatagram).toHaveBeenCalledTimes(2);
    const retry = sendDatagram.mock.calls[1]?.[0];
    expect(alice.codec.decodeFrame(retry.payload)).toEqual(expect.objectContaining({ type: 'req' }));
    await alice.stop();
  });

  it('retries when the route disconnects while a subscription request is in flight', async () => {
    const peerListeners = new Set<(event: unknown) => void>();
    let resolveFirstSend = () => {};
    const firstSend = new Promise<void>((resolve) => { resolveFirstSend = resolve; });
    const sendDatagram = vi.fn()
      .mockImplementationOnce(() => firstSend)
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
      node,
      peers: () => [BOB],
    }).start();
    alice.subscribe([{ kinds: [1060] }], vi.fn());
    expect(sendDatagram).toHaveBeenCalledTimes(1);

    for (const listener of peerListeners) {
      listener({ remotePubkey: BOB, state: 'disconnected' });
    }
    resolveFirstSend();
    await alice.idle();

    for (const listener of peerListeners) {
      listener({ remotePubkey: BOB, state: 'connected' });
    }
    await alice.idle();

    const messageTypes = sendDatagram.mock.calls.map(([request]) =>
      alice.codec.decodeFrame(request.payload).type,
    );
    expect(messageTypes).toEqual(['req', 'close', 'req']);
    await alice.stop();
  });

  it('forwards a new signed event across explicit admitted peer links', async () => {
    const network = new MemoryFipsNetwork();
    const alice = new FipsNostrPubsubClient({
      node: network.node(ALICE),
      peers: () => [BOB],
    }).start();
    const bob = new FipsNostrPubsubClient({
      node: network.node(BOB),
      peers: () => [ALICE, CHARLIE],
    }).start();
    const charlie = new FipsNostrPubsubClient({
      node: network.node(CHARLIE),
      peers: () => [BOB],
    }).start();
    const received = vi.fn();
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

  it('retries events that were not sent to an admitted peer', async () => {
    const peers = new Set<string>();
    const sendDatagram = vi.fn()
      .mockRejectedValueOnce(new Error('temporary route failure'))
      .mockResolvedValue(undefined);
    const alice = new FipsNostrPubsubClient({
      node: { registerService: () => () => {}, sendDatagram },
      peers: () => [...peers],
    }).start();
    const event = chatEvent(1_700_000_020, 'retry me');

    await expect(alice.publish(event)).rejects.toThrow(/no admitted/);
    expect(sendDatagram).not.toHaveBeenCalled();

    peers.add(BOB);
    await expect(alice.publish(event)).rejects.toThrow(/all FIPS pubsub deliveries failed/);
    await expect(alice.publish(event)).resolves.toBeUndefined();
    expect(sendDatagram).toHaveBeenCalledTimes(2);

    await alice.stop();
  });
});
