import { afterEach, describe, expect, it, vi } from 'vitest';
import { WebSocket } from 'ws';
import { finalizeEvent, generateSecretKey, getPublicKey, verifyEvent } from 'nostr-tools/pure';
import * as nip44 from 'nostr-tools/nip44';
import {
  createPrivateRelayPublisher, isPrivateRelayEvent,
  isRelayDiscoveryFilter, splitRelayDiscoveryFilters, MemoryEventStore, NostrRuntime, publicationLaneStore,
  type NostrEvent,
} from '../src/index.js';
import { RuntimeRelayFixture, until } from './runtime-relay-fixture.js';

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });
const owner = generateSecretKey();
const ownerKey = getPublicKey(owner);
const recipient = getPublicKey(generateSecretKey());
const ws = WebSocket as unknown as typeof globalThis.WebSocket;
function event(kind = 1060, tags = [['p', recipient], ['v', '2']]): NostrEvent {
  return finalizeEvent({ kind, tags, created_at: Math.floor(Date.now() / 1000), content: 'ciphertext' }, generateSecretKey());
}
async function relay(): Promise<RuntimeRelayFixture> {
  const server = new RuntimeRelayFixture(); await server.url(); server.requireAuth = true;
  cleanup.push(() => server.close()); return server;
}
async function publisher(server: RuntimeRelayFixture, store = new MemoryEventStore()) {
  const result = createPrivateRelayPublisher({ store, relays: [await server.url()], websocketImplementation: ws, publishTimeoutMs: 1000 });
  cleanup.push(() => result.close()); return result;
}
function frames(server: RuntimeRelayFixture) { return [...server.frames.values()]; }
function auth(connection: unknown[][]): NostrEvent { return connection.find(frame => frame[0] === 'AUTH')?.[1] as NostrEvent; }

describe('private publication connection boundary', () => {
  it.each(['peer-first', 'relay-first'] as const)('keeps peer and relay receipts independent for one envelope (%s)', async (order) => {
    const server = await relay(); server.acknowledge = 'silent';
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const publish = vi.fn(async () => { await blocked; return { accepted: true, priority: 0 }; });
    const sender = createPrivateRelayPublisher({ websocketImplementation: ws, publishTimeoutMs: 1000,
      sources: [{ id: 'fips', publishAcceptance: 'queued', publish }] });
    cleanup.push(() => sender.close());
    const envelope = event(), url = await server.url();
    const peerOptions = { relays: [], sources: ['fips'], queue: false };
    const relayOptions = { relays: [url], requireAck: true };
    let peer: ReturnType<typeof sender.publish> | undefined;
    let direct: ReturnType<typeof sender.publish> | undefined;
    const received = () => frames(server).some(connection => connection.filter(frame =>
      frame[0] === 'EVENT' && (frame[1] as NostrEvent).id === envelope.id).length === 2);
    const acknowledge = () => {
      for (const [socket, connection] of server.frames) if (socket.readyState === WebSocket.OPEN && connection.some(frame =>
        frame[0] === 'EVENT' && (frame[1] as NostrEvent).id === envelope.id)) {
        socket.send(JSON.stringify(['OK', envelope.id, true, 'stored']));
      }
    };
    try {
      if (order === 'peer-first') {
        peer = sender.publish(envelope, peerOptions); await until(() => publish.mock.calls.length === 1);
        direct = sender.publish(envelope, relayOptions);
      } else {
        direct = sender.publish(envelope, relayOptions); await until(received);
        peer = sender.publish(envelope, peerOptions);
      }
      expect(direct).not.toBe(peer);
      expect(sender.publish(envelope, { ...peerOptions })).toBe(peer);
      expect(sender.publish(envelope, { ...relayOptions })).toBe(direct);
      await until(() => received() && publish.mock.calls.length === 1);
      let peerSettled = false; void peer.then(() => { peerSettled = true; });
      acknowledge();
      expect(await direct).toMatchObject({ accepted: true, remoteAccepted: true, queued: false,
        sources: [{ id: url, accepted: true }] });
      expect(peerSettled).toBe(false);
      expect(server.authEvents).toHaveLength(1);
      expect(server.authEvents[0]!.pubkey).not.toBe(ownerKey);
      expect(frames(server).every(connection => connection.every(frame => ['AUTH', 'EVENT'].includes(frame[0] as string)))).toBe(true);
    } finally {
      release(); acknowledge(); await Promise.allSettled([peer, direct]);
    }
    expect(await peer).toMatchObject({ accepted: false, remoteAccepted: false, queued: false,
      sources: [{ id: 'fips', accepted: false, queued: true }] });
  });

  it.each([
    { name: 'acknowledgment policy', change: { requireAck: true } },
    { name: 'queue policy', change: { queue: true } },
    { name: 'local echo policy', change: { localEcho: true } },
    { name: 'source destination', change: { sources: ['provider'] } },
  ])('does not coalesce different $name options and retires settled attempts', async ({ change }) => {
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const publish = vi.fn(async () => { await blocked; return { accepted: true, priority: 0 }; });
    const sender = createPrivateRelayPublisher({ publishTimeoutMs: 1000, sources: [
      { id: 'fips', publishAcceptance: 'queued', publish }, { id: 'provider', publishAcceptance: 'queued', publish },
    ] });
    cleanup.push(() => sender.close());
    const envelope = event(), options = { relays: [], sources: ['fips'], queue: false, localEcho: false };
    const first = sender.publish(envelope, options);
    let second: ReturnType<typeof sender.publish> | undefined;
    try {
      await until(() => publish.mock.calls.length === 1);
      second = sender.publish(envelope, { ...options, ...change });
      expect(second).not.toBe(first);
      expect(sender.publish(envelope, { ...options })).toBe(first);
      await until(() => publish.mock.calls.length === 2);
    } finally { release(); await Promise.allSettled([first, second]); }
    expect(await first).toMatchObject({ accepted: false, remoteAccepted: false, queued: false });
    expect(await second).toMatchObject({ accepted: false, remoteAccepted: false, queued: change.queue === true });
    const retry = sender.publish(envelope, options);
    expect(retry).not.toBe(first);
    expect(await retry).toMatchObject({ accepted: false, remoteAccepted: false, queued: false });
    expect(publish).toHaveBeenCalledTimes(3);
  });

  it('opens independent authenticated connections for different explicit relay scopes', async () => {
    const a = await relay(), b = await relay(); a.acknowledge = b.acknowledge = 'silent';
    const sender = createPrivateRelayPublisher({ websocketImplementation: ws, publishTimeoutMs: 1000 });
    cleanup.push(() => sender.close());
    const envelope = event(), urls = [await a.url(), await b.url()];
    const received = (server: RuntimeRelayFixture) => frames(server).some(connection => connection.filter(frame =>
      frame[0] === 'EVENT' && (frame[1] as NostrEvent).id === envelope.id).length === 2);
    const acknowledge = () => { for (const server of [a, b]) for (const [socket, connection] of server.frames) {
      if (socket.readyState === WebSocket.OPEN && connection.some(frame => frame[0] === 'EVENT' && (frame[1] as NostrEvent).id === envelope.id)) {
        socket.send(JSON.stringify(['OK', envelope.id, true, 'stored']));
      }
    } };
    const first = sender.publish(envelope, { relays: [urls[0]!], requireAck: true });
    let second: ReturnType<typeof sender.publish> | undefined;
    try {
      await until(() => received(a)); second = sender.publish(envelope, { relays: [urls[1]!], requireAck: true });
      expect(second).not.toBe(first); await until(() => received(b)); acknowledge();
      expect(await first).toMatchObject({ remoteAccepted: true, sources: [{ id: urls[0], accepted: true }] });
      expect(await second).toMatchObject({ remoteAccepted: true, sources: [{ id: urls[1], accepted: true }] });
      expect(a.authEvents).toHaveLength(1); expect(b.authEvents).toHaveLength(1);
      expect(a.authEvents[0]!.pubkey).not.toBe(b.authEvents[0]!.pubkey);
    } finally { acknowledge(); await Promise.allSettled([first, second]); }
  });

  it('preserves an encrypted invite owner proof without exposing its identity or accepting plaintext proof tags', async () => {
    const server = await relay(); const sender = await publisher(server);
    const envelopeSecret = generateSecretKey();
    const proof = finalizeEvent({kind: 37368, created_at: Math.floor(Date.now() / 1000),
      tags: [['p', getPublicKey(generateSecretKey())]], content: ''}, owner);
    const encrypted = nip44.encrypt(JSON.stringify(proof), nip44.getConversationKey(envelopeSecret, recipient));
    const envelope = finalizeEvent({kind: 1059, created_at: proof.created_at, content: 'encrypted handshake',
      tags: [['p', recipient], ['v', '2'], ['owner-proof', encrypted]]}, envelopeSecret);
    expect((await sender.publish(envelope, {requireAck: true})).accepted).toBe(true);
    expect(server.events[0]?.tags).toContainEqual(['owner-proof', encrypted]);
    expect(JSON.stringify(frames(server))).not.toContain(ownerKey);
    for (const [kind, tags] of [
      [1060, [['p', recipient], ['v', '2'], ['owner-proof', encrypted]]],
      [1059, [['p', recipient], ['owner-proof', JSON.stringify(proof)]]],
      [1059, [['p', recipient], ['owner-proof', encrypted], ['owner-proof', encrypted]]],
    ] as Array<[number, string[][]]>) await expect(sender.publish(event(kind, tags))).rejects.toThrow('recipient-specific');
  });

  it('keeps invite responses, ratchet messages and wakes off account receive/AUTH sockets', async () => {
    const server = await relay();
    const receiver = new NostrRuntime({ relays: [await server.url()], websocketImplementation: ws,
      store: publicationLaneStore(new MemoryEventStore(), 'public'),
      signAuthEvent: async (_relay, template) => finalizeEvent(template, owner) });
    cleanup.push(() => receiver.close());
    receiver.subscribe([{ kinds: [1059], '#p': [ownerKey] }], { onEvent() {} }, { cache: 'network-only' });
    const sender = await publisher(server);
    const envelopes = [event(1059, [['p', recipient]]), event(), event(1060, [['p', ownerKey], ['v', '2']]), event(21111, [['p', recipient]])];
    for (const envelope of envelopes) expect((await sender.publish(envelope, { requireAck: true })).accepted).toBe(true);
    await until(() => server.requests.length > 0 && server.authEvents.length === envelopes.length + 1);
    expect(server.authEvents.every(value => verifyEvent(value))).toBe(true);
    const receiveConnection = frames(server).find(connection => auth(connection)?.pubkey === ownerKey)!;
    const sendConnections = frames(server).filter(connection => connection.some(frame => frame[0] === 'EVENT'));
    expect(sendConnections).toHaveLength(envelopes.length);
    expect(new Set(sendConnections.map(connection => auth(connection).pubkey)).size).toBe(envelopes.length);
    for (const connection of sendConnections) {
      expect(connection).not.toBe(receiveConnection);
      expect(auth(connection).pubkey).not.toBe(ownerKey);
      expect(envelopes.map(value => value.pubkey)).not.toContain(auth(connection).pubkey);
      expect(connection.every(frame => frame[0] === 'AUTH' || frame[0] === 'EVENT')).toBe(true);
      // AUTH-required retries may repeat exactly one envelope on this socket.
      expect(new Set(connection.filter(frame => frame[0] === 'EVENT').map(frame => (frame[1] as NostrEvent).id)).size).toBe(1);
    }
    expect(receiveConnection.some(frame => frame[0] === 'EVENT')).toBe(false);
    expect(sendConnections.flatMap(connection => connection.filter(frame => frame[0] === 'EVENT').map(frame => (frame[1] as NostrEvent).id)))
      .toEqual(expect.arrayContaining(envelopes.map(value => value.id)));
  });

  it('uses fresh authentication after a queued send survives publisher restart', async () => {
    const server = await relay(); server.acknowledge = false;
    const store = new MemoryEventStore(); const first = await publisher(server, store);
    const envelope = event();
    expect((await first.publish(envelope)).queued).toBe(true);
    expect((await store.listPending())[0]?.event).toEqual(structuredClone(envelope));
    await first.close(); server.acknowledge = true;
    const second = await publisher(server, store); await second.retryPending();
    await until(() => server.authEvents.length === 2);
    expect(auth(frames(server)[0]!).pubkey).not.toBe(auth(frames(server)[1]!).pubkey);
    expect(server.events).toContainEqual(structuredClone(envelope));
    expect(await store.listPending()).toEqual([]);
    expect(frames(server).every(connection => connection.every(frame => ['AUTH', 'EVENT'].includes(frame[0] as string)))).toBe(true);
  });

  it('uses a new connection and AUTH for retries by the same live publisher', async () => {
    const server = await relay(); server.acknowledge = false;
    const store = new MemoryEventStore(); const sender = await publisher(server, store);
    const envelope = event(); await sender.publish(envelope);
    await sender.retryPending();
    expect((await store.listPending())[0]?.attempts).toBe(2);
    expect(server.authEvents).toHaveLength(2);
    expect(server.authEvents[0]!.pubkey).not.toBe(server.authEvents[1]!.pubkey);
    server.acknowledge = true; await sender.retryPending();
    expect(server.authEvents).toHaveLength(3);
    expect(new Set(server.authEvents.map(value => value.pubkey)).size).toBe(3);
    expect(await store.listPending()).toEqual([]);
    for (const connection of frames(server)) {
      expect(connection.every(frame => ['AUTH', 'EVENT'].includes(frame[0] as string))).toBe(true);
      for (const frame of connection.filter(frame => frame[0] === 'EVENT')) expect(frame[1]).toEqual(structuredClone(envelope));
    }
  });

  it('rejects public or identity-tagged traffic before storage and strips source read capabilities', async () => {
    const store = new MemoryEventStore(); const query = vi.fn(); const subscribe = vi.fn(); const publish = vi.fn(async () => ({ accepted: true, priority: 0 }));
    const identitySigner = vi.fn();
    const source = { id: 'fips', query, subscribe, publish };
    const sender = createPrivateRelayPublisher({ store, sources: [source], signAuthEvent: identitySigner } as Parameters<typeof createPrivateRelayPublisher>[0]);
    cleanup.push(() => sender.close());
    for (const invalid of [event(1), event(30078), event(37368), event(1060, []),
      event(1060, [['p', recipient], ['p', ownerKey]]), event(1060, [['p', recipient], ['e', ownerKey]]),
      event(1060, [['p', recipient]]), event(1060, [['p', recipient, ownerKey], ['v', '2']])]) {
      await expect(sender.publish(invalid)).rejects.toThrow('recipient-specific');
    }
    expect(await store.listPending()).toEqual([]); expect(await store.query([{}])).toEqual([]);
    expect(publish).not.toHaveBeenCalled();
    expect((await sender.publish(event(), { requireAck: true })).accepted).toBe(true);
    expect(publish).toHaveBeenCalledOnce(); expect(query).not.toHaveBeenCalled(); expect(subscribe).not.toHaveBeenCalled();
    expect(identitySigner).not.toHaveBeenCalled();
    expect(Object.keys(sender)).not.toEqual(expect.arrayContaining(['query', 'subscribe']));
    expect('store' in sender).toBe(false); expect('ingest' in sender).toBe(false);
    expect(isPrivateRelayEvent(event(1060, []))).toBe(true);
  });

  it('rejects invalid signatures before durable intent and retires expired queued events', async () => {
    const store = new MemoryEventStore(); const sender = createPrivateRelayPublisher({ store });
    cleanup.push(() => sender.close());
    const invalid = { ...event(), content: 'modified ciphertext' };
    await expect(sender.publish(invalid)).rejects.toThrow();
    expect(await store.listPending()).toEqual([]);
    const expired = event(1060, [['p', recipient], ['v', '2'], ['expiration', '1']]);
    expect((await sender.publish(expired)).queued).toBe(false);
    expect(await store.listPending()).toEqual([]);
  });

  it('quarantines old private and group records before receiver retries without deleting them', async () => {
    const server = await relay(); const store = new MemoryEventStore();
    const privateEvent = event(); const bareGroup = event(1060, [['v', '2']]);
    const roster = event(37368, [['type', 'group_roster'], ['member', recipient]]);
    const publicEvent = event(1, []);
    for (const value of [privateEvent, bareGroup, roster, publicEvent]) await store.putPending({ event: value, attempts: 0, updatedAt: 0 });
    const receiver = new NostrRuntime({ store: publicationLaneStore(store, 'public'), relays: [await server.url()], websocketImplementation: ws,
      signAuthEvent: async (_relay, template) => finalizeEvent(template, owner) });
    cleanup.push(() => receiver.close()); await receiver.retryPending();
    expect(server.events.map(value => value.id)).toEqual([publicEvent.id]);
    expect((await store.listPending()).map(row => row.event.id).sort()).toEqual([privateEvent.id, bareGroup.id, roster.id].sort());
    const privateStore = new MemoryEventStore();
    for (const value of [privateEvent, bareGroup, roster, publicEvent]) await privateStore.putPending({ event: value, attempts: 0, updatedAt: 0 });
    const sender = await publisher(server, privateStore); await sender.retryPending();
    expect(server.events.map(value => value.id)).toEqual([publicEvent.id, privateEvent.id]);
    expect((await privateStore.listPending()).map(row => row.event.id).sort()).toEqual([bareGroup.id, roster.id, publicEvent.id].sort());
  });
});

describe('public lookup filter scopes', () => {
  it('splits profile/relay-list author batches and mixed own settings into single-key scopes', () => {
    const split = splitRelayDiscoveryFilters([
      { kinds: [0, 10013], authors: [ownerKey] },
      { kinds: [0, 1059, 10002, 10050, 37195], authors: [recipient, 'a'.repeat(64)] },
      { kinds: [1060], '#p': [ownerKey] },
      { kinds: [0], limit: 20 },
    ]);
    expect(split.receiver).toEqual([
      { kinds: [10013], authors: [ownerKey] }, { kinds: [1060], '#p': [ownerKey] }, { kinds: [0], limit: 20 },
    ]);
    expect(split.discovery).toHaveLength(3);
    for (const scope of split.discovery) expect(new Set(scope.flatMap(filter => filter.authors)).size).toBe(1);
  });

  it('rejects unscoped, private-roster and combined identity lookups', () => {
    expect(() => splitRelayDiscoveryFilters([{ kinds: [37368] }])).toThrow();
    expect(() => splitRelayDiscoveryFilters([{ kinds: [37368], authors: [ownerKey], '#type': ['group_roster'] }])).toThrow();
    expect(() => splitRelayDiscoveryFilters([{ kinds: [0], authors: [ownerKey], '#p': [recipient] }])).toThrow();
    expect(isRelayDiscoveryFilter({ kinds: [37368] })).toBe(true);
    expect(isRelayDiscoveryFilter({ kinds: [1060] })).toBe(false);
  });
});
