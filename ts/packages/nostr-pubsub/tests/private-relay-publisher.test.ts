import { afterEach, describe, expect, it, vi } from 'vitest';
import { WebSocket } from 'ws';
import { finalizeEvent, generateSecretKey, getPublicKey, verifyEvent } from 'nostr-tools/pure';
import {
  createAnonymousRelayDiscovery, createPrivateRelayPublisher, isPrivateRelayEvent,
  isRelayDiscoveryFilter, MemoryEventStore, NostrRuntime, publicationLaneStore,
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

describe('anonymous public-key discovery boundary', () => {
  it('uses separate fresh AUTH connections per author and reverse-device lookup with no pending sends', async () => {
    const server = await relay(); const store = new MemoryEventStore();
    await store.putPending({ event: event(), attempts: 0, updatedAt: 0 });
    const closeStore = vi.fn(); Object.assign(store, { close: closeStore });
    const discovery = createAnonymousRelayDiscovery({ store, relays: [await server.url()], websocketImplementation: ws, historyTimeoutMs: 1000 });
    cleanup.push(() => discovery.close());
    const filters = [{ kinds: [30078], authors: [ownerKey], '#l': ['double-ratchet/invites'] },
      { kinds: [37368], authors: [recipient] }, { kinds: [37368], '#p': [recipient] }];
    const results = await Promise.all(filters.map(filter => discovery.query([filter], { cache: 'network-only' })));
    expect(results.every(result => result.complete)).toBe(true);
    expect(server.authEvents).toHaveLength(3);
    expect(new Set(server.authEvents.map(value => value.pubkey)).size).toBe(3);
    expect(server.authEvents.every(value => verifyEvent(value) && ![ownerKey, recipient].includes(value.pubkey))).toBe(true);
    expect(frames(server)).toHaveLength(3);
    for (const connection of frames(server)) {
      expect(connection.some(frame => frame[0] === 'EVENT')).toBe(false);
      const requested = connection.filter(frame => frame[0] === 'REQ').flatMap(frame => frame.slice(2));
      expect(new Set(requested.map(filter => JSON.stringify(filter))).size).toBe(1);
    }
    await until(() => server.clients.size === 0);
    expect(await store.listPending()).toHaveLength(1);
    await discovery.close(); expect(closeStore).not.toHaveBeenCalled();
    expect('publish' in discovery).toBe(false); expect('addSource' in discovery).toBe(false);
  });

  it('rejects unscoped, cross-author, cross-kind and combined identity lookups without opening a socket', async () => {
    const server = await relay();
    const discovery = createAnonymousRelayDiscovery({ relays: [await server.url()], websocketImplementation: ws });
    cleanup.push(() => discovery.close());
    const invalid = [[], [{ kinds: [37368] }], [{ kinds: [37368], authors: [ownerKey, recipient] }],
      [{ kinds: [37368], authors: [ownerKey], '#p': [recipient] }], [{ kinds: [30078], '#p': [recipient] }],
      [{ kinds: [1060], authors: [recipient] }], [{ kinds: [37368, 1], authors: [ownerKey] }],
      [{ kinds: [37368], authors: [ownerKey] }, { kinds: [37368], authors: [recipient] }]];
    for (const filters of invalid) await expect(discovery.query(filters)).rejects.toThrow();
    expect(server.frames.size).toBe(0);
    expect(isRelayDiscoveryFilter({ kinds: [37368] })).toBe(true);
    expect(isRelayDiscoveryFilter({ kinds: [1060] })).toBe(false);
  });

  it('releases bounded scope capacity on abort and gives the next lookup a new authentication key', async () => {
    const server = await relay();
    const discovery = createAnonymousRelayDiscovery({ relays: [await server.url()], websocketImplementation: ws, maxScopes: 1 });
    cleanup.push(() => discovery.close());
    const signal = new AbortController();
    const first = discovery.subscribe([{ kinds: [37368], authors: [ownerKey] }], { onEvent() {} }, { signal: signal.signal, cache: 'network-only' });
    await until(() => server.authEvents.length === 1);
    expect(() => discovery.subscribe([{ kinds: [37368], authors: [recipient] }], { onEvent() {} })).toThrow('capacity');
    signal.abort(); first.close();
    const result = await discovery.query([{ kinds: [37368], authors: [recipient] }], { cache: 'network-only' });
    expect(result.complete).toBe(true);
    expect(server.authEvents).toHaveLength(2); expect(server.authEvents[0]!.pubkey).not.toBe(server.authEvents[1]!.pubkey);
  });
});
