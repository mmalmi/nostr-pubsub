import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { AbstractSimplePool } from 'nostr-tools/abstract-pool';
import { finalizeEvent, verifyEvent } from 'nostr-tools/pure';
import { NostrRuntime, MemoryEventStore, SimplePoolNostrRelayTransport, verifyNostrEvent, type NostrEvent, type NostrRuntimeOptions, type QueryEvent } from '../src/index.js';
import { RuntimeRelayFixture, until } from './runtime-relay-fixture.js';

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const stop of cleanup.splice(0).reverse()) await stop(); });
const secret = new Uint8Array(32).fill(7);
function event(content: string, kind = 1, time = 100, tags: string[][] = []): NostrEvent {
  return finalizeEvent({ content, kind, created_at: time, tags }, secret);
}
function runtime(options: NostrRuntimeOptions = {}): NostrRuntime {
  const result = new NostrRuntime({ websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket, historyTimeoutMs: 1500, reconnectDelayMs: 10, ...options });
  cleanup.push(() => result.close()); return result;
}
async function relay(): Promise<RuntimeRelayFixture> {
  const result = new RuntimeRelayFixture(); await result.url(); cleanup.push(() => result.close()); return result;
}

describe('Nostr runtime over real relay sockets', () => {
  it('batches exact OR interests without Cartesian recipient widening', async () => {
    const server = await relay();
    const client = runtime({ relays: [await server.url()], batchWindowMs: 10 });
    const received: string[] = [];
    const sample = event('sample');
    for (let i = 0; i < 100; i++) client.subscribe([{ authors: [sample.pubkey], kinds: [i % 2 ? 1 : 4], '#p': [`recipient-${i}`] }], { onEvent: (event) => received.push(event.id) }, { cache: 'network-only' });
    await until(() => server.requests.length === 4);
    expect(server.requests.flat()).toHaveLength(100);
    for (const filter of server.requests.flat()) {
      expect(filter.authors).toHaveLength(1); expect(filter.kinds).toHaveLength(1); expect(filter['#p']).toHaveLength(1);
    }
    server.emit(event('does not match original pair', 1, 100, [['p', 'recipient-0']]));
    server.emit(event('matches', 4, 100, [['p', 'recipient-0']]));
    await until(() => received.length === 1);
    expect(client.metrics().relaySubscriptions).toBe(4);
  });
  it('waits for actual delayed EOSE and keeps live subscriptions open after a partial timeout', async () => {
    const server = await relay(); server.eoseDelay = 750;
    const client = runtime({ relays: [await server.url()] });
    const start = Date.now();
    const result = await client.query([{ kinds: [1] }], { cache: 'network-only' });
    expect(Date.now() - start).toBeGreaterThanOrEqual(700);
    expect(result.complete).toBe(true);
    const received: string[] = []; const statuses: boolean[] = [];
    client.subscribe([{ kinds: [1] }], { onEvent: (event) => received.push(event.id), onEose: (status) => statuses.push(status.complete) }, { cache: 'network-only', deadline: Date.now() + 30 });
    await until(() => statuses.length === 1);
    expect(statuses).toEqual([false]);
    server.emit(event('after quiet interval'));
    await until(() => received.length === 1);
  });
  it('reports incomplete history to every matching query when durable admission fails after EOSE', async () => {
    const server = await relay(); server.events.push(event('must persist'));
    class FailingStore extends MemoryEventStore {
      override async put(): Promise<void> {
        await new Promise(resolve => setTimeout(resolve, 25));
        throw new Error('Persistent index write failed');
      }
    }
    const client = runtime({ relays: [await server.url()], store: new FailingStore() });
    const results = await Promise.all([
      client.query([{ kinds: [1] }], { cache: 'network-only' }),
      client.query([{ authors: [server.events[0]!.pubkey] }], { cache: 'network-only' }),
    ]);
    for (const result of results) {
      expect(result).toMatchObject({ complete: false, reason: 'unavailable', events: [] });
      expect(result.sources).toContainEqual({ id: 'local-cache', complete: false, error: 'Persistent index write failed' });
    }
  });
  it.each(['cache-first', 'network-only'] as const)('waits for admissions queued behind an older write before completing %s history', async (cache) => {
    const releases: Array<() => void> = [];
    class GatedStore extends MemoryEventStore {
      override async put(value: NostrEvent): Promise<void> {
        await new Promise<void>(resolve => releases.push(resolve));
        await super.put(value);
      }
    }
    let deliver!: (value: QueryEvent) => void;
    let eose!: () => void;
    const completion = new Promise<void>(resolve => { eose = resolve; });
    const source = { id: 'peer',
      subscribe: (_filters: unknown, handler: typeof deliver) => { deliver = handler; return { close() {} }; },
      query: async () => { await completion; return { events: [], complete: true }; },
    };
    const store = new GatedStore();
    const client = runtime({ store, sources: [source] });
    const olderWrite = client.ingest(event('older unrelated write'));
    await until(() => releases.length === 1);
    const expected = verifyNostrEvent(event('friend opinion', 3));
    let resolved = false;
    const history = client.query([{ kinds: [3] }], { cache }).then(result => { resolved = true; return result; });
    try {
      deliver({ event: expected, source: { id: source.id, kind: 'peer' }, priority: 0 });
      eose();
      await new Promise(resolve => setImmediate(resolve));
      releases[0]!();
      await until(() => releases.length === 2);
      await new Promise(resolve => setImmediate(resolve));
      expect(resolved, 'EOSE must wait for the newly queued durable admission').toBe(false);
      const controller = new AbortController();
      const cancelled = client.query([{ kinds: [3] }], { cache, signal: controller.signal });
      controller.abort();
      await expect(cancelled).rejects.toMatchObject({ name: 'AbortError' });
      releases[1]!();
      expect(await history).toMatchObject({ complete: true, events: [expected] });
      expect(await store.query([{ kinds: [3] }])).toEqual([structuredClone(expected)]);
    } finally {
      eose(); releases.forEach(release => release());
      await olderWrite;
    }
  });
  it('reconnects with overlapping time and does not lose another event in the same second', async () => {
    const server = await relay(); const first = event('first'); server.events.push(first);
    const client = runtime({ relays: [await server.url()] });
    const received: string[] = []; let complete = false;
    client.subscribe([{ kinds: [1] }], { onEvent: (event) => received.push(event.id), onEose: () => { complete = true; } }, { cache: 'network-only' });
    await until(() => complete && received.length === 1);
    server.disconnect(); server.events.push(event('same second'));
    await until(() => received.length === 2);
    expect(new Set(received).size).toBe(2);
    expect(server.requests.at(-1)?.[0]?.since).toBe(99);
  });
  it('reports remote rejection honestly, persists outbox intent, and confirms before local echo when requested', async () => {
    const server = await relay(); server.acknowledge = false;
    const store = new MemoryEventStore(); const client = runtime({ relays: [await server.url()], store });
    const seen: string[] = [];
    client.subscribe([{}], { onEvent: (event) => seen.push(event.id) }, { cache: 'cache-only' });
    const rejected = event('requires ack');
    expect(await client.publish(rejected, { requireAck: true })).toMatchObject({ accepted: false, queued: false });
    expect(await store.query([{ ids: [rejected.id] }])).toEqual([]); expect(seen).toEqual([]);
    const queued = event('queue me');
    expect(await client.publish(queued)).toMatchObject({ accepted: false, remoteAccepted: false, queued: true });
    expect((await store.listPending()).map((entry) => entry.event.id)).toEqual([queued.id]);
    expect(seen).toEqual([queued.id]);
    server.acknowledge = true; await client.retryPending();
    expect(await store.listPending()).toEqual([]); expect(seen).toEqual([queued.id]);
    expect(await client.publish(rejected, { requireAck: true })).toMatchObject({ accepted: true, queued: false });
    expect(seen).toEqual([queued.id, rejected.id]);
  });
  it('never treats a connection failure as a positive relay acknowledgment', async () => {
    const server = new RuntimeRelayFixture(); const url = await server.url(); await server.close();
    const client = runtime({ relays: [url], historyTimeoutMs: 100, publishTimeoutMs: 100 });
    expect(await client.publish(event('offline'))).toMatchObject({ accepted: false, queued: true });
  });
  it('does not report an offline relay as accepted through the existing relay adapter', async () => {
    const server = new RuntimeRelayFixture(); const url = await server.url(); await server.close();
    const pool = new AbstractSimplePool({ verifyEvent, websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket });
    const transport = new SimplePoolNostrRelayTransport({ getRelays: () => [url], pool, publishTimeoutMs: 100 });
    try { await expect(transport.publish(verifyNostrEvent(event('offline legacy adapter')))).rejects.toThrow(); }
    finally { pool.destroy(); }
  });
  it('resolves first positive acknowledgment without waiting for a silent relay', async () => {
    const fast = await relay(); const silent = await relay(); silent.acknowledge = 'silent';
    const client = runtime({ relays: [await fast.url(), await silent.url()], publishTimeoutMs: 800 });
    const start = Date.now();
    const result = await client.publish(event('fast'), { requireAck: true });
    expect(result.remoteAccepted).toBe(true); expect(Date.now() - start).toBeLessThan(500);
  });
  it('preserves selected relay evidence during concurrent local admission without leaking other routes', async () => {
    const selected = await relay(); const other = await relay();
    const selectedUrl = await selected.url(); const otherUrl = await other.url();
    let release!: () => void; let started = false;
    const pendingWrite = new Promise<void>(resolve => { release = resolve; });
    class DelayedStore extends MemoryEventStore {
      override async put(value: NostrEvent): Promise<void> { started = true; await pendingWrite; await super.put(value); }
    }
    const client = runtime({ relays: [otherUrl], store: new DelayedStore() });
    const seen: Array<{ id: string; source: string }> = [];
    client.subscribe([{ kinds: [1] }], { onEvent: (event, info) => seen.push({ id: event.id, source: info.source }) },
      { cache: 'network-only', relays: [selectedUrl], sources: [], localEcho: false });
    const otherSubscription = client.subscribe([{ kinds: [1] }], { onEvent: () => {} }, { cache: 'network-only', relays: [otherUrl] });
    await until(() => selected.requests.length > 0 && other.requests.length > 0);
    const signed = event('authorization evidence');
    const optimistic = client.publish(signed, { relays: [], sources: [] });
    await until(() => started);
    selected.emit(signed); other.emit(signed);
    await until(() => client.metrics().receivedEvents >= 2);
    expect(seen).toEqual([]);
    release(); await optimistic;
    await until(() => seen.length === 1);
    expect(seen).toEqual([{ id: signed.id, source: selectedUrl }]);
    const unrelated = event('other route only'); other.emit(unrelated);
    await until(() => client.metrics().receivedEvents >= 3);
    await client.ingest(unrelated, otherUrl);
    expect(seen).toHaveLength(1);
    await client.ingest(event('peer source only'), 'fips');
    expect(seen).toHaveLength(1);
    selected.emit(signed); await new Promise(resolve => setTimeout(resolve, 20));
    expect(seen).toHaveLength(1);
    otherSubscription.close();
  });
  it('scopes private interests and queued retry destinations to explicit relays', async () => {
    const defaultRelay = await relay(); const privateRelay = await relay(); privateRelay.acknowledge = false;
    const store = new MemoryEventStore();
    const client = runtime({ relays: [await defaultRelay.url()], store });
    const target = await privateRelay.url(); const signed = event('private');
    await client.query([{ kinds: [4] }], { relays: [target], cache: 'network-only' });
    expect(defaultRelay.requests).toHaveLength(0);
    await client.publish(signed, { relays: [target] });
    expect((await store.listPending())[0]?.relays).toEqual([target]);
    privateRelay.acknowledge = true; await client.retryPending();
    expect(defaultRelay.events).toEqual([]); expect(privateRelay.events.map((event) => event.id)).toContain(signed.id);
  });
  it('retains replacement winners and authorized deletions across runtime recreation', async () => {
    const store = new MemoryEventStore(); const first = runtime({ store });
    const old = event('old profile', 0, 100); const fresh = event('fresh profile', 0, 101);
    await first.ingest(fresh); expect(await first.ingest(old)).toBe(false);
    const note = event('delete me'); await first.ingest(note);
    await first.ingest(event('', 5, 102, [['e', note.id]]));
    await first.close();
    const second = runtime({ store });
    expect(await second.ingest(note)).toBe(false);
    expect((await second.query([{ kinds: [0] }], { cache: 'cache-only' })).events.map((event) => event.id)).toEqual([fresh.id]);
  });
  it('allows explicit remote replaceable history without caching old versions or bypassing tombstones', async () => {
    const server = await relay();
    const old = event('older playable root', 30078, 100, [['d', 'video']]);
    const latest = event('new unavailable root', 30078, 101, [['d', 'video']]);
    server.events.push(old, latest);
    const store = new MemoryEventStore();
    const client = runtime({ relays: [await server.url()], store });
    await client.ingest(latest);
    const filters = [{ kinds: [30078], authors: [old.pubkey], '#d': ['video'] }];
    expect((await client.query(filters, { cache: 'network-only' })).events.map(e => e.id)).toEqual([latest.id]);
    expect((await client.query(filters, { cache: 'network-only', includeSuperseded: true })).events.map(e => e.id)).toEqual([latest.id, old.id]);
    expect((await store.query(filters)).map(e => e.id)).toEqual([latest.id]);
    await client.ingest(event('', 5, 102, [['e', old.id]]));
    const result = await client.query(filters, { cache: 'network-only', includeSuperseded: true });
    expect(result.complete).toBe(true);
    expect(result.events.map(e => e.id)).toEqual([latest.id]);
    await expect(client.query(filters, { cache: 'cache-only', includeSuperseded: true })).rejects.toThrow('network-only');
  });
  it('authenticates a relay using the application signer', async () => {
    const server = await relay(); server.requireAuth = true;
    const url = await server.url();
    const client = runtime({ relays: [url], signAuthEvent: async (_, template) => finalizeEvent(template, secret) });
    const received: string[] = [];
    client.subscribe([{ kinds: [1] }], { onEvent: (event) => received.push(event.id) }, { cache: 'network-only' });
    expect(await client.publish(event('authenticated'), { requireAck: true })).toMatchObject({ remoteAccepted: true });
    await until(() => received.length === 1);
    expect(server.authEvents[0]?.kind).toBe(22242);
    expect(server.authEvents[0]?.tags).toContainEqual(['relay', url]);
    expect(server.authEvents[0]?.tags).toContainEqual(['challenge', 'runtime-test-challenge']);
  });
  it('reuses signature admission across overlapping relay batches and rejects mutated duplicates', async () => {
    const server = await relay(); let verifications = 0;
    const client = runtime({ relays: [await server.url()], maxFiltersPerBatch: 1,
      verifyEvent: (event) => { verifications++; return verifyEvent(event); } });
    let deliveries = 0; let ready = 0;
    for (let i = 0; i < 20; i++) client.subscribe([{ kinds: [1], since: i }], {
      onEvent: () => { deliveries++; }, onEose: () => { ready++; },
    }, { cache: 'network-only' });
    await until(() => ready === 20);
    const signed = event('shared'); server.emit(signed);
    await until(() => deliveries === 20);
    expect(verifications).toBe(1);
    server.emit({ ...signed, content: 'forged' });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(deliveries).toBe(20);
    expect(verifications).toBeGreaterThan(1);
  });
  it('treats missing and empty d tags as one address without deleting other identifiers', async () => {
    const store = new MemoryEventStore(); const client = runtime({ store });
    await client.ingest(event('missing identifier', 30078, 100));
    const separate = event('separate', 30078, 100, [['d', 'other']]); await client.ingest(separate);
    const replacement = event('empty identifier', 30078, 101, [['d', '']]); await client.ingest(replacement);
    expect((await store.query([{ kinds: [30078] }])).map((event) => event.id).sort()).toEqual([replacement.id, separate.id].sort());
    await client.ingest(event('', 5, 102, [['a', `30078:${replacement.pubkey}:`]]));
    expect((await store.query([{ kinds: [30078] }])).map((event) => event.id)).toEqual([separate.id]);
  });
  it('closes outstanding queries and does not mistake a queued mesh send for remote acceptance', async () => {
    const server = await relay(); server.eoseDelay = 1000;
    const client = runtime({ relays: [await server.url()], sources: [{ id: 'mesh', publishAcceptance: 'queued', publish: async () => ({ accepted: true, priority: 0 }) }] });
    const query = client.query([{ kinds: [1] }]);
    await until(() => server.requests.length === 1);
    expect(await client.publish(event('mesh only'), { relays: [], sources: ['mesh'] })).toMatchObject({ accepted: false, queued: true, sources: [{ id: 'mesh', accepted: false, queued: true }] });
    await client.close(); expect(await query).toMatchObject({ complete: false, reason: 'closed' });
  });
  it('coalesces 500 subscription removals arriving as separate worker tasks', async () => {
    const server = await relay(); const client = runtime({ relays: [await server.url()] });
    let ready = 0;
    const subscriptions = Array.from({ length: 500 }, (_, i) => client.subscribe([{ kinds: [1], '#p': [`remove-${i}`] }], {
      onEvent: () => {}, onEose: () => { ready++; },
    }, { cache: 'network-only' }));
    await until(() => ready === 500);
    expect(server.requests).toHaveLength(16);
    // setImmediate yields a separate task, like main-thread messages to a worker.
    for (const subscription of subscriptions) {
      subscription.close();
      await new Promise((resolve) => setImmediate(resolve));
    }
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(server.requests.length).toBeLessThanOrEqual(20);
    expect(client.metrics().relaySubscriptions).toBe(0);
  });
  it('stress: 1200 interests share bounded batches and deliver exactly once per matching listener', async () => {
    const server = await relay();
    const client = runtime({ relays: [await server.url()], maxSubscriptions: 1500, batchWindowMs: 10, historyTimeoutMs: 10000 });
    const count = 1200;
    const signed = Array.from({ length: count }, (_, i) => event(`stress-${i}`, 1, 100 + i, [['p', `stress-${i}`]]));
    const deliveries = new Uint16Array(count);
    let eose = 0;
    const start = performance.now(); const cpu = process.cpuUsage();
    for (let i = 0; i < count; i++) client.subscribe([{ kinds: [1], '#p': [`stress-${i}`] }], {
      onEvent: (event) => { if (event.id !== signed[i]!.id) throw new Error('Incorrect recipient delivery'); deliveries[i]++; },
      onEose: () => { eose++; },
    }, { cache: 'network-only' });
    await until(() => eose === count, 10000);
    for (const event of signed) server.emit(event);
    await until(() => deliveries.every((count) => count === 1), 10000);
    expect(server.requests.length).toBe(Math.ceil(count / 32));
    expect(server.requests.every((filters) => filters.length <= 32)).toBe(true);
    const elapsed = process.cpuUsage(cpu);
    process.stdout.write('runtime stress ' + JSON.stringify({ interests: count, events: count, relayRequests: server.requests.length, wallMs: Math.round(performance.now() - start), cpuMs: Math.round((elapsed.user + elapsed.system) / 1000), deliveries: deliveries.reduce((a, b) => a + b, 0) }) + '\n');
  }, 20000);

});
