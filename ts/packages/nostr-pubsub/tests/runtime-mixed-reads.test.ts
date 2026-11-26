import { expect, it, vi } from 'vitest';
import { finalizeEvent } from 'nostr-tools/pure';
import { createNostrRuntime, localIndexSource, MemoryEventStore, verifyNostrEvent } from '../src/index.js';
import type { NostrEvent, NostrFilter, RuntimeSource } from '../src/index.js';

const relay = 'wss://relay.example/';
const secret = new Uint8Array(32).fill(9);
function note(time: number, tags: string[][] = []) {
  return verifyNostrEvent(finalizeEvent({ kind: 1, created_at: time, content: String(time), tags }, secret));
}
const source = localIndexSource('hashtree');

it.each([false, true])('merges index and shared relay history with explicit scope %s', async (scoped) => {
  const indexed = note(1), shared = note(2), remote = note(3);
  const filters = [{ kinds: [1], limit: 3 }];
  const relayEose = Promise.withResolvers<void>();
  const query = vi.fn(async (_filters: NostrFilter[]) => {
    if (scoped) await relayEose.promise;
    return { events: [indexed, shared].map(event => ({ event, source, priority: 0 })), complete: true };
  });
  const requests: unknown[] = [];
  class RelaySocket extends EventTarget {
    onopen?: () => void;
    onmessage?: (message: MessageEvent) => void;
    constructor() { super(); queueMicrotask(() => this.onopen?.()); }
    send(raw: string) {
      const [type, id, ...requested] = JSON.parse(raw);
      if (type !== 'REQ') return;
      requests.push(requested);
      for (const frame of [['EVENT', id, shared], ['EVENT', id, remote], ['EOSE', id]]) {
        this.onmessage?.(new MessageEvent('message', { data: JSON.stringify(frame) }));
      }
    }
    close() {}
  }
  const gate = Promise.withResolvers<void>();
  const store = new MemoryEventStore();
  const put = store.put.bind(store);
  vi.spyOn(store, 'put').mockImplementation(async event => { await gate.promise; await put(event); });
  const runtime = createNostrRuntime({
    relays: [relay], sources: [{ id: source.id, durable: true, query }], store,
    websocketImplementation: RelaySocket as unknown as typeof WebSocket,
  });
  try {
    const destinations = scoped ? { relays: [relay], sources: [source.id] } : {};
    // This subscription owns the first (blocked) admission of each relay event.
    runtime.subscribe(filters, {
      onEvent() {},
      onSource: status => { if (status.id === relay && status.complete) relayEose.resolve(); },
    }, { ...destinations, sources: [], cache: 'network-only' });
    const result = await runtime.query(filters, { ...destinations, persist: 'background', deadline: Date.now() + 2000 });
    expect(requests).toEqual([filters]);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toEqual(filters);
    expect(result.events.map(event => event.id)).toEqual([remote.id, shared.id, indexed.id]);
    expect(result.complete).toBe(true);
    expect(result.sources.map(status => status.id).sort()).toEqual([source.id, relay].sort());
    expect(await store.query(filters)).toEqual([]);
  } finally { gate.resolve(); await runtime.close(); }
  expect((await store.query(filters)).map(event => event.id)).toEqual([remote.id, shared.id]);
});

it('checks filter support when adding an index to existing subscriptions', async () => {
  const query = vi.fn(async (_filters: NostrFilter[]) => ({ events: [], complete: true }));
  const runtime = createNostrRuntime();
  try {
    runtime.subscribe([{ search: 'unsupported' }], { onEvent() {} });
    runtime.subscribe([{ kinds: [1] }], { onEvent() {} });
    runtime.addSource({ id: source.id, supports: filters => filters.every(filter => !filter.search), query });
    expect(query.mock.calls.map(call => call[0])).toEqual([[{ kinds: [1] }]]);
  } finally { await runtime.close(); }
});

it('runs an explicitly index-only read independently of relay request limits', async () => {
  const indexed = note(11);
  const runtime = createNostrRuntime({
    maxFiltersPerBatch: 1, relays: [relay],
    sources: [{ id: source.id, durable: true, query: async () => ({
      events: [{ event: indexed, source, priority: 0 }],
    }) }],
  });
  try {
    const result = await runtime.query([{ kinds: [0] }, { kinds: [1] }], { relays: [], sources: [source.id] });
    expect(result).toMatchObject({ events: [indexed], complete: true, sources: [{ id: source.id }] });
  } finally { await runtime.close(); }
});

it('releases an empty background query even when no sources support the filter', async () => {
  const runtime = createNostrRuntime({ maxSubscriptions: 1 });
  try {
    const read = () => runtime.query([{ kinds: [1] }], { cache: 'network-only', persist: 'background' });
    expect((await read()).complete).toBe(false);
    expect(runtime.metrics().subscriptions).toBe(0);
    expect((await read()).reason).toBe('unavailable');
  } finally { await runtime.close(); }
});

it('settles exact IDs independently of the live deduplication window', async () => {
  const events = [note(4), note(5)];
  const runtime = createNostrRuntime({ maxSeenEventsPerSubscription: 1, sources: [
    { id: source.id, durable: true, query: async () => ({ events: events.map(event => ({ event, source, priority: 0 })) }) },
    { id: 'incomplete', query: async () => ({ events: [], complete: false }) },
  ] });
  try {
    const result = await runtime.query([{ ids: events.map(event => event.id) }], { isolated: true, settle: { ids: true } });
    expect(result.events).toHaveLength(2);
    expect(result.complete).toBe(true);
  } finally { await runtime.close(); }
});

it.each([true, false])('ignores expired events before exact-ID settlement (durable %s)', async durable => {
  const event = note(6, [['expiration', '1']]);
  const index: RuntimeSource = { id: source.id, durable, query: async () => ({
    events: [{ event, source, priority: 0 }], complete: false,
  }) };
  const runtime = createNostrRuntime({ sources: [index] });
  try {
    const received: NostrEvent[] = [];
    const complete = await new Promise<boolean>(resolve => runtime.subscribe([{ ids: [event.id] }], {
      onEvent: value => received.push(value), onEose: status => resolve(status.complete),
    }, { persist: 'background', settle: { ids: true } }));
    expect(received).toEqual([]);
    expect(complete).toBe(false);
  } finally { await runtime.close(); }
});

it('keeps valid index rows and reports incomplete history when another row fails verification', async () => {
  const good = note(7);
  const bad = { ...good, content: 'corrupt row' };
  const index: RuntimeSource = { id: source.id, durable: true, query: async () => ({
    events: [bad, good].map(event => ({ event, source, priority: 0 })), complete: true,
  }) };
  const runtime = createNostrRuntime({ sources: [index] });
  try {
    const result = await runtime.query([{ kinds: [1] }]);
    expect(result.events).toEqual([good]);
    expect(result).toMatchObject({ complete: false, sources: [{ id: source.id, complete: false, error: expect.any(String) }] });
  } finally { await runtime.close(); }
});

it('waits for cache replay when all selected sources finish in the grace period', async () => {
  const cached = note(8);
  const gate = Promise.withResolvers<void>();
  const store = new MemoryEventStore();
  vi.spyOn(store, 'query').mockImplementation(async () => { await gate.promise; return [cached]; });
  const runtime = createNostrRuntime({ store, sources: [{ id: source.id, query: async () => ({ events: [] }) }] });
  try {
    const result = runtime.query([{ kinds: [1] }], { settle: { sources: 1, graceMs: 0 }, persist: 'background' });
    await new Promise(resolve => setTimeout(resolve, 20));
    gate.resolve();
    expect(await result).toMatchObject({ events: [cached], complete: true });
  } finally { gate.resolve(); await runtime.close(); }
});

it('waits for slow local cache replay when settle completes while another source is still pending', async () => {
  const cached = note(9);
  const gate = Promise.withResolvers<void>();
  const store = new MemoryEventStore();
  vi.spyOn(store, 'query').mockImplementation(async () => { await gate.promise; return [cached]; });
  const stalled: RuntimeSource = {
    id: 'slow-relay',
    query: (_filters, options) => new Promise(resolve => {
      options?.signal?.addEventListener('abort', () => resolve({ events: [], complete: false }), { once: true });
    }),
  };
  const runtime = createNostrRuntime({
    store,
    sources: [{ id: source.id, query: async () => ({ events: [], complete: true }) }, stalled],
  });
  try {
    const result = runtime.query([{ kinds: [1] }], {
      settle: { sources: 1, graceMs: 0 },
      persist: 'background',
      deadline: Date.now() + 30 * 86400_000,
    });
    await new Promise(resolve => setTimeout(resolve, 25));
    gate.resolve();
    expect(await result).toMatchObject({ events: [cached], complete: false, reason: 'eose' });
  } finally { gate.resolve(); await runtime.close(); }
});
