import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { finalizeEvent } from 'nostr-tools/pure';
import { MemoryEventStore, NostrRuntime, type NostrEvent, type NostrRuntimeOptions } from '../src/index.js';
import { RuntimeRelayFixture, until } from './runtime-relay-fixture.js';

const key = new Uint8Array(32).fill(7);
function event(content: string, kind: number, created_at = 100, tags: string[][] = []): NostrEvent {
  return finalizeEvent({ content, kind, created_at, tags }, key);
}

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

async function fixture(options: NostrRuntimeOptions = {}) {
  const relay = new RuntimeRelayFixture();
  const url = await relay.url();
  cleanup.push(() => relay.close());
  const runtime = new NostrRuntime({
    relays: [url],
    websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
    historyTimeoutMs: 1_000,
    ...options,
  });
  cleanup.push(() => runtime.close());
  return { relay, runtime, url };
}

describe('isolated transient relay queries', () => {
  it('takes a verified page and its actual EOSE from the warmed socket while an unrelated write is blocked', async () => {
    const original = event('regular durable note', 1);
    const expected = event('matching profile', 0, 200);
    const expired = event('expired profile', 0, 210, [['expiration', '1']]);
    const wrongKind = event('wrong kind', 1, 220);
    const wrongSignature = { ...expected, content: 'signature no longer matches' };
    const second = finalizeEvent({ content: 'another matching profile', kind: 0, created_at: 195, tags: [] }, new Uint8Array(32).fill(8));
    const beyondLimit = event('outside the result limit', 0, 190);
    const local = event('local-only duplicate-looking profile', 0, 230);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let resumeTest!: () => void;
    const writeStarted = new Promise<void>((resolve) => { resumeTest = resolve; });
    const writes: string[] = [];
    let reads = 0;
    let discoveryRequests = 0;
    const store = new MemoryEventStore();
    await store.put(local);
    const put = store.put.bind(store);
    const query = store.query.bind(store);
    store.put = async (candidate) => {
      writes.push(candidate.id);
      if (candidate.id === original.id) {
        resumeTest();
        await gate;
      }
      await put(candidate);
    };
    store.query = async (...args) => {
      reads++;
      return await query(...args);
    };
    const { relay, runtime, url } = await fixture({
      store,
      reconciliation: {
        async fetch() {
          discoveryRequests++;
          return Response.json({ supported_nips: [77] });
        },
      },
    });
    relay.events.push(original);
    relay.server.on('connection', (socket) => socket.on('message', (data) => {
      const [type, id, filter] = JSON.parse(data.toString());
      if (type !== 'REQ' || !filter?.kinds?.includes(0)) return;
      for (const candidate of [wrongSignature, expired, wrongKind, expected, expected, second, beyondLimit]) {
        socket.send(JSON.stringify(['EVENT', id, candidate]));
      }
    }));

    const ordinary = runtime.query([{ ids: [original.id] }], { cache: 'network-only' });
    try {
      await writeStarted;
      const readsAtStall = reads;
      const transient = await runtime.queryTransientRelay(url, [{ kinds: [0], limit: 5 }], { limit: 2 });
      expect(transient).toEqual({
        events: [
          expect.objectContaining({ id: expected.id, content: expected.content }),
          expect.objectContaining({ id: second.id, content: second.content }),
        ],
        reason: 'eose', complete: true, sources: [{ id: url, complete: true }],
      });
      expect(relay.frames.size).toBe(1);
      expect(relay.requests).toEqual([[{ ids: [original.id] }], [{ kinds: [0], limit: 2 }]]);
      expect(reads).toBe(readsAtStall);
      expect(writes).toEqual([original.id]);
      expect(discoveryRequests).toBe(0);
      expect(await query([{ ids: [expected.id] }])).toEqual([]);
    } finally {
      release();
    }
    expect((await ordinary).events.map(({ id }) => id)).toEqual([original.id]);
  });

  it('returns partials on deadline and closes aborted queries while keeping the pooled socket', async () => {
    const { relay, runtime, url } = await fixture();
    const profile = event('a valid partial', 0);
    relay.events.push(profile);
    await runtime.queryTransientRelay(url, [{ kinds: [0] }]);
    relay.eoseDelay = 200;

    const result = await runtime.queryTransientRelay(url, [{ kinds: [0] }], { deadline: Date.now() + 100 });
    expect(result).toMatchObject({ events: [{ id: profile.id }], complete: false, reason: 'timeout' });

    const controller = new AbortController();
    const cancelled = runtime.queryTransientRelay(url, [{ kinds: [0] }], { signal: controller.signal });
    const rejection = expect(cancelled).rejects.toMatchObject({ name: 'AbortError' });
    await until(() => relay.requests.length === 3);
    const frames = [...relay.frames.values()][0]!;
    const cancelledId = frames.filter(([type]) => type === 'REQ').at(-1)?.[1];
    controller.abort();
    await rejection;
    await until(() => frames.some(([type, id]) => type === 'CLOSE' && id === cancelledId));
    expect(relay.clients.size).toBe(1);
    expect([...relay.clients.values()][0]?.size).toBe(0);
    relay.eoseDelay = 0;
    const afterAbort = await runtime.queryTransientRelay(url, [{ kinds: [0] }]);
    expect(afterAbort).toMatchObject({ complete: true, events: [{ id: profile.id }] });
    expect(relay.frames.size).toBe(1);
  });

  it('does not sign an AUTH challenge for a transient query', async () => {
    let calls = 0;
    const { relay, runtime, url } = await fixture({
      async signAuthEvent(_url, template) {
        calls++;
        return finalizeEvent(template, key);
      },
    });
    relay.requireAuth = true;
    const response = await runtime.queryTransientRelay(url, [{ kinds: [0] }]);
    expect(response).toMatchObject({ complete: false, events: [], reason: 'unavailable' });
    expect(response.sources[0]?.error).toMatch(/^auth-required:/);
    expect(calls).toBe(0);
    expect(relay.authEvents).toEqual([]);
  });
});
