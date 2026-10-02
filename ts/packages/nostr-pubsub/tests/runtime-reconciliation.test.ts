import { afterEach, describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { finalizeEvent } from 'nostr-tools/pure';
import { MemoryEventStore, NostrRuntime, type NostrEvent, type RuntimeReconciliationOptions } from '../src/index.js';
import { ReconciliationRelayFixture } from './runtime-reconciliation-fixture.js';
import { until } from './runtime-relay-fixture.js';

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });
const now = Math.floor(Date.now() / 1000);
const secret = new Uint8Array(32).fill(7);
function event(content: string, time = now - 10, kind = 1, tags: string[][] = []): NostrEvent {
  return finalizeEvent({ content, kind, created_at: time, tags }, secret);
}
async function setup(options: RuntimeReconciliationOptions | false = {}, store = new MemoryEventStore()) {
  const relay = new ReconciliationRelayFixture(); cleanup.push(() => relay.close());
  const client = new NostrRuntime({
    store, relays: [await relay.url()], websocketImplementation: WebSocket as unknown as typeof globalThis.WebSocket,
    reconciliation: options === false ? undefined : { timeoutMs: 400, capabilityTimeoutMs: 100, ...options },
    batchWindowMs: 0, reconnectDelayMs: 10,
  });
  cleanup.push(() => client.close());
  return { relay, client, store };
}

describe('optional runtime NIP-77 over the existing relay socket', () => {
  it('keeps cache and ordinary history immediate, recovers gaps, and keeps live events flowing', async () => {
    const { relay, client, store } = await setup();
    const cached = event('cached'), recent = event('ordinary'), missing = event('gap');
    await store.put(cached); relay.ordinaryEvents.push(recent); relay.events.push(cached, recent, missing);
    const received: string[] = [], completions: boolean[] = [];
    client.subscribe([{ kinds: [1], authors: [cached.pubkey], limit: 1 }], {
      onEvent: value => received.push(value.id), onEose: status => completions.push(status.complete),
    });
    await until(() => received.includes(missing.id));
    expect(received.slice(0, 2)).toEqual([cached.id, recent.id]);
    expect(completions).toEqual([true]); // comparison does not invent an extra EOSE
    expect(relay.frames[0]?.[0]).toBe('REQ');
    expect(relay.clients.size).toBe(1);
    expect(relay.frames.filter(frame => frame[0] === 'NEG-OPEN')).toHaveLength(1);
    expect(relay.frames.find(frame => frame[0] === 'NEG-OPEN')?.[2]).toMatchObject({ authors: [cached.pubkey], kinds: [1] });
    expect(relay.frames.find(frame => frame[0] === 'NEG-OPEN')?.[2]).not.toHaveProperty('limit');
    expect((await store.query([{ ids: [missing.id] }])).map(value => value.id)).toEqual([missing.id]);
    const live = event('live', now); relay.emit(live);
    await until(() => received.includes(live.id));
  });

  it.each(['disabled', 'unsupported', 'metadata timeout', 'silent', 'error', 'malformed', 'flood'] as const)('falls back without delaying history or live events: %s', async mode => {
    const { relay, client } = await setup(mode === 'disabled' ? false : {});
    if (mode === 'unsupported') relay.supported = false;
    if (mode === 'metadata timeout') relay.metadataSilent = true;
    if (['silent', 'error', 'malformed', 'flood'].includes(mode)) relay.negMode = mode as typeof relay.negMode;
    const sample = event('history'); relay.ordinaryEvents.push(sample);
    const received: string[] = []; let complete = false;
    client.subscribe([{ kinds: [1] }], { onEvent: value => received.push(value.id), onEose: () => { complete = true; } });
    await until(() => complete && received.includes(sample.id));
    const live = event('live', now); relay.emit(live);
    await until(() => received.includes(live.id));
    if (mode === 'disabled') expect(relay.metadataRequests).toBe(0);
    else await until(() => relay.metadataRequests === 1);
    if (['silent', 'error', 'malformed', 'flood'].includes(mode)) await until(() => relay.frames.some(frame => frame[0] === 'NEG-CLOSE'));
    if (['disabled', 'unsupported', 'metadata timeout'].includes(mode)) expect(relay.frames.some(frame => frame[0] === 'NEG-OPEN')).toBe(false);
    expect(relay.frames.filter(frame => frame[0] === 'REQ')).toHaveLength(1);
  });

  it('preserves exact author/kind/tag intersections and clamps historical windows', async () => {
    const { relay, client } = await setup({ lookbackSeconds: 60 });
    const wanted = event('wanted', now - 30, 1, [['p', 'recipient-a']]);
    relay.events.push(wanted, event('wrong kind', now - 30, 4, [['p', 'recipient-a']]), event('wrong recipient', now - 30, 1, [['p', 'recipient-b']]), event('outside window', now - 100, 1, [['p', 'recipient-a']]));
    const received: string[] = [];
    client.subscribe([{ authors: [wanted.pubkey], kinds: [1], '#p': ['recipient-a'], since: 0, until: now }], { onEvent: value => received.push(value.id) });
    await until(() => received.length === 1);
    expect(received).toEqual([wanted.id]);
    expect(relay.frames.find(frame => frame[0] === 'NEG-OPEN')?.[2]).toEqual({ authors: [wanted.pubkey], kinds: [1], '#p': ['recipient-a'], since: now - 60, until: now });
  });

  it('recovers a requested older window without moving it to recent history', async () => {
    const { relay, client } = await setup({ lookbackSeconds: 60 });
    const end = now - 365 * 86400;
    const older = event('older requested page', end - 20);
    relay.events.push(older, event('recent unrelated page'));
    const received: string[] = [];
    client.subscribe([{ kinds: [1], since: end - 30, until: end }], { onEvent: value => received.push(value.id) });
    await until(() => received.includes(older.id));
    expect(received).toEqual([older.id]);
    expect(relay.frames.find(frame => frame[0] === 'NEG-OPEN')?.[2]).toEqual({ kinds: [1], since: end - 30, until: end });
  });

  it('does not make a short query wait for reconciliation or leak a comparison afterward', async () => {
    const { relay, client } = await setup(); relay.metadataSilent = true;
    const sample = event('ordinary result'); relay.ordinaryEvents.push(sample);
    const result = await client.query([{ kinds: [1] }]);
    expect(result).toMatchObject({ complete: true, events: [sample] });
    expect(relay.frames.map(frame => frame[0])).not.toContain('NEG-OPEN');
    await until(() => relay.frames.some(frame => frame[0] === 'CLOSE'));
  });

  it('never opens comparison for cache-only, peer-only, or exact-ID reads', async () => {
    const { relay, client } = await setup();
    await client.query([{ kinds: [1] }], { cache: 'cache-only' });
    await client.query([{ kinds: [1] }], { relays: [] });
    await client.query([{ ids: [event('exact').id] }]);
    expect(relay.metadataRequests).toBe(0);
    expect(relay.frames.filter(frame => frame[0] === 'REQ')).toHaveLength(1);
  });

  it('rejects invalid signatures and tombstoned missing events through ordinary admission', async () => {
    const { relay, client, store } = await setup();
    const removed = event('removed'), invalid = event('invalid');
    await client.ingest(event('', now - 5, 5, [['e', removed.id]]));
    relay.events.push(removed, { ...invalid, sig: '0'.repeat(128) });
    const received: NostrEvent[] = [];
    client.subscribe([{ kinds: [1] }], { onEvent: value => received.push(value) });
    await until(() => relay.frames.filter(frame => frame[0] === 'REQ').length === 2);
    await until(() => relay.frames.filter(frame => frame[0] === 'CLOSE').length >= 2);
    expect(received).toEqual([]);
    expect(await store.query([{ kinds: [1] }])).toEqual([]);
    expect(await store.query([{ kinds: [5] }])).toHaveLength(1);
  });

  it('cancels bounded work on unsubscribe and does not retry on rapid resubscribe', async () => {
    const { relay, client } = await setup(); relay.negMode = 'silent';
    const filters = [{ kinds: [1] }];
    const subscription = client.subscribe(filters, { onEvent() {} });
    await until(() => relay.frames.some(frame => frame[0] === 'NEG-OPEN'));
    subscription.close();
    await until(() => relay.frames.some(frame => frame[0] === 'NEG-CLOSE'));
    client.subscribe(filters, { onEvent() {} });
    await until(() => relay.frames.filter(frame => frame[0] === 'REQ').length === 2);
    expect(relay.frames.filter(frame => frame[0] === 'NEG-OPEN')).toHaveLength(1);
  });

  it('refuses truncated inventory and oversized missing sets without changing normal history', async () => {
    const { relay, client, store } = await setup({ maxRecords: 2, maxNeedIds: 1 });
    for (let i = 0; i < 3; i++) await store.put(event(`cached ${i}`));
    let complete = false;
    client.subscribe([{ kinds: [1] }], { onEvent() {}, onEose: () => { complete = true; } });
    await until(() => complete && relay.metadataRequests === 1);
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(relay.frames.some(frame => frame[0] === 'NEG-OPEN')).toBe(false);
    relay.events.push(event('missing 1', now - 10, 2), event('missing 2', now - 10, 2));
    client.subscribe([{ kinds: [2] }], { onEvent() {} });
    await until(() => relay.frames.some(frame => frame[0] === 'NEG-CLOSE'));
    expect(relay.frames.filter(frame => frame[0] === 'REQ')).toHaveLength(2);
  });
});
