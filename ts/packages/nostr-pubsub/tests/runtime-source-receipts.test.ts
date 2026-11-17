import { createPrivateRelayPublisher } from '../src/private-relay-publisher.js';
import { expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { createNostrRuntime } from '../src/runtime.js';
import { MemoryEventStore } from '../src/runtime-store.js';
import { createScopedFipsSource } from '../src/scoped-fips-source.js';
import type { RuntimeOutboxEntry } from '../src/runtime-types.js';
import type { PublishReport } from '../src/event-bus.js';

const event = () => finalizeEvent({ kind: 1060, created_at: 1_700_000_000,
  tags: [['v', '2'], ['header', 'encrypted header']], content: 'encrypted fixture' }, generateSecretKey());

it('keeps an event pending until a queued source provides its actual remote receipt', async () => {
  const store = new MemoryEventStore();
  let acknowledge!: (report: PublishReport) => void;
  const receipt = new Promise<PublishReport>(resolve => { acknowledge = resolve; });
  const receiverPublish = vi.fn();
  const source = createScopedFipsSource({ receiver: { id: 'live', publish: receiverPublish },
    publisher: { publish: () => receipt }, discovery: { query: vi.fn(), subscribe: vi.fn() } });
  const runtime = createNostrRuntime({ relays: [], store, sources: [source] });
  try {
    const signed = event();
    // The caller gets its answer once the event is durably queued; delivery continues behind it.
    expect(await runtime.publish(signed)).toMatchObject({ accepted: false, remoteAccepted: false, queued: true,
      sources: [{ id: 'fips', accepted: false, pending: true }] });
    expect((await store.listPending()).map(row => row.event.id)).toEqual([signed.id]);
    acknowledge({ accepted: true, remoteAccepted: true, priority: 100 });
    await vi.waitFor(async () => expect(await store.listPending()).toEqual([]));
    expect(receiverPublish).not.toHaveBeenCalled();
  } finally { source.close(); await runtime.close(); }
});

it.each([undefined, false])('does not retire local gossip with remoteAccepted=%s', async remoteAccepted => {
  const store = new MemoryEventStore();
  const runtime = createNostrRuntime({ relays: [], store,
    sources: [{ id: 'fips', publishAcceptance: 'queued',
      publish: async () => ({ accepted: true, remoteAccepted, priority: 100 }) }] });
  try {
    const signed = event();
    expect(await runtime.publish(signed)).toMatchObject({ accepted: false, remoteAccepted: false, queued: true,
      sources: [{ id: 'fips', accepted: false, pending: true }] });
    // Closing waits for the delivery attempt, which must leave the entry for a later retry.
    await runtime.close();
    expect((await store.listPending()).map(row => row.event.id)).toEqual([signed.id]);
  } finally { await runtime.close(); }
});

it('still returns the remote receipt itself when the caller opts out of queueing', async () => {
  const store = new MemoryEventStore();
  let acknowledge!: (report: PublishReport) => void;
  const receipt = new Promise<PublishReport>(resolve => { acknowledge = resolve; });
  const runtime = createNostrRuntime({ relays: [], store, sources: [{ id: 'fips', publish: () => receipt }] });
  try {
    const publishing = runtime.publish(event(), { queue: false });
    acknowledge({ accepted: true, remoteAccepted: true, priority: 100 });
    expect(await publishing).toMatchObject({ accepted: true, remoteAccepted: true, queued: false,
      sources: [{ id: 'fips', accepted: true }] });
    expect(await store.listPending()).toEqual([]);
  } finally { await runtime.close(); }
});

it('does not turn a rejected result into a receipt', async () => {
  const store = new MemoryEventStore();
  const runtime = createNostrRuntime({ relays: [], store,
    sources: [{ id: 'fips', publishAcceptance: 'queued',
      publish: async () => ({ accepted: false, remoteAccepted: true, priority: 100 }) }] });
  try {
    const signed = event();
    expect(await runtime.publish(signed)).toMatchObject({ accepted: false, remoteAccepted: false, queued: true });
    expect((await store.listPending()).map(row => row.event.id)).toEqual([signed.id]);
  } finally { await runtime.close(); }
});

it.each(['runtime', 'private'] as const)('preserves independent queued destinations across receipts and restart (%s)', async owner => {
  const store = new MemoryEventStore();
  // Exercise the original ID-keyed adapter interface and a pending row written by an older client.
  const legacy = { query: store.query.bind(store), put: store.put.bind(store), delete: store.delete.bind(store),
    listPending: store.listPending.bind(store), putPending: store.putPending.bind(store), deletePending: store.deletePending.bind(store) };
  const signed = event();
  await legacy.putPending({ event: signed, attempts: 2, updatedAt: 1, relays: [], sources: ['offline'] });
  const create = owner === 'private' ? createPrivateRelayPublisher : createNostrRuntime;
  let acknowledge!: (receipt: PublishReport) => void;
  const delayed = new Promise<PublishReport>(resolve => { acknowledge = resolve; });
  const first = create({ store: legacy, relays: [], sources: [
    { id: 'fast', publish: () => delayed }, { id: 'slow', publish: async () => ({ accepted: false, priority: 0 }) },
  ] });
  const sends = ['fast', 'slow'].map(id => first.publish(signed, { relays: [], sources: [id] }));
  try {
    await vi.waitFor(async () => expect((await store.listPending())[0]?.additionalScopes).toHaveLength(2));
  } finally {
    acknowledge({ accepted: true, priority: 0 });
    await Promise.all(sends);
    await first.close();
  }
  const retried: string[] = [];
  const second = create({ store: legacy, relays: [], sources: ['fast', 'slow', 'offline'].map(id => ({
    id, publish: async () => { retried.push(id); return { accepted: true, priority: 0 }; },
  })) });
  try {
    await second.retryPending();
    expect(retried.sort()).toEqual(['offline', 'slow']);
    expect(await store.listPending()).toEqual([]);
  } finally { await second.close(); }
});

it('reports a storage error when a legacy adapter discards the second destination', async () => {
  const memory = new MemoryEventStore();
  const store = { query: memory.query.bind(memory), put: memory.put.bind(memory), delete: memory.delete.bind(memory),
    listPending: memory.listPending.bind(memory), deletePending: memory.deletePending.bind(memory),
    putPending: ({ additionalScopes: _ignored, ...entry }: RuntimeOutboxEntry) => memory.putPending(entry) };
  const runtime = createNostrRuntime({ store, relays: [] });
  const signed = event();
  try {
    await runtime.publish(signed, { sources: ['first'] });
    await expect(runtime.publish(signed, { sources: ['second'] })).rejects.toThrow('additionalScopes');
    expect((await memory.listPending()).map(row => row.sources)).toEqual([['first']]);
  } finally { await runtime.close(); }
});
