import { expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { createNostrRuntime } from '../src/runtime.js';
import { MemoryEventStore } from '../src/runtime-store.js';
import { createScopedFipsSource } from '../src/scoped-fips-source.js';
import type { PublishReport } from '../src/event-bus.js';

const event = () => finalizeEvent({ kind: 1060, created_at: 1_700_000_000,
  tags: [['p', 'a'.repeat(64)], ['v', '2']], content: 'encrypted fixture' }, generateSecretKey());

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
    const publishing = runtime.publish(signed);
    await vi.waitFor(async () => expect((await store.listPending()).map(row => row.event.id)).toEqual([signed.id]));
    acknowledge({ accepted: true, remoteAccepted: true, priority: 100 });
    expect(await publishing).toMatchObject({ accepted: true, remoteAccepted: true, queued: false,
      sources: [{ id: 'fips', accepted: true }] });
    expect(await store.listPending()).toEqual([]);
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
      sources: [{ id: 'fips', accepted: false, queued: true }] });
    expect((await store.listPending()).map(row => row.event.id)).toEqual([signed.id]);
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
