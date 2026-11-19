import { expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { createNostrRuntime } from '../src/runtime.js';
import { MemoryEventStore } from '../src/runtime-store.js';
import type { NostrEvent } from '../src/types.js';
import type { RuntimeSource } from '../src/runtime-types.js';

const key = generateSecretKey();
const note = (index: number) => finalizeEvent({ kind: 1, created_at: 1_700_000_000 + index, tags: [], content: `note ${index}` }, key);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

class SlowStore extends MemoryEventStore {
  override async put(event: NostrEvent): Promise<void> { await sleep(400); return await super.put(event); }
}

const answering = (id: string, events: NostrEvent[], extra: Partial<RuntimeSource> = {}): RuntimeSource => ({
  id, query: async () => ({ events: events.map((event) => ({ event })), complete: true }), ...extra,
});
const stalled = (id: string): RuntimeSource => ({
  id, query: (_filters, options) => new Promise((resolve) => {
    options?.signal?.addEventListener('abort', () => resolve({ events: [], complete: false }), { once: true });
  }),
});

it('delivers source events without waiting for slow store admission', async () => {
  const events = [note(1), note(2), note(3)];
  const runtime = createNostrRuntime({ relays: [], store: new SlowStore(), sources: [answering('archive', events)] });
  try {
    const started = Date.now();
    const result = await runtime.query([{ kinds: [1] }], { persist: 'background', deadline: Date.now() + 5_000 });
    expect(Date.now() - started).toBeLessThan(300);
    expect(result.events.map((event) => event.id).sort()).toEqual(events.map((event) => event.id).sort());
    expect(result.complete).toBe(true);
    // The events are still persisted afterwards.
    await vi.waitFor(async () => expect(await runtime.store.query([{ kinds: [1] }])).toHaveLength(3), { timeout: 3_000 });
  } finally { await runtime.close(); }
});

it('settles after the first finished source plus a grace period', async () => {
  const fast = [note(4)];
  const statuses: string[] = [];
  const runtime = createNostrRuntime({ relays: [], sources: [answering('relay-a', fast), stalled('relay-b')] });
  try {
    const started = Date.now();
    const result = await new Promise<{ ids: string[]; complete: boolean }>((resolve) => {
      const ids: string[] = [];
      const subscription = runtime.subscribe([{ kinds: [1] }], {
        onEvent: (event) => ids.push(event.id),
        onSource: (status) => statuses.push(`${status.id}:${status.complete}`),
        onEose: (completion) => { subscription.close(); resolve({ ids, complete: completion.complete }); },
      }, { persist: 'background', settle: { sources: 1, graceMs: 50 }, deadline: Date.now() + 5_000 });
    });
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(result.ids).toEqual([fast[0]!.id]);
    // A source that did not finish keeps the history marked incomplete.
    expect(result.complete).toBe(false);
    expect(statuses).toEqual(['relay-a:true']);
  } finally { await runtime.close(); }
});

it('settles as soon as every requested id was delivered', async () => {
  const wanted = [note(5), note(6)];
  const runtime = createNostrRuntime({ relays: [], sources: [answering('relay-a', wanted), stalled('relay-b')] });
  try {
    const started = Date.now();
    const result = await runtime.query([{ ids: wanted.map((event) => event.id) }], {
      persist: 'background', settle: { ids: true }, deadline: Date.now() + 5_000,
    });
    expect(Date.now() - started).toBeLessThan(500);
    expect(result.events.map((event) => event.id).sort()).toEqual(wanted.map((event) => event.id).sort());
    expect(result.complete).toBe(true);
  } finally { await runtime.close(); }
});

it('skips sources that do not support a query instead of reporting them unavailable', async () => {
  const events = [note(7)];
  const unsupported = vi.fn();
  const runtime = createNostrRuntime({ relays: [], sources: [
    answering('archive', events),
    { id: 'search-only', supports: (filters) => filters.every((filter) => Boolean(filter.search)), query: unsupported },
  ] });
  try {
    const result = await runtime.query([{ kinds: [1] }], { deadline: Date.now() + 5_000 });
    expect(unsupported).not.toHaveBeenCalled();
    expect(result.complete).toBe(true);
    expect(result.sources.map((source) => source.id)).toEqual(['archive']);
  } finally { await runtime.close(); }
});

it('keeps the awaited default: history completes after admission', async () => {
  const runtime = createNostrRuntime({ relays: [], store: new SlowStore(), sources: [answering('archive', [note(8)])] });
  try {
    const started = Date.now();
    const result = await runtime.query([{ kinds: [1] }], { deadline: Date.now() + 5_000 });
    expect(Date.now() - started).toBeGreaterThanOrEqual(350);
    expect(result.events).toHaveLength(1);
  } finally { await runtime.close(); }
});
