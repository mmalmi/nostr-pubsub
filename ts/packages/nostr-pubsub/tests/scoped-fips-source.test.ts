import { expect, it, vi } from 'vitest';
import { createScopedFipsSource } from '../src/scoped-fips-source.js';
import type { RuntimeSource } from '../src/runtime-types.js';
import type { NostrEventSubscription } from '../src/event-bus.js';
import type { NostrEvent, NostrFilter } from '../src/types.js';
import { localIndexSource } from '../src/source.js';

const alice = 'a'.repeat(64), bob = 'b'.repeat(64);
const empty = { events: [], complete: true };
const event = (kind: number) => ({ kind } as NostrEvent);
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

it('preserves mixed public/inbox scopes and keeps each sensitive author lookup independent', async () => {
  const receiver = { id: 'live', query: vi.fn(async (_filters: NostrFilter[]) => empty) };
  const discovery = { query: vi.fn(async (_filters: NostrFilter[]) => ({ ...empty, complete: false })), subscribe: vi.fn() };
  const source = createScopedFipsSource({ receiver, discovery, publisher: { publish: vi.fn() } });
  const result = await source.query!([
    { kinds: [0, 1], authors: [alice, bob] },
    { kinds: [1059], '#p': [alice] },
    { kinds: [37368], authors: [bob] },
  ]);
  expect(result.complete).toBe(false);
  expect(receiver.query.mock.calls[0][0]).toEqual([
    { kinds: [1], authors: [alice, bob] }, { kinds: [1059], '#p': [alice] },
  ]);
  expect(discovery.query.mock.calls.map(call => call[0])).toEqual([
    [{ kinds: [0], authors: [alice] }],
    [{ kinds: [0], authors: [bob] }, { kinds: [37368], authors: [bob] }],
  ]);
  source.close();
});

it('never falls back to the live endpoint after private publication rejects', async () => {
  const receiver = { id: 'live', publish: vi.fn(async () => ({ accepted: true, priority: 1 })) };
  const publisher = { publish: vi.fn(async () => { throw new Error('private endpoint unavailable'); }) };
  const source = createScopedFipsSource({ receiver, publisher, discovery: { query: vi.fn(), subscribe: vi.fn() } });
  for (const kind of [1059, 1060, 21111]) await expect(source.publish!(event(kind), localIndexSource('test'))).rejects.toThrow('private endpoint unavailable');
  expect(receiver.publish).not.toHaveBeenCalled();
  await source.publish!(event(1), localIndexSource('test'));
  expect(receiver.publish).toHaveBeenCalledOnce();
  source.close();
});

it('closes anonymous and receiver starts that finish after cancellation', async () => {
  const first = deferred<NostrEventSubscription>(), second = deferred<NostrEventSubscription>();
  const closeFirst = vi.fn(), closeSecond = vi.fn();
  const receiver: RuntimeSource = { id: 'live', subscribe: () => first.promise };
  const source = createScopedFipsSource({ receiver, publisher: { publish: vi.fn() },
    discovery: { query: vi.fn(), subscribe: () => second.promise } });
  const pending = source.subscribe!([{ kinds: [1060], '#p': [alice] }, { kinds: [37368], authors: [bob] }], vi.fn());
  source.close();
  first.resolve({ close: closeFirst }); second.resolve({ close: closeSecond });
  await expect(pending).rejects.toThrow();
  expect(closeFirst).toHaveBeenCalledOnce(); expect(closeSecond).toHaveBeenCalledOnce();
});

it('releases every opened route when another scoped subscription fails', async () => {
  const close = vi.fn();
  const source = createScopedFipsSource({
    receiver: { id: 'live', subscribe: () => ({ close }) }, publisher: { publish: vi.fn() },
    discovery: { query: vi.fn(), subscribe: async () => { throw new Error('scope rejected'); } },
  });
  await expect(source.subscribe!([{ kinds: [1060], '#p': [alice] }, { kinds: [37368], authors: [bob] }], vi.fn())).rejects.toThrow('scope rejected');
  expect(close).toHaveBeenCalledOnce(); source.close();
});
