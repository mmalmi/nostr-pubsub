import { expect, it, vi } from 'vitest';
import { finalizeEvent } from 'nostr-tools/pure';
import { RetainedPeerDiscovery } from '../src/retained-peer-discovery.js';
import { RetainedEvents } from '../src/retained-events.js';
import { localIndexSource } from '../src/source.js';
import type { EventBus } from '../src/event-bus.js';
import type { RuntimeSubscriptionHandlers } from '../src/runtime-types.js';

const policy = { maxEvents: 4, filters: [{ kinds: [37195], '#d': ['mesh'] }] };
function advert(created_at: number, content = 'peer') {
  return finalizeEvent({ kind: 37195, created_at,
    tags: [['d', 'mesh'], ['expiration', String(created_at + 600)]], content }, new Uint8Array(32).fill(19));
}
function harness(cache = new RetainedEvents(policy)) {
  let handlers: RuntimeSubscriptionHandlers | undefined;
  const closed = vi.fn();
  const source = localIndexSource('fixture');
  const writes = vi.fn(async (event: ReturnType<typeof advert>) => ({ accepted: cache.insert(event), priority: 0 }));
  const store: EventBus = {
    publish: writes,
    query: async (filters, options) => ({ events: cache.query(filters, options).map(event => ({ event, source, priority: 0 })) }),
  };
  const publish = vi.fn(async () => { throw new Error('offline'); });
  const subscribe = vi.fn((_filters, next: RuntimeSubscriptionHandlers) => {
    handlers = next; return { close: closed };
  });
  const errors = vi.fn();
  return { cache, store, errors, publish, subscribe, closed, writes,
    bridge: new RetainedPeerDiscovery({ publish, subscribe }, store, errors),
    receive: (event: ReturnType<typeof advert>) => handlers!.onEvent(event, { source: 'fixture', cached: false }),
  };
}
it('restores signed announcements before any carrier responds and publishes offline', async () => {
  const now = Math.floor(Date.now() / 1000);
  const first = harness();
  const event = advert(now);
  await first.bridge.publish(event);
  expect(first.errors).toHaveBeenCalledOnce();
  await first.bridge.close();
  const cache = new RetainedEvents(policy); cache.restore(JSON.parse(JSON.stringify(first.cache.snapshot())));
  const second = harness(cache), received = vi.fn();
  await second.bridge.subscribe(policy.filters[0], received);
  expect(received).toHaveBeenCalledWith(event, 'local-cache');
  expect(second.publish).not.toHaveBeenCalled();
  await second.bridge.close();
  expect(second.closed).toHaveBeenCalledOnce();
});
it('does not deliver a superseded or expired network replay over the durable winner', async () => {
  const now = Math.floor(Date.now() / 1000), run = harness(), received = vi.fn();
  await run.store.publish(advert(now), localIndexSource('fixture'));
  await run.bridge.subscribe(policy.filters[0], received);
  run.receive(advert(now - 1, 'old')); run.receive(advert(now - 601, 'expired'));
  await vi.waitFor(() => expect(run.writes).toHaveBeenCalledTimes(3));
  expect(received).toHaveBeenCalledTimes(1);
  await run.bridge.close();
  expect(run.cache.snapshot()).toHaveLength(1);
});
it('bounds pending storage work and closes network interest before draining', async () => {
  const run = harness(), received = vi.fn();
  await run.bridge.subscribe(policy.filters[0], received);
  const event = advert(Math.floor(Date.now() / 1000));
  for (let i = 0; i < 80; i++) run.receive(event);
  expect(run.errors).toHaveBeenCalledTimes(16);
  await run.bridge.close();
  expect(run.closed).toHaveBeenCalledOnce();
  expect(received).not.toHaveBeenCalled();
});
