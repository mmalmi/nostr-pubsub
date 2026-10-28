import { afterEach, expect, it, vi } from 'vitest';
import { finalizeEvent } from 'nostr-tools/pure';
import { RetainedEvents } from '../src/retained-events.js';

const secret = new Uint8Array(32).fill(17);
const policy = { maxEvents: 3, filters: [{ kinds: [37195], '#d': ['mesh'] }] };
const now = 1000;
function event(content: string, time = now, scope = 'mesh', author = secret) {
  return finalizeEvent({ kind: 37195, created_at: time,
    tags: [['d', scope], ['expiration', String(time + 600)]], content }, author);
}
afterEach(() => vi.useRealTimers());
it('restores only signed coordinate winners and expires them after restart', () => {
  vi.useFakeTimers(); vi.setSystemTime(now * 1000);
  const latest = event('new', now + 1);
  const cache = new RetainedEvents(policy);
  cache.restore([event('old'), latest, event('old'), event('private', now, 'private'), { ...latest, content: 'forged' }]);
  const restored = new RetainedEvents(policy);
  restored.restore(JSON.parse(JSON.stringify(cache.snapshot())));
  expect(restored.query([{}]).map(e => e.id)).toEqual([latest.id]);
  vi.setSystemTime((now + 601) * 1000);
  expect(restored.query([{}])).toEqual([]);
});
it('uses the lower ID on ties, enforces capacity and respects request limits', () => {
  vi.useFakeTimers(); vi.setSystemTime(now * 1000);
  const a = event('a'), b = event('b');
  for (const events of [[a, b], [b, a]]) {
    const cache = new RetainedEvents(policy);
    cache.restore(events);
    expect(cache.snapshot().map(e => e.id)).toEqual([[a.id, b.id].sort()[0]]);
    for (let i = 20; i < 28; i++) cache.insert(event('peer', now + 1, 'mesh', new Uint8Array(32).fill(i)));
    expect(cache.snapshot()).toHaveLength(3);
    expect(cache.query([{ limit: 1 }])).toHaveLength(1);
    expect(cache.query([{ limit: 0 }])).toEqual([]);
    expect(cache.query([{}], { limit: 0 })).toEqual([]);
  }
});
it('rejects oversized and future-dated events before they displace useful addresses', () => {
  vi.useFakeTimers(); vi.setSystemTime(now * 1000);
  const cache = new RetainedEvents(policy);
  const original = event('good'); cache.insert(original);
  expect(cache.insert(event('x'.repeat(65536), now + 1))).toBe(false);
  expect(cache.insert(event('future', now + 301))).toBe(false);
  expect(cache.snapshot().map(e => e.id)).toEqual([original.id]);
});

it('rejects malformed expiration rather than treating it as permanent', () => {
  vi.useFakeTimers(); vi.setSystemTime(now * 1000);
  for (const value of ['', 'invalid', '+1600', '-1', '18446744073709551616']) {
    const event = finalizeEvent({ kind: 37195, created_at: now, content: 'invalid expiration',
      tags: [['d', 'mesh'], ['expiration', value]] }, secret);
    const cache = new RetainedEvents(policy); cache.restore([event]);
    expect(cache.snapshot()).toEqual([]);
  }
});
