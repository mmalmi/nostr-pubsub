import { expect, it } from 'vitest';
import { drainRelayOutbox, enqueueRelayPublication, retryRelayOutbox, type RelayDelivery } from '../src/relay-outbox.js';
import { RelayRateLimitError } from '../src/relay-rate-limit.js';

it('publishes once, resumes each destination independently, and respects stored retry deadlines', async () => {
  const rows = new Map<string, RelayDelivery>();
  const deadlines = new Map<string, number>();
  await enqueueRelayPublication({ id: 'event', created_at: 1 }, ['a','b','a'], async row => {
    rows.set(row.relay, row);
  }, { broadcast: true });
  expect(rows.size).toBe(2);
  const store = {
    deadline: async (url: string) => deadlines.get(url),
    defer: async (url: string, time: number) => { deadlines.set(url, time); },
  };
  let limited = true;
  const attempts: string[] = [];
  const pass = () => retryRelayOutbox(['a','b'], store, async url => {
    await drainRelayOutbox(url, [...rows.values()], {
      enabled: async () => true, connected: () => true,
      send: async row => {
        attempts.push(row.relay);
        if (url === 'b' && limited) throw new RelayRateLimitError(url, 'rate-limited:', 10000);
      },
      acknowledge: async ids => { expect(ids).toEqual(['event']); rows.delete(url); },
    });
  }, () => {}, (_, error) => { throw error; });
  await pass();
  expect([...rows.keys()]).toEqual(['b']);
  expect(deadlines.get('b')).toBeGreaterThan(Date.now());
  await pass();
  expect(attempts.sort()).toEqual(['a','b']);
  deadlines.set('b', Date.now() - 1);
  limited = false;
  await pass();
  expect(rows.size).toBe(0);
  expect(attempts).toEqual(['a','b','b']);
});

it('compares only enabled broadcast rows and keeps paused rows pending', async () => {
  const rows = ['paused-first', 'already-present', 'paused-middle', 'missing'].map(id =>
    ({ id, relay: 'a', broadcast: true }));
  const confirmed: string[] = [];
  const sent: string[] = [];
  const progress = await drainRelayOutbox('a', rows, {
    enabled: async row => !row.id.startsWith('paused'),
    missing: async batch => {
      expect(batch.map(row => row.id)).toEqual(['already-present', 'missing']);
      return new Set(['missing']);
    },
    send: async row => { sent.push(row.id); },
    acknowledge: async ids => { confirmed.push(...ids); },
    connected: () => true,
  });
  expect(sent).toEqual(['missing']);
  expect(confirmed).toEqual(['already-present', 'missing']);
  expect(progress).toEqual({ sent: 1, present: 1, failed: 0 });
});

it('paused rows do not use up the automatic send budget', async () => {
  const rows = Array.from({ length: 205 }, (_, i) => ({ id: String(i), relay: 'a' }));
  const sent: string[] = [];
  await drainRelayOutbox('a', rows, {
    enabled: async row => Number(row.id) >= 102,
    send: async row => { sent.push(row.id); },
    acknowledge: async () => {}, connected: () => true,
  });
  expect(sent).toEqual(rows.slice(102, 202).map(row => row.id));
});

it('retains a delivery paused while the inventory comparison is in flight', async () => {
  let enabled = true;
  const confirmed: string[] = [];
  await drainRelayOutbox('a', [{ id: 'post', relay: 'a', broadcast: true }], {
    enabled: async () => enabled,
    missing: async () => { enabled = false; return new Set(); },
    send: async () => { throw new Error('Unexpected send'); },
    acknowledge: async ids => { confirmed.push(...ids); },
    connected: () => true,
  });
  expect(confirmed).toEqual([]);
});
