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
