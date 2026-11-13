import { describe, expect, it } from 'vitest';
import { generateSecretKey, finalizeEvent } from 'nostr-tools/pure';
import {
  NostrRuntime, parseRelayRateLimit, RateLimitAwarePool, RelayRateLimitError, SimplePoolNostrRelayTransport,
} from '../src/index.js';

const slow = 'wss://slow.example/', fast = 'wss://fast.example/';
const event = finalizeEvent({ kind: 1, content: 'test', created_at: 1, tags: [] }, generateSecretKey());

class Wire extends EventTarget {
  static OPEN = 1;
  readyState = 1;
  onopen?: () => void;
  onclose?: (ev: Event) => void;
  onmessage?: (msg: MessageEvent) => void;
  constructor(readonly url: string) { super(); queueMicrotask(() => this.onopen?.()); }
  send(raw: string) {
    const [type, payload] = JSON.parse(raw);
    if (type !== 'EVENT') return;
    queueMicrotask(() => {
      const frame = this.url === slow ? ['OK', payload.id, false, 'rate-limited: retry in 7s', '2500']
        : ['OK', payload.id, true, ''];
      const msg = new MessageEvent('message', { data: JSON.stringify(frame) });
      this.dispatchEvent(msg);
      this.onmessage?.(msg);
    });
  }
  close() { this.readyState = 3; this.onclose?.(new Event('close')); }
}
const websocketImplementation = Wire as unknown as typeof WebSocket;

describe('structured relay rate limits', () => {
  it('preserves the relay and wire delay and still succeeds when another relay accepts', async () => {
    const pool = new RateLimitAwarePool({ websocketImplementation });
    try {
      const transport = new SimplePoolNostrRelayTransport({ pool, getRelays: () => [slow] });
      const error = await transport.publish(event).catch(error => error);
      expect(error).toBeInstanceOf(RelayRateLimitError);
      expect(error).toMatchObject({ relay: slow, message: 'rate-limited: retry in 7s', retryAfterMs: 2500 });
      await expect(new SimplePoolNostrRelayTransport({ pool, getRelays: () => [slow, fast] }).publish(event)).resolves.toBeUndefined();
    } finally { pool.destroy(); }
  });

  it('includes the hint in the runtime per-source publication outcome', async () => {
    const client = new NostrRuntime({ relays: [slow], websocketImplementation });
    try {
      const report = await client.publish(event, { requireAck: true });
      expect(report.sources).toEqual([{ id: slow, accepted: false, rateLimited: true, retryAfterMs: 2500, error: 'rate-limited: retry in 7s' }]);
    } finally { await client.close(); }
  });

  it('classifies text-only rejections and leaves the default wait to the caller', () => {
    expect(parseRelayRateLimit(slow, 'rate-limited: retry in 4s')?.retryAfterMs).toBe(4000);
    expect(parseRelayRateLimit(slow, Error('rate-limited: slow down'), '0')).toMatchObject({ relay: slow, retryAfterMs: undefined });
    expect(parseRelayRateLimit(slow, 'blocked: try again', '300')).toBeUndefined();
  });
});
