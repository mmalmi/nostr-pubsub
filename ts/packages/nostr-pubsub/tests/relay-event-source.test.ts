import { describe, expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import {
  NostrRelayEventSource,
  type NostrEvent,
  type NostrFilter,
  type NostrRelaySubscription,
  type NostrRelayTransport,
  type NostrRelayTransportHandlers,
  type NostrVerifiedEvent,
} from '../src/index.js';

class MemoryRelay implements NostrRelayTransport {
  readonly subscriptions = new Set<NostrRelayTransportHandlers>();
  readonly published: NostrVerifiedEvent[] = [];

  subscribe(
    _filters: NostrFilter[],
    handlers: NostrRelayTransportHandlers,
  ): NostrRelaySubscription {
    this.subscriptions.add(handlers);
    return { close: () => this.subscriptions.delete(handlers) };
  }

  publish(event: NostrVerifiedEvent): void {
    this.published.push(event);
  }

  event(event: NostrEvent): void {
    for (const handlers of this.subscriptions) handlers.onEvent(event);
  }

  eose(): void {
    for (const handlers of [...this.subscriptions]) handlers.onClose?.();
  }
}

describe('traditional relay router source', () => {
  it('queries verified relay events and completes on EOSE', async () => {
    const relay = new MemoryRelay();
    const source = new NostrRelayEventSource('wss://relay.example', relay);
    const event = note('historical', 2);
    const query = source.query([{ kinds: [1] }]);
    relay.event({ ...event, content: 'forged' });
    relay.event(event);
    relay.eose();

    await expect(query).resolves.toEqual({
      events: [{
        event: expect.objectContaining({ id: event.id }),
        source: { id: 'wss://relay.example', kind: 'relay', url: 'wss://relay.example' },
        priority: -100,
      }],
    });
  });

  it('publishes and exposes a verified live subscription', async () => {
    const relay = new MemoryRelay();
    const source = new NostrRelayEventSource('wss://relay.example', relay);
    const handler = vi.fn();
    const subscription = source.subscribe([{ kinds: [1] }], handler);
    const event = note('live', 3);

    relay.event(event);
    await source.publish(event, { id: 'local', kind: 'local-index' });
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({
      event: expect.objectContaining({ id: event.id }),
      priority: -100,
    }));
    expect(relay.published).toHaveLength(1);
    subscription.close();
    expect(relay.subscriptions.size).toBe(0);
  });
});

function note(content: string, createdAt: number) {
  return finalizeEvent({ kind: 1, created_at: createdAt, tags: [], content }, generateSecretKey());
}
