import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  finalizeEvent,
  generateSecretKey,
  verifiedSymbol,
  verifyEvent,
} from 'nostr-tools/pure';
import {
  createSimplePoolNostrRelayVerificationBoundary,
  SimplePoolNostrRelayTransport,
  type NostrEvent,
  type NostrFilter,
  type NostrRelayTransportHandlers,
  type NostrVerifiedEvent,
} from '../src/index.js';

type PoolSubscription = {
  relays: string[];
  filter: NostrFilter;
  params: {
    onevent(event: NostrEvent): void;
    oneose?(): void;
    onclose?(reasons: string[]): void;
  };
  close: ReturnType<typeof vi.fn>;
};

class MemorySimplePool {
  readonly subscriptions: PoolSubscription[] = [];
  readonly publish = vi.fn((_relays: string[], _event: NostrEvent) => [Promise.resolve('ok')]);

  subscribeMany(
    relays: string[],
    filter: NostrFilter,
    params: PoolSubscription['params'],
  ): { close(reason?: string): void } {
    const subscription = { relays, filter, params, close: vi.fn() };
    this.subscriptions.push(subscription);
    return subscription;
  }
}

afterEach(() => {
  vi.useRealTimers();
});

describe('SimplePool Nostr relay transport', () => {
  it('deduplicates relays and rejects invalid inbound events at its boundary', () => {
    const pool = new MemorySimplePool();
    const transport = new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example', 'wss://one.example', 'wss://two.example'],
      pool,
    });
    const handlers: NostrRelayTransportHandlers = { onEvent: vi.fn() };
    transport.subscribe([{ kinds: [1] }], handlers);
    const event = note('valid', 3);

    pool.subscriptions[0]?.params.onevent({ ...event, content: 'forged' });
    pool.subscriptions[0]?.params.onevent(event);

    expect(pool.subscriptions[0]?.relays).toEqual([
      'wss://one.example',
      'wss://two.example',
    ]);
    expect(handlers.onEvent).toHaveBeenCalledTimes(1);
    expect(handlers.onEvent).toHaveBeenCalledWith(expect.objectContaining({ id: event.id }));
  });

  it('reuses an exact SimplePool WASM-style admission without duplicate verification', () => {
    const verifier = vi.fn((event: NostrEvent) => verifyEvent(event));
    const boundary = createSimplePoolNostrRelayVerificationBoundary(verifier);
    const pool = new MemorySimplePool();
    const transport = new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example'],
      pool,
      verificationBoundary: boundary,
    });
    const handlers: NostrRelayTransportHandlers = { onEvent: vi.fn() };
    transport.subscribe([{ kinds: [1] }], handlers);
    const signed = note('boundary', 4);
    const event = Object.freeze({
      ...signed,
      tags: Object.freeze(signed.tags.map((tag) => Object.freeze([...tag]))),
    }) as NostrEvent;

    expect(boundary.verifyEvent(event)).toBe(true);
    pool.subscriptions[0]?.params.onevent(event);

    expect(verifier).toHaveBeenCalledTimes(1);
    expect(handlers.onEvent).toHaveBeenCalledTimes(1);
    expect(handlers.onEvent).toHaveBeenCalledWith(expect.objectContaining({
      id: event.id,
      content: 'boundary',
    }));
    expect(vi.mocked(handlers.onEvent).mock.calls[0]?.[0]).not.toBe(event);
  });

  it('requires an exact boundary proof and rejects spoofed or stale verification markers', () => {
    const boundary = createSimplePoolNostrRelayVerificationBoundary();
    const pool = new MemorySimplePool();
    const transport = new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example'],
      pool,
      verificationBoundary: boundary,
    });
    const handlers: NostrRelayTransportHandlers = { onEvent: vi.fn() };
    transport.subscribe([{ kinds: [1] }], handlers);

    const unproved = note('unproved', 5);
    pool.subscriptions[0]?.params.onevent(unproved);

    const signed = note('signed', 6);
    const spoofed = {
      ...signed,
      content: 'forged',
      [verifiedSymbol]: true,
    };
    expect(boundary.verifyEvent(spoofed)).toBe(false);
    pool.subscriptions[0]?.params.onevent(spoofed);

    const mutable = {
      ...note('before', 7),
      tags: [] as string[][],
    };
    expect(boundary.verifyEvent(mutable)).toBe(true);
    mutable.content = 'after';
    pool.subscriptions[0]?.params.onevent(mutable);
    expect(handlers.onEvent).toHaveBeenCalledWith(expect.objectContaining({ content: 'before' }));

    expect(boundary.verifyEvent(mutable)).toBe(false);
    pool.subscriptions[0]?.params.onevent(mutable);
    expect(handlers.onEvent).toHaveBeenCalledTimes(1);
  });

  it('does not accept an opaque boundary without its paired shared pool', () => {
    const boundary = createSimplePoolNostrRelayVerificationBoundary();
    expect(() => new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example'],
      verificationBoundary: boundary,
    })).toThrow('requires its paired shared pool');
  });

  it('bounds historical queries by a quiet window without closing live subscriptions', () => {
    vi.useFakeTimers();
    const pool = new MemorySimplePool();
    const transport = new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example'],
      pool,
      queryQuietWindowMs: 100,
    });
    const queryHandlers: NostrRelayTransportHandlers = {
      onEvent: vi.fn(),
      onClose: vi.fn(),
    };
    const liveHandlers: NostrRelayTransportHandlers = {
      onEvent: vi.fn(),
      onClose: vi.fn(),
    };

    transport.subscribe([{ kinds: [0] }], queryHandlers, { closeOnEose: true });
    transport.subscribe([{ kinds: [1] }], liveHandlers);
    vi.advanceTimersByTime(99);
    expect(queryHandlers.onClose).not.toHaveBeenCalled();
    pool.subscriptions[0]?.params.onevent(note('profile', 4));
    vi.advanceTimersByTime(99);
    expect(queryHandlers.onClose).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);

    expect(queryHandlers.onClose).toHaveBeenCalledWith([
      'Nostr relay query quiet window elapsed',
    ]);
    expect(pool.subscriptions[0]?.close).toHaveBeenCalledWith(
      'Nostr relay query quiet window elapsed',
    );
    expect(liveHandlers.onClose).not.toHaveBeenCalled();
    expect(pool.subscriptions[1]?.close).not.toHaveBeenCalled();
  });

  it('finishes a historical query on aggregate EOSE', () => {
    const pool = new MemorySimplePool();
    const transport = new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example'],
      pool,
    });
    const handlers: NostrRelayTransportHandlers = { onEvent: vi.fn(), onClose: vi.fn() };
    transport.subscribe(
      [{ kinds: [0] }, { kinds: [1] }],
      handlers,
      { closeOnEose: true },
    );

    pool.subscriptions[0]?.params.oneose?.();
    expect(handlers.onClose).not.toHaveBeenCalled();
    pool.subscriptions[1]?.params.oneose?.();

    expect(handlers.onClose).toHaveBeenCalledWith(['Nostr relay query reached EOSE']);
    expect(pool.subscriptions.every((subscription) => subscription.close.mock.calls.length === 1))
      .toBe(true);
  });

  it('verifies outbound events and succeeds when any relay accepts publication', async () => {
    const pool = new MemorySimplePool();
    pool.publish.mockReturnValueOnce([
      Promise.reject(new Error('offline')),
      Promise.resolve('accepted'),
    ]);
    const transport = new SimplePoolNostrRelayTransport({
      getRelays: () => ['wss://one.example', 'wss://two.example'],
      pool,
    });
    const event = note('publish', 5);

    await expect(transport.publish(event)).resolves.toBeUndefined();
    expect(pool.publish).toHaveBeenCalledWith(
      ['wss://one.example', 'wss://two.example'],
      expect.objectContaining({ id: event.id }),
      expect.objectContaining({ maxWait: 4_500 }),
    );
    await expect(transport.publish({ ...event, content: 'forged' } as NostrVerifiedEvent))
      .rejects.toThrow('invalid Nostr event id or signature');
    expect(pool.publish).toHaveBeenCalledTimes(1);
  });

  it('closes immediately when no relays are configured', () => {
    const transport = new SimplePoolNostrRelayTransport({ getRelays: () => [] });
    const handlers: NostrRelayTransportHandlers = { onEvent: vi.fn(), onClose: vi.fn() };

    const subscription = transport.subscribe([{ kinds: [1] }], handlers);

    expect(handlers.onClose).toHaveBeenCalledWith(['No Nostr relays configured']);
    expect(() => subscription.close()).not.toThrow();
  });
});

function note(content: string, createdAt: number): NostrVerifiedEvent {
  return finalizeEvent(
    { kind: content === 'profile' ? 0 : 1, created_at: createdAt, tags: [], content },
    generateSecretKey(),
  );
}
