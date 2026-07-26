import { describe, expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey, verifiedSymbol } from 'nostr-tools/pure';
import {
  NostrPubsubRouter,
  allowWithPriority,
  localIndexRoute,
  localIndexSource,
  verifyNostrEvent,
  verifyNostrEventsWith,
  type NostrEvent,
  type NostrEventBatchVerifier,
  type NostrVerifiedEvent,
  type PubsubPolicy,
} from '../src/index.js';

const allowAll: PubsubPolicy = {
  checkEvent: () => allowWithPriority(0),
  checkSource: () => allowWithPriority(0),
};

describe('external Nostr event verification', () => {
  it('passes immutable defensive clones and admits fresh canonical copies', async () => {
    const event = signedEvent('original');
    const controller = new AbortController();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let verifierEvents: readonly NostrEvent[] | undefined;

    const verification = verifyNostrEventsWith([event], async (events, options) => {
      verifierEvents = events;
      expect(options.signal).toBe(controller.signal);
      expect(events).not.toBe(event);
      expect(events[0]).not.toBe(event);
      expect(events[0][verifiedSymbol]).toBeUndefined();
      expect(Object.isFrozen(events)).toBe(true);
      expect(Object.isFrozen(events[0])).toBe(true);
      expect(Object.isFrozen(events[0].tags)).toBe(true);
      expect(Object.isFrozen(events[0].tags[0])).toBe(true);
      await gate;
      return [true];
    }, { signal: controller.signal });

    event.content = 'mutated after verifier dispatch';
    event.tags[0].push('mutated');
    release();
    const [verified] = await verification;

    expect(verified.content).toBe('original');
    expect(verified.tags).toEqual([['t', 'test']]);
    expect(verified).not.toBe(verifierEvents?.[0]);
    expect(verified[verifiedSymbol]).toBe(true);
    expect(Object.isFrozen(verified)).toBe(true);
    expect(Object.isFrozen(verified.tags)).toBe(true);
    expect(Object.isFrozen(verified.tags[0])).toBe(true);
  });

  it('fully validates event structure before invoking the verifier', async () => {
    const valid = signedEvent('valid');
    const malformed: NostrEvent[] = [
      null as unknown as NostrEvent,
      { ...valid, id: 'not-an-id' },
      { ...valid, pubkey: valid.pubkey.toUpperCase() },
      { ...valid, sig: valid.sig.slice(2) },
      { ...valid, content: 1 as unknown as string },
      { ...valid, created_at: -1 },
      { ...valid, created_at: 1.5 },
      { ...valid, kind: 65_536 },
      { ...valid, tags: [['ok', 1 as unknown as string]] },
    ];
    const verifier = vi.fn(async () => [true]);

    for (const event of malformed) {
      await expect(verifyNostrEventsWith([event], verifier)).rejects.toMatchObject({
        name: 'PubsubError',
        kind: 'validation',
      });
    }
    expect(verifier).not.toHaveBeenCalled();
  });

  it.each([
    ['non-array', { 0: true, length: 1 }],
    ['short', []],
    ['long', [true, true]],
    ['non-boolean', [1]],
  ])('rejects a malformed %s verifier result', async (_name, result) => {
    await expect(verifyNostrEventsWith(
      [signedEvent('result')],
      async () => result as unknown as readonly boolean[],
    )).rejects.toMatchObject({
      name: 'PubsubError',
      kind: 'validation',
      message: 'external Nostr event verifier returned malformed results',
    });
  });

  it('rejects the whole batch when any signature is invalid', async () => {
    const first = forgedEvent('first');
    const second = forgedEvent('second');
    let verifierEvents: readonly NostrEvent[] = [];

    await expect(verifyNostrEventsWith([first, second], async (events) => {
      verifierEvents = events;
      return [true, false];
    })).rejects.toMatchObject({
      name: 'PubsubError',
      kind: 'validation',
      message: 'invalid Nostr event id or signature at batch index 1',
    });

    const report = await routerFor(verifierEvents[0] as NostrVerifiedEvent).query([{}]);
    expect(report.events).toEqual([]);
  });

  it('lets the router copy canonical worker-admitted events without local crypto', async () => {
    const forged = forgedEvent('trusted external verifier');
    expect(() => verifyNostrEvent(forged)).toThrow('invalid Nostr event id or signature');

    const [admitted] = await verifyNostrEventsWith([forged], async () => [true]);
    const report = await routerFor(admitted).query([{}]);

    expect(report.events.map(({ event }) => event.content)).toEqual([
      'trusted external verifier (forged)',
    ]);
    expect(report.events[0].event).not.toBe(admitted);
    expect(report.events[0].event[verifiedSymbol]).toBe(true);
    expect(Object.isFrozen(report.events[0].event)).toBe(true);
  });

  it('reuses only private canonical admission across protocol layers', async () => {
    const forged = forgedEvent('trusted protocol handoff');
    const spoofed = {
      ...forged,
      [verifiedSymbol]: true,
    } as NostrVerifiedEvent;
    expect(() => verifyNostrEvent(spoofed)).toThrow('invalid Nostr event id or signature');

    const [admitted] = await verifyNostrEventsWith([forged], async () => [true]);
    const copied = verifyNostrEvent(admitted);

    expect(copied).not.toBe(admitted);
    expect(copied.content).toBe('trusted protocol handoff (forged)');
    expect(copied[verifiedSymbol]).toBe(true);
    expect(Object.isFrozen(copied)).toBe(true);
  });

  it('cancels in-flight verification without admitting late results', async () => {
    const controller = new AbortController();
    const forged = forgedEvent('cancelled');
    let verifierEvent: NostrEvent | undefined;
    let finish!: (result: readonly boolean[]) => void;
    const verifier: NostrEventBatchVerifier = (events, options) => {
      verifierEvent = events[0];
      expect(options.signal).toBe(controller.signal);
      return new Promise((resolve) => {
        finish = resolve;
      });
    };

    const verification = verifyNostrEventsWith([forged], verifier, {
      signal: controller.signal,
    });
    controller.abort('superseded');
    await expect(verification).rejects.toMatchObject({
      name: 'AbortError',
      message: 'superseded',
    });
    finish([true]);
    await Promise.resolve();

    const report = await routerFor(verifierEvent as NostrVerifiedEvent).query([{}]);
    expect(report.events).toEqual([]);
  });

  it('does not invoke the verifier when already cancelled', async () => {
    const controller = new AbortController();
    controller.abort();
    const verifier = vi.fn(async () => [true]);

    await expect(verifyNostrEventsWith(
      [signedEvent('pre-cancelled')],
      verifier,
      { signal: controller.signal },
    )).rejects.toMatchObject({ name: 'AbortError' });
    expect(verifier).not.toHaveBeenCalled();
  });
});

function signedEvent(content: string): NostrEvent {
  return finalizeEvent({
    kind: 1,
    created_at: 1,
    tags: [['t', 'test']],
    content,
  }, generateSecretKey());
}

function forgedEvent(content: string): NostrEvent {
  return { ...signedEvent(content), content: `${content} (forged)` };
}

function routerFor(event: NostrVerifiedEvent): NostrPubsubRouter {
  const source = localIndexSource('external-worker');
  return new NostrPubsubRouter({
    policy: allowAll,
    querySources: [{
      route: localIndexRoute(source.id),
      reader: {
        query: async () => ({
          events: [{ event, source, priority: 0 }],
        }),
      },
    }],
  });
}
