import { describe, expect, it } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import {
  InMemoryEventBus,
  allowWithPriority,
  drop,
  localIndexRoute,
  localIndexSource,
  queryRoutesWithPolicy,
  verifyNostrEvent,
  withRouteDataset,
  type EventBus,
  type NostrEventReader,
  type NostrEventPublisher,
  type NostrVerifiedEvent,
  type PolicyDecision,
  type PubsubPolicy,
  type QueryOptions,
  type QueryReport,
} from '../src/index.js';

const allowAll = policy(() => allowWithPriority(0));

describe('routed Nostr event queries', () => {
  it('merges every additive dataset', async () => {
    const older = note(10, 'older');
    const newer = note(20, 'newer');
    const first = reader('first', [older]);
    const second = reader('second', [newer]);

    const report = await queryRoutesWithPolicy([
      routeSource('first', 'archive-a', first),
      routeSource('second', 'archive-b', second),
    ], [{}], {}, allowAll);

    expect(first.calls).toBe(1);
    expect(second.calls).toBe(1);
    expect(report.events.map(({ event }) => event.id)).toEqual([newer.id, older.id]);
    expect(report.datasets).toEqual([
      { datasetId: 'archive-a', complete: true, eventCount: 1 },
      { datasetId: 'archive-b', complete: true, eventCount: 1 },
    ]);
    expect(report.complete).toBe(true);
  });

  it('fails over between replicas without treating them as additive', async () => {
    const event = note(10, 'replica result');
    const failing = reader('primary', [], async () => {
      throw new Error('primary unavailable');
    });
    const replica = reader('replica', [event]);

    const report = await queryRoutesWithPolicy([
      routeSource('primary', 'shared-archive', failing, 20),
      routeSource('replica', 'shared-archive', replica, 10),
    ], [{}], {}, allowAll);

    expect(report.events.map(({ event: result }) => result.id)).toEqual([event.id]);
    expect(report.attempts.map(({ route, outcome }) => [route.id, outcome.type])).toEqual([
      ['primary', 'failure'],
      ['replica', 'success'],
    ]);
    expect(report.datasets).toEqual([
      { datasetId: 'shared-archive', complete: true, eventCount: 1 },
    ]);
  });

  it('continues to another replica after a partial response', async () => {
    const partialEvent = note(10, 'partial');
    const completeEvent = note(20, 'complete');
    const partial = reader('partial', [partialEvent], undefined, false);
    const complete = reader('complete', [partialEvent, completeEvent]);

    const report = await queryRoutesWithPolicy([
      routeSource('partial', 'shared', partial, 20),
      routeSource('complete', 'shared', complete, 10),
    ], [{}], {}, allowAll);

    expect(report.events.map(({ event }) => event.id)).toEqual([
      completeEvent.id,
      partialEvent.id,
    ]);
    expect(report.attempts.map(({ outcome }) => outcome.type)).toEqual(['partial', 'success']);
    expect(report.datasets[0]).toEqual({ datasetId: 'shared', complete: true, eventCount: 2 });
  });

  it('deduplicates verified event ids while retaining every provenance', async () => {
    const event = note(10, 'same signed event');
    const report = await queryRoutesWithPolicy([
      routeSource('cache', 'cache-dataset', reader('cache', [event, event])),
      routeSource('archive', 'archive-dataset', reader('archive', [event])),
    ], [{}], {}, allowAll);

    expect(report.events).toHaveLength(1);
    expect(report.events[0].source.id).toBe('cache');
    expect(report.events[0].provenance.map(({ routeId, datasetId }) => ({ routeId, datasetId })))
      .toEqual([
        { routeId: 'archive', datasetId: 'archive-dataset' },
        { routeId: 'cache', datasetId: 'cache-dataset' },
      ]);
  });

  it('accepts a valid event verified by a structurally compatible reader instance', async () => {
    const external = finalizeEvent({
      kind: 1,
      created_at: 10,
      tags: [],
      content: 'verified outside this package instance',
    }, generateSecretKey());
    const source = localIndexSource('external-reader');
    const externalReader: NostrEventReader = {
      query: async () => ({ events: [{ event: external, source, priority: 0 }] }),
    };

    const report = await queryRoutesWithPolicy([
      routeSource('external-reader', 'external', externalReader),
    ], [{}], {}, allowAll);

    expect(report.events.map(({ event }) => event.id)).toEqual([external.id]);
    expect(report.complete).toBe(true);
  });

  it('orders and globally limits merged results independent of completion order', async () => {
    const sameTimeA = note(30, 'same-a');
    const sameTimeB = note(30, 'same-b');
    const older = note(20, 'older');
    const expectedTieOrder = [sameTimeA, sameTimeB].sort((left, right) =>
      left.id < right.id ? -1 : 1,
    );

    const report = await queryRoutesWithPolicy([
      routeSource('slow', 'slow-set', delayedReader('slow', [sameTimeB], 30)),
      routeSource('fast', 'fast-set', delayedReader('fast', [older], 0)),
      routeSource('middle', 'middle-set', delayedReader('middle', [sameTimeA], 10)),
    ], [{}], { query: { limit: 2 } }, allowAll);

    expect(report.events.map(({ event }) => event.id)).toEqual(expectedTieOrder.map(({ id }) => id));
    expect(report.attempts).toHaveLength(3);
  });

  it('applies NIP-01 limits to each OR filter instead of taking their minimum', async () => {
    const bus = new InMemoryEventBus();
    const source = localIndexSource('nip01');
    const profile = note(30, 'profile', 0);
    const textNote = note(20, 'text note', 1);
    await bus.publish(profile, source);
    await bus.publish(textNote, source);

    const filters = [
      { kinds: [1], limit: 1 },
      { kinds: [0], limit: 1 },
    ];
    const direct = await bus.query(filters);
    expect(direct.events.map(({ event }) => event.id)).toEqual([profile.id, textNote.id]);

    const report = await queryRoutesWithPolicy([
      { route: localIndexRoute('nip01'), reader: bus },
    ], filters, {}, allowAll);

    expect(report.events.map(({ event }) => event.id)).toEqual([profile.id, textNote.id]);
  });

  it('isolates a dataset failure and reports incomplete coverage', async () => {
    const available = note(10, 'available');
    const report = await queryRoutesWithPolicy([
      routeSource('broken', 'broken-set', reader('broken', [], async () => {
        throw new TypeError('corrupt catalog');
      })),
      routeSource('healthy', 'healthy-set', reader('healthy', [available])),
    ], [{}], {}, allowAll);

    expect(report.events.map(({ event }) => event.id)).toEqual([available.id]);
    expect(report.complete).toBe(false);
    expect(report.datasets).toEqual([
      { datasetId: 'broken-set', complete: false, eventCount: 0 },
      { datasetId: 'healthy-set', complete: true, eventCount: 1 },
    ]);
    expect(report.attempts[0].outcome).toMatchObject({
      type: 'failure',
      error: { name: 'TypeError', message: 'corrupt catalog' },
    });
  });

  it('cancels in-flight readers and reports partial coverage', async () => {
    const controller = new AbortController();
    let observedSignal: AbortSignal | undefined;
    let markStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const hanging: NostrEventReader = {
      query: (_filters, options) => {
        observedSignal = options?.signal;
        markStarted?.();
        return new Promise(() => undefined);
      },
    };
    const query = queryRoutesWithPolicy([
      routeSource('hanging', 'remote', hanging),
    ], [{}], { query: { signal: controller.signal } }, allowAll);

    await started;
    controller.abort('view changed');
    const report = await query;

    expect(observedSignal?.aborted).toBe(true);
    expect(report.complete).toBe(false);
    expect(report.attempts[0].outcome.type).toBe('cancelled');
  });

  it('enforces a shared absolute deadline even when a reader ignores cancellation', async () => {
    const hanging = reader('hanging', [], () => new Promise(() => undefined));
    const started = Date.now();
    const report = await queryRoutesWithPolicy([
      routeSource('hanging', 'remote', hanging),
    ], [{}], { query: { deadline: Date.now() + 20 } }, allowAll);

    expect(Date.now() - started).toBeLessThan(500);
    expect(report.complete).toBe(false);
    expect(report.attempts[0].outcome.type).toBe('deadline-exceeded');
  });

  it.each([
    ['AbortError', 'cancelled'],
    ['TimeoutError', 'deadline-exceeded'],
  ] as const)('classifies a reader %s and still tries its replica', async (name, outcome) => {
    const event = note(10, `${name} fallback`);
    const unavailable = reader('unavailable', [], async () => {
      throw new DOMException(`${name} from reader`, name);
    });
    const replica = reader('replica', [event]);

    const report = await queryRoutesWithPolicy([
      routeSource('unavailable', 'shared', unavailable, 20),
      routeSource('replica', 'shared', replica, 10),
    ], [{}], {}, allowAll);

    expect(report.attempts.map(({ outcome: result }) => result.type)).toEqual([
      outcome,
      'success',
    ]);
    expect(report.events.map(({ event: result }) => result.id)).toEqual([event.id]);
  });

  it('rejects invalid global limits before policy or reader work', async () => {
    const eventReader = reader('never-called', []);
    let policyCalls = 0;
    const countingPolicy = policy(() => {
      policyCalls += 1;
      return allowWithPriority(0);
    });

    await expect(queryRoutesWithPolicy([
      routeSource('never-called', 'archive', eventReader),
    ], [{}], { query: { limit: -1 } }, countingPolicy)).rejects.toThrow(/non-negative/);
    await expect(new InMemoryEventBus().query([{}], { limit: 1.5 })).rejects.toThrow(/safe integer/);
    expect(policyCalls).toBe(0);
    expect(eventReader.calls).toBe(0);
  });

  it('enforces cancellation and deadline on direct in-memory reads', async () => {
    const bus = new InMemoryEventBus();
    const controller = new AbortController();
    controller.abort('caller stopped');

    await expect(bus.query([{}], { signal: controller.signal })).rejects.toMatchObject({
      name: 'AbortError',
    });
    await expect(bus.query([{}], { deadline: Date.now() - 1 })).rejects.toMatchObject({
      name: 'TimeoutError',
    });
  });

  it('applies hard-drop policy before invoking a reader', async () => {
    const forbidden = reader('relay', [note(10, 'must not escape')]);
    const report = await queryRoutesWithPolicy([
      routeSource('relay', 'relay-content', forbidden),
    ], [{}], {}, policy(() => drop('relay reads disabled')));

    expect(forbidden.calls).toBe(0);
    expect(report.events).toEqual([]);
    expect(report.attempts).toEqual([]);
    expect(report.complete).toBe(true);
  });

  it('composes the combined EventBus through the canonical reader route', async () => {
    const bus: EventBus = new InMemoryEventBus();
    const publisher: NostrEventPublisher = bus;
    const readerContract: NostrEventReader = bus;
    const event = note(10, 'combined bus');
    await publisher.publish(event, localIndexSource('combined'));

    expect((await readerContract.query([{}])).events).toHaveLength(1);
    const report = await queryRoutesWithPolicy([
      { route: localIndexRoute('combined-route'), reader: bus },
    ], [{}], {}, allowAll);
    expect(report.events.map(({ event: result }) => result.id)).toEqual([event.id]);
    expect(report.attempts[0].route.datasetId).toBe('default');
  });
});

function note(createdAt: number, content: string, kind = 1): NostrVerifiedEvent {
  return verifyNostrEvent(finalizeEvent({
    kind,
    created_at: createdAt,
    tags: [],
    content,
  }, generateSecretKey()));
}

function reader(
  id: string,
  events: NostrVerifiedEvent[],
  query?: (options?: QueryOptions) => Promise<QueryReport>,
  complete = true,
): NostrEventReader & { calls: number } {
  const source = localIndexSource(id);
  return {
    calls: 0,
    async query(_filters, options) {
      this.calls += 1;
      if (query !== undefined) return query(options);
      return {
        events: events.map((event) => ({ event, source, priority: 0 })),
        complete,
      };
    },
  };
}

function delayedReader(
  id: string,
  events: NostrVerifiedEvent[],
  delayMs: number,
): NostrEventReader & { calls: number } {
  const result = reader(id, events);
  const query = result.query.bind(result);
  result.query = async (filters, options) => {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    return query(filters, options);
  };
  return result;
}

function routeSource(
  id: string,
  datasetId: string,
  eventReader: NostrEventReader,
  priority = 0,
) {
  return {
    route: withRouteDataset({ ...localIndexRoute(id), priority }, datasetId),
    reader: eventReader,
  };
}

function policy(decide: (sourceId: string) => PolicyDecision): PubsubPolicy {
  return {
    checkEvent: () => allowWithPriority(0),
    checkSource: ({ candidate }) => decide(candidate.source.id),
  };
}
