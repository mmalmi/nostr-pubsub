import { describe, expect, it } from 'vitest';
import { MemoryEventStore, selectStoredEvents, type NostrEvent, type NostrFilter } from '../src/index.js';
import { eventOrder } from '../src/runtime-store.js';
import { compareUtf8 } from '../src/utf8-order.js';

/** Deterministic pseudo-random integers below `bound`. */
function sequence(seed: number): (bound: number) => number {
  let state = seed;
  return (bound) => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state % bound;
  };
}

function stored(index: number, kind: number, time: number, expiration?: number): NostrEvent {
  return {
    id: index.toString(16).padStart(64, '0'),
    pubkey: (index % 3).toString(16).padStart(64, 'a'),
    kind,
    created_at: time,
    content: `event ${index}`,
    tags: expiration === undefined ? [] : [['expiration', String(expiration)]],
    sig: '0'.repeat(128),
  };
}

describe('MemoryEventStore', () => {
  it('answers every query as a freshly sorted scan would across writes, rewrites, deletes and eviction', async () => {
    const next = sequence(7);
    const capacity = 12;
    const store = new MemoryEventStore(capacity);
    // The reference keeps the previous implementation: an insertion-ordered map sorted on demand.
    const reference = new Map<string, NostrEvent>();
    const pending = new Set<string>();
    const filters: NostrFilter[][] = [
      [{}],
      [{ kinds: [1], limit: 3 }],
      [{ kinds: [5] }, { authors: ['a'.repeat(63) + '1'], limit: 2 }],
      [{ since: 3, until: 6 }],
    ];

    for (let step = 0; step < 600; step++) {
      const index = next(40);
      const action = next(10);
      if (action < 6) {
        // Few distinct timestamps force ties; a rewrite may move an existing ID to another timestamp.
        const event = stored(index, next(6) === 0 ? 5 : 1, next(8), next(9) === 0 ? 1 : undefined);
        if (!reference.has(event.id) && reference.size >= capacity) {
          const removable = [...reference.values()].filter((e) => e.kind !== 5 && !pending.has(e.id)).sort(eventOrder).pop();
          if (removable) reference.delete(removable.id);
        }
        if (reference.has(event.id) || reference.size < capacity) {
          reference.set(event.id, event);
          await store.put(event);
        } else {
          await expect(store.put(event)).rejects.toThrow('capacity exhausted');
        }
      } else if (action < 8) {
        const ids = [stored(index, 1, 0).id, stored(next(40), 1, 0).id];
        for (const id of ids) reference.delete(id);
        await store.delete(ids);
      } else if (reference.has(stored(index, 1, 0).id) && pending.size < 4) {
        const event = reference.get(stored(index, 1, 0).id)!;
        pending.add(event.id);
        await store.putPending({ event, attempts: 0, updatedAt: 0 });
      }
      for (const filter of filters) {
        expect(await store.query(filter), `step ${step}`).toEqual(selectStoredEvents(reference.values(), filter));
      }
      expect(await store.query([{}], { limit: 2 })).toEqual(selectStoredEvents(reference.values(), [{}], { limit: 2 }));
    }
  });

  it('matches a reference scan across multi-block splits, indexed lookups and edge filters', async () => {
    const next = sequence(42);
    const capacity = 600;
    const store = new MemoryEventStore(capacity);
    const reference = new Map<string, NostrEvent>();
    const authors = Array.from({ length: 12 }, (_, i) => i.toString(16).padStart(64, 'b'));
    const mk = (index: number, kind: number, time: number, expiration?: number): NostrEvent => ({
      id: index.toString(16).padStart(64, '0'),
      pubkey: authors[index % authors.length]!,
      kind,
      created_at: time,
      content: `block-${index}`,
      tags: [...(expiration !== undefined ? [['expiration', String(expiration)]] : []), ['p', authors[(index + 1) % authors.length]!]],
      sig: '0'.repeat(128),
    });

    for (let step = 0; step < 900; step++) {
      const index = next(800);
      const kind = [0, 1, 1, 1, 5, 30078][next(6)]!;
      const ev = mk(index, kind, next(40), next(15) === 0 ? 1 : undefined);
      if (!reference.has(ev.id) && reference.size >= capacity) {
        const removable = [...reference.values()].filter((e) => e.kind !== 5).sort(eventOrder).pop();
        if (removable) reference.delete(removable.id);
      }
      reference.set(ev.id, ev);
      await store.put(ev);
      if (step % 7 === 0) {
        const dropId = mk(next(800), 1, 0).id;
        reference.delete(dropId);
        await store.delete([dropId]);
      }
    }

    const ids = [...reference.keys()].slice(0, 6);
    const queries: NostrFilter[][] = [
      [{ ids: [ids[0]!, ids[0]!, ids[2]!, 'f'.repeat(64)] }],
      [{ ids: [ids[0]!, ids[1]!, ids[2]!], limit: 2 }],
      [{ ids: [] }],
      [{ kinds: [] }],
      [{ authors: [] }],
      [{ kinds: [1], limit: 0 }],
      [{ kinds: [0], authors: authors.slice(0, 4) }],
      [{ authors: [authors[0]!, authors[0]!, authors[1]!], kinds: [1], limit: 15 }],
      [{ authors: [authors[2]!] }],
      [{ kinds: [5] }],
      [{ kinds: [1], since: 0, until: 0 }],
      [{ kinds: [1], since: 10, until: 25, limit: 30 }],
      [{ kinds: [1], '#p': [authors[3]!], limit: 10 }],
      [{ kinds: [0], authors: [authors[0]!] }, { kinds: [30078], limit: 5 }],
    ];
    for (const filters of queries) {
      expect(await store.query(filters), JSON.stringify(filters)).toEqual(selectStoredEvents(reference.values(), filters));
    }
    expect(await store.query([{ kinds: [1] }], { limit: 25 })).toEqual(selectStoredEvents(reference.values(), [{ kinds: [1] }], { limit: 25 }));
  });
});

describe('compareUtf8', () => {
  it('orders strings exactly as their UTF-8 bytes do, including surrogates', () => {
    const encoder = new TextEncoder();
    const byBytes = (left: string, right: string): number => {
      const a = encoder.encode(left);
      const b = encoder.encode(right);
      for (let index = 0; index < Math.min(a.length, b.length); index++) {
        if (a[index] !== b[index]) return a[index]! - b[index]!;
      }
      return a.length - b.length;
    };
    const units = ['a', 'b', '#', 'é', '߿', 'ࠀ', '퟿', '', '￿', '😀', '𐀀', '\ud83d', '\ude00'];
    const next = sequence(11);
    const strings = Array.from({ length: 300 }, () =>
      Array.from({ length: next(5) }, () => units[next(units.length)]!).join(''));
    for (const left of strings) {
      for (const right of strings) {
        expect(Math.sign(compareUtf8(left, right)), JSON.stringify([left, right])).toBe(Math.sign(byBytes(left, right)));
      }
    }
  });
});
