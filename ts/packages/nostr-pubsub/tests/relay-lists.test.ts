import { describe, expect, it } from 'vitest';
import { normalizeRelayUrl, rankRelays, relayListEntries, type NostrEvent } from '../src/index.js';

let serial = 0;
const key = (name: string) => name.repeat(64);
function list(author: string, relays: Array<string | [string, string]>, created_at = 1_700_000_000): NostrEvent {
  const tags = relays.map(relay => typeof relay === 'string' ? ['r', relay] : ['r', ...relay]);
  return { kind: 10002, pubkey: key(author), created_at, tags, content: '', sig: '',
    id: (++serial).toString(16).padStart(64, '0') };
}

describe('relay list parsing', () => {
  it('canonicalizes public secure relay URLs', () => {
    expect(normalizeRelayUrl('WSS://Relay.Example.com:443/')).toBe('wss://relay.example.com');
    expect(normalizeRelayUrl('wss://relay.example.com/inbox/')).toBe('wss://relay.example.com/inbox');
    expect(normalizeRelayUrl('wss://relay.example.com:8443')).toBe('wss://relay.example.com:8443');
  });

  it.each([
    'ws://relay.example.com', 'https://relay.example.com', 'wss://user:pw@relay.example.com',
    'wss://relay.example.com/#x', 'wss://localhost', 'wss://printer.local', 'wss://router.lan',
    'wss://127.0.0.1', 'wss://10.1.2.3', 'wss://192.168.1.1', 'wss://172.16.0.1', 'wss://169.254.1.1',
    'wss://100.64.0.1', 'wss://2130706433', 'wss://0x7f.1', 'wss://[::1]', 'wss://[fe80::1]',
    'wss://[fd00::1]', 'wss://[::ffff:10.0.0.1]', 'wss://singlelabel', `wss://${'a'.repeat(260)}.com`,
    'not a url', '', undefined, 7,
  ])('rejects %s', value => { expect(normalizeRelayUrl(value)).toBeUndefined(); });

  it('reads markers, merges duplicates and ignores other kinds and tags', () => {
    const event = list('a', ['wss://both.example.com', ['wss://in.example.com', 'read'],
      ['wss://out.example.com', 'write'], ['wss://out.example.com/', 'read'],
      ['wss://odd.example.com', 'archive'], 'wss://192.168.0.2']);
    event.tags.push(['p', 'wss://tagged.example.com']);
    expect(relayListEntries(event)).toEqual([
      { url: 'wss://both.example.com', read: true, write: true },
      { url: 'wss://in.example.com', read: true, write: false },
      { url: 'wss://out.example.com', read: true, write: true },
    ]);
    expect(relayListEntries({ ...event, kind: 3 })).toEqual([]);
  });
});

describe('relay ranking', () => {
  const big = 'wss://big.example.com', mid = 'wss://mid.example.com', small = 'wss://small.example.com';

  it('orders by distinct announcing authors and applies the author threshold', () => {
    const events = [list('a', [big, mid]), list('b', [big, mid]), list('c', [big, small]), list('d', [big])];
    expect(rankRelays(events, { minAuthors: 1 })).toEqual([
      { url: big, authors: 4 }, { url: mid, authors: 2 }, { url: small, authors: 1 }]);
    expect(rankRelays(events, { minAuthors: 2 })).toEqual([{ url: big, authors: 4 }, { url: mid, authors: 2 }]);
    expect(rankRelays(events)).toEqual([{ url: big, authors: 4 }]);
    expect(rankRelays(events, { minAuthors: 1, limit: 1 })).toEqual([{ url: big, authors: 4 }]);
    expect(rankRelays(events, { minAuthors: 1, exclude: ['wss://BIG.example.com/'] }).map(relay => relay.url))
      .toEqual([mid, small]);
  });

  it('counts only the requested authors and each author once', () => {
    const events = [list('a', [big, `${big}/`]), list('b', [big]), list('z', [big, mid])];
    expect(rankRelays(events, { minAuthors: 1, authors: [key('a'), key('b')] })).toEqual([{ url: big, authors: 2 }]);
  });

  it('uses only the newest list per author', () => {
    const events = [list('a', [mid], 200), list('a', [big], 100), list('a', [small], 150)];
    expect(rankRelays(events, { minAuthors: 1 })).toEqual([{ url: mid, authors: 1 }]);
    const tied = [list('b', [big], 300), list('b', [small], 300)];
    expect(rankRelays(tied, { minAuthors: 1 })).toEqual([{ url: big, authors: 1 }]);
    expect(rankRelays(tied.reverse(), { minAuthors: 1 })).toEqual([{ url: big, authors: 1 }]);
  });

  it('separates where authors publish from where they receive', () => {
    const events = [list('a', [[big, 'write'], [mid, 'read']]), list('b', [[big, 'read'], small])];
    expect(rankRelays(events, { minAuthors: 1 }).map(relay => relay.url)).toEqual([big, small]);
    expect(rankRelays(events, { minAuthors: 1, use: 'read' }).map(relay => relay.url)).toEqual([big, mid, small]);
  });

  it('ignores oversized lists', () => {
    const spam = Array.from({ length: 11 }, (_, index) => `wss://r${index}.example.com`);
    const events = [list('a', spam), list('b', [big])];
    expect(rankRelays(events, { minAuthors: 1 })).toEqual([{ url: big, authors: 1 }]);
    expect(rankRelays(events, { minAuthors: 1, maxRelaysPerAuthor: 11 })).toHaveLength(12);
  });

  it('orders by newly reached authors and stops when nothing new is reached', () => {
    // big reaches a-d; mid duplicates a,b; small adds e,f.
    const events = [list('a', [big, mid]), list('b', [big, mid]), list('c', [big, mid]), list('d', [big]),
      list('e', [small]), list('f', [small])];
    expect(rankRelays(events, { minAuthors: 2 }).map(relay => relay.url)).toEqual([big, mid, small]);
    expect(rankRelays(events, { minAuthors: 2, order: 'coverage' })).toEqual([
      { url: big, authors: 4 }, { url: small, authors: 2 }]);
    expect(rankRelays(events, { minAuthors: 2, order: 'coverage', limit: 1 })).toEqual([{ url: big, authors: 4 }]);
  });

  it('rejects invalid bounds', () => {
    for (const options of [{ minAuthors: 0 }, { maxRelaysPerAuthor: 1.5 }, { limit: 0 }])
      expect(() => rankRelays([], options)).toThrow(RangeError);
  });
});
