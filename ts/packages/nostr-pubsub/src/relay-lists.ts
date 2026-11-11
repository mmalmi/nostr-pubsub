import type { NostrEvent } from './types.js';

/** NIP-65 relay list metadata. */
export const RELAY_LIST_KIND = 10002;
export const MAX_RELAY_URL_LENGTH = 256;

export type RelayListUse = 'read' | 'write';
export interface RelayListEntry { url: string; read: boolean; write: boolean; }
export interface RankedRelay {
  url: string;
  /** Distinct counted authors announcing this relay for the requested use. */
  authors: number;
}
export interface RelayRankOptions {
  /** Count only these authors, typically the caller's follows. Omit to count every list supplied. */
  authors?: Iterable<string>;
  /** `write` is where authors publish, `read` where they receive. Default `write`. */
  use?: RelayListUse;
  /** Omit relays announced by fewer counted authors. Default 3. */
  minAuthors?: number;
  /** Ignore lists naming more relays than this; they carry no routing signal. Default 10. */
  maxRelaysPerAuthor?: number;
  /** `popularity` orders by announcing authors. `coverage` orders by authors not yet
   * reached through an earlier relay and stops once nothing new is reached. Default `popularity`. */
  order?: 'popularity' | 'coverage';
  /** Relay URLs to leave out, such as those already configured. */
  exclude?: Iterable<string>;
  limit?: number;
}

/** Canonical public `wss:` relay URL, or undefined for anything a stranger should not make us dial. */
export function normalizeRelayUrl(value: unknown): string | undefined {
  if (typeof value !== 'string' || value.length > MAX_RELAY_URL_LENGTH) return undefined;
  let url: URL;
  try { url = new URL(value); } catch { return undefined; }
  if (url.protocol !== 'wss:' || url.username || url.password || url.href.includes('#')
    || !isPublicHost(url.hostname)) return undefined;
  const href = url.href.replace(/\/$/, '');
  return href.length > MAX_RELAY_URL_LENGTH ? undefined : href;
}

/** The valid relays one relay-list event announces, in list order. Pass verified events. */
export function relayListEntries(event: NostrEvent): RelayListEntry[] {
  if (event.kind !== RELAY_LIST_KIND) return [];
  const entries = new Map<string, RelayListEntry>();
  for (const [name, value, marker] of event.tags) {
    if (name !== 'r' || marker !== undefined && marker !== 'read' && marker !== 'write') continue;
    const url = normalizeRelayUrl(value);
    if (!url) continue;
    const entry = entries.get(url) ?? { url, read: false, write: false };
    entry.read ||= marker !== 'write';
    entry.write ||= marker !== 'read';
    entries.set(url, entry);
  }
  return [...entries.values()];
}

/**
 * Rank relays by how many of the given authors currently announce them. Only each
 * author's newest list counts. The result is a suggestion: nothing here connects
 * anywhere, and the caller decides whether to use any of it.
 */
export function rankRelays(events: Iterable<NostrEvent>, options: RelayRankOptions = {}): RankedRelay[] {
  const use = options.use ?? 'write';
  const minAuthors = bound(options.minAuthors ?? 3, 'minAuthors');
  const maxRelaysPerAuthor = bound(options.maxRelaysPerAuthor ?? 10, 'maxRelaysPerAuthor');
  const limit = options.limit === undefined ? Infinity : bound(options.limit, 'limit');
  const counted = options.authors && new Set(options.authors);
  const excluded = new Set([...options.exclude ?? []].map(url => normalizeRelayUrl(url) ?? url));

  const newest = new Map<string, NostrEvent>();
  for (const event of events) {
    if (event.kind !== RELAY_LIST_KIND || counted && !counted.has(event.pubkey)) continue;
    const current = newest.get(event.pubkey);
    if (!current || event.created_at > current.created_at
      || event.created_at === current.created_at && event.id < current.id) newest.set(event.pubkey, event);
  }

  const announcers = new Map<string, Set<string>>();
  for (const event of newest.values()) {
    const entries = relayListEntries(event);
    if (entries.length > maxRelaysPerAuthor) continue;
    for (const entry of entries) {
      if (!entry[use] || excluded.has(entry.url)) continue;
      const authors = announcers.get(entry.url);
      if (authors) authors.add(event.pubkey); else announcers.set(entry.url, new Set([event.pubkey]));
    }
  }

  const ranked = [...announcers].filter(([, authors]) => authors.size >= minAuthors)
    .sort(([a, x], [b, y]) => y.size - x.size || (a < b ? -1 : 1));
  if (options.order !== 'coverage') return ranked.slice(0, limit).map(([url, authors]) => ({ url, authors: authors.size }));

  const reached = new Set<string>();
  const cover: RankedRelay[] = [];
  while (cover.length < limit) {
    // `ranked` is already in popularity order, so the first best gain also wins ties.
    let best: [string, Set<string>] | undefined, gain = 0;
    for (const candidate of ranked) {
      let added = 0;
      for (const author of candidate[1]) if (!reached.has(author)) added++;
      if (added > gain) { best = candidate; gain = added; }
    }
    if (!best) break;
    for (const author of best[1]) reached.add(author);
    cover.push({ url: best[0], authors: best[1].size });
  }
  return cover;
}

function bound(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`Invalid ${name}`);
  return value;
}

// URL canonicalizes integer, hexadecimal and abbreviated IPv4 literals before this check.
function isPublicHost(host: string): boolean {
  if (host.startsWith('[')) {
    // Native global unicast only: mapped IPv4, ULA, link-local and multicast fall outside
    // 2000::/3. Protocol assignments (including Teredo), documentation and 6to4 are excluded.
    const [prefix, subnet] = host.slice(1, -1).split(':');
    const first = parseInt(prefix!, 16), second = parseInt(subnet || '0', 16);
    return first >= 0x2000 && first < 0x4000 && first !== 0x2002
      && !(first === 0x2001 && (second < 0x200 || second === 0xdb8))
      && !(first === 0x3fff && second < 0x1000);
  }
  const name = host.replace(/\.+$/, '');
  if (/^\d+\.\d+\.\d+\.\d+$/.test(name)) {
    const [a, b, c] = name.split('.').map(Number) as [number, number, number];
    return !(a === 0 || a === 10 || a === 127 || a >= 224
      || a === 100 && b >= 64 && b <= 127
      || a === 169 && b === 254
      || a === 172 && b >= 16 && b <= 31
      || a === 192 && b === 168
      || a === 192 && b === 0 && (c === 0 || c === 2)
      || a === 192 && b === 88 && c === 99
      || a === 198 && (b === 18 || b === 19 || b === 51 && c === 100)
      || a === 203 && b === 0 && c === 113);
  }
  return /^(?:[a-z0-9-]+\.)+[a-z0-9-]+$/.test(name)
    && !/(?:^|\.)(?:localhost|local|localdomain|lan|internal|home|corp|intranet|arpa)$/.test(name);
}
