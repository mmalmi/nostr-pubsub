import { matchFilter } from 'nostr-tools/filter';
import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';
import type { RuntimeEventStore, RuntimeOutboxEntry } from './runtime-types.js';

const BLOCK_TARGET = 256;
const BLOCK_SPLIT = 512;
type StoredEntry = { readonly event: NostrEvent; readonly expiresAt: number | undefined };

export function selectStoredEvents(events: Iterable<NostrEvent>, filters: NostrFilter[], options: QueryOptions = {}): NostrEvent[] {
  const nowSeconds = Date.now() / 1000;
  const ordered = [...events].filter((event) => !expired(event, nowSeconds)).sort(eventOrder);
  const result = new Map<string, NostrEvent>();
  for (const filter of filters) {
    let count = 0;
    for (const event of ordered) {
      if (options.signal?.aborted) throw abortError(options.signal.reason);
      if (filter.limit !== undefined && count >= filter.limit) break;
      if (matchFilter(filter, event)) { result.set(event.id, event); count++; }
    }
  }
  const values = [...result.values()];
  if (filters.length > 1 && values.length > 1) values.sort(eventOrder);
  return options.limit === undefined ? values : values.slice(0, options.limit);
}
export function eventOrder(a: NostrEvent, b: NostrEvent): number {
  return b.created_at - a.created_at || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
}
function parseExpiration(event: NostrEvent): number | undefined {
  const value = event.tags.find((tag) => tag[0] === 'expiration')?.[1];
  return value !== undefined && /^\d+$/.test(value) ? Number(value) : undefined;
}
export function expired(event: NostrEvent, nowSeconds = Date.now() / 1000): boolean {
  const at = parseExpiration(event);
  return at !== undefined && at <= nowSeconds;
}
export function abortError(reason?: unknown): DOMException {
  return new DOMException(typeof reason === 'string' ? reason : 'Nostr operation cancelled', 'AbortError');
}
function replacementIdentifier(event: NostrEvent): string | undefined {
  if (event.kind === 0 || event.kind === 3 || (event.kind >= 10000 && event.kind < 20000)) return '';
  if (event.kind >= 30000 && event.kind < 40000) return event.tags.find(tag => tag[0] === 'd')?.[1] ?? '';
}
export function replacementFilter(event: NostrEvent): NostrFilter | undefined {
  const identifier = replacementIdentifier(event);
  // Missing and empty d tags share an address but only the latter matches a #d filter.
  if (identifier !== undefined) return { authors: [event.pubkey], kinds: [event.kind], ...(identifier ? { '#d': [identifier] } : {}) };
}
export function eventAddress(event: NostrEvent): string | undefined {
  const identifier = replacementIdentifier(event);
  if (identifier !== undefined) return `${event.kind}:${event.pubkey}:${identifier}`;
}
function deletedBy(event: NostrEvent, deletion: NostrEvent): boolean {
  if (deletion.kind !== 5 || deletion.pubkey !== event.pubkey || deletion.created_at < event.created_at || event.kind === 5) return false;
  const key = eventAddress(event);
  return deletion.tags.some((tag) => (tag[0] === 'e' && tag[1] === event.id) || (tag[0] === 'a' && key !== undefined && tag[1] === key));
}
/** Called serially by a runtime after signature admission. Deletion events are retained as tombstones. */
export async function storeRuntimeEvent(store: RuntimeEventStore, event: NostrEvent): Promise<boolean> {
  return await admitRuntimeEvent(store, event) === 'admitted';
}
/** Preserve rejection reasons so historical callers can opt into superseded versions only. */
export async function admitRuntimeEvent(store: RuntimeEventStore, event: NostrEvent): Promise<'admitted' | 'superseded' | 'rejected'> {
  if (expired(event)) return 'rejected';
  if (event.kind >= 20000 && event.kind < 30000) return 'admitted';
  const key = eventAddress(event);
  const ownDeletions = await store.query([{ authors: [event.pubkey], kinds: [5], '#e': [event.id] },
    ...(key ? [{ authors: [event.pubkey], kinds: [5], '#a': [key] }] : [])]);
  if (ownDeletions.some((deletion) => deletedBy(event, deletion))) return 'rejected';
  const replacement = replacementFilter(event);
  if (replacement) {
    const previous = (await store.query([replacement])).filter((old) => eventAddress(old) === key);
    // Older stores may already hold multiple winners split by incidental d tags.
    const winner = previous.reduce((best, old) => eventOrder(old, best) < 0 ? old : best, event);
    const stale = previous.filter(old => old.id !== winner.id).map(old => old.id);
    if (stale.length) await store.delete(stale);
    if (winner.id !== event.id) return 'superseded';
  }
  if (event.kind === 5) {
    const filters: NostrFilter[] = [];
    const ids = event.tags.filter((tag) => tag[0] === 'e' && tag[1]).map((tag) => tag[1]!);
    if (ids.length) filters.push({ ids, authors: [event.pubkey] });
    for (const tag of event.tags) {
      if (tag[0] !== 'a' || !tag[1]) continue;
      const [kind, author, ...identifier] = tag[1].split(':');
      if (author !== event.pubkey || !/^\d+$/.test(kind!)) continue;
      filters.push({ kinds: [Number(kind)], authors: [author], ...(Number(kind) >= 30000 && identifier.join(':') ? { '#d': [identifier.join(':')] } : {}) });
    }
    const previous = filters.length ? await store.query(filters) : [];
    await store.delete(previous.filter((old) => deletedBy(old, event)).map((old) => old.id));
  }
  await store.put(event);
  return 'admitted';
}

function lowerBound(length: number, before: (index: number) => boolean): number {
  let low = 0, high = length;
  while (low < high) { const mid = (low + high) >>> 1; if (before(mid)) low = mid + 1; else high = mid; }
  return low;
}
function indexKeys(event: NostrEvent): [string, string, string] {
  return [`k:${event.kind}`, `a:${event.pubkey}`, `ak:${event.pubkey}:${event.kind}`];
}
function unique<T>(values: readonly T[]): T[] { return values.length <= 1 ? [...values] : [...new Set(values)]; }
/** Bounded default cache. Supply a persistent store for offline operation across reloads. */
export class MemoryEventStore implements RuntimeEventStore {
  private readonly events = new Map<string, StoredEntry>();
  private readonly index = new Map<string, Set<StoredEntry>>();
  /** Non-empty blocks kept in `eventOrder` so live inserts and evictions avoid full-store shifts. */
  private blocks: StoredEntry[][] = [];
  private readonly pending = new Map<string, RuntimeOutboxEntry>();
  private pendingScopeCount = 0;
  constructor(readonly maxEvents = 20000, readonly maxPending = 1000) {
    if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || !Number.isSafeInteger(maxPending) || maxPending < 1) throw new RangeError('Invalid event store bounds');
  }
  async query(filters: NostrFilter[], options: QueryOptions = {}): Promise<NostrEvent[]> {
    const nowSeconds = Date.now() / 1000;
    const result = new Map<string, NostrEvent>();
    for (const filter of filters) this.queryFilter(filter, nowSeconds, options, result);
    const values = [...result.values()];
    if (filters.length > 1 && values.length > 1) values.sort(eventOrder);
    return options.limit === undefined ? values : values.slice(0, options.limit);
  }
  async put(event: NostrEvent): Promise<void> {
    const previous = this.events.get(event.id);
    if (!previous && this.events.size >= this.maxEvents) {
      const victim = this.oldestEvictable();
      if (!victim) throw new Error('Event store retention capacity exhausted');
      this.remove(victim);
    }
    if (previous) this.remove(previous);
    const stored = structuredClone(event);
    const entry: StoredEntry = { event: stored, expiresAt: parseExpiration(stored) };
    this.events.set(stored.id, entry);
    for (const key of indexKeys(stored)) {
      let bucket = this.index.get(key);
      if (!bucket) { bucket = new Set(); this.index.set(key, bucket); }
      bucket.add(entry);
    }
    this.insertBlock(entry);
  }
  async delete(ids: string[]): Promise<void> {
    for (const id of ids) { const entry = this.events.get(id); if (entry) this.remove(entry); }
  }
  private queryFilter(filter: NostrFilter, nowSeconds: number, options: QueryOptions, result: Map<string, NostrEvent>): void {
    if (options.signal?.aborted) throw abortError(options.signal.reason);
    const limit = filter.limit;
    if ((limit !== undefined && limit <= 0) || filter.ids?.length === 0 || filter.kinds?.length === 0 || filter.authors?.length === 0) return;
    const indexed = this.selectCandidates(filter);
    if (indexed !== undefined) {
      if (indexed.count === 0) return;
      if (filter.ids || indexed.count <= BLOCK_SPLIT || (limit === undefined && indexed.count < (this.events.size >>> 1))) {
        const matched: NostrEvent[] = [];
        for (const bucket of indexed.buckets) for (const entry of bucket) {
          if (options.signal?.aborted) throw abortError(options.signal.reason);
          if ((entry.expiresAt === undefined || entry.expiresAt > nowSeconds) && matchFilter(filter, entry.event)) matched.push(entry.event);
        }
        if (matched.length > 1) matched.sort(eventOrder);
        for (const event of limit === undefined ? matched : matched.slice(0, limit)) result.set(event.id, event);
        return;
      }
    }
    const hasUntil = Boolean(filter.until), hasSince = Boolean(filter.since);
    const startBlock = hasUntil ? lowerBound(this.blocks.length, (i) => this.blocks[i]![this.blocks[i]!.length - 1]!.event.created_at > filter.until!) : 0;
    let count = 0;
    for (let blockIndex = startBlock; blockIndex < this.blocks.length; blockIndex++) {
      const block = this.blocks[blockIndex]!;
      const start = blockIndex === startBlock && hasUntil ? lowerBound(block.length, (i) => block[i]!.event.created_at > filter.until!) : 0;
      for (let index = start; index < block.length; index++) {
        if (options.signal?.aborted) throw abortError(options.signal.reason);
        const entry = block[index]!;
        if (hasSince && entry.event.created_at < filter.since!) return;
        if ((entry.expiresAt === undefined || entry.expiresAt > nowSeconds) && matchFilter(filter, entry.event)) {
          result.set(entry.event.id, entry.event);
          if (limit !== undefined && ++count >= limit) return;
        }
      }
    }
  }
  private selectCandidates(filter: NostrFilter): { count: number; buckets: Iterable<StoredEntry>[] } | undefined {
    if (filter.ids) {
      const entries: StoredEntry[] = [];
      for (const id of unique(filter.ids)) { const entry = this.events.get(id); if (entry) entries.push(entry); }
      return { count: entries.length, buckets: [entries] };
    }
    const authors = filter.authors ? unique(filter.authors) : undefined;
    const kinds = filter.kinds ? unique(filter.kinds) : undefined;
    const plans: string[][] = [];
    if (authors && kinds && authors.length * kinds.length <= 256) plans.push(authors.flatMap((a) => kinds.map((k) => `ak:${a}:${k}`)));
    else {
      if (authors && authors.length <= 256) plans.push(authors.map((a) => `a:${a}`));
      if (kinds && kinds.length <= 32) plans.push(kinds.map((k) => `k:${k}`));
    }
    let best: { count: number; buckets: Set<StoredEntry>[] } | undefined;
    for (const keys of plans) {
      let count = 0;
      const buckets: Set<StoredEntry>[] = [];
      for (const key of keys) { const bucket = this.index.get(key); if (bucket) { count += bucket.size; buckets.push(bucket); } }
      if (!best || count < best.count) best = { count, buckets };
    }
    return best;
  }
  private oldestEvictable(): StoredEntry | undefined {
    for (let blockIndex = this.blocks.length - 1; blockIndex >= 0; blockIndex--) {
      const block = this.blocks[blockIndex]!;
      for (let index = block.length - 1; index >= 0; index--) {
        const entry = block[index]!;
        if (entry.event.kind !== 5 && !this.pending.has(entry.event.id)) return entry;
      }
    }
  }
  private remove(entry: StoredEntry): void {
    this.events.delete(entry.event.id);
    for (const key of indexKeys(entry.event)) {
      const bucket = this.index.get(key);
      if (bucket && bucket.delete(entry) && !bucket.size) this.index.delete(key);
    }
    if (!this.blocks.length) return;
    const blockIndex = this.findBlock(entry.event);
    const block = this.blocks[blockIndex]!;
    const index = lowerBound(block.length, (i) => eventOrder(block[i]!.event, entry.event) < 0);
    if (block[index] === entry) { block.splice(index, 1); if (!block.length) this.blocks.splice(blockIndex, 1); return; }
    const entries = [...this.events.values()].sort((a, b) => eventOrder(a.event, b.event));
    this.blocks = [];
    for (let offset = 0; offset < entries.length; offset += BLOCK_TARGET) this.blocks.push(entries.slice(offset, offset + BLOCK_TARGET));
  }
  private findBlock(event: NostrEvent): number {
    return lowerBound(this.blocks.length - 1, (i) => eventOrder(this.blocks[i]![this.blocks[i]!.length - 1]!.event, event) < 0);
  }
  private insertBlock(entry: StoredEntry): void {
    if (!this.blocks.length) { this.blocks.push([entry]); return; }
    const blockIndex = this.findBlock(entry.event);
    const block = this.blocks[blockIndex]!;
    block.splice(lowerBound(block.length, (i) => eventOrder(block[i]!.event, entry.event) <= 0), 0, entry);
    if (block.length >= BLOCK_SPLIT) this.blocks.splice(blockIndex + 1, 0, block.splice(BLOCK_TARGET));
  }
  async updatePending(id: string, update: (row: RuntimeOutboxEntry | undefined) => RuntimeOutboxEntry | undefined): Promise<void> {
    const current = structuredClone(this.pending.get(id));
    const next = update(current);
    if (next === current) return;
    if (next) await this.putPending(next);
    else await this.deletePending(id);
  }
  async listPending(): Promise<RuntimeOutboxEntry[]> { return [...this.pending.values()].map((entry) => structuredClone(entry)); }
  async putPending(entry: RuntimeOutboxEntry): Promise<void> {
    const previous = this.pending.get(entry.event.id);
    const count = this.pendingScopeCount + pendingCount(entry) - pendingCount(previous);
    if (count > this.maxPending) throw new Error('Outbox capacity exhausted');
    this.pending.set(entry.event.id, structuredClone(entry));
    this.pendingScopeCount = count;
  }
  async deletePending(id: string): Promise<void> {
    this.pendingScopeCount -= pendingCount(this.pending.get(id));
    this.pending.delete(id);
  }
}

function pendingCount(entry: RuntimeOutboxEntry | undefined): number {
  return entry ? 1 + (entry.additionalScopes?.length ?? 0) : 0;
}
