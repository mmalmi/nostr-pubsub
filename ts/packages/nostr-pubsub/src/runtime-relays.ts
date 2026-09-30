import { AbstractSimplePool, type SubCloser } from 'nostr-tools/abstract-pool';
import { verifyEvent } from 'nostr-tools/pure';
import { matchFilters } from 'nostr-tools/filter';
import { createSimplePoolNostrRelayVerificationBoundary } from './simple-pool-relay-transport.js';
import { verifyNostrEvent, type NostrEvent, type NostrFilter, type NostrVerifiedEvent } from './types.js';
import type { NostrRuntimeOptions, RuntimeRelayStats } from './runtime-types.js';

type Entry = { relays?: readonly string[]; filters: NostrFilter[]; event: (event: NostrEvent, relay: string) => void; state: (relay: string, complete: boolean, error?: string) => void; closed: boolean; batch?: Batch };
type Link = { generation: number; close?: () => void; timer?: ReturnType<typeof setTimeout>; attempts: number; eosed: boolean; latest: Map<string, number> };
type Batch = { entries: Entry[]; links: Map<string, Link>; closed: boolean; reopenTimer?: ReturnType<typeof setTimeout> };

/** Batches OR filters without merging their fields, preserving recipient/author intersections. */
export class RuntimeRelays {
  private readonly pool: AbstractSimplePool;
  private readonly boundary: ReturnType<typeof createSimplePoolNostrRelayVerificationBoundary>;
  private relays: string[];
  private readonly batches = new Set<Batch>();
  private pending: Entry[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private stopped = false;
  private serial = 0;
  private readonly maxFilters: number;
  private readonly maxBytes: number;
  constructor(private readonly options: NostrRuntimeOptions) {
    this.boundary = createSimplePoolNostrRelayVerificationBoundary(boundedVerifier(options.verifyEvent ?? verifyEvent));
    this.relays = normalizeRelays(options.relays ?? []);
    this.maxFilters = positive(options.maxFiltersPerBatch, 20);
    this.maxBytes = positive(options.maxFilterBytesPerBatch, 32768);
    this.pool = new AbstractSimplePool({
      maxWaitForConnection: options.historyTimeoutMs ?? 3000,
      websocketImplementation: options.websocketImplementation,
      verifyEvent: (event): event is NostrVerifiedEvent => this.boundary.verifyEvent(event),
      // Own reconnect so replay overlaps the last timestamp instead of skipping its second.
      enableReconnect: false,
      automaticallyAuth: options.signAuthEvent ? (url) => async (event) => {
        const signed = await options.signAuthEvent!(url, event);
        if (!this.boundary.verifyEvent(signed)) throw new Error('Invalid relay authentication event');
        return this.boundary.admitEvent(signed);
      } : undefined,
    });
  }
  urls(): string[] { return [...this.relays]; }
  stats(): RuntimeRelayStats[] {
    const statuses = this.pool.listConnectionStatus();
    return this.relays.map((url) => ({ url, connected: statuses.get(url) ?? false }));
  }
  count(): number { return [...this.batches].reduce((total, batch) => total + batch.links.size, 0); }
  subscribe(filters: NostrFilter[], event: Entry['event'], state: Entry['state'], relays?: readonly string[]): SubCloser {
    if (this.stopped) throw new Error('Nostr runtime is closed');
    if (filters.length > this.maxFilters || bytes(filters) > this.maxBytes) throw new RangeError('Nostr subscription exceeds filter batch bounds');
    const entry: Entry = { relays: relays === undefined ? undefined : normalizeRelays(relays), filters: structuredClone(filters), event, state, closed: false };
    this.pending.push(entry);
    if (!this.timer) this.timer = setTimeout(() => this.flush(), this.options.batchWindowMs ?? 10);
    return { close: () => {
      if (entry.closed) return;
      entry.closed = true;
      const batch = entry.batch;
      if (!batch) return;
      if (batch.entries.every((entry) => entry.closed)) { this.closeBatch(batch); return; }
      // UI unsubscriptions arrive as separate worker tasks. A microtask cannot
      // coalesce them, and reopening after each removal creates a REQ storm.
      if (!batch.reopenTimer) batch.reopenTimer = setTimeout(() => {
        batch.reopenTimer = undefined;
        if (!batch.closed) this.reopen(batch);
      }, this.options.batchWindowMs ?? 10);
    } };
  }
  setRelays(urls: readonly string[]): void {
    const next = normalizeRelays(urls);
    const removed = this.relays.filter((url) => !next.includes(url));
    this.relays = next;
    for (const batch of this.batches) if (batch.entries[0]?.relays === undefined) this.reopen(batch);
    const scoped = new Set([...this.batches].flatMap((batch) => [...(batch.entries[0]?.relays ?? [])]));
    this.pool.close(removed.filter((url) => !scoped.has(url)));
  }
  publishAttempts(event: NostrEvent, urls?: readonly string[]): Array<{ id: string; result: Promise<void> }> {
    return (urls === undefined ? this.relays : normalizeRelays(urls)).map((url) => ({
      id: url,
      result: this.publishOne(url, event),
    }));
  }
  private async publishOne(url: string, event: NostrEvent): Promise<void> {
    const timeout = this.options.publishTimeoutMs ?? 5000;
    const relay = await this.pool.ensureRelay(url, { connectionTimeout: timeout });
    relay.publishTimeout = timeout;
    try { await relay.publish(event); }
    catch (error) {
      if (!errorText(error).startsWith('auth-required:') || !this.options.signAuthEvent) throw error;
      await relay.auth(async (template) => verifyNostrEvent(await this.options.signAuthEvent!(url, template)));
      await relay.publish(event);
    }
  }
  close(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.pending = [];
    for (const batch of [...this.batches]) this.closeBatch(batch);
    this.pool.destroy();
  }
  private flush(): void {
    this.timer = undefined;
    const entries = this.pending.filter((entry) => !entry.closed);
    this.pending = [];
    let group: Entry[] = [];
    for (const entry of entries) {
      const candidate = uniqueFilters([...group, entry]);
      if (group.length && (JSON.stringify(group[0]!.relays) !== JSON.stringify(entry.relays) || candidate.length > this.maxFilters || bytes(candidate) > this.maxBytes)) {
        this.startBatch(group); group = [];
      }
      group.push(entry);
    }
    if (group.length) this.startBatch(group);
  }
  private startBatch(entries: Entry[]): void {
    const batch: Batch = { entries, links: new Map(), closed: false };
    for (const entry of entries) entry.batch = batch;
    this.batches.add(batch);
    this.reopen(batch);
  }
  private reopen(batch: Batch): void {
    if (batch.reopenTimer) clearTimeout(batch.reopenTimer);
    batch.reopenTimer = undefined;
    for (const link of batch.links.values()) this.closeLink(link);
    batch.links.clear();
    batch.entries = batch.entries.filter((entry) => !entry.closed);
    if (!batch.entries.length) { this.closeBatch(batch); return; }
    for (const url of batch.entries[0]!.relays ?? this.relays) {
      const link: Link = { generation: 0, attempts: 0, eosed: false, latest: new Map() };
      batch.links.set(url, link);
      void this.connect(batch, url, link);
    }
  }
  private async connect(batch: Batch, url: string, link: Link): Promise<void> {
    const generation = ++link.generation;
    const current = (): boolean => !this.stopped && !batch.closed && generation === link.generation;
    try {
      const relay = await this.pool.ensureRelay(url);
      if (!current()) return;
      const original = uniqueFilters(batch.entries);
      const filters = original.map((filter) => {
        const latest = link.latest.get(filterKey(filter));
        return link.eosed && latest !== undefined
          ? { ...filter, since: Math.max(filter.since ?? 0, latest - 1) }
          : structuredClone(filter);
      });
      // prepareSubscription is public. Sending REQ ourselves avoids nostr-tools' timer
      // which labels a timeout as EOSE; only a relay's actual EOSE is complete here.
      const subscription = relay.prepareSubscription(filters, {
        id: `pubsub-${++this.serial}`,
        onevent: (raw) => {
          if (!current()) return;
          const event = this.boundary.admitEvent(raw);
          for (const filter of original) if (matchFilters([filter], event)) {
            const key = filterKey(filter);
            link.latest.set(key, Math.max(link.latest.get(key) ?? 0, event.created_at));
          }
          for (const entry of batch.entries) if (!entry.closed && matchFilters(entry.filters, event)) entry.event(event, url);
        },
        oneose: () => {
          if (!current()) return;
          link.eosed = true; link.attempts = 0;
          for (const entry of batch.entries) if (!entry.closed) entry.state(url, true);
        },
        onclose: (reason) => { if (current()) this.retry(batch, url, link, reason); },
      });
      relay.ongoingOperations++;
      relay.idleSince = undefined;
      link.close = () => { if (relay.openSubs.has(subscription.id)) subscription.close('Nostr runtime subscription closed'); };
      await relay.send(JSON.stringify(['REQ', subscription.id, ...filters]));
    } catch (error) { if (current()) this.retry(batch, url, link, errorText(error)); }
  }
  private retry(batch: Batch, url: string, link: Link, reason: string): void {
    this.closeLink(link);
    for (const entry of batch.entries) if (!entry.closed) entry.state(url, false, reason);
    const delay = Math.min(30000, (this.options.reconnectDelayMs ?? 500) * 2 ** Math.min(link.attempts++, 6));
    link.timer = setTimeout(() => { link.timer = undefined; void this.connect(batch, url, link); }, delay);
  }
  private closeLink(link: Link): void {
    link.generation++;
    if (link.timer) clearTimeout(link.timer);
    link.timer = undefined;
    const close = link.close; link.close = undefined; close?.();
  }
  private closeBatch(batch: Batch): void {
    batch.closed = true;
    if (batch.reopenTimer) clearTimeout(batch.reopenTimer);
    batch.reopenTimer = undefined;
    for (const link of batch.links.values()) this.closeLink(link);
    batch.links.clear(); this.batches.delete(batch);
  }
}
function uniqueFilters(entries: Entry[]): NostrFilter[] {
  const filters = new Map<string, NostrFilter>();
  for (const entry of entries) if (!entry.closed) for (const filter of entry.filters) filters.set(filterKey(filter), filter);
  return [...filters.values()];
}
export function filterKey(filter: NostrFilter): string {
  return JSON.stringify(Object.fromEntries(Object.entries(filter).filter(([, value]) => value !== undefined).sort(([a], [b]) => a.localeCompare(b))));
}
function bytes(filters: NostrFilter[]): number { return new TextEncoder().encode(JSON.stringify(filters)).byteLength; }
function positive(value: number | undefined, fallback: number): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < 1) throw new RangeError('Invalid runtime bounds');
  return result;
}
function normalizeRelays(urls: readonly string[]): string[] {
  return [...new Set(urls.map((url) => {
    const parsed = new URL(url);
    if (parsed.protocol !== 'ws:' && parsed.protocol !== 'wss:') throw new Error('Nostr relay must use ws or wss');
    return parsed.toString();
  }))];
}
function errorText(error: unknown): string { return error instanceof Error ? error.message : String(error); }

/** Bound duplicate verification work without trusting event IDs or public verified markers. */
function boundedVerifier(verify: (event: NostrEvent) => boolean): (event: NostrEvent) => boolean {
  const known = new Map<string, string>();
  let retainedBytes = 0;
  return (event) => {
    const key = JSON.stringify([event.id, event.pubkey, event.sig, event.kind, event.created_at, event.tags, event.content]);
    if (known.get(event.id) === key) return true;
    if (!verify(event)) return false;
    const previous = known.get(event.id);
    if (previous) retainedBytes -= previous.length * 2;
    known.delete(event.id); known.set(event.id, key); retainedBytes += key.length * 2;
    while (known.size > 2048 || retainedBytes > 8 * 1024 * 1024) {
      const id = known.keys().next().value!;
      retainedBytes -= known.get(id)!.length * 2; known.delete(id);
    }
    return true;
  };
}
