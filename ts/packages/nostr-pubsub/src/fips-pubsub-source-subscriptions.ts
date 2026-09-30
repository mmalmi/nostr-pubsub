import { matchFilters } from 'nostr-tools/filter';
import type { NostrEventSubscription, QueryEvent } from './event-bus.js';
import type { FipsNostrPubsubClient } from './fips-pubsub-client.js';
import { fipsEndpointSource, SOURCE_PRIORITY_FIPS_ENDPOINT } from './source.js';
import type { NostrFilter, NostrVerifiedEvent } from './types.js';
import { FipsPubsubWireCodec } from './wire.js';

type Handler = (event: QueryEvent) => void;
interface Interest {
  key: string; filters: NostrFilter[]; handlers: Set<Handler>; recent: Map<string, QueryEvent>;
  ready: Promise<void>; resolve(): void; reject(error: unknown): void; batch: Batch;
}
interface Batch {
  interests: Set<Interest>; subscription?: NostrEventSubscription; timer?: ReturnType<typeof setTimeout>;
}

// A timer also coalesces separate worker MessageEvents, unlike a microtask.
const BATCH_WINDOW_MS = 8;
const RESERVED_SUBSCRIPTION_ID = 'ts-1y2p0ij32e8e7';

/** Exact OR batching. Author/recipient fields are never unioned across filters. */
export class FipsSourceSubscriptions {
  private readonly interests = new Map<string, Interest>();
  private readonly batches = new Set<Batch>();
  private readonly codec: FipsPubsubWireCodec;
  private readonly maxFilters: number;

  constructor(private readonly client: FipsNostrPubsubClient) {
    this.codec = new FipsPubsubWireCodec(client.limits.maxFrameBytes);
    this.maxFilters = client.limits.maxFiltersPerSubscription;
  }

  async subscribe(filters: NostrFilter[], handler: Handler): Promise<NostrEventSubscription> {
    // Validate and detach caller-owned arrays before scheduling any wire work.
    const normalized = this.normalize(filters);
    const key = JSON.stringify(normalized.map(filter => Object.fromEntries(Object.entries(filter).sort(([a], [b]) => a.localeCompare(b)))));
    let interest = this.interests.get(key);
    if (!interest) {
      const batch = [...this.batches].find(candidate => this.fits([...this.filters(candidate), ...normalized]));
      if (!batch && this.batches.size >= this.client.limits.maxActiveSubscriptions) {
        throw new Error('FIPS source subscription batch limit reached');
      }
      const selected = batch ?? { interests: new Set<Interest>() };
      let resolve!: () => void; let reject!: (error: unknown) => void;
      const ready = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
      interest = { key, filters: normalized, handlers: new Set(), recent: new Map(), ready, resolve, reject, batch: selected };
      this.interests.set(key, interest); selected.interests.add(interest); this.batches.add(selected);
      this.schedule(selected);
    }
    const entry = interest;
    const receive: Handler = incoming => handler(incoming);
    entry.handlers.add(receive);
    let closed = false;
    const close = (): void => {
      if (closed) return;
      closed = true; entry.handlers.delete(receive);
      if (!entry.handlers.size) {
        if (this.interests.get(entry.key) === entry) this.interests.delete(entry.key);
        entry.batch.interests.delete(entry);
        if (!entry.batch.interests.size) this.retire(entry.batch);
        else this.schedule(entry.batch);
      }
    };
    try {
      // Replay is scoped per logical interest so a busy batch cannot evict a
      // different author's last event before its history query attaches.
      for (const incoming of entry.recent.values()) handler(incoming);
      await entry.ready;
      return { close };
    } catch (error) { close(); throw error; }
  }

  private normalize(filters: NostrFilter[]): NostrFilter[] {
    if (!filters.length || filters.length > this.maxFilters) {
      throw new Error(`FIPS source requires 1..${this.maxFilters} filters per interest`);
    }
    const decoded = this.codec.decodeFrame(this.codec.encodeFrame({ type: 'req', subscriptionId: RESERVED_SUBSCRIPTION_ID, filters }));
    if (decoded.type !== 'req') throw new Error('Invalid FIPS source filters');
    return decoded.filters;
  }
  private filters(batch: Batch): NostrFilter[] { return [...batch.interests].flatMap(entry => entry.filters); }
  private fits(filters: NostrFilter[]): boolean {
    if (filters.length > this.maxFilters) return false;
    try { this.codec.encodeFrame({ type: 'req', subscriptionId: RESERVED_SUBSCRIPTION_ID, filters }); return true; }
    catch { return false; }
  }
  private schedule(batch: Batch): void {
    batch.timer ??= setTimeout(() => { batch.timer = undefined; this.flush(batch); }, BATCH_WINDOW_MS);
  }
  private retire(batch: Batch): void {
    if (batch.timer !== undefined) clearTimeout(batch.timer);
    batch.timer = undefined; batch.subscription?.close(); batch.subscription = undefined;
    this.batches.delete(batch);
  }
  private flush(batch: Batch): void {
    const entries = [...batch.interests];
    if (!entries.length) { this.retire(batch); return; }
    try {
      // Retire before replacing to respect the carrier's hard subscription cap.
      batch.subscription?.close();
      batch.subscription = this.client.subscribe(entries.flatMap(entry => entry.filters), (event, peerId) => this.deliver(batch, event, peerId));
      for (const entry of entries) entry.resolve();
    } catch (error) {
      batch.interests.clear();
      this.retire(batch);
      for (const entry of entries) { this.interests.delete(entry.key); entry.reject(error); }
    }
  }
  private deliver(batch: Batch, event: NostrVerifiedEvent, peerId: string): void {
    const incoming = { event, source: fipsEndpointSource(peerId), priority: SOURCE_PRIORITY_FIPS_ENDPOINT };
    let failure: unknown;
    for (const entry of batch.interests) {
      if (!entry.handlers.size || entry.recent.has(event.id) || !matchFilters(entry.filters, event)) continue;
      entry.recent.set(event.id, incoming);
      if (entry.recent.size > this.client.limits.maxReplayEvents) entry.recent.delete(entry.recent.keys().next().value!);
      for (const receive of [...entry.handlers]) { try { receive(incoming); } catch (error) { failure = error; } }
    }
    if (failure) throw failure;
  }
}
