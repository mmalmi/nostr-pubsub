import { pendingPublications, publicationOptions, updatePublicationScope } from './runtime-outbox.js';
import { RelayRateLimitError } from './relay-rate-limit.js';
import { matchFilters } from 'nostr-tools/filter';
import { verifyEvent } from 'nostr-tools/pure';
import { verifyNostrEvent, validateQueryOptions, type NostrEvent, type NostrEventVerifier, type NostrFilter, type NostrVerifiedEvent, type QueryOptions } from './types.js';
import { localIndexSource } from './source.js';
import { MemoryEventStore, abortError, expired, selectStoredEvents, admitRuntimeEvent } from './runtime-store.js';
import { RuntimeInterests } from './runtime-interests.js';
import { RuntimeRelays, boundedVerifier } from './runtime-relays.js';
import type {
  NostrRuntimeOptions, RuntimeCompletion, RuntimeEventInfo, RuntimeEventStore, RuntimeMetrics,
  RuntimePublishOptions, RuntimePublishResult, RuntimeQueryOptions, RuntimeQueryResult, RuntimeSource,
  RuntimeSubscribeOptions, RuntimeSubscription, RuntimeSubscriptionHandlers,
} from './runtime-types.js';

const MAX_TIMER_MS = 2_147_483_647;

type Listener = {
  filters: NostrFilter[]; handlers: RuntimeSubscriptionHandlers; options: RuntimeSubscribeOptions;
  seen: Set<string>; closers: Map<string, () => void>; statuses: Map<string, { complete: boolean; error?: string }>;
  pending: Set<string>; timer?: ReturnType<typeof setTimeout>; settleTimer?: ReturnType<typeof setTimeout>;
  stopped: boolean; notified: boolean; remainingIds?: Set<string>; replayed: boolean; settled?: boolean;
  scopedRelays?: readonly string[];
  controller: AbortController; abort?: () => void; admissionError?: string;
};

/** One signer-neutral owner of relay connections, live interests, local events and an outbox. */
export class NostrRuntime {
  readonly store: RuntimeEventStore;
  private readonly verifier: NostrEventVerifier;
  private readonly relays: RuntimeRelays;
  private readonly sources = new Map<string, RuntimeSource>();
  private readonly listeners = new Set<Listener>();
  private readonly interests = new RuntimeInterests<Listener>();
  private readonly incoming = new Map<string, { operation: Promise<Awaited<ReturnType<typeof admitRuntimeEvent>>>; sources: Set<string> }>();
  private readonly publishing = new Map<string, { result: Promise<RuntimePublishResult>; delivery: Promise<RuntimePublishResult> }>();
  private writes: Promise<unknown> = Promise.resolve();
  private stopped = false;
  private retryTimer?: ReturnType<typeof setTimeout>;
  private retrying = false;
  private readonly counters = { receivedEvents: 0, deliveredEvents: 0, duplicateEvents: 0, verificationFailures: 0 };
  constructor(private readonly options: NostrRuntimeOptions = {}) {
    this.verifier = boundedVerifier(options.verifyEvent ?? verifyEvent);
    this.store = options.store ?? new MemoryEventStore();
    this.relays = new RuntimeRelays({ ...options, store: this.store }, this.verifier);
    for (const source of options.sources ?? []) this.sources.set(source.id, source);
    this.scheduleRetry();
  }
  subscribe(filters: NostrFilter[], handlers: RuntimeSubscriptionHandlers, options: RuntimeSubscribeOptions = {}): RuntimeSubscription {
    this.ensureOpen();
    if (!filters.length) throw new RangeError('Nostr subscriptions require at least one filter');
    if (options.includeSuperseded && options.cache !== 'network-only') throw new Error('Historical replaceable versions require network-only queries');
    this.ensureSubscriptionCapacity();
    validateQueryOptions({ deadline: options.deadline });
    const listener: Listener = {
      filters: structuredClone(filters), handlers, options, seen: new Set(), closers: new Map(),
      statuses: new Map(), pending: new Set(), controller: new AbortController(), stopped: false, notified: false, replayed: false,
    };
    if (options.settle?.ids && filters.every(filter => filter.ids?.length)) {
      listener.remainingIds = new Set(filters.flatMap(filter => filter.ids!));
    }
    this.listeners.add(listener);
    this.interests.add(listener, listener.filters);
    const close = (): void => this.stopListener(listener);
    listener.abort = close;
    options.signal?.addEventListener('abort', close, { once: true });
    if (options.signal?.aborted) { close(); return { close }; }
    void this.startListener(listener).then(() => {
      listener.replayed = true;
      return this.maybeComplete(listener);
    }).catch((error) => {
      this.report(listener, error);
      this.complete(listener, 'unavailable');
    });
    return { close };
  }
  async query(filters: NostrFilter[], options: RuntimeQueryOptions = {}): Promise<RuntimeQueryResult> {
    validateQueryOptions(options);
    if (options.signal?.aborted) throw abortError(options.signal.reason);
    if (options.deadline !== undefined && options.deadline <= Date.now()) return { events: [], complete: false, reason: 'timeout', sources: [] };
    return new Promise((resolve, reject) => {
      const events = new Map<string, NostrEvent>();
      let subscription: RuntimeSubscription | undefined;
      const cancel = (): void => { subscription?.close(); reject(abortError(options.signal?.reason)); };
      options.signal?.addEventListener('abort', cancel, { once: true });
      subscription = this.subscribe(filters, {
        onEvent: (event) => events.set(event.id, event),
        onEose: (completion) => {
          subscription?.close();
          options.signal?.removeEventListener('abort', cancel);
          resolve({ ...completion, events: selectStoredEvents(events.values(), filters, options) });
        },
        onError: (error) => this.options.onError?.(error),
      }, options);
    });
  }
  /**
   * Read a bounded page (1–256 events) from one explicit relay on the shared socket.
   * Checks signatures, filters and expiration without local replay, writes or reconciliation.
   * This request sends no AUTH. If the socket is already authenticated, its existing
   * connection identity still applies. Use an anonymous runtime when required.
   */
  async queryTransientRelay(relay: string, filters: NostrFilter[], options: QueryOptions = {}): Promise<RuntimeQueryResult> {
    this.ensureOpen();
    this.ensureSubscriptionCapacity();
    return await this.relays.queryTransient(relay, filters, options);
  }
  async ingest(raw: NostrEvent, source = 'external'): Promise<boolean> {
    this.ensureOpen();
    return this.admit(this.verify(raw), source);
  }
  private async admit(event: NostrVerifiedEvent, source: string, target?: Listener): Promise<boolean> {
    this.ensureOpen();
    let pending = this.incoming.get(event.id);
    const owner = !pending;
    if (!pending) {
      const operation = this.writes.then(() => admitRuntimeEvent(this.store, event)).catch((error) => {
        // Shared admission failure makes every matching history incomplete.
        for (const listener of this.interests.candidates(event)) {
          if (!listener.stopped && matchFilters(listener.filters, event)) listener.admissionError = asError(error).message;
        }
        throw error;
      });
      this.writes = operation.catch(() => undefined);
      pending = { operation, sources: new Set() };
      this.incoming.set(event.id, pending);
    }
    const newSource = !pending.sources.has(source);
    pending.sources.add(source);
    try {
      const admission = await pending.operation;
      if (admission !== 'rejected' && !this.stopped) for (const listener of this.interests.candidates(event)) {
        if (listener.options.isolated ? listener !== target && source !== 'local-publish' : !newSource) continue;
        if (admission === 'admitted' || listener.options.includeSuperseded) this.deliver(listener, event, { source, cached: false });
      }
      return admission === 'admitted';
    } finally { if (owner) this.incoming.delete(event.id); }
  }

  publish(event: NostrEvent, options: RuntimePublishOptions = {}): Promise<RuntimePublishResult> {
    return this.startPublish(event, publicationOptions(options)).result;
  }
  private startPublish(event: NostrEvent, options: RuntimePublishOptions) {
    this.ensureOpen();
    const key = JSON.stringify([event.id, options]);
    const existing = this.publishing.get(key);
    if (existing) return existing;
    let resolve!: (result: RuntimePublishResult) => void;
    let reject!: (error: unknown) => void;
    const result = new Promise<RuntimePublishResult>((res, rej) => { resolve = res; reject = rej; });
    let queued = false;
    const delivery = this.publishOnce(event, options, (report) => { queued = true; resolve(report); });
    void delivery.then(resolve, (error) => {
      reject(error);
      if (queued) { try { this.options.onError?.(asError(error)); } catch { /* preserve delivery cleanup */ } }
    }).finally(() => this.publishing.delete(key));
    const publication = { result, delivery };
    this.publishing.set(key, publication);
    return publication;
  }
  async retryPending(): Promise<void> {
    if (this.stopped || this.retrying) return;
    this.retrying = true;
    try {
      const pending = pendingPublications(await this.store.listPending());
      for (let offset = 0; offset < pending.length && !this.stopped; offset += 4) {
        await Promise.all(pending.slice(offset, offset + 4).map(({ event, relays, sources }) => {
          const { result, delivery } = this.startPublish(event, { relays, sources });
          return Promise.all([result, delivery]).catch((error) => this.options.onError?.(asError(error)));
        }));
      }
    } finally { this.retrying = false; }
  }
  setRelays(urls: readonly string[]): void {
    this.ensureOpen(); this.relays.setRelays(urls);
    for (const listener of this.listeners) if (listener.options.cache !== 'cache-only' && listener.options.relays === undefined) {
      for (const id of [...listener.statuses.keys()]) if (!this.sources.has(id)) listener.statuses.delete(id);
      for (const id of [...listener.pending]) if (!this.sources.has(id)) listener.pending.delete(id);
      for (const id of this.relays.urls()) listener.pending.add(id);
    }
    void this.retryPending().catch((error) => this.options.onError?.(asError(error)));
  }
  getRelayStats() { return this.relays.stats(); }
  metrics(): RuntimeMetrics { return { ...this.counters, subscriptions: this.listeners.size + this.relays.transientCount(), relaySubscriptions: this.relays.count() }; }
  addSource(source: RuntimeSource): void {
    this.ensureOpen(); this.removeSource(source.id); this.sources.set(source.id, source);
    for (const listener of this.listeners) void this.attachSource(listener, source);
    void this.retryPending().catch((error) => this.options.onError?.(asError(error)));
  }
  removeSource(id: string): void {
    if (!this.sources.delete(id)) return;
    for (const listener of this.listeners) {
      listener.closers.get(id)?.(); listener.closers.delete(id); listener.statuses.delete(id);
      if (listener.pending.delete(id)) queueMicrotask(() => { void this.maybeComplete(listener); });
    }
  }
  async close(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    for (const listener of [...this.listeners]) { this.complete(listener, 'closed'); this.stopListener(listener); }
    this.relays.close();
    await this.writes;
    await Promise.allSettled([...this.publishing.values()].map(({ delivery }) => delivery));
    await this.store.close?.();
  }
  private async startListener(listener: Listener): Promise<void> {
    const timeout = Math.min(MAX_TIMER_MS, Math.max(0, (listener.options.deadline ?? Date.now() + (this.options.historyTimeoutMs ?? 10000)) - Date.now()));
    listener.timer = setTimeout(() => this.complete(listener, 'timeout'), timeout);
    // Open live routes before cache replay so an event cannot fall between the two.
    if (listener.options.cache !== 'cache-only') {
      if (listener.options.relays?.length !== 0) {
        for (const url of this.relayScope(listener)) listener.pending.add(url);
        const subscription = this.relays.subscribe(listener.filters, (event, source) => this.receive(listener, event, source),
          (source, complete, error) => this.sourceState(listener, source, complete, error),
          { relays: listener.options.relays, isolated: listener.options.isolated });
        listener.closers.set('relay-transport', () => subscription.close());
      }
      for (const source of this.sources.values()) void this.attachSource(listener, source);
    }
    if (listener.options.cache !== 'network-only') {
      const events = await this.store.query(listener.filters, { signal: listener.controller.signal, deadline: listener.options.deadline });
      for (const raw of events) {
        if (listener.stopped) return;
        try { this.deliver(listener, verifyNostrEvent(raw, this.verifier), { source: 'local-cache', cached: true }); }
        catch (error) { this.report(listener, error); }
      }
    }
    if (listener.options.cache === 'cache-only') this.complete(listener, 'cache');
  }
  private verify(raw: NostrEvent): NostrVerifiedEvent {
    try { return verifyNostrEvent(raw, this.verifier); }
    catch (error) { this.counters.verificationFailures++; throw error; }
  }
  /** Return a write only when history must wait for admission. */
  private readEvent(listener: Listener, raw: NostrEvent, source: string, durable = false): Promise<boolean> | undefined {
    const event = this.verify(raw);
    if (!durable && listener.options.persist !== 'background') return this.admit(event, source, listener);
    this.deliver(listener, event, { source, cached: false });
    if (!durable && !this.incoming.get(event.id)?.sources.has(source)) {
      void this.admit(event, source).catch(error => this.report(listener, error));
    }
  }
  private wantsSource(listener: Listener, source: RuntimeSource): boolean {
    const { cache, relays, sources } = listener.options;
    const selected = sources ? sources.includes(source.id) : relays === undefined;
    if (!selected || cache === 'cache-only') return false;
    try { return source.supports?.(listener.filters) ?? true; } catch { return false; }
  }
  private async attachSource(listener: Listener, source: RuntimeSource): Promise<void> {
    if (!this.wantsSource(listener, source)) return;
    listener.pending.add(source.id);
    const current = () => !listener.stopped && this.sources.get(source.id) === source;
    try {
      if (source.subscribe) {
        const subscription = await source.subscribe(listener.filters, ({ event }) => {
          if (current()) this.receive(listener, event, source.id, source.durable);
        });
        if (!current()) { subscription.close(); return; }
        listener.closers.set(source.id, () => subscription.close());
      }
      if (!source.query) return this.sourceState(listener, source.id, false, 'Source has no historical completion');
      const report = await source.query(listener.filters, {
        deadline: listener.options.deadline ?? Date.now() + (this.options.historyTimeoutMs ?? 10000), signal: listener.controller.signal,
      });
      let failure: Error | undefined;
      for (const { event } of report.events) {
        if (!current()) return;
        try {
          const write = this.readEvent(listener, event, source.id, source.durable);
          if (write) await write;
        } catch (error) { failure = asError(error); this.report(listener, failure); }
      }
      if (current()) this.sourceState(listener, source.id, report.complete !== false && !failure, failure?.message);
    } catch (error) {
      if (current()) {
        this.sourceState(listener, source.id, false, asError(error).message); this.report(listener, error);
      }
    }
  }
  private receive(listener: Listener, event: NostrEvent, source: string, durable = false): void {
    if (listener.stopped) return;
    this.counters.receivedEvents++;
    const key = listener.options.isolated ? `${source}:${event.id}` : event.id;
    const sharesWrite = !durable && !listener.options.isolated && listener.options.persist !== 'background';
    if (listener.seen.has(key) || (sharesWrite && this.incoming.get(event.id)?.sources.has(source))) {
      this.counters.duplicateEvents++;
      return;
    }
    try { this.readEvent(listener, event, source, durable)?.catch(error => this.report(listener, error)); }
    catch (error) { this.report(listener, error); }
  }
  private deliver(listener: Listener, event: NostrEvent, info: RuntimeEventInfo): void {
    if (listener.stopped || expired(event) || !matchFilters(listener.filters, event) || !this.acceptsOrigin(listener, info)) return;
    const key = listener.options.isolated ? `${info.source}:${event.id}` : event.id;
    if (listener.seen.has(key)) { this.counters.duplicateEvents++; return; }
    listener.seen.add(key);
    if (listener.seen.size > (this.options.maxSeenEventsPerSubscription ?? 4096)) listener.seen.delete(listener.seen.values().next().value!);
    this.counters.deliveredEvents++;
    try { listener.handlers.onEvent(event, info); } catch (error) { this.report(listener, error); }
    if (!listener.notified && listener.remainingIds?.delete(event.id) && !listener.remainingIds.size) {
      // A source can replay synchronously inside subscribe(), before the caller has its closer.
      queueMicrotask(() => this.complete(listener, 'eose'));
    }
  }
  private acceptsOrigin(listener: Listener, info: RuntimeEventInfo): boolean {
    if (info.cached) return listener.options.cache !== 'network-only';
    if (info.source === 'local-publish') return listener.options.localEcho !== false;
    const { relays, sources } = listener.options;
    if (relays === undefined && sources === undefined) return true;
    return this.relayScope(listener).includes(info.source) || (sources?.includes(info.source) ?? false);
  }
  private relayScope(listener: Listener): readonly string[] {
    if (listener.options.relays === undefined) return this.relays.urls();
    return listener.scopedRelays ??= listener.options.relays.map(url => new URL(url).href);
  }
  private sourceState(listener: Listener, id: string, complete: boolean, error?: string): void {
    if (listener.stopped) return;
    listener.pending.delete(id); listener.statuses.set(id, { complete, error });
    try { listener.handlers.onSource?.({ id, complete, ...(error ? { error } : {}) }); } catch (caught) { this.report(listener, caught); }
    const settle = listener.options.settle;
    if (settle?.sources && !listener.settleTimer && !listener.notified && listener.statuses.size >= settle.sources) {
      listener.settleTimer = setTimeout(() => {
        listener.settled = true;
        // Wait for cache replay and awaited writes before exposing settled history.
        void this.maybeComplete(listener);
      }, Math.max(0, settle.graceMs ?? 300));
    }
    // Wait for ordered admission/cache writes before exposing EOSE.
    void this.maybeComplete(listener);
  }
  private async maybeComplete(listener: Listener): Promise<void> {
    if (listener.remainingIds?.size === 0) return; // Exact-ID completion is already queued.
    // New arrivals can extend the write chain while an older admission is pending.
    // Drain the current chain before reporting EOSE, including arrivals during cache replay.
    while (listener.replayed && !listener.stopped && !listener.notified && (listener.pending.size === 0 || listener.settled)) {
      const writes = this.writes;
      // Background persistence reports history without draining unrelated writes.
      if (listener.options.persist !== 'background') {
        await writes;
        if (writes !== this.writes) continue;
      }
      if (listener.stopped || listener.notified || (listener.pending.size && !listener.settled)) return;
      if (listener.pending.size) { this.complete(listener, 'eose'); return; }
      const states = [...listener.statuses.values()];
      this.complete(listener, states.length && states.every((state) => state.complete) ? 'eose'
        : states.some((state) => state.error) || !states.length ? 'unavailable' : 'timeout');
      return;
    }
  }
  private complete(listener: Listener, reason: RuntimeCompletion['reason']): void {
    if (listener.stopped || listener.notified) return;
    listener.notified = true;
    if (listener.timer) clearTimeout(listener.timer);
    if (listener.settleTimer) clearTimeout(listener.settleTimer);
    const sources = [...listener.statuses].map(([id, state]) => ({ id, ...state }));
    for (const id of listener.pending) sources.push({ id, complete: false, error: 'Historical query is incomplete' });
    if (listener.admissionError) {
      reason = 'unavailable';
      sources.push({ id: 'local-cache', complete: false, error: listener.admissionError });
    }
    // Every requested id was delivered: the answer is complete even if other sources were still asking.
    const complete = reason === 'cache' || (reason === 'eose' && (listener.remainingIds?.size === 0
      || (sources.length > 0 && sources.every((source) => source.complete))));
    try { listener.handlers.onEose?.({ complete, reason, sources }); } catch (error) { this.report(listener, error); }
  }
  private stopListener(listener: Listener): void {
    if (listener.stopped) return;
    listener.stopped = true;
    listener.controller.abort();
    if (listener.timer) clearTimeout(listener.timer);
    if (listener.settleTimer) clearTimeout(listener.settleTimer);
    listener.options.signal?.removeEventListener('abort', listener.abort!);
    for (const close of listener.closers.values()) close();
    listener.closers.clear(); this.listeners.delete(listener); this.interests.remove(listener);
  }
  private async publishOnce(raw: NostrEvent, options: RuntimePublishOptions, onQueued: (result: RuntimePublishResult) => void): Promise<RuntimePublishResult> {
    const event = verifyNostrEvent(raw, this.verifier);
    const queue = !options.requireAck && options.queue !== false;
    const echo = !options.requireAck && options.localEcho !== false;
    if (queue) await updatePublicationScope(this.store, event, options, 'queue');
    if (echo && !await this.ingest(event, 'local-publish')) {
      if (queue) await updatePublicationScope(this.store, event, options, 'retire');
      return { accepted: false, remoteAccepted: false, queued: false, sources: [{ id: 'local-cache', accepted: false, error: 'Event is expired, deleted or superseded' }] };
    }
    const attempts = this.relays.publishAttempts(event, options.relays).map(({ id, result }) => ({ id, remote: true, result }));
    for (const source of this.sources.values()) {
      if (!source.publish || (options.sources ? !options.sources.includes(source.id) : options.relays !== undefined)) continue;
      const attempt = { id: source.id, remote: source.publishAcceptance !== 'queued', result: Promise.resolve() };
      attempt.result = bounded(
        source.publish(event, localIndexSource('runtime-outbox')).then((report) => {
          if (!report.accepted) throw new Error(report.reason ?? 'Publication rejected');
          if (report.remoteAccepted === true) attempt.remote = true;
        }), this.options.publishTimeoutMs ?? 5000,
      );
      attempts.push(attempt);
    }
    const outcomes: RuntimePublishResult['sources'] = attempts.map(({ id }) => ({ id, accepted: false, pending: true }));
    // Resolve the caller after local persistence, while retaining delivery for retries and shutdown.
    if (queue) onQueued({ accepted: false, remoteAccepted: false, queued: true, sources: outcomes.map((outcome) => ({ ...outcome })) });
    const accepted = await new Promise<boolean>((resolve) => {
      let remaining = attempts.length;
      if (!remaining) resolve(false);
      attempts.forEach(({ result }, index) => {
        void result.then(() => {
          const remote = attempts[index]!.remote;
          outcomes[index] = { id: attempts[index]!.id, accepted: remote, ...(remote ? {} : { queued: true }) };
          if (remote) resolve(true);
        }, (error) => {
          outcomes[index] = { id: attempts[index]!.id, accepted: false, error: asError(error).message,
            ...(error instanceof RelayRateLimitError ? { rateLimited: true, retryAfterMs: error.retryAfterMs } : {}),
          };
        })
          .finally(() => { if (--remaining === 0) resolve(false); });
      });
    });
    if (accepted) {
      if (queue) await updatePublicationScope(this.store, event, options, 'retire');
      if (!echo) await this.ingest(event, 'local-publish');
    }
    return { accepted, remoteAccepted: accepted, queued: queue && !accepted, sources: outcomes.map((outcome) => ({ ...outcome })) };
  }
  private scheduleRetry(): void {
    this.retryTimer = setTimeout(() => {
      void this.retryPending().catch((error) => this.options.onError?.(asError(error))).finally(() => { if (!this.stopped) this.scheduleRetry(); });
    }, 5000);
  }
  private report(listener: Listener, error: unknown): void {
    const normalized = asError(error);
    try { (listener.handlers.onError ?? this.options.onError)?.(normalized); } catch { /* consumer errors do not stop network processing */ }
  }
  private ensureOpen(): void { if (this.stopped) throw new Error('Nostr runtime is closed'); }
  private ensureSubscriptionCapacity(): void {
    if (this.listeners.size + this.relays.transientCount() >= (this.options.maxSubscriptions ?? 1024)) {
      throw new RangeError('Nostr subscription capacity exhausted');
    }
  }
}
export function createNostrRuntime(options: NostrRuntimeOptions = {}): NostrRuntime { return new NostrRuntime(options); }
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }

function bounded<T>(promise: Promise<T>, timeout: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Nostr source publication timed out')), timeout);
    promise.then((value) => { clearTimeout(timer); resolve(value); }, (error) => { clearTimeout(timer); reject(error); });
  });
}
