import { matchFilters } from 'nostr-tools/filter';
import { verifyNostrEvent, validateQueryOptions, type NostrEvent, type NostrFilter } from './types.js';
import { localIndexSource } from './source.js';
import { MemoryEventStore, abortError, selectStoredEvents, admitRuntimeEvent } from './runtime-store.js';
import { RuntimeInterests } from './runtime-interests.js';
import { RuntimeRelays } from './runtime-relays.js';
import type {
  NostrRuntimeOptions, RuntimeCompletion, RuntimeEventInfo, RuntimeEventStore, RuntimeMetrics,
  RuntimePublishOptions, RuntimePublishResult, RuntimeQueryOptions, RuntimeQueryResult, RuntimeSource,
  RuntimeSubscribeOptions, RuntimeSubscription, RuntimeSubscriptionHandlers,
} from './runtime-types.js';

type Listener = {
  filters: NostrFilter[]; handlers: RuntimeSubscriptionHandlers; options: RuntimeSubscribeOptions;
  seen: Set<string>; closers: Map<string, () => void>; statuses: Map<string, { complete: boolean; error?: string }>;
  pending: Set<string>; timer?: ReturnType<typeof setTimeout>; stopped: boolean; notified: boolean;
  controller: AbortController; abort?: () => void; ready: Promise<void>; admissionError?: string;
};

/** One signer-neutral owner of relay connections, live interests, local events and an outbox. */
export class NostrRuntime {
  readonly store: RuntimeEventStore;
  private readonly relays: RuntimeRelays;
  private readonly sources = new Map<string, RuntimeSource>();
  private readonly listeners = new Set<Listener>();
  private readonly interests = new RuntimeInterests<Listener>();
  private readonly incoming = new Map<string, Promise<boolean>>();
  private readonly publishing = new Map<string, Promise<RuntimePublishResult>>();
  private writes: Promise<unknown> = Promise.resolve();
  private stopped = false;
  private retryTimer?: ReturnType<typeof setTimeout>;
  private retrying = false;
  private readonly counters = { receivedEvents: 0, deliveredEvents: 0, duplicateEvents: 0, verificationFailures: 0 };
  constructor(private readonly options: NostrRuntimeOptions = {}) {
    this.store = options.store ?? new MemoryEventStore();
    this.relays = new RuntimeRelays(options);
    for (const source of options.sources ?? []) this.sources.set(source.id, source);
    this.scheduleRetry();
  }
  subscribe(filters: NostrFilter[], handlers: RuntimeSubscriptionHandlers, options: RuntimeSubscribeOptions = {}): RuntimeSubscription {
    this.ensureOpen();
    if (!filters.length) throw new RangeError('Nostr subscriptions require at least one filter');
    if (options.includeSuperseded && options.cache !== 'network-only') throw new Error('Historical replaceable versions require network-only queries');
    if (this.listeners.size >= (this.options.maxSubscriptions ?? 1024)) throw new RangeError('Nostr subscription capacity exhausted');
    validateQueryOptions({ deadline: options.deadline });
    const listener: Listener = {
      filters: structuredClone(filters), handlers, options, seen: new Set(), closers: new Map(),
      statuses: new Map(), pending: new Set(), controller: new AbortController(), stopped: false, notified: false, ready: Promise.resolve(),
    };
    this.listeners.add(listener);
    this.interests.add(listener, listener.filters);
    const close = (): void => this.stopListener(listener);
    listener.abort = close;
    options.signal?.addEventListener('abort', close, { once: true });
    if (options.signal?.aborted) { close(); return { close }; }
    listener.ready = this.startListener(listener).catch((error) => {
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
  async ingest(raw: NostrEvent, source = 'external'): Promise<boolean> {
    this.ensureOpen();
    let event: NostrEvent;
    try { event = verifyNostrEvent(raw, this.options.verifyEvent); }
    catch (error) { this.counters.verificationFailures++; throw error; }
    const ongoing = this.incoming.get(event.id);
    if (ongoing) return ongoing;
    const operation = this.writes.then(async () => {
      const admission = await admitRuntimeEvent(this.store, event);
      if (admission !== 'rejected' && !this.stopped) for (const listener of this.interests.candidates(event)) {
        if (admission === 'admitted' || listener.options.includeSuperseded) this.deliver(listener, event, { source, cached: false });
      }
      return admission === 'admitted';
    }).catch((error) => {
      // Admission is shared across overlapping interests. A failed durable write
      // must make every affected history incomplete, even if relay EOSE arrived.
      for (const listener of this.interests.candidates(event)) {
        if (!listener.stopped && matchFilters(listener.filters, event)) listener.admissionError = asError(error).message;
      }
      throw error;
    });
    this.writes = operation.catch(() => undefined);
    this.incoming.set(event.id, operation);
    try { return await operation; } finally { this.incoming.delete(event.id); }
  }
  publish(event: NostrEvent, options: RuntimePublishOptions = {}): Promise<RuntimePublishResult> {
    this.ensureOpen();
    const key = JSON.stringify([event.id, options]);
    const existing = this.publishing.get(key);
    if (existing) return existing;
    const promise = this.publishOnce(event, options).finally(() => this.publishing.delete(key));
    this.publishing.set(key, promise);
    return promise;
  }
  async retryPending(): Promise<void> {
    if (this.stopped || this.retrying) return;
    this.retrying = true;
    try {
      const pending = await this.store.listPending();
      for (let offset = 0; offset < pending.length && !this.stopped; offset += 4) {
        await Promise.all(pending.slice(offset, offset + 4).map(({ event, relays, sources }) => this.publish(event, { relays, sources }).catch((error) => this.options.onError?.(asError(error)))));
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
  metrics(): RuntimeMetrics { return { ...this.counters, subscriptions: this.listeners.size, relaySubscriptions: this.relays.count() }; }
  addSource(source: RuntimeSource): void {
    this.ensureOpen(); this.removeSource(source.id); this.sources.set(source.id, source);
    for (const listener of this.listeners) if (this.wantsSource(listener, source.id)) void this.attachSource(listener, source);
    void this.retryPending().catch((error) => this.options.onError?.(asError(error)));
  }
  removeSource(id: string): void {
    this.sources.delete(id);
    for (const listener of this.listeners) {
      listener.closers.get(id)?.(); listener.closers.delete(id); listener.pending.delete(id); listener.statuses.delete(id);
    }
  }
  async close(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    for (const listener of [...this.listeners]) { this.complete(listener, 'closed'); this.stopListener(listener); }
    this.relays.close();
    await this.writes;
    await Promise.allSettled([...this.publishing.values()]);
    await this.store.close?.();
  }
  private async startListener(listener: Listener): Promise<void> {
    const timeout = Math.max(0, (listener.options.deadline ?? Date.now() + (this.options.historyTimeoutMs ?? 10000)) - Date.now());
    listener.timer = setTimeout(() => this.complete(listener, 'timeout'), timeout);
    // Open live routes before cache replay so an event cannot fall between the two.
    if (listener.options.cache !== 'cache-only') {
      for (const url of listener.options.relays?.map((url) => new URL(url).toString()) ?? this.relays.urls()) listener.pending.add(url);
      const subscription = this.relays.subscribe(listener.filters, (event, source) => this.receive(listener, event, source),
        (source, complete, error) => this.sourceState(listener, source, complete, error), listener.options.relays);
      listener.closers.set('relay-transport', () => subscription.close());
      for (const source of this.sources.values()) if (this.wantsSource(listener, source.id)) { listener.pending.add(source.id); void this.attachSource(listener, source); }
    }
    if (listener.options.cache !== 'network-only') {
      const events = await this.store.query(listener.filters, { signal: listener.controller.signal, deadline: listener.options.deadline });
      for (const raw of events) {
        if (listener.stopped) return;
        try { this.deliver(listener, verifyNostrEvent(raw, this.options.verifyEvent), { source: 'local-cache', cached: true }); }
        catch (error) { this.report(listener, error); }
      }
    }
    if (listener.stopped) return;
    if (listener.options.cache === 'cache-only') { this.complete(listener, 'cache'); return; }
    await this.writes;
    this.maybeComplete(listener);
  }
  private wantsSource(listener: Listener, id: string): boolean {
    return listener.options.cache !== 'cache-only' && (listener.options.sources ? listener.options.sources.includes(id) : listener.options.relays === undefined);
  }
  private async attachSource(listener: Listener, source: RuntimeSource): Promise<void> {
    listener.pending.add(source.id);
    try {
      if (source.subscribe) {
        const subscription = await source.subscribe(listener.filters, ({ event }) => this.receive(listener, event, source.id));
        if (listener.stopped || this.sources.get(source.id) !== source) { subscription.close(); return; }
        listener.closers.set(source.id, () => subscription.close());
      }
      if (source.query) {
        const report = await source.query(listener.filters, {
          deadline: listener.options.deadline ?? Date.now() + (this.options.historyTimeoutMs ?? 10000), signal: listener.controller.signal,
        });
        if (listener.stopped || this.sources.get(source.id) !== source) return;
        for (const { event } of report.events) await this.ingest(event, source.id);
        this.sourceState(listener, source.id, report.complete !== false);
      } else this.sourceState(listener, source.id, false, 'Source has no historical completion');
    } catch (error) {
      if (!listener.stopped && this.sources.get(source.id) === source) {
        this.sourceState(listener, source.id, false, asError(error).message); this.report(listener, error);
      }
    }
  }
  private receive(listener: Listener, event: NostrEvent, source: string): void {
    if (listener.stopped) return;
    this.counters.receivedEvents++;
    if (listener.seen.has(event.id) || this.incoming.has(event.id)) { this.counters.duplicateEvents++; return; }
    void this.ingest(event, source).catch((error) => this.report(listener, error));
  }
  private deliver(listener: Listener, event: NostrEvent, info: RuntimeEventInfo): void {
    if (listener.stopped || !matchFilters(listener.filters, event)) return;
    if (listener.seen.has(event.id)) { this.counters.duplicateEvents++; return; }
    listener.seen.add(event.id);
    if (listener.seen.size > (this.options.maxSeenEventsPerSubscription ?? 4096)) listener.seen.delete(listener.seen.values().next().value!);
    this.counters.deliveredEvents++;
    try { listener.handlers.onEvent(event, info); } catch (error) { this.report(listener, error); }
  }
  private sourceState(listener: Listener, id: string, complete: boolean, error?: string): void {
    if (listener.stopped) return;
    listener.pending.delete(id); listener.statuses.set(id, { complete, error });
    // Wait for ordered admission/cache writes before exposing EOSE.
    void listener.ready.then(() => this.writes).then(() => this.maybeComplete(listener));
  }
  private maybeComplete(listener: Listener): void {
    if (listener.pending.size === 0) {
      const states = [...listener.statuses.values()];
      this.complete(listener, states.length && states.every((state) => state.complete) ? 'eose'
        : states.some((state) => state.error) || !states.length ? 'unavailable' : 'timeout');
    }
  }
  private complete(listener: Listener, reason: RuntimeCompletion['reason']): void {
    if (listener.stopped || listener.notified) return;
    listener.notified = true;
    if (listener.timer) clearTimeout(listener.timer);
    const sources = [...listener.statuses].map(([id, state]) => ({ id, ...state }));
    for (const id of listener.pending) sources.push({ id, complete: false, error: 'Historical query is incomplete' });
    if (listener.admissionError) {
      reason = 'unavailable';
      sources.push({ id: 'local-cache', complete: false, error: listener.admissionError });
    }
    const complete = reason === 'cache' || (reason === 'eose' && sources.length > 0 && sources.every((source) => source.complete));
    try { listener.handlers.onEose?.({ complete, reason, sources }); } catch (error) { this.report(listener, error); }
  }
  private stopListener(listener: Listener): void {
    if (listener.stopped) return;
    listener.stopped = true;
    listener.controller.abort();
    if (listener.timer) clearTimeout(listener.timer);
    listener.options.signal?.removeEventListener('abort', listener.abort!);
    for (const close of listener.closers.values()) close();
    listener.closers.clear(); this.listeners.delete(listener); this.interests.remove(listener);
  }
  private async publishOnce(raw: NostrEvent, options: RuntimePublishOptions): Promise<RuntimePublishResult> {
    const event = verifyNostrEvent(raw, this.options.verifyEvent);
    const queue = !options.requireAck && options.queue !== false;
    const echo = !options.requireAck && options.localEcho !== false;
    if (queue) {
      const old = (await this.store.listPending()).find((entry) => entry.event.id === event.id);
      await this.store.putPending({ event, attempts: (old?.attempts ?? 0) + 1, updatedAt: Date.now(), relays: options.relays, sources: options.sources });
    }
    if (echo && !await this.ingest(event, 'local-publish')) {
      if (queue) await this.store.deletePending(event.id);
      return { accepted: false, remoteAccepted: false, queued: false, sources: [{ id: 'local-cache', accepted: false, error: 'Event is expired, deleted or superseded' }] };
    }
    const attempts = this.relays.publishAttempts(event, options.relays).map(({ id, result }) => ({ id, remote: true, result }));
    for (const source of this.sources.values()) {
      if (!source.publish || (options.sources ? !options.sources.includes(source.id) : options.relays !== undefined)) continue;
      attempts.push({ id: source.id, remote: source.publishAcceptance !== 'queued', result: bounded(
        source.publish(event, localIndexSource('runtime-outbox')).then((report) => {
          if (!report.accepted) throw new Error(report.reason ?? 'Publication rejected');
        }), this.options.publishTimeoutMs ?? 5000,
      ) });
    }
    const outcomes: RuntimePublishResult['sources'] = attempts.map(({ id }) => ({ id, accepted: false, pending: true }));
    const accepted = await new Promise<boolean>((resolve) => {
      let remaining = attempts.length;
      if (!remaining) resolve(false);
      attempts.forEach(({ result, remote }, index) => {
        void result.then(() => {
          outcomes[index] = { id: attempts[index]!.id, accepted: remote, ...(remote ? {} : { queued: true }) };
          if (remote) resolve(true);
        }, (error) => { outcomes[index] = { id: attempts[index]!.id, accepted: false, error: asError(error).message }; })
          .finally(() => { if (--remaining === 0) resolve(false); });
      });
    });
    if (accepted) {
      if (queue) await this.store.deletePending(event.id);
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
}
export function createNostrRuntime(options: NostrRuntimeOptions = {}): NostrRuntime { return new NostrRuntime(options); }
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }

function bounded<T>(promise: Promise<T>, timeout: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Nostr source publication timed out')), timeout);
    promise.then((value) => { clearTimeout(timer); resolve(value); }, (error) => { clearTimeout(timer); reject(error); });
  });
}
