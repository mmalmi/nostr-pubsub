import type { EventBus, PublishReport, QueryReport } from './event-bus.js';
import type { EventRetentionPolicy } from './filter.js';
import { RetainedEvents, MAX_RETAINED_SNAPSHOT_BYTES } from './retained-events.js';
import { localIndexSource, SOURCE_PRIORITY_LOCAL_INDEX, type EventSource } from './source.js';
import { validateQueryOptions, type NostrEvent, type NostrFilter, type NostrVerifiedEvent, type QueryOptions } from './types.js';

/** Atomic browser storage across tabs/workers. Use a database name specific to
 * the owner, discovery scope and privacy lane. No outbox or remote queries.
 */
export class IndexedDbEventBus implements EventBus {
  // Reuse only immutable events whose complete stored bytes still match. A new
  // instance or any disk/tab edit crosses signature verification again.
  private verified = new Map<string, NostrVerifiedEvent>();
  private constructor(private readonly database: IDBDatabase, private readonly policy: EventRetentionPolicy) {}
  static async open(name: string, policy: EventRetentionPolicy): Promise<IndexedDbEventBus> {
    new RetainedEvents(policy);
    if (!name.trim()) throw new Error('Event database requires an explicit owner/scope name');
    const database = await new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(name, 1);
      request.onupgradeneeded = () => request.result.createObjectStore('snapshots');
      let blocked = false;
      request.onsuccess = () => { if (blocked) request.result.close(); else resolve(request.result); };
      request.onerror = () => reject(request.error);
      request.onblocked = () => { blocked = true; reject(new Error('Event database upgrade blocked')); };
    });
    database.onversionchange = () => database.close();
    return new IndexedDbEventBus(database, structuredClone(policy));
  }
  async publish(event: NostrEvent, _source: EventSource): Promise<PublishReport> {
    const accepted = await this.transaction('readwrite', cache => cache.insert(event));
    return { accepted, priority: SOURCE_PRIORITY_LOCAL_INDEX,
      ...(!accepted ? { reason: 'Event unchanged or outside retention' } : {}) };
  }
  async query(filters: NostrFilter[], options: QueryOptions = {}): Promise<QueryReport> {
    validateQueryOptions(options);
    const events = await this.transaction('readonly', cache => cache.query(filters, options), options);
    return { complete: true, events: events.map(event => ({
      event, source: localIndexSource('retained-events'), priority: SOURCE_PRIORITY_LOCAL_INDEX,
    })) };
  }
  close(): void { this.database.close(); }
  private transaction<T>(mode: IDBTransactionMode, operation: (events: RetainedEvents) => T,
    options: QueryOptions = {}): Promise<T> {
    return new Promise((resolve, reject) => {
      if (options.signal?.aborted) { reject(options.signal.reason); return; }
      if (options.deadline !== undefined && options.deadline <= Date.now()) { reject(new Error('Event query deadline exceeded')); return; }
      const transaction = this.database.transaction('snapshots', mode);
      const store = transaction.objectStore('snapshots');
      const request = store.get('events');
      let result: T;
      let verified: Map<string, NostrVerifiedEvent>;
      let failure: unknown;
      const abort = () => transaction.abort();
      options.signal?.addEventListener('abort', abort, { once: true });
      let timer: ReturnType<typeof setTimeout> | undefined;
      if (options.deadline !== undefined) timer = setTimeout(abort, Math.max(0, options.deadline - Date.now()));
      const cleanup = () => { options.signal?.removeEventListener('abort', abort); if (timer) clearTimeout(timer); };
      transaction.oncomplete = () => { this.verified = verified; cleanup(); resolve(result); };
      transaction.onabort = () => { cleanup(); reject(failure ?? transaction.error ?? new Error('Event transaction aborted')); };
      transaction.onerror = () => { failure ??= transaction.error; };
      request.onsuccess = () => {
        try {
          const snapshot = request.result as { version: number; events: NostrEvent[] } | undefined;
          if (snapshot && (snapshot.version !== 1 || !Array.isArray(snapshot.events) ||
            new TextEncoder().encode(JSON.stringify(snapshot)).length > MAX_RETAINED_SNAPSHOT_BYTES))
            throw new Error('Invalid event snapshot');
          const cache = new RetainedEvents(this.policy);
          cache.restore((snapshot?.events ?? []).map(event => this.verified.get(JSON.stringify(event)) ?? event));
          result = operation(cache);
          verified = new Map(cache.query([{}]).map(event => [JSON.stringify(event), event]));
          if (mode === 'readwrite' && result === true) store.put({ version: 1, events: cache.snapshot() }, 'events');
        } catch (error) { failure = error; transaction.abort(); }
      };
    });
  }
}
