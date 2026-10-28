import { RetainedEvents, MAX_RETAINED_SNAPSHOT_BYTES } from './retained-events.js';
import { localIndexSource, SOURCE_PRIORITY_LOCAL_INDEX } from './source.js';
import { validateQueryOptions } from './types.js';
/** Atomic browser storage across tabs/workers. Use a database name specific to
 * the owner, discovery scope and privacy lane. No outbox or remote queries.
 */
export class IndexedDbEventBus {
    database;
    policy;
    // Reuse only immutable events whose complete stored bytes still match. A new
    // instance or any disk/tab edit crosses signature verification again.
    verified = new Map();
    constructor(database, policy) {
        this.database = database;
        this.policy = policy;
    }
    static async open(name, policy) {
        new RetainedEvents(policy);
        if (!name.trim())
            throw new Error('Event database requires an explicit owner/scope name');
        const database = await new Promise((resolve, reject) => {
            const request = indexedDB.open(name, 1);
            request.onupgradeneeded = () => request.result.createObjectStore('snapshots');
            let blocked = false;
            request.onsuccess = () => { if (blocked)
                request.result.close();
            else
                resolve(request.result); };
            request.onerror = () => reject(request.error);
            request.onblocked = () => { blocked = true; reject(new Error('Event database upgrade blocked')); };
        });
        database.onversionchange = () => database.close();
        return new IndexedDbEventBus(database, structuredClone(policy));
    }
    async publish(event, _source) {
        const accepted = await this.transaction('readwrite', cache => cache.insert(event));
        return { accepted, priority: SOURCE_PRIORITY_LOCAL_INDEX,
            ...(!accepted ? { reason: 'Event unchanged or outside retention' } : {}) };
    }
    async query(filters, options = {}) {
        validateQueryOptions(options);
        const events = await this.transaction('readonly', cache => cache.query(filters, options), options);
        return { complete: true, events: events.map(event => ({
                event, source: localIndexSource('retained-events'), priority: SOURCE_PRIORITY_LOCAL_INDEX,
            })) };
    }
    close() { this.database.close(); }
    transaction(mode, operation, options = {}) {
        return new Promise((resolve, reject) => {
            if (options.signal?.aborted) {
                reject(options.signal.reason);
                return;
            }
            if (options.deadline !== undefined && options.deadline <= Date.now()) {
                reject(new Error('Event query deadline exceeded'));
                return;
            }
            const transaction = this.database.transaction('snapshots', mode);
            const store = transaction.objectStore('snapshots');
            const request = store.get('events');
            let result;
            let verified;
            let failure;
            const abort = () => transaction.abort();
            options.signal?.addEventListener('abort', abort, { once: true });
            let timer;
            if (options.deadline !== undefined)
                timer = setTimeout(abort, Math.max(0, options.deadline - Date.now()));
            const cleanup = () => { options.signal?.removeEventListener('abort', abort); if (timer)
                clearTimeout(timer); };
            transaction.oncomplete = () => { this.verified = verified; cleanup(); resolve(result); };
            transaction.onabort = () => { cleanup(); reject(failure ?? transaction.error ?? new Error('Event transaction aborted')); };
            transaction.onerror = () => { failure ??= transaction.error; };
            request.onsuccess = () => {
                try {
                    const snapshot = request.result;
                    if (snapshot && (snapshot.version !== 1 || !Array.isArray(snapshot.events) ||
                        new TextEncoder().encode(JSON.stringify(snapshot)).length > MAX_RETAINED_SNAPSHOT_BYTES))
                        throw new Error('Invalid event snapshot');
                    const cache = new RetainedEvents(this.policy);
                    cache.restore((snapshot?.events ?? []).map(event => this.verified.get(JSON.stringify(event)) ?? event));
                    result = operation(cache);
                    verified = new Map(cache.query([{}]).map(event => [JSON.stringify(event), event]));
                    if (mode === 'readwrite' && result === true)
                        store.put({ version: 1, events: cache.snapshot() }, 'events');
                }
                catch (error) {
                    failure = error;
                    transaction.abort();
                }
            };
        });
    }
}
//# sourceMappingURL=indexed-db-event-bus.js.map