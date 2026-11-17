import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';
import type { RuntimeEventStore, RuntimeOutboxEntry } from './runtime-types.js';
export declare function selectStoredEvents(events: Iterable<NostrEvent>, filters: NostrFilter[], options?: QueryOptions): NostrEvent[];
export declare function eventOrder(a: NostrEvent, b: NostrEvent): number;
export declare function expired(event: NostrEvent): boolean;
export declare function abortError(reason?: unknown): DOMException;
export declare function replacementFilter(event: NostrEvent): NostrFilter | undefined;
export declare function eventAddress(event: NostrEvent): string | undefined;
/** Called serially by a runtime after signature admission. Deletion events are retained as tombstones. */
export declare function storeRuntimeEvent(store: RuntimeEventStore, event: NostrEvent): Promise<boolean>;
/** Preserve rejection reasons so historical callers can opt into superseded versions only. */
export declare function admitRuntimeEvent(store: RuntimeEventStore, event: NostrEvent): Promise<'admitted' | 'superseded' | 'rejected'>;
/** Bounded default cache. Supply a persistent store for offline operation across reloads. */
export declare class MemoryEventStore implements RuntimeEventStore {
    readonly maxEvents: number;
    readonly maxPending: number;
    private readonly events;
    /** The stored events in `eventOrder`, kept current so a query need not sort the store; rebuilt when unset. */
    private ordered?;
    private readonly pending;
    private pendingScopeCount;
    constructor(maxEvents?: number, maxPending?: number);
    query(filters: NostrFilter[], options?: QueryOptions): Promise<NostrEvent[]>;
    put(event: NostrEvent): Promise<void>;
    delete(ids: string[]): Promise<void>;
    private sorted;
    private remove;
    /** Swap or drop one stored object in place; false when the order no longer holds it where expected. */
    private replaceOrdered;
    updatePending(id: string, update: (row: RuntimeOutboxEntry | undefined) => RuntimeOutboxEntry | undefined): Promise<void>;
    listPending(): Promise<RuntimeOutboxEntry[]>;
    putPending(entry: RuntimeOutboxEntry): Promise<void>;
    deletePending(id: string): Promise<void>;
}
//# sourceMappingURL=runtime-store.d.ts.map