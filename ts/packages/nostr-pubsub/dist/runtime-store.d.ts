import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';
import type { RuntimeEventStore, RuntimeOutboxEntry } from './runtime-types.js';
export declare function selectStoredEvents(events: Iterable<NostrEvent>, filters: NostrFilter[], options?: QueryOptions): NostrEvent[];
export declare function eventOrder(a: NostrEvent, b: NostrEvent): number;
export declare function expired(event: NostrEvent): boolean;
export declare function abortError(reason?: unknown): DOMException;
export declare function replacementFilter(event: NostrEvent): NostrFilter | undefined;
/** Called serially by a runtime after signature admission. Deletion events are retained as tombstones. */
export declare function storeRuntimeEvent(store: RuntimeEventStore, event: NostrEvent): Promise<boolean>;
/** Preserve rejection reasons so historical callers can opt into superseded versions only. */
export declare function admitRuntimeEvent(store: RuntimeEventStore, event: NostrEvent): Promise<'admitted' | 'superseded' | 'rejected'>;
/** Bounded default cache. Supply a persistent store for offline operation across reloads. */
export declare class MemoryEventStore implements RuntimeEventStore {
    readonly maxEvents: number;
    readonly maxPending: number;
    private readonly events;
    private readonly pending;
    constructor(maxEvents?: number, maxPending?: number);
    query(filters: NostrFilter[], options?: QueryOptions): Promise<NostrEvent[]>;
    put(event: NostrEvent): Promise<void>;
    delete(ids: string[]): Promise<void>;
    listPending(): Promise<RuntimeOutboxEntry[]>;
    putPending(entry: RuntimeOutboxEntry): Promise<void>;
    deletePending(id: string): Promise<void>;
}
//# sourceMappingURL=runtime-store.d.ts.map