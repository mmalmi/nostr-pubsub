import type { EventBus, PublishReport, QueryReport } from './event-bus.js';
import type { EventRetentionPolicy } from './filter.js';
import { type EventSource } from './source.js';
import { type NostrEvent, type NostrFilter, type QueryOptions } from './types.js';
/** Atomic browser storage across tabs/workers. Use a database name specific to
 * the owner, discovery scope and privacy lane. No outbox or remote queries.
 */
export declare class IndexedDbEventBus implements EventBus {
    private readonly database;
    private readonly policy;
    private verified;
    private constructor();
    static open(name: string, policy: EventRetentionPolicy): Promise<IndexedDbEventBus>;
    publish(event: NostrEvent, _source: EventSource): Promise<PublishReport>;
    query(filters: NostrFilter[], options?: QueryOptions): Promise<QueryReport>;
    close(): void;
    private transaction;
}
//# sourceMappingURL=indexed-db-event-bus.d.ts.map