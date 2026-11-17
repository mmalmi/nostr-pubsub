import type { NostrEvent } from './types.js';
import type { RuntimeEventStore, RuntimeOutboxEntry, RuntimePublishOptions } from './runtime-types.js';
type Destination = Pick<RuntimePublishOptions, 'relays' | 'sources'>;
type Update = (current: RuntimeOutboxEntry | undefined) => RuntimeOutboxEntry | undefined;
/** Older stores key complete records by event ID. Serialize their read/modify/write across runtime owners. */
export declare function updatePending(store: RuntimeEventStore, id: string, update: Update): Promise<void>;
/** Capture caller-owned selections before asynchronous persistence or delivery. */
export declare function publicationOptions(options: RuntimePublishOptions): RuntimePublishOptions;
export declare function pendingPublications(rows: RuntimeOutboxEntry[]): RuntimeOutboxEntry[];
export declare function updatePublicationScope(store: RuntimeEventStore, event: NostrEvent, destination: Destination, action: 'queue' | 'retire'): Promise<void>;
export {};
//# sourceMappingURL=runtime-outbox.d.ts.map