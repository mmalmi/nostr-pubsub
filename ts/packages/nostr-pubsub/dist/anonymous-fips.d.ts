import type { FipsPubsubClientNode } from './fips-pubsub-client-types.js';
import type { FipsReceiptOptions } from './fips-publication-receipts.js';
import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';
import type { NostrEventSubscription, QueryEvent, QueryReport } from './event-bus.js';
/** Fresh random endpoint, with explicit approved public providers only. Never
 * return the account/live receiver endpoint, contacts, or discovered phone peers. */
export interface AnonymousFipsEndpoint {
    node: FipsPubsubClientNode;
    localPeerId: string;
    peers(): readonly string[];
    close(): Promise<void>;
}
export interface AnonymousFipsOptions {
    createEndpoint(filters?: NostrFilter[]): Promise<AnonymousFipsEndpoint>;
    timeoutMs?: number;
    id?: string;
}
/** Remote-acknowledged RuntimeSource publisher. Every publish/copy/retry gets
 * a new endpoint. An old, rejecting or unreachable provider leaves queues pending. */
export declare function createAnonymousFipsPublisher(options: AnonymousFipsOptions): {
    id: string;
    publishAcceptance: "remote";
    publish(event: NostrEvent, _source?: unknown, receiptOptions?: FipsReceiptOptions): Promise<{
        accepted: boolean;
        remoteAccepted: boolean;
        priority: number;
        reason: string;
    }>;
};
/** One public author/device scope per fresh endpoint. A lookup never reuses
 * an identified receiver or another subject's transport. */
export declare function createAnonymousFipsDiscovery(options: AnonymousFipsOptions): {
    id: string;
    query(filters: NostrFilter[], queryOptions?: QueryOptions): Promise<QueryReport>;
    subscribe(filters: NostrFilter[], handler: (event: QueryEvent) => void): Promise<NostrEventSubscription>;
};
//# sourceMappingURL=anonymous-fips.d.ts.map