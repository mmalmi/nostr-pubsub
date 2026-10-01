import type { NostrEventSubscription, QueryEvent } from './event-bus.js';
import type { FipsNostrPubsubClient } from './fips-pubsub-client.js';
import type { NostrFilter } from './types.js';
type Handler = (event: QueryEvent) => void;
/** Exact OR batching. Author/recipient fields are never unioned across filters. */
export declare class FipsSourceSubscriptions {
    private readonly client;
    private readonly interests;
    private readonly batches;
    private readonly codec;
    private readonly maxFilters;
    constructor(client: FipsNostrPubsubClient);
    subscribe(filters: NostrFilter[], handler: Handler): Promise<NostrEventSubscription>;
    private normalize;
    private filters;
    private fits;
    private schedule;
    private retire;
    private flush;
    private deliver;
}
export {};
//# sourceMappingURL=fips-pubsub-source-subscriptions.d.ts.map