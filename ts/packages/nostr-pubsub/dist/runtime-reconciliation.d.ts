import type { AbstractRelay } from 'nostr-tools/abstract-relay';
import type { NostrEvent, NostrFilter } from './types.js';
import type { RuntimeEventStore, RuntimeReconciliationOptions } from './runtime-types.js';
/** Uses the pool's public custom-message subscriptions; never owns a second socket. */
export declare class RuntimeReconciliation {
    private readonly store;
    private readonly capabilities;
    private readonly active;
    private readonly attempts;
    private readonly limits;
    private serial;
    constructor(options: RuntimeReconciliationOptions, store: RuntimeEventStore);
    start(relay: AbstractRelay, originals: NostrFilter[], deliver: (event: NostrEvent) => void): () => void;
    close(): void;
}
//# sourceMappingURL=runtime-reconciliation.d.ts.map