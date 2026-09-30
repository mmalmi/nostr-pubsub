import { type NostrEvent, type NostrFilter } from './types.js';
import type { NostrRuntimeOptions, RuntimeEventStore, RuntimeMetrics, RuntimePublishOptions, RuntimePublishResult, RuntimeQueryOptions, RuntimeQueryResult, RuntimeSource, RuntimeSubscribeOptions, RuntimeSubscription, RuntimeSubscriptionHandlers } from './runtime-types.js';
/** One signer-neutral owner of relay connections, live interests, local events and an outbox. */
export declare class NostrRuntime {
    private readonly options;
    readonly store: RuntimeEventStore;
    private readonly relays;
    private readonly sources;
    private readonly listeners;
    private readonly interests;
    private readonly incoming;
    private readonly publishing;
    private writes;
    private stopped;
    private retryTimer?;
    private retrying;
    private readonly counters;
    constructor(options?: NostrRuntimeOptions);
    subscribe(filters: NostrFilter[], handlers: RuntimeSubscriptionHandlers, options?: RuntimeSubscribeOptions): RuntimeSubscription;
    query(filters: NostrFilter[], options?: RuntimeQueryOptions): Promise<RuntimeQueryResult>;
    ingest(raw: NostrEvent, source?: string): Promise<boolean>;
    publish(event: NostrEvent, options?: RuntimePublishOptions): Promise<RuntimePublishResult>;
    retryPending(): Promise<void>;
    setRelays(urls: readonly string[]): void;
    getRelayStats(): import("./runtime-types.js").RuntimeRelayStats[];
    metrics(): RuntimeMetrics;
    addSource(source: RuntimeSource): void;
    removeSource(id: string): void;
    close(): Promise<void>;
    private startListener;
    private wantsSource;
    private attachSource;
    private receive;
    private deliver;
    private acceptsOrigin;
    private sourceState;
    private maybeComplete;
    private complete;
    private stopListener;
    private publishOnce;
    private scheduleRetry;
    private report;
    private ensureOpen;
}
export declare function createNostrRuntime(options?: NostrRuntimeOptions): NostrRuntime;
//# sourceMappingURL=runtime.d.ts.map