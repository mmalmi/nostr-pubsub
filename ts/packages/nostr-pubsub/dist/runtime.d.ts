import { type NostrEvent, type NostrFilter, type QueryOptions } from './types.js';
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
    /**
     * Read a bounded page (1–256 events) from one explicit relay on the shared socket.
     * Checks signatures, filters and expiration without local replay, writes or reconciliation.
     * This request sends no AUTH. If the socket is already authenticated, its existing
     * connection identity still applies. Use an anonymous runtime when required.
     */
    queryTransientRelay(relay: string, filters: NostrFilter[], options?: QueryOptions): Promise<RuntimeQueryResult>;
    ingest(raw: NostrEvent, source?: string): Promise<boolean>;
    private admit;
    publish(event: NostrEvent, options?: RuntimePublishOptions): Promise<RuntimePublishResult>;
    private startPublish;
    retryPending(): Promise<void>;
    setRelays(urls: readonly string[]): void;
    getRelayStats(): import("./runtime-types.js").RuntimeRelayStats[];
    metrics(): RuntimeMetrics;
    addSource(source: RuntimeSource): void;
    removeSource(id: string): void;
    close(): Promise<void>;
    private startListener;
    private sourceSupports;
    private background;
    /** Deliver a verified network event now and persist it without holding the read. */
    private deliverThenAdmit;
    private wantsSource;
    private attachSource;
    private receive;
    private deliver;
    private allIdsSeen;
    private acceptsOrigin;
    private sourceState;
    private maybeComplete;
    private complete;
    private stopListener;
    private publishOnce;
    private scheduleRetry;
    private report;
    private ensureOpen;
    private ensureSubscriptionCapacity;
}
export declare function createNostrRuntime(options?: NostrRuntimeOptions): NostrRuntime;
//# sourceMappingURL=runtime.d.ts.map