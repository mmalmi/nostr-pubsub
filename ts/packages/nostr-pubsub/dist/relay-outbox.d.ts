export interface RelayDelivery {
    id: string;
    relay: string;
    created_at?: number;
    broadcast?: boolean;
}
/** Use the same local transaction as the event. A completed enqueue never waits for a socket. */
export declare function enqueueRelayPublication(event: {
    id: string;
    created_at: number;
}, relays: readonly string[], put: (row: RelayDelivery) => Promise<unknown>, options?: {
    broadcast?: boolean;
}): Promise<void>;
export interface RelayDeliveryProgress {
    sent: number;
    present: number;
    failed: number;
    error?: string;
}
export interface RelayDrainOptions {
    /** Called before each send so paused/removed destinations can keep their outstanding rows. */
    enabled(row: RelayDelivery): Promise<boolean>;
    /** Resolve on a positive remote receipt. Throw on rejection or disconnect. */
    send(row: RelayDelivery): Promise<void>;
    acknowledge(ids: string[]): Promise<void>;
    connected(): boolean;
    /** Return IDs missing at this relay, or undefined to publish the batch directly. */
    missing?(batch: RelayDelivery[]): Promise<ReadonlySet<string> | undefined>;
    onProgress?(progress: RelayDeliveryProgress): void;
}
/** Drain one destination independently. Unconfirmed records survive interruption and restart. */
export declare function drainRelayOutbox(relay: string, rows: readonly RelayDelivery[], options: RelayDrainOptions): Promise<RelayDeliveryProgress>;
export declare function relayRetryDeadline(error: {
    retryAfterMs?: number;
}, now?: number): number;
export interface RelayRetryStore {
    deadline(relay: string): Promise<number | undefined>;
    defer(relay: string, until: number): Promise<void>;
}
/** Run independent destination passes and persist backoff. The host supplies the wake-up mechanism. */
export declare function retryRelayOutbox(relays: readonly string[], store: RelayRetryStore, run: (relay: string) => Promise<void>, onDeferred: (relay: string, until: number) => void, onError: (relay: string, error: unknown) => void): Promise<void>;
//# sourceMappingURL=relay-outbox.d.ts.map