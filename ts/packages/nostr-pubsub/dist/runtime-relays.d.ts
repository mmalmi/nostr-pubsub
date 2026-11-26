import type { SubCloser } from 'nostr-tools/abstract-pool';
import { type NostrEvent, type NostrEventVerifier, type NostrFilter, type QueryOptions } from './types.js';
import type { NostrRuntimeOptions, RuntimeQueryResult, RuntimeRelayStats } from './runtime-types.js';
type Scope = {
    relays?: readonly string[];
    isolated?: boolean;
    transient?: boolean;
};
type Entry = {
    isolated: boolean;
    transient: boolean;
    relays?: readonly string[];
    filters: NostrFilter[];
    event: (event: NostrEvent, relay: string) => void;
    state: (relay: string, complete: boolean, error?: string) => void;
    closed: boolean;
    batch?: Batch;
};
type Link = {
    generation: number;
    close?: () => void;
    timer?: ReturnType<typeof setTimeout>;
    attempts: number;
    eosed: boolean;
    authenticating?: boolean;
    latest: Map<string, number>;
};
type Batch = {
    entries: Entry[];
    links: Map<string, Link>;
    closed: boolean;
    reopenTimer?: ReturnType<typeof setTimeout>;
};
/** Batches OR filters without merging their fields, preserving recipient/author intersections. */
export declare class RuntimeRelays {
    private readonly options;
    private readonly pool;
    private readonly boundary;
    private readonly reconciliation?;
    private relays;
    private readonly batches;
    private readonly transientQueries;
    private pending;
    private timer?;
    private stopped;
    private serial;
    private readonly maxFilters;
    private readonly maxBytes;
    constructor(options: NostrRuntimeOptions, verifier?: NostrEventVerifier);
    urls(): readonly string[];
    stats(): RuntimeRelayStats[];
    count(): number;
    transientCount(): number;
    queryTransient(relay: string, filters: NostrFilter[], options: QueryOptions): Promise<RuntimeQueryResult>;
    subscribe(filters: NostrFilter[], event: Entry['event'], state: Entry['state'], { relays, isolated, transient }?: Scope): SubCloser;
    setRelays(urls: readonly string[]): void;
    publishAttempts(event: NostrEvent, urls?: readonly string[]): Array<{
        id: string;
        result: Promise<void>;
    }>;
    private publishOne;
    close(): void;
    private flush;
    private startBatch;
    private reopen;
    private connect;
    private retry;
    private closeLink;
    private closeBatch;
}
export declare function filterKey(filter: NostrFilter): string;
/** Bound duplicate verification work without trusting event IDs or public verified markers. */
export declare function boundedVerifier(verify: (event: NostrEvent) => boolean): (event: NostrEvent) => boolean;
export {};
//# sourceMappingURL=runtime-relays.d.ts.map