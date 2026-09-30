import { type SubCloser } from 'nostr-tools/abstract-pool';
import { type NostrEvent, type NostrFilter } from './types.js';
import type { NostrRuntimeOptions, RuntimeRelayStats } from './runtime-types.js';
type Entry = {
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
    private relays;
    private readonly batches;
    private pending;
    private timer?;
    private stopped;
    private serial;
    private readonly maxFilters;
    private readonly maxBytes;
    constructor(options: NostrRuntimeOptions);
    urls(): string[];
    stats(): RuntimeRelayStats[];
    count(): number;
    subscribe(filters: NostrFilter[], event: Entry['event'], state: Entry['state'], relays?: readonly string[]): SubCloser;
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
export {};
//# sourceMappingURL=runtime-relays.d.ts.map