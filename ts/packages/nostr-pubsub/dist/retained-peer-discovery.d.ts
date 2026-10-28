import type { EventBus } from './event-bus.js';
import type { NostrRuntime } from './runtime.js';
import { type NostrEvent, type NostrFilter } from './types.js';
/** Public announcement bridge for a transport's injected discovery provider.
 * The caller owns persistence and network routing. Private discovery caches
 * must remain local and must never be supplied to a public replay reader.
 */
export declare class RetainedPeerDiscovery {
    private readonly runtime;
    readonly store: EventBus;
    private readonly onError;
    private tail;
    private pending;
    private closed;
    private readonly subscriptions;
    constructor(runtime: Pick<NostrRuntime, 'publish' | 'subscribe'>, store: EventBus, onError: (error: Error) => void);
    publish(input: NostrEvent): Promise<void>;
    subscribe(filter: NostrFilter, onEvent: (event: NostrEvent, source: string) => void): Promise<() => void>;
    close(): Promise<void>;
}
//# sourceMappingURL=retained-peer-discovery.d.ts.map