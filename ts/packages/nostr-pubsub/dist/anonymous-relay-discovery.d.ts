import { NostrRuntime } from './runtime.js';
import type { NostrFilter } from './types.js';
import type { NostrRuntimeOptions } from './runtime-types.js';
/** Identify targeted public device/invite lookups before validating their scope. */
export declare function isRelayDiscoveryFilter(filter: NostrFilter): boolean;
export type AnonymousRelayDiscoveryOptions = Pick<NostrRuntimeOptions, 'relays' | 'store' | 'websocketImplementation' | 'historyTimeoutMs' | 'reconnectDelayMs' | 'onError'> & {
    maxScopes?: number;
};
export type AnonymousRelayDiscovery = Pick<NostrRuntime, 'query' | 'subscribe' | 'setRelays' | 'getRelayStats' | 'close'>;
/**
 * Isolate each public lookup key from account reception and every other lookup.
 * Connections authenticate with fresh scope keys and can never publish pending
 * events. The caller retains ownership of a supplied local cache.
 * Network addresses and timing remain observable to message servers.
 */
export declare function createAnonymousRelayDiscovery(options?: AnonymousRelayDiscoveryOptions): AnonymousRelayDiscovery;
//# sourceMappingURL=anonymous-relay-discovery.d.ts.map