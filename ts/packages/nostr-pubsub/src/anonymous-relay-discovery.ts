import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { NostrRuntime } from './runtime.js';
import { MemoryEventStore } from './runtime-store.js';
import type { NostrFilter } from './types.js';
import type { NostrRuntimeOptions, RuntimeEventStore } from './runtime-types.js';

const authorLookupKinds = new Set([0, 1059, 10002, 10050, 30078, 37368, 37195]);

/** Identify targeted public lookups before validating their scope. */
export function isRelayDiscoveryFilter(filter: NostrFilter): boolean {
  return !!filter.kinds?.some(kind => kind === 37368 || kind === 30078
    || authorLookupKinds.has(kind) && (filter.authors !== undefined || kind !== 1059 && filter['#p'] !== undefined));
}

/** Preserve OR semantics while keeping every lookup key in a separate scope. */
export function splitRelayDiscoveryFilters(filters: NostrFilter[]): {
  receiver: NostrFilter[]; discovery: NostrFilter[][];
} {
  const receiver: NostrFilter[] = [];
  const discovery = new Map<string, NostrFilter[]>();
  for (const filter of filters) {
    if (!isRelayDiscoveryFilter(filter)) { receiver.push(filter); continue; }
    const scopedKind = (kind: number): boolean => authorLookupKinds.has(kind) && (kind !== 1059 || filter.authors !== undefined);
    const kinds = filter.kinds!.filter(scopedKind);
    const ordinary = filter.kinds!.filter(kind => !scopedKind(kind));
    const authors = filter.authors;
    const devices = filter['#p'];
    if (authors !== undefined && devices !== undefined) throw new Error('Public lookups require one key selector');
    const selector = authors !== undefined ? 'authors' : '#p';
    const keys = authors ?? devices;
    if (!keys?.length) throw new Error('Discovery requires a public key scope');
    for (const key of new Set(keys)) {
      const scoped = { ...filter, kinds, [selector]: [key] };
      const scope = discoveryScope([scoped]);
      const group = discovery.get(scope);
      if (group) group.push(scoped); else discovery.set(scope, [scoped]);
    }
    if (ordinary.length) receiver.push({ ...filter, kinds: ordinary });
  }
  return { receiver, discovery: [...discovery.values()] };
}

export function discoveryScope(filters: NostrFilter[]): string {
  if (!filters.length) throw new Error('Discovery requires a public key scope');
  const scopes = filters.map(filter => {
    if (!filter.kinds?.length || filter.kinds.some(kind => !authorLookupKinds.has(kind)))
      throw new Error('Discovery only reads public profiles, relay lists, devices and invites');
    if (filter.kinds.includes(37368) && filter['#type']?.includes('group_roster'))
      throw new Error('Private group membership cannot use public discovery');
    const author = filter.authors?.length === 1 && filter['#p'] === undefined ? filter.authors[0] : undefined;
    const device = filter.authors === undefined && filter.kinds.every(kind => kind === 37368)
      && filter['#p']?.length === 1 ? filter['#p'][0] : undefined;
    const key = author ?? device;
    if (!key || !/^[0-9a-f]{64}$/.test(key)) throw new Error('Discovery requires exactly one public author or device');
    return `${author ? 'author' : 'device'}:${key}`;
  });
  if (new Set(scopes).size !== 1) throw new Error('Different public keys require separate discovery connections');
  return scopes[0]!;
}

export type AnonymousRelayDiscoveryOptions = Pick<NostrRuntimeOptions,
  'relays' | 'store' | 'websocketImplementation' | 'historyTimeoutMs' | 'reconnectDelayMs' | 'onError'>
  & { maxScopes?: number };
export type AnonymousRelayDiscovery = Pick<NostrRuntime, 'query' | 'subscribe' | 'setRelays' | 'getRelayStats' | 'close'>;

/**
 * Isolate each public lookup key from account reception and every other lookup.
 * Connections authenticate with fresh scope keys and can never publish pending
 * events. The caller retains ownership of a supplied local cache.
 * Network addresses and timing remain observable to message servers.
 */
export function createAnonymousRelayDiscovery(options: AnonymousRelayDiscoveryOptions = {}): AnonymousRelayDiscovery {
  const store = options.store ?? new MemoryEventStore();
  const cache: RuntimeEventStore = {
    query: (filters, queryOptions) => store.query(filters, queryOptions),
    put: event => store.put(event), delete: ids => store.delete(ids),
    listPending: async () => [],
    putPending: async () => { throw new Error('Discovery cannot publish events'); },
    deletePending: async () => { throw new Error('Discovery cannot change publication queues'); },
  };
  const scopes = new Map<string, { runtime: NostrRuntime; users: number }>();
  const closing = new Set<Promise<void>>();
  const maxScopes = options.maxScopes ?? 512;
  if (!Number.isSafeInteger(maxScopes) || maxScopes < 1) throw new RangeError('Invalid discovery scope capacity');
  let relays = options.relays;
  let stopped = false;
  const acquire = (filters: NostrFilter[]) => {
    if (stopped) throw new Error('Discovery is closed');
    const key = discoveryScope(filters);
    let scope = scopes.get(key);
    if (!scope) {
      if (scopes.size >= maxScopes) throw new Error('Discovery connection capacity exhausted');
      const secret = generateSecretKey();
      scope = { users: 0, runtime: new NostrRuntime({
        store: cache, relays, websocketImplementation: options.websocketImplementation,
        historyTimeoutMs: options.historyTimeoutMs, reconnectDelayMs: options.reconnectDelayMs,
        onError: options.onError,
        signAuthEvent: async (_relay, event) => finalizeEvent(event, secret),
      }) };
      scopes.set(key, scope);
    }
    scope.users++;
    let released = false;
    return { runtime: scope.runtime, release() {
      if (released) return;
      released = true;
      if (--scope!.users === 0) {
        scopes.delete(key);
        const finished = scope!.runtime.close().catch(error => options.onError?.(error));
        closing.add(finished);
        void finished.finally(() => closing.delete(finished));
      }
    } };
  };
  return {
    async query(filters, queryOptions) {
      const scope = acquire(filters);
      try { return await scope.runtime.query(filters, queryOptions); }
      finally { scope.release(); }
    },
    subscribe(filters, handlers, subscribeOptions) {
      const scope = acquire(filters);
      try {
        const subscription = scope.runtime.subscribe(filters, handlers, subscribeOptions);
        const close = () => {
          subscribeOptions?.signal?.removeEventListener('abort', close);
          subscription.close(); scope.release();
        };
        subscribeOptions?.signal?.addEventListener('abort', close, { once: true });
        if (subscribeOptions?.signal?.aborted) close();
        return { close };
      } catch (error) { scope.release(); throw error; }
    },
    setRelays(urls) {
      if (stopped) throw new Error('Discovery is closed');
      relays = [...urls];
      for (const scope of scopes.values()) scope.runtime.setRelays(urls);
    },
    getRelayStats: () => [...scopes.values()].flatMap(scope => scope.runtime.getRelayStats()),
    async close() {
      stopped = true;
      const pending = [...scopes.values()].map(scope => scope.runtime.close());
      scopes.clear();
      await Promise.all([...pending, ...closing]);
    },
  };
}
