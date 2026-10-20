import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { NostrRuntime } from './runtime.js';
import { MemoryEventStore } from './runtime-store.js';
/** Classify before validation so malformed private traffic cannot fall back to public publication. */
export function isPrivateRelayEvent(event) {
    return event.kind === 1059 || event.kind === 1060 || event.kind === 21111;
}
function privateEnvelopeAllowed(event) {
    if (!isPrivateRelayEvent(event) || !Array.isArray(event.tags)
        || !event.tags.every(tag => Array.isArray(tag) && tag.every(part => typeof part === 'string')))
        return false;
    const recipients = event.tags.filter(tag => tag[0] === 'p');
    return recipients.length === 1 && recipients[0]?.length === 2 && /^[0-9a-f]{64}$/.test(recipients[0]?.[1] ?? '')
        && (event.kind !== 1060 || event.tags.filter(tag => tag[0] === 'v').length === 1
            && event.tags.some(tag => tag[0] === 'v' && tag[1] === '2'))
        && event.tags.every(tag => ['p', 'header', 'v', 'wk', 'expiration'].includes(tag[0] ?? ''));
}
function privateRoster(event) {
    return event.kind === 37368 && Array.isArray(event.tags)
        && event.tags.some(tag => Array.isArray(tag) && tag[0] === 'type' && tag[1] === 'group_roster');
}
/**
 * Keep retry queues in their intended connection lane, retaining quarantined
 * records in storage. Historical received events remain locally readable.
 * Supply separate underlying stores for public reception and private sending.
 */
export function publicationLaneStore(store, lane) {
    const allowed = (event) => lane === 'private'
        ? privateEnvelopeAllowed(event) : !isPrivateRelayEvent(event) && !privateRoster(event);
    return {
        query: (filters, options) => store.query(filters, options),
        put: event => store.put(event),
        delete: ids => store.delete(ids),
        listPending: async () => (await store.listPending()).filter(entry => allowed(entry.event)),
        putPending: entry => {
            if (!allowed(entry.event))
                return Promise.reject(new Error('Publication belongs to a different connection lane'));
            return store.putPending(entry);
        },
        deletePending: id => store.deletePending(id),
        ...(store.close ? { close: () => store.close() } : {}),
    };
}
function publishOnly(source) {
    if (typeof source.publish !== 'function')
        throw new Error('Private source requires a publisher');
    return { id: source.id, publishAcceptance: source.publishAcceptance,
        publish: (...args) => source.publish(...args) };
}
/**
 * An EVENT-only relay connection owner with a fresh NIP-42 signer per attempt.
 * It cannot expose an account signer or issue subscriptions, queries or inventories.
 * Supplied storage remains caller-owned. This isolates protocol identities; it
 * does not hide network addresses or timing.
 */
export function createPrivateRelayPublisher(options = {}) {
    const store = publicationLaneStore(options.store ?? new MemoryEventStore(), 'private');
    const sources = new Map(options.sources?.map(source => [source.id, publishOnly(source)]));
    const active = new Map();
    let relays = options.relays;
    let stopped = false;
    let retrying = false;
    const report = (error) => options.onError?.(error instanceof Error ? error : new Error(String(error)));
    const publish = (event, publishOptions = {}) => {
        if (stopped)
            return Promise.reject(new Error('Private publisher is closed'));
        if (!privateEnvelopeAllowed(event))
            return Promise.reject(new Error('Private publisher requires a recipient-specific encrypted event'));
        const existing = active.get(event.id);
        if (existing)
            return existing.result;
        const authenticationKey = generateSecretKey();
        // An operation may write its durable intent, but can never discover/retry any
        // pending row itself. Every retry must acquire a new socket and AUTH key.
        const operationStore = {
            query: (filters, queryOptions) => store.query(filters, queryOptions),
            put: value => store.put(value), delete: ids => store.delete(ids), listPending: async () => [],
            putPending: async (entry) => {
                const old = (await store.listPending()).find(row => row.event.id === entry.event.id);
                await store.putPending({ ...entry, attempts: (old?.attempts ?? 0) + 1 });
            },
            deletePending: id => store.deletePending(id),
        };
        const runtime = new NostrRuntime({
            relays, store: operationStore, sources: [...sources.values()],
            websocketImplementation: options.websocketImplementation,
            publishTimeoutMs: options.publishTimeoutMs, historyTimeoutMs: options.historyTimeoutMs,
            onError: options.onError,
            signAuthEvent: async (_relay, template) => finalizeEvent(template, authenticationKey),
        });
        const result = runtime.publish(event, publishOptions).finally(async () => {
            await runtime.close();
            active.delete(event.id);
        });
        active.set(event.id, { runtime, result });
        return result;
    };
    const retryPending = async () => {
        if (stopped || retrying)
            return;
        retrying = true;
        try {
            const pending = await store.listPending();
            for (let offset = 0; offset < pending.length && !stopped; offset += 4) {
                await Promise.all(pending.slice(offset, offset + 4).map(entry => publish(entry.event, { relays: entry.relays, sources: entry.sources }).catch(report)));
            }
        }
        finally {
            retrying = false;
        }
    };
    const timer = setInterval(() => { void retryPending().catch(report); }, 5000);
    return {
        publish, retryPending,
        setRelays(urls) {
            if (stopped)
                throw new Error('Private publisher is closed');
            relays = [...urls];
            void retryPending().catch(report);
        },
        getRelayStats: () => [...active.values()].flatMap(({ runtime }) => runtime.getRelayStats()),
        addSource(source) {
            if (stopped)
                throw new Error('Private publisher is closed');
            sources.set(source.id, publishOnly(source));
            void retryPending().catch(report);
        },
        removeSource(id) {
            sources.delete(id);
            for (const { runtime } of active.values())
                runtime.removeSource(id);
        },
        async close() {
            stopped = true;
            clearInterval(timer);
            await Promise.allSettled([...active.values()].map(({ runtime }) => runtime.close()));
            await Promise.allSettled([...active.values()].map(({ result }) => result));
        },
    };
}
//# sourceMappingURL=private-relay-publisher.js.map