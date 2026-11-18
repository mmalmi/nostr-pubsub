import { RateLimitAwarePool, withPublishRateLimit } from './relay-rate-limit.js';
import { verifyEvent } from 'nostr-tools/pure';
import { matchFilter, matchFilters } from 'nostr-tools/filter';
import { createSimplePoolNostrRelayVerificationBoundary } from './simple-pool-relay-transport.js';
import { verifyNostrEvent, validateQueryOptions } from './types.js';
import { RuntimeReconciliation } from './runtime-reconciliation.js';
import { abortError, expired, selectStoredEvents } from './runtime-store.js';
const MAX_TRANSIENT_RESULTS = 256;
const MAX_TIMER_MS = 2_147_483_647;
/** Batches OR filters without merging their fields, preserving recipient/author intersections. */
export class RuntimeRelays {
    options;
    pool;
    boundary;
    reconciliation;
    relays;
    batches = new Set();
    transientQueries = new Set();
    pending = [];
    timer;
    stopped = false;
    serial = 0;
    maxFilters;
    maxBytes;
    constructor(options) {
        this.options = options;
        if (options.reconciliation && options.store)
            this.reconciliation = new RuntimeReconciliation(options.reconciliation, options.store);
        this.boundary = createSimplePoolNostrRelayVerificationBoundary(boundedVerifier(options.verifyEvent ?? verifyEvent));
        this.relays = normalizeRelays(options.relays ?? []);
        this.maxFilters = positive(options.maxFiltersPerBatch, 20);
        this.maxBytes = positive(options.maxFilterBytesPerBatch, 32768);
        this.pool = new RateLimitAwarePool({
            maxWaitForConnection: options.historyTimeoutMs ?? 3000,
            websocketImplementation: options.websocketImplementation,
            verifyEvent: (event) => this.boundary.verifyEvent(event),
            // Own reconnect so replay overlaps the last timestamp instead of skipping its second.
            enableReconnect: false,
            // Authenticate explicitly below. nostr-tools' fire-and-forget automatic
            // AUTH can reject without a handler when a short-lived scope closes.
        });
    }
    urls() { return [...this.relays]; }
    stats() {
        const statuses = this.pool.listConnectionStatus();
        return this.relays.map((url) => ({ url, connected: statuses.get(url) ?? false }));
    }
    count() { return [...this.batches].reduce((total, batch) => total + batch.links.size, 0); }
    transientCount() { return this.transientQueries.size; }
    async queryTransient(relay, filters, options) {
        validateQueryOptions(options);
        if (options.signal?.aborted)
            throw abortError(options.signal.reason);
        if (filters.length === 0)
            throw new RangeError('Nostr queries require at least one filter');
        const limit = options.limit ?? MAX_TRANSIENT_RESULTS;
        if (limit < 1 || limit > MAX_TRANSIENT_RESULTS)
            throw new RangeError('Transient query limit must be between 1 and 256');
        const deadline = options.deadline ?? Date.now() + (this.options.historyTimeoutMs ?? 10_000);
        if (deadline <= Date.now())
            return { events: [], complete: false, reason: 'timeout', sources: [] };
        const url = normalizeRelays([relay])[0];
        const boundedFilters = filters.map((filter) => {
            validateQueryOptions({ limit: filter.limit });
            return { ...filter, limit: Math.min(filter.limit ?? limit, limit) };
        });
        return new Promise((resolve, reject) => {
            const events = new Map();
            const onClose = () => finish('closed');
            const onAbort = () => {
                cleanup();
                reject(abortError(options.signal?.reason));
            };
            const cleanup = () => {
                subscription.close();
                clearTimeout(timer);
                this.transientQueries.delete(onClose);
                options.signal?.removeEventListener('abort', onAbort);
            };
            const finish = (reason, error) => {
                cleanup();
                const complete = reason === 'eose';
                resolve({
                    events: selectStoredEvents(events.values(), boundedFilters, { limit }),
                    complete, reason, sources: [{ id: url, complete, ...(error && { error }) }],
                });
            };
            const subscription = this.subscribe(boundedFilters, (event) => {
                if (events.size < limit && !events.has(event.id) && !expired(event))
                    events.set(event.id, event);
            }, (_source, complete, error) => {
                finish(complete ? 'eose' : events.size ? 'closed' : 'unavailable', error);
            }, { relays: [url], isolated: true, transient: true });
            const timer = setTimeout(() => finish('timeout'), Math.min(MAX_TIMER_MS, Math.max(0, deadline - Date.now())));
            this.transientQueries.add(onClose);
            options.signal?.addEventListener('abort', onAbort, { once: true });
        });
    }
    subscribe(filters, event, state, { relays, isolated = false, transient = false } = {}) {
        if (this.stopped)
            throw new Error('Nostr runtime is closed');
        if (filters.length > this.maxFilters || bytes(filters) > this.maxBytes)
            throw new RangeError('Nostr subscription exceeds filter batch bounds');
        const entry = { isolated, transient, relays: relays === undefined ? undefined : normalizeRelays(relays), filters: structuredClone(filters), event, state, closed: false };
        this.pending.push(entry);
        if (!this.timer)
            this.timer = setTimeout(() => this.flush(), this.options.batchWindowMs ?? 10);
        return { close: () => {
                if (entry.closed)
                    return;
                entry.closed = true;
                const batch = entry.batch;
                if (!batch)
                    return;
                if (batch.entries.every((entry) => entry.closed)) {
                    this.closeBatch(batch);
                    return;
                }
                // UI unsubscriptions arrive as separate worker tasks. A microtask cannot
                // coalesce them, and reopening after each removal creates a REQ storm.
                if (!batch.reopenTimer)
                    batch.reopenTimer = setTimeout(() => {
                        batch.reopenTimer = undefined;
                        if (!batch.closed)
                            this.reopen(batch);
                    }, this.options.batchWindowMs ?? 10);
            } };
    }
    setRelays(urls) {
        const next = normalizeRelays(urls);
        const removed = this.relays.filter((url) => !next.includes(url));
        this.relays = next;
        for (const batch of this.batches)
            if (batch.entries[0]?.relays === undefined)
                this.reopen(batch);
        const scoped = new Set([...this.batches].flatMap((batch) => [...(batch.entries[0]?.relays ?? [])]));
        this.pool.close(removed.filter((url) => !scoped.has(url)));
    }
    publishAttempts(event, urls) {
        return (urls === undefined ? this.relays : normalizeRelays(urls)).map((url) => ({
            id: url,
            result: this.publishOne(url, event),
        }));
    }
    async publishOne(url, event) {
        const timeout = this.options.publishTimeoutMs ?? 5000;
        const relay = await this.pool.ensureRelay(url, { connectionTimeout: timeout });
        relay.publishTimeout = timeout;
        const publish = () => withPublishRateLimit(this.pool, url, event.id, () => relay.publish(event));
        try {
            await publish();
        }
        catch (error) {
            if (!errorText(error).startsWith('auth-required:') || !this.options.signAuthEvent)
                throw error;
            await relay.auth(async (template) => verifyNostrEvent(await this.options.signAuthEvent(url, template)));
            await publish();
        }
    }
    close() {
        this.stopped = true;
        for (const close of this.transientQueries)
            close();
        if (this.timer)
            clearTimeout(this.timer);
        this.pending = [];
        for (const batch of [...this.batches])
            this.closeBatch(batch);
        this.reconciliation?.close();
        this.pool.destroy();
    }
    flush() {
        this.timer = undefined;
        const entries = this.pending.filter((entry) => !entry.closed);
        this.pending = [];
        let group = [];
        for (const entry of entries) {
            const candidate = uniqueFilters([...group, entry]);
            if (group.length && (entry.isolated || group[0].isolated || JSON.stringify(group[0].relays) !== JSON.stringify(entry.relays) || candidate.length > this.maxFilters || bytes(candidate) > this.maxBytes)) {
                this.startBatch(group);
                group = [];
            }
            group.push(entry);
        }
        if (group.length)
            this.startBatch(group);
    }
    startBatch(entries) {
        const batch = { entries, links: new Map(), closed: false };
        for (const entry of entries)
            entry.batch = batch;
        this.batches.add(batch);
        this.reopen(batch);
    }
    reopen(batch) {
        if (batch.reopenTimer)
            clearTimeout(batch.reopenTimer);
        batch.reopenTimer = undefined;
        for (const link of batch.links.values())
            this.closeLink(link);
        batch.links.clear();
        batch.entries = batch.entries.filter((entry) => !entry.closed);
        if (!batch.entries.length) {
            this.closeBatch(batch);
            return;
        }
        for (const url of batch.entries[0].relays ?? this.relays) {
            const link = { generation: 0, attempts: 0, eosed: false, latest: new Map() };
            batch.links.set(url, link);
            void this.connect(batch, url, link);
        }
    }
    async connect(batch, url, link) {
        const generation = ++link.generation;
        const current = () => !this.stopped && !batch.closed && generation === link.generation;
        try {
            const relay = await this.pool.ensureRelay(url);
            if (!current())
                return;
            const transient = batch.entries[0].transient;
            const original = uniqueFilters(batch.entries);
            const keys = original.map(filterKey);
            const filters = original.map((filter, index) => {
                const latest = link.latest.get(keys[index]);
                return link.eosed && latest !== undefined
                    ? { ...filter, since: Math.max(filter.since ?? 0, latest - 1) }
                    : structuredClone(filter);
            });
            // prepareSubscription is public. Sending REQ ourselves avoids nostr-tools' timer
            // which labels a timeout as EOSE; only a relay's actual EOSE is complete here.
            const deliver = (raw) => {
                if (!current())
                    return;
                const event = this.boundary.admitEvent(raw);
                for (const [index, filter] of original.entries())
                    if (matchFilter(filter, event)) {
                        const key = keys[index];
                        link.latest.set(key, Math.max(link.latest.get(key) ?? 0, event.created_at));
                    }
                for (const entry of batch.entries)
                    if (!entry.closed && matchFilters(entry.filters, event))
                        entry.event(event, url);
            };
            const subscription = relay.prepareSubscription(filters, {
                id: `pubsub-${++this.serial}`,
                onevent: deliver,
                oneose: () => {
                    if (!current())
                        return;
                    link.eosed = true;
                    link.attempts = 0;
                    for (const entry of batch.entries)
                        if (!entry.closed)
                            entry.state(url, true);
                },
                onclose: (reason) => {
                    if (!current())
                        return;
                    if (!transient && reason.startsWith('auth-required:') && this.options.signAuthEvent && !link.authenticating) {
                        link.authenticating = true;
                        // AUTH is initial setup, not a failed historical result. Keep the
                        // caller's deadline active while awaiting its acknowledgment.
                        void relay.auth(async (template) => verifyNostrEvent(await this.options.signAuthEvent(url, template)))
                            .then(() => { if (current())
                            void this.connect(batch, url, link); })
                            .catch(error => { if (current())
                            this.retry(batch, url, link, errorText(error)); });
                    }
                    else
                        this.retry(batch, url, link, reason);
                },
            });
            relay.ongoingOperations++;
            relay.idleSince = undefined;
            link.close = () => { if (relay.openSubs.has(subscription.id))
                subscription.close('Nostr runtime subscription closed'); };
            await relay.send(JSON.stringify(['REQ', subscription.id, ...filters]));
            if (current() && !transient && this.reconciliation) {
                const stop = this.reconciliation.start(relay, original, deliver);
                const close = link.close;
                link.close = () => { stop(); close?.(); };
            }
        }
        catch (error) {
            if (current())
                this.retry(batch, url, link, errorText(error));
        }
    }
    retry(batch, url, link, reason) {
        this.closeLink(link);
        for (const entry of batch.entries)
            if (!entry.closed)
                entry.state(url, false, reason);
        if (batch.closed)
            return;
        const delay = Math.min(30000, (this.options.reconnectDelayMs ?? 500) * 2 ** Math.min(link.attempts++, 6));
        link.timer = setTimeout(() => { link.timer = undefined; void this.connect(batch, url, link); }, delay);
    }
    closeLink(link) {
        link.generation++;
        if (link.timer)
            clearTimeout(link.timer);
        link.timer = undefined;
        const close = link.close;
        link.close = undefined;
        close?.();
    }
    closeBatch(batch) {
        batch.closed = true;
        if (batch.reopenTimer)
            clearTimeout(batch.reopenTimer);
        batch.reopenTimer = undefined;
        for (const link of batch.links.values())
            this.closeLink(link);
        batch.links.clear();
        this.batches.delete(batch);
    }
}
function uniqueFilters(entries) {
    const filters = new Map();
    for (const entry of entries)
        if (!entry.closed)
            for (const filter of entry.filters)
                filters.set(filterKey(filter), filter);
    return [...filters.values()];
}
export function filterKey(filter) {
    return JSON.stringify(Object.fromEntries(Object.entries(filter).filter(([, value]) => value !== undefined).sort(([a], [b]) => a.localeCompare(b))));
}
function bytes(filters) { return new TextEncoder().encode(JSON.stringify(filters)).byteLength; }
function positive(value, fallback) {
    const result = value ?? fallback;
    if (!Number.isSafeInteger(result) || result < 1)
        throw new RangeError('Invalid runtime bounds');
    return result;
}
function normalizeRelays(urls) {
    return [...new Set(urls.map((url) => {
            const parsed = new URL(url);
            if (parsed.protocol !== 'ws:' && parsed.protocol !== 'wss:')
                throw new Error('Nostr relay must use ws or wss');
            return parsed.toString();
        }))];
}
function errorText(error) { return error instanceof Error ? error.message : String(error); }
/** Bound duplicate verification work without trusting event IDs or public verified markers. */
function boundedVerifier(verify) {
    const known = new Map();
    let retainedBytes = 0;
    return (event) => {
        const key = JSON.stringify([event.id, event.pubkey, event.sig, event.kind, event.created_at, event.tags, event.content]);
        if (known.get(event.id) === key)
            return true;
        if (!verify(event))
            return false;
        const previous = known.get(event.id);
        if (previous)
            retainedBytes -= previous.length * 2;
        known.delete(event.id);
        known.set(event.id, key);
        retainedBytes += key.length * 2;
        while (known.size > 2048 || retainedBytes > 8 * 1024 * 1024) {
            const id = known.keys().next().value;
            retainedBytes -= known.get(id).length * 2;
            known.delete(id);
        }
        return true;
    };
}
//# sourceMappingURL=runtime-relays.js.map