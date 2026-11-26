import { matchFilter } from 'nostr-tools/filter';
const BLOCK_TARGET = 256;
const BLOCK_SPLIT = 512;
export function selectStoredEvents(events, filters, options = {}) {
    const nowSeconds = Date.now() / 1000;
    const ordered = [...events].filter((event) => !expired(event, nowSeconds)).sort(eventOrder);
    const result = new Map();
    for (const filter of filters) {
        let count = 0;
        for (const event of ordered) {
            if (options.signal?.aborted)
                throw abortError(options.signal.reason);
            if (filter.limit !== undefined && count >= filter.limit)
                break;
            if (matchFilter(filter, event)) {
                result.set(event.id, event);
                count++;
            }
        }
    }
    const values = [...result.values()];
    if (filters.length > 1 && values.length > 1)
        values.sort(eventOrder);
    return options.limit === undefined ? values : values.slice(0, options.limit);
}
export function eventOrder(a, b) {
    return b.created_at - a.created_at || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
}
function parseExpiration(event) {
    const value = event.tags.find((tag) => tag[0] === 'expiration')?.[1];
    return value !== undefined && /^\d+$/.test(value) ? Number(value) : undefined;
}
export function expired(event, nowSeconds = Date.now() / 1000) {
    const at = parseExpiration(event);
    return at !== undefined && at <= nowSeconds;
}
export function abortError(reason) {
    return new DOMException(typeof reason === 'string' ? reason : 'Nostr operation cancelled', 'AbortError');
}
function replacementIdentifier(event) {
    if (event.kind === 0 || event.kind === 3 || (event.kind >= 10000 && event.kind < 20000))
        return '';
    if (event.kind >= 30000 && event.kind < 40000)
        return event.tags.find(tag => tag[0] === 'd')?.[1] ?? '';
}
export function replacementFilter(event) {
    const identifier = replacementIdentifier(event);
    // Missing and empty d tags share an address but only the latter matches a #d filter.
    if (identifier !== undefined)
        return { authors: [event.pubkey], kinds: [event.kind], ...(identifier ? { '#d': [identifier] } : {}) };
}
export function eventAddress(event) {
    const identifier = replacementIdentifier(event);
    if (identifier !== undefined)
        return `${event.kind}:${event.pubkey}:${identifier}`;
}
function deletedBy(event, deletion) {
    if (deletion.kind !== 5 || deletion.pubkey !== event.pubkey || deletion.created_at < event.created_at || event.kind === 5)
        return false;
    const key = eventAddress(event);
    return deletion.tags.some((tag) => (tag[0] === 'e' && tag[1] === event.id) || (tag[0] === 'a' && key !== undefined && tag[1] === key));
}
/** Called serially by a runtime after signature admission. Deletion events are retained as tombstones. */
export async function storeRuntimeEvent(store, event) {
    return await admitRuntimeEvent(store, event) === 'admitted';
}
/** Preserve rejection reasons so historical callers can opt into superseded versions only. */
export async function admitRuntimeEvent(store, event) {
    if (expired(event))
        return 'rejected';
    if (event.kind >= 20000 && event.kind < 30000)
        return 'admitted';
    const key = eventAddress(event);
    const ownDeletions = await store.query([{ authors: [event.pubkey], kinds: [5], '#e': [event.id] },
        ...(key ? [{ authors: [event.pubkey], kinds: [5], '#a': [key] }] : [])]);
    if (ownDeletions.some((deletion) => deletedBy(event, deletion)))
        return 'rejected';
    const replacement = replacementFilter(event);
    if (replacement) {
        const previous = (await store.query([replacement])).filter((old) => eventAddress(old) === key);
        // Older stores may already hold multiple winners split by incidental d tags.
        const winner = previous.reduce((best, old) => eventOrder(old, best) < 0 ? old : best, event);
        const stale = previous.filter(old => old.id !== winner.id).map(old => old.id);
        if (stale.length)
            await store.delete(stale);
        if (winner.id !== event.id)
            return 'superseded';
    }
    if (event.kind === 5) {
        const filters = [];
        const ids = event.tags.filter((tag) => tag[0] === 'e' && tag[1]).map((tag) => tag[1]);
        if (ids.length)
            filters.push({ ids, authors: [event.pubkey] });
        for (const tag of event.tags) {
            if (tag[0] !== 'a' || !tag[1])
                continue;
            const [kind, author, ...identifier] = tag[1].split(':');
            if (author !== event.pubkey || !/^\d+$/.test(kind))
                continue;
            filters.push({ kinds: [Number(kind)], authors: [author], ...(Number(kind) >= 30000 && identifier.join(':') ? { '#d': [identifier.join(':')] } : {}) });
        }
        const previous = filters.length ? await store.query(filters) : [];
        await store.delete(previous.filter((old) => deletedBy(old, event)).map((old) => old.id));
    }
    await store.put(event);
    return 'admitted';
}
function lowerBound(length, before) {
    let low = 0, high = length;
    while (low < high) {
        const mid = (low + high) >>> 1;
        if (before(mid))
            low = mid + 1;
        else
            high = mid;
    }
    return low;
}
function indexKeys(event) {
    return [`k:${event.kind}`, `a:${event.pubkey}`, `ak:${event.pubkey}:${event.kind}`];
}
function unique(values) { return values.length <= 1 ? [...values] : [...new Set(values)]; }
/** Bounded default cache. Supply a persistent store for offline operation across reloads. */
export class MemoryEventStore {
    maxEvents;
    maxPending;
    events = new Map();
    index = new Map();
    /** Non-empty blocks kept in `eventOrder` so live inserts and evictions avoid full-store shifts. */
    blocks = [];
    pending = new Map();
    pendingScopeCount = 0;
    constructor(maxEvents = 20000, maxPending = 1000) {
        this.maxEvents = maxEvents;
        this.maxPending = maxPending;
        if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || !Number.isSafeInteger(maxPending) || maxPending < 1)
            throw new RangeError('Invalid event store bounds');
    }
    async query(filters, options = {}) {
        const nowSeconds = Date.now() / 1000;
        const result = new Map();
        for (const filter of filters)
            this.queryFilter(filter, nowSeconds, options, result);
        const values = [...result.values()];
        if (filters.length > 1 && values.length > 1)
            values.sort(eventOrder);
        return options.limit === undefined ? values : values.slice(0, options.limit);
    }
    async put(event) {
        const previous = this.events.get(event.id);
        if (!previous && this.events.size >= this.maxEvents) {
            const victim = this.oldestEvictable();
            if (!victim)
                throw new Error('Event store retention capacity exhausted');
            this.remove(victim);
        }
        if (previous)
            this.remove(previous);
        const stored = structuredClone(event);
        const entry = { event: stored, expiresAt: parseExpiration(stored) };
        this.events.set(stored.id, entry);
        for (const key of indexKeys(stored)) {
            let bucket = this.index.get(key);
            if (!bucket) {
                bucket = new Set();
                this.index.set(key, bucket);
            }
            bucket.add(entry);
        }
        this.insertBlock(entry);
    }
    async delete(ids) {
        for (const id of ids) {
            const entry = this.events.get(id);
            if (entry)
                this.remove(entry);
        }
    }
    queryFilter(filter, nowSeconds, options, result) {
        if (options.signal?.aborted)
            throw abortError(options.signal.reason);
        const limit = filter.limit;
        if ((limit !== undefined && limit <= 0) || filter.ids?.length === 0 || filter.kinds?.length === 0 || filter.authors?.length === 0)
            return;
        const indexed = this.selectCandidates(filter);
        if (indexed !== undefined) {
            if (indexed.count === 0)
                return;
            if (filter.ids || indexed.count <= BLOCK_SPLIT || (limit === undefined && indexed.count < (this.events.size >>> 1))) {
                const matched = [];
                for (const bucket of indexed.buckets)
                    for (const entry of bucket) {
                        if (options.signal?.aborted)
                            throw abortError(options.signal.reason);
                        if ((entry.expiresAt === undefined || entry.expiresAt > nowSeconds) && matchFilter(filter, entry.event))
                            matched.push(entry.event);
                    }
                if (matched.length > 1)
                    matched.sort(eventOrder);
                for (const event of limit === undefined ? matched : matched.slice(0, limit))
                    result.set(event.id, event);
                return;
            }
        }
        const hasUntil = Boolean(filter.until), hasSince = Boolean(filter.since);
        const startBlock = hasUntil ? lowerBound(this.blocks.length, (i) => this.blocks[i][this.blocks[i].length - 1].event.created_at > filter.until) : 0;
        let count = 0;
        for (let blockIndex = startBlock; blockIndex < this.blocks.length; blockIndex++) {
            const block = this.blocks[blockIndex];
            const start = blockIndex === startBlock && hasUntil ? lowerBound(block.length, (i) => block[i].event.created_at > filter.until) : 0;
            for (let index = start; index < block.length; index++) {
                if (options.signal?.aborted)
                    throw abortError(options.signal.reason);
                const entry = block[index];
                if (hasSince && entry.event.created_at < filter.since)
                    return;
                if ((entry.expiresAt === undefined || entry.expiresAt > nowSeconds) && matchFilter(filter, entry.event)) {
                    result.set(entry.event.id, entry.event);
                    if (limit !== undefined && ++count >= limit)
                        return;
                }
            }
        }
    }
    selectCandidates(filter) {
        if (filter.ids) {
            const entries = [];
            for (const id of unique(filter.ids)) {
                const entry = this.events.get(id);
                if (entry)
                    entries.push(entry);
            }
            return { count: entries.length, buckets: [entries] };
        }
        const authors = filter.authors ? unique(filter.authors) : undefined;
        const kinds = filter.kinds ? unique(filter.kinds) : undefined;
        const plans = [];
        if (authors && kinds && authors.length * kinds.length <= 256)
            plans.push(authors.flatMap((a) => kinds.map((k) => `ak:${a}:${k}`)));
        else {
            if (authors && authors.length <= 256)
                plans.push(authors.map((a) => `a:${a}`));
            if (kinds && kinds.length <= 32)
                plans.push(kinds.map((k) => `k:${k}`));
        }
        let best;
        for (const keys of plans) {
            let count = 0;
            const buckets = [];
            for (const key of keys) {
                const bucket = this.index.get(key);
                if (bucket) {
                    count += bucket.size;
                    buckets.push(bucket);
                }
            }
            if (!best || count < best.count)
                best = { count, buckets };
        }
        return best;
    }
    oldestEvictable() {
        for (let blockIndex = this.blocks.length - 1; blockIndex >= 0; blockIndex--) {
            const block = this.blocks[blockIndex];
            for (let index = block.length - 1; index >= 0; index--) {
                const entry = block[index];
                if (entry.event.kind !== 5 && !this.pending.has(entry.event.id))
                    return entry;
            }
        }
    }
    remove(entry) {
        this.events.delete(entry.event.id);
        for (const key of indexKeys(entry.event)) {
            const bucket = this.index.get(key);
            if (bucket && bucket.delete(entry) && !bucket.size)
                this.index.delete(key);
        }
        if (!this.blocks.length)
            return;
        const blockIndex = this.findBlock(entry.event);
        const block = this.blocks[blockIndex];
        const index = lowerBound(block.length, (i) => eventOrder(block[i].event, entry.event) < 0);
        if (block[index] === entry) {
            block.splice(index, 1);
            if (!block.length)
                this.blocks.splice(blockIndex, 1);
            return;
        }
        const entries = [...this.events.values()].sort((a, b) => eventOrder(a.event, b.event));
        this.blocks = [];
        for (let offset = 0; offset < entries.length; offset += BLOCK_TARGET)
            this.blocks.push(entries.slice(offset, offset + BLOCK_TARGET));
    }
    findBlock(event) {
        return lowerBound(this.blocks.length - 1, (i) => eventOrder(this.blocks[i][this.blocks[i].length - 1].event, event) < 0);
    }
    insertBlock(entry) {
        if (!this.blocks.length) {
            this.blocks.push([entry]);
            return;
        }
        const blockIndex = this.findBlock(entry.event);
        const block = this.blocks[blockIndex];
        block.splice(lowerBound(block.length, (i) => eventOrder(block[i].event, entry.event) <= 0), 0, entry);
        if (block.length >= BLOCK_SPLIT)
            this.blocks.splice(blockIndex + 1, 0, block.splice(BLOCK_TARGET));
    }
    async updatePending(id, update) {
        const current = structuredClone(this.pending.get(id));
        const next = update(current);
        if (next === current)
            return;
        if (next)
            await this.putPending(next);
        else
            await this.deletePending(id);
    }
    async listPending() { return [...this.pending.values()].map((entry) => structuredClone(entry)); }
    async putPending(entry) {
        const previous = this.pending.get(entry.event.id);
        const count = this.pendingScopeCount + pendingCount(entry) - pendingCount(previous);
        if (count > this.maxPending)
            throw new Error('Outbox capacity exhausted');
        this.pending.set(entry.event.id, structuredClone(entry));
        this.pendingScopeCount = count;
    }
    async deletePending(id) {
        this.pendingScopeCount -= pendingCount(this.pending.get(id));
        this.pending.delete(id);
    }
}
function pendingCount(entry) {
    return entry ? 1 + (entry.additionalScopes?.length ?? 0) : 0;
}
//# sourceMappingURL=runtime-store.js.map