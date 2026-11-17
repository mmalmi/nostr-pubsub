import { matchFilter } from 'nostr-tools/filter';
export function selectStoredEvents(events, filters, options = {}) {
    return selectOrderedEvents([...events].filter((event) => !expired(event)).sort(eventOrder), filters, options);
}
/** `ordered` holds unexpired events already sorted by `eventOrder`. */
function selectOrderedEvents(ordered, filters, options) {
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
    return [...result.values()].sort(eventOrder).slice(0, options.limit);
}
export function eventOrder(a, b) {
    return b.created_at - a.created_at || a.id.localeCompare(b.id);
}
export function expired(event) {
    const value = event.tags.find((tag) => tag[0] === 'expiration')?.[1];
    return value !== undefined && /^\d+$/.test(value) && Number(value) <= Date.now() / 1000;
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
/** First index whose event sorts at or after `event`, or strictly after it when `after` is set. */
function orderedIndex(ordered, event, after) {
    let low = 0;
    let high = ordered.length;
    while (low < high) {
        const middle = (low + high) >>> 1;
        const order = eventOrder(ordered[middle], event);
        if (order < 0 || (after && order === 0))
            low = middle + 1;
        else
            high = middle;
    }
    return low;
}
/** Bounded default cache. Supply a persistent store for offline operation across reloads. */
export class MemoryEventStore {
    maxEvents;
    maxPending;
    events = new Map();
    /** The stored events in `eventOrder`, kept current so a query need not sort the store; rebuilt when unset. */
    ordered;
    pending = new Map();
    pendingScopeCount = 0;
    constructor(maxEvents = 20000, maxPending = 1000) {
        this.maxEvents = maxEvents;
        this.maxPending = maxPending;
        if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || !Number.isSafeInteger(maxPending) || maxPending < 1)
            throw new RangeError('Invalid event store bounds');
    }
    async query(filters, options = {}) {
        return selectOrderedEvents(this.sorted().filter((event) => !expired(event)), filters, options);
    }
    async put(event) {
        const previous = this.events.get(event.id);
        if (!previous && this.events.size >= this.maxEvents) {
            const ordered = this.sorted();
            let last = ordered.length - 1;
            while (last >= 0 && (ordered[last].kind === 5 || this.pending.has(ordered[last].id)))
                last--;
            if (last < 0)
                throw new Error('Event store retention capacity exhausted');
            this.remove(ordered[last]);
        }
        const stored = structuredClone(event);
        this.events.set(stored.id, stored);
        if (!this.ordered)
            return;
        if (!previous)
            this.ordered.splice(orderedIndex(this.ordered, stored, true), 0, stored);
        // A rewritten ID keeps its place among equals; anything else is re-sorted on the next read.
        else if (eventOrder(previous, stored) !== 0 || !this.replaceOrdered(previous, stored))
            this.ordered = undefined;
    }
    async delete(ids) {
        for (const id of ids) {
            const event = this.events.get(id);
            if (event)
                this.remove(event);
        }
    }
    sorted() { return this.ordered ??= [...this.events.values()].sort(eventOrder); }
    remove(event) {
        this.events.delete(event.id);
        if (this.ordered && !this.replaceOrdered(event))
            this.ordered = undefined;
    }
    /** Swap or drop one stored object in place; false when the order no longer holds it where expected. */
    replaceOrdered(event, replacement) {
        const ordered = this.ordered;
        for (let index = orderedIndex(ordered, event, false); index < ordered.length && eventOrder(ordered[index], event) === 0; index++) {
            if (ordered[index] !== event)
                continue;
            if (replacement)
                ordered[index] = replacement;
            else
                ordered.splice(index, 1);
            return true;
        }
        return false;
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