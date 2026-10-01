import { matchFilter } from 'nostr-tools/filter';
export function selectStoredEvents(events, filters, options = {}) {
    const ordered = [...events].filter((event) => !expired(event)).sort(eventOrder);
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
export function replacementFilter(event) {
    if (event.kind === 0 || event.kind === 3 || (event.kind >= 10000 && event.kind < 20000)) {
        return { authors: [event.pubkey], kinds: [event.kind] };
    }
    if (event.kind >= 30000 && event.kind < 40000) {
        const identifier = event.tags.find((t) => t[0] === 'd')?.[1] ?? '';
        // A missing d tag has the same address as d="" but does not match a #d filter.
        return { authors: [event.pubkey], kinds: [event.kind], ...(identifier ? { '#d': [identifier] } : {}) };
    }
}
function address(event) {
    return replacementFilter(event) ? `${event.kind}:${event.pubkey}:${event.tags.find((t) => t[0] === 'd')?.[1] ?? ''}` : undefined;
}
function deletedBy(event, deletion) {
    if (deletion.kind !== 5 || deletion.pubkey !== event.pubkey || deletion.created_at < event.created_at || event.kind === 5)
        return false;
    const key = address(event);
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
    const ownDeletions = await store.query([{ authors: [event.pubkey], kinds: [5], '#e': [event.id] },
        ...(address(event) ? [{ authors: [event.pubkey], kinds: [5], '#a': [address(event)] }] : [])]);
    if (ownDeletions.some((deletion) => deletedBy(event, deletion)))
        return 'rejected';
    const replacement = replacementFilter(event);
    if (replacement) {
        const previous = (await store.query([replacement])).filter((old) => address(old) === address(event));
        if (previous.some((old) => eventOrder(old, event) < 0))
            return 'superseded';
        // Retain only the NIP-01 winner, including the lower ID on timestamp ties.
        await store.delete(previous.filter((old) => old.id !== event.id).map((old) => old.id));
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
/** Bounded default cache. Supply a persistent store for offline operation across reloads. */
export class MemoryEventStore {
    maxEvents;
    maxPending;
    events = new Map();
    pending = new Map();
    constructor(maxEvents = 20000, maxPending = 1000) {
        this.maxEvents = maxEvents;
        this.maxPending = maxPending;
        if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || !Number.isSafeInteger(maxPending) || maxPending < 1)
            throw new RangeError('Invalid event store bounds');
    }
    async query(filters, options = {}) {
        return selectStoredEvents(this.events.values(), filters, options);
    }
    async put(event) {
        if (!this.events.has(event.id) && this.events.size >= this.maxEvents) {
            const removable = [...this.events.values()].filter((e) => e.kind !== 5 && !this.pending.has(e.id)).sort(eventOrder).pop();
            if (!removable)
                throw new Error('Event store retention capacity exhausted');
            this.events.delete(removable.id);
        }
        this.events.set(event.id, structuredClone(event));
    }
    async delete(ids) { for (const id of ids)
        this.events.delete(id); }
    async listPending() { return [...this.pending.values()].map((entry) => structuredClone(entry)); }
    async putPending(entry) {
        if (!this.pending.has(entry.event.id) && this.pending.size >= this.maxPending)
            throw new Error('Outbox capacity exhausted');
        this.pending.set(entry.event.id, structuredClone(entry));
    }
    async deletePending(id) { this.pending.delete(id); }
}
//# sourceMappingURL=runtime-store.js.map