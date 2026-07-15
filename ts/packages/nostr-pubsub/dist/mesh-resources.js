import { retainOrder, saturatingAdd } from './mesh-state.js';
export const DEFAULT_INV_WANT_MAX_CACHE_BYTES = 16 * 1024 * 1024;
/** Exact payload accounting plus count- and byte-bounded FIFO eviction. */
export class BoundedEventCache {
    maxEvents;
    maxBytes;
    ttlMs;
    events = new Map();
    order = [];
    retainedBytes = 0;
    constructor(maxEvents, maxBytes, ttlMs) {
        this.maxEvents = maxEvents;
        this.maxBytes = maxBytes;
        this.ttlMs = ttlMs;
    }
    get size() {
        return this.events.size;
    }
    get payloadBytes() {
        return this.retainedBytes;
    }
    has(eventId) {
        return this.events.has(eventId);
    }
    get(eventId) {
        return this.events.get(eventId)?.event;
    }
    orderedEvents() {
        return this.order.flatMap((eventId) => {
            const cached = this.events.get(eventId);
            return cached === undefined
                ? []
                : [{ event: cached.event, payloadBytes: cached.payloadBytes }];
        });
    }
    store(event, payloadBytes, nowMs) {
        const cached = this.events.get(event.id);
        if (cached !== undefined) {
            this.retainedBytes = this.retainedBytes - cached.payloadBytes + payloadBytes;
            this.events.set(event.id, {
                event,
                payloadBytes,
                expiresAtMs: saturatingAdd(nowMs, this.ttlMs),
            });
            return;
        }
        while (this.events.size >= this.maxEvents ||
            saturatingAdd(this.retainedBytes, payloadBytes) > this.maxBytes) {
            if (!this.evictOldest())
                break;
        }
        this.order.push(event.id);
        this.retainedBytes = saturatingAdd(this.retainedBytes, payloadBytes);
        this.events.set(event.id, {
            event,
            payloadBytes,
            expiresAtMs: saturatingAdd(nowMs, this.ttlMs),
        });
    }
    prune(nowMs) {
        for (const [eventId, cached] of this.events) {
            if (cached.expiresAtMs > nowMs)
                continue;
            this.events.delete(eventId);
            this.retainedBytes -= cached.payloadBytes;
        }
        retainOrder(this.order, this.events);
    }
    evictOldest() {
        while (this.order.length > 0) {
            const oldest = this.order.shift();
            if (oldest === undefined)
                break;
            const cached = this.events.get(oldest);
            if (cached === undefined)
                continue;
            this.events.delete(oldest);
            this.retainedBytes -= cached.payloadBytes;
            return true;
        }
        return false;
    }
}
//# sourceMappingURL=mesh-resources.js.map