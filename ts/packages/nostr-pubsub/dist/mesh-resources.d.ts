import type { NostrVerifiedEvent } from './types.js';
export declare const DEFAULT_INV_WANT_MAX_CACHE_BYTES: number;
/** Raw retained-state units for deterministic memory accounting. */
export interface InvWantMeshRetainedState {
    cachedEvents: number;
    cachedEventBytes: number;
    seenInventories: number;
    deliveredEvents: number;
    upstreamRoutes: number;
    transportDisruptedRoutePeers: number;
    pendingEvents: number;
    pendingPeers: number;
    forwardedWants: number;
    peerBehaviors: number;
}
export interface CachedInvWantEvent {
    event: NostrVerifiedEvent;
    payloadBytes: number;
}
/** Exact payload accounting plus count- and byte-bounded FIFO eviction. */
export declare class BoundedEventCache {
    private readonly maxEvents;
    private readonly maxBytes;
    private readonly ttlMs;
    private readonly events;
    private readonly order;
    private retainedBytes;
    constructor(maxEvents: number, maxBytes: number, ttlMs: number);
    get size(): number;
    get payloadBytes(): number;
    has(eventId: string): boolean;
    get(eventId: string): NostrVerifiedEvent | undefined;
    orderedEvents(): CachedInvWantEvent[];
    store(event: NostrVerifiedEvent, payloadBytes: number, nowMs: number): void;
    prune(nowMs: number): void;
    private evictOldest;
}
//# sourceMappingURL=mesh-resources.d.ts.map