import { retainOrder, saturatingAdd } from './mesh-state.js';
import type { NostrVerifiedEvent } from './types.js';

export const DEFAULT_INV_WANT_MAX_CACHE_BYTES = 16 * 1024 * 1024;

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

interface CachedEvent {
  event: NostrVerifiedEvent;
  expiresAtMs: number;
  payloadBytes: number;
}

/** Exact payload accounting plus count- and byte-bounded FIFO eviction. */
export class BoundedEventCache {
  private readonly events = new Map<string, CachedEvent>();
  private readonly order: string[] = [];
  private retainedBytes = 0;

  constructor(
    private readonly maxEvents: number,
    private readonly maxBytes: number,
    private readonly ttlMs: number,
  ) {}

  get size(): number {
    return this.events.size;
  }

  get payloadBytes(): number {
    return this.retainedBytes;
  }

  has(eventId: string): boolean {
    return this.events.has(eventId);
  }

  get(eventId: string): NostrVerifiedEvent | undefined {
    return this.events.get(eventId)?.event;
  }

  store(event: NostrVerifiedEvent, payloadBytes: number, nowMs: number): void {
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
    while (
      this.events.size >= this.maxEvents ||
      saturatingAdd(this.retainedBytes, payloadBytes) > this.maxBytes
    ) {
      if (!this.evictOldest()) break;
    }
    this.order.push(event.id);
    this.retainedBytes = saturatingAdd(this.retainedBytes, payloadBytes);
    this.events.set(event.id, {
      event,
      payloadBytes,
      expiresAtMs: saturatingAdd(nowMs, this.ttlMs),
    });
  }

  prune(nowMs: number): void {
    for (const [eventId, cached] of this.events) {
      if (cached.expiresAtMs > nowMs) continue;
      this.events.delete(eventId);
      this.retainedBytes -= cached.payloadBytes;
    }
    retainOrder(this.order, this.events);
  }

  private evictOldest(): boolean {
    while (this.order.length > 0) {
      const oldest = this.order.shift();
      if (oldest === undefined) break;
      const cached = this.events.get(oldest);
      if (cached === undefined) continue;
      this.events.delete(oldest);
      this.retainedBytes -= cached.payloadBytes;
      return true;
    }
    return false;
  }
}
