import type { FipsPubsubWireMessage } from './wire.js';

interface InventoryProvider {
  peerId: string;
  subscriptionIds: Set<string>;
}

interface PendingWant {
  selected: InventoryProvider;
  alternatives: InventoryProvider[];
  previous: InventoryProvider[];
  expiresAtMs: number;
  eventKind: number;
  payloadBytes: number;
  hopLimit: number;
  requestedAtMs: number;
  needsRequest: boolean;
}

export interface WantRequest {
  peerId: string;
  eventId: string;
}

// A slow main thread must not turn a queued reliable TCP response into a miss.
const WANT_LIFETIME_MS = 10_000;

/** Bounded global WANT selection shared by live and historical inventories. */
export class FipsPubsubInvWantState {
  private readonly pending = new Map<string, PendingWant>();
  private readonly order: string[] = [];

  constructor(
    private readonly maxEvents: number,
    private readonly maxAlternatives: number,
  ) {}

  accept(
    peerId: string,
    message: Extract<FipsPubsubWireMessage, { type: 'inv' }>,
    validSubscriptionIds: string[],
    nowMs: number,
  ): WantRequest | undefined {
    const existing = this.pending.get(message.eventId);
    if (existing !== undefined) {
      if (
        existing.eventKind !== message.eventKind ||
        existing.payloadBytes !== message.payloadBytes
      ) return undefined;
      const provider = [existing.selected, ...existing.alternatives, ...existing.previous]
        .find((candidate) => candidate.peerId === peerId);
      if (provider !== undefined) {
        for (const id of validSubscriptionIds) provider.subscriptionIds.add(id);
      } else if (existing.alternatives.length + existing.previous.length < this.maxAlternatives) {
        existing.alternatives.push({
          peerId,
          subscriptionIds: new Set(validSubscriptionIds),
        });
      }
      return undefined;
    }

    this.pending.set(message.eventId, {
      selected: { peerId, subscriptionIds: new Set(validSubscriptionIds) },
      alternatives: [],
      previous: [],
      expiresAtMs: nowMs + WANT_LIFETIME_MS,
      eventKind: message.eventKind,
      payloadBytes: message.payloadBytes,
      hopLimit: message.hopLimit,
      requestedAtMs: nowMs,
      needsRequest: false,
    });
    this.order.push(message.eventId);
    this.trim();
    return { peerId, eventId: message.eventId };
  }

  complete(
    peerId: string,
    subscriptionId: string,
    eventId: string,
    eventKind: number,
    payloadBytes: number,
  ): number | undefined {
    const pending = this.pending.get(eventId);
    if (
      pending === undefined ||
      ![pending.selected, ...pending.previous].some(provider =>
        provider.peerId === peerId && provider.subscriptionIds.has(subscriptionId)) ||
      pending.eventKind !== eventKind ||
      pending.payloadBytes !== payloadBytes
    ) return undefined;
    this.delete(eventId);
    return Math.max(0, pending.hopLimit - 1);
  }

  retryDue(nowMs: number, retryAfterMs: number): WantRequest[] {
    const retries: WantRequest[] = [];
    for (const [eventId, pending] of this.pending) {
      if (nowMs >= pending.expiresAtMs) { this.delete(eventId); continue; }
      if (!pending.needsRequest && nowMs - pending.requestedAtMs < retryAfterMs) continue;
      if (!pending.needsRequest) {
        const next = pending.alternatives.shift();
        // Retain evidence for the in-flight TCP response even when no alternate
        // exists. A retry interval is not proof that the original request failed.
        if (next === undefined) continue;
        pending.previous.push(pending.selected);
        pending.selected = next;
      }
      pending.needsRequest = false;
      pending.requestedAtMs = nowMs;
      retries.push({ peerId: pending.selected.peerId, eventId });
    }
    return retries;
  }

  removeSubscription(subscriptionId: string): void {
    for (const [eventId, pending] of this.pending) {
      pending.selected.subscriptionIds.delete(subscriptionId);
      for (const provider of [...pending.alternatives, ...pending.previous]) {
        provider.subscriptionIds.delete(subscriptionId);
      }
      pending.alternatives = pending.alternatives.filter(provider => provider.subscriptionIds.size > 0);
      pending.previous = pending.previous.filter(provider => provider.subscriptionIds.size > 0);
      if (pending.selected.subscriptionIds.size > 0) continue;
      const next = pending.alternatives.shift() ?? pending.previous.shift();
      if (next === undefined) this.delete(eventId);
      else { pending.selected = next; pending.needsRequest = true; }
    }
  }

  dropPeer(peerId: string): void {
    for (const [eventId, pending] of this.pending) {
      pending.alternatives = pending.alternatives
        .filter((provider) => provider.peerId !== peerId);
      pending.previous = pending.previous.filter(provider => provider.peerId !== peerId);
      if (pending.selected.peerId !== peerId) continue;
      const next = pending.alternatives.shift() ?? pending.previous.shift();
      if (next === undefined) this.delete(eventId);
      else {
        pending.selected = next;
        pending.needsRequest = true;
      }
    }
  }

  clear(): void {
    this.pending.clear();
    this.order.length = 0;
  }

  private trim(): void {
    while (this.pending.size > this.maxEvents) {
      const oldest = this.order.shift();
      if (oldest !== undefined) this.pending.delete(oldest);
    }
  }

  private delete(eventId: string): void {
    this.pending.delete(eventId);
    const index = this.order.indexOf(eventId);
    if (index >= 0) this.order.splice(index, 1);
  }
}
