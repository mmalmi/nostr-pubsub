import { subscriptionFiltersMatch } from './filter.js';
import { FipsPubsubInvWantState } from './fips-pubsub-invwant.js';
import { rememberId } from './fips-pubsub-client-support.js';
import type { NostrFilter, NostrVerifiedEvent } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';

export interface CachedFipsPubsubEvent {
  event: NostrVerifiedEvent;
  sourcePeer?: string;
  hopLimit: number;
}

export interface FipsPubsubLocalSubscription {
  readonly id: string;
  readonly filters: NostrFilter[];
  readonly handler: (event: NostrVerifiedEvent, sourcePeer: string) => void;
  readonly peers: Set<string>;
  readonly pendingPeers: Set<string>;
  readonly recentIds: Set<string>;
  readonly recentOrder: string[];
}

export class FipsPubsubEventCache {
  private readonly events = new Map<string, CachedFipsPubsubEvent>();
  private readonly order: string[] = [];

  constructor(private readonly maximum: number) {}

  has(eventId: string): boolean {
    return this.events.has(eventId);
  }

  get(eventId: string): CachedFipsPubsubEvent | undefined {
    return this.events.get(eventId);
  }

  remember(event: NostrVerifiedEvent, sourcePeer: string | undefined, hopLimit: number): boolean {
    if (this.events.has(event.id)) return false;
    this.events.set(event.id, { event, sourcePeer, hopLimit });
    this.order.push(event.id);
    while (this.order.length > this.maximum) {
      const removed = this.order.shift();
      if (removed !== undefined) this.events.delete(removed);
    }
    return true;
  }

  replay(filters: NostrFilter[], limit: number): CachedFipsPubsubEvent[] {
    return this.order
      .map((eventId) => this.events.get(eventId))
      .filter((cached): cached is CachedFipsPubsubEvent =>
        cached !== undefined && subscriptionFiltersMatch(filters, cached.event))
      .slice(-limit);
  }

  clear(): void {
    this.events.clear();
    this.order.length = 0;
  }
}

export interface FipsPubsubProtocolContext {
  maxActiveSubscriptions: number;
  maxHops: number;
  invWant: FipsPubsubInvWantState;
  events: FipsPubsubEventCache;
  validSubscriptionIds(peerId: string, subscriptionIds: string[], eventId: string): string[];
  eventForWant(peerId: string, eventId: string):
    | { subscriptionId: string; event: NostrVerifiedEvent }
    | undefined;
  deliverEvent(peerId: string, event: NostrVerifiedEvent): boolean;
  forwardEvent(peerId: string, event: NostrVerifiedEvent, hopLimit: number): void;
  send(peerId: string, message: FipsPubsubWireMessage): void;
}

export function inventoryMessage(
  event: NostrVerifiedEvent,
  subscriptionIds: string[],
  hopLimit: number,
): Extract<FipsPubsubWireMessage, { type: 'inv' }> {
  return {
    type: 'inv',
    subscriptionIds: [...new Set(subscriptionIds)],
    eventId: event.id,
    eventKind: event.kind,
    payloadBytes: eventPayloadBytes(event),
    hopLimit,
  };
}

export function acceptInventory(
  context: FipsPubsubProtocolContext,
  peerId: string,
  message: Extract<FipsPubsubWireMessage, { type: 'inv' }>,
): void {
  if (
    context.events.has(message.eventId) ||
    message.subscriptionIds.length > context.maxActiveSubscriptions
  ) return;
  const valid = context.validSubscriptionIds(peerId, message.subscriptionIds, message.eventId);
  if (valid.length === 0) return;
  const want = context.invWant.accept(peerId, message, valid, Date.now());
  if (want !== undefined) context.send(want.peerId, { type: 'want', eventId: want.eventId });
}

export function answerWant(
  context: FipsPubsubProtocolContext,
  peerId: string,
  eventId: string,
): void {
  const answer = context.eventForWant(peerId, eventId);
  if (answer === undefined) return;
  context.send(peerId, {
    type: 'event',
    subscriptionId: answer.subscriptionId,
    event: answer.event,
  });
}

export function acceptEvent(
  context: FipsPubsubProtocolContext,
  peerId: string,
  message: Extract<FipsPubsubWireMessage, { type: 'event' }>,
): void {
  let hopLimit = context.maxHops;
  if (message.subscriptionId !== undefined) {
    const completed = context.invWant.complete(
      peerId,
      message.subscriptionId,
      message.event.id,
      message.event.kind,
      eventPayloadBytes(message.event),
    );
    if (completed === undefined) return;
    hopLimit = completed;
  }
  if (!context.deliverEvent(peerId, message.event)) return;
  if (context.events.remember(message.event, peerId, hopLimit)) {
    context.forwardEvent(peerId, message.event, hopLimit);
  }
}

export function retryPendingWants(context: FipsPubsubProtocolContext, nowMs: number): void {
  for (const retry of context.invWant.retryDue(nowMs, 500)) {
    context.send(retry.peerId, { type: 'want', eventId: retry.eventId });
  }
}

export function deliverSubscription(
  subscription: FipsPubsubLocalSubscription,
  event: NostrVerifiedEvent,
  sourcePeer: string,
  recentLimit: number,
  reportError: (error: unknown) => void,
): boolean {
  if (
    (!subscription.peers.has(sourcePeer) && !subscription.pendingPeers.has(sourcePeer)) ||
    !subscriptionFiltersMatch(subscription.filters, event) ||
    subscription.recentIds.has(event.id)
  ) return false;
  rememberId(subscription.recentIds, subscription.recentOrder, event.id, recentLimit);
  try {
    subscription.handler(event, sourcePeer);
  } catch (error) {
    reportError(error);
  }
  return true;
}

export function replayLocalSubscription(
  events: FipsPubsubEventCache,
  subscription: FipsPubsubLocalSubscription,
  replayLimit: number,
  recentLimit: number,
  reportError: (error: unknown) => void,
): void {
  for (const cached of events.replay(subscription.filters, replayLimit)) {
    if (cached.sourcePeer !== undefined) {
      deliverSubscription(subscription, cached.event, cached.sourcePeer, recentLimit, reportError);
    }
  }
}

function eventPayloadBytes(event: NostrVerifiedEvent): number {
  const wireEvent = {
    content: event.content,
    created_at: event.created_at,
    id: event.id,
    kind: event.kind,
    pubkey: event.pubkey,
    sig: event.sig,
    tags: event.tags,
  };
  return new TextEncoder().encode(JSON.stringify(wireEvent)).byteLength;
}
