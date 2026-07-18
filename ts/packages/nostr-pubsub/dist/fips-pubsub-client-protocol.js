import { subscriptionFiltersMatch } from './filter.js';
import { rememberId } from './fips-pubsub-client-support.js';
export class FipsPubsubEventCache {
    maximum;
    events = new Map();
    order = [];
    constructor(maximum) {
        this.maximum = maximum;
    }
    has(eventId) {
        return this.events.has(eventId);
    }
    get(eventId) {
        return this.events.get(eventId);
    }
    remember(event, sourcePeer, hopLimit) {
        if (this.events.has(event.id))
            return false;
        this.events.set(event.id, { event, sourcePeer, hopLimit });
        this.order.push(event.id);
        while (this.order.length > this.maximum) {
            const removed = this.order.shift();
            if (removed !== undefined)
                this.events.delete(removed);
        }
        return true;
    }
    replay(filters, limit) {
        return this.order
            .map((eventId) => this.events.get(eventId))
            .filter((cached) => cached !== undefined && subscriptionFiltersMatch(filters, cached.event))
            .slice(-limit);
    }
    clear() {
        this.events.clear();
        this.order.length = 0;
    }
}
export function inventoryMessage(event, subscriptionIds, hopLimit) {
    return {
        type: 'inv',
        subscriptionIds: [...new Set(subscriptionIds)],
        eventId: event.id,
        eventKind: event.kind,
        payloadBytes: eventPayloadBytes(event),
        hopLimit,
    };
}
export function acceptInventory(context, peerId, message) {
    if (context.events.has(message.eventId) ||
        message.subscriptionIds.length > context.maxActiveSubscriptions)
        return;
    const valid = context.validSubscriptionIds(peerId, message.subscriptionIds, message.eventId);
    if (valid.length === 0)
        return;
    const want = context.invWant.accept(peerId, message, valid, Date.now());
    if (want !== undefined)
        context.send(want.peerId, { type: 'want', eventId: want.eventId });
}
export function answerWant(context, peerId, eventId) {
    const answer = context.eventForWant(peerId, eventId);
    if (answer === undefined)
        return;
    context.send(peerId, {
        type: 'event',
        subscriptionId: answer.subscriptionId,
        event: answer.event,
    });
}
export function acceptEvent(context, peerId, message) {
    let hopLimit = context.maxHops;
    if (message.subscriptionId !== undefined) {
        const completed = context.invWant.complete(peerId, message.subscriptionId, message.event.id, message.event.kind, eventPayloadBytes(message.event));
        if (completed === undefined)
            return;
        hopLimit = completed;
    }
    if (!context.deliverEvent(peerId, message.event))
        return;
    if (context.events.remember(message.event, peerId, hopLimit)) {
        context.forwardEvent(peerId, message.event, hopLimit);
    }
}
export function retryPendingWants(context, nowMs) {
    for (const retry of context.invWant.retryDue(nowMs, 500)) {
        context.send(retry.peerId, { type: 'want', eventId: retry.eventId });
    }
}
export function deliverSubscription(subscription, event, sourcePeer, recentLimit, reportError) {
    if ((!subscription.peers.has(sourcePeer) && !subscription.pendingPeers.has(sourcePeer)) ||
        !subscriptionFiltersMatch(subscription.filters, event) ||
        subscription.recentIds.has(event.id))
        return false;
    rememberId(subscription.recentIds, subscription.recentOrder, event.id, recentLimit);
    try {
        subscription.handler(event, sourcePeer);
    }
    catch (error) {
        reportError(error);
    }
    return true;
}
export function replayLocalSubscription(events, subscription, replayLimit, recentLimit, reportError) {
    for (const cached of events.replay(subscription.filters, replayLimit)) {
        if (cached.sourcePeer !== undefined) {
            deliverSubscription(subscription, cached.event, cached.sourcePeer, recentLimit, reportError);
        }
    }
}
function eventPayloadBytes(event) {
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
//# sourceMappingURL=fips-pubsub-client-protocol.js.map