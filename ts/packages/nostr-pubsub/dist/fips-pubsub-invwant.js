/** Bounded global WANT selection shared by live and historical inventories. */
export class FipsPubsubInvWantState {
    maxEvents;
    maxAlternatives;
    pending = new Map();
    order = [];
    constructor(maxEvents, maxAlternatives) {
        this.maxEvents = maxEvents;
        this.maxAlternatives = maxAlternatives;
    }
    accept(peerId, message, validSubscriptionIds, nowMs) {
        const existing = this.pending.get(message.eventId);
        if (existing !== undefined) {
            if (existing.eventKind !== message.eventKind ||
                existing.payloadBytes !== message.payloadBytes)
                return undefined;
            const provider = [existing.selected, ...existing.alternatives]
                .find((candidate) => candidate.peerId === peerId);
            if (provider !== undefined) {
                for (const id of validSubscriptionIds)
                    provider.subscriptionIds.add(id);
            }
            else if (existing.alternatives.length < this.maxAlternatives) {
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
            eventKind: message.eventKind,
            payloadBytes: message.payloadBytes,
            hopLimit: message.hopLimit,
            requestedAtMs: nowMs,
        });
        this.order.push(message.eventId);
        this.trim();
        return { peerId, eventId: message.eventId };
    }
    complete(peerId, subscriptionId, eventId, eventKind, payloadBytes) {
        const pending = this.pending.get(eventId);
        if (pending === undefined ||
            pending.selected.peerId !== peerId ||
            !pending.selected.subscriptionIds.has(subscriptionId) ||
            pending.eventKind !== eventKind ||
            pending.payloadBytes !== payloadBytes)
            return undefined;
        this.delete(eventId);
        return Math.max(0, pending.hopLimit - 1);
    }
    retryDue(nowMs, retryAfterMs) {
        const retries = [];
        for (const [eventId, pending] of this.pending) {
            if (nowMs - pending.requestedAtMs < retryAfterMs)
                continue;
            const next = pending.alternatives.shift();
            if (next === undefined) {
                this.delete(eventId);
                continue;
            }
            pending.selected = next;
            pending.requestedAtMs = nowMs;
            retries.push({ peerId: next.peerId, eventId });
        }
        return retries;
    }
    removeSubscription(subscriptionId) {
        for (const [eventId, pending] of this.pending) {
            pending.selected.subscriptionIds.delete(subscriptionId);
            for (const provider of pending.alternatives) {
                provider.subscriptionIds.delete(subscriptionId);
            }
            pending.alternatives = pending.alternatives
                .filter((provider) => provider.subscriptionIds.size > 0);
            if (pending.selected.subscriptionIds.size > 0)
                continue;
            const next = pending.alternatives.shift();
            if (next === undefined)
                this.delete(eventId);
            else
                pending.selected = next;
        }
    }
    dropPeer(peerId) {
        for (const [eventId, pending] of this.pending) {
            pending.alternatives = pending.alternatives
                .filter((provider) => provider.peerId !== peerId);
            if (pending.selected.peerId !== peerId)
                continue;
            const next = pending.alternatives.shift();
            if (next === undefined)
                this.delete(eventId);
            else {
                pending.selected = next;
                pending.requestedAtMs = 0;
            }
        }
    }
    clear() {
        this.pending.clear();
        this.order.length = 0;
    }
    trim() {
        while (this.pending.size > this.maxEvents) {
            const oldest = this.order.shift();
            if (oldest !== undefined)
                this.pending.delete(oldest);
        }
    }
    delete(eventId) {
        this.pending.delete(eventId);
        const index = this.order.indexOf(eventId);
        if (index >= 0)
            this.order.splice(index, 1);
    }
}
//# sourceMappingURL=fips-pubsub-invwant.js.map