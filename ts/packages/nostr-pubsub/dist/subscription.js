import { cloneFilter, subscriptionFiltersMatch } from './filter.js';
import { PubsubError } from './types.js';
export function defaultPubsubSubscriptionLimits() {
    return {
        maxPeers: 1024,
        maxSubscriptionsPerPeer: 64,
        maxFiltersPerSubscription: 16,
    };
}
export function createPeerSubscription(subscriptionId, filters) {
    return { subscriptionId, filters: filters.map(cloneFilter) };
}
class PeerSubscriptionSet {
    subscriptions = new Map();
    order = [];
    upsert(subscription, limits) {
        const subscriptionId = subscription.subscriptionId;
        removeFromArray(this.order, subscriptionId);
        this.order.push(subscriptionId);
        const replaced = this.subscriptions.has(subscriptionId);
        this.subscriptions.set(subscriptionId, subscription);
        return replaced ? undefined : this.evictOldestOverLimit(limits.maxSubscriptionsPerPeer);
    }
    remove(subscriptionId) {
        removeFromArray(this.order, subscriptionId);
        const removed = this.subscriptions.get(subscriptionId);
        this.subscriptions.delete(subscriptionId);
        return removed;
    }
    isEmpty() {
        return this.subscriptions.size === 0;
    }
    evictOldestOverLimit(limit) {
        while (this.subscriptions.size > limit) {
            const subscriptionId = this.order.shift();
            if (subscriptionId === undefined)
                break;
            const removed = this.subscriptions.get(subscriptionId);
            if (removed !== undefined) {
                this.subscriptions.delete(subscriptionId);
                return removed;
            }
        }
        return undefined;
    }
}
export class PubsubPeerSubscriptionStore {
    limitsValue;
    peers = new Map();
    peerOrder = [];
    constructor(limits = defaultPubsubSubscriptionLimits()) {
        this.limitsValue = { ...limits };
    }
    limits() {
        return { ...this.limitsValue };
    }
    peerCount() {
        return this.peers.size;
    }
    subscriptionCount() {
        let count = 0;
        for (const peer of this.peers.values()) {
            count += peer.subscriptions.size;
        }
        return count;
    }
    peerSubscriptionCount(peerId) {
        return this.peers.get(peerId)?.subscriptions.size ?? 0;
    }
    applyClientMessage(peerId, message) {
        if (!Array.isArray(message))
            return 'ignored';
        if (message[0] === 'REQ' && typeof message[1] === 'string') {
            const filters = message.slice(2).filter(isFilterLike).map((filter) => cloneFilter(filter));
            this.upsertFilters(peerId, message[1], filters);
            return 'subscribed';
        }
        if (message[0] === 'CLOSE' && typeof message[1] === 'string') {
            this.remove(peerId, message[1]);
            return 'closed';
        }
        return 'ignored';
    }
    upsertFilters(peerId, subscriptionId, filters) {
        return this.upsert(peerId, createPeerSubscription(subscriptionId, filters));
    }
    upsert(peerId, subscription) {
        this.validate(subscription);
        const isNewPeer = !this.peers.has(peerId);
        this.touchPeer(peerId);
        if (isNewPeer) {
            this.evictPeersOverLimit();
        }
        let peer = this.peers.get(peerId);
        if (peer === undefined) {
            peer = new PeerSubscriptionSet();
            this.peers.set(peerId, peer);
        }
        return peer.upsert(subscription, this.limitsValue);
    }
    remove(peerId, subscriptionId) {
        const peer = this.peers.get(peerId);
        const removed = peer?.remove(subscriptionId);
        if (peer?.isEmpty()) {
            this.removePeer(peerId);
        }
        return removed;
    }
    removePeer(peerId) {
        removeFromArray(this.peerOrder, peerId);
        const peer = this.peers.get(peerId);
        this.peers.delete(peerId);
        return peer === undefined
            ? []
            : [...peer.subscriptions.values()].sort(compareSubscriptionsById);
    }
    peerInterest(peerId, event) {
        const peer = this.peers.get(peerId);
        if (peer === undefined)
            return 'unknown';
        for (const subscription of peer.subscriptions.values()) {
            if (subscriptionFiltersMatch(subscription.filters, event)) {
                return 'subscribed';
            }
        }
        return 'unsubscribed';
    }
    matchingSubscriptions(peerId, event) {
        const peer = this.peers.get(peerId);
        if (peer === undefined)
            return [];
        return [...peer.subscriptions.values()]
            .filter((subscription) => subscriptionFiltersMatch(subscription.filters, event))
            .sort(compareSubscriptionsById);
    }
    interestedPeers(event) {
        return [...this.peers.entries()]
            .filter(([, peer]) => {
            for (const subscription of peer.subscriptions.values()) {
                if (subscriptionFiltersMatch(subscription.filters, event))
                    return true;
            }
            return false;
        })
            .map(([peerId]) => peerId)
            .sort();
    }
    validate(subscription) {
        if (this.limitsValue.maxPeers === 0) {
            throw PubsubError.validation('peer subscription store maxPeers must be greater than zero');
        }
        if (this.limitsValue.maxSubscriptionsPerPeer === 0) {
            throw PubsubError.validation('peer subscription store maxSubscriptionsPerPeer must be greater than zero');
        }
        if (subscription.filters.length > this.limitsValue.maxFiltersPerSubscription) {
            throw PubsubError.validation(`subscription ${subscription.subscriptionId} has ${subscription.filters.length} filters, limit is ${this.limitsValue.maxFiltersPerSubscription}`);
        }
    }
    touchPeer(peerId) {
        removeFromArray(this.peerOrder, peerId);
        this.peerOrder.push(peerId);
    }
    evictPeersOverLimit() {
        while (this.peers.size >= this.limitsValue.maxPeers) {
            const peerId = this.peerOrder.shift();
            if (peerId === undefined)
                break;
            if (this.peers.delete(peerId))
                break;
        }
    }
}
function removeFromArray(values, value) {
    let index = values.indexOf(value);
    while (index !== -1) {
        values.splice(index, 1);
        index = values.indexOf(value);
    }
}
function compareSubscriptionsById(left, right) {
    return left.subscriptionId.localeCompare(right.subscriptionId);
}
function isFilterLike(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}
//# sourceMappingURL=subscription.js.map