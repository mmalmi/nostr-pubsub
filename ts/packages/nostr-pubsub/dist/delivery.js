export function pushSubscribedDeliveryPolicy() {
    return { strategy: 'push-subscribed' };
}
export function inventoryToSubscribersDeliveryPolicy() {
    return { strategy: 'inventory-first' };
}
export function inventoryToPeersDeliveryPolicy() {
    return { strategy: 'inventory-first' };
}
export function deliveryActionForPeer(policy, interest) {
    if (policy.strategy === 'push-subscribed' && interest === 'subscribed') {
        return 'push-frame';
    }
    if (policy.strategy === 'inventory-first' && interest === 'subscribed') {
        return 'announce-inventory';
    }
    return 'skip';
}
export function deliveryActionForEvent(policy, subscriptions, peerId, event) {
    return deliveryActionForPeer(policy, subscriptions.peerInterest(peerId, event));
}
//# sourceMappingURL=delivery.js.map