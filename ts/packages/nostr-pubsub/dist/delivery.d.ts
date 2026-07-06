import type { SourceId } from './types.js';
import type { PubsubPeerInterest, PubsubPeerSubscriptionStore } from './subscription.js';
import type { NostrVerifiedEvent } from './types.js';
export type PubsubDeliveryStrategy = 'push-subscribed' | 'inventory-first';
export type PubsubDeliveryAction = 'push-frame' | 'announce-inventory' | 'skip';
export interface PubsubDeliveryPolicy {
    strategy: PubsubDeliveryStrategy;
}
export declare function pushSubscribedDeliveryPolicy(): PubsubDeliveryPolicy;
export declare function inventoryToSubscribersDeliveryPolicy(): PubsubDeliveryPolicy;
export declare function inventoryToPeersDeliveryPolicy(): PubsubDeliveryPolicy;
export declare function deliveryActionForPeer(policy: PubsubDeliveryPolicy, interest: PubsubPeerInterest): PubsubDeliveryAction;
export declare function deliveryActionForEvent(policy: PubsubDeliveryPolicy, subscriptions: PubsubPeerSubscriptionStore, peerId: SourceId, event: NostrVerifiedEvent): PubsubDeliveryAction;
//# sourceMappingURL=delivery.d.ts.map