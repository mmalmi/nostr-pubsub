import type { SourceId } from './types.js';
import type { PubsubPeerInterest, PubsubPeerSubscriptionStore } from './subscription.js';
import type { NostrVerifiedEvent } from './types.js';

export type PubsubDeliveryStrategy = 'push-subscribed' | 'inventory-first';
export type PubsubDeliveryAction = 'push-frame' | 'announce-inventory' | 'skip';

export interface PubsubDeliveryPolicy {
  strategy: PubsubDeliveryStrategy;
}

export function pushSubscribedDeliveryPolicy(): PubsubDeliveryPolicy {
  return { strategy: 'push-subscribed' };
}

export function inventoryToSubscribersDeliveryPolicy(): PubsubDeliveryPolicy {
  return { strategy: 'inventory-first' };
}

export function inventoryToPeersDeliveryPolicy(): PubsubDeliveryPolicy {
  return { strategy: 'inventory-first' };
}

export function deliveryActionForPeer(
  policy: PubsubDeliveryPolicy,
  interest: PubsubPeerInterest,
): PubsubDeliveryAction {
  if (policy.strategy === 'push-subscribed' && interest === 'subscribed') {
    return 'push-frame';
  }
  if (policy.strategy === 'inventory-first' && interest === 'subscribed') {
    return 'announce-inventory';
  }
  return 'skip';
}

export function deliveryActionForEvent(
  policy: PubsubDeliveryPolicy,
  subscriptions: PubsubPeerSubscriptionStore,
  peerId: SourceId,
  event: NostrVerifiedEvent,
): PubsubDeliveryAction {
  return deliveryActionForPeer(policy, subscriptions.peerInterest(peerId, event));
}
