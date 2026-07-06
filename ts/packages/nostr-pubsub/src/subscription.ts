import { cloneFilter, subscriptionFiltersMatch } from './filter.js';
import { PubsubError, type NostrFilter, type NostrVerifiedEvent, type SourceId } from './types.js';

export type PubsubPeerInterest = 'subscribed' | 'unsubscribed' | 'unknown';
export type PubsubSubscriptionUpdate = 'subscribed' | 'closed' | 'ignored';

export interface PubsubSubscriptionLimits {
  maxPeers: number;
  maxSubscriptionsPerPeer: number;
  maxFiltersPerSubscription: number;
}

export interface PubsubPeerSubscription {
  subscriptionId: string;
  filters: NostrFilter[];
}

export type NostrClientMessage =
  | readonly ['REQ', string, ...NostrFilter[]]
  | readonly ['CLOSE', string]
  | readonly unknown[];

export function defaultPubsubSubscriptionLimits(): PubsubSubscriptionLimits {
  return {
    maxPeers: 1024,
    maxSubscriptionsPerPeer: 64,
    maxFiltersPerSubscription: 16,
  };
}

export function createPeerSubscription(
  subscriptionId: string,
  filters: NostrFilter[],
): PubsubPeerSubscription {
  return { subscriptionId, filters: filters.map(cloneFilter) };
}

class PeerSubscriptionSet {
  readonly subscriptions = new Map<string, PubsubPeerSubscription>();
  readonly order: string[] = [];

  upsert(
    subscription: PubsubPeerSubscription,
    limits: PubsubSubscriptionLimits,
  ): PubsubPeerSubscription | undefined {
    const subscriptionId = subscription.subscriptionId;
    removeFromArray(this.order, subscriptionId);
    this.order.push(subscriptionId);
    const replaced = this.subscriptions.has(subscriptionId);
    this.subscriptions.set(subscriptionId, subscription);
    return replaced ? undefined : this.evictOldestOverLimit(limits.maxSubscriptionsPerPeer);
  }

  remove(subscriptionId: string): PubsubPeerSubscription | undefined {
    removeFromArray(this.order, subscriptionId);
    const removed = this.subscriptions.get(subscriptionId);
    this.subscriptions.delete(subscriptionId);
    return removed;
  }

  isEmpty(): boolean {
    return this.subscriptions.size === 0;
  }

  private evictOldestOverLimit(limit: number): PubsubPeerSubscription | undefined {
    while (this.subscriptions.size > limit) {
      const subscriptionId = this.order.shift();
      if (subscriptionId === undefined) break;
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
  private readonly limitsValue: PubsubSubscriptionLimits;
  private readonly peers = new Map<SourceId, PeerSubscriptionSet>();
  private readonly peerOrder: SourceId[] = [];

  constructor(limits: PubsubSubscriptionLimits = defaultPubsubSubscriptionLimits()) {
    this.limitsValue = { ...limits };
  }

  limits(): PubsubSubscriptionLimits {
    return { ...this.limitsValue };
  }

  peerCount(): number {
    return this.peers.size;
  }

  subscriptionCount(): number {
    let count = 0;
    for (const peer of this.peers.values()) {
      count += peer.subscriptions.size;
    }
    return count;
  }

  peerSubscriptionCount(peerId: SourceId): number {
    return this.peers.get(peerId)?.subscriptions.size ?? 0;
  }

  applyClientMessage(peerId: SourceId, message: NostrClientMessage): PubsubSubscriptionUpdate {
    if (!Array.isArray(message)) return 'ignored';
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

  upsertFilters(
    peerId: SourceId,
    subscriptionId: string,
    filters: NostrFilter[],
  ): PubsubPeerSubscription | undefined {
    return this.upsert(peerId, createPeerSubscription(subscriptionId, filters));
  }

  upsert(
    peerId: SourceId,
    subscription: PubsubPeerSubscription,
  ): PubsubPeerSubscription | undefined {
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

  remove(peerId: SourceId, subscriptionId: string): PubsubPeerSubscription | undefined {
    const peer = this.peers.get(peerId);
    const removed = peer?.remove(subscriptionId);
    if (peer?.isEmpty()) {
      this.removePeer(peerId);
    }
    return removed;
  }

  removePeer(peerId: SourceId): PubsubPeerSubscription[] {
    removeFromArray(this.peerOrder, peerId);
    const peer = this.peers.get(peerId);
    this.peers.delete(peerId);
    return peer === undefined
      ? []
      : [...peer.subscriptions.values()].sort(compareSubscriptionsById);
  }

  peerInterest(peerId: SourceId, event: NostrVerifiedEvent): PubsubPeerInterest {
    const peer = this.peers.get(peerId);
    if (peer === undefined) return 'unknown';
    for (const subscription of peer.subscriptions.values()) {
      if (subscriptionFiltersMatch(subscription.filters, event)) {
        return 'subscribed';
      }
    }
    return 'unsubscribed';
  }

  matchingSubscriptions(peerId: SourceId, event: NostrVerifiedEvent): PubsubPeerSubscription[] {
    const peer = this.peers.get(peerId);
    if (peer === undefined) return [];
    return [...peer.subscriptions.values()]
      .filter((subscription) => subscriptionFiltersMatch(subscription.filters, event))
      .sort(compareSubscriptionsById);
  }

  interestedPeers(event: NostrVerifiedEvent): SourceId[] {
    return [...this.peers.entries()]
      .filter(([, peer]) => {
        for (const subscription of peer.subscriptions.values()) {
          if (subscriptionFiltersMatch(subscription.filters, event)) return true;
        }
        return false;
      })
      .map(([peerId]) => peerId)
      .sort();
  }

  private validate(subscription: PubsubPeerSubscription): void {
    if (this.limitsValue.maxPeers === 0) {
      throw PubsubError.validation('peer subscription store maxPeers must be greater than zero');
    }
    if (this.limitsValue.maxSubscriptionsPerPeer === 0) {
      throw PubsubError.validation(
        'peer subscription store maxSubscriptionsPerPeer must be greater than zero',
      );
    }
    if (subscription.filters.length > this.limitsValue.maxFiltersPerSubscription) {
      throw PubsubError.validation(
        `subscription ${subscription.subscriptionId} has ${subscription.filters.length} filters, limit is ${this.limitsValue.maxFiltersPerSubscription}`,
      );
    }
  }

  private touchPeer(peerId: SourceId): void {
    removeFromArray(this.peerOrder, peerId);
    this.peerOrder.push(peerId);
  }

  private evictPeersOverLimit(): void {
    while (this.peers.size >= this.limitsValue.maxPeers) {
      const peerId = this.peerOrder.shift();
      if (peerId === undefined) break;
      if (this.peers.delete(peerId)) break;
    }
  }
}

function removeFromArray(values: string[], value: string): void {
  let index = values.indexOf(value);
  while (index !== -1) {
    values.splice(index, 1);
    index = values.indexOf(value);
  }
}

function compareSubscriptionsById(
  left: PubsubPeerSubscription,
  right: PubsubPeerSubscription,
): number {
  return left.subscriptionId.localeCompare(right.subscriptionId);
}

function isFilterLike(value: unknown): value is NostrFilter {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
