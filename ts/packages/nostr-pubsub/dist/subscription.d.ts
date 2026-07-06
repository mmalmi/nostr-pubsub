import { type NostrFilter, type NostrVerifiedEvent, type SourceId } from './types.js';
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
export type NostrClientMessage = readonly ['REQ', string, ...NostrFilter[]] | readonly ['CLOSE', string] | readonly unknown[];
export declare function defaultPubsubSubscriptionLimits(): PubsubSubscriptionLimits;
export declare function createPeerSubscription(subscriptionId: string, filters: NostrFilter[]): PubsubPeerSubscription;
export declare class PubsubPeerSubscriptionStore {
    private readonly limitsValue;
    private readonly peers;
    private readonly peerOrder;
    constructor(limits?: PubsubSubscriptionLimits);
    limits(): PubsubSubscriptionLimits;
    peerCount(): number;
    subscriptionCount(): number;
    peerSubscriptionCount(peerId: SourceId): number;
    applyClientMessage(peerId: SourceId, message: NostrClientMessage): PubsubSubscriptionUpdate;
    upsertFilters(peerId: SourceId, subscriptionId: string, filters: NostrFilter[]): PubsubPeerSubscription | undefined;
    upsert(peerId: SourceId, subscription: PubsubPeerSubscription): PubsubPeerSubscription | undefined;
    remove(peerId: SourceId, subscriptionId: string): PubsubPeerSubscription | undefined;
    removePeer(peerId: SourceId): PubsubPeerSubscription[];
    peerInterest(peerId: SourceId, event: NostrVerifiedEvent): PubsubPeerInterest;
    matchingSubscriptions(peerId: SourceId, event: NostrVerifiedEvent): PubsubPeerSubscription[];
    interestedPeers(event: NostrVerifiedEvent): SourceId[];
    private validate;
    private touchPeer;
    private evictPeersOverLimit;
}
//# sourceMappingURL=subscription.d.ts.map