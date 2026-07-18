import { FipsPubsubInvWantState } from './fips-pubsub-invwant.js';
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
export declare class FipsPubsubEventCache {
    private readonly maximum;
    private readonly events;
    private readonly order;
    constructor(maximum: number);
    has(eventId: string): boolean;
    get(eventId: string): CachedFipsPubsubEvent | undefined;
    remember(event: NostrVerifiedEvent, sourcePeer: string | undefined, hopLimit: number): boolean;
    replay(filters: NostrFilter[], limit: number): CachedFipsPubsubEvent[];
    clear(): void;
}
export interface FipsPubsubProtocolContext {
    maxActiveSubscriptions: number;
    maxHops: number;
    invWant: FipsPubsubInvWantState;
    events: FipsPubsubEventCache;
    validSubscriptionIds(peerId: string, subscriptionIds: string[], eventId: string): string[];
    eventForWant(peerId: string, eventId: string): {
        subscriptionId: string;
        event: NostrVerifiedEvent;
    } | undefined;
    deliverEvent(peerId: string, event: NostrVerifiedEvent): boolean;
    forwardEvent(peerId: string, event: NostrVerifiedEvent, hopLimit: number): void;
    send(peerId: string, message: FipsPubsubWireMessage): void;
}
export declare function inventoryMessage(event: NostrVerifiedEvent, subscriptionIds: string[], hopLimit: number): Extract<FipsPubsubWireMessage, {
    type: 'inv';
}>;
export declare function acceptInventory(context: FipsPubsubProtocolContext, peerId: string, message: Extract<FipsPubsubWireMessage, {
    type: 'inv';
}>): void;
export declare function answerWant(context: FipsPubsubProtocolContext, peerId: string, eventId: string): void;
export declare function acceptEvent(context: FipsPubsubProtocolContext, peerId: string, message: Extract<FipsPubsubWireMessage, {
    type: 'event';
}>): void;
export declare function retryPendingWants(context: FipsPubsubProtocolContext, nowMs: number): void;
export declare function deliverSubscription(subscription: FipsPubsubLocalSubscription, event: NostrVerifiedEvent, sourcePeer: string, recentLimit: number, reportError: (error: unknown) => void): boolean;
export declare function replayLocalSubscription(events: FipsPubsubEventCache, subscription: FipsPubsubLocalSubscription, replayLimit: number, recentLimit: number, reportError: (error: unknown) => void): void;
//# sourceMappingURL=fips-pubsub-client-protocol.d.ts.map