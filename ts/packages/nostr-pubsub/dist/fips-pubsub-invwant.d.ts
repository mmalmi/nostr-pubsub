import type { FipsPubsubWireMessage } from './wire.js';
export interface WantRequest {
    peerId: string;
    eventId: string;
}
/** Bounded global WANT selection shared by live and historical inventories. */
export declare class FipsPubsubInvWantState {
    private readonly maxEvents;
    private readonly maxAlternatives;
    private readonly pending;
    private readonly order;
    constructor(maxEvents: number, maxAlternatives: number);
    accept(peerId: string, message: Extract<FipsPubsubWireMessage, {
        type: 'inv';
    }>, validSubscriptionIds: string[], nowMs: number): WantRequest | undefined;
    complete(peerId: string, subscriptionId: string, eventId: string, eventKind: number, payloadBytes: number): number | undefined;
    retryDue(nowMs: number, retryAfterMs: number): WantRequest[];
    removeSubscription(subscriptionId: string): void;
    dropPeer(peerId: string): void;
    clear(): void;
    private trim;
    private delete;
}
//# sourceMappingURL=fips-pubsub-invwant.d.ts.map