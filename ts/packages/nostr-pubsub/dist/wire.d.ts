import type { NostrFilter, NostrVerifiedEvent, SourceId } from './types.js';
import { PubsubPeerSubscriptionStore, type PubsubPeerSubscription, type PubsubSubscriptionUpdate } from './subscription.js';
export declare const DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES: number;
export type FipsPubsubWireMessage = {
    type: 'req';
    subscriptionId: string;
    filters: NostrFilter[];
} | {
    type: 'close';
    subscriptionId: string;
} | {
    type: 'event';
    event: NostrVerifiedEvent;
    subscriptionId?: string;
};
export declare class FipsPubsubWireCodec {
    readonly maxFrameBytes: number;
    constructor(maxFrameBytes?: number);
    encodeFrame(message: FipsPubsubWireMessage): Uint8Array;
    decodeFrame(frame: Uint8Array): FipsPubsubWireMessage;
    private checkFrameSize;
}
export interface FipsPubsubInbound {
    message: FipsPubsubWireMessage;
    subscriptionUpdate: PubsubSubscriptionUpdate;
}
export declare class FipsPubsubWireAdapter {
    readonly codec: FipsPubsubWireCodec;
    readonly subscriptions: PubsubPeerSubscriptionStore;
    constructor(codec?: FipsPubsubWireCodec, subscriptions?: PubsubPeerSubscriptionStore);
    /** Drop subscriptions retained for a transport peer that disconnected. */
    disconnectPeer(peerId: SourceId): PubsubPeerSubscription[];
    decodeInbound(peerId: SourceId, frame: Uint8Array): FipsPubsubInbound;
    applyInbound(peerId: SourceId, message: FipsPubsubWireMessage): FipsPubsubInbound;
    encodeOutbound(message: FipsPubsubWireMessage): Uint8Array;
}
//# sourceMappingURL=wire.d.ts.map