import { type InvWantWireMessage } from './mesh-codec.js';
import { type MeshPeer } from './mesh-peer.js';
import type { NostrEvent, NostrVerifiedEvent } from './types.js';
export interface InvWantMeshOptions {
    fanout: number;
    unknownPeerReserve: number;
    maxHops: number;
    maxEventBytes: number;
    maxCachedEvents: number;
    maxSeenEvents: number;
    maxPendingPeersPerEvent: number;
    routeTtlMs: number;
    eventTtlMs: number;
    allowedKinds?: ReadonlySet<number>;
}
export type InvWantAction = {
    type: 'send';
    peerId: string;
    message: InvWantWireMessage;
} | {
    type: 'deliver';
    sourcePeer: string;
    event: NostrVerifiedEvent;
};
export declare function defaultInvWantMeshOptions(): InvWantMeshOptions;
/** Transport-neutral bounded inv/want state machine matching Rust's `InvWantMesh`. */
export declare class InvWantMesh {
    readonly options: InvWantMeshOptions;
    private readonly cachedEvents;
    private readonly cacheOrder;
    private readonly seenInventories;
    private readonly seenOrder;
    private readonly deliveredEvents;
    private readonly deliveredOrder;
    private readonly upstreamRoutes;
    private readonly pendingDownstream;
    private readonly wantForwarded;
    private readonly peerBehaviors;
    private readonly peerBehaviorOrder;
    constructor(options?: Partial<InvWantMeshOptions>);
    peerBehaviorScore(peerId: string): number | undefined;
    recordInvalidMessage(peerId: string): void;
    dismissFrame(peerId: string, eventId: string): void;
    publish(event: NostrEvent, peers: readonly MeshPeer[], nowMs: number): InvWantAction[];
    replayToPeer(event: NostrEvent, peerId: string, nowMs: number): InvWantAction[];
    receive(sourcePeer: string, message: InvWantWireMessage, peers: readonly MeshPeer[], nowMs: number): InvWantAction[];
    private receiveInventory;
    private receiveWant;
    private receiveFrame;
    private validateEvent;
    private validateKind;
    private validateEventLength;
    private storeEvent;
    private rememberInventory;
    private rememberDelivered;
    private sendToSelectedPeers;
    private peersWithBehavior;
    private recordPeerBehavior;
    private prune;
}
//# sourceMappingURL=mesh.d.ts.map