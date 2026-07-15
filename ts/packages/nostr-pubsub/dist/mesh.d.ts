import { type InvWantWireMessage } from './mesh-codec.js';
import { type MeshPeer } from './mesh-peer.js';
import { type InvWantMeshRetainedState } from './mesh-resources.js';
import { defaultInvWantMeshOptions, type InvWantAction, type InvWantMeshOptions, type PeerBehaviorObservation } from './mesh-types.js';
import type { NostrEvent, NostrVerifiedEvent } from './types.js';
export { defaultInvWantMeshOptions };
export type { InvWantAction, InvWantMeshOptions, PeerBehaviorObservation };
/** Transport-neutral bounded inv/want state machine matching Rust's `InvWantMesh`. */
export declare class InvWantMesh {
    readonly options: InvWantMeshOptions;
    private readonly cachedEvents;
    private readonly seenInventories;
    private readonly seenOrder;
    private readonly deliveredEvents;
    private readonly deliveredOrder;
    private readonly upstreamRoutes;
    private readonly pendingDownstream;
    private pendingPeerCount;
    private readonly wantForwarded;
    private readonly peerBehaviors;
    private readonly peerBehaviorOrder;
    constructor(options?: Partial<InvWantMeshOptions>);
    retainedState(): InvWantMeshRetainedState;
    peerBehaviorScore(peerId: string): number | undefined;
    peerBehaviorObservation(peerId: string): PeerBehaviorObservation | undefined;
    recordInvalidMessage(peerId: string): void;
    /** Prune expired state and score requested inventories that were never served. */
    maintain(nowMs: number): void;
    dismissFrame(peerId: string, eventId: string): void;
    /** Mark a locally confirmed request failure; another want clears it. */
    recordTransportDisruption(peerId: string, eventId: string): boolean;
    publish(event: NostrEvent, peers: readonly MeshPeer[], nowMs: number): InvWantAction[];
    /** Publish an event whose signature was already checked at the trust boundary. */
    publishVerified(event: NostrVerifiedEvent, peers: readonly MeshPeer[], nowMs: number): InvWantAction[];
    private publishEvent;
    replayToPeer(event: NostrEvent, peerId: string, nowMs: number): InvWantAction[];
    /** Replay a verified event without repeating signature verification. */
    replayVerifiedToPeer(event: NostrVerifiedEvent, peerId: string, nowMs: number): InvWantAction[];
    private replayEventToPeer;
    receive(sourcePeer: string, message: InvWantWireMessage, peers: readonly MeshPeer[], nowMs: number): InvWantAction[];
    /** Admit a frame already verified by event policy at the trust boundary. */
    receiveVerifiedFrame(sourcePeer: string, eventId: string, event: NostrVerifiedEvent, peers: readonly MeshPeer[], nowMs: number): InvWantAction[];
    private receiveInventory;
    private receiveWant;
    private receiveFrame;
    private validateEvent;
    private acceptEvent;
    private validateKind;
    private validateEventLength;
    private storeEvent;
    private rememberInventory;
    private removePendingEvent;
    private rememberDelivered;
    private sendToSelectedPeers;
    private peersWithBehavior;
    private recordPeerBehavior;
    private prune;
}
//# sourceMappingURL=mesh.d.ts.map