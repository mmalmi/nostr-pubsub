import { type InvWantWireMessage } from './mesh-codec.js';
import type { NostrVerifiedEvent } from './types.js';
export declare const DEFAULT_ROUTE_TTL_MS: number;
export declare const DEFAULT_EVENT_TTL_MS: number;
export declare const MAX_UPSTREAM_PROVIDERS_PER_EVENT = 3;
export declare const MAX_TRACKED_PEER_BEHAVIORS = 4096;
export declare const MIN_PEER_BEHAVIOR_SAMPLES = 3;
export declare const VALID_FRAME_REWARD = 20;
export declare const INVALID_MESSAGE_PENALTY = -40;
export declare const UNSERVED_INVENTORY_PENALTY = -20;
export interface InvWantMeshOptions {
    fanout: number;
    unknownPeerReserve: number;
    maxHops: number;
    maxEventBytes: number;
    maxCachedEvents: number;
    maxCachedEventBytes: number;
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
export interface UpstreamRoute {
    peerId: string;
    alternatePeerIds: Set<string>;
    eventKind: number;
    payloadBytes: number;
    hopLimit: number;
    expiresAtMs: number;
    fulfilled: boolean;
}
export interface PendingPeers {
    peers: Set<string>;
    expiresAtMs: number;
}
export interface PeerBehaviorObservation {
    score: number;
    samples: number;
    validFrames: number;
    invalidMessages: number;
    unservedInventories: number;
}
export type PeerBehaviorEvidence = keyof Pick<PeerBehaviorObservation, 'validFrames' | 'invalidMessages' | 'unservedInventories'>;
export declare function defaultInvWantMeshOptions(): InvWantMeshOptions;
export declare function normalizeInvWantMeshOptions(options: Partial<InvWantMeshOptions>): InvWantMeshOptions;
//# sourceMappingURL=mesh-types.d.ts.map