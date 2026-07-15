import type { QueryEvent } from './event-bus.js';
import { type MeshPeer } from './mesh-peer.js';
import type { InvWantMeshRetainedState } from './mesh-resources.js';
import { type InvWantMeshOptions } from './mesh.js';
import type { PubsubPolicy } from './policy.js';
import { type NostrVerifiedEvent } from './types.js';
export declare const FIPS_NOSTR_PUBSUB_INV_WANT_PROTOCOL = "nostr.pubsub";
export declare const FIPS_NOSTR_PUBSUB_INV_WANT_VERSION = 1;
export interface FipsInvWantStreamOptions {
    mesh: Partial<InvWantMeshOptions>;
    protocol: string;
    protocolVersion: number;
    maxRecordBytes: number;
    maxInputPeers: number;
    maxRecordsPerReceive: number;
}
export interface MeshPeerPolicy {
    selectMeshPeer(peerId: string): MeshPeer | undefined;
}
export type FipsInvWantStreamAction = {
    type: 'send';
    peerId: string;
    record: Uint8Array;
} | {
    type: 'deliver';
    event: QueryEvent;
};
export declare function defaultFipsInvWantStreamOptions(): FipsInvWantStreamOptions;
/** Bounded Inv/WANT state above an authenticated reliable byte stream. */
export declare class FipsInvWantStream {
    readonly options: FipsInvWantStreamOptions;
    private readonly mesh;
    private readonly codec;
    private readonly inputs;
    private eventPolicy?;
    private peerPolicy?;
    constructor(options?: Partial<FipsInvWantStreamOptions>);
    withEventPolicy(policy: PubsubPolicy): this;
    withPeerPolicy(policy: MeshPeerPolicy): this;
    seed(event: NostrVerifiedEvent, nowMs: number): void;
    publish(event: NostrVerifiedEvent, connectedPeers: Iterable<string>, nowMs: number): FipsInvWantStreamAction[];
    peerConnected(peerId: string, nowMs: number): FipsInvWantStreamAction[];
    disconnectPeer(peerId: string): void;
    receiveBytes(sourcePeer: string, bytes: Uint8Array, connectedPeers: Iterable<string>, nowMs: number): Promise<FipsInvWantStreamAction[]>;
    retainedState(): InvWantMeshRetainedState;
    bufferedInputBytes(peerId: string): number;
    inputPeerCount(): number;
    remainingInputCapacity(peerId: string): number;
    hasReadyInput(peerId: string): boolean;
    maintain(nowMs: number): void;
    private receiveMessage;
    private selectPeers;
    private selectPeer;
    private ensureEventRecordFits;
    private decodeRecords;
    private encodeActions;
}
//# sourceMappingURL=fips-invwant-stream.d.ts.map