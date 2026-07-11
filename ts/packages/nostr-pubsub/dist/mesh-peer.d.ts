export interface MeshPeer {
    id: string;
    qualityScore?: number;
}
export declare function meshPeer(id: string, qualityScore?: number): MeshPeer;
export declare function selectMeshPeers(peers: readonly MeshPeer[], excludedPeer: string | undefined, fanout: number, unknownPeerReserve: number): MeshPeer[];
//# sourceMappingURL=mesh-peer.d.ts.map