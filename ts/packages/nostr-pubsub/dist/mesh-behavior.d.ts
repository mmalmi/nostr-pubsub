import { type PeerBehaviorEvidence, type PeerBehaviorObservation } from './mesh-types.js';
/** Bounded local evidence about authenticated mesh peers. */
export declare class PeerBehaviorTracker {
    private readonly behaviors;
    private readonly order;
    get size(): number;
    score(peerId: string): number | undefined;
    observation(peerId: string): PeerBehaviorObservation | undefined;
    record(peerId: string, delta: number, evidence: PeerBehaviorEvidence): void;
}
//# sourceMappingURL=mesh-behavior.d.ts.map