import { clamp } from './mesh-state.js';
import { MAX_TRACKED_PEER_BEHAVIORS, MIN_PEER_BEHAVIOR_SAMPLES, } from './mesh-types.js';
/** Bounded local evidence about authenticated mesh peers. */
export class PeerBehaviorTracker {
    behaviors = new Map();
    order = [];
    get size() {
        return this.behaviors.size;
    }
    score(peerId) {
        return this.observation(peerId)?.score;
    }
    observation(peerId) {
        const behavior = this.behaviors.get(peerId);
        return behavior !== undefined && behavior.samples >= MIN_PEER_BEHAVIOR_SAMPLES
            ? { ...behavior }
            : undefined;
    }
    record(peerId, delta, evidence) {
        if (!this.behaviors.has(peerId)) {
            while (this.behaviors.size >= MAX_TRACKED_PEER_BEHAVIORS) {
                const oldest = this.order.shift();
                if (oldest === undefined)
                    break;
                this.behaviors.delete(oldest);
            }
            this.order.push(peerId);
        }
        const behavior = this.behaviors.get(peerId) ?? {
            score: 0,
            samples: 0,
            validFrames: 0,
            invalidMessages: 0,
            unservedInventories: 0,
        };
        behavior.samples = Math.min(0xffff_ffff, behavior.samples + 1);
        behavior.score = clamp(behavior.score + delta, -100, 100);
        behavior[evidence] = Math.min(0xffff_ffff, behavior[evidence] + 1);
        this.behaviors.set(peerId, behavior);
    }
}
//# sourceMappingURL=mesh-behavior.js.map