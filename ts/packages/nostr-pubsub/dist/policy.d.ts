import type { EventSource } from './source.js';
import type { NostrVerifiedEvent } from './types.js';
export type PolicyDecision = {
    type: 'allow';
    priority: number;
} | {
    type: 'throttle';
    priority: number;
    reason: string;
} | {
    type: 'drop';
    reason: string;
};
export interface SourceHealth {
    readonly [key: string]: never;
}
export interface SourceCandidate {
    source: EventSource;
    priority: number;
    reason?: string;
    freshnessHint?: number;
    health: SourceHealth;
}
export interface EventPolicyContext {
    event: NostrVerifiedEvent;
    source: EventSource;
}
export interface SourcePolicyContext {
    candidate: SourceCandidate;
    authorPubkey?: string;
    capabilities: string[];
}
export interface PubsubPolicy {
    checkEvent(context: EventPolicyContext): Promise<PolicyDecision> | PolicyDecision;
    checkSource(context: SourcePolicyContext): Promise<PolicyDecision> | PolicyDecision;
}
export declare function allowWithPriority(priority: number): PolicyDecision;
export declare function throttle(priority: number, reason: string): PolicyDecision;
export declare function drop(reason: string): PolicyDecision;
export declare function decisionPriority(decision: PolicyDecision): number;
export declare function reportParts(decision: PolicyDecision): {
    accepted: boolean;
    priority: number;
    reason?: string;
};
//# sourceMappingURL=policy.d.ts.map