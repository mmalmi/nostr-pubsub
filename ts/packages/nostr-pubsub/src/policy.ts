import type { EventSource } from './source.js';
import type { NostrVerifiedEvent } from './types.js';

export type PolicyDecision =
  | { type: 'allow'; priority: number }
  | { type: 'throttle'; priority: number; reason: string }
  | { type: 'drop'; reason: string };

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

export function allowWithPriority(priority: number): PolicyDecision {
  return { type: 'allow', priority };
}

export function throttle(priority: number, reason: string): PolicyDecision {
  return { type: 'throttle', priority, reason };
}

export function drop(reason: string): PolicyDecision {
  return { type: 'drop', reason };
}

export function decisionPriority(decision: PolicyDecision): number {
  return decision.type === 'drop' ? 0 : decision.priority;
}

export function reportParts(decision: PolicyDecision): {
  accepted: boolean;
  priority: number;
  reason?: string;
} {
  switch (decision.type) {
    case 'allow':
      return { accepted: true, priority: decision.priority };
    case 'throttle':
      return { accepted: true, priority: decision.priority, reason: decision.reason };
    case 'drop':
      return { accepted: false, priority: 0, reason: decision.reason };
  }
}
