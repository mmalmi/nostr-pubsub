import { DEFAULT_INV_WANT_HOP_LIMIT } from './invwant.js';
import {
  DEFAULT_INV_WANT_FANOUT,
  DEFAULT_INV_WANT_MAX_EVENT_BYTES,
  type InvWantWireMessage,
} from './mesh-codec.js';
import { DEFAULT_INV_WANT_MAX_CACHE_BYTES } from './mesh-resources.js';
import { boundedPositive, nonNegative, requireKind } from './mesh-state.js';
import type { NostrVerifiedEvent } from './types.js';

export const DEFAULT_ROUTE_TTL_MS = 2 * 60 * 1_000;
export const DEFAULT_EVENT_TTL_MS = 10 * 60 * 1_000;
export const MAX_UPSTREAM_PROVIDERS_PER_EVENT = 3;
export const MAX_TRACKED_PEER_BEHAVIORS = 4_096;
export const MIN_PEER_BEHAVIOR_SAMPLES = 3;
export const VALID_FRAME_REWARD = 20;
export const INVALID_MESSAGE_PENALTY = -40;
export const UNSERVED_INVENTORY_PENALTY = -20;

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

export type InvWantAction =
  | { type: 'send'; peerId: string; message: InvWantWireMessage }
  | { type: 'deliver'; sourcePeer: string; event: NostrVerifiedEvent };

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

export type PeerBehaviorEvidence = keyof Pick<
  PeerBehaviorObservation,
  'validFrames' | 'invalidMessages' | 'unservedInventories'
>;

export function defaultInvWantMeshOptions(): InvWantMeshOptions {
  return {
    fanout: DEFAULT_INV_WANT_FANOUT,
    unknownPeerReserve: 1,
    maxHops: DEFAULT_INV_WANT_HOP_LIMIT,
    maxEventBytes: DEFAULT_INV_WANT_MAX_EVENT_BYTES,
    maxCachedEvents: 1_024,
    maxCachedEventBytes: DEFAULT_INV_WANT_MAX_CACHE_BYTES,
    maxSeenEvents: 4_096,
    maxPendingPeersPerEvent: 64,
    routeTtlMs: DEFAULT_ROUTE_TTL_MS,
    eventTtlMs: DEFAULT_EVENT_TTL_MS,
  };
}

export function normalizeInvWantMeshOptions(
  options: Partial<InvWantMeshOptions>,
): InvWantMeshOptions {
  const merged = { ...defaultInvWantMeshOptions(), ...options };
  return {
    ...merged,
    fanout: boundedPositive(merged.fanout),
    unknownPeerReserve: Math.min(
      nonNegative(merged.unknownPeerReserve),
      boundedPositive(merged.fanout),
    ),
    maxHops: boundedPositive(merged.maxHops, 255),
    maxEventBytes: boundedPositive(merged.maxEventBytes),
    maxCachedEvents: boundedPositive(merged.maxCachedEvents),
    maxCachedEventBytes: Math.max(
      boundedPositive(merged.maxCachedEventBytes),
      boundedPositive(merged.maxEventBytes),
    ),
    maxSeenEvents: boundedPositive(merged.maxSeenEvents),
    maxPendingPeersPerEvent: boundedPositive(merged.maxPendingPeersPerEvent),
    routeTtlMs: boundedPositive(merged.routeTtlMs),
    eventTtlMs: Math.max(
      boundedPositive(merged.eventTtlMs),
      boundedPositive(merged.routeTtlMs),
    ),
    allowedKinds: merged.allowedKinds === undefined
      ? undefined
      : new Set([...merged.allowedKinds].map(requireKind)),
  };
}
