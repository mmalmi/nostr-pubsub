import { PubsubPeerSubscriptionStore } from './subscription.js';
import { PubsubError } from './types.js';
import { FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES } from './wire.js';
import {
  defaultFipsNostrPubsubClientLimits,
  type FipsNostrPubsubClientLimits,
} from './fips-pubsub-client-types.js';

export function validateClientLimits(
  overrides: Partial<FipsNostrPubsubClientLimits> | undefined,
): FipsNostrPubsubClientLimits {
  const limits = { ...defaultFipsNostrPubsubClientLimits(), ...overrides };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw clientError(`${name} must be a positive safe integer`);
    }
  }
  if (limits.maxFrameBytes > FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES) {
    throw clientError(`maxFrameBytes cannot exceed ${FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES}`);
  }
  if (limits.maxReplayEvents > limits.maxCachedEvents) {
    throw clientError('maxReplayEvents cannot exceed maxCachedEvents');
  }
  return limits;
}

export function createClientPeerSubscriptionStore(
  limits: FipsNostrPubsubClientLimits,
): PubsubPeerSubscriptionStore {
  return new PubsubPeerSubscriptionStore({
    maxPeers: limits.maxPeers,
    maxSubscriptionsPerPeer: limits.maxSubscriptionsPerPeer,
    maxFiltersPerSubscription: limits.maxFiltersPerSubscription,
  });
}

export function normalizeAllowedKinds(kinds: readonly number[] | undefined): Set<number> | undefined {
  if (kinds === undefined) return undefined;
  if (kinds.some((kind) => !Number.isSafeInteger(kind) || kind < 0 || kind > 65_535)) {
    throw clientError('allowedKinds must contain valid Nostr kind integers');
  }
  return new Set(kinds);
}

export function normalizePeerId(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const normalized = value.toLowerCase();
  return /^(02|03)[0-9a-f]{64}$/.test(normalized) ? normalized : undefined;
}

export function parseConnectionEvent(
  event: unknown,
): { peerId: string; connected: boolean } | undefined {
  if (event === null || typeof event !== 'object') return undefined;
  const candidate = event as { remotePubkey?: unknown; state?: unknown };
  const peerId = normalizePeerId(candidate.remotePubkey);
  if (peerId === undefined || typeof candidate.state !== 'string') return undefined;
  if (candidate.state === 'connected' || candidate.state === 'established') {
    return { peerId, connected: true };
  }
  if (candidate.state === 'disconnected' || candidate.state === 'closed') {
    return { peerId, connected: false };
  }
  return undefined;
}

export function rememberId(ids: Set<string>, order: string[], id: string, maximum: number): void {
  ids.add(id);
  order.push(id);
  while (order.length > maximum) {
    const removed = order.shift();
    if (removed !== undefined) ids.delete(removed);
  }
}

export function clientError(message: string): PubsubError {
  return PubsubError.validation(`FIPS Nostr pubsub client: ${message}`);
}
