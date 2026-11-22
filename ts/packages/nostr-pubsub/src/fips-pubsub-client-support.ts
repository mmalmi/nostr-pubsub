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

/** Older FIPS runtimes only supply the compressed route key. */
export function authenticatedPeerId(route: unknown, peerId?: unknown): string | undefined {
  if (typeof route !== 'string') return undefined;
  const key = normalizePeerId(route)?.slice(2);
  if (peerId === undefined) return key;
  if (typeof peerId !== 'string' || !/^[0-9a-f]{64}$/.test(peerId)) return undefined;
  return (key ?? route.toLowerCase()) === peerId ? peerId : undefined;
}

/** Key the admitted roster by the complete BIP-340 identity, keeping its caller-facing spelling. */
export function admittedFipsPeers(values: readonly string[], maximum: number): Map<string, string> {
  const peers = new Map<string, string>();
  for (const value of values) {
    const id = normalizePeerId(value);
    if (id === undefined) continue;
    const xOnly = id.slice(2);
    if (!peers.has(xOnly)) peers.set(xOnly, id);
    if (peers.size >= maximum) break;
  }
  return peers;
}

export function parseConnectionEvent(
  event: unknown,
): { peerId: string; connected: boolean } | undefined {
  if (event === null || typeof event !== 'object') return undefined;
  const candidate = event as { remotePubkey?: unknown; peerId?: unknown; state?: unknown };
  const xOnly = authenticatedPeerId(candidate.remotePubkey, candidate.peerId);
  if (xOnly === undefined) return undefined;
  const peerId = normalizePeerId(candidate.remotePubkey) ?? '02' + xOnly;
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
