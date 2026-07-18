import type { FipsDatagramEndpoint } from '@fips/tcp';
import type { NostrVerifiedEvent } from './types.js';
import { FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES } from './wire.js';

export interface FipsPubsubClientNode extends FipsDatagramEndpoint {
  on?(event: 'peer' | 'session', listener: (event: unknown) => void): () => void;
}

export interface FipsNostrPubsubClientLimits {
  maxPeers: number;
  maxActiveSubscriptions: number;
  maxSubscriptionsPerPeer: number;
  maxFiltersPerSubscription: number;
  maxReplayEvents: number;
  maxCachedEvents: number;
  maxFrameBytes: number;
  maxHops: number;
  receiveBatchSize: number;
}

export interface FipsNostrPubsubClientErrorContext {
  operation: 'receive' | 'send' | 'event-handler';
  peerId?: string;
  subscriptionId?: string;
}

export interface FipsNostrPubsubClientOptions {
  node: FipsPubsubClientNode;
  /** Local compressed FIPS public key used for deterministic TCP stream ownership. */
  localPeerId: string;
  /** Explicit application-admitted FIPS identities. Connected peers are not inferred. */
  peers: () => readonly string[];
  allowedKinds?: readonly number[];
  limits?: Partial<FipsNostrPubsubClientLimits>;
  onError?: (error: Error, context: FipsNostrPubsubClientErrorContext) => void;
}

export interface FipsNostrPubsubSubscription {
  readonly id: string;
  close(): void;
}

export type FipsNostrPubsubEventHandler = (
  event: NostrVerifiedEvent,
  sourcePeer: string,
) => void;

export function defaultFipsNostrPubsubClientLimits(): FipsNostrPubsubClientLimits {
  return {
    maxPeers: 64,
    maxActiveSubscriptions: 64,
    maxSubscriptionsPerPeer: 64,
    maxFiltersPerSubscription: 4,
    maxReplayEvents: 8,
    maxCachedEvents: 256,
    maxFrameBytes: FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES,
    maxHops: 4,
    receiveBatchSize: 64,
  };
}
