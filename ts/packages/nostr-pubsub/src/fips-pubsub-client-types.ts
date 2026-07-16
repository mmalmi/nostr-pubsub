import type { FipsPubsubServiceHandler } from './fips-relay-service.js';
import type { NostrVerifiedEvent } from './types.js';
import { FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES } from './wire.js';

export interface FipsPubsubClientNode {
  registerService(port: number, handler: FipsPubsubServiceHandler): () => void;
  sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void>;
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
}

export interface FipsNostrPubsubClientErrorContext {
  operation: 'receive' | 'send' | 'event-handler';
  peerId?: string;
  subscriptionId?: string;
}

export interface FipsNostrPubsubClientOptions {
  node: FipsPubsubClientNode;
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
    maxFrameBytes: FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES,
  };
}
