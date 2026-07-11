import { cloneFilter, subscriptionFiltersMatch } from './filter.js';
import { PubsubPeerSubscriptionStore } from './subscription.js';
import {
  PubsubError,
  type NostrEvent,
  type NostrFilter,
  type NostrVerifiedEvent,
  verifyNostrEvent,
} from './types.js';
import {
  DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES,
  FipsPubsubWireAdapter,
  FipsPubsubWireCodec,
  type FipsPubsubWireMessage,
} from './wire.js';

export const FIPS_NOSTR_PUBSUB_SERVICE_PORT = 7368;
export const FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS = 8;
const MAX_PENDING_REPLIES_PER_SUBSCRIPTION = 8;
const MAX_RECENT_EVENT_IDS_PER_SUBSCRIPTION = 64;

export interface FipsPubsubServiceContext {
  src: string;
  srcPort: number;
  dstPort: number;
  payload: Uint8Array;
  reply: (data: Uint8Array, replyDstPort?: number) => Promise<void>;
}

export type FipsPubsubServiceHandler = (
  context: FipsPubsubServiceContext,
) => Promise<void> | void;

export interface FipsPubsubServiceNode {
  registerService(port: number, handler: FipsPubsubServiceHandler): () => void;
  on?(event: 'session', listener: (event: unknown) => void): () => void;
}

export interface NostrRelaySubscription {
  close(reason?: string): void;
}

export interface NostrRelayTransportHandlers {
  onEvent(event: NostrEvent): void;
  onEose?(): void;
  onClose?(reasons?: readonly string[]): void;
}

export interface NostrRelayTransport {
  subscribe(
    filters: NostrFilter[],
    handlers: NostrRelayTransportHandlers,
  ): NostrRelaySubscription;
  publish(event: NostrVerifiedEvent): Promise<void> | void;
}

export interface FipsNostrRelayServiceLimits {
  maxPeers: number;
  maxSubscriptionsPerPeer: number;
  maxFiltersPerSubscription: number;
  maxReplayEventsPerFilter: number;
  subscriptionTtlMs: number;
  maxFrameBytes: number;
}

export interface FipsNostrRelayServiceErrorContext {
  operation: 'relay-event' | 'relay-eose' | 'subscription-close';
  peerId: string;
  subscriptionId: string;
}

export interface FipsNostrRelayServiceOptions {
  node: FipsPubsubServiceNode;
  relay: NostrRelayTransport;
  limits?: Partial<FipsNostrRelayServiceLimits>;
  onError?: (error: Error, context: FipsNostrRelayServiceErrorContext) => void;
}

interface ActiveSubscription {
  readonly token: object;
  readonly filters: NostrFilter[];
  readonly reply: FipsPubsubServiceContext['reply'];
  readonly recentEventIds: Set<string>;
  readonly recentEventIdQueue: string[];
  pendingReplyCount: number;
  replyChain: Promise<void>;
  backpressureReported: boolean;
  eoseSent: boolean;
  relaySubscription?: NostrRelaySubscription;
  timer?: ReturnType<typeof setTimeout>;
}

export function defaultFipsNostrRelayServiceLimits(): FipsNostrRelayServiceLimits {
  return {
    maxPeers: 64,
    maxSubscriptionsPerPeer: 8,
    maxFiltersPerSubscription: 4,
    maxReplayEventsPerFilter: FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS,
    subscriptionTtlMs: 5 * 60 * 1000,
    maxFrameBytes: DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES,
  };
}

export class FipsNostrRelayService {
  readonly adapter: FipsPubsubWireAdapter;
  readonly limits: FipsNostrRelayServiceLimits;

  private readonly node: FipsPubsubServiceNode;
  private readonly relay: NostrRelayTransport;
  private readonly onError: NonNullable<FipsNostrRelayServiceOptions['onError']>;
  private readonly peers = new Map<string, Map<string, ActiveSubscription>>();
  private readonly pendingReplies = new Set<Promise<void>>();
  private unregisterService?: () => void;
  private removeSessionListener?: () => void;

  constructor(options: FipsNostrRelayServiceOptions) {
    this.node = options.node;
    this.relay = options.relay;
    this.onError = options.onError ?? (() => {});
    this.limits = validatedLimits(options.limits);
    this.adapter = new FipsPubsubWireAdapter(
      new FipsPubsubWireCodec(this.limits.maxFrameBytes),
      new PubsubPeerSubscriptionStore({
        maxPeers: this.limits.maxPeers,
        maxSubscriptionsPerPeer: this.limits.maxSubscriptionsPerPeer,
        maxFiltersPerSubscription: this.limits.maxFiltersPerSubscription,
      }),
    );
  }

  start(): this {
    if (this.unregisterService !== undefined) return this;
    this.unregisterService = this.node.registerService(
      FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      (context) => this.handle(context),
    );
    this.removeSessionListener = this.node.on?.('session', (event) => {
      const session = parseClosedSession(event);
      if (session !== undefined) this.closePeer(session);
    });
    return this;
  }

  async stop(): Promise<void> {
    this.unregisterService?.();
    this.unregisterService = undefined;
    this.removeSessionListener?.();
    this.removeSessionListener = undefined;
    for (const peerId of [...this.peers.keys()]) this.closePeer(peerId);
    await Promise.allSettled([...this.pendingReplies]);
  }

  activePeerCount(): number {
    return this.peers.size;
  }

  activeSubscriptionCount(): number {
    let count = 0;
    for (const subscriptions of this.peers.values()) count += subscriptions.size;
    return count;
  }

  peerSubscriptionCount(peerId: string): number {
    return this.peers.get(peerId.toLowerCase())?.size ?? 0;
  }

  closePeer(peerId: string): void {
    const normalizedPeerId = peerId.toLowerCase();
    const subscriptions = this.peers.get(normalizedPeerId);
    if (subscriptions === undefined) return;
    for (const subscriptionId of [...subscriptions.keys()]) {
      this.closeSubscription(normalizedPeerId, subscriptionId);
    }
  }

  private async handle(context: FipsPubsubServiceContext): Promise<void> {
    const peerId = authenticatedPeerId(context.src);
    if (context.srcPort !== FIPS_NOSTR_PUBSUB_SERVICE_PORT) {
      throw serviceError(`expected source port ${FIPS_NOSTR_PUBSUB_SERVICE_PORT}`);
    }
    if (context.dstPort !== FIPS_NOSTR_PUBSUB_SERVICE_PORT) {
      throw serviceError(`expected destination port ${FIPS_NOSTR_PUBSUB_SERVICE_PORT}`);
    }
    if (!(context.payload instanceof Uint8Array)) {
      throw serviceError('payload must be a Uint8Array');
    }

    const message = this.adapter.codec.decodeFrame(context.payload);
    switch (message.type) {
      case 'req':
        this.openSubscription(peerId, message, context.reply);
        return;
      case 'close':
        this.adapter.applyInbound(peerId, message);
        this.closeSubscription(peerId, message.subscriptionId);
        return;
      case 'eose':
        throw serviceError('client cannot send EOSE');
      case 'event':
        if (message.subscriptionId !== undefined) {
          throw serviceError('client cannot publish a subscription-addressed EVENT');
        }
        await this.relay.publish(message.event);
    }
  }

  private openSubscription(
    peerId: string,
    message: Extract<FipsPubsubWireMessage, { type: 'req' }>,
    reply: FipsPubsubServiceContext['reply'],
  ): void {
    let subscriptions = this.peers.get(peerId);
    const existing = subscriptions?.get(message.subscriptionId);
    if (subscriptions === undefined && this.peers.size >= this.limits.maxPeers) {
      throw serviceError(`peer limit is ${this.limits.maxPeers}`);
    }
    if (message.filters.length > this.limits.maxFiltersPerSubscription) {
      throw serviceError(
        `subscription filter limit is ${this.limits.maxFiltersPerSubscription}`,
      );
    }
    if (
      existing === undefined &&
      subscriptions !== undefined &&
      subscriptions.size >= this.limits.maxSubscriptionsPerPeer
    ) {
      throw serviceError(
        `peer subscription limit is ${this.limits.maxSubscriptionsPerPeer}`,
      );
    }

    const filters = boundedReplayFilters(
      message.filters,
      this.limits.maxReplayEventsPerFilter,
    );
    if (existing !== undefined) this.closeSubscription(peerId, message.subscriptionId);
    subscriptions = this.peers.get(peerId);
    if (subscriptions === undefined) {
      subscriptions = new Map();
      this.peers.set(peerId, subscriptions);
    }

    this.adapter.applyInbound(peerId, {
      type: 'req',
      subscriptionId: message.subscriptionId,
      filters,
    });
    const active: ActiveSubscription = {
      token: {},
      filters,
      reply,
      recentEventIds: new Set(),
      recentEventIdQueue: [],
      pendingReplyCount: 0,
      replyChain: Promise.resolve(),
      backpressureReported: false,
      eoseSent: false,
    };
    subscriptions.set(message.subscriptionId, active);

    try {
      const relaySubscription = this.relay.subscribe(filters.map(cloneFilter), {
        onEvent: (event) => {
          this.queueRelayEvent(peerId, message.subscriptionId, active.token, event);
        },
        onEose: () => {
          this.queueRelayEose(peerId, message.subscriptionId, active.token);
        },
        onClose: () => {
          this.closeSubscription(peerId, message.subscriptionId, active.token, false);
        },
      });
      if (subscriptions.get(message.subscriptionId) !== active) {
        relaySubscription.close('FIPS subscription closed during relay setup');
        return;
      }
      active.relaySubscription = relaySubscription;
      active.timer = setTimeout(() => {
        this.closeSubscription(peerId, message.subscriptionId, active.token);
      }, this.limits.subscriptionTtlMs);
    } catch (error) {
      this.closeSubscription(peerId, message.subscriptionId, active.token, false);
      throw error;
    }
  }

  private queueRelayEvent(
    peerId: string,
    subscriptionId: string,
    token: object,
    event: NostrEvent,
  ): void {
    const active = this.peers.get(peerId)?.get(subscriptionId);
    if (active === undefined || active.token !== token) return;

    let verified: NostrVerifiedEvent;
    try {
      verified = verifyNostrEvent(event);
    } catch (error) {
      this.onError(asError(error), { operation: 'relay-event', peerId, subscriptionId });
      return;
    }
    if (!subscriptionFiltersMatch(active.filters, verified)) return;
    if (active.recentEventIds.has(verified.id)) return;
    if (active.pendingReplyCount >= MAX_PENDING_REPLIES_PER_SUBSCRIPTION) {
      if (!active.backpressureReported) {
        active.backpressureReported = true;
        this.onError(serviceError('subscription reply queue is full'), {
          operation: 'relay-event',
          peerId,
          subscriptionId,
        });
      }
      return;
    }
    rememberEventId(active, verified.id);

    const frame = this.adapter.encodeOutbound({
      type: 'event',
      subscriptionId,
      event: verified,
    });
    this.queueReply(peerId, subscriptionId, active, frame, 'relay-event');
  }

  private queueRelayEose(peerId: string, subscriptionId: string, token: object): void {
    const active = this.peers.get(peerId)?.get(subscriptionId);
    if (active === undefined || active.token !== token || active.eoseSent) return;
    active.eoseSent = true;
    const frame = this.adapter.encodeOutbound({
      type: 'eose',
      subscriptionId,
      eventCount: active.recentEventIds.size,
    });
    this.queueReply(peerId, subscriptionId, active, frame, 'relay-eose');
  }

  private queueReply(
    peerId: string,
    subscriptionId: string,
    active: ActiveSubscription,
    frame: Uint8Array,
    operation: 'relay-event' | 'relay-eose',
  ): void {
    active.pendingReplyCount += 1;
    const task = active.replyChain.catch(() => {}).then(async () => {
      if (this.peers.get(peerId)?.get(subscriptionId) !== active) return;
      await active.reply(frame, FIPS_NOSTR_PUBSUB_SERVICE_PORT);
    });
    active.replyChain = task;
    this.pendingReplies.add(task);
    void task
      .catch((error: unknown) => {
        this.onError(asError(error), { operation, peerId, subscriptionId });
      })
      .finally(() => {
        active.pendingReplyCount -= 1;
        if (active.pendingReplyCount < MAX_PENDING_REPLIES_PER_SUBSCRIPTION) {
          active.backpressureReported = false;
        }
        this.pendingReplies.delete(task);
      });
  }

  private closeSubscription(
    peerId: string,
    subscriptionId: string,
    expectedToken?: object,
    closeRelay = true,
  ): void {
    const subscriptions = this.peers.get(peerId);
    const active = subscriptions?.get(subscriptionId);
    if (active === undefined || (expectedToken !== undefined && active.token !== expectedToken)) {
      return;
    }
    subscriptions?.delete(subscriptionId);
    if (active.timer !== undefined) clearTimeout(active.timer);
    if (closeRelay) {
      try {
        active.relaySubscription?.close('FIPS subscription closed');
      } catch (error) {
        this.onError(asError(error), { operation: 'subscription-close', peerId, subscriptionId });
      }
    }
    this.adapter.subscriptions.remove(peerId, subscriptionId);
    if (subscriptions?.size === 0) this.peers.delete(peerId);
  }
}

function rememberEventId(active: ActiveSubscription, eventId: string): void {
  active.recentEventIds.add(eventId);
  active.recentEventIdQueue.push(eventId);
  if (active.recentEventIdQueue.length <= MAX_RECENT_EVENT_IDS_PER_SUBSCRIPTION) return;
  const oldest = active.recentEventIdQueue.shift();
  if (oldest !== undefined) active.recentEventIds.delete(oldest);
}

function boundedReplayFilters(filters: NostrFilter[], maxReplay: number): NostrFilter[] {
  return filters.map((filter) => ({
    ...cloneFilter(filter),
    limit: Math.min(filter.limit ?? maxReplay, maxReplay),
  }));
}

function authenticatedPeerId(value: string): string {
  const normalized = value.toLowerCase();
  if (!/^(02|03)[0-9a-f]{64}$/.test(normalized)) {
    throw serviceError('source is not an authenticated FIPS peer public key');
  }
  return normalized;
}

function parseClosedSession(event: unknown): string | undefined {
  if (event === null || typeof event !== 'object') return undefined;
  const candidate = event as { remotePubkey?: unknown; state?: unknown };
  if (candidate.state !== 'closed' || typeof candidate.remotePubkey !== 'string') {
    return undefined;
  }
  return /^(02|03)[0-9a-f]{64}$/i.test(candidate.remotePubkey)
    ? candidate.remotePubkey.toLowerCase()
    : undefined;
}

function validatedLimits(
  overrides: Partial<FipsNostrRelayServiceLimits> | undefined,
): FipsNostrRelayServiceLimits {
  const limits = { ...defaultFipsNostrRelayServiceLimits(), ...overrides };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw serviceError(`${name} must be a positive safe integer`);
    }
  }
  if (limits.maxReplayEventsPerFilter > FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS) {
    throw serviceError(
      `maxReplayEventsPerFilter cannot exceed ${FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS}`,
    );
  }
  if (limits.maxFrameBytes > DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES) {
    throw serviceError(
      `maxFrameBytes cannot exceed ${DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES}`,
    );
  }
  return limits;
}

function serviceError(message: string): PubsubError {
  return PubsubError.validation(`FIPS Nostr relay service: ${message}`);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
