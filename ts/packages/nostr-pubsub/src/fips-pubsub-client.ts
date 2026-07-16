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
  FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES,
  FipsPubsubWireCodec,
  type FipsPubsubWireMessage,
} from './wire.js';
import {
  FIPS_NOSTR_PUBSUB_SERVICE_PORT,
  type FipsPubsubServiceContext,
} from './fips-relay-service.js';
import {
  defaultFipsNostrPubsubClientLimits,
  type FipsNostrPubsubClientErrorContext,
  type FipsNostrPubsubClientLimits,
  type FipsNostrPubsubClientOptions,
  type FipsNostrPubsubEventHandler,
  type FipsNostrPubsubSubscription,
  type FipsPubsubClientNode,
} from './fips-pubsub-client-types.js';

export const FIPS_NOSTR_PUBSUB_CAPABILITY = 'nostr.pubsub/1';

interface LocalSubscription {
  readonly id: string;
  readonly filters: NostrFilter[];
  readonly handler: FipsNostrPubsubEventHandler;
  readonly peers: Set<string>;
  readonly recentIds: Set<string>;
  readonly recentOrder: string[];
}

interface CachedEvent {
  event: NostrVerifiedEvent;
  sourcePeer?: string;
}

export class FipsNostrPubsubClient {
  readonly limits: FipsNostrPubsubClientLimits;
  readonly codec: FipsPubsubWireCodec;

  private readonly node: FipsPubsubClientNode;
  private readonly peers: FipsNostrPubsubClientOptions['peers'];
  private readonly allowedKinds?: Set<number>;
  private readonly onError: NonNullable<FipsNostrPubsubClientOptions['onError']>;
  private peerSubscriptions: PubsubPeerSubscriptionStore;
  private readonly subscriptions = new Map<string, LocalSubscription>();
  private readonly cachedEvents = new Map<string, CachedEvent>();
  private readonly cachedEventOrder: string[] = [];
  private readonly pending = new Set<Promise<unknown>>();
  private unregisterService?: () => void;
  private removePeerListener?: () => void;
  private removeSessionListener?: () => void;
  private nextSubscriptionId = 1;

  constructor(options: FipsNostrPubsubClientOptions) {
    this.node = options.node;
    this.peers = options.peers;
    this.onError = options.onError ?? (() => {});
    this.limits = validateLimits(options.limits);
    this.codec = new FipsPubsubWireCodec(this.limits.maxFrameBytes);
    this.allowedKinds = normalizeAllowedKinds(options.allowedKinds);
    this.peerSubscriptions = createPeerSubscriptionStore(this.limits);
  }

  start(): this {
    if (this.unregisterService !== undefined) return this;
    this.unregisterService = this.node.registerService(
      FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      (context) => this.handle(context),
    );
    this.removePeerListener = this.node.on?.('peer', (event) => this.handlePeerEvent(event));
    this.removeSessionListener = this.node.on?.('session', (event) => this.handleSessionEvent(event));
    this.refreshPeers();
    return this;
  }

  async stop(): Promise<void> {
    for (const subscriptionId of [...this.subscriptions.keys()]) {
      this.closeSubscription(subscriptionId);
    }
    await this.idle();
    this.unregisterService?.();
    this.unregisterService = undefined;
    this.removePeerListener?.();
    this.removePeerListener = undefined;
    this.removeSessionListener?.();
    this.removeSessionListener = undefined;
    this.peerSubscriptions = createPeerSubscriptionStore(this.limits);
    this.cachedEvents.clear();
    this.cachedEventOrder.length = 0;
  }

  subscribe(
    filters: NostrFilter[],
    handler: FipsNostrPubsubEventHandler,
  ): FipsNostrPubsubSubscription {
    this.requireStarted();
    if (this.subscriptions.size >= this.limits.maxActiveSubscriptions) {
      throw clientError(`active subscription limit is ${this.limits.maxActiveSubscriptions}`);
    }
    if (filters.length === 0 || filters.length > this.limits.maxFiltersPerSubscription) {
      throw clientError(
        `subscription requires 1..${this.limits.maxFiltersPerSubscription} filters`,
      );
    }
    const id = `ts-${this.nextSubscriptionId.toString(36)}`;
    this.nextSubscriptionId += 1;
    const normalized = this.normalizedFilters(id, filters);
    const subscription: LocalSubscription = {
      id,
      filters: normalized,
      handler,
      peers: new Set(),
      recentIds: new Set(),
      recentOrder: [],
    };
    this.subscriptions.set(id, subscription);
    this.refreshSubscriptionPeers(subscription);
    this.replayLocal(subscription);
    let closed = false;
    return {
      id,
      close: () => {
        if (closed) return;
        closed = true;
        this.closeSubscription(id);
      },
    };
  }

  async publish(event: NostrEvent | NostrVerifiedEvent): Promise<void> {
    this.requireStarted();
    const verified = this.admitEvent(event);
    const peers = this.currentPeers();
    if (peers.length === 0) throw clientError('no admitted FIPS pubsub peers are available');
    if (!this.rememberEvent(verified)) return;

    const addressed = peers.flatMap((peerId) =>
      this.peerSubscriptions.matchingSubscriptions(peerId, verified).map((subscription) => ({
        peerId,
        message: {
          type: 'event',
          subscriptionId: subscription.subscriptionId,
          event: verified,
        } satisfies FipsPubsubWireMessage,
      })),
    );
    const deliveries = addressed.length > 0
      ? addressed
      : peers.map((peerId) => ({
        peerId,
        message: { type: 'event', event: verified } satisfies FipsPubsubWireMessage,
      }));
    const results = await Promise.all(deliveries.map(async ({ peerId, message }) => {
      try {
        await this.track(this.send(peerId, message));
        return true;
      } catch (error) {
        this.report(error, { operation: 'send', peerId });
        return false;
      }
    }));
    if (!results.some(Boolean)) {
      this.cachedEvents.delete(verified.id);
      const cachedIndex = this.cachedEventOrder.indexOf(verified.id);
      if (cachedIndex >= 0) this.cachedEventOrder.splice(cachedIndex, 1);
      throw clientError('all FIPS pubsub deliveries failed');
    }
  }

  refreshPeers(): void {
    for (const subscription of this.subscriptions.values()) {
      this.refreshSubscriptionPeers(subscription);
    }
  }

  peerSubscriptionCount(peerId: string): number {
    return this.peerSubscriptions.peerSubscriptionCount(peerId.toLowerCase());
  }

  activeSubscriptionCount(): number {
    return this.subscriptions.size;
  }

  async idle(): Promise<void> {
    while (this.pending.size > 0) await Promise.allSettled([...this.pending]);
  }

  private async handle(context: FipsPubsubServiceContext): Promise<void> {
    const peerId = normalizePeerId(context.src);
    if (
      peerId === undefined ||
      context.srcPort !== FIPS_NOSTR_PUBSUB_SERVICE_PORT ||
      context.dstPort !== FIPS_NOSTR_PUBSUB_SERVICE_PORT ||
      !this.currentPeers().includes(peerId)
    ) {
      this.report(clientError('dropped a datagram outside admitted nostr.pubsub/1 peers'), {
        operation: 'receive',
        peerId,
      });
      return;
    }

    let message: FipsPubsubWireMessage;
    try {
      message = this.codec.decodeFrame(context.payload);
      if (message.type === 'event') this.admitEvent(message.event);
    } catch (error) {
      this.report(error, { operation: 'receive', peerId });
      return;
    }

    if (message.type === 'req') {
      this.peerSubscriptions.upsertFilters(peerId, message.subscriptionId, message.filters);
      for (const cached of this.replayEvents(message.filters)) {
        const frame = this.codec.encodeFrame({
          type: 'event',
          subscriptionId: message.subscriptionId,
          event: cached.event,
        });
        this.background(
          context.reply(frame, FIPS_NOSTR_PUBSUB_SERVICE_PORT),
          { operation: 'send', peerId, subscriptionId: message.subscriptionId },
        );
      }
      return;
    }
    if (message.type === 'close') {
      this.peerSubscriptions.remove(peerId, message.subscriptionId);
      return;
    }

    const isNew = this.rememberEvent(message.event, peerId);
    if (message.subscriptionId !== undefined) {
      const subscription = this.subscriptions.get(message.subscriptionId);
      if (subscription !== undefined) this.deliver(subscription, message.event, peerId);
    } else {
      for (const subscription of this.subscriptions.values()) {
        this.deliver(subscription, message.event, peerId);
      }
    }
    if (isNew) this.forwardEvent(message.event, peerId);
  }

  private forwardEvent(event: NostrVerifiedEvent, sourcePeer: string): void {
    for (const peerId of this.currentPeers()) {
      if (peerId === sourcePeer) continue;
      for (const subscription of this.peerSubscriptions.matchingSubscriptions(peerId, event)) {
        this.background(this.send(peerId, {
          type: 'event',
          subscriptionId: subscription.subscriptionId,
          event,
        }), { operation: 'send', peerId, subscriptionId: subscription.subscriptionId });
      }
    }
  }

  private normalizedFilters(subscriptionId: string, filters: NostrFilter[]): NostrFilter[] {
    const decoded = this.codec.decodeFrame(this.codec.encodeFrame({
      type: 'req',
      subscriptionId,
      filters: filters.map(cloneFilter),
    }));
    if (decoded.type !== 'req') throw clientError('failed to normalize subscription filters');
    return decoded.filters;
  }

  private refreshSubscriptionPeers(subscription: LocalSubscription): void {
    const current = new Set(this.currentPeers());
    for (const peerId of [...subscription.peers]) {
      if (!current.has(peerId)) subscription.peers.delete(peerId);
    }
    for (const peerId of current) {
      if (subscription.peers.has(peerId)) continue;
      subscription.peers.add(peerId);
      this.background(this.send(peerId, {
        type: 'req',
        subscriptionId: subscription.id,
        filters: subscription.filters,
      }), { operation: 'send', peerId, subscriptionId: subscription.id });
    }
  }

  private closeSubscription(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (subscription === undefined) return;
    this.subscriptions.delete(subscriptionId);
    for (const peerId of subscription.peers) {
      this.background(this.send(peerId, { type: 'close', subscriptionId }), {
        operation: 'send',
        peerId,
        subscriptionId,
      });
    }
  }

  private deliver(
    subscription: LocalSubscription,
    event: NostrVerifiedEvent,
    sourcePeer: string,
  ): void {
    if (
      !subscription.peers.has(sourcePeer) ||
      !subscriptionFiltersMatch(subscription.filters, event) ||
      subscription.recentIds.has(event.id)
    ) return;
    rememberId(
      subscription.recentIds,
      subscription.recentOrder,
      event.id,
      this.limits.maxCachedEvents,
    );
    try {
      subscription.handler(event, sourcePeer);
    } catch (error) {
      this.report(error, {
        operation: 'event-handler',
        peerId: sourcePeer,
        subscriptionId: subscription.id,
      });
    }
  }

  private replayLocal(subscription: LocalSubscription): void {
    for (const cached of this.replayEvents(subscription.filters)) {
      if (cached.sourcePeer !== undefined) {
        this.deliver(subscription, cached.event, cached.sourcePeer);
      }
    }
  }

  private replayEvents(filters: NostrFilter[]): CachedEvent[] {
    return this.cachedEventOrder
      .map((eventId) => this.cachedEvents.get(eventId))
      .filter((cached): cached is CachedEvent =>
        cached !== undefined && subscriptionFiltersMatch(filters, cached.event))
      .slice(-this.limits.maxReplayEvents);
  }

  private rememberEvent(event: NostrVerifiedEvent, sourcePeer?: string): boolean {
    if (this.cachedEvents.has(event.id)) return false;
    this.cachedEvents.set(event.id, { event, sourcePeer });
    this.cachedEventOrder.push(event.id);
    while (this.cachedEventOrder.length > this.limits.maxCachedEvents) {
      const removed = this.cachedEventOrder.shift();
      if (removed !== undefined) this.cachedEvents.delete(removed);
    }
    return true;
  }

  private admitEvent(event: NostrEvent | NostrVerifiedEvent): NostrVerifiedEvent {
    const verified = verifyNostrEvent(event);
    if (this.allowedKinds !== undefined && !this.allowedKinds.has(verified.kind)) {
      throw clientError(`event kind ${verified.kind} is not admitted`);
    }
    return verified;
  }

  private currentPeers(): string[] {
    const peers = new Set<string>();
    for (const value of this.peers()) {
      const peerId = normalizePeerId(value);
      if (peerId !== undefined) peers.add(peerId);
      if (peers.size >= this.limits.maxPeers) break;
    }
    return [...peers].sort();
  }

  private send(peerId: string, message: FipsPubsubWireMessage): Promise<void> {
    return this.node.sendDatagram({
      dst: peerId,
      srcPort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      dstPort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
      payload: this.codec.encodeFrame(message),
    });
  }

  private background(task: Promise<unknown>, context: FipsNostrPubsubClientErrorContext): void {
    void this.track(task).catch((error: unknown) => this.report(error, context));
  }

  private track<T>(task: Promise<T>): Promise<T> {
    this.pending.add(task);
    void task.finally(() => this.pending.delete(task)).catch(() => {});
    return task;
  }

  private handlePeerEvent(event: unknown): void {
    const parsed = parseConnectionEvent(event);
    if (parsed === undefined) return;
    if (parsed.connected) this.refreshPeers();
    else this.dropPeer(parsed.peerId);
  }

  private handleSessionEvent(event: unknown): void {
    const parsed = parseConnectionEvent(event);
    if (parsed === undefined) return;
    if (parsed.connected) this.refreshPeers();
    else this.dropPeer(parsed.peerId);
  }

  private dropPeer(peerId: string): void {
    this.peerSubscriptions.removePeer(peerId);
    for (const subscription of this.subscriptions.values()) subscription.peers.delete(peerId);
  }

  private requireStarted(): void {
    if (this.unregisterService === undefined) throw clientError('client is not started');
  }

  private report(error: unknown, context: FipsNostrPubsubClientErrorContext): void {
    this.onError(error instanceof Error ? error : new Error(String(error)), context);
  }
}

function validateLimits(
  overrides: Partial<FipsNostrPubsubClientLimits> | undefined,
): FipsNostrPubsubClientLimits {
  const limits = { ...defaultFipsNostrPubsubClientLimits(), ...overrides };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw clientError(`${name} must be a positive safe integer`);
    }
  }
  if (limits.maxFrameBytes > FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES) {
    throw clientError(`maxFrameBytes cannot exceed ${FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES}`);
  }
  if (limits.maxReplayEvents > limits.maxCachedEvents) {
    throw clientError('maxReplayEvents cannot exceed maxCachedEvents');
  }
  return limits;
}

function createPeerSubscriptionStore(
  limits: FipsNostrPubsubClientLimits,
): PubsubPeerSubscriptionStore {
  return new PubsubPeerSubscriptionStore({
    maxPeers: limits.maxPeers,
    maxSubscriptionsPerPeer: limits.maxSubscriptionsPerPeer,
    maxFiltersPerSubscription: limits.maxFiltersPerSubscription,
  });
}

function normalizeAllowedKinds(kinds: readonly number[] | undefined): Set<number> | undefined {
  if (kinds === undefined) return undefined;
  if (kinds.some((kind) => !Number.isSafeInteger(kind) || kind < 0 || kind > 65_535)) {
    throw clientError('allowedKinds must contain valid Nostr kind integers');
  }
  return new Set(kinds);
}

function normalizePeerId(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const normalized = value.toLowerCase();
  return /^(02|03)[0-9a-f]{64}$/.test(normalized) ? normalized : undefined;
}

function parseConnectionEvent(
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

function rememberId(ids: Set<string>, order: string[], id: string, maximum: number): void {
  ids.add(id);
  order.push(id);
  while (order.length > maximum) {
    const removed = order.shift();
    if (removed !== undefined) ids.delete(removed);
  }
}

function clientError(message: string): PubsubError {
  return PubsubError.validation(`FIPS Nostr pubsub client: ${message}`);
}
