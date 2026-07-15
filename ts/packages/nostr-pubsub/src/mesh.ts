import {
  meshEventJsonBytes,
  type InvWantWireMessage,
} from './mesh-codec.js';
import { meshPeer, selectMeshPeers, type MeshPeer } from './mesh-peer.js';
import { BoundedEventCache, type InvWantMeshRetainedState } from './mesh-resources.js';
import {
  clamp,
  requireEventId,
  requireKind,
  requireNow,
  requireUnsignedByte,
  retainMap,
  retainOrder,
  saturatingAdd,
  validation,
} from './mesh-state.js';
import {
  INVALID_MESSAGE_PENALTY,
  MAX_TRACKED_PEER_BEHAVIORS,
  MAX_UPSTREAM_PROVIDERS_PER_EVENT,
  MIN_PEER_BEHAVIOR_SAMPLES,
  UNSERVED_INVENTORY_PENALTY,
  VALID_FRAME_REWARD,
  defaultInvWantMeshOptions,
  normalizeInvWantMeshOptions,
  type InvWantAction,
  type InvWantMeshOptions,
  type PeerBehaviorEvidence,
  type PeerBehaviorObservation,
  type PendingPeers,
  type UpstreamRoute,
} from './mesh-types.js';
import type { NostrEvent, NostrVerifiedEvent } from './types.js';
import { copyVerifiedNostrEvent, verifyNostrEvent } from './types.js';

export { defaultInvWantMeshOptions };
export type { InvWantAction, InvWantMeshOptions, PeerBehaviorObservation };

/** Transport-neutral bounded inv/want state machine matching Rust's `InvWantMesh`. */
export class InvWantMesh {
  readonly options: InvWantMeshOptions;
  private readonly cachedEvents: BoundedEventCache;
  private readonly seenInventories = new Map<string, number>();
  private readonly seenOrder: string[] = [];
  private readonly deliveredEvents = new Map<string, number>();
  private readonly deliveredOrder: string[] = [];
  private readonly upstreamRoutes = new Map<string, UpstreamRoute>();
  private readonly pendingDownstream = new Map<string, PendingPeers>();
  private pendingPeerCount = 0;
  private readonly wantForwarded = new Map<string, number>();
  private readonly peerBehaviors = new Map<string, PeerBehaviorObservation>();
  private readonly peerBehaviorOrder: string[] = [];

  constructor(options: Partial<InvWantMeshOptions> = {}) {
    this.options = normalizeInvWantMeshOptions(options);
    this.cachedEvents = new BoundedEventCache(
      this.options.maxCachedEvents,
      this.options.maxCachedEventBytes,
      this.options.eventTtlMs,
    );
  }

  retainedState(): InvWantMeshRetainedState {
    return {
      cachedEvents: this.cachedEvents.size,
      cachedEventBytes: this.cachedEvents.payloadBytes,
      seenInventories: this.seenInventories.size,
      deliveredEvents: this.deliveredEvents.size,
      upstreamRoutes: this.upstreamRoutes.size,
      transportDisruptedRoutePeers: [...this.upstreamRoutes.values()]
        .reduce((total, route) => total + route.transportDisruptedPeerIds.size, 0),
      pendingEvents: this.pendingDownstream.size,
      pendingPeers: this.pendingPeerCount,
      forwardedWants: this.wantForwarded.size,
      peerBehaviors: this.peerBehaviors.size,
    };
  }

  peerBehaviorScore(peerId: string): number | undefined {
    return this.peerBehaviorObservation(peerId)?.score;
  }

  peerBehaviorObservation(peerId: string): PeerBehaviorObservation | undefined {
    const behavior = this.peerBehaviors.get(peerId);
    return behavior !== undefined && behavior.samples >= MIN_PEER_BEHAVIOR_SAMPLES
      ? { ...behavior }
      : undefined;
  }

  recordInvalidMessage(peerId: string): void {
    this.recordPeerBehavior(peerId, INVALID_MESSAGE_PENALTY, 'invalidMessages');
  }

  /** Prune expired state and score requested inventories that were never served. */
  maintain(nowMs: number): void {
    requireNow(nowMs);
    this.prune(nowMs);
  }

  dismissFrame(peerId: string, eventId: string): void {
    const route = this.upstreamRoutes.get(eventId);
    if (route !== undefined && routeHasProvider(route, peerId)) {
      route.fulfilled = true;
      route.transportDisruptedPeerIds.clear();
      this.removePendingEvent(eventId);
    }
  }

  /** Mark a locally confirmed request failure; another want clears it. */
  recordTransportDisruption(peerId: string, eventId: string): boolean {
    const route = this.upstreamRoutes.get(eventId);
    if (route === undefined || route.fulfilled || !routeHasProvider(route, peerId)) return false;
    const before = route.transportDisruptedPeerIds.size;
    route.transportDisruptedPeerIds.add(peerId);
    return route.transportDisruptedPeerIds.size !== before;
  }

  publish(event: NostrEvent, peers: readonly MeshPeer[], nowMs: number): InvWantAction[] {
    return this.publishEvent(event, peers, nowMs, false);
  }

  /** Publish an event whose signature was already checked at the trust boundary. */
  publishVerified(
    event: NostrVerifiedEvent,
    peers: readonly MeshPeer[],
    nowMs: number,
  ): InvWantAction[] {
    return this.publishEvent(event, peers, nowMs, true);
  }

  private publishEvent(
    event: NostrEvent,
    peers: readonly MeshPeer[],
    nowMs: number,
    alreadyVerified: boolean,
  ): InvWantAction[] {
    requireNow(nowMs);
    this.prune(nowMs);
    const verified = this.acceptEvent(event, alreadyVerified);
    const payloadBytes = meshEventJsonBytes(verified);
    this.storeEvent(verified, payloadBytes, nowMs);
    if (!this.rememberInventory(verified.id, nowMs)) return [];
    return this.sendToSelectedPeers(peers, undefined, {
      type: 'inventory',
      eventId: verified.id,
      eventKind: verified.kind,
      payloadBytes,
      hopLimit: this.options.maxHops,
    });
  }

  replayToPeer(event: NostrEvent, peerId: string, nowMs: number): InvWantAction[] {
    return this.replayEventToPeer(event, peerId, nowMs, false);
  }

  /** Replay a verified event without repeating signature verification. */
  replayVerifiedToPeer(
    event: NostrVerifiedEvent,
    peerId: string,
    nowMs: number,
  ): InvWantAction[] {
    return this.replayEventToPeer(event, peerId, nowMs, true);
  }

  private replayEventToPeer(
    event: NostrEvent,
    peerId: string,
    nowMs: number,
    alreadyVerified: boolean,
  ): InvWantAction[] {
    requireNow(nowMs);
    this.prune(nowMs);
    const verified = this.acceptEvent(event, alreadyVerified);
    const payloadBytes = meshEventJsonBytes(verified);
    this.storeEvent(verified, payloadBytes, nowMs);
    return [send(peerId, {
      type: 'inventory',
      eventId: verified.id,
      eventKind: verified.kind,
      payloadBytes,
      hopLimit: this.options.maxHops,
    })];
  }

  receive(
    sourcePeer: string,
    message: InvWantWireMessage,
    peers: readonly MeshPeer[],
    nowMs: number,
  ): InvWantAction[] {
    requireNow(nowMs);
    this.prune(nowMs);
    try {
      switch (message.type) {
        case 'inventory':
          return this.receiveInventory(sourcePeer, message, nowMs);
        case 'want':
          return this.receiveWant(sourcePeer, message.eventId, nowMs);
        case 'frame':
          return this.receiveFrame(sourcePeer, message.eventId, message.event, peers, nowMs, false);
      }
    } catch (error) {
      this.recordInvalidMessage(sourcePeer);
      throw error;
    }
  }

  /** Admit a frame already verified by event policy at the trust boundary. */
  receiveVerifiedFrame(
    sourcePeer: string,
    eventId: string,
    event: NostrVerifiedEvent,
    peers: readonly MeshPeer[],
    nowMs: number,
  ): InvWantAction[] {
    requireNow(nowMs);
    this.prune(nowMs);
    try {
      return this.receiveFrame(sourcePeer, eventId, event, peers, nowMs, true);
    } catch (error) {
      this.recordInvalidMessage(sourcePeer);
      throw error;
    }
  }

  private receiveInventory(
    sourcePeer: string,
    message: Extract<InvWantWireMessage, { type: 'inventory' }>,
    nowMs: number,
  ): InvWantAction[] {
    requireEventId(message.eventId);
    this.validateKind(message.eventKind);
    this.validateEventLength(message.payloadBytes);
    requireUnsignedByte(message.hopLimit, 'hop limit');
    if (message.hopLimit === 0) return [];
    if (message.hopLimit > this.options.maxHops) {
      throw validation(`inv/want hop limit ${message.hopLimit} exceeds local maximum ${this.options.maxHops}`);
    }
    if (this.cachedEvents.has(message.eventId)) return [];
    if (!this.rememberInventory(message.eventId, nowMs)) {
      const route = this.upstreamRoutes.get(message.eventId);
      if (route === undefined || route.fulfilled) {
        return [];
      }
      if (route.eventKind !== message.eventKind || route.payloadBytes !== message.payloadBytes) {
        throw validation('retried inv/want inventory changed kind or size');
      }
      route.hopLimit = Math.max(route.hopLimit, message.hopLimit);
      if (!routeHasProvider(route, sourcePeer)) {
        if (route.alternatePeerIds.size + 1 >= MAX_UPSTREAM_PROVIDERS_PER_EVENT) return [];
        route.alternatePeerIds.add(sourcePeer);
        const extendedExpiry = saturatingAdd(nowMs, this.options.routeTtlMs);
        route.expiresAtMs = extendedExpiry;
        this.seenInventories.set(message.eventId, extendedExpiry);
        this.wantForwarded.set(message.eventId, extendedExpiry);
      }
      route.transportDisruptedPeerIds.delete(sourcePeer);
      return [send(sourcePeer, { type: 'want', eventId: message.eventId })];
    }
    const expiresAtMs = saturatingAdd(nowMs, this.options.routeTtlMs);
    if (!this.upstreamRoutes.has(message.eventId)) {
      this.upstreamRoutes.set(message.eventId, {
        peerId: sourcePeer,
        alternatePeerIds: new Set(),
        transportDisruptedPeerIds: new Set(),
        eventKind: message.eventKind,
        payloadBytes: message.payloadBytes,
        hopLimit: message.hopLimit,
        expiresAtMs,
        fulfilled: false,
      });
    }
    this.wantForwarded.set(message.eventId, expiresAtMs);
    return [send(sourcePeer, { type: 'want', eventId: message.eventId })];
  }

  private receiveWant(sourcePeer: string, eventId: string, nowMs: number): InvWantAction[] {
    requireEventId(eventId);
    const cached = this.cachedEvents.get(eventId);
    if (cached !== undefined) return [send(sourcePeer, { type: 'frame', eventId, event: cached })];
    const route = this.upstreamRoutes.get(eventId);
    if (route === undefined) return [];
    if (route.fulfilled) return [];

    let pending = this.pendingDownstream.get(eventId);
    if (pending === undefined) {
      pending = { peers: new Set(), expiresAtMs: saturatingAdd(nowMs, this.options.routeTtlMs) };
      this.pendingDownstream.set(eventId, pending);
    }
    if (
      pending.peers.size < this.options.maxPendingPeersPerEvent &&
      !pending.peers.has(sourcePeer)
    ) {
      pending.peers.add(sourcePeer);
      this.pendingPeerCount += 1;
    }
    if ((this.wantForwarded.get(eventId) ?? 0) > nowMs) return [];
    route.transportDisruptedPeerIds.delete(route.peerId);
    this.wantForwarded.set(eventId, route.expiresAtMs);
    return [send(route.peerId, { type: 'want', eventId })];
  }

  private receiveFrame(
    sourcePeer: string,
    eventId: string,
    event: NostrEvent,
    peers: readonly MeshPeer[],
    nowMs: number,
    alreadyVerified: boolean,
  ): InvWantAction[] {
    requireEventId(eventId);
    const route = this.upstreamRoutes.get(eventId);
    if (route === undefined) throw validation('unsolicited inv/want frame');
    if (!this.wantForwarded.has(eventId) || !routeHasProvider(route, sourcePeer)) {
      throw validation('inv/want frame was not requested from source');
    }
    const verified = this.acceptEvent(event, alreadyVerified);
    const payloadBytes = meshEventJsonBytes(verified);
    if (verified.id !== eventId) throw validation('inv/want frame id does not match signed event id');
    if (route.eventKind !== verified.kind || route.payloadBytes !== payloadBytes) {
      throw validation('inv/want frame does not match announced kind or payload size');
    }
    if (route.fulfilled) return [];
    this.recordPeerBehavior(sourcePeer, VALID_FRAME_REWARD, 'validFrames');
    this.storeEvent(verified, payloadBytes, nowMs);
    route.fulfilled = true;
    route.transportDisruptedPeerIds.clear();

    const actions: InvWantAction[] = [];
    if (this.rememberDelivered(eventId, nowMs)) {
      actions.push({ type: 'deliver', sourcePeer, event: verified });
    }
    const pending = this.removePendingEvent(eventId);
    if (pending !== undefined) {
      for (const peerId of [...pending.peers].sort()) {
        actions.push(send(peerId, { type: 'frame', eventId, event: verified }));
      }
    }
    if (route.hopLimit > 1) {
      actions.push(...this.sendToSelectedPeers(peers, sourcePeer, {
        type: 'inventory',
        eventId,
        eventKind: verified.kind,
        payloadBytes,
        hopLimit: route.hopLimit - 1,
      }));
    }
    return actions;
  }

  private validateEvent(event: NostrEvent): NostrVerifiedEvent {
    const verified = verifyNostrEvent(event);
    this.validateKind(verified.kind);
    this.validateEventLength(meshEventJsonBytes(verified));
    return verified;
  }

  private acceptEvent(event: NostrEvent, alreadyVerified: boolean): NostrVerifiedEvent {
    if (!alreadyVerified) return this.validateEvent(event);
    const verified = copyVerifiedNostrEvent(event as NostrVerifiedEvent);
    this.validateKind(verified.kind);
    this.validateEventLength(meshEventJsonBytes(verified));
    return verified;
  }

  private validateKind(kind: number): void {
    requireKind(kind);
    if (this.options.allowedKinds !== undefined && !this.options.allowedKinds.has(kind)) {
      throw validation(`unsupported Nostr event kind ${kind}`);
    }
  }

  private validateEventLength(length: number): void {
    if (
      !Number.isSafeInteger(length) ||
      length < 0 ||
      length > 0xffff_ffff ||
      length > this.options.maxEventBytes
    ) {
      throw validation(`inv/want event is ${length} bytes, maximum is ${this.options.maxEventBytes}`);
    }
  }

  private storeEvent(event: NostrVerifiedEvent, payloadBytes: number, nowMs: number): void {
    this.cachedEvents.store(event, payloadBytes, nowMs);
  }

  private rememberInventory(eventId: string, nowMs: number): boolean {
    if ((this.seenInventories.get(eventId) ?? 0) > nowMs) return false;
    if (!this.seenInventories.has(eventId)) {
      while (this.seenInventories.size >= this.options.maxSeenEvents) {
        const oldest = this.seenOrder.shift();
        if (oldest === undefined) break;
        this.seenInventories.delete(oldest);
        this.upstreamRoutes.delete(oldest);
        this.removePendingEvent(oldest);
        this.wantForwarded.delete(oldest);
      }
      this.seenOrder.push(eventId);
    }
    this.seenInventories.set(eventId, saturatingAdd(nowMs, this.options.routeTtlMs));
    return true;
  }

  private removePendingEvent(eventId: string): PendingPeers | undefined {
    const pending = this.pendingDownstream.get(eventId);
    if (pending === undefined) return undefined;
    this.pendingDownstream.delete(eventId);
    this.pendingPeerCount -= pending.peers.size;
    return pending;
  }

  private rememberDelivered(eventId: string, nowMs: number): boolean {
    if (this.deliveredEvents.has(eventId)) return false;
    this.deliveredOrder.push(eventId);
    while (this.deliveredEvents.size >= this.options.maxSeenEvents) {
      const oldest = this.deliveredOrder.shift();
      if (oldest === undefined) break;
      this.deliveredEvents.delete(oldest);
    }
    this.deliveredEvents.set(eventId, saturatingAdd(nowMs, this.options.eventTtlMs));
    return true;
  }

  private sendToSelectedPeers(
    peers: readonly MeshPeer[],
    excludedPeer: string | undefined,
    message: InvWantWireMessage,
  ): InvWantAction[] {
    return selectMeshPeers(
      this.peersWithBehavior(peers),
      excludedPeer,
      this.options.fanout,
      this.options.unknownPeerReserve,
    ).map((peer) => send(peer.id, message));
  }

  private peersWithBehavior(peers: readonly MeshPeer[]): MeshPeer[] {
    return peers.map((peer) => {
      const local = this.peerBehaviorScore(peer.id);
      if (peer.qualityScore !== undefined && local !== undefined) {
        return meshPeer(peer.id, clamp(peer.qualityScore + local, -100, 100));
      }
      return meshPeer(peer.id, peer.qualityScore ?? local);
    });
  }

  private recordPeerBehavior(peerId: string, delta: number, evidence: PeerBehaviorEvidence): void {
    if (!this.peerBehaviors.has(peerId)) {
      while (this.peerBehaviors.size >= MAX_TRACKED_PEER_BEHAVIORS) {
        const oldest = this.peerBehaviorOrder.shift();
        if (oldest === undefined) break;
        this.peerBehaviors.delete(oldest);
      }
      this.peerBehaviorOrder.push(peerId);
    }
    const behavior = this.peerBehaviors.get(peerId) ?? {
      score: 0, samples: 0, validFrames: 0, invalidMessages: 0, unservedInventories: 0,
    };
    behavior.samples = Math.min(0xffff_ffff, behavior.samples + 1);
    behavior.score = clamp(behavior.score + delta, -100, 100);
    behavior[evidence] = Math.min(0xffff_ffff, behavior[evidence] + 1);
    this.peerBehaviors.set(peerId, behavior);
  }

  private prune(nowMs: number): void {
    for (const route of this.upstreamRoutes.values()) {
      if (route.fulfilled || route.expiresAtMs > nowMs) continue;
      if (!route.transportDisruptedPeerIds.has(route.peerId))
        this.recordPeerBehavior(route.peerId, UNSERVED_INVENTORY_PENALTY, 'unservedInventories');
      for (const peerId of route.alternatePeerIds) {
        if (!route.transportDisruptedPeerIds.has(peerId))
          this.recordPeerBehavior(peerId, UNSERVED_INVENTORY_PENALTY, 'unservedInventories');
      }
    }
    this.cachedEvents.prune(nowMs);
    retainMap(this.seenInventories, (expiry) => expiry > nowMs);
    retainOrder(this.seenOrder, this.seenInventories);
    retainMap(this.deliveredEvents, (expiry) => expiry > nowMs);
    retainOrder(this.deliveredOrder, this.deliveredEvents);
    retainMap(this.upstreamRoutes, (route) => route.expiresAtMs > nowMs);
    retainMap(this.pendingDownstream, (pending, eventId) =>
      pending.expiresAtMs > nowMs && this.upstreamRoutes.has(eventId));
    this.pendingPeerCount = [...this.pendingDownstream.values()]
      .reduce((total, pending) => total + pending.peers.size, 0);
    retainMap(this.wantForwarded, (expiry, eventId) =>
      expiry > nowMs && this.upstreamRoutes.has(eventId));
  }
}

function send(peerId: string, message: InvWantWireMessage): InvWantAction {
  return { type: 'send', peerId, message };
}

function routeHasProvider(route: UpstreamRoute, peerId: string): boolean {
  return route.peerId === peerId || route.alternatePeerIds.has(peerId);
}
