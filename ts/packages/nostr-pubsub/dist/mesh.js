import { DEFAULT_INV_WANT_HOP_LIMIT } from './invwant.js';
import { DEFAULT_INV_WANT_FANOUT, DEFAULT_INV_WANT_MAX_EVENT_BYTES, meshEventJsonBytes, } from './mesh-codec.js';
import { meshPeer, selectMeshPeers } from './mesh-peer.js';
import { PubsubError, verifyNostrEvent } from './types.js';
const DEFAULT_ROUTE_TTL_MS = 2 * 60 * 1_000;
const DEFAULT_EVENT_TTL_MS = 10 * 60 * 1_000;
const MAX_TRACKED_PEER_BEHAVIORS = 4_096;
const MIN_PEER_BEHAVIOR_SAMPLES = 3;
const VALID_FRAME_REWARD = 20;
const INVALID_MESSAGE_PENALTY = -40;
const UNSERVED_INVENTORY_PENALTY = -20;
export function defaultInvWantMeshOptions() {
    return {
        fanout: DEFAULT_INV_WANT_FANOUT,
        unknownPeerReserve: 1,
        maxHops: DEFAULT_INV_WANT_HOP_LIMIT,
        maxEventBytes: DEFAULT_INV_WANT_MAX_EVENT_BYTES,
        maxCachedEvents: 1_024,
        maxSeenEvents: 4_096,
        maxPendingPeersPerEvent: 64,
        routeTtlMs: DEFAULT_ROUTE_TTL_MS,
        eventTtlMs: DEFAULT_EVENT_TTL_MS,
    };
}
/** Transport-neutral bounded inv/want state machine matching Rust's `InvWantMesh`. */
export class InvWantMesh {
    options;
    cachedEvents = new Map();
    cacheOrder = [];
    seenInventories = new Map();
    seenOrder = [];
    deliveredEvents = new Set();
    deliveredOrder = [];
    upstreamRoutes = new Map();
    pendingDownstream = new Map();
    wantForwarded = new Map();
    peerBehaviors = new Map();
    peerBehaviorOrder = [];
    constructor(options = {}) {
        const defaults = defaultInvWantMeshOptions();
        const merged = { ...defaults, ...options };
        this.options = {
            ...merged,
            fanout: boundedPositive(merged.fanout),
            unknownPeerReserve: Math.min(nonNegative(merged.unknownPeerReserve), boundedPositive(merged.fanout)),
            maxHops: boundedPositive(merged.maxHops, 255),
            maxEventBytes: boundedPositive(merged.maxEventBytes),
            maxCachedEvents: boundedPositive(merged.maxCachedEvents),
            maxSeenEvents: boundedPositive(merged.maxSeenEvents),
            maxPendingPeersPerEvent: boundedPositive(merged.maxPendingPeersPerEvent),
            routeTtlMs: boundedPositive(merged.routeTtlMs),
            eventTtlMs: Math.max(boundedPositive(merged.eventTtlMs), boundedPositive(merged.routeTtlMs)),
            allowedKinds: merged.allowedKinds === undefined ? undefined : new Set([...merged.allowedKinds].map(requireKind)),
        };
    }
    peerBehaviorScore(peerId) {
        const behavior = this.peerBehaviors.get(peerId);
        return behavior !== undefined && behavior.samples >= MIN_PEER_BEHAVIOR_SAMPLES
            ? behavior.score
            : undefined;
    }
    recordInvalidMessage(peerId) {
        this.recordPeerBehavior(peerId, INVALID_MESSAGE_PENALTY);
    }
    dismissFrame(peerId, eventId) {
        if (this.upstreamRoutes.get(eventId)?.peerId === peerId) {
            this.upstreamRoutes.delete(eventId);
            this.wantForwarded.delete(eventId);
        }
    }
    publish(event, peers, nowMs) {
        requireNow(nowMs);
        this.prune(nowMs);
        const verified = this.validateEvent(event);
        const payloadBytes = meshEventJsonBytes(verified);
        this.storeEvent(verified, nowMs);
        if (!this.rememberInventory(verified.id, nowMs))
            return [];
        return this.sendToSelectedPeers(peers, undefined, {
            type: 'inventory',
            eventId: verified.id,
            eventKind: verified.kind,
            payloadBytes,
            hopLimit: this.options.maxHops,
        });
    }
    replayToPeer(event, peerId, nowMs) {
        requireNow(nowMs);
        this.prune(nowMs);
        const verified = this.validateEvent(event);
        const payloadBytes = meshEventJsonBytes(verified);
        this.storeEvent(verified, nowMs);
        return [send(peerId, {
                type: 'inventory',
                eventId: verified.id,
                eventKind: verified.kind,
                payloadBytes,
                hopLimit: this.options.maxHops,
            })];
    }
    receive(sourcePeer, message, peers, nowMs) {
        requireNow(nowMs);
        this.prune(nowMs);
        try {
            switch (message.type) {
                case 'inventory':
                    return this.receiveInventory(sourcePeer, message, nowMs);
                case 'want':
                    return this.receiveWant(sourcePeer, message.eventId, nowMs);
                case 'frame':
                    return this.receiveFrame(sourcePeer, message.eventId, message.event, peers, nowMs);
            }
        }
        catch (error) {
            this.recordInvalidMessage(sourcePeer);
            throw error;
        }
    }
    receiveInventory(sourcePeer, message, nowMs) {
        requireEventId(message.eventId);
        this.validateKind(message.eventKind);
        this.validateEventLength(message.payloadBytes);
        requireUnsignedByte(message.hopLimit, 'hop limit');
        if (message.hopLimit === 0)
            return [];
        if (!this.rememberInventory(message.eventId, nowMs)) {
            const route = this.upstreamRoutes.get(message.eventId);
            if (route === undefined || route.peerId !== sourcePeer || this.cachedEvents.has(message.eventId)) {
                return [];
            }
            if (route.eventKind !== message.eventKind ||
                route.payloadBytes !== message.payloadBytes ||
                route.hopLimit !== message.hopLimit) {
                throw validation('retried inv/want inventory changed kind, size, or hop limit');
            }
            return [send(sourcePeer, { type: 'want', eventId: message.eventId })];
        }
        const expiresAtMs = saturatingAdd(nowMs, this.options.routeTtlMs);
        if (!this.upstreamRoutes.has(message.eventId)) {
            this.upstreamRoutes.set(message.eventId, {
                peerId: sourcePeer,
                eventKind: message.eventKind,
                payloadBytes: message.payloadBytes,
                hopLimit: message.hopLimit,
                expiresAtMs,
            });
        }
        this.wantForwarded.set(message.eventId, expiresAtMs);
        return [send(sourcePeer, { type: 'want', eventId: message.eventId })];
    }
    receiveWant(sourcePeer, eventId, nowMs) {
        requireEventId(eventId);
        const cached = this.cachedEvents.get(eventId);
        if (cached !== undefined)
            return [send(sourcePeer, { type: 'frame', eventId, event: cached.event })];
        let pending = this.pendingDownstream.get(eventId);
        if (pending === undefined) {
            pending = { peers: new Set(), expiresAtMs: saturatingAdd(nowMs, this.options.routeTtlMs) };
            this.pendingDownstream.set(eventId, pending);
        }
        if (pending.peers.size < this.options.maxPendingPeersPerEvent)
            pending.peers.add(sourcePeer);
        const route = this.upstreamRoutes.get(eventId);
        if (route === undefined || (this.wantForwarded.get(eventId) ?? 0) > nowMs)
            return [];
        this.wantForwarded.set(eventId, route.expiresAtMs);
        return [send(route.peerId, { type: 'want', eventId })];
    }
    receiveFrame(sourcePeer, eventId, event, peers, nowMs) {
        requireEventId(eventId);
        const verified = this.validateEvent(event);
        const payloadBytes = meshEventJsonBytes(verified);
        if (verified.id !== eventId)
            throw validation('inv/want frame id does not match signed event id');
        const route = this.upstreamRoutes.get(eventId);
        if (route !== undefined &&
            (route.eventKind !== verified.kind || route.payloadBytes !== payloadBytes)) {
            throw validation('inv/want frame does not match announced kind or payload size');
        }
        if (route?.peerId === sourcePeer && this.wantForwarded.has(eventId)) {
            this.recordPeerBehavior(sourcePeer, VALID_FRAME_REWARD);
        }
        this.storeEvent(verified, nowMs);
        const actions = [];
        if (this.rememberDelivered(eventId))
            actions.push({ type: 'deliver', sourcePeer, event: verified });
        const pending = this.pendingDownstream.get(eventId);
        if (pending !== undefined) {
            for (const peerId of [...pending.peers].sort()) {
                actions.push(send(peerId, { type: 'frame', eventId, event: verified }));
            }
            this.pendingDownstream.delete(eventId);
        }
        this.upstreamRoutes.delete(eventId);
        this.wantForwarded.delete(eventId);
        if (route !== undefined && route.hopLimit > 1) {
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
    validateEvent(event) {
        const verified = verifyNostrEvent(event);
        this.validateKind(verified.kind);
        this.validateEventLength(meshEventJsonBytes(verified));
        return verified;
    }
    validateKind(kind) {
        requireKind(kind);
        if (this.options.allowedKinds !== undefined && !this.options.allowedKinds.has(kind)) {
            throw validation(`unsupported Nostr event kind ${kind}`);
        }
    }
    validateEventLength(length) {
        if (!Number.isSafeInteger(length) ||
            length < 0 ||
            length > 0xffff_ffff ||
            length > this.options.maxEventBytes) {
            throw validation(`inv/want event is ${length} bytes, maximum is ${this.options.maxEventBytes}`);
        }
    }
    storeEvent(event, nowMs) {
        if (!this.cachedEvents.has(event.id)) {
            while (this.cachedEvents.size >= this.options.maxCachedEvents) {
                const oldest = this.cacheOrder.shift();
                if (oldest === undefined)
                    break;
                this.cachedEvents.delete(oldest);
            }
            this.cacheOrder.push(event.id);
        }
        this.cachedEvents.set(event.id, {
            event,
            expiresAtMs: saturatingAdd(nowMs, this.options.eventTtlMs),
        });
    }
    rememberInventory(eventId, nowMs) {
        if ((this.seenInventories.get(eventId) ?? 0) > nowMs)
            return false;
        if (!this.seenInventories.has(eventId)) {
            while (this.seenInventories.size >= this.options.maxSeenEvents) {
                const oldest = this.seenOrder.shift();
                if (oldest === undefined)
                    break;
                this.seenInventories.delete(oldest);
            }
            this.seenOrder.push(eventId);
        }
        this.seenInventories.set(eventId, saturatingAdd(nowMs, this.options.routeTtlMs));
        return true;
    }
    rememberDelivered(eventId) {
        if (this.deliveredEvents.has(eventId))
            return false;
        this.deliveredEvents.add(eventId);
        this.deliveredOrder.push(eventId);
        while (this.deliveredEvents.size > this.options.maxSeenEvents) {
            const oldest = this.deliveredOrder.shift();
            if (oldest === undefined)
                break;
            this.deliveredEvents.delete(oldest);
        }
        return true;
    }
    sendToSelectedPeers(peers, excludedPeer, message) {
        return selectMeshPeers(this.peersWithBehavior(peers), excludedPeer, this.options.fanout, this.options.unknownPeerReserve).map((peer) => send(peer.id, message));
    }
    peersWithBehavior(peers) {
        return peers.map((peer) => {
            const local = this.peerBehaviorScore(peer.id);
            if (peer.qualityScore !== undefined && local !== undefined) {
                return meshPeer(peer.id, clamp(peer.qualityScore + local, -100, 100));
            }
            return meshPeer(peer.id, peer.qualityScore ?? local);
        });
    }
    recordPeerBehavior(peerId, delta) {
        if (!this.peerBehaviors.has(peerId)) {
            while (this.peerBehaviors.size >= MAX_TRACKED_PEER_BEHAVIORS) {
                const oldest = this.peerBehaviorOrder.shift();
                if (oldest === undefined)
                    break;
                this.peerBehaviors.delete(oldest);
            }
            this.peerBehaviorOrder.push(peerId);
        }
        const behavior = this.peerBehaviors.get(peerId) ?? { score: 0, samples: 0 };
        behavior.samples = Math.min(0xffff_ffff, behavior.samples + 1);
        behavior.score = clamp(behavior.score + delta, -100, 100);
        this.peerBehaviors.set(peerId, behavior);
    }
    prune(nowMs) {
        for (const route of this.upstreamRoutes.values()) {
            if (route.expiresAtMs <= nowMs)
                this.recordPeerBehavior(route.peerId, UNSERVED_INVENTORY_PENALTY);
        }
        retainMap(this.cachedEvents, (cached) => cached.expiresAtMs > nowMs);
        retainOrder(this.cacheOrder, this.cachedEvents);
        retainMap(this.seenInventories, (expiry) => expiry > nowMs);
        retainOrder(this.seenOrder, this.seenInventories);
        retainMap(this.upstreamRoutes, (route) => route.expiresAtMs > nowMs);
        retainMap(this.pendingDownstream, (pending) => pending.expiresAtMs > nowMs);
        retainMap(this.wantForwarded, (expiry) => expiry > nowMs);
    }
}
function send(peerId, message) {
    return { type: 'send', peerId, message };
}
function requireEventId(eventId) {
    if (!/^[0-9a-fA-F]{64}$/.test(eventId))
        throw validation(`invalid inv/want event id ${eventId}`);
}
function requireKind(kind) {
    if (!Number.isSafeInteger(kind) || kind < 0 || kind > 65_535) {
        throw validation(`invalid inv/want event kind ${kind}`);
    }
    return kind;
}
function requireUnsignedByte(value, field) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 255) {
        throw validation(`invalid inv/want ${field} ${value}`);
    }
}
function requireNow(value) {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw validation(`invalid inv/want timestamp ${value}`);
    }
}
function boundedPositive(value, maximum = Number.MAX_SAFE_INTEGER) {
    if (!Number.isSafeInteger(value))
        throw validation(`invalid positive integer ${value}`);
    return clamp(Math.max(1, value), 1, maximum);
}
function nonNegative(value) {
    if (!Number.isSafeInteger(value))
        throw validation(`invalid non-negative integer ${value}`);
    return Math.max(0, value);
}
function saturatingAdd(left, right) {
    return Math.min(Number.MAX_SAFE_INTEGER, left + right);
}
function clamp(value, minimum, maximum) {
    return Math.max(minimum, Math.min(maximum, value));
}
function retainMap(map, predicate) {
    for (const [key, value] of map)
        if (!predicate(value))
            map.delete(key);
}
function retainOrder(order, map) {
    let write = 0;
    for (const id of order)
        if (map.has(id))
            order[write++] = id;
    order.length = write;
}
function validation(message) {
    return PubsubError.validation(message);
}
//# sourceMappingURL=mesh.js.map