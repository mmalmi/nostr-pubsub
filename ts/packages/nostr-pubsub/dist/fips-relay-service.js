import { cloneFilter, subscriptionFiltersMatch } from './filter.js';
import { PubsubPeerSubscriptionStore } from './subscription.js';
import { PubsubError, verifyNostrEvent, } from './types.js';
import { DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES, FipsPubsubWireAdapter, FipsPubsubWireCodec, } from './wire.js';
export const FIPS_NOSTR_PUBSUB_SERVICE_PORT = 7368;
export const FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS = 8;
const MAX_PENDING_REPLIES_PER_SUBSCRIPTION = 8;
const MAX_RECENT_EVENT_IDS_PER_SUBSCRIPTION = 64;
export function defaultFipsNostrRelayServiceLimits() {
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
    adapter;
    limits;
    node;
    relay;
    onError;
    peers = new Map();
    pendingReplies = new Set();
    unregisterService;
    removeSessionListener;
    constructor(options) {
        this.node = options.node;
        this.relay = options.relay;
        this.onError = options.onError ?? (() => { });
        this.limits = validatedLimits(options.limits);
        this.adapter = new FipsPubsubWireAdapter(new FipsPubsubWireCodec(this.limits.maxFrameBytes), new PubsubPeerSubscriptionStore({
            maxPeers: this.limits.maxPeers,
            maxSubscriptionsPerPeer: this.limits.maxSubscriptionsPerPeer,
            maxFiltersPerSubscription: this.limits.maxFiltersPerSubscription,
        }));
    }
    start() {
        if (this.unregisterService !== undefined)
            return this;
        this.unregisterService = this.node.registerService(FIPS_NOSTR_PUBSUB_SERVICE_PORT, (context) => this.handle(context));
        this.removeSessionListener = this.node.on?.('session', (event) => {
            const session = parseClosedSession(event);
            if (session !== undefined)
                this.closePeer(session);
        });
        return this;
    }
    async stop() {
        this.unregisterService?.();
        this.unregisterService = undefined;
        this.removeSessionListener?.();
        this.removeSessionListener = undefined;
        for (const peerId of [...this.peers.keys()])
            this.closePeer(peerId);
        await Promise.allSettled([...this.pendingReplies]);
    }
    activePeerCount() {
        return this.peers.size;
    }
    activeSubscriptionCount() {
        let count = 0;
        for (const subscriptions of this.peers.values())
            count += subscriptions.size;
        return count;
    }
    peerSubscriptionCount(peerId) {
        return this.peers.get(peerId.toLowerCase())?.size ?? 0;
    }
    closePeer(peerId) {
        const normalizedPeerId = peerId.toLowerCase();
        const subscriptions = this.peers.get(normalizedPeerId);
        if (subscriptions === undefined)
            return;
        for (const subscriptionId of [...subscriptions.keys()]) {
            this.closeSubscription(normalizedPeerId, subscriptionId);
        }
    }
    async handle(context) {
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
    openSubscription(peerId, message, reply) {
        let subscriptions = this.peers.get(peerId);
        const existing = subscriptions?.get(message.subscriptionId);
        if (subscriptions === undefined && this.peers.size >= this.limits.maxPeers) {
            throw serviceError(`peer limit is ${this.limits.maxPeers}`);
        }
        if (message.filters.length > this.limits.maxFiltersPerSubscription) {
            throw serviceError(`subscription filter limit is ${this.limits.maxFiltersPerSubscription}`);
        }
        if (existing === undefined &&
            subscriptions !== undefined &&
            subscriptions.size >= this.limits.maxSubscriptionsPerPeer) {
            throw serviceError(`peer subscription limit is ${this.limits.maxSubscriptionsPerPeer}`);
        }
        const filters = boundedReplayFilters(message.filters, this.limits.maxReplayEventsPerFilter);
        if (existing !== undefined)
            this.closeSubscription(peerId, message.subscriptionId);
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
        const active = {
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
        }
        catch (error) {
            this.closeSubscription(peerId, message.subscriptionId, active.token, false);
            throw error;
        }
    }
    queueRelayEvent(peerId, subscriptionId, token, event) {
        const active = this.peers.get(peerId)?.get(subscriptionId);
        if (active === undefined || active.token !== token)
            return;
        let verified;
        try {
            verified = verifyNostrEvent(event);
        }
        catch (error) {
            this.onError(asError(error), { operation: 'relay-event', peerId, subscriptionId });
            return;
        }
        if (!subscriptionFiltersMatch(active.filters, verified))
            return;
        if (active.recentEventIds.has(verified.id))
            return;
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
    queueRelayEose(peerId, subscriptionId, token) {
        const active = this.peers.get(peerId)?.get(subscriptionId);
        if (active === undefined || active.token !== token || active.eoseSent)
            return;
        active.eoseSent = true;
        const frame = this.adapter.encodeOutbound({
            type: 'eose',
            subscriptionId,
            eventCount: active.recentEventIds.size,
        });
        this.queueReply(peerId, subscriptionId, active, frame, 'relay-eose');
    }
    queueReply(peerId, subscriptionId, active, frame, operation) {
        active.pendingReplyCount += 1;
        const task = active.replyChain.catch(() => { }).then(async () => {
            if (this.peers.get(peerId)?.get(subscriptionId) !== active)
                return;
            await active.reply(frame, FIPS_NOSTR_PUBSUB_SERVICE_PORT);
        });
        active.replyChain = task;
        this.pendingReplies.add(task);
        void task
            .catch((error) => {
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
    closeSubscription(peerId, subscriptionId, expectedToken, closeRelay = true) {
        const subscriptions = this.peers.get(peerId);
        const active = subscriptions?.get(subscriptionId);
        if (active === undefined || (expectedToken !== undefined && active.token !== expectedToken)) {
            return;
        }
        subscriptions?.delete(subscriptionId);
        if (active.timer !== undefined)
            clearTimeout(active.timer);
        if (closeRelay) {
            try {
                active.relaySubscription?.close('FIPS subscription closed');
            }
            catch (error) {
                this.onError(asError(error), { operation: 'subscription-close', peerId, subscriptionId });
            }
        }
        this.adapter.subscriptions.remove(peerId, subscriptionId);
        if (subscriptions?.size === 0)
            this.peers.delete(peerId);
    }
}
function rememberEventId(active, eventId) {
    active.recentEventIds.add(eventId);
    active.recentEventIdQueue.push(eventId);
    if (active.recentEventIdQueue.length <= MAX_RECENT_EVENT_IDS_PER_SUBSCRIPTION)
        return;
    const oldest = active.recentEventIdQueue.shift();
    if (oldest !== undefined)
        active.recentEventIds.delete(oldest);
}
function boundedReplayFilters(filters, maxReplay) {
    return filters.map((filter) => ({
        ...cloneFilter(filter),
        limit: Math.min(filter.limit ?? maxReplay, maxReplay),
    }));
}
function authenticatedPeerId(value) {
    const normalized = value.toLowerCase();
    if (!/^(02|03)[0-9a-f]{64}$/.test(normalized)) {
        throw serviceError('source is not an authenticated FIPS peer public key');
    }
    return normalized;
}
function parseClosedSession(event) {
    if (event === null || typeof event !== 'object')
        return undefined;
    const candidate = event;
    if (candidate.state !== 'closed' || typeof candidate.remotePubkey !== 'string') {
        return undefined;
    }
    return /^(02|03)[0-9a-f]{64}$/i.test(candidate.remotePubkey)
        ? candidate.remotePubkey.toLowerCase()
        : undefined;
}
function validatedLimits(overrides) {
    const limits = { ...defaultFipsNostrRelayServiceLimits(), ...overrides };
    for (const [name, value] of Object.entries(limits)) {
        if (!Number.isSafeInteger(value) || value <= 0) {
            throw serviceError(`${name} must be a positive safe integer`);
        }
    }
    if (limits.maxReplayEventsPerFilter > FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS) {
        throw serviceError(`maxReplayEventsPerFilter cannot exceed ${FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS}`);
    }
    if (limits.maxFrameBytes > DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES) {
        throw serviceError(`maxFrameBytes cannot exceed ${DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES}`);
    }
    return limits;
}
function serviceError(message) {
    return PubsubError.validation(`FIPS Nostr relay service: ${message}`);
}
function asError(error) {
    return error instanceof Error ? error : new Error(String(error));
}
//# sourceMappingURL=fips-relay-service.js.map