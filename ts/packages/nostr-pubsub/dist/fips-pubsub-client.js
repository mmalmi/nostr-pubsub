import { cloneFilter } from './filter.js';
import { verifyNostrEvent, } from './types.js';
import { FipsPubsubWireCodec, } from './wire.js';
import { FIPS_NOSTR_PUBSUB_SERVICE_PORT } from './wire.js';
import { FipsPubsubTcpTransport } from './fips-pubsub-tcp-transport.js';
import { FipsPubsubInvWantState } from './fips-pubsub-invwant.js';
import { FipsPubsubEventCache, acceptEvent, acceptInventory, answerWant, deliverSubscription, inventoryMessage, replayLocalSubscription, retryPendingWants, } from './fips-pubsub-client-protocol.js';
import { clientError, createClientPeerSubscriptionStore, normalizeAllowedKinds, normalizePeerId, parseConnectionEvent, validateClientLimits, } from './fips-pubsub-client-support.js';
export const FIPS_NOSTR_PUBSUB_CAPABILITY = 'nostr.pubsub/1';
export class FipsNostrPubsubClient {
    limits;
    codec;
    node;
    localPeerId;
    peers;
    allowedKinds;
    onError;
    peerSubscriptions;
    subscriptions = new Map();
    events;
    invWant;
    pending = new Set();
    transport;
    removePeerListener;
    removeSessionListener;
    nextSubscriptionId = 1;
    constructor(options) {
        this.node = options.node;
        this.localPeerId = normalizePeerId(options.localPeerId) ?? '';
        if (this.localPeerId === '')
            throw clientError('localPeerId must be a compressed FIPS key');
        this.peers = options.peers;
        this.onError = options.onError ?? (() => { });
        this.limits = validateClientLimits(options.limits);
        this.invWant = new FipsPubsubInvWantState(this.limits.maxActiveSubscriptions * this.limits.maxReplayEvents, this.limits.maxPeers);
        this.events = new FipsPubsubEventCache(this.limits.maxCachedEvents);
        this.codec = new FipsPubsubWireCodec(this.limits.maxFrameBytes);
        this.allowedKinds = normalizeAllowedKinds(options.allowedKinds);
        this.peerSubscriptions = createClientPeerSubscriptionStore(this.limits);
    }
    start() {
        if (this.transport !== undefined)
            return this;
        const maxQueuedRecordsPerPeer = this.limits.maxActiveSubscriptions
            + this.limits.maxReplayEvents + 1;
        this.transport = new FipsPubsubTcpTransport(this.node, this.localPeerId, {
            servicePort: FIPS_NOSTR_PUBSUB_SERVICE_PORT,
            maxPeers: this.limits.maxPeers,
            maxFrameBytes: this.limits.maxFrameBytes,
            maxQueuedRecordsPerPeer,
            maxQueuedBytesPerPeer: (this.limits.maxFrameBytes + 4) * maxQueuedRecordsPerPeer,
            maxIoBytesPerDrive: 512 * 1024,
            maxFramesPerDrive: this.limits.receiveBatchSize,
        }, {
            frame: (peerId, frame) => this.handleFrame(peerId, frame),
            connected: (peerId) => this.handleTransportConnected(peerId),
            disconnected: (peerId) => this.handleTransportDisconnected(peerId),
            tick: (nowMs) => this.retryPendingWants(nowMs),
            error: (error) => this.report(error, { operation: 'receive' }),
        });
        this.removePeerListener = this.node.on?.('peer', (event) => this.handlePeerEvent(event));
        this.removeSessionListener = this.node.on?.('session', (event) => this.handleSessionEvent(event));
        this.refreshPeers();
        return this;
    }
    async stop() {
        for (const subscriptionId of [...this.subscriptions.keys()]) {
            this.closeSubscription(subscriptionId);
        }
        await this.idle();
        this.removePeerListener?.();
        this.removePeerListener = undefined;
        this.removeSessionListener?.();
        this.removeSessionListener = undefined;
        const transport = this.transport;
        this.transport = undefined;
        await transport?.dispose();
        this.peerSubscriptions = createClientPeerSubscriptionStore(this.limits);
        this.events.clear();
        this.invWant.clear();
    }
    subscribe(filters, handler) {
        this.requireStarted();
        if (this.subscriptions.size >= this.limits.maxActiveSubscriptions) {
            throw clientError(`active subscription limit is ${this.limits.maxActiveSubscriptions}`);
        }
        if (filters.length === 0 || filters.length > this.limits.maxFiltersPerSubscription) {
            throw clientError(`subscription requires 1..${this.limits.maxFiltersPerSubscription} filters`);
        }
        const id = `ts-${this.nextSubscriptionId.toString(36)}`;
        this.nextSubscriptionId += 1;
        const normalized = this.normalizedFilters(id, filters);
        const subscription = {
            id,
            filters: normalized,
            handler,
            peers: new Set(),
            pendingPeers: new Set(),
            recentIds: new Set(),
            recentOrder: [],
        };
        this.subscriptions.set(id, subscription);
        this.refreshSubscriptionPeers(subscription);
        replayLocalSubscription(this.events, subscription, this.limits.maxReplayEvents, this.limits.maxCachedEvents, (error) => this.report(error, { operation: 'event-handler', subscriptionId: id }));
        let closed = false;
        return {
            id,
            close: () => {
                if (closed)
                    return;
                closed = true;
                this.closeSubscription(id);
            },
        };
    }
    async publish(event) {
        this.requireStarted();
        const verified = this.admitEvent(event);
        const peers = this.currentPeers();
        if (peers.length === 0)
            throw clientError('no admitted FIPS pubsub peers are available');
        if (!this.events.remember(verified, undefined, this.limits.maxHops))
            return;
        const deliveries = peers.flatMap((peerId) => {
            const subscriptionIds = this.peerSubscriptions
                .matchingSubscriptions(peerId, verified)
                .map((subscription) => subscription.subscriptionId);
            return subscriptionIds.length === 0
                ? []
                : [{ peerId, message: inventoryMessage(verified, subscriptionIds, this.limits.maxHops) }];
        });
        if (deliveries.length === 0)
            return;
        const results = await Promise.all(deliveries.map(async ({ peerId, message }) => {
            try {
                await this.track(this.send(peerId, message));
                return true;
            }
            catch (error) {
                this.report(error, { operation: 'send', peerId });
                return false;
            }
        }));
        if (!results.some(Boolean)) {
            throw clientError('all FIPS pubsub inventory deliveries failed');
        }
    }
    refreshPeers() {
        for (const subscription of this.subscriptions.values()) {
            this.refreshSubscriptionPeers(subscription);
        }
    }
    peerSubscriptionCount(peerId) {
        return this.peerSubscriptions.peerSubscriptionCount(peerId.toLowerCase());
    }
    activeSubscriptionCount() {
        return this.subscriptions.size;
    }
    async idle() {
        for (let turn = 0; turn < 4; turn += 1) {
            while (this.pending.size > 0)
                await Promise.allSettled([...this.pending]);
            await this.transport?.idle();
        }
    }
    handleFrame(sourcePeer, frame) {
        const peerId = normalizePeerId(sourcePeer);
        if (peerId === undefined || !this.currentPeers().includes(peerId)) {
            this.report(clientError('dropped a frame outside admitted nostr.pubsub/1 peers'), {
                operation: 'receive',
                peerId,
            });
            return;
        }
        let message;
        try {
            message = this.codec.decodeFrame(frame);
            if (message.type === 'event')
                this.admitEvent(message.event);
        }
        catch (error) {
            this.report(error, { operation: 'receive', peerId });
            return;
        }
        if (message.type === 'req') {
            this.peerSubscriptions.upsertFilters(peerId, message.subscriptionId, message.filters);
            for (const cached of this.events.replay(message.filters, this.limits.maxReplayEvents)) {
                this.background(this.send(peerId, inventoryMessage(cached.event, [message.subscriptionId], cached.hopLimit)), { operation: 'send', peerId, subscriptionId: message.subscriptionId });
            }
            return;
        }
        if (message.type === 'close') {
            this.peerSubscriptions.remove(peerId, message.subscriptionId);
            return;
        }
        if (message.type === 'inv') {
            acceptInventory(this.protocolContext(), peerId, message);
            return;
        }
        if (message.type === 'want') {
            answerWant(this.protocolContext(), peerId, message.eventId);
            return;
        }
        acceptEvent(this.protocolContext(), peerId, message);
    }
    forwardEvent(event, sourcePeer, hopLimit) {
        if (hopLimit === 0)
            return;
        for (const peerId of this.currentPeers()) {
            if (peerId === sourcePeer)
                continue;
            const subscriptionIds = this.peerSubscriptions
                .matchingSubscriptions(peerId, event)
                .map((subscription) => subscription.subscriptionId);
            if (subscriptionIds.length === 0)
                continue;
            this.background(this.send(peerId, inventoryMessage(event, subscriptionIds, hopLimit)), { operation: 'send', peerId });
        }
    }
    retryPendingWants(nowMs) {
        retryPendingWants(this.protocolContext(), nowMs);
    }
    protocolContext() {
        return {
            maxActiveSubscriptions: this.limits.maxActiveSubscriptions,
            maxHops: this.limits.maxHops,
            invWant: this.invWant,
            events: this.events,
            validSubscriptionIds: (peerId, subscriptionIds, eventId) => subscriptionIds.filter((subscriptionId) => {
                const subscription = this.subscriptions.get(subscriptionId);
                return subscription !== undefined &&
                    (subscription.peers.has(peerId) || subscription.pendingPeers.has(peerId)) &&
                    !subscription.recentIds.has(eventId);
            }),
            eventForWant: (peerId, eventId) => {
                const cached = this.events.get(eventId);
                if (cached === undefined)
                    return undefined;
                const subscription = this.peerSubscriptions
                    .matchingSubscriptions(peerId, cached.event)[0];
                return subscription === undefined
                    ? undefined
                    : { subscriptionId: subscription.subscriptionId, event: cached.event };
            },
            deliverEvent: (peerId, event) => {
                let delivered = false;
                for (const subscription of this.subscriptions.values()) {
                    delivered = deliverSubscription(subscription, event, peerId, this.limits.maxCachedEvents, (error) => this.report(error, {
                        operation: 'event-handler',
                        peerId,
                        subscriptionId: subscription.id,
                    })) || delivered;
                }
                return delivered;
            },
            forwardEvent: (peerId, event, hopLimit) => this.forwardEvent(event, peerId, hopLimit),
            send: (peerId, message) => this.background(this.send(peerId, message), {
                operation: 'send',
                peerId,
            }),
        };
    }
    normalizedFilters(subscriptionId, filters) {
        const decoded = this.codec.decodeFrame(this.codec.encodeFrame({
            type: 'req',
            subscriptionId,
            filters: filters.map(cloneFilter),
        }));
        if (decoded.type !== 'req')
            throw clientError('failed to normalize subscription filters');
        return decoded.filters;
    }
    refreshSubscriptionPeers(subscription) {
        const current = new Set(this.currentPeers());
        for (const peerId of [...subscription.peers]) {
            if (!current.has(peerId))
                subscription.peers.delete(peerId);
        }
        for (const peerId of [...subscription.pendingPeers]) {
            if (!current.has(peerId))
                subscription.pendingPeers.delete(peerId);
        }
        for (const peerId of current) {
            if (subscription.peers.has(peerId) || subscription.pendingPeers.has(peerId))
                continue;
            subscription.pendingPeers.add(peerId);
            if (this.transport?.isConnected(peerId))
                this.queueSubscriptionRequest(subscription, peerId);
            else
                this.connectPeer(peerId);
        }
    }
    connectPeer(peerId) {
        const transport = this.transport;
        if (transport === undefined)
            return;
        this.background(transport.connectPeer(peerId).catch((error) => {
            for (const subscription of this.subscriptions.values()) {
                subscription.pendingPeers.delete(peerId);
            }
            throw error;
        }), { operation: 'send', peerId });
    }
    queueSubscriptionRequest(subscription, peerId) {
        this.background(this.sendSubscriptionRequest(subscription, peerId), { operation: 'send', peerId, subscriptionId: subscription.id });
    }
    async sendSubscriptionRequest(subscription, peerId) {
        try {
            await this.send(peerId, {
                type: 'req',
                subscriptionId: subscription.id,
                filters: subscription.filters,
            });
            const remainsActive = this.subscriptions.get(subscription.id) === subscription;
            const remainsAdmitted = this.currentPeers().includes(peerId);
            const remainsPending = subscription.pendingPeers.has(peerId);
            if (remainsActive && remainsAdmitted && remainsPending) {
                subscription.peers.add(peerId);
                return;
            }
            await this.send(peerId, { type: 'close', subscriptionId: subscription.id });
        }
        finally {
            subscription.pendingPeers.delete(peerId);
        }
    }
    closeSubscription(subscriptionId) {
        const subscription = this.subscriptions.get(subscriptionId);
        if (subscription === undefined)
            return;
        this.subscriptions.delete(subscriptionId);
        this.invWant.removeSubscription(subscriptionId);
        for (const peerId of subscription.peers) {
            this.background(this.send(peerId, { type: 'close', subscriptionId }), {
                operation: 'send',
                peerId,
                subscriptionId,
            });
        }
    }
    admitEvent(event) {
        const verified = verifyNostrEvent(event);
        if (this.allowedKinds !== undefined && !this.allowedKinds.has(verified.kind)) {
            throw clientError(`event kind ${verified.kind} is not admitted`);
        }
        return verified;
    }
    currentPeers() {
        const peers = new Set();
        for (const value of this.peers()) {
            const peerId = normalizePeerId(value);
            if (peerId !== undefined)
                peers.add(peerId);
            if (peers.size >= this.limits.maxPeers)
                break;
        }
        return [...peers].sort();
    }
    send(peerId, message) {
        const transport = this.transport;
        if (transport === undefined)
            return Promise.reject(clientError('client is not started'));
        try {
            transport.queueFrame(peerId, this.codec.encodeFrame(message));
            return Promise.resolve();
        }
        catch (error) {
            return Promise.reject(error);
        }
    }
    background(task, context) {
        void this.track(task).catch((error) => this.report(error, context));
    }
    track(task) {
        this.pending.add(task);
        void task.finally(() => this.pending.delete(task)).catch(() => { });
        return task;
    }
    handlePeerEvent(event) {
        const parsed = parseConnectionEvent(event);
        if (parsed === undefined)
            return;
        if (parsed.connected)
            this.refreshPeers();
        else
            this.dropPeer(parsed.peerId);
    }
    handleSessionEvent(event) {
        const parsed = parseConnectionEvent(event);
        if (parsed === undefined)
            return;
        if (parsed.connected)
            this.refreshPeers();
        else
            this.dropPeer(parsed.peerId);
    }
    dropPeer(peerId) {
        void this.transport?.abortPeer(peerId).catch((error) => {
            this.report(error, { operation: 'receive', peerId });
        });
        this.handleTransportDisconnected(peerId, false);
    }
    handleTransportConnected(peerId) {
        if (!this.currentPeers().includes(peerId)) {
            void this.transport?.abortPeer(peerId);
            return;
        }
        for (const subscription of this.subscriptions.values()) {
            if (!subscription.peers.has(peerId)) {
                subscription.pendingPeers.add(peerId);
                this.queueSubscriptionRequest(subscription, peerId);
            }
        }
    }
    handleTransportDisconnected(peerId, reconnect = true) {
        this.peerSubscriptions.removePeer(peerId);
        for (const subscription of this.subscriptions.values()) {
            subscription.peers.delete(peerId);
            if (reconnect && this.currentPeers().includes(peerId))
                subscription.pendingPeers.add(peerId);
            else
                subscription.pendingPeers.delete(peerId);
        }
        this.invWant.dropPeer(peerId);
        if (reconnect && this.currentPeers().includes(peerId))
            this.connectPeer(peerId);
    }
    requireStarted() {
        if (this.transport === undefined)
            throw clientError('client is not started');
    }
    report(error, context) {
        this.onError(error instanceof Error ? error : new Error(String(error)), context);
    }
}
//# sourceMappingURL=fips-pubsub-client.js.map