import { FipsPubsubInvWantState } from './fips-pubsub-invwant.js';
import { FeedSync, FeedWindow, publicFeedId, warmFeedId } from './fips-feed-sync.js';
import { deliverSubscription, inventoryMessage } from './fips-pubsub-client-protocol.js';
import { privatePeerEvent, validPublicFeedFilter } from './public-peer-source.js';
import { subscriptionFiltersMatch } from './filter.js';
import { expired } from './runtime-store.js';
export class FipsFeedSubscriptions {
    local;
    limits;
    canExport;
    send;
    drain;
    wants;
    sync;
    sent = new WeakSet();
    published = new Map();
    remote = new Map();
    constructor(local, limits, canExport, send, drain) {
        this.local = local;
        this.limits = limits;
        this.canExport = canExport;
        this.send = send;
        this.drain = drain;
        this.wants = new FipsPubsubInvWantState(limits.maxActiveSubscriptions * limits.maxReplayEvents, limits.maxPeers);
        this.sync = new FeedSync(send, limits.maxFrameBytes, limits.maxActiveSubscriptions);
    }
    publication(event) {
        if (privatePeerEvent(event) || expired(event))
            return;
        event = this.published.get(event.id) ?? event;
        this.published.set(event.id, event);
        if (this.published.size > this.limits.maxCachedEvents)
            this.published.delete(this.published.keys().next().value);
        for (const sub of this.local.values())
            sub.feed?.remember(event);
        return event;
    }
    lookup(peer, wireId) {
        const sub = this.local.get(publicFeedId.test(wireId) ? wireId.slice(4) : wireId);
        return sub?.wireIds.get(peer) === wireId ? sub : undefined;
    }
    validIds(peer, ids, eventId) {
        return ids.filter(id => {
            const sub = this.lookup(peer, id);
            return sub && (sub.peers.has(peer) || sub.pendingPeers.has(peer)) && !sub.recentIds.has(eventId);
        });
    }
    deliver(peer, event, offeredIds, onError) {
        let delivered = false;
        for (const sub of this.local.values()) {
            const wireId = sub.wireIds.get(peer);
            if (sub.feed && (!wireId || !offeredIds.includes(wireId) || !sub.feed.accepts(event)))
                continue;
            const fresh = deliverSubscription(sub, event, peer, this.limits.maxCachedEvents, e => onError(e, sub.id));
            if (fresh)
                sub.feed?.remember(event);
            delivered ||= fresh;
        }
        return delivered;
    }
    control(peer, message) {
        const sub = this.lookup(peer, message.subscriptionId);
        return this.sync.receive(peer, message, sub?.feed, () => {
            if (!sub || sub.opened.has(peer))
                return false;
            sub.opened.add(peer);
            return true;
        });
    }
    accepts(peer, id, filters) {
        return !publicFeedId.test(id) || (this.remote.size < this.limits.maxPeers * this.limits.maxSubscriptionsPerPeer &&
            !this.remote.has(peer + ':' + id) && filters.length === 1 && validPublicFeedFilter(filters[0]));
    }
    offer(peer, id, filters) {
        const key = peer + ':' + id;
        const window = [...this.local.values()].find(sub => sub.feed && JSON.stringify(sub.filters) === JSON.stringify(filters))?.feed;
        const remote = { window, offered: new Map() };
        this.remote.set(key, remote);
        const records = new Map(this.published);
        for (const event of window?.snapshot() ?? [])
            records.set(event.id, event);
        const eligible = [...records.values()].filter(e => !expired(e) && this.canExport(e) && subscriptionFiltersMatch(filters, e))
            .slice(-Math.min(256, this.limits.maxCachedEvents));
        const finish = async (events, fallback = false) => {
            if (fallback)
                events = events.slice(-this.limits.maxReplayEvents);
            for (let i = 0; i < events.length; i++) {
                if (i && i % this.limits.maxReplayEvents === 0) {
                    try {
                        await this.drain();
                    }
                    catch {
                        return;
                    }
                }
                if (this.remote.get(key) !== remote)
                    return;
                const event = events[i];
                if (!this.canExport(event) || expired(event))
                    continue;
                this.send(peer, inventoryMessage(event, [id], this.limits.maxHops));
            }
        };
        if (warmFeedId.test(id) && eligible.length) {
            // A publisher need not subscribe to the recipient's own notification or following feed.
            const owner = window ?? new FeedWindow(filters[0], 256);
            remote.window = owner;
            return this.sync.offer(peer, id, owner, eligible, finish);
        }
        return finish(eligible.slice(-this.limits.maxReplayEvents));
    }
    /** A cache hit from an unrelated local query can never authorize feed INV, WANT or forwarding. */
    inventory(peer, message, fallback) {
        let event = fallback;
        const ids = message.subscriptionIds.filter(id => {
            if (!publicFeedId.test(id))
                return true;
            const feed = this.remote.get(peer + ':' + id);
            const candidate = this.published.get(message.eventId) ?? feed?.window?.get(message.eventId);
            if (!feed || !candidate || !this.canExport(candidate) || expired(candidate))
                return false;
            event = candidate;
            feed.offered.set(candidate.id, candidate);
            if (feed.offered.size > this.limits.maxCachedEvents)
                feed.offered.delete(feed.offered.keys().next().value);
            return true;
        });
        return ids.length ? { message: { ...message, subscriptionIds: ids }, event } : undefined;
    }
    answer(peer, eventId) {
        for (const [key, feed] of this.remote)
            if (key.startsWith(peer + ':')) {
                const event = feed.offered.get(eventId);
                if (event && this.canExport(event) && !expired(event))
                    return { subscriptionId: key.slice(peer.length + 1), event };
            }
    }
    retireRemote(peer, id) {
        this.remote.delete(peer + ':' + id);
        this.sync.cancel(peer, id, undefined, 'offer');
    }
    retireLocal(window) {
        this.sync.cancel(undefined, undefined, window);
        for (const remote of this.remote.values()) {
            if (remote.window === window || ![...this.local.values()].some(s => s.feed === remote.window)) {
                if (remote.window)
                    this.sync.cancel(undefined, undefined, remote.window);
                remote.window = undefined;
            }
            for (const id of remote.offered.keys())
                if (!remote.window?.get(id))
                    remote.offered.delete(id);
        }
        this.published.clear();
    }
    disconnect(peer) {
        this.sync.cancel(peer);
        for (const key of this.remote.keys())
            if (!peer || key.startsWith(peer + ':'))
                this.remote.delete(key);
        if (peer)
            this.wants.dropPeer(peer);
        else {
            this.published.clear();
            this.wants.clear();
        }
    }
}
//# sourceMappingURL=fips-feed-subscriptions.js.map