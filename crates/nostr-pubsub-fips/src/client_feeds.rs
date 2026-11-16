//! Public peer feeds on the client: TypeScript-compatible `wf1-`/`cf1-` feed
//! requests, Negentropy offers to subscribers and catch-up from providers.

use std::collections::{HashMap, HashSet, VecDeque};

use nostr::EventId;
use nostr_pubsub::{Filter, PubsubPeerInterest, Result, SourceId, SubscriptionId, VerifiedEvent};

use crate::client_inner::{ActiveSubscription, ClientInner};
use crate::feed::{
    FEED_WINDOW_LIMIT, FeedSync, FeedSyncFrame, FeedSyncOutput, FeedWindow, feed_subscription_key,
    feed_wire_id, is_public_feed_id, is_warm_feed_id, private_peer_event, valid_public_feed_filter,
};
use crate::privacy_roles::ClientRole;
use crate::provider_behavior::ProviderViolation;
use crate::{
    FipsPubsubClientOptions, FipsPubsubWireMessage, PeerIdentity, TransportCommand, invalid_option,
    now_ms, poisoned,
};

/// A local feed subscription: its window, the request id each peer got, and
/// optionally the only peers it is sent to.
pub(super) struct FeedSubscription {
    pub(super) window: FeedWindow,
    pub(super) wire_ids: HashMap<String, String>,
    opened: HashSet<String>,
    peers: Option<HashSet<String>>,
}

impl FeedSubscription {
    pub(super) fn new(filter: Filter, peers: Option<HashSet<String>>) -> Self {
        Self {
            window: FeedWindow::new(filter, FEED_WINDOW_LIMIT),
            wire_ids: HashMap::new(),
            opened: HashSet::new(),
            peers,
        }
    }

    pub(super) fn admits(&self, peer: &str) -> bool {
        self.peers.as_ref().is_none_or(|peers| peers.contains(peer))
    }

    /// The request id for `peer`, warm when the window holds events.
    pub(super) fn request_id(&mut self, key: &str, peer: &str) -> String {
        let id = feed_wire_id(key, !self.window.is_empty());
        self.wire_ids.insert(peer.to_string(), id.clone());
        id
    }

    pub(super) fn forget_peer(&mut self, peer: &str) {
        self.wire_ids.remove(peer);
        self.opened.remove(peer);
    }
}

/// Bounded events in insertion order.
#[derive(Default)]
struct EventCache {
    order: VecDeque<EventId>,
    events: HashMap<EventId, VerifiedEvent>,
}

impl EventCache {
    fn insert(&mut self, event: &VerifiedEvent) {
        let id = event.as_event().id;
        if self.events.contains_key(&id) {
            return;
        }
        self.order.push_back(id);
        self.events.insert(id, event.clone());
        while self.order.len() > FEED_WINDOW_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.events.remove(&oldest);
            }
        }
    }

    fn get(&self, id: &EventId) -> Option<&VerifiedEvent> {
        self.events.get(id)
    }

    fn ordered(&self) -> impl Iterator<Item = &VerifiedEvent> {
        self.order.iter().filter_map(|id| self.events.get(id))
    }
}

/// A peer's feed request this client provides.
struct RemoteFeed {
    /// The local feed subscription with the same filter, if any.
    window: Option<String>,
    offered: EventCache,
}

/// Provider-side feed state: local publications, peers' feed requests and
/// synchronization sessions. Lock after `subscriptions`, never before it.
pub(super) struct FeedState {
    published: EventCache,
    remote: HashMap<(String, String), RemoteFeed>,
    max_remote: usize,
    sync: FeedSync,
}

impl FeedState {
    pub(super) fn new(options: &FipsPubsubClientOptions) -> Self {
        Self {
            published: EventCache::default(),
            remote: HashMap::new(),
            max_remote: options
                .max_connected_peers
                .saturating_mul(options.max_active_subscriptions.saturating_add(1)),
            sync: FeedSync::new(options.max_frame_bytes, options.max_active_subscriptions),
        }
    }

    fn disconnect(&mut self, peer: &str) {
        self.sync.cancel(Some(peer), None, None, false);
        self.remote
            .retain(|(remote_peer, _), _| remote_peer != peer);
    }
}

fn exportable(role: &ClientRole, event: &VerifiedEvent) -> bool {
    role.can_export(event) && !event.as_event().is_expired()
}

impl ClientInner {
    fn serves_feeds(&self) -> bool {
        matches!(self.role, ClientRole::Full | ClientRole::Receiver(_))
    }

    /// Remember a local publication for feed offers and every feed window.
    pub(super) fn feed_publication(&self, event: &VerifiedEvent) {
        if private_peer_event(event) || event.as_event().is_expired() {
            return;
        }
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            for active in subscriptions.values_mut() {
                if let Some(feed) = active.feed.as_mut() {
                    feed.window.remember(event);
                }
            }
        }
        if let Ok(mut feeds) = self.feeds.lock() {
            feeds.published.insert(event);
        }
    }

    pub(super) fn new_feed_subscription(
        &self,
        filter: &Filter,
        peers: Option<&HashSet<String>>,
    ) -> Result<FeedSubscription> {
        if !self.serves_feeds() {
            return Err(invalid_option("public feeds need a full or receiver role"));
        }
        self.role.validate_filters(std::slice::from_ref(filter))?;
        if !valid_public_feed_filter(filter) {
            return Err(invalid_option(
                "a feed takes public kinds, optionally with authors or one notified key",
            ));
        }
        Ok(FeedSubscription::new(filter.clone(), peers.cloned()))
    }

    /// Change the peers a feed subscription is sent to: removed peers get a
    /// `CLOSE`, newly admitted connected peers a request.
    pub(super) async fn set_feed_peers(
        &self,
        key: &str,
        peers: Option<HashSet<String>>,
    ) -> Result<()> {
        let connected = self.connected_peers().await?;
        let mut frames = Vec::new();
        {
            let mut subscriptions = self.lock_subscriptions()?;
            let active = subscriptions
                .get_mut(key)
                .ok_or_else(|| invalid_option("feed subscription is closed"))?;
            let ActiveSubscription {
                filters,
                peers: requested,
                feed,
                ..
            } = active;
            let feed = feed
                .as_mut()
                .ok_or_else(|| invalid_option("not a feed subscription"))?;
            feed.peers = peers;
            let removed = requested
                .iter()
                .filter(|peer| !feed.admits(peer))
                .cloned()
                .collect::<Vec<_>>();
            for peer in removed {
                requested.remove(&peer);
                if let Some(id) = feed.wire_ids.get(&peer) {
                    let close = FipsPubsubWireMessage::close(SubscriptionId::new(id.clone()));
                    frames.push((peer.clone(), self.codec.encode_frame(&close)?));
                }
                feed.forget_peer(&peer);
                if let Ok(mut feeds) = self.feeds.lock() {
                    feeds.sync.cancel(Some(&peer), None, Some(key), false);
                }
            }
            for peer in connected {
                if requested.contains(&peer.npub) || !feed.admits(&peer.npub) {
                    continue;
                }
                let id = feed.request_id(key, &peer.npub);
                let request = FipsPubsubWireMessage::req(SubscriptionId::new(id), filters.clone());
                frames.push((peer.npub.clone(), self.codec.encode_frame(&request)?));
                requested.insert(peer.npub);
            }
        }
        for (peer, frame) in frames {
            if let Ok(peer) = PeerIdentity::from_npub(&peer) {
                let _ = self.send_frame(peer, frame);
            }
        }
        Ok(())
    }

    /// Replace each feed request id from `peer` with its local subscription,
    /// dropping ids this client never sent that peer.
    pub(super) fn local_feed_ids(
        &self,
        peer: &str,
        ids: Vec<SubscriptionId>,
    ) -> Vec<SubscriptionId> {
        if !ids.iter().any(|id| is_public_feed_id(id.as_str())) {
            return ids;
        }
        let Ok(subscriptions) = self.subscriptions.lock() else {
            return Vec::new();
        };
        ids.into_iter()
            .filter_map(|id| {
                let Some(key) = feed_subscription_key(id.as_str()) else {
                    return Some(id);
                };
                let feed = subscriptions.get(key)?.feed.as_ref()?;
                (feed.wire_ids.get(peer).map(String::as_str) == Some(id.as_str()))
                    .then(|| SubscriptionId::new(key))
            })
            .collect()
    }

    /// A peer's feed request: offer what this client holds for that feed.
    pub(super) fn handle_feed_req(
        &self,
        peer: PeerIdentity,
        subscription_id: &SubscriptionId,
        filters: &[Filter],
    ) {
        let [filter] = filters else {
            return;
        };
        let npub = peer.npub();
        let id = subscription_id.to_string();
        if !self.serves_feeds() || !valid_public_feed_filter(filter) {
            return;
        }
        let window = self.lock_subscriptions().ok().and_then(|subscriptions| {
            subscriptions.iter().find_map(|(key, active)| {
                let feed = active.feed.as_ref()?;
                (feed.window.filter() == filter).then(|| (key.clone(), feed.window.snapshot()))
            })
        });
        let Ok(mut feeds) = self.feeds.lock() else {
            return;
        };
        let key = (npub.clone(), id.clone());
        if feeds.remote.len() >= feeds.max_remote || feeds.remote.contains_key(&key) {
            return;
        }
        let evicted = self
            .peer_subscriptions
            .lock()
            .map_err(|_| poisoned("FIPS peer subscription state"))
            .and_then(|mut subscriptions| {
                subscriptions.upsert_filters(
                    SourceId::new(npub.clone()),
                    id.clone(),
                    filters.to_vec(),
                )
            });
        match evicted {
            Ok(Some(evicted)) => {
                feeds
                    .remote
                    .remove(&(npub.clone(), evicted.subscription_id.clone()));
                feeds
                    .sync
                    .cancel(Some(&npub), Some(&evicted.subscription_id), None, true);
            }
            Ok(None) => {}
            Err(_) => return,
        }
        let mut records = feeds.published.ordered().cloned().collect::<Vec<_>>();
        let (owner, snapshot) =
            window.map_or((None, Vec::new()), |(key, events)| (Some(key), events));
        let mut seen = records
            .iter()
            .map(|event| event.as_event().id)
            .collect::<HashSet<_>>();
        records.extend(
            snapshot
                .into_iter()
                .filter(|event| seen.insert(event.as_event().id)),
        );
        let mut eligible = records
            .into_iter()
            .filter(|event| {
                exportable(&self.role, event)
                    && PubsubPeerInterest::from_filters(filters, event)
                        == PubsubPeerInterest::Subscribed
            })
            .collect::<Vec<_>>();
        let start = eligible.len().saturating_sub(FEED_WINDOW_LIMIT);
        eligible.drain(..start);
        feeds.remote.insert(
            key,
            RemoteFeed {
                window: owner.clone(),
                offered: EventCache::default(),
            },
        );
        let output = if is_warm_feed_id(&id) && !eligible.is_empty() {
            feeds
                .sync
                .offer(&npub, &id, owner.as_deref(), eligible, now_ms())
        } else {
            let start = eligible
                .len()
                .saturating_sub(self.options.max_replay_events);
            FeedSyncOutput {
                peer: npub,
                frames: Vec::new(),
                finish: Some(crate::feed::FeedSyncFinish {
                    subscription_id: id,
                    events: eligible.split_off(start),
                    fallback: false,
                }),
            }
        };
        drop(feeds);
        self.send_feed_output(output);
    }

    pub(super) fn handle_feed_sync(&self, source: PeerIdentity, frame: &[u8]) {
        let Ok(frame) = FeedSyncFrame::decode(frame) else {
            self.record_provider_violation(source, ProviderViolation::MalformedFrame);
            return;
        };
        if !self.serves_feeds() {
            return;
        }
        let peer = source.npub();
        let Ok(mut subscriptions) = self.subscriptions.lock() else {
            return;
        };
        let local = feed_subscription_key(&frame.subscription_id).and_then(|key| {
            let feed = subscriptions.get_mut(key)?.feed.as_mut()?;
            (feed.wire_ids.get(&peer) == Some(&frame.subscription_id))
                .then(|| (key.to_string(), feed))
        });
        let Ok(mut feeds) = self.feeds.lock() else {
            return;
        };
        let output = match local {
            Some((key, feed)) => {
                let snapshot = feed.window.snapshot();
                let opened = &mut feed.opened;
                feeds.sync.receive(
                    &peer,
                    &frame,
                    Some((Some(&key), snapshot)),
                    || opened.insert(peer.clone()),
                    now_ms(),
                )
            }
            None => feeds.sync.receive(&peer, &frame, None, || false, now_ms()),
        };
        drop(feeds);
        drop(subscriptions);
        self.send_feed_output(output);
    }

    /// Frames and fallback inventories of sessions past their deadline.
    pub(super) fn expire_feed_syncs(&self, now_ms: u64) {
        let outputs = self
            .feeds
            .lock()
            .map(|mut feeds| feeds.sync.expire(now_ms))
            .unwrap_or_default();
        for output in outputs {
            self.send_feed_output(output);
        }
    }

    fn send_feed_output(&self, output: FeedSyncOutput) {
        let Ok(peer) = PeerIdentity::from_npub(&output.peer) else {
            return;
        };
        for frame in &output.frames {
            if let Ok(frame) = frame.encode() {
                let _ = self
                    .transport_tx
                    .try_send(TransportCommand::Send { peer, frame });
            }
        }
        let Some(finish) = output.finish else {
            return;
        };
        let mut events = finish.events;
        if finish.fallback {
            let start = events.len().saturating_sub(self.options.max_replay_events);
            events.drain(..start);
        }
        let id = SubscriptionId::new(finish.subscription_id);
        for event in events {
            if !exportable(&self.role, &event) {
                continue;
            }
            let ids = self.feed_inventory_ids(&output.peer, vec![id.clone()], &event);
            if ids.is_empty() {
                continue;
            }
            if let Ok(frame) = self.inventory_frame(ids, &event, self.options.max_hops) {
                let _ = self.send_frame(peer, frame);
            }
        }
    }

    /// Keep only feed request ids this client may announce `event` on, and
    /// remember the offer so the peer's `WANT` is answered.
    pub(super) fn feed_inventory_ids(
        &self,
        peer: &str,
        ids: Vec<SubscriptionId>,
        event: &VerifiedEvent,
    ) -> Vec<SubscriptionId> {
        if !ids.iter().any(|id| is_public_feed_id(id.as_str())) {
            return ids;
        }
        let event_id = event.as_event().id;
        let Ok(subscriptions) = self.subscriptions.lock() else {
            return Vec::new();
        };
        let Ok(mut feeds) = self.feeds.lock() else {
            return Vec::new();
        };
        let FeedState {
            published, remote, ..
        } = &mut *feeds;
        ids.into_iter()
            .filter(|id| {
                if !is_public_feed_id(id.as_str()) {
                    return true;
                }
                let Some(feed) = remote.get_mut(&(peer.to_string(), id.to_string())) else {
                    return false;
                };
                let window = feed
                    .window
                    .as_ref()
                    .and_then(|key| subscriptions.get(key)?.feed.as_ref());
                let candidate = published
                    .get(&event_id)
                    .or_else(|| window.and_then(|feed| feed.window.get(&event_id)));
                match candidate {
                    Some(candidate) if exportable(&self.role, candidate) => {
                        feed.offered.insert(candidate);
                        true
                    }
                    _ => false,
                }
            })
            .collect()
    }

    /// A feed offer the peer now wants.
    pub(super) fn feed_answer(
        &self,
        peer: &str,
        event_id: &EventId,
    ) -> Option<(SubscriptionId, VerifiedEvent)> {
        let feeds = self.feeds.lock().ok()?;
        feeds.remote.iter().find_map(|((remote_peer, id), feed)| {
            let event = feed.offered.get(event_id)?;
            (remote_peer == peer && exportable(&self.role, event))
                .then(|| (SubscriptionId::new(id.clone()), event.clone()))
        })
    }

    pub(super) fn forget_feed_peer(&self, peer: &str) {
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            for active in subscriptions.values_mut() {
                if let Some(feed) = active.feed.as_mut() {
                    feed.opened.remove(peer);
                }
            }
        }
        if let Ok(mut feeds) = self.feeds.lock() {
            feeds.disconnect(peer);
        }
    }

    /// A closed local feed no longer answers sessions or lends its window.
    pub(super) fn retire_local_feed(&self, key: &str) {
        if let Ok(mut feeds) = self.feeds.lock() {
            feeds.sync.cancel(None, None, Some(key), false);
            for remote in feeds.remote.values_mut() {
                if remote.window.as_deref() == Some(key) {
                    remote.window = None;
                }
            }
        }
    }

    /// The `CLOSE` id each peer knows this subscription by.
    pub(super) fn close_ids(active: &ActiveSubscription, key: &str, peer: &str) -> String {
        active
            .feed
            .as_ref()
            .and_then(|feed| feed.wire_ids.get(peer).cloned())
            .unwrap_or_else(|| key.to_string())
    }
}

impl ClientInner {
    pub(super) async fn subscribe_feed(
        self: &std::sync::Arc<Self>,
        filter: Filter,
        peers: Option<HashSet<String>>,
    ) -> Result<crate::FipsPubsubSubscription> {
        drop(self.admit()?);
        let mut feed = self.new_feed_subscription(&filter, peers.as_ref())?;
        let connected = self.connected_peers().await?;
        let _admission = self.admit()?;
        let sequence = self
            .next_subscription_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let key = format!("ts-{}", base36(sequence));
        let filters = vec![filter];
        let mut frames = Vec::new();
        let mut requested = HashSet::new();
        for peer in connected {
            if !feed.admits(&peer.npub) {
                continue;
            }
            let id = feed.request_id(&key, &peer.npub);
            let request = FipsPubsubWireMessage::req(SubscriptionId::new(id), filters.clone());
            frames.push((peer.identity, self.codec.encode_frame(&request)?));
            requested.insert(peer.npub);
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(FEED_WINDOW_LIMIT);
        let delivery = std::sync::Arc::new(crate::local_delivery::LocalDelivery::new(
            sender,
            FEED_WINDOW_LIMIT,
        ));
        {
            let mut subscriptions = self.lock_subscriptions()?;
            if subscriptions.len() > self.options.max_active_subscriptions {
                return Err(nostr_pubsub::PubsubError::Storage(format!(
                    "application FIPS pubsub subscription limit is {}",
                    self.options.max_active_subscriptions
                )));
            }
            subscriptions.insert(
                key.clone(),
                ActiveSubscription {
                    filters,
                    fresh: true,
                    peers: requested,
                    delivery: std::sync::Arc::clone(&delivery),
                    feed: Some(feed),
                },
            );
        }
        for (peer, frame) in frames {
            let _ = self.send_frame(peer, frame);
        }
        Ok(crate::FipsPubsubSubscription {
            subscription_id: SubscriptionId::new(key.clone()),
            key,
            receiver,
            delivery,
            inner: std::sync::Arc::clone(self),
            closed: false,
        })
    }
}

fn base36(mut value: u64) -> String {
    let mut digits = Vec::new();
    loop {
        digits.push(char::from_digit(u32::try_from(value % 36).unwrap_or(0), 36).unwrap_or('0'));
        value /= 36;
        if value == 0 {
            break;
        }
    }
    digits.iter().rev().collect()
}
