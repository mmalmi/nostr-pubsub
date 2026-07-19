use super::{
    Arc, AtomicU64, AtomicUsize, EventId, EventSource, Filter, FipsEndpoint,
    FipsPubsubClientOptions, FipsPubsubSubscription, FipsPubsubWireCodec, FipsPubsubWireMessage,
    HashMap, HashSet, Mutex, Ordering, PeerIdentity, PubsubError, PubsubPeerInterest,
    PubsubPeerSubscriptionStore, QueryEvent, Result, SOURCE_PRIORITY_FIPS_ENDPOINT, SourceId,
    SubscriptionId, TransportCommand, VecDeque, VerifiedEvent, event_payload_bytes, mpsc,
    no_connected_peers, now_ms, poisoned, storage_error,
};
use crate::FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS;
use crate::provider_behavior::{ProviderBehavior, ProviderViolation};
use crate::seen_ids::ScopedSeenIds;

pub(super) struct ClientInner {
    pub(super) endpoint: Arc<FipsEndpoint>,
    pub(super) codec: FipsPubsubWireCodec,
    pub(super) options: FipsPubsubClientOptions,
    pub(super) peer_transport: Option<&'static str>,
    pub(super) excluded_peer_transports: HashSet<String>,
    pub(super) transport_tx: mpsc::Sender<TransportCommand>,
    pub(super) connected_transport_peers: AtomicUsize,
    pub(super) req_frames_received: AtomicU64,
    pub(super) close_frames_received: AtomicU64,
    pub(super) event_frames_received: AtomicU64,
    pub(super) inv_frames_received: AtomicU64,
    pub(super) want_frames_received: AtomicU64,
    pub(super) want_frames_sent: AtomicU64,
    pub(super) subscription_events_received: AtomicU64,
    pub(super) expired_wants: AtomicU64,
    pub(super) provider_cooldowns: AtomicU64,
    pub(super) tcp_receive_batches: AtomicU64,
    pub(super) tcp_datagrams_received: AtomicU64,
    pub(super) tcp_datagrams_rejected: AtomicU64,
    pub(super) tcp_poll_turns: AtomicU64,
    pub(super) next_subscription_id: AtomicU64,
    pub(super) subscriptions: Mutex<HashMap<String, ActiveSubscription>>,
    pub(super) peer_subscriptions: Mutex<PubsubPeerSubscriptionStore>,
    pub(super) recent_events: Mutex<RecentEvents>,
    pub(super) observed_inventories: Mutex<ScopedSeenIds>,
    pub(super) observed_full_events: Mutex<ScopedSeenIds>,
    pub(super) provider_behavior: Mutex<ProviderBehavior>,
    pub(super) pending_wants: Mutex<PendingWants>,
}

impl ClientInner {
    pub(super) async fn connected_peers(&self) -> Result<Vec<ConnectedPeer>> {
        let snapshot = self
            .endpoint
            .peers()
            .await
            .map_err(|error| storage_error("snapshot FIPS peers", error))?;
        let mut peers = snapshot
            .into_iter()
            .filter(|peer| {
                peer.connected
                    && self
                        .peer_transport
                        .is_none_or(|transport| peer.transport_type.as_deref() == Some(transport))
                    && peer
                        .transport_type
                        .as_deref()
                        .is_none_or(|transport| !self.excluded_peer_transports.contains(transport))
            })
            .map(|peer| {
                let npub = peer.npub;
                let identity = PeerIdentity::from_npub(&npub).map_err(|error| {
                    PubsubError::Validation(format!(
                        "invalid authenticated FIPS peer {npub}: {error}"
                    ))
                })?;
                Ok(ConnectedPeer {
                    npub,
                    identity,
                    link_id: peer.link_id,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        peers.sort_unstable_by(|left, right| left.npub.cmp(&right.npub));
        peers.dedup_by(|left, right| left.npub == right.npub);
        if peers.len() > self.options.max_connected_peers {
            return Err(PubsubError::Storage(format!(
                "connected local {} peer count {} exceeds limit {}",
                self.peer_transport.unwrap_or("FIPS"),
                peers.len(),
                self.options.max_connected_peers
            )));
        }
        Ok(peers)
    }

    pub(super) async fn subscribe(
        self: &Arc<Self>,
        mut filters: Vec<Filter>,
    ) -> Result<FipsPubsubSubscription> {
        if filters.is_empty() {
            filters.push(Filter::new());
        }
        if filters.len() > self.options.max_filters_per_subscription {
            return Err(PubsubError::Validation(format!(
                "FIPS pubsub filter count {} exceeds limit {}",
                filters.len(),
                self.options.max_filters_per_subscription
            )));
        }
        for filter in &mut filters {
            filter.limit = Some(
                filter
                    .limit
                    .unwrap_or(self.options.max_replay_events)
                    .min(self.options.max_replay_events),
            );
        }

        let peers = self.connected_peers().await?;
        if peers.is_empty() {
            return Err(no_connected_peers(self.peer_transport));
        }
        let sequence = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let subscription_id = SubscriptionId::new(format!("fips-{sequence}"));
        let key = subscription_id.to_string();
        let frame = self.codec.encode_frame(&FipsPubsubWireMessage::req(
            subscription_id.clone(),
            filters.clone(),
        ))?;
        let (sender, receiver) = mpsc::channel(self.options.max_replay_events);
        let subscribed_peers = peers
            .iter()
            .map(|peer| peer.npub.clone())
            .collect::<HashSet<_>>();
        {
            let mut subscriptions = self.lock_subscriptions()?;
            if subscriptions.len() >= self.options.max_active_subscriptions {
                return Err(PubsubError::Storage(format!(
                    "active FIPS pubsub subscription limit is {}",
                    self.options.max_active_subscriptions
                )));
            }
            subscriptions.insert(
                key.clone(),
                ActiveSubscription {
                    filters,
                    peers: subscribed_peers,
                    recent_event_ids: HashSet::new(),
                    recent_event_order: VecDeque::new(),
                    sender,
                },
            );
        }

        self.replay_local_events(&key)?;

        let mut sent = 0usize;
        let mut last_error = None;
        for peer in peers {
            match self.send_frame(peer.identity, frame.clone()) {
                Ok(()) => sent += 1,
                Err(error) => {
                    last_error = Some(error);
                    if let Some(active) = self.lock_subscriptions()?.get_mut(&key) {
                        active.peers.remove(&peer.npub);
                    }
                }
            }
        }
        if sent == 0 {
            self.close_subscription(&key);
            return Err(last_error.unwrap_or_else(|| no_connected_peers(self.peer_transport)));
        }

        Ok(FipsPubsubSubscription {
            subscription_id,
            key,
            receiver,
            inner: Arc::clone(self),
            closed: false,
        })
    }

    pub(super) fn send_frame(&self, peer: PeerIdentity, frame: Vec<u8>) -> Result<()> {
        self.transport_tx
            .try_send(TransportCommand::Send { peer, frame })
            .map_err(|error| storage_error("queue TCP/FIPS Nostr pubsub frame", error))
    }

    pub(super) fn handle_frame(&self, source_peer: PeerIdentity, frame: &[u8]) {
        let Ok(message) = self.codec.decode_frame(frame) else {
            self.record_provider_violation(source_peer, ProviderViolation::MalformedFrame);
            return;
        };

        let source_npub = source_peer.npub();
        let source_id = SourceId::new(source_npub.clone());
        match message {
            FipsPubsubWireMessage::Req {
                subscription_id,
                filters,
            } => {
                self.req_frames_received.fetch_add(1, Ordering::Relaxed);
                self.handle_req(source_peer, source_id, &subscription_id, &filters);
            }
            FipsPubsubWireMessage::Close { subscription_id } => {
                self.close_frames_received.fetch_add(1, Ordering::Relaxed);
                self.handle_close(&source_id, &subscription_id);
            }
            FipsPubsubWireMessage::Event {
                subscription_id,
                event,
            } => {
                self.event_frames_received.fetch_add(1, Ordering::Relaxed);
                let subscription_key = subscription_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let event_id = event.as_event().id.to_string();
                let first_observation = self
                    .observed_full_events
                    .lock()
                    .is_ok_and(|mut seen| seen.observe(&source_npub, &subscription_key, &event_id));
                let is_subscribed =
                    self.event_matches_subscription(&source_npub, subscription_id.as_ref(), &event);
                if !is_subscribed {
                    self.record_provider_violation(
                        source_peer,
                        ProviderViolation::OutOfFilterEvent {
                            repeated: !first_observation,
                        },
                    );
                    return;
                }
                if !first_observation {
                    return;
                }
                self.handle_event(&source_npub, &source_id, subscription_id.as_ref(), &event);
            }
            FipsPubsubWireMessage::Inv {
                subscription_ids,
                event_id,
                event_kind,
                payload_bytes,
                hop_limit,
            } => self.handle_inv(
                source_peer,
                &source_npub,
                InventoryAdvertisement {
                    subscription_ids,
                    event_id,
                    event_kind,
                    payload_bytes,
                    hop_limit,
                },
            ),
            FipsPubsubWireMessage::Want { event_id } => {
                self.want_frames_received.fetch_add(1, Ordering::Relaxed);
                self.handle_want(source_peer, &source_id, &event_id);
            }
        }
    }

    pub(super) fn handle_req(
        &self,
        source_peer: PeerIdentity,
        source_id: SourceId,
        subscription_id: &SubscriptionId,
        filters: &[Filter],
    ) {
        if self
            .remember_peer_subscription(source_id, subscription_id, filters.to_vec())
            .is_err()
        {
            return;
        }
        for cached in self.recent_matching_events(filters).unwrap_or_default() {
            let Ok(frame) = self.inventory_frame(
                vec![subscription_id.clone()],
                &cached.event,
                cached.hop_limit,
            ) else {
                continue;
            };
            let _ = self.send_frame(source_peer, frame);
        }
    }

    pub(super) fn handle_close(&self, source_id: &SourceId, subscription_id: &SubscriptionId) {
        if let Ok(mut subscriptions) = self.peer_subscriptions.lock() {
            subscriptions.remove(source_id, &subscription_id.to_string());
        }
    }

    pub(super) fn handle_event(
        &self,
        source_npub: &str,
        source_id: &SourceId,
        subscription_id: Option<&SubscriptionId>,
        event: &VerifiedEvent,
    ) {
        let hop_limit = match subscription_id {
            Some(subscription_id) => Some(
                self.complete_want(source_npub, subscription_id, event)
                    .unwrap_or(None)
                    .unwrap_or(0),
            ),
            None => Some(self.options.max_hops),
        };
        let Some(hop_limit) = hop_limit else {
            return;
        };
        if subscription_id.is_some() {
            self.subscription_events_received
                .fetch_add(1, Ordering::Relaxed);
        }
        if !self.deliver_local_event(source_npub, event) {
            return;
        }
        let source = EventSource::fips_endpoint(source_npub);
        if self
            .remember_event(event.clone(), source, hop_limit)
            .unwrap_or(false)
            && hop_limit > 0
        {
            let targets = self
                .peer_delivery_targets(event, Some(source_id))
                .unwrap_or_default();
            let _ = self.send_inventories(targets, event, hop_limit);
        }
    }

    pub(super) fn handle_inv(
        &self,
        source_peer: PeerIdentity,
        source_npub: &str,
        inventory: InventoryAdvertisement,
    ) {
        self.inv_frames_received.fetch_add(1, Ordering::Relaxed);
        let Ok(Some(frame)) = self.accept_inventory(source_npub, inventory) else {
            return;
        };
        if self.send_frame(source_peer, frame).is_ok() {
            self.want_frames_sent.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn handle_want(
        &self,
        source_peer: PeerIdentity,
        source_id: &SourceId,
        event_id: &EventId,
    ) {
        let Ok(Some((subscription_id, event))) = self.event_for_want(source_id, event_id) else {
            return;
        };
        let Ok(frame) = self
            .codec
            .encode_frame(&FipsPubsubWireMessage::deliver(subscription_id, event))
        else {
            return;
        };
        let _ = self.send_frame(source_peer, frame);
    }

    pub(super) fn remember_peer_subscription(
        &self,
        peer_id: SourceId,
        subscription_id: &SubscriptionId,
        filters: Vec<Filter>,
    ) -> Result<()> {
        self.peer_subscriptions
            .lock()
            .map_err(|_| poisoned("FIPS peer subscription state"))?
            .upsert_filters(peer_id, subscription_id.to_string(), filters)?;
        Ok(())
    }

    pub(super) fn deliver_local_event(&self, source_npub: &str, event: &VerifiedEvent) -> bool {
        let event_id = event.as_event().id.to_string();
        let Ok(mut subscriptions) = self.subscriptions.lock() else {
            return false;
        };
        let mut delivered = false;
        for active in subscriptions.values_mut() {
            if !active.peers.contains(source_npub)
                || PubsubPeerInterest::from_filters(&active.filters, event)
                    != PubsubPeerInterest::Subscribed
                || active.recent_event_ids.contains(&event_id)
            {
                continue;
            }
            deliver_local(
                active,
                event.clone(),
                EventSource::fips_endpoint(source_npub),
                &event_id,
                FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS,
            );
            delivered = true;
        }
        delivered
    }

    fn event_matches_subscription(
        &self,
        source_npub: &str,
        subscription_id: Option<&SubscriptionId>,
        event: &VerifiedEvent,
    ) -> bool {
        let Ok(subscriptions) = self.subscriptions.lock() else {
            return false;
        };
        match subscription_id {
            Some(subscription_id) => {
                subscriptions
                    .get(&subscription_id.to_string())
                    .is_some_and(|active| {
                        active.peers.contains(source_npub)
                            && PubsubPeerInterest::from_filters(&active.filters, event)
                                == PubsubPeerInterest::Subscribed
                    })
            }
            None => subscriptions.values().any(|active| {
                active.peers.contains(source_npub)
                    && PubsubPeerInterest::from_filters(&active.filters, event)
                        == PubsubPeerInterest::Subscribed
            }),
        }
    }

    pub(super) fn remember_event(
        &self,
        event: VerifiedEvent,
        source: EventSource,
        hop_limit: u8,
    ) -> Result<bool> {
        self.recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))
            .map(|mut events| events.insert(event, source, hop_limit))
    }

    pub(super) fn recent_matching_events(&self, filters: &[Filter]) -> Result<Vec<CachedEvent>> {
        self.recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))
            .map(|events| events.matching(filters))
    }

    pub(super) fn replay_local_events(&self, key: &str) -> Result<()> {
        let recent = self
            .recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))?
            .entries
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut subscriptions = self.lock_subscriptions()?;
        let Some(active) = subscriptions.get_mut(key) else {
            return Ok(());
        };
        for cached in recent {
            if PubsubPeerInterest::from_filters(&active.filters, &cached.event)
                != PubsubPeerInterest::Subscribed
            {
                continue;
            }
            let event_id = cached.event.as_event().id.to_string();
            deliver_local(
                active,
                cached.event,
                cached.source,
                &event_id,
                self.options.max_replay_events,
            );
        }
        Ok(())
    }

    pub(super) fn peer_delivery_targets(
        &self,
        event: &VerifiedEvent,
        excluded_peer: Option<&SourceId>,
    ) -> Result<Vec<(String, Vec<SubscriptionId>)>> {
        let subscriptions = self
            .peer_subscriptions
            .lock()
            .map_err(|_| poisoned("FIPS peer subscription state"))?;
        let mut targets: Vec<(String, Vec<SubscriptionId>)> = Vec::new();
        for (peer, subscription) in subscriptions.matching_peer_subscriptions(event) {
            if excluded_peer == Some(peer) {
                continue;
            }
            let peer_npub = peer.as_str();
            if let Some((last_peer, subscription_ids)) = targets.last_mut()
                && last_peer == peer_npub
            {
                subscription_ids.push(SubscriptionId::new(subscription.subscription_id.clone()));
            } else {
                targets.push((
                    peer_npub.to_string(),
                    vec![SubscriptionId::new(subscription.subscription_id.clone())],
                ));
            }
        }
        Ok(targets)
    }

    pub(super) fn send_inventories(
        &self,
        targets: Vec<(String, Vec<SubscriptionId>)>,
        event: &VerifiedEvent,
        hop_limit: u8,
    ) -> usize {
        if hop_limit == 0 {
            return 0;
        }
        let mut sent = 0;
        for (npub, subscription_ids) in targets {
            let Ok(peer) = PeerIdentity::from_npub(&npub) else {
                continue;
            };
            let Ok(frame) = self.inventory_frame(subscription_ids, event, hop_limit) else {
                continue;
            };
            if self.send_frame(peer, frame).is_ok() {
                sent += 1;
            }
        }
        sent
    }

    pub(super) fn inventory_frame(
        &self,
        subscription_ids: Vec<SubscriptionId>,
        event: &VerifiedEvent,
        hop_limit: u8,
    ) -> Result<Vec<u8>> {
        let payload_bytes = event_payload_bytes(event)?;
        self.codec.encode_frame(&FipsPubsubWireMessage::inv(
            subscription_ids,
            event.as_event().id,
            event.as_event().kind.as_u16(),
            payload_bytes,
            hop_limit,
        ))
    }

    pub(super) fn accept_inventory(
        &self,
        source_npub: &str,
        inventory: InventoryAdvertisement,
    ) -> Result<Option<Vec<u8>>> {
        let InventoryAdvertisement {
            subscription_ids,
            event_id,
            event_kind,
            payload_bytes,
            hop_limit,
        } = inventory;
        if subscription_ids.len() > self.options.max_active_subscriptions {
            return Ok(None);
        }
        let event_id_hex = event_id.to_hex();
        if self
            .recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))?
            .contains(&event_id_hex)
        {
            return Ok(None);
        }
        let subscriptions = self.lock_subscriptions()?;
        let candidate_subscription_ids = subscription_ids
            .into_iter()
            .filter(|subscription_id| {
                subscriptions
                    .get(&subscription_id.to_string())
                    .is_some_and(|active| {
                        active.peers.contains(source_npub)
                            && !active.recent_event_ids.contains(&event_id_hex)
                    })
            })
            .map(|subscription_id| subscription_id.to_string())
            .collect::<Vec<_>>();
        drop(subscriptions);
        let mut observed = self
            .observed_inventories
            .lock()
            .map_err(|_| poisoned("FIPS observed inventory IDs"))?;
        let valid_subscription_ids = candidate_subscription_ids
            .into_iter()
            .filter(|subscription_id| observed.observe(source_npub, subscription_id, &event_id_hex))
            .collect::<Vec<_>>();
        if valid_subscription_ids.is_empty() {
            return Ok(None);
        }
        drop(observed);

        let provider = InventoryProvider {
            peer_npub: source_npub.to_string(),
            subscription_ids: valid_subscription_ids,
        };
        let inventory = PendingInventory {
            selected: provider,
            alternatives: VecDeque::new(),
            event_kind,
            payload_bytes,
            hop_limit,
            requested_at_ms: now_ms(),
            retry_count: 0,
        };
        let should_request = self
            .pending_wants
            .lock()
            .map_err(|_| poisoned("FIPS pending WANT state"))?
            .insert(event_id_hex, inventory);
        if !should_request {
            return Ok(None);
        }
        self.codec
            .encode_frame(&FipsPubsubWireMessage::want(event_id))
            .map(Some)
    }

    pub(super) fn complete_want(
        &self,
        source_npub: &str,
        subscription_id: &SubscriptionId,
        event: &VerifiedEvent,
    ) -> Result<Option<u8>> {
        let event_id = event.as_event().id.to_string();
        let payload_bytes = event_payload_bytes(event)?;
        let subscription_key = subscription_id.to_string();
        let subscriptions = self.lock_subscriptions()?;
        let Some(active) = subscriptions.get(&subscription_key) else {
            return Ok(None);
        };
        if !active.peers.contains(source_npub)
            || PubsubPeerInterest::from_filters(&active.filters, event)
                != PubsubPeerInterest::Subscribed
        {
            return Ok(None);
        }
        drop(subscriptions);
        let pending = self
            .pending_wants
            .lock()
            .map_err(|_| poisoned("FIPS pending WANT state"))?
            .take_matching(
                &event_id,
                source_npub,
                &subscription_key,
                event.as_event().kind.as_u16(),
                payload_bytes,
            );
        let Some(pending) = pending else {
            return Ok(None);
        };
        Ok(Some(pending.hop_limit.saturating_sub(1)))
    }

    pub(super) fn retry_pending_frames(&self, now_ms: u64) -> Vec<(PeerIdentity, Vec<u8>)> {
        let retry_after_ms = self
            .options
            .query_timeout
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let due = self
            .pending_wants
            .lock()
            .map(|mut pending| pending.retry_due(now_ms, retry_after_ms))
            .unwrap_or_default();
        self.expired_wants
            .fetch_add(due.expired_event_count as u64, Ordering::Relaxed);
        for provider in due.expired_providers {
            if let Ok(peer) = PeerIdentity::from_npub(&provider.peer_npub) {
                self.record_provider_violation(peer, ProviderViolation::UnansweredInventory);
            }
        }
        due.retries
            .into_iter()
            .filter_map(|(event_id, provider)| {
                let peer = PeerIdentity::from_npub(&provider.peer_npub).ok()?;
                let event_id = EventId::from_hex(&event_id).ok()?;
                let frame = self
                    .codec
                    .encode_frame(&FipsPubsubWireMessage::want(event_id))
                    .ok()?;
                Some((peer, frame))
            })
            .collect()
    }

    pub(super) fn event_for_want(
        &self,
        source_id: &SourceId,
        event_id: &EventId,
    ) -> Result<Option<(SubscriptionId, VerifiedEvent)>> {
        let events = self
            .recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))?;
        let Some(event) = events.event(&event_id.to_hex()).cloned() else {
            return Ok(None);
        };
        drop(events);
        let subscriptions = self
            .peer_subscriptions
            .lock()
            .map_err(|_| poisoned("FIPS peer subscription state"))?;
        let Some(subscription) = subscriptions
            .matching_subscriptions(source_id, &event)
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        Ok(Some((
            SubscriptionId::new(subscription.subscription_id.clone()),
            event,
        )))
    }

    pub(super) fn close_subscription(&self, key: &str) {
        let active = self
            .subscriptions
            .lock()
            .ok()
            .and_then(|mut subscriptions| subscriptions.remove(key));
        let Some(active) = active else {
            return;
        };
        if let Ok(mut pending) = self.pending_wants.lock() {
            pending.remove_subscription(key);
        }
        if let Ok(mut observed) = self.observed_inventories.lock() {
            observed.clear_subscription(key);
        }
        if let Ok(mut observed) = self.observed_full_events.lock() {
            observed.clear_subscription(key);
        }
        self.send_close(key, active.peers);
    }

    pub(super) fn close_all(&self) {
        let active = self
            .subscriptions
            .lock()
            .map(|mut subscriptions| subscriptions.drain().collect::<Vec<_>>())
            .unwrap_or_default();
        for (key, subscription) in active {
            self.send_close(&key, subscription.peers);
        }
        if let Ok(mut pending) = self.pending_wants.lock() {
            pending.clear();
        }
    }

    pub(super) fn send_close(&self, key: &str, peers: HashSet<String>) {
        let Ok(frame) =
            self.codec
                .encode_frame(&FipsPubsubWireMessage::close(SubscriptionId::new(
                    key.to_string(),
                )))
        else {
            return;
        };
        for npub in peers {
            let Ok(peer) = PeerIdentity::from_npub(&npub) else {
                continue;
            };
            let _ = self.transport_tx.try_send(TransportCommand::Send {
                peer,
                frame: frame.clone(),
            });
        }
    }

    pub(super) fn lock_subscriptions(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, ActiveSubscription>>> {
        self.subscriptions.lock().map_err(|_| {
            PubsubError::Storage("FIPS pubsub subscription state poisoned".to_string())
        })
    }

    pub(super) fn replay_frames_for_peer(&self, peer_npub: &str) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            for (key, active) in subscriptions.iter_mut() {
                active.peers.insert(peer_npub.to_string());
                if let Ok(frame) = self.codec.encode_frame(&FipsPubsubWireMessage::req(
                    SubscriptionId::new(key.clone()),
                    active.filters.clone(),
                )) {
                    frames.push(frame);
                }
            }
        }
        frames
    }

    pub(super) fn reset_peer_epoch(&self, peer_npub: &str) {
        if let Ok(mut observed) = self.observed_inventories.lock() {
            observed.clear_peer(peer_npub);
        }
        if let Ok(mut observed) = self.observed_full_events.lock() {
            observed.clear_peer(peer_npub);
        }
        if let Ok(mut pending) = self.pending_wants.lock() {
            pending.remove_peer(peer_npub);
        }
    }

    pub(super) fn peer_is_in_cooldown(&self, peer_npub: &str, now_ms: u64) -> bool {
        self.provider_behavior
            .lock()
            .is_ok_and(|mut behavior| behavior.is_in_cooldown(peer_npub, now_ms))
    }

    fn record_provider_violation(&self, peer: PeerIdentity, violation: ProviderViolation) {
        let peer_npub = peer.npub();
        let cooldown = self
            .provider_behavior
            .lock()
            .ok()
            .and_then(|mut behavior| behavior.record(&peer_npub, violation, now_ms()));
        if cooldown.is_some()
            && self
                .transport_tx
                .try_send(TransportCommand::Cooldown { peer })
                .is_ok()
        {
            self.provider_cooldowns.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(super) struct ConnectedPeer {
    pub(super) npub: String,
    pub(super) identity: PeerIdentity,
    pub(super) link_id: u64,
}

pub(super) struct ActiveSubscription {
    pub(super) filters: Vec<Filter>,
    pub(super) peers: HashSet<String>,
    pub(super) recent_event_ids: HashSet<String>,
    pub(super) recent_event_order: VecDeque<String>,
    pub(super) sender: mpsc::Sender<QueryEvent>,
}

pub(super) struct InventoryAdvertisement {
    pub(super) subscription_ids: Vec<SubscriptionId>,
    pub(super) event_id: EventId,
    pub(super) event_kind: u16,
    pub(super) payload_bytes: u32,
    pub(super) hop_limit: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct InventoryProvider {
    pub(super) peer_npub: String,
    pub(super) subscription_ids: Vec<String>,
}

pub(super) struct PendingInventory {
    pub(super) selected: InventoryProvider,
    pub(super) alternatives: VecDeque<InventoryProvider>,
    pub(super) event_kind: u16,
    pub(super) payload_bytes: u32,
    pub(super) hop_limit: u8,
    pub(super) requested_at_ms: u64,
    pub(super) retry_count: u8,
}

const MAX_WANT_RETRIES: u8 = 5;

#[derive(Default)]
pub(super) struct PendingWantRetryBatch {
    pub(super) retries: Vec<(String, InventoryProvider)>,
    pub(super) expired_providers: Vec<InventoryProvider>,
    pub(super) expired_event_count: usize,
}

pub(super) struct PendingWants {
    pub(super) max_events: usize,
    pub(super) max_alternatives: usize,
    pub(super) entries: HashMap<String, PendingInventory>,
    pub(super) order: VecDeque<String>,
}

impl PendingWants {
    pub(super) fn new(max_events: usize, max_alternatives: usize) -> Self {
        Self {
            max_events,
            max_alternatives,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn insert(&mut self, event_id: String, incoming: PendingInventory) -> bool {
        if let Some(pending) = self.entries.get_mut(&event_id) {
            if pending.event_kind != incoming.event_kind
                || pending.payload_bytes != incoming.payload_bytes
            {
                return false;
            }
            let mut provider = incoming.selected;
            if provider.peer_npub == pending.selected.peer_npub {
                merge_subscription_ids(
                    &mut pending.selected.subscription_ids,
                    provider.subscription_ids,
                );
                return false;
            }
            if let Some(existing) = pending
                .alternatives
                .iter_mut()
                .find(|existing| existing.peer_npub == provider.peer_npub)
            {
                merge_subscription_ids(&mut existing.subscription_ids, provider.subscription_ids);
            } else if pending.alternatives.len() < self.max_alternatives {
                provider.subscription_ids.sort_unstable();
                provider.subscription_ids.dedup();
                pending.alternatives.push_back(provider);
            }
            return false;
        }

        self.order.push_back(event_id.clone());
        self.entries.insert(event_id, incoming);
        while self.entries.len() > self.max_events {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
        true
    }

    pub(super) fn take_matching(
        &mut self,
        event_id: &str,
        source_npub: &str,
        subscription_id: &str,
        event_kind: u16,
        payload_bytes: u32,
    ) -> Option<PendingInventory> {
        let pending = self.entries.get(event_id)?;
        if pending.selected.peer_npub != source_npub
            || !pending
                .selected
                .subscription_ids
                .iter()
                .any(|candidate| candidate == subscription_id)
            || pending.event_kind != event_kind
            || pending.payload_bytes != payload_bytes
        {
            return None;
        }
        self.order.retain(|candidate| candidate != event_id);
        self.entries.remove(event_id)
    }

    pub(super) fn remove_subscription(&mut self, subscription_id: &str) {
        self.entries.retain(|_, pending| {
            pending
                .selected
                .subscription_ids
                .retain(|candidate| candidate != subscription_id);
            for provider in &mut pending.alternatives {
                provider
                    .subscription_ids
                    .retain(|candidate| candidate != subscription_id);
            }
            pending
                .alternatives
                .retain(|provider| !provider.subscription_ids.is_empty());
            if pending.selected.subscription_ids.is_empty()
                && let Some(next) = pending.alternatives.pop_front()
            {
                pending.selected = next;
            }
            !pending.selected.subscription_ids.is_empty()
        });
        self.order
            .retain(|event_id| self.entries.contains_key(event_id));
    }

    pub(super) fn retry_due(&mut self, now_ms: u64, retry_after_ms: u64) -> PendingWantRetryBatch {
        let mut batch = PendingWantRetryBatch::default();
        let mut expired_ids = Vec::new();
        for (event_id, pending) in &mut self.entries {
            let retry_delay = retry_after_ms
                .saturating_mul(1_u64 << u32::from(pending.retry_count.min(MAX_WANT_RETRIES)));
            if now_ms.saturating_sub(pending.requested_at_ms) < retry_delay {
                continue;
            }
            if pending.retry_count >= MAX_WANT_RETRIES {
                batch.expired_event_count += 1;
                batch.expired_providers.push(pending.selected.clone());
                batch
                    .expired_providers
                    .extend(pending.alternatives.iter().cloned());
                expired_ids.push(event_id.clone());
                continue;
            }
            if let Some(next) = pending.alternatives.pop_front() {
                let previous = std::mem::replace(&mut pending.selected, next);
                pending.alternatives.push_back(previous);
            }
            pending.requested_at_ms = now_ms;
            pending.retry_count += 1;
            batch
                .retries
                .push((event_id.clone(), pending.selected.clone()));
        }
        for event_id in expired_ids {
            self.entries.remove(&event_id);
        }
        self.order
            .retain(|event_id| self.entries.contains_key(event_id));
        batch
    }

    pub(super) fn remove_peer(&mut self, peer_npub: &str) {
        self.entries.retain(|_, pending| {
            pending
                .alternatives
                .retain(|provider| provider.peer_npub != peer_npub);
            if pending.selected.peer_npub == peer_npub {
                let Some(next) = pending.alternatives.pop_front() else {
                    return false;
                };
                pending.selected = next;
                pending.retry_count = 0;
                pending.requested_at_ms = now_ms();
            }
            true
        });
        self.order
            .retain(|event_id| self.entries.contains_key(event_id));
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

fn merge_subscription_ids(existing: &mut Vec<String>, incoming: Vec<String>) {
    existing.extend(incoming);
    existing.sort_unstable();
    existing.dedup();
}

#[derive(Clone)]
pub(super) struct CachedEvent {
    pub(super) event: VerifiedEvent,
    pub(super) source: EventSource,
    pub(super) hop_limit: u8,
}

pub(super) struct RecentEvents {
    pub(super) max_payload_events: usize,
    pub(super) max_seen_ids: usize,
    pub(super) event_ids: HashSet<String>,
    pub(super) event_id_order: VecDeque<String>,
    pub(super) entries: VecDeque<CachedEvent>,
}

impl RecentEvents {
    pub(super) fn new(max_payload_events: usize, max_seen_ids: usize) -> Self {
        Self {
            max_payload_events,
            max_seen_ids,
            event_ids: HashSet::new(),
            event_id_order: VecDeque::new(),
            entries: VecDeque::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        event: VerifiedEvent,
        source: EventSource,
        hop_limit: u8,
    ) -> bool {
        let event_id = event.as_event().id.to_string();
        if !self.event_ids.insert(event_id.clone()) {
            return false;
        }
        self.event_id_order.push_back(event_id);
        self.entries.push_back(CachedEvent {
            event,
            source,
            hop_limit,
        });
        while self.entries.len() > self.max_payload_events {
            self.entries.pop_front();
        }
        while self.event_ids.len() > self.max_seen_ids {
            let Some(removed) = self.event_id_order.pop_front() else {
                break;
            };
            self.event_ids.remove(&removed);
        }
        true
    }

    pub(super) fn contains(&self, event_id: &str) -> bool {
        self.event_ids.contains(event_id)
    }

    pub(super) fn event(&self, event_id: &str) -> Option<&VerifiedEvent> {
        self.entries
            .iter()
            .find(|cached| cached.event.as_event().id.to_string() == event_id)
            .map(|cached| &cached.event)
    }

    pub(super) fn matching(&self, filters: &[Filter]) -> Vec<CachedEvent> {
        self.entries
            .iter()
            .filter(|cached| {
                PubsubPeerInterest::from_filters(filters, &cached.event)
                    == PubsubPeerInterest::Subscribed
            })
            .cloned()
            .collect()
    }
}

pub(super) fn deliver_local(
    active: &mut ActiveSubscription,
    event: VerifiedEvent,
    source: EventSource,
    event_id: &str,
    max_replay_events: usize,
) {
    active.recent_event_ids.insert(event_id.to_string());
    active.recent_event_order.push_back(event_id.to_string());
    while active.recent_event_order.len() > max_replay_events {
        if let Some(oldest) = active.recent_event_order.pop_front() {
            active.recent_event_ids.remove(&oldest);
        }
    }
    let _ = active.sender.try_send(QueryEvent {
        event,
        source,
        priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
    });
}
