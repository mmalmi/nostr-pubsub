//! Pubsub over authenticated peers on the `fips_core::FipsEndpoint` service API.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::sync::{atomic::AtomicU64, atomic::Ordering};
use std::time::Duration;

use async_trait::async_trait;
use fips_core::discovery::local::LocalInstanceCapability;
use fips_core::{
    FipsEndpoint, FipsEndpointServiceDatagram, FipsEndpointServiceReceiver, PeerIdentity,
};
use nostr_pubsub::{
    EventBus, EventSource, Filter, FipsPubsubWireCodec, FipsPubsubWireMessage, PublishReport,
    PubsubError, PubsubPeerInterest, PubsubPeerSubscriptionStore, PubsubProvider,
    PubsubProviderMode, PubsubSubscriptionLimits, QueryEvent, QueryOptions, QueryReport, Result,
    SOURCE_PRIORITY_FIPS_ENDPOINT, SourceId, SubscriptionId, VerifiedEvent,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

mod peerfinding;
mod reputation;
mod stats;
mod stream;
pub use peerfinding::*;
pub use reputation::*;
pub use stream::*;

pub const FIPS_NOSTR_PUBSUB_SERVICE_PORT: u16 = 7368;
pub const FIPS_NOSTR_PUBSUB_CAPABILITY: &str = "nostr.pubsub/1";
pub const FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS: usize = 8;

/// Maximum FSP service body after its encrypted inner header and port header.
pub const FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES: usize = u16::MAX as usize - 10;

const FIRST_ROUTE_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Resource limits and replay window for a FIPS pubsub client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FipsPubsubClientOptions {
    /// How long an [`EventBus::query`] waits for subscribed peer replies.
    pub query_timeout: Duration,
    /// Maximum encoded Nostr frame accepted from or sent to a peer.
    pub max_frame_bytes: usize,
    /// Maximum connected FIPS peers included in one fanout.
    pub max_connected_peers: usize,
    /// Maximum simultaneous streaming or query subscriptions.
    pub max_active_subscriptions: usize,
    /// Maximum Nostr filters carried by one subscription.
    pub max_filters_per_subscription: usize,
    /// Replay limit, delivery queue capacity, and recent event ID bound.
    pub max_replay_events: usize,
    /// Maximum service datagrams drained from FIPS in one receive turn.
    pub receive_batch_size: usize,
}

impl Default for FipsPubsubClientOptions {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_millis(500),
            max_frame_bytes: FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES,
            max_connected_peers: 64,
            max_active_subscriptions: 64,
            max_filters_per_subscription: 4,
            max_replay_events: FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS,
            receive_batch_size: 64,
        }
    }
}

impl FipsPubsubClientOptions {
    fn validate(&self) -> Result<()> {
        if self.query_timeout.is_zero() {
            return Err(invalid_option("query_timeout must be greater than zero"));
        }
        for (name, value) in [
            ("max_frame_bytes", self.max_frame_bytes),
            ("max_connected_peers", self.max_connected_peers),
            ("max_active_subscriptions", self.max_active_subscriptions),
            (
                "max_filters_per_subscription",
                self.max_filters_per_subscription,
            ),
            ("max_replay_events", self.max_replay_events),
            ("receive_batch_size", self.receive_batch_size),
        ] {
            if value == 0 {
                return Err(invalid_option(format!("{name} must be greater than zero")));
            }
        }
        if self.max_frame_bytes > FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES {
            return Err(invalid_option(format!(
                "max_frame_bytes cannot exceed FIPS service datagram limit {FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES}"
            )));
        }
        Ok(())
    }
}

pub struct FipsPubsubClient {
    inner: Arc<ClientInner>,
    receiver_task: Option<JoinHandle<()>>,
}

impl FipsPubsubClient {
    pub async fn start(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
    ) -> Result<Self> {
        Self::start_for_peer_transport(endpoint, options, None).await
    }

    #[cfg(test)]
    async fn start_for_transport(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        peer_transport: &'static str,
    ) -> Result<Self> {
        Self::start_for_peer_transport(endpoint, options, Some(peer_transport)).await
    }

    async fn start_for_peer_transport(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        peer_transport: Option<&'static str>,
    ) -> Result<Self> {
        options.validate()?;
        let service_receiver = endpoint
            .register_service_receiver_with_capability(LocalInstanceCapability::service(
                FIPS_NOSTR_PUBSUB_CAPABILITY,
                FIPS_NOSTR_PUBSUB_SERVICE_PORT,
            ))
            .await
            .map_err(|error| storage_error("register FIPS pubsub service", error))?;
        let codec = FipsPubsubWireCodec::new(options.max_frame_bytes)?;
        let receive_batch_size = options.receive_batch_size;
        let subscription_limits = PubsubSubscriptionLimits {
            max_peers: options.max_connected_peers,
            max_subscriptions_per_peer: options.max_active_subscriptions,
            max_filters_per_subscription: options.max_filters_per_subscription,
        };
        let max_replay_events = options.max_replay_events;
        let inner = Arc::new(ClientInner {
            endpoint: Arc::clone(&endpoint),
            codec,
            options,
            peer_transport,
            next_subscription_id: AtomicU64::new(1),
            subscriptions: Mutex::new(HashMap::new()),
            peer_subscriptions: Mutex::new(PubsubPeerSubscriptionStore::new(subscription_limits)),
            recent_events: Mutex::new(RecentEvents::new(max_replay_events)),
        });
        let receiver_task = tokio::spawn(receive_loop(
            Arc::downgrade(&inner),
            service_receiver,
            receive_batch_size,
        ));
        Ok(Self {
            inner,
            receiver_task: Some(receiver_task),
        })
    }

    #[must_use]
    pub fn options(&self) -> &FipsPubsubClientOptions {
        &self.inner.options
    }

    pub async fn connected_peer_count(&self) -> Result<usize> {
        self.inner.connected_peers().await.map(|peers| peers.len())
    }

    pub fn active_subscription_count(&self) -> Result<usize> {
        Ok(self.inner.lock_subscriptions()?.len())
    }

    pub fn peer_subscription_count(&self) -> Result<usize> {
        self.inner
            .peer_subscriptions
            .lock()
            .map(|subscriptions| subscriptions.subscription_count())
            .map_err(|_| poisoned("FIPS peer subscription state"))
    }

    pub async fn subscribe(&self, filters: Vec<Filter>) -> Result<FipsPubsubSubscription> {
        self.inner.subscribe(filters).await
    }

    pub async fn shutdown(mut self) {
        self.inner.close_all();
        if let Some(task) = self.receiver_task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for FipsPubsubClient {
    fn drop(&mut self) {
        self.inner.close_all();
        if let Some(task) = self.receiver_task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl EventBus for FipsPubsubClient {
    async fn publish(&self, event: VerifiedEvent, source: EventSource) -> Result<PublishReport> {
        let peers = self.inner.connected_peers().await?;
        if peers.is_empty() {
            return Err(no_connected_peers(self.inner.peer_transport));
        }

        let is_new = self.inner.remember_event(event.clone(), source)?;
        if !is_new {
            return Ok(PublishReport {
                accepted: true,
                priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
                reason: Some("event was already published".to_string()),
            });
        }

        let subscribed = self.inner.peer_delivery_targets(&event, None)?;
        if !subscribed.is_empty() {
            let peer_count = subscribed.len();
            let sent = self.inner.send_deliveries(subscribed, &event).await;
            return publish_report(sent, peer_count, "subscribed FIPS peers");
        }

        // A raw client EVENT lets relay-like peers ingest publications even
        // before they have sent us a REQ. Ordinary peer clients cache it and
        // replay it once their local consumer subscribes.
        let frame = self
            .inner
            .codec
            .encode_frame(&FipsPubsubWireMessage::publish(event))?;
        let peer_count = peers.len();
        let mut sent = 0usize;
        let mut last_error = None;
        for peer in peers {
            match self
                .inner
                .send_frame(peer.identity, frame.clone(), peer.needs_session_setup)
                .await
            {
                Ok(()) => sent += 1,
                Err(error) => last_error = Some(error),
            }
        }
        if sent == 0 {
            return Err(last_error.unwrap_or_else(|| no_connected_peers(self.inner.peer_transport)));
        }

        let mut report = publish_report(sent, peer_count, "connected FIPS peers")?;
        if report.reason.is_none() && last_error.is_some() {
            report.reason = Some("a FIPS publication send failed".to_string());
        }
        Ok(report)
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let limit = options
            .limit
            .or_else(|| filters.iter().filter_map(|filter| filter.limit).min())
            .unwrap_or(self.inner.options.max_replay_events)
            .min(self.inner.options.max_replay_events);
        if limit == 0 {
            return Ok(QueryReport::default());
        }

        let mut subscription = self.subscribe(filters).await?;
        let deadline = Instant::now() + self.inner.options.query_timeout;
        let mut events = Vec::with_capacity(limit);
        while events.len() < limit {
            match tokio::time::timeout_at(deadline, subscription.recv()).await {
                Ok(Some(event)) => events.push(event),
                Ok(None) | Err(_) => break,
            }
        }
        subscription.close();
        Ok(QueryReport { events })
    }
}

impl PubsubProvider for FipsPubsubClient {
    fn mode(&self) -> PubsubProviderMode {
        PubsubProviderMode::LocalOnly
    }
}

pub struct FipsPubsubSubscription {
    subscription_id: SubscriptionId,
    key: String,
    receiver: mpsc::Receiver<QueryEvent>,
    inner: Arc<ClientInner>,
    closed: bool,
}

impl FipsPubsubSubscription {
    #[must_use]
    pub fn id(&self) -> &SubscriptionId {
        &self.subscription_id
    }

    pub async fn recv(&mut self) -> Option<QueryEvent> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> std::result::Result<QueryEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn close(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.inner.close_subscription(&self.key);
    }
}

impl Drop for FipsPubsubSubscription {
    fn drop(&mut self) {
        self.cleanup();
    }
}

struct ClientInner {
    endpoint: Arc<FipsEndpoint>,
    codec: FipsPubsubWireCodec,
    options: FipsPubsubClientOptions,
    peer_transport: Option<&'static str>,
    next_subscription_id: AtomicU64,
    subscriptions: Mutex<HashMap<String, ActiveSubscription>>,
    peer_subscriptions: Mutex<PubsubPeerSubscriptionStore>,
    recent_events: Mutex<RecentEvents>,
}

impl ClientInner {
    async fn connected_peers(&self) -> Result<Vec<ConnectedPeer>> {
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
                    needs_session_setup: peer.last_outbound_route.is_none(),
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

    async fn subscribe(
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
            match self
                .send_frame(peer.identity, frame.clone(), peer.needs_session_setup)
                .await
            {
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

    async fn send_frame(
        &self,
        peer: PeerIdentity,
        frame: Vec<u8>,
        needs_session_setup: bool,
    ) -> Result<()> {
        self.endpoint
            .send_datagram(
                peer,
                FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                frame.clone(),
            )
            .await
            .map_err(|error| storage_error("send FIPS pubsub datagram", error))?;
        if needs_session_setup {
            tokio::time::sleep(FIRST_ROUTE_RETRY_DELAY).await;
            let _ = self
                .endpoint
                .send_datagram(
                    peer,
                    FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                    FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                    frame,
                )
                .await;
        }
        Ok(())
    }

    async fn handle_datagram(&self, datagram: &FipsEndpointServiceDatagram) {
        if datagram.source_port != FIPS_NOSTR_PUBSUB_SERVICE_PORT
            || datagram.destination_port != FIPS_NOSTR_PUBSUB_SERVICE_PORT
        {
            return;
        }
        let Ok(message) = self.codec.decode_frame(datagram.data.as_slice()) else {
            return;
        };

        let source_npub = datagram.source_peer.npub();
        let source_id = SourceId::new(source_npub.clone());
        match message {
            FipsPubsubWireMessage::Req {
                subscription_id,
                filters,
            } => {
                if self
                    .remember_peer_subscription(source_id, &subscription_id, filters.clone())
                    .is_err()
                {
                    return;
                }
                let replay = self.recent_matching_events(&filters).unwrap_or_default();
                for event in replay {
                    let Ok(frame) = self.codec.encode_frame(&FipsPubsubWireMessage::deliver(
                        subscription_id.clone(),
                        event,
                    )) else {
                        continue;
                    };
                    let _ = self
                        .endpoint
                        .send_datagram(
                            datagram.source_peer,
                            FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                            FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                            frame,
                        )
                        .await;
                }
            }
            FipsPubsubWireMessage::Close { subscription_id } => {
                if let Ok(mut subscriptions) = self.peer_subscriptions.lock() {
                    subscriptions.remove(&source_id, &subscription_id.to_string());
                }
            }
            FipsPubsubWireMessage::Event {
                subscription_id,
                event,
            } => {
                self.deliver_local_event(&source_npub, subscription_id.as_ref(), &event);
                let source = EventSource::fips_endpoint(source_npub);
                if self.remember_event(event.clone(), source).unwrap_or(false) {
                    let targets = self
                        .peer_delivery_targets(&event, Some(&source_id))
                        .unwrap_or_default();
                    let _ = self.send_deliveries(targets, &event).await;
                }
            }
        }
    }

    fn remember_peer_subscription(
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

    fn deliver_local_event(
        &self,
        source_npub: &str,
        subscription_id: Option<&SubscriptionId>,
        event: &VerifiedEvent,
    ) {
        let event_id = event.as_event().id.to_string();
        let Ok(mut subscriptions) = self.subscriptions.lock() else {
            return;
        };
        for (key, active) in subscriptions.iter_mut() {
            if subscription_id.is_some_and(|id| id.to_string() != *key)
                || !active.peers.contains(source_npub)
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
                self.options.max_replay_events,
            );
        }
    }

    fn remember_event(&self, event: VerifiedEvent, source: EventSource) -> Result<bool> {
        self.recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))
            .map(|mut events| events.insert(event, source))
    }

    fn recent_matching_events(&self, filters: &[Filter]) -> Result<Vec<VerifiedEvent>> {
        self.recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))
            .map(|events| events.matching(filters))
    }

    fn replay_local_events(&self, key: &str) -> Result<()> {
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

    fn peer_delivery_targets(
        &self,
        event: &VerifiedEvent,
        excluded_peer: Option<&SourceId>,
    ) -> Result<Vec<(String, SubscriptionId)>> {
        let subscriptions = self
            .peer_subscriptions
            .lock()
            .map_err(|_| poisoned("FIPS peer subscription state"))?;
        let mut targets = Vec::new();
        for (peer, subscription) in subscriptions.matching_peer_subscriptions(event) {
            if excluded_peer == Some(peer) {
                continue;
            }
            targets.push((
                peer.as_str().to_string(),
                SubscriptionId::new(subscription.subscription_id.clone()),
            ));
        }
        Ok(targets)
    }

    async fn send_deliveries(
        &self,
        targets: Vec<(String, SubscriptionId)>,
        event: &VerifiedEvent,
    ) -> usize {
        let mut sent = 0;
        for (npub, subscription_id) in targets {
            let Ok(peer) = PeerIdentity::from_npub(&npub) else {
                continue;
            };
            let Ok(frame) = self.codec.encode_frame(&FipsPubsubWireMessage::deliver(
                subscription_id,
                event.clone(),
            )) else {
                continue;
            };
            if self.send_frame(peer, frame, false).await.is_ok() {
                sent += 1;
            }
        }
        sent
    }

    fn close_subscription(&self, key: &str) {
        let active = self
            .subscriptions
            .lock()
            .ok()
            .and_then(|mut subscriptions| subscriptions.remove(key));
        let Some(active) = active else {
            return;
        };
        self.send_close(key, active.peers);
    }

    fn close_all(&self) {
        let active = self
            .subscriptions
            .lock()
            .map(|mut subscriptions| subscriptions.drain().collect::<Vec<_>>())
            .unwrap_or_default();
        for (key, subscription) in active {
            self.send_close(&key, subscription.peers);
        }
    }

    fn send_close(&self, key: &str, peers: HashSet<String>) {
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
            let _ = self.endpoint.blocking_send_datagram(
                peer,
                FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                frame.clone(),
            );
        }
    }

    fn lock_subscriptions(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, ActiveSubscription>>> {
        self.subscriptions.lock().map_err(|_| {
            PubsubError::Storage("FIPS pubsub subscription state poisoned".to_string())
        })
    }
}

struct ConnectedPeer {
    npub: String,
    identity: PeerIdentity,
    needs_session_setup: bool,
}

struct ActiveSubscription {
    filters: Vec<Filter>,
    peers: HashSet<String>,
    recent_event_ids: HashSet<String>,
    recent_event_order: VecDeque<String>,
    sender: mpsc::Sender<QueryEvent>,
}

async fn receive_loop(
    inner: Weak<ClientInner>,
    service_receiver: FipsEndpointServiceReceiver,
    receive_batch_size: usize,
) {
    let mut datagrams = Vec::with_capacity(receive_batch_size);
    loop {
        let Some(_) = service_receiver
            .recv_batch_into(&mut datagrams, receive_batch_size)
            .await
        else {
            break;
        };
        let Some(inner) = inner.upgrade() else {
            break;
        };
        for datagram in datagrams.drain(..) {
            inner.handle_datagram(&datagram).await;
        }
    }
}

#[derive(Clone)]
struct CachedEvent {
    event: VerifiedEvent,
    source: EventSource,
}

struct RecentEvents {
    max_events: usize,
    event_ids: HashSet<String>,
    entries: VecDeque<CachedEvent>,
}

impl RecentEvents {
    fn new(max_events: usize) -> Self {
        Self {
            max_events,
            event_ids: HashSet::new(),
            entries: VecDeque::new(),
        }
    }

    fn insert(&mut self, event: VerifiedEvent, source: EventSource) -> bool {
        let event_id = event.as_event().id.to_string();
        if !self.event_ids.insert(event_id) {
            return false;
        }
        self.entries.push_back(CachedEvent { event, source });
        while self.entries.len() > self.max_events {
            if let Some(removed) = self.entries.pop_front() {
                self.event_ids
                    .remove(&removed.event.as_event().id.to_string());
            }
        }
        true
    }

    fn matching(&self, filters: &[Filter]) -> Vec<VerifiedEvent> {
        self.entries
            .iter()
            .filter(|cached| {
                PubsubPeerInterest::from_filters(filters, &cached.event)
                    == PubsubPeerInterest::Subscribed
            })
            .map(|cached| cached.event.clone())
            .collect()
    }
}

fn deliver_local(
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

fn publish_report(sent: usize, peer_count: usize, peers: &str) -> Result<PublishReport> {
    if sent == 0 {
        return Err(PubsubError::Storage(format!(
            "failed to publish to {peer_count} {peers}"
        )));
    }
    Ok(PublishReport {
        accepted: true,
        priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
        reason: (sent < peer_count).then(|| format!("sent to {sent} of {peer_count} {peers}")),
    })
}

fn invalid_option(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(format!(
        "invalid FIPS pubsub client option: {}",
        message.into()
    ))
}

fn no_connected_peers(transport: Option<&str>) -> PubsubError {
    let scope = transport.map_or_else(String::new, |value| format!(" {value}"));
    PubsubError::Storage(format!("no connected{scope} FIPS pubsub peers"))
}

fn storage_error(operation: &str, error: impl std::fmt::Display) -> PubsubError {
    PubsubError::Storage(format!("{operation}: {error}"))
}

fn poisoned(name: &str) -> PubsubError {
    PubsubError::Storage(format!("{name} poisoned"))
}

#[cfg(test)]
mod tests;
