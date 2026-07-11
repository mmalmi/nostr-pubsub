//! Local Ethernet pubsub over the `fips_core::FipsEndpoint` service API.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::sync::{atomic::AtomicU64, atomic::Ordering};
use std::time::Duration;

use async_trait::async_trait;
use fips_core::{FipsEndpoint, FipsEndpointServiceDatagram, PeerIdentity};
use nostr_pubsub::{
    EventBus, EventSource, Filter, FipsPubsubWireCodec, FipsPubsubWireMessage, PublishReport,
    PubsubError, PubsubPeerInterest, PubsubProvider, PubsubProviderMode, QueryEvent, QueryOptions,
    QueryReport, Result, SOURCE_PRIORITY_FIPS_ENDPOINT, SubscriptionId, VerifiedEvent,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

mod reputation;
pub use reputation::*;

pub const FIPS_NOSTR_PUBSUB_SERVICE_PORT: u16 = 7368;
pub const FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS: usize = 8;

/// Maximum FSP service body after its encrypted inner header and port header.
pub const FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES: usize = u16::MAX as usize - 10;

const LOCAL_ETHERNET_TRANSPORT: &str = "ethernet";
const FIRST_ROUTE_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Resource limits and replay window for a FIPS pubsub client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FipsPubsubClientOptions {
    /// How long an [`EventBus::query`] waits for subscribed peer replies.
    pub query_timeout: Duration,
    /// Maximum encoded Nostr frame accepted from or sent to a peer.
    pub max_frame_bytes: usize,
    /// Maximum connected Ethernet peers included in one fanout.
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
        Self::start_for_transport(endpoint, options, LOCAL_ETHERNET_TRANSPORT).await
    }

    async fn start_for_transport(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        peer_transport: &'static str,
    ) -> Result<Self> {
        options.validate()?;
        endpoint
            .register_service(FIPS_NOSTR_PUBSUB_SERVICE_PORT)
            .await
            .map_err(|error| storage_error("register FIPS pubsub service", error))?;
        let codec = FipsPubsubWireCodec::new(options.max_frame_bytes)?;
        let receive_batch_size = options.receive_batch_size;
        let inner = Arc::new(ClientInner {
            endpoint: Arc::clone(&endpoint),
            codec,
            options,
            peer_transport,
            next_subscription_id: AtomicU64::new(1),
            subscriptions: Mutex::new(HashMap::new()),
        });
        let receiver_task = tokio::spawn(receive_loop(
            Arc::downgrade(&inner),
            endpoint,
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
    async fn publish(&self, event: VerifiedEvent, _source: EventSource) -> Result<PublishReport> {
        let frame = self
            .inner
            .codec
            .encode_frame(&FipsPubsubWireMessage::publish(event))?;
        let peers = self.inner.connected_peers().await?;
        if peers.is_empty() {
            return Err(no_local_peers(self.inner.peer_transport));
        }

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
            return Err(last_error.unwrap_or_else(|| no_local_peers(self.inner.peer_transport)));
        }

        Ok(PublishReport {
            accepted: true,
            priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
            reason: (sent < peer_count)
                .then(|| format!("sent to {sent} of {peer_count} local Ethernet peers")),
        })
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
            match tokio::time::timeout_at(deadline, subscription.recv_query_item()).await {
                Ok(Some(SubscriptionDelivery::Event(event))) => events.push(event),
                Ok(Some(SubscriptionDelivery::Eose) | None) | Err(_) => break,
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
    receiver: mpsc::Receiver<SubscriptionDelivery>,
    inner: Arc<ClientInner>,
    closed: bool,
}

impl FipsPubsubSubscription {
    #[must_use]
    pub fn id(&self) -> &SubscriptionId {
        &self.subscription_id
    }

    pub async fn recv(&mut self) -> Option<QueryEvent> {
        loop {
            match self.receiver.recv().await? {
                SubscriptionDelivery::Event(event) => return Some(event),
                SubscriptionDelivery::Eose => {}
            }
        }
    }

    async fn recv_query_item(&mut self) -> Option<SubscriptionDelivery> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> std::result::Result<QueryEvent, mpsc::error::TryRecvError> {
        loop {
            match self.receiver.try_recv()? {
                SubscriptionDelivery::Event(event) => return Ok(event),
                SubscriptionDelivery::Eose => {}
            }
        }
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
    peer_transport: &'static str,
    next_subscription_id: AtomicU64,
    subscriptions: Mutex<HashMap<String, ActiveSubscription>>,
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
                peer.connected && peer.transport_type.as_deref() == Some(self.peer_transport)
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
                self.peer_transport,
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
            return Err(no_local_peers(self.peer_transport));
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
                    peer_event_ids: HashMap::new(),
                    peer_eose_counts: HashMap::new(),
                    replay_complete_sent: false,
                    recent_event_ids: HashSet::new(),
                    recent_event_order: VecDeque::new(),
                    sender,
                },
            );
        }

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
            return Err(last_error.unwrap_or_else(|| no_local_peers(self.peer_transport)));
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

    fn handle_datagram(&self, datagram: &FipsEndpointServiceDatagram) {
        if datagram.source_port != FIPS_NOSTR_PUBSUB_SERVICE_PORT
            || datagram.destination_port != FIPS_NOSTR_PUBSUB_SERVICE_PORT
        {
            return;
        }
        let Ok(message) = self.codec.decode_frame(datagram.data.as_slice()) else {
            return;
        };

        let (subscription_id, event, eose_count) = match message {
            FipsPubsubWireMessage::Event {
                subscription_id: Some(subscription_id),
                event,
            } => (subscription_id, Some(event), None),
            FipsPubsubWireMessage::Eose {
                subscription_id,
                event_count,
            } => (subscription_id, None, Some(event_count)),
            _ => return,
        };

        let key = subscription_id.to_string();
        let source_npub = datagram.source_peer.npub();
        let Ok(mut subscriptions) = self.subscriptions.lock() else {
            return;
        };
        let Some(active) = subscriptions.get_mut(&key) else {
            return;
        };
        if !active.peers.contains(&source_npub) {
            return;
        }
        let Some(event) = event else {
            active
                .peer_eose_counts
                .insert(source_npub, eose_count.unwrap_or_default());
            maybe_signal_replay_complete(active);
            return;
        };
        let event_id = event.as_event().id.to_string();
        if PubsubPeerInterest::from_filters(&active.filters, &event)
            != PubsubPeerInterest::Subscribed
        {
            return;
        }
        let peer_events = active
            .peer_event_ids
            .entry(source_npub.clone())
            .or_default();
        if !peer_events.insert(event_id.clone()) {
            return;
        }

        if active.recent_event_ids.contains(&event_id) {
            maybe_signal_replay_complete(active);
            return;
        }

        active.recent_event_ids.insert(event_id.clone());
        active.recent_event_order.push_back(event_id);
        while active.recent_event_order.len() > self.options.max_replay_events {
            if let Some(oldest) = active.recent_event_order.pop_front() {
                active.recent_event_ids.remove(&oldest);
            }
        }
        let _ = active
            .sender
            .try_send(SubscriptionDelivery::Event(QueryEvent {
                event,
                source: EventSource::fips_endpoint(source_npub),
                priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
            }));
        maybe_signal_replay_complete(active);
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
    peer_event_ids: HashMap<String, HashSet<String>>,
    peer_eose_counts: HashMap<String, usize>,
    replay_complete_sent: bool,
    recent_event_ids: HashSet<String>,
    recent_event_order: VecDeque<String>,
    sender: mpsc::Sender<SubscriptionDelivery>,
}

enum SubscriptionDelivery {
    Event(QueryEvent),
    Eose,
}

fn maybe_signal_replay_complete(active: &mut ActiveSubscription) {
    if active.replay_complete_sent {
        return;
    }
    let complete = active.peers.iter().all(|peer| {
        active.peer_eose_counts.get(peer).is_some_and(|expected| {
            active.peer_event_ids.get(peer).map_or(0, HashSet::len) >= *expected
        })
    });
    if complete {
        active.replay_complete_sent = true;
        let _ = active.sender.try_send(SubscriptionDelivery::Eose);
    }
}

async fn receive_loop(
    inner: Weak<ClientInner>,
    endpoint: Arc<FipsEndpoint>,
    receive_batch_size: usize,
) {
    let mut datagrams = Vec::with_capacity(receive_batch_size);
    loop {
        let Some(_) = endpoint
            .recv_service_datagram_batch_into(&mut datagrams, receive_batch_size)
            .await
        else {
            break;
        };
        let Some(inner) = inner.upgrade() else {
            break;
        };
        for datagram in datagrams.drain(..) {
            inner.handle_datagram(&datagram);
        }
    }
}

fn invalid_option(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(format!(
        "invalid FIPS pubsub client option: {}",
        message.into()
    ))
}

fn no_local_peers(transport: &str) -> PubsubError {
    PubsubError::Storage(format!("no connected local {transport} FIPS pubsub peers"))
}

fn storage_error(operation: &str, error: impl std::fmt::Display) -> PubsubError {
    PubsubError::Storage(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests;
