//! Pubsub over authenticated peers on the `fips_core::FipsEndpoint` service API.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fips_core::{FipsEndpoint, PeerIdentity};
use nostr::JsonUtil;
use nostr_pubsub::{
    EventBus, EventId, EventSource, Filter, FipsPubsubWireCodec, FipsPubsubWireMessage,
    NostrEventHandler, NostrEventSubscriber, NostrEventSubscription, PublishReport, PubsubError,
    PubsubPeerInterest, PubsubPeerSubscriptionStore, PubsubProvider, PubsubProviderMode,
    PubsubSubscriptionLimits, QueryEvent, QueryOptions, QueryReport, Result,
    SOURCE_PRIORITY_FIPS_ENDPOINT, SourceId, SubscriptionId, VerifiedEvent,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

mod client_inner;
mod client_transport;
mod peerfinding;
mod pending_wants;
mod provider_behavior;
mod reputation;
mod seen_ids;
mod stats;
mod stream;
mod stream_tcp;
mod wire_tcp;
use client_inner::{ClientInner, RecentEvents};
use client_transport::transport_loop;
pub use peerfinding::*;
use pending_wants::PendingWants;
pub use reputation::*;
pub use stats::*;
pub use stream::*;
pub use stream_tcp::*;
use wire_tcp::{WireTcpDriver, WireTcpOptions};

pub const FIPS_NOSTR_PUBSUB_SERVICE_PORT: u16 = 7368;
pub const FIPS_NOSTR_PUBSUB_CAPABILITY: &str = "nostr.pubsub/1";
pub const FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS: usize = 8;
pub const FIPS_NOSTR_PUBSUB_DEFAULT_MAX_HOPS: u8 = 4;
/// Event IDs retained without full signed payloads for accepted-event dedup.
pub const FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS: usize = 4_096;
/// Unaccepted IDs retained for one authenticated peer/subscription epoch.
pub const FIPS_NOSTR_PUBSUB_MAX_OBSERVED_IDS_PER_SCOPE: usize = 1_024;
/// Aggregate bound across all authenticated peer/subscription observations.
pub const FIPS_NOSTR_PUBSUB_MAX_OBSERVED_IDS: usize = 16_384;

/// Maximum encoded Nostr frame carried in one reliable TCP record.
pub const FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES: usize = u16::MAX as usize - 10;

const TCP_POLL_INTERVAL: Duration = Duration::from_millis(200);

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
    /// Maximum TCP records drained from FIPS in one receive turn.
    pub receive_batch_size: usize,
    /// Maximum live inventory propagation distance through the peer mesh.
    pub max_hops: u8,
}

impl Default for FipsPubsubClientOptions {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_millis(500),
            max_frame_bytes: FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES,
            max_connected_peers: 64,
            max_active_subscriptions: 64,
            max_filters_per_subscription: 4,
            max_replay_events: FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS,
            receive_batch_size: 64,
            max_hops: FIPS_NOSTR_PUBSUB_DEFAULT_MAX_HOPS,
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
        if self.max_frame_bytes > FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES {
            return Err(invalid_option(format!(
                "max_frame_bytes cannot exceed TCP/FIPS record limit {FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES}"
            )));
        }
        if self.max_hops == 0 {
            return Err(invalid_option("max_hops must be greater than zero"));
        }
        Ok(())
    }
}

pub struct FipsPubsubClient {
    inner: Arc<ClientInner>,
    transport_task: Option<JoinHandle<()>>,
}

enum TransportCommand {
    Send { peer: PeerIdentity, frame: Vec<u8> },
    Cooldown { peer: PeerIdentity },
}

impl FipsPubsubClient {
    pub async fn start(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
    ) -> Result<Self> {
        Self::start_for_peer_selection(endpoint, options, None, HashSet::new()).await
    }

    /// Start a client that never uses peers carried by the named FIPS
    /// transports. This prevents a provider from recursively selecting the
    /// transport that it is itself carrying while leaving every other
    /// authenticated peer eligible.
    pub async fn start_excluding_peer_transports<I, S>(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        excluded_peer_transports: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let excluded_peer_transports = excluded_peer_transports
            .into_iter()
            .map(Into::into)
            .collect();
        Self::start_for_peer_selection(endpoint, options, None, excluded_peer_transports).await
    }

    #[cfg(test)]
    async fn start_for_transport(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        peer_transport: &'static str,
    ) -> Result<Self> {
        Self::start_for_peer_selection(endpoint, options, Some(peer_transport), HashSet::new())
            .await
    }

    async fn start_for_peer_selection(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        peer_transport: Option<&'static str>,
        excluded_peer_transports: HashSet<String>,
    ) -> Result<Self> {
        options.validate()?;
        let codec = FipsPubsubWireCodec::new(options.max_frame_bytes)?;
        let subscription_limits = PubsubSubscriptionLimits {
            max_peers: options.max_connected_peers,
            max_subscriptions_per_peer: options.max_active_subscriptions,
            max_filters_per_subscription: options.max_filters_per_subscription,
        };
        let max_replay_events = options.max_replay_events;
        let max_pending_events = options
            .max_active_subscriptions
            .saturating_mul(options.max_replay_events)
            .max(1);
        let max_pending_alternatives = options.max_connected_peers;
        let max_queued_records_per_peer = options
            .max_active_subscriptions
            .saturating_add(options.max_replay_events)
            .saturating_add(1);
        let max_queued_bytes_per_peer = options
            .max_frame_bytes
            .saturating_add(4)
            .saturating_mul(max_queued_records_per_peer);
        let driver = WireTcpDriver::bind(
            Arc::clone(&endpoint),
            WireTcpOptions {
                frame_capacity: options.max_frame_bytes,
                peer_capacity: options.max_connected_peers,
                queue_records_per_peer: max_queued_records_per_peer,
                queue_bytes_per_peer: max_queued_bytes_per_peer,
                drive_io_bytes: 512 * 1_024,
                drive_frames: options.receive_batch_size,
            },
            tcp_isn_seed(endpoint.npub()),
        )
        .await?;
        let command_capacity = options
            .max_connected_peers
            .saturating_mul(options.receive_batch_size)
            .max(1);
        let (transport_tx, transport_rx) = mpsc::channel(command_capacity);
        let inner = Arc::new(ClientInner {
            endpoint: Arc::clone(&endpoint),
            codec,
            options,
            peer_transport,
            excluded_peer_transports,
            transport_tx,
            connected_transport_peers: AtomicUsize::new(0),
            req_frames_received: AtomicU64::new(0),
            close_frames_received: AtomicU64::new(0),
            event_frames_received: AtomicU64::new(0),
            inv_frames_received: AtomicU64::new(0),
            want_frames_received: AtomicU64::new(0),
            want_frames_sent: AtomicU64::new(0),
            subscription_events_received: AtomicU64::new(0),
            expired_wants: AtomicU64::new(0),
            provider_cooldowns: AtomicU64::new(0),
            tcp_receive_batches: AtomicU64::new(0),
            tcp_datagrams_received: AtomicU64::new(0),
            tcp_datagrams_rejected: AtomicU64::new(0),
            tcp_poll_turns: AtomicU64::new(0),
            next_subscription_id: AtomicU64::new(1),
            subscriptions: Mutex::new(HashMap::new()),
            peer_subscriptions: Mutex::new(PubsubPeerSubscriptionStore::new(subscription_limits)),
            recent_events: Mutex::new(RecentEvents::new(
                max_replay_events,
                FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS,
            )),
            observed_inventories: Mutex::new(seen_ids::ScopedSeenIds::new(
                FIPS_NOSTR_PUBSUB_MAX_OBSERVED_IDS_PER_SCOPE,
                FIPS_NOSTR_PUBSUB_MAX_OBSERVED_IDS,
            )),
            observed_full_events: Mutex::new(seen_ids::ScopedSeenIds::new(
                FIPS_NOSTR_PUBSUB_MAX_OBSERVED_IDS_PER_SCOPE,
                FIPS_NOSTR_PUBSUB_MAX_OBSERVED_IDS,
            )),
            provider_behavior: Mutex::new(provider_behavior::ProviderBehavior::default()),
            pending_wants: Mutex::new(PendingWants::new(
                max_pending_events,
                max_pending_alternatives,
            )),
        });
        let transport_task =
            tokio::spawn(transport_loop(Arc::downgrade(&inner), driver, transport_rx));
        Ok(Self {
            inner,
            transport_task: Some(transport_task),
        })
    }

    #[must_use]
    pub fn options(&self) -> &FipsPubsubClientOptions {
        &self.inner.options
    }

    pub fn connected_peer_count(&self) -> Result<usize> {
        Ok(self.inner.connected_transport_peers.load(Ordering::Relaxed))
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
        if let Some(task) = self.transport_task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for FipsPubsubClient {
    fn drop(&mut self) {
        self.inner.close_all();
        if let Some(task) = self.transport_task.take() {
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

        let is_new =
            self.inner
                .remember_event(event.clone(), source, self.inner.options.max_hops)?;
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
            let sent = self
                .inner
                .send_inventories(subscribed, &event, self.inner.options.max_hops);
            return publish_report(sent, peer_count, "subscribed FIPS peers");
        }

        Ok(PublishReport {
            accepted: true,
            priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
            reason: Some(format!(
                "cached for live subscriptions from {} connected FIPS peers",
                peers.len()
            )),
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

#[async_trait]
impl NostrEventSubscriber for FipsPubsubClient {
    async fn subscribe(
        &self,
        filters: Vec<Filter>,
        handler: NostrEventHandler,
    ) -> Result<Box<dyn NostrEventSubscription>> {
        let mut subscription = FipsPubsubClient::subscribe(self, filters).await?;
        let (close_sender, close_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = close_receiver => {}
                () = async {
                    while let Some(event) = subscription.recv().await {
                        handler(event);
                    }
                } => {}
            }
            subscription.close();
        });
        Ok(Box::new(FipsRoutedSubscription {
            close_sender: Some(close_sender),
            task: Some(task),
        }))
    }
}

struct FipsRoutedSubscription {
    close_sender: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Drop for FipsRoutedSubscription {
    fn drop(&mut self) {
        self.close_sender.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl NostrEventSubscription for FipsRoutedSubscription {
    async fn close(mut self: Box<Self>) -> Result<()> {
        self.close_sender.take();
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| PubsubError::Storage(format!("close FIPS live route: {error}")))?;
        }
        Ok(())
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

fn event_payload_bytes(event: &VerifiedEvent) -> Result<u32> {
    u32::try_from(event.as_event().as_json().len())
        .map_err(|_| PubsubError::Validation("Nostr event payload is too large".to_string()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn tcp_isn_seed(local_npub: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    local_npub.hash(&mut hasher);
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    hasher.finish()
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
