//! Minimal in-process pubsub primitives for Nostr event routing.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use nostr::Event;
pub use nostr::filter::MatchEventOptions;
pub use nostr::{ClientMessage, Filter, PublicKey, RelayMessage, SubscriptionId};

pub const CAP_HASHTREE_FETCH: &str = "hashtree.fetch";

pub type Result<T> = std::result::Result<T, PubsubError>;

#[derive(Debug, thiserror::Error)]
pub enum PubsubError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("storage failed: {0}")]
    Storage(String),
}

#[derive(Debug, Clone)]
pub struct VerifiedEvent {
    event: Event,
}

impl VerifiedEvent {
    pub fn as_event(&self) -> &Event {
        &self.event
    }
}

impl TryFrom<Event> for VerifiedEvent {
    type Error = PubsubError;

    fn try_from(event: Event) -> Result<Self> {
        Ok(Self { event })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub String);

impl SourceId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventSourceKind {
    LocalIndex,
    Peer,
    FipsEndpoint,
    Relay,
}

impl EventSourceKind {
    #[must_use]
    pub fn default_priority(self) -> i32 {
        match self {
            Self::LocalIndex => SOURCE_PRIORITY_LOCAL_INDEX,
            Self::FipsEndpoint => SOURCE_PRIORITY_FIPS_ENDPOINT,
            Self::Peer => SOURCE_PRIORITY_PEER,
            Self::Relay => SOURCE_PRIORITY_RELAY,
        }
    }
}

pub const SOURCE_PRIORITY_LOCAL_INDEX: i32 = 300;
pub const SOURCE_PRIORITY_FIPS_ENDPOINT: i32 = 200;
pub const SOURCE_PRIORITY_PEER: i32 = 100;
pub const SOURCE_PRIORITY_RELAY: i32 = -100;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventSource {
    pub id: SourceId,
    pub kind: EventSourceKind,
    pub url: Option<String>,
}

impl EventSource {
    #[must_use]
    pub fn local_index(id: impl Into<String>) -> Self {
        Self {
            id: SourceId::new(id),
            kind: EventSourceKind::LocalIndex,
            url: None,
        }
    }

    #[must_use]
    pub fn peer(id: impl Into<String>) -> Self {
        Self {
            id: SourceId::new(id),
            kind: EventSourceKind::Peer,
            url: None,
        }
    }

    #[must_use]
    pub fn fips_endpoint(id: impl Into<String>) -> Self {
        Self {
            id: SourceId::new(id),
            kind: EventSourceKind::FipsEndpoint,
            url: None,
        }
    }

    #[must_use]
    pub fn relay(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            id: SourceId::new(url.clone()),
            kind: EventSourceKind::Relay,
            url: Some(url),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow { priority: i32 },
    Throttle { priority: i32, reason: String },
    Drop { reason: String },
}

impl PolicyDecision {
    #[must_use]
    pub fn allow_with_priority(priority: i32) -> Self {
        Self::Allow { priority }
    }

    pub fn throttle(priority: i32, reason: impl Into<String>) -> Self {
        Self::Throttle {
            priority,
            reason: reason.into(),
        }
    }

    pub fn drop(reason: impl Into<String>) -> Self {
        Self::Drop {
            reason: reason.into(),
        }
    }
}

pub struct EventPolicyContext<'a> {
    pub event: &'a VerifiedEvent,
    pub source: &'a EventSource,
}

pub struct SourcePolicyContext<'a> {
    pub candidate: &'a SourceCandidate,
    pub author_pubkey: Option<&'a str>,
    pub capabilities: &'a [String],
}

#[async_trait]
pub trait PubsubPolicy: Send + Sync {
    async fn check_event(&self, context: EventPolicyContext<'_>) -> Result<PolicyDecision>;
    async fn check_source(&self, context: SourcePolicyContext<'_>) -> Result<PolicyDecision>;
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceHealth {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCandidate {
    pub source: EventSource,
    pub priority: i32,
    pub reason: Option<String>,
    pub freshness_hint: Option<u64>,
    pub health: SourceHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReport {
    pub accepted: bool,
    pub priority: i32,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryEvent {
    pub event: VerifiedEvent,
    pub source: EventSource,
    pub priority: i32,
}

#[derive(Debug, Clone, Default)]
pub struct QueryReport {
    pub events: Vec<QueryEvent>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryOptions {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRetentionPolicy {
    pub filters: Vec<Filter>,
    pub max_events: usize,
}

impl EventRetentionPolicy {
    #[must_use]
    pub fn new(max_events: usize, filters: Vec<Filter>) -> Self {
        Self {
            filters,
            max_events,
        }
    }

    #[must_use]
    pub fn accepts(&self, event: &VerifiedEvent) -> bool {
        self.accepts_event(event.as_event())
    }

    #[must_use]
    pub fn accepts_event(&self, event: &Event) -> bool {
        self.max_events > 0 && filters_match(&self.filters, event)
    }
}

#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, event: VerifiedEvent, source: EventSource) -> Result<PublishReport>;
    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport>;
}

pub const DEFAULT_INV_WANT_HOP_LIMIT: u8 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubDeliveryStrategy {
    PushSubscribed,
    InventoryFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubPeerInterest {
    Subscribed,
    Unsubscribed,
    Unknown,
}

impl PubsubPeerInterest {
    #[must_use]
    pub fn from_filters(filters: &[Filter], event: &VerifiedEvent) -> Self {
        if subscription_filters_match(filters, event.as_event()) {
            Self::Subscribed
        } else {
            Self::Unsubscribed
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubDeliveryAction {
    PushFrame,
    AnnounceInventory,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubsubDeliveryPolicy {
    pub strategy: PubsubDeliveryStrategy,
}

impl PubsubDeliveryPolicy {
    #[must_use]
    pub const fn push_subscribed() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::PushSubscribed,
        }
    }

    #[must_use]
    pub const fn inventory_to_subscribers() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::InventoryFirst,
        }
    }

    /// Inventory-first delivery to peers with matching subscriptions.
    ///
    /// This is kept as a mesh-oriented constructor, but inventory is still
    /// gated by Nostr subscription/filter interest.
    #[must_use]
    pub const fn inventory_to_peers() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::InventoryFirst,
        }
    }

    #[must_use]
    pub fn action_for_peer(self, interest: PubsubPeerInterest) -> PubsubDeliveryAction {
        match (self.strategy, interest) {
            (PubsubDeliveryStrategy::PushSubscribed, PubsubPeerInterest::Subscribed) => {
                PubsubDeliveryAction::PushFrame
            }
            (PubsubDeliveryStrategy::InventoryFirst, PubsubPeerInterest::Subscribed) => {
                PubsubDeliveryAction::AnnounceInventory
            }
            _ => PubsubDeliveryAction::Skip,
        }
    }

    #[must_use]
    pub fn action_for_event(
        self,
        subscriptions: &PubsubPeerSubscriptionStore,
        peer_id: &SourceId,
        event: &VerifiedEvent,
    ) -> PubsubDeliveryAction {
        self.action_for_peer(subscriptions.peer_interest(peer_id, event))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubsubSubscriptionLimits {
    pub max_peers: usize,
    pub max_subscriptions_per_peer: usize,
    pub max_filters_per_subscription: usize,
}

impl Default for PubsubSubscriptionLimits {
    fn default() -> Self {
        Self {
            max_peers: 1024,
            max_subscriptions_per_peer: 64,
            max_filters_per_subscription: 16,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubPeerSubscription {
    pub subscription_id: String,
    pub filters: Vec<Filter>,
}

impl PubsubPeerSubscription {
    #[must_use]
    pub fn new(subscription_id: impl Into<String>, filters: Vec<Filter>) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            filters,
        }
    }

    #[must_use]
    pub fn matches(&self, event: &VerifiedEvent) -> bool {
        self.matches_event(event.as_event())
    }

    #[must_use]
    pub fn matches_event(&self, event: &Event) -> bool {
        subscription_filters_match(&self.filters, event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubsubSubscriptionUpdate {
    Subscribed,
    Closed,
    Ignored,
}

#[derive(Debug, Clone, Default)]
struct PeerSubscriptionSet {
    subscriptions: BTreeMap<String, PubsubPeerSubscription>,
    order: VecDeque<String>,
}

impl PeerSubscriptionSet {
    fn upsert(
        &mut self,
        subscription: PubsubPeerSubscription,
        limits: PubsubSubscriptionLimits,
    ) -> Option<PubsubPeerSubscription> {
        let subscription_id = subscription.subscription_id.clone();
        self.order.retain(|id| id != &subscription_id);
        self.order.push_back(subscription_id.clone());
        let replaced = self
            .subscriptions
            .insert(subscription_id.clone(), subscription);
        if replaced.is_none() {
            self.evict_oldest_over_limit(limits.max_subscriptions_per_peer)
        } else {
            None
        }
    }

    fn remove(&mut self, subscription_id: &str) -> Option<PubsubPeerSubscription> {
        self.order.retain(|id| id != subscription_id);
        self.subscriptions.remove(subscription_id)
    }

    fn evict_oldest_over_limit(&mut self, limit: usize) -> Option<PubsubPeerSubscription> {
        while self.subscriptions.len() > limit {
            let Some(subscription_id) = self.order.pop_front() else {
                break;
            };
            if let Some(removed) = self.subscriptions.remove(&subscription_id) {
                return Some(removed);
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PubsubPeerSubscriptionStore {
    limits: PubsubSubscriptionLimits,
    peers: BTreeMap<SourceId, PeerSubscriptionSet>,
    peer_order: VecDeque<SourceId>,
}

impl Default for PubsubPeerSubscriptionStore {
    fn default() -> Self {
        Self::new(PubsubSubscriptionLimits::default())
    }
}

impl PubsubPeerSubscriptionStore {
    #[must_use]
    pub fn new(limits: PubsubSubscriptionLimits) -> Self {
        Self {
            limits,
            peers: BTreeMap::new(),
            peer_order: VecDeque::new(),
        }
    }

    #[must_use]
    pub fn limits(&self) -> PubsubSubscriptionLimits {
        self.limits
    }

    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.peers
            .values()
            .map(|peer| peer.subscriptions.len())
            .sum()
    }

    #[must_use]
    pub fn peer_subscription_count(&self, peer_id: &SourceId) -> usize {
        self.peers
            .get(peer_id)
            .map_or(0, |peer| peer.subscriptions.len())
    }

    pub fn apply_client_message(
        &mut self,
        peer_id: SourceId,
        message: ClientMessage<'_>,
    ) -> Result<PubsubSubscriptionUpdate> {
        match message {
            ClientMessage::Req {
                subscription_id,
                filters,
            } => {
                let subscription_id = subscription_id.into_owned().to_string();
                let filters = filters
                    .into_iter()
                    .map(std::borrow::Cow::into_owned)
                    .collect::<Vec<_>>();
                self.upsert_filters(peer_id, subscription_id, filters)?;
                Ok(PubsubSubscriptionUpdate::Subscribed)
            }
            ClientMessage::Close(subscription_id) => {
                let subscription_id = subscription_id.into_owned().to_string();
                self.remove(&peer_id, &subscription_id);
                Ok(PubsubSubscriptionUpdate::Closed)
            }
            _ => Ok(PubsubSubscriptionUpdate::Ignored),
        }
    }

    pub fn upsert_filters(
        &mut self,
        peer_id: SourceId,
        subscription_id: impl Into<String>,
        filters: Vec<Filter>,
    ) -> Result<Option<PubsubPeerSubscription>> {
        let subscription = PubsubPeerSubscription::new(subscription_id, filters);
        self.upsert(peer_id, subscription)
    }

    pub fn upsert(
        &mut self,
        peer_id: SourceId,
        subscription: PubsubPeerSubscription,
    ) -> Result<Option<PubsubPeerSubscription>> {
        if self.limits.max_peers == 0 {
            return Err(PubsubError::Validation(
                "peer subscription store max_peers must be greater than zero".to_string(),
            ));
        }
        if self.limits.max_subscriptions_per_peer == 0 {
            return Err(PubsubError::Validation(
                "peer subscription store max_subscriptions_per_peer must be greater than zero"
                    .to_string(),
            ));
        }
        if subscription.filters.len() > self.limits.max_filters_per_subscription {
            return Err(PubsubError::Validation(format!(
                "subscription {} has {} filters, limit is {}",
                subscription.subscription_id,
                subscription.filters.len(),
                self.limits.max_filters_per_subscription
            )));
        }

        let is_new_peer = !self.peers.contains_key(&peer_id);
        self.touch_peer(peer_id.clone());
        if is_new_peer {
            self.evict_peers_over_limit();
        }
        let peer = self.peers.entry(peer_id).or_default();
        Ok(peer.upsert(subscription, self.limits))
    }

    pub fn remove(
        &mut self,
        peer_id: &SourceId,
        subscription_id: &str,
    ) -> Option<PubsubPeerSubscription> {
        let removed = self
            .peers
            .get_mut(peer_id)
            .and_then(|peer| peer.remove(subscription_id));
        if self
            .peers
            .get(peer_id)
            .is_some_and(PeerSubscriptionSet::is_empty)
        {
            self.remove_peer(peer_id);
        }
        removed
    }

    pub fn remove_peer(&mut self, peer_id: &SourceId) -> Vec<PubsubPeerSubscription> {
        self.peer_order.retain(|candidate| candidate != peer_id);
        self.peers
            .remove(peer_id)
            .map(|peer| peer.subscriptions.into_values().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn peer_interest(&self, peer_id: &SourceId, event: &VerifiedEvent) -> PubsubPeerInterest {
        let Some(peer) = self.peers.get(peer_id) else {
            return PubsubPeerInterest::Unknown;
        };
        if peer
            .subscriptions
            .values()
            .any(|subscription| subscription.matches(event))
        {
            PubsubPeerInterest::Subscribed
        } else {
            PubsubPeerInterest::Unsubscribed
        }
    }

    #[must_use]
    pub fn matching_subscriptions<'a>(
        &'a self,
        peer_id: &SourceId,
        event: &VerifiedEvent,
    ) -> Vec<&'a PubsubPeerSubscription> {
        self.peers
            .get(peer_id)
            .into_iter()
            .flat_map(|peer| peer.subscriptions.values())
            .filter(|subscription| subscription.matches(event))
            .collect()
    }

    #[must_use]
    pub fn interested_peers(&self, event: &VerifiedEvent) -> Vec<SourceId> {
        self.peers
            .iter()
            .filter(|(_, peer)| {
                peer.subscriptions
                    .values()
                    .any(|subscription| subscription.matches(event))
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    fn touch_peer(&mut self, peer_id: SourceId) {
        self.peer_order.retain(|candidate| candidate != &peer_id);
        self.peer_order.push_back(peer_id);
    }

    fn evict_peers_over_limit(&mut self) {
        while self.peers.len() >= self.limits.max_peers {
            let Some(peer_id) = self.peer_order.pop_front() else {
                break;
            };
            if self.peers.remove(&peer_id).is_some() {
                break;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PubsubStreamId(pub String);

impl PubsubStreamId {
    #[must_use]
    pub fn new(stream_id: impl Into<String>) -> Self {
        Self(stream_id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PubsubContentKey {
    pub stream_id: PubsubStreamId,
    pub origin: SourceId,
    pub seq: u64,
}

impl PubsubContentKey {
    #[must_use]
    pub fn new(stream_id: impl Into<String>, origin: impl Into<String>, seq: u64) -> Self {
        Self {
            stream_id: PubsubStreamId::new(stream_id),
            origin: SourceId::new(origin),
            seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubInventory {
    pub key: PubsubContentKey,
    pub payload_bytes: u64,
    pub hop_limit: u8,
}

impl PubsubInventory {
    #[must_use]
    pub fn new(key: PubsubContentKey, payload_bytes: u64, hop_limit: u8) -> Self {
        Self {
            key,
            payload_bytes,
            hop_limit,
        }
    }

    #[must_use]
    pub fn want(&self) -> PubsubWant {
        PubsubWant::new(self.key.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubWant {
    pub key: PubsubContentKey,
}

impl PubsubWant {
    #[must_use]
    pub fn new(key: PubsubContentKey) -> Self {
        Self { key }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubFrame {
    pub key: PubsubContentKey,
    pub payload: Vec<u8>,
    pub hop_limit: u8,
}

impl PubsubFrame {
    #[must_use]
    pub fn new(key: PubsubContentKey, payload: impl Into<Vec<u8>>, hop_limit: u8) -> Self {
        Self {
            key,
            payload: payload.into(),
            hop_limit,
        }
    }

    #[must_use]
    pub fn inventory(&self) -> PubsubInventory {
        PubsubInventory::new(
            self.key.clone(),
            u64::try_from(self.payload.len()).unwrap_or(u64::MAX),
            self.hop_limit,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvWantMessage {
    Inventory(PubsubInventory),
    Want(PubsubWant),
    Frame(PubsubFrame),
}

impl InvWantMessage {
    #[must_use]
    pub fn key(&self) -> &PubsubContentKey {
        match self {
            Self::Inventory(inventory) => &inventory.key,
            Self::Want(want) => &want.key,
            Self::Frame(frame) => &frame.key,
        }
    }

    #[must_use]
    pub fn stream_id(&self) -> &PubsubStreamId {
        match self {
            Self::Inventory(inventory) => &inventory.key.stream_id,
            Self::Want(want) => &want.key.stream_id,
            Self::Frame(frame) => &frame.key.stream_id,
        }
    }
}

#[async_trait]
pub trait InvWantBus: Send + Sync {
    async fn announce_inventory(
        &self,
        inventory: PubsubInventory,
        source: EventSource,
    ) -> Result<PublishReport>;

    async fn request_want(&self, want: PubsubWant, source: EventSource) -> Result<()>;

    async fn publish_frame(&self, frame: PubsubFrame, source: EventSource)
    -> Result<PublishReport>;
}

#[derive(Clone)]
struct StoredEvent {
    event: VerifiedEvent,
    source: EventSource,
    priority: i32,
}

#[derive(Clone, Default)]
pub struct InMemoryEventBus {
    events: Arc<RwLock<Vec<StoredEvent>>>,
    policy: Option<Arc<dyn PubsubPolicy>>,
}

impl InMemoryEventBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: Arc<dyn PubsubPolicy>) -> Self {
        Self {
            events: Arc::default(),
            policy: Some(policy),
        }
    }
}

#[async_trait]
impl EventBus for InMemoryEventBus {
    async fn publish(&self, event: VerifiedEvent, source: EventSource) -> Result<PublishReport> {
        let decision = if let Some(policy) = &self.policy {
            policy
                .check_event(EventPolicyContext {
                    event: &event,
                    source: &source,
                })
                .await?
        } else {
            PolicyDecision::allow_with_priority(0)
        };

        let (accepted, priority, reason) = report_parts(&decision);
        if accepted {
            self.events
                .write()
                .map_err(|_| PubsubError::Storage("event bus lock poisoned".to_string()))?
                .push(StoredEvent {
                    event,
                    source,
                    priority,
                });
        }

        Ok(PublishReport {
            accepted,
            priority,
            reason,
        })
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let limit = options.limit.or_else(|| filter_limit(&filters));
        let events = self
            .events
            .read()
            .map_err(|_| PubsubError::Storage("event bus lock poisoned".to_string()))?
            .iter()
            .filter(|stored| filters_match(&filters, stored.event.as_event()))
            .take(limit.unwrap_or(usize::MAX))
            .map(|stored| QueryEvent {
                event: stored.event.clone(),
                source: stored.source.clone(),
                priority: stored.priority,
            })
            .collect();

        Ok(QueryReport { events })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRoute {
    pub id: String,
    pub source: EventSource,
    pub priority: i32,
    pub reason: Option<String>,
    pub capabilities: Vec<String>,
}

impl SourceRoute {
    #[must_use]
    pub fn from_source(source: EventSource) -> Self {
        let id = source.id.as_str().to_string();
        Self {
            id,
            priority: source.kind.default_priority(),
            source,
            reason: None,
            capabilities: Vec::new(),
        }
    }

    #[must_use]
    pub fn local_index(id: impl Into<String>) -> Self {
        Self::from_source(EventSource::local_index(id))
    }

    #[must_use]
    pub fn peer(id: impl Into<String>) -> Self {
        Self::from_source(EventSource::peer(id))
    }

    #[must_use]
    pub fn fips_peer_default(id: impl Into<String>) -> Self {
        Self::from_source(EventSource::fips_endpoint(id))
    }

    #[must_use]
    pub fn fips_peer(id: impl Into<String>, priority: i32) -> Self {
        Self::fips_peer_default(id).with_priority(priority)
    }

    #[must_use]
    pub fn relay(url: impl Into<String>) -> Self {
        Self::from_source(EventSource::relay(url))
    }

    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    #[must_use]
    pub fn with_capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    #[must_use]
    pub fn with_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.capabilities
            .extend(capabilities.into_iter().map(Into::into));
        self
    }
}

pub struct RouteQuerySource<'a> {
    pub route: SourceRoute,
    bus: &'a dyn EventBus,
}

impl<'a> RouteQuerySource<'a> {
    pub fn new<B>(route: SourceRoute, bus: &'a B) -> Self
    where
        B: EventBus + 'a,
    {
        Self { route, bus }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RoutedQueryOptions {
    pub query: QueryOptions,
}

#[derive(Debug, Clone)]
pub struct RouteAttempt {
    pub route: SourceRoute,
    pub decision: PolicyDecision,
}

#[derive(Debug, Clone, Default)]
pub struct RoutedQueryReport {
    pub events: Vec<QueryEvent>,
    pub attempts: Vec<RouteAttempt>,
}

pub async fn query_routes_with_policy<P>(
    routes: &[RouteQuerySource<'_>],
    filters: Vec<Filter>,
    options: RoutedQueryOptions,
    author_pubkey: Option<&str>,
    policy: &P,
    capabilities: Option<&[String]>,
) -> Result<RoutedQueryReport>
where
    P: PubsubPolicy + ?Sized,
{
    let mut candidates = Vec::new();
    for route_source in routes {
        let route = &route_source.route;
        let capabilities = capabilities.unwrap_or(&route.capabilities);
        let candidate = SourceCandidate {
            source: route.source.clone(),
            priority: route.priority,
            reason: route.reason.clone(),
            freshness_hint: None,
            health: SourceHealth::default(),
        };
        let decision = policy
            .check_source(SourcePolicyContext {
                candidate: &candidate,
                author_pubkey,
                capabilities,
            })
            .await?;
        if matches!(decision, PolicyDecision::Drop { .. }) {
            continue;
        }
        let effective_priority = route.priority.saturating_add(decision_priority(&decision));
        candidates.push((effective_priority, route_source, decision));
    }

    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.0));

    let mut report = RoutedQueryReport::default();
    let limit = options.query.limit.unwrap_or(usize::MAX);
    for (_, route_source, decision) in candidates {
        if report.events.len() >= limit {
            break;
        }
        report.attempts.push(RouteAttempt {
            route: route_source.route.clone(),
            decision,
        });
        let remaining = limit.saturating_sub(report.events.len());
        let mut route_options = options.query;
        route_options.limit = Some(route_options.limit.unwrap_or(remaining).min(remaining));
        let mut route_report = route_source
            .bus
            .query(filters.clone(), route_options)
            .await?;
        report.events.append(&mut route_report.events);
    }

    Ok(report)
}

fn report_parts(decision: &PolicyDecision) -> (bool, i32, Option<String>) {
    match decision {
        PolicyDecision::Allow { priority } => (true, *priority, None),
        PolicyDecision::Throttle { priority, reason } => (true, *priority, Some(reason.clone())),
        PolicyDecision::Drop { reason } => (false, 0, Some(reason.clone())),
    }
}

fn decision_priority(decision: &PolicyDecision) -> i32 {
    match decision {
        PolicyDecision::Allow { priority } | PolicyDecision::Throttle { priority, .. } => *priority,
        PolicyDecision::Drop { .. } => 0,
    }
}

fn filters_match(filters: &[Filter], event: &Event) -> bool {
    filters.is_empty() || subscription_filters_match(filters, event)
}

fn subscription_filters_match(filters: &[Filter], event: &Event) -> bool {
    !filters.is_empty()
        && filters
            .iter()
            .any(|filter| filter.match_event(event, MatchEventOptions::new()))
}

fn filter_limit(filters: &[Filter]) -> Option<usize> {
    filters.iter().filter_map(|filter| filter.limit).min()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    #[test]
    fn retention_policy_accepts_matching_events_only() {
        let keys = Keys::generate();
        let note = signed_event(&keys, Kind::TextNote, "hello");
        let metadata = signed_event(&keys, Kind::Metadata, "{}");
        let policy = EventRetentionPolicy::new(8, vec![Filter::new().kind(Kind::TextNote)]);

        assert!(policy.accepts(&note));
        assert!(!policy.accepts(&metadata));
    }

    #[test]
    fn retention_policy_matches_generic_tag_filters() {
        let keys = Keys::generate();
        let matching = signed_event_with_tags(
            &keys,
            Kind::Custom(37195),
            "advert",
            [Tag::identifier("fips-test")],
        );
        let other_app = signed_event_with_tags(
            &keys,
            Kind::Custom(37195),
            "advert",
            [Tag::identifier("other-app")],
        );
        let policy = EventRetentionPolicy::new(
            8,
            vec![
                Filter::new()
                    .kind(Kind::Custom(37195))
                    .identifier("fips-test"),
            ],
        );

        assert!(policy.accepts(&matching));
        assert!(!policy.accepts(&other_app));
    }

    #[test]
    fn retention_policy_with_zero_capacity_stores_nothing() {
        let keys = Keys::generate();
        let event = signed_event(&keys, Kind::TextNote, "hello");
        let policy = EventRetentionPolicy::new(0, vec![Filter::new()]);

        assert!(!policy.accepts(&event));
    }

    #[test]
    fn retention_policy_without_filters_accepts_any_event_when_capacity_exists() {
        let keys = Keys::generate();
        let event = signed_event(&keys, Kind::TextNote, "hello");
        let policy = EventRetentionPolicy::new(8, Vec::new());

        assert!(policy.accepts(&event));
    }

    #[test]
    fn delivery_policy_pushes_only_to_subscribed_peers() {
        let policy = PubsubDeliveryPolicy::push_subscribed();

        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Subscribed),
            PubsubDeliveryAction::PushFrame
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unsubscribed),
            PubsubDeliveryAction::Skip
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unknown),
            PubsubDeliveryAction::Skip
        );
    }

    #[test]
    fn delivery_policy_can_inventory_only_subscribers() {
        let policy = PubsubDeliveryPolicy::inventory_to_subscribers();

        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Subscribed),
            PubsubDeliveryAction::AnnounceInventory
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unsubscribed),
            PubsubDeliveryAction::Skip
        );
    }

    #[test]
    fn delivery_policy_requires_subscription_match_before_inventory() {
        let policy = PubsubDeliveryPolicy::inventory_to_peers();

        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Subscribed),
            PubsubDeliveryAction::AnnounceInventory
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unsubscribed),
            PubsubDeliveryAction::Skip
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unknown),
            PubsubDeliveryAction::Skip
        );
    }

    #[test]
    fn peer_subscription_store_records_bounded_peer_subscriptions() {
        let mut subscriptions = PubsubPeerSubscriptionStore::new(PubsubSubscriptionLimits {
            max_peers: 2,
            max_subscriptions_per_peer: 2,
            max_filters_per_subscription: 2,
        });
        let peer_a = SourceId::new("peer-a");
        let peer_b = SourceId::new("peer-b");
        let peer_c = SourceId::new("peer-c");

        subscriptions
            .upsert_filters(
                peer_a.clone(),
                "sub-1",
                vec![Filter::new().kind(Kind::TextNote)],
            )
            .unwrap();
        subscriptions
            .upsert_filters(
                peer_a.clone(),
                "sub-2",
                vec![Filter::new().kind(Kind::Metadata)],
            )
            .unwrap();
        let evicted = subscriptions
            .upsert_filters(
                peer_a.clone(),
                "sub-3",
                vec![Filter::new().kind(Kind::EncryptedDirectMessage)],
            )
            .unwrap()
            .unwrap();

        assert_eq!(evicted.subscription_id, "sub-1");
        assert_eq!(subscriptions.peer_subscription_count(&peer_a), 2);

        subscriptions
            .upsert_filters(
                peer_b.clone(),
                "sub-1",
                vec![Filter::new().kind(Kind::TextNote)],
            )
            .unwrap();
        subscriptions
            .upsert_filters(
                peer_c.clone(),
                "sub-1",
                vec![Filter::new().kind(Kind::TextNote)],
            )
            .unwrap();

        assert_eq!(subscriptions.peer_count(), 2);
        assert_eq!(
            subscriptions.peer_interest(
                &peer_a,
                &signed_event(&Keys::generate(), Kind::TextNote, "hello")
            ),
            PubsubPeerInterest::Unknown
        );
        assert_eq!(subscriptions.peer_subscription_count(&peer_b), 1);
        assert_eq!(subscriptions.peer_subscription_count(&peer_c), 1);
    }

    #[test]
    fn peer_subscription_store_rejects_filter_spam() {
        let mut subscriptions = PubsubPeerSubscriptionStore::new(PubsubSubscriptionLimits {
            max_peers: 8,
            max_subscriptions_per_peer: 8,
            max_filters_per_subscription: 1,
        });
        let result = subscriptions.upsert_filters(
            SourceId::new("peer-a"),
            "sub-1",
            vec![
                Filter::new().kind(Kind::TextNote),
                Filter::new().kind(Kind::Metadata),
            ],
        );

        assert!(matches!(result, Err(PubsubError::Validation(_))));
    }

    #[test]
    fn peer_subscriptions_use_nostr_filter_matching() {
        let keys = Keys::generate();
        let matching = signed_event_with_tags(
            &keys,
            Kind::Custom(37195),
            "advert",
            [Tag::identifier("fips-test")],
        );
        let other = signed_event_with_tags(
            &keys,
            Kind::Custom(37195),
            "advert",
            [Tag::identifier("other-app")],
        );
        let peer_id = SourceId::new("peer-a");
        let mut subscriptions = PubsubPeerSubscriptionStore::default();
        subscriptions
            .upsert_filters(
                peer_id.clone(),
                "fips",
                vec![
                    Filter::new()
                        .kind(Kind::Custom(37195))
                        .identifier("fips-test"),
                ],
            )
            .unwrap();

        assert_eq!(
            subscriptions.peer_interest(&peer_id, &matching),
            PubsubPeerInterest::Subscribed
        );
        assert_eq!(
            subscriptions.peer_interest(&peer_id, &other),
            PubsubPeerInterest::Unsubscribed
        );
    }

    #[test]
    fn nostr_client_messages_update_peer_subscriptions() {
        let mut subscriptions = PubsubPeerSubscriptionStore::default();
        let peer_id = SourceId::new("peer-a");
        let subscription_id = SubscriptionId::new("sub-1");
        let req = ClientMessage::req(
            subscription_id.clone(),
            vec![Filter::new().kind(Kind::TextNote)],
        );
        let close = ClientMessage::close(subscription_id);

        assert_eq!(
            subscriptions
                .apply_client_message(peer_id.clone(), req)
                .unwrap(),
            PubsubSubscriptionUpdate::Subscribed
        );
        assert_eq!(subscriptions.peer_subscription_count(&peer_id), 1);
        assert_eq!(
            subscriptions
                .apply_client_message(peer_id.clone(), close)
                .unwrap(),
            PubsubSubscriptionUpdate::Closed
        );
        assert_eq!(
            subscriptions.peer_interest(
                &peer_id,
                &signed_event(&Keys::generate(), Kind::TextNote, "hello")
            ),
            PubsubPeerInterest::Unknown
        );
    }

    #[test]
    fn inventory_delivery_simulation_matches_peer_subscriptions_before_invwant() {
        let keys = Keys::generate();
        let fips_event = signed_event_with_tags(
            &keys,
            Kind::Custom(37195),
            "fips advert",
            [Tag::identifier("fips.peer")],
        );
        let hashtree_event = signed_event_with_tags(
            &keys,
            Kind::Custom(30078),
            "hashtree root",
            [Tag::identifier("hashtree.root")],
        );
        let social_event = signed_event(&keys, Kind::TextNote, "trusted status");
        let fips_peer = SourceId::new("fips-node");
        let hashtree_peer = SourceId::new("hashtree-node");
        let social_peer = SourceId::new("social-graph-node");
        let unrelated_peer = SourceId::new("unrelated-node");
        let unknown_peer = SourceId::new("unknown-node");
        let mut subscriptions = PubsubPeerSubscriptionStore::default();
        let policy = PubsubDeliveryPolicy::inventory_to_peers();

        subscriptions
            .upsert_filters(
                fips_peer.clone(),
                "fips-adverts",
                vec![
                    Filter::new()
                        .kind(Kind::Custom(37195))
                        .identifier("fips.peer"),
                ],
            )
            .unwrap();
        subscriptions
            .upsert_filters(
                hashtree_peer.clone(),
                "hashtree-roots",
                vec![
                    Filter::new()
                        .kind(Kind::Custom(30078))
                        .identifier("hashtree.root"),
                ],
            )
            .unwrap();
        subscriptions
            .upsert_filters(
                social_peer.clone(),
                "trusted-notes",
                vec![Filter::new().kind(Kind::TextNote).author(keys.public_key())],
            )
            .unwrap();
        subscriptions
            .upsert_filters(
                unrelated_peer.clone(),
                "cashu",
                vec![
                    Filter::new()
                        .kind(Kind::Custom(37195))
                        .identifier("cashu.mint"),
                ],
            )
            .unwrap();

        assert_eq!(
            subscriptions.interested_peers(&fips_event),
            vec![fips_peer.clone()]
        );
        assert_eq!(
            subscriptions.interested_peers(&hashtree_event),
            vec![hashtree_peer.clone()]
        );
        assert_eq!(
            subscriptions.interested_peers(&social_event),
            vec![social_peer.clone()]
        );
        assert_eq!(
            policy.action_for_event(&subscriptions, &fips_peer, &fips_event),
            PubsubDeliveryAction::AnnounceInventory
        );
        assert_eq!(
            policy.action_for_event(&subscriptions, &hashtree_peer, &fips_event),
            PubsubDeliveryAction::Skip
        );
        assert_eq!(
            policy.action_for_event(&subscriptions, &unknown_peer, &fips_event),
            PubsubDeliveryAction::Skip
        );
    }

    #[test]
    fn frame_inventory_and_want_share_content_key() {
        let key = PubsubContentKey::new("author:alice", "publisher-a", 7);
        let frame = PubsubFrame::new(key.clone(), b"hello".to_vec(), 4);

        let inventory = frame.inventory();
        assert_eq!(inventory.key, key);
        assert_eq!(inventory.payload_bytes, 5);
        assert_eq!(inventory.hop_limit, 4);

        let want = inventory.want();
        assert_eq!(want.key, frame.key);
    }

    #[test]
    fn protocol_messages_expose_their_content_key() {
        let key = PubsubContentKey::new("ratings:exit", "peer-a", 42);
        let inventory = PubsubInventory::new(key.clone(), 512, DEFAULT_INV_WANT_HOP_LIMIT);
        let want = PubsubWant::new(key.clone());
        let frame = PubsubFrame::new(key.clone(), vec![1, 2, 3], DEFAULT_INV_WANT_HOP_LIMIT);

        assert_eq!(InvWantMessage::Inventory(inventory).key(), &key);
        assert_eq!(InvWantMessage::Want(want).key(), &key);
        assert_eq!(InvWantMessage::Frame(frame).key(), &key);
    }

    #[test]
    fn standard_subscriptions_use_nostr_protocol_messages() {
        let subscription_id = SubscriptionId::new("author-alice");
        let req = ClientMessage::req(subscription_id.clone(), vec![Filter::new()]);
        let close = ClientMessage::close(subscription_id);

        assert!(req.is_req());
        assert!(close.is_close());
    }

    #[test]
    fn source_route_defaults_put_relay_after_local_sources() {
        let local = SourceRoute::local_index("hashtree:events");
        let fips = SourceRoute::fips_peer_default("npub1fips");
        let peer = SourceRoute::peer("npub1peer");
        let relay = SourceRoute::relay("wss://relay.example");

        assert_eq!(local.priority, SOURCE_PRIORITY_LOCAL_INDEX);
        assert_eq!(fips.priority, SOURCE_PRIORITY_FIPS_ENDPOINT);
        assert_eq!(peer.priority, SOURCE_PRIORITY_PEER);
        assert_eq!(relay.priority, SOURCE_PRIORITY_RELAY);
        assert!(local.priority > fips.priority);
        assert!(fips.priority > peer.priority);
        assert!(peer.priority > relay.priority);
    }

    #[test]
    fn route_defaults_sort_relay_last_by_priority() {
        let mut routes = [
            SourceRoute::relay("wss://relay.example"),
            SourceRoute::peer("npub1peer"),
            SourceRoute::local_index("hashtree:events"),
            SourceRoute::fips_peer_default("npub1fips"),
        ];
        routes.sort_by_key(|route| std::cmp::Reverse(route.priority));

        let attempted = routes
            .iter()
            .map(|route| route.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            attempted,
            vec![
                "hashtree:events",
                "npub1fips",
                "npub1peer",
                "wss://relay.example"
            ]
        );
    }

    #[test]
    fn route_priority_can_be_overridden_explicitly() {
        let relay = SourceRoute::relay("wss://relay.example").with_priority(400);

        assert_eq!(relay.priority, 400);
        assert_eq!(relay.source.kind, EventSourceKind::Relay);
    }

    fn signed_event(keys: &Keys, kind: Kind, content: &str) -> VerifiedEvent {
        let event = EventBuilder::new(kind, content)
            .sign_with_keys(keys)
            .unwrap();
        VerifiedEvent::try_from(event).unwrap()
    }

    fn signed_event_with_tags<I>(keys: &Keys, kind: Kind, content: &str, tags: I) -> VerifiedEvent
    where
        I: IntoIterator<Item = Tag>,
    {
        let event = EventBuilder::new(kind, content)
            .tags(tags)
            .sign_with_keys(keys)
            .unwrap();
        VerifiedEvent::try_from(event).unwrap()
    }
}
