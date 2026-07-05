//! Minimal in-process pubsub primitives for Nostr event routing.

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use nostr::Event;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubDeliveryAction {
    PushFrame,
    AnnounceInventory,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubsubDeliveryPolicy {
    pub strategy: PubsubDeliveryStrategy,
    pub announce_inventory_without_subscription: bool,
}

impl PubsubDeliveryPolicy {
    #[must_use]
    pub const fn push_subscribed() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::PushSubscribed,
            announce_inventory_without_subscription: false,
        }
    }

    #[must_use]
    pub const fn inventory_to_subscribers() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::InventoryFirst,
            announce_inventory_without_subscription: false,
        }
    }

    #[must_use]
    pub const fn inventory_to_peers() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::InventoryFirst,
            announce_inventory_without_subscription: true,
        }
    }

    #[must_use]
    pub fn action_for_peer(self, interest: PubsubPeerInterest) -> PubsubDeliveryAction {
        match (
            self.strategy,
            interest,
            self.announce_inventory_without_subscription,
        ) {
            (PubsubDeliveryStrategy::PushSubscribed, PubsubPeerInterest::Subscribed, _) => {
                PubsubDeliveryAction::PushFrame
            }
            (PubsubDeliveryStrategy::InventoryFirst, PubsubPeerInterest::Subscribed, _)
            | (PubsubDeliveryStrategy::InventoryFirst, _, true) => {
                PubsubDeliveryAction::AnnounceInventory
            }
            _ => PubsubDeliveryAction::Skip,
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
    filters.is_empty() || filters.iter().any(|filter| filter_matches(filter, event))
}

fn filter_matches(filter: &Filter, event: &Event) -> bool {
    if filter
        .ids
        .as_ref()
        .is_some_and(|ids| !ids.contains(&event.id))
    {
        return false;
    }
    if filter
        .authors
        .as_ref()
        .is_some_and(|authors| !authors.contains(&event.pubkey))
    {
        return false;
    }
    if filter
        .kinds
        .as_ref()
        .is_some_and(|kinds| !kinds.contains(&event.kind))
    {
        return false;
    }
    if filter.since.is_some_and(|since| event.created_at < since) {
        return false;
    }
    if filter.until.is_some_and(|until| event.created_at > until) {
        return false;
    }
    if !filter.generic_tags.is_empty() && !filter_generic_tags_match(filter, event) {
        return false;
    }
    true
}

fn filter_generic_tags_match(filter: &Filter, event: &Event) -> bool {
    filter
        .generic_tags
        .iter()
        .all(|(tag_name, accepted_values)| {
            let tag_name = tag_name.as_char().to_string();
            event.tags.iter().any(|tag| {
                let parts = tag.as_slice();
                parts
                    .first()
                    .is_some_and(|candidate| candidate == &tag_name)
                    && parts
                        .get(1)
                        .is_some_and(|value| accepted_values.contains(value))
            })
        })
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
    fn delivery_policy_can_inventory_unknown_peers_for_mesh_relaying() {
        let policy = PubsubDeliveryPolicy::inventory_to_peers();

        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Subscribed),
            PubsubDeliveryAction::AnnounceInventory
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unsubscribed),
            PubsubDeliveryAction::AnnounceInventory
        );
        assert_eq!(
            policy.action_for_peer(PubsubPeerInterest::Unknown),
            PubsubDeliveryAction::AnnounceInventory
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
