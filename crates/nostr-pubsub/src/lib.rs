//! Minimal in-process pubsub primitives for Nostr event routing.

use std::{fmt, str::FromStr};

use async_trait::async_trait;
use nostr::Event;
pub use nostr::filter::MatchEventOptions;
pub use nostr::{ClientMessage, EventId, Filter, PublicKey, RelayMessage, SubscriptionId};

mod mesh;
mod wire;

mod inv_want;
mod memory;
mod routes;
mod subscriptions;
pub use mesh::*;
pub use wire::*;

pub use inv_want::*;
pub use memory::*;
pub use routes::*;
pub use subscriptions::*;
pub const CAP_HASHTREE_FETCH: &str = "hashtree.fetch";

pub type Result<T> = std::result::Result<T, PubsubError>;

#[derive(Debug, thiserror::Error)]
pub enum PubsubError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("storage failed: {0}")]
    Storage(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedEvent {
    event: Event,
}

impl VerifiedEvent {
    pub fn as_event(&self) -> &Event {
        &self.event
    }

    #[must_use]
    pub fn into_event(self) -> Event {
        self.event
    }
}

impl TryFrom<Event> for VerifiedEvent {
    type Error = PubsubError;

    fn try_from(event: Event) -> Result<Self> {
        event
            .verify()
            .map_err(|error| PubsubError::Validation(format!("invalid Nostr event: {error}")))?;
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

/// The explicitly selected provider class for a pubsub consumer.
///
/// Provider construction belongs to the application. The base crate does not
/// open sockets, combine providers, or fall back from one mode to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PubsubProviderMode {
    LocalOnly,
    DirectRelay,
}

impl PubsubProviderMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalOnly => "local-only",
            Self::DirectRelay => "direct-relay",
        }
    }
}

impl fmt::Display for PubsubProviderMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PubsubProviderMode {
    type Err = PubsubError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "local-only" => Ok(Self::LocalOnly),
            "direct-relay" => Ok(Self::DirectRelay),
            _ => Err(PubsubError::Validation(format!(
                "unknown pubsub provider mode {value:?}; expected local-only or direct-relay"
            ))),
        }
    }
}

/// A selected pubsub provider presented to transport-blind consumers.
pub trait PubsubProvider: EventBus {
    fn mode(&self) -> PubsubProviderMode;
}

pub const DEFAULT_INV_WANT_HOP_LIMIT: u8 = 16;
fn report_parts(decision: &PolicyDecision) -> (bool, i32, Option<String>) {
    match decision {
        PolicyDecision::Allow { priority } => (true, *priority, None),
        PolicyDecision::Throttle { priority, reason } => (true, *priority, Some(reason.clone())),
        PolicyDecision::Drop { reason } => (false, 0, Some(reason.clone())),
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
    fn provider_modes_parse_without_implicit_fallback() {
        assert_eq!(
            "local-only".parse::<PubsubProviderMode>().unwrap(),
            PubsubProviderMode::LocalOnly
        );
        assert_eq!(
            "direct-relay".parse::<PubsubProviderMode>().unwrap(),
            PubsubProviderMode::DirectRelay
        );
        assert!("relay".parse::<PubsubProviderMode>().is_err());
        assert!("".parse::<PubsubProviderMode>().is_err());
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
    fn verified_event_rejects_tampered_content() {
        let keys = Keys::generate();
        let mut event = EventBuilder::new(Kind::TextNote, "signed")
            .sign_with_keys(&keys)
            .unwrap();
        event.content = "tampered".to_string();

        assert!(VerifiedEvent::try_from(event).is_err());
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
