use std::{collections::BTreeMap, fs, path::Path};

use async_trait::async_trait;
use nostr::Event;
use nostr_pubsub::{
    DEFAULT_INV_WANT_HOP_LIMIT, EventBus, EventPolicyContext, EventRetentionPolicy, Filter,
    FipsPubsubWireAdapter, FipsPubsubWireCodec, FipsPubsubWireMessage, InMemoryEventBus,
    InvWantMessage, PolicyDecision, PubsubContentKey, PubsubDeliveryAction, PubsubDeliveryPolicy,
    PubsubFrame, PubsubPeerInterest, PubsubPeerSubscriptionStore, PubsubPolicy,
    PubsubSubscriptionLimits, PubsubSubscriptionUpdate, QueryOptions, RouteQuerySource,
    RoutedQueryOptions, SourceId, SourcePolicyContext, SourceRoute, SubscriptionId, VerifiedEvent,
    query_routes_with_policy,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InteropVectors {
    events: BTreeMap<String, Event>,
    wire_cases: Vec<WireCase>,
    invalid_wire_cases: Vec<InvalidWireCase>,
    route_defaults: RouteDefaults,
    retention_cases: Vec<RetentionCase>,
    peer_subscription_case: PeerSubscriptionCase,
    inv_want_case: InvWantCase,
    routed_query_case: RoutedQueryCase,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireCase {
    name: String,
    message: WireMessageVector,
    json: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
enum WireMessageVector {
    #[serde(rename = "req")]
    Req {
        #[serde(rename = "subscriptionId")]
        subscription_id: String,
        filters: Vec<Filter>,
    },
    #[serde(rename = "close")]
    Close {
        #[serde(rename = "subscriptionId")]
        subscription_id: String,
    },
    #[serde(rename = "eose")]
    Eose {
        #[serde(rename = "subscriptionId")]
        subscription_id: String,
        #[serde(rename = "eventCount")]
        event_count: usize,
    },
    #[serde(rename = "event")]
    Event {
        #[serde(default, rename = "subscriptionId")]
        subscription_id: Option<String>,
        event: String,
    },
}

#[derive(Debug, Deserialize)]
struct InvalidWireCase {
    name: String,
    json: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RouteDefaults {
    expected_priorities: ExpectedPriorities,
    expected_order: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedPriorities {
    local_index: i32,
    fips_endpoint: i32,
    peer: i32,
    relay: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RetentionCase {
    name: String,
    policy: RetentionPolicyVector,
    event: String,
    accepts: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RetentionPolicyVector {
    max_events: usize,
    filters: Vec<Filter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeerSubscriptionCase {
    limits: SubscriptionLimitsVector,
    operations: Vec<PeerSubscriptionOperation>,
    expected_peer_count: usize,
    expected_subscription_count: usize,
    interests: Vec<InterestVector>,
    interested_peers: Vec<InterestedPeersVector>,
    delivery_actions: Vec<DeliveryActionVector>,
}

#[derive(Debug, Deserialize)]
struct SubscriptionLimitsVector {
    #[serde(rename = "maxPeers")]
    peers: usize,
    #[serde(rename = "maxSubscriptionsPerPeer")]
    subscriptions_per_peer: usize,
    #[serde(rename = "maxFiltersPerSubscription")]
    filters_per_subscription: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeerSubscriptionOperation {
    peer_id: String,
    subscription_id: String,
    filters: Vec<Filter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InterestVector {
    peer_id: String,
    event: String,
    interest: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InterestedPeersVector {
    event: String,
    peers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryActionVector {
    peer_id: String,
    event: String,
    action: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InvWantCase {
    key: ContentKeyVector,
    payload: Vec<u8>,
    hop_limit: u8,
    expected_payload_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContentKeyVector {
    stream_id: String,
    origin: String,
    seq: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutedQueryCase {
    filters: Vec<Filter>,
    limit: usize,
    routes: Vec<RoutedQueryRouteCase>,
    expected_attempts: Vec<String>,
    expected_events: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutedQueryRouteCase {
    route: RouteVector,
    events: Vec<String>,
    policy_decision: PolicyDecisionVector,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RouteVector {
    kind: String,
    id: String,
    priority: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
enum PolicyDecisionVector {
    #[serde(rename = "allow")]
    Allow { priority: i32 },
    #[serde(rename = "throttle")]
    Throttle { priority: i32, reason: String },
    #[serde(rename = "drop")]
    Drop { reason: String },
}

impl From<PolicyDecisionVector> for PolicyDecision {
    fn from(value: PolicyDecisionVector) -> Self {
        match value {
            PolicyDecisionVector::Allow { priority } => {
                PolicyDecision::allow_with_priority(priority)
            }
            PolicyDecisionVector::Throttle { priority, reason } => {
                PolicyDecision::throttle(priority, reason)
            }
            PolicyDecisionVector::Drop { reason } => PolicyDecision::drop(reason),
        }
    }
}

#[test]
fn source_route_defaults_match_typescript_vectors() {
    let vectors = load_vectors();
    let local = SourceRoute::local_index("hashtree:events");
    let fips = SourceRoute::fips_peer_default("npub1fips");
    let peer = SourceRoute::peer("npub1peer");
    let relay = SourceRoute::relay("wss://relay.example");

    assert_eq!(
        local.priority,
        vectors.route_defaults.expected_priorities.local_index
    );
    assert_eq!(
        fips.priority,
        vectors.route_defaults.expected_priorities.fips_endpoint
    );
    assert_eq!(
        peer.priority,
        vectors.route_defaults.expected_priorities.peer
    );
    assert_eq!(
        relay.priority,
        vectors.route_defaults.expected_priorities.relay
    );

    let mut routes = [relay, peer, local, fips];
    routes.sort_by_key(|route| std::cmp::Reverse(route.priority));
    let attempted = routes
        .iter()
        .map(|route| route.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(attempted, vectors.route_defaults.expected_order);
}

#[test]
fn fips_wire_codec_matches_typescript_vectors() {
    let vectors = load_vectors();
    for test_case in &vectors.wire_cases {
        let expected = wire_message_from_vector(&vectors, &test_case.message);
        let codec = FipsPubsubWireCodec::new(test_case.json.len()).unwrap();

        assert_eq!(
            codec.encode_frame(&expected).unwrap(),
            test_case.json.as_bytes(),
            "{}",
            test_case.name
        );
        assert_eq!(
            codec.decode_frame(test_case.json.as_bytes()).unwrap(),
            expected,
            "{}",
            test_case.name
        );
    }
}

#[test]
fn fips_wire_codec_rejects_unverified_and_oversized_frames() {
    let vectors = load_vectors();
    let codec = FipsPubsubWireCodec::default();
    for test_case in &vectors.invalid_wire_cases {
        assert!(
            codec.decode_frame(test_case.json.as_bytes()).is_err(),
            "{}",
            test_case.name
        );
    }

    let frame = vectors
        .wire_cases
        .iter()
        .find(|test_case| matches!(test_case.message, WireMessageVector::Event { .. }))
        .unwrap()
        .json
        .as_bytes();
    let bounded = FipsPubsubWireCodec::new(frame.len() - 1).unwrap();
    assert!(bounded.decode_frame(frame).is_err());
}

#[test]
fn fips_wire_adapter_applies_req_and_close_to_peer_subscriptions() {
    let vectors = load_vectors();
    let request = vectors
        .wire_cases
        .iter()
        .find(|test_case| matches!(test_case.message, WireMessageVector::Req { .. }))
        .unwrap();
    let close = vectors
        .wire_cases
        .iter()
        .find(|test_case| matches!(test_case.message, WireMessageVector::Close { .. }))
        .unwrap();
    let peer_id = SourceId::new("browser-fips-peer");
    let mut adapter = FipsPubsubWireAdapter::default();

    let request_result = adapter
        .decode_inbound(peer_id.clone(), request.json.as_bytes())
        .unwrap();
    assert_eq!(
        request_result.subscription_update,
        PubsubSubscriptionUpdate::Subscribed
    );
    assert_eq!(adapter.subscriptions().peer_subscription_count(&peer_id), 1);

    let close_result = adapter
        .decode_inbound(peer_id.clone(), close.json.as_bytes())
        .unwrap();
    assert_eq!(
        close_result.subscription_update,
        PubsubSubscriptionUpdate::Closed
    );
    assert_eq!(adapter.subscriptions().peer_subscription_count(&peer_id), 0);
}

#[test]
fn retention_policy_matches_typescript_vectors() {
    let vectors = load_vectors();
    for test_case in &vectors.retention_cases {
        let policy = EventRetentionPolicy::new(
            test_case.policy.max_events,
            test_case.policy.filters.clone(),
        );
        let event = verified_event(&vectors, &test_case.event);
        assert_eq!(
            policy.accepts(&event),
            test_case.accepts,
            "{}",
            test_case.name
        );
    }
}

#[test]
fn peer_subscription_delivery_matches_typescript_vectors() {
    let vectors = load_vectors();
    let test_case = &vectors.peer_subscription_case;
    let mut subscriptions = PubsubPeerSubscriptionStore::new(PubsubSubscriptionLimits {
        max_peers: test_case.limits.peers,
        max_subscriptions_per_peer: test_case.limits.subscriptions_per_peer,
        max_filters_per_subscription: test_case.limits.filters_per_subscription,
    });

    for operation in &test_case.operations {
        subscriptions
            .upsert_filters(
                nostr_pubsub::SourceId::new(operation.peer_id.clone()),
                operation.subscription_id.clone(),
                operation.filters.clone(),
            )
            .unwrap();
    }

    assert_eq!(subscriptions.peer_count(), test_case.expected_peer_count);
    assert_eq!(
        subscriptions.subscription_count(),
        test_case.expected_subscription_count
    );

    for expected in &test_case.interests {
        let event = verified_event(&vectors, &expected.event);
        assert_eq!(
            interest_name(subscriptions.peer_interest(
                &nostr_pubsub::SourceId::new(expected.peer_id.clone()),
                &event
            )),
            expected.interest
        );
    }

    for expected in &test_case.interested_peers {
        let event = verified_event(&vectors, &expected.event);
        let peers = subscriptions
            .interested_peers(&event)
            .into_iter()
            .map(|peer_id| peer_id.0)
            .collect::<Vec<_>>();
        assert_eq!(peers, expected.peers);
    }

    let delivery = PubsubDeliveryPolicy::inventory_to_peers();
    for expected in &test_case.delivery_actions {
        let event = verified_event(&vectors, &expected.event);
        assert_eq!(
            action_name(delivery.action_for_event(
                &subscriptions,
                &nostr_pubsub::SourceId::new(expected.peer_id.clone()),
                &event
            )),
            expected.action
        );
    }
}

#[test]
fn inv_want_frames_match_typescript_vectors() {
    let vectors = load_vectors();
    let test_case = vectors.inv_want_case;
    let key = PubsubContentKey::new(
        test_case.key.stream_id,
        test_case.key.origin,
        test_case.key.seq,
    );
    let frame = PubsubFrame::new(key.clone(), test_case.payload, test_case.hop_limit);
    let inventory = frame.inventory();
    let want = inventory.want();

    assert_eq!(DEFAULT_INV_WANT_HOP_LIMIT, 16);
    assert_eq!(inventory.key, key);
    assert_eq!(inventory.payload_bytes, test_case.expected_payload_bytes);
    assert_eq!(inventory.hop_limit, test_case.hop_limit);
    assert_eq!(want.key, frame.key);
    assert_eq!(InvWantMessage::Frame(frame).key(), &key);
}

#[tokio::test]
async fn routed_queries_match_typescript_vectors() {
    let vectors = load_vectors();
    let test_case = &vectors.routed_query_case;
    let policy = VectorPolicy {
        decisions: test_case
            .routes
            .iter()
            .map(|route| (route.route.id.clone(), route.policy_decision.clone().into()))
            .collect(),
    };
    let mut route_buses = Vec::new();

    for route_case in &test_case.routes {
        let route = route_from_vector(&route_case.route);
        let bus = InMemoryEventBus::new();
        for event_name in &route_case.events {
            bus.publish(verified_event(&vectors, event_name), route.source.clone())
                .await
                .unwrap();
        }
        route_buses.push((route, bus));
    }

    let routes = route_buses
        .iter()
        .map(|(route, bus)| RouteQuerySource::new(route.clone(), bus))
        .collect::<Vec<_>>();

    let report = query_routes_with_policy(
        &routes,
        test_case.filters.clone(),
        RoutedQueryOptions {
            query: QueryOptions {
                limit: Some(test_case.limit),
            },
        },
        None,
        &policy,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        report
            .attempts
            .iter()
            .map(|attempt| attempt.route.id.clone())
            .collect::<Vec<_>>(),
        test_case.expected_attempts
    );
    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.event.as_event().id.to_hex())
            .collect::<Vec<_>>(),
        test_case.expected_events
    );
}

struct VectorPolicy {
    decisions: BTreeMap<String, PolicyDecision>,
}

#[async_trait]
impl PubsubPolicy for VectorPolicy {
    async fn check_event(
        &self,
        _context: EventPolicyContext<'_>,
    ) -> nostr_pubsub::Result<PolicyDecision> {
        Ok(PolicyDecision::allow_with_priority(0))
    }

    async fn check_source(
        &self,
        context: SourcePolicyContext<'_>,
    ) -> nostr_pubsub::Result<PolicyDecision> {
        Ok(self
            .decisions
            .get(context.candidate.source.id.as_str())
            .cloned()
            .unwrap_or_else(|| PolicyDecision::allow_with_priority(0)))
    }
}

fn route_from_vector(route: &RouteVector) -> SourceRoute {
    match route.kind.as_str() {
        "relay" => SourceRoute::relay(&route.id),
        "fips" => match route.priority {
            Some(priority) => SourceRoute::fips_peer(&route.id, priority),
            None => SourceRoute::fips_peer_default(&route.id),
        },
        "peer" => SourceRoute::peer(&route.id),
        "local" => SourceRoute::local_index(&route.id),
        other => panic!("unknown route kind: {other}"),
    }
}

fn wire_message_from_vector(
    vectors: &InteropVectors,
    message: &WireMessageVector,
) -> FipsPubsubWireMessage {
    match message {
        WireMessageVector::Req {
            subscription_id,
            filters,
        } => FipsPubsubWireMessage::req(SubscriptionId::new(subscription_id), filters.clone()),
        WireMessageVector::Close { subscription_id } => {
            FipsPubsubWireMessage::close(SubscriptionId::new(subscription_id))
        }
        WireMessageVector::Eose {
            subscription_id,
            event_count,
        } => FipsPubsubWireMessage::eose(SubscriptionId::new(subscription_id), *event_count),
        WireMessageVector::Event {
            subscription_id,
            event,
        } => {
            let event = verified_event(vectors, event);
            subscription_id.as_ref().map_or_else(
                || FipsPubsubWireMessage::publish(event.clone()),
                |subscription_id| {
                    FipsPubsubWireMessage::deliver(
                        SubscriptionId::new(subscription_id),
                        event.clone(),
                    )
                },
            )
        }
    }
}

fn verified_event(vectors: &InteropVectors, name: &str) -> VerifiedEvent {
    VerifiedEvent::try_from(
        vectors
            .events
            .get(name)
            .unwrap_or_else(|| panic!("missing event fixture {name}"))
            .clone(),
    )
    .unwrap()
}

fn interest_name(interest: PubsubPeerInterest) -> &'static str {
    match interest {
        PubsubPeerInterest::Subscribed => "subscribed",
        PubsubPeerInterest::Unsubscribed => "unsubscribed",
        PubsubPeerInterest::Unknown => "unknown",
    }
}

fn action_name(action: PubsubDeliveryAction) -> &'static str {
    match action {
        PubsubDeliveryAction::PushFrame => "push-frame",
        PubsubDeliveryAction::AnnounceInventory => "announce-inventory",
        PubsubDeliveryAction::Skip => "skip",
    }
}

fn load_vectors() -> InteropVectors {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../ts/packages/nostr-pubsub/test-data/interop-vectors.json");
    let data = fs::read_to_string(path).unwrap();
    serde_json::from_str(&data).unwrap()
}
