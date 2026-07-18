use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nostr::{EventBuilder, Keys, Kind, Timestamp};
use nostr_pubsub::{
    CAP_HASHTREE_FETCH, EventBus, EventPolicyContext, EventSource, InMemoryEventBus,
    LiveRouteSource, NostrEventSubscriber, NostrPubsubRouter, PolicyDecision, PubsubPolicy,
    PubsubProvider, PubsubProviderMode, QueryOptions, Result, RoutedLiveEvent, RoutedLiveOptions,
    RoutedQueryOptions, RouterLiveSource, RouterPublishSource, RouterQuerySource,
    SourcePolicyContext, SourceRoute, VerifiedEvent, subscribe_routes_with_policy,
};

#[derive(Default)]
struct SourcePolicy;

#[async_trait]
impl PubsubPolicy for SourcePolicy {
    async fn check_event(&self, _context: EventPolicyContext<'_>) -> Result<PolicyDecision> {
        Ok(PolicyDecision::allow_with_priority(0))
    }

    async fn check_source(&self, context: SourcePolicyContext<'_>) -> Result<PolicyDecision> {
        if context.candidate.source == EventSource::relay("wss://blocked.example") {
            Ok(PolicyDecision::drop("blocked relay"))
        } else if context.candidate.source.kind == nostr_pubsub::EventSourceKind::LocalIndex
            && !context
                .capabilities
                .iter()
                .any(|capability| capability == CAP_HASHTREE_FETCH)
        {
            Ok(PolicyDecision::drop("not a Hashtree index"))
        } else {
            Ok(PolicyDecision::allow_with_priority(0))
        }
    }
}

#[tokio::test]
async fn live_routes_select_sources_deduplicate_globally_and_fan_out_close() {
    let local = InMemoryEventBus::new();
    let peer = InMemoryEventBus::new();
    let blocked_relay = InMemoryEventBus::new();
    let local_route = SourceRoute::local_index("hashtree-main").with_capability(CAP_HASHTREE_FETCH);
    let peer_route = SourceRoute::fips_peer_default("peer-a");
    let blocked_route = SourceRoute::relay("wss://blocked.example");
    let routes = [
        LiveRouteSource::new(local_route, &local),
        LiveRouteSource::new(peer_route, &peer),
        LiveRouteSource::new(blocked_route, &blocked_relay),
    ];
    let received = Arc::new(Mutex::new(Vec::<RoutedLiveEvent>::new()));
    let output = Arc::clone(&received);
    let subscription = subscribe_routes_with_policy(
        &routes,
        vec![nostr::Filter::new().kind(Kind::TextNote)],
        &SourcePolicy,
        Arc::new(move |event| output.lock().unwrap().push(event)),
        RoutedLiveOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        subscription.route_ids(),
        &["hashtree-main".to_string(), "peer-a".to_string()]
    );
    let event = signed_note("arrives through index and mesh");
    local
        .publish(event.clone(), EventSource::local_index("hashtree-main"))
        .await
        .unwrap();
    peer.publish(event.clone(), EventSource::fips_endpoint("peer-a"))
        .await
        .unwrap();
    blocked_relay
        .publish(event, EventSource::relay("wss://blocked.example"))
        .await
        .unwrap();

    {
        let delivered = received.lock().unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].route.id, "hashtree-main");
    }

    subscription.close().await.unwrap();
    local
        .publish(
            signed_note("after close"),
            EventSource::local_index("hashtree-main"),
        )
        .await
        .unwrap();
    assert_eq!(received.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn bounded_live_dedup_allows_an_evicted_event_again() {
    let local = InMemoryEventBus::new();
    let route = SourceRoute::local_index("hashtree-main").with_capability(CAP_HASHTREE_FETCH);
    let routes = [LiveRouteSource::new(route, &local)];
    let received = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::clone(&received);
    let subscription = subscribe_routes_with_policy(
        &routes,
        vec![nostr::Filter::new()],
        &SourcePolicy,
        Arc::new(move |event| output.lock().unwrap().push(event)),
        RoutedLiveOptions {
            max_seen_events: 1,
            ..RoutedLiveOptions::default()
        },
    )
    .await
    .unwrap();
    let first = signed_note("first");
    let second = signed_note("second");
    for event in [first.clone(), second, first] {
        local
            .publish(event, EventSource::local_index("hashtree-main"))
            .await
            .unwrap();
    }
    assert_eq!(received.lock().unwrap().len(), 3);
    subscription.close().await.unwrap();
}

#[tokio::test]
async fn memory_live_subscriptions_match_filters_and_stop_on_close() {
    let bus = InMemoryEventBus::new();
    let route = SourceRoute::peer("memory");
    let routes = [LiveRouteSource::new(route, &bus)];
    let count = Arc::new(Mutex::new(0_usize));
    let output = Arc::clone(&count);
    let subscription = subscribe_routes_with_policy(
        &routes,
        vec![nostr::Filter::new().kind(Kind::Metadata)],
        &SourcePolicy,
        Arc::new(move |_| *output.lock().unwrap() += 1),
        RoutedLiveOptions::default(),
    )
    .await
    .unwrap();
    bus.publish(
        signed_note("wrong kind"),
        EventSource::local_index("memory"),
    )
    .await
    .unwrap();
    assert_eq!(*count.lock().unwrap(), 0);
    subscription.close().await.unwrap();

    let report = bus
        .query(vec![nostr::Filter::new()], QueryOptions::default())
        .await
        .unwrap();
    assert_eq!(report.events.len(), 1);
}

#[tokio::test]
async fn memory_queries_apply_per_filter_limits_then_dedup_sort_and_globally_limit() {
    let bus = InMemoryEventBus::new();
    let older_note = signed_at(Kind::TextNote, "older", 10);
    let metadata = signed_at(Kind::Metadata, "{}", 20);
    let newest_note = signed_at(Kind::TextNote, "newest", 30);
    for event in [
        older_note,
        newest_note.clone(),
        metadata.clone(),
        newest_note.clone(),
    ] {
        bus.publish(event, EventSource::local_index("memory"))
            .await
            .unwrap();
    }
    let report = bus
        .query(
            vec![
                nostr::Filter::new().kind(Kind::TextNote).limit(1),
                nostr::Filter::new().kind(Kind::Metadata).limit(1),
            ],
            QueryOptions { limit: Some(2) },
        )
        .await
        .unwrap();
    assert_eq!(report.events.len(), 2);
    assert_eq!(
        report.events[0].event.as_event().id,
        newest_note.as_event().id
    );
    assert_eq!(report.events[1].event.as_event().id, metadata.as_event().id);

    let match_all = bus
        .query(Vec::new(), QueryOptions { limit: Some(1) })
        .await
        .unwrap();
    assert_eq!(
        match_all.events[0].event.as_event().id,
        newest_note.as_event().id
    );
}

#[tokio::test]
async fn owned_router_combines_query_publish_and_live_sources() {
    let hashtree = Arc::new(InMemoryEventBus::new());
    let relay = Arc::new(InMemoryEventBus::new());
    let hashtree_route = SourceRoute::local_index("hashtree-main")
        .with_capability(CAP_HASHTREE_FETCH)
        .with_dataset("local")
        .unwrap();
    let relay_route = SourceRoute::relay("wss://relay.example")
        .with_dataset("relay")
        .unwrap();
    let router = NostrPubsubRouter::new(Arc::new(SourcePolicy))
        .with_query_source(RouterQuerySource::new(
            hashtree_route.clone(),
            Arc::clone(&hashtree),
        ))
        .with_query_source(RouterQuerySource::new(
            relay_route.clone(),
            Arc::clone(&relay),
        ))
        .with_publish_source(RouterPublishSource::new(
            hashtree_route.clone(),
            Arc::clone(&hashtree),
        ))
        .with_publish_source(RouterPublishSource::new(
            relay_route.clone(),
            Arc::clone(&relay),
        ))
        .with_live_source(RouterLiveSource::new(hashtree_route, Arc::clone(&hashtree)))
        .with_live_source(RouterLiveSource::new(relay_route, Arc::clone(&relay)));
    assert_eq!(router.mode(), PubsubProviderMode::Router);

    let received = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::clone(&received);
    let subscription = NostrEventSubscriber::subscribe(
        &router,
        vec![nostr::Filter::new().kind(Kind::TextNote)],
        Arc::new(move |event| output.lock().unwrap().push(event)),
    )
    .await
    .unwrap();
    let event = signed_note("one event through every explicit route");
    let report = router
        .publish(event.clone(), EventSource::local_index("producer"))
        .await
        .unwrap();
    assert!(report.accepted);
    assert_eq!(received.lock().unwrap().len(), 1);

    let queried = router
        .query_with_context(
            vec![nostr::Filter::new().kind(Kind::TextNote)],
            RoutedQueryOptions::default(),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(queried.events.len(), 1);
    assert_eq!(queried.events[0].event, event);
    assert_eq!(queried.events[0].provenance.len(), 2);
    subscription.close().await.unwrap();
}

fn signed_note(content: &str) -> VerifiedEvent {
    let keys = Keys::generate();
    EventBuilder::new(Kind::TextNote, content)
        .sign_with_keys(&keys)
        .unwrap()
        .try_into()
        .unwrap()
}

fn signed_at(kind: Kind, content: &str, created_at: u64) -> VerifiedEvent {
    let keys = Keys::generate();
    EventBuilder::new(kind, content)
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(&keys)
        .unwrap()
        .try_into()
        .unwrap()
}
