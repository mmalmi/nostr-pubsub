use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use nostr::{EventBuilder, Filter, Keys, Kind, Timestamp};
use nostr_pubsub::{
    EventBus, EventSource, PolicyDecision, PubsubError, PubsubPolicy, QueryEvent, QueryOptions,
    QueryReport, RouteAttemptOutcome, RouteQuerySource, RoutedQueryOptions, SourcePolicyContext,
    SourceRoute, VerifiedEvent, query_routes_with_policy,
};
use tokio::sync::Barrier;
use tokio::time::{Duration, timeout};

struct AllowAll;

#[async_trait]
impl PubsubPolicy for AllowAll {
    async fn check_event(
        &self,
        _context: nostr_pubsub::EventPolicyContext<'_>,
    ) -> nostr_pubsub::Result<PolicyDecision> {
        Ok(PolicyDecision::allow_with_priority(0))
    }

    async fn check_source(
        &self,
        _context: SourcePolicyContext<'_>,
    ) -> nostr_pubsub::Result<PolicyDecision> {
        Ok(PolicyDecision::allow_with_priority(0))
    }
}

struct Reader {
    result: ReaderResult,
    calls: Arc<AtomicUsize>,
    barrier: Option<Arc<Barrier>>,
}

enum ReaderResult {
    Events(Vec<QueryEvent>),
    Failure(String),
}

impl Reader {
    fn events(events: Vec<QueryEvent>) -> Self {
        Self {
            result: ReaderResult::Events(events),
            calls: Arc::new(AtomicUsize::new(0)),
            barrier: None,
        }
    }

    fn failure(message: &str) -> Self {
        Self {
            result: ReaderResult::Failure(message.to_string()),
            calls: Arc::new(AtomicUsize::new(0)),
            barrier: None,
        }
    }

    fn with_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.barrier = Some(barrier);
        self
    }
}

#[async_trait]
impl EventBus for Reader {
    async fn publish(
        &self,
        _event: VerifiedEvent,
        _source: EventSource,
    ) -> nostr_pubsub::Result<nostr_pubsub::PublishReport> {
        unreachable!("test reader is read-only")
    }

    async fn query(
        &self,
        _filters: Vec<Filter>,
        _options: QueryOptions,
    ) -> nostr_pubsub::Result<QueryReport> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        match &self.result {
            ReaderResult::Events(events) => Ok(QueryReport {
                events: events.clone(),
            }),
            ReaderResult::Failure(message) => Err(PubsubError::Storage(message.clone())),
        }
    }
}

#[tokio::test]
async fn additive_datasets_query_concurrently_and_merge_newest_first() {
    let barrier = Arc::new(Barrier::new(2));
    let old = event(10, "old");
    let new = event(20, "new");
    let first = Reader::events(vec![query_event(old.clone(), "hashtree")])
        .with_barrier(Arc::clone(&barrier));
    let second = Reader::events(vec![query_event(new.clone(), "relay")]).with_barrier(barrier);
    let routes = [
        RouteQuerySource::new(
            dataset(SourceRoute::local_index("hashtree"), "archive"),
            &first,
        ),
        RouteQuerySource::new(
            dataset(SourceRoute::relay("wss://relay.example"), "relay"),
            &second,
        ),
    ];

    let report = timeout(
        Duration::from_secs(1),
        query_routes_with_policy(
            &routes,
            vec![Filter::new().kind(Kind::TextNote)],
            RoutedQueryOptions::default(),
            None,
            &AllowAll,
            None,
        ),
    )
    .await
    .expect("additive datasets must run concurrently")
    .expect("routed query");

    assert_eq!(
        report
            .events
            .iter()
            .map(|event| event.event.as_event().id)
            .collect::<Vec<_>>(),
        vec![new.as_event().id, old.as_event().id]
    );
    assert_eq!(report.datasets.len(), 2);
    assert!(report.complete);
}

#[tokio::test]
async fn replicas_fail_over_and_one_failed_dataset_does_not_hide_healthy_results() {
    let available = event(10, "available replica");
    let primary = Reader::failure("primary unavailable");
    let replica = Reader::events(vec![query_event(available.clone(), "replica")]);
    let broken = Reader::failure("archive unavailable");
    let routes = [
        RouteQuerySource::new(
            dataset(SourceRoute::local_index("primary"), "shared"),
            &primary,
        ),
        RouteQuerySource::new(
            dataset(SourceRoute::local_index("replica"), "shared"),
            &replica,
        ),
        RouteQuerySource::new(
            dataset(SourceRoute::relay("wss://broken.example"), "broken"),
            &broken,
        ),
    ];

    let report = query_routes_with_policy(
        &routes,
        vec![Filter::new()],
        RoutedQueryOptions::default(),
        None,
        &AllowAll,
        None,
    )
    .await
    .expect("backend failures are reported, not returned as router errors");

    assert_eq!(report.events.len(), 1);
    assert_eq!(report.events[0].event, available);
    assert_eq!(
        report
            .attempts
            .iter()
            .map(|attempt| (&*attempt.route.id, &attempt.outcome))
            .collect::<Vec<_>>(),
        vec![
            (
                "primary",
                &RouteAttemptOutcome::Failure {
                    message: "storage failed: primary unavailable".to_string()
                }
            ),
            ("replica", &RouteAttemptOutcome::Success { event_count: 1 }),
            (
                "wss://broken.example",
                &RouteAttemptOutcome::Failure {
                    message: "storage failed: archive unavailable".to_string()
                }
            ),
        ]
    );
    assert!(!report.complete);
    assert_eq!(
        report
            .datasets
            .iter()
            .map(|dataset| (&*dataset.dataset_id, dataset.complete))
            .collect::<Vec<_>>(),
        vec![("shared", true), ("broken", false)]
    );
}

#[tokio::test]
async fn routed_results_deduplicate_ids_preserve_provenance_and_limit_after_merge() {
    let duplicate = event(10, "duplicate");
    let newest = event(30, "newest");
    let hashtree = Reader::events(vec![
        query_event(duplicate.clone(), "hashtree"),
        query_event(newest.clone(), "hashtree"),
    ]);
    let relay = Reader::events(vec![query_event(duplicate.clone(), "relay")]);
    let routes = [
        RouteQuerySource::new(
            dataset(SourceRoute::local_index("hashtree"), "local"),
            &hashtree,
        ),
        RouteQuerySource::new(
            dataset(SourceRoute::relay("wss://relay.example"), "remote"),
            &relay,
        ),
    ];

    let full = query_routes_with_policy(
        &routes,
        vec![Filter::new()],
        RoutedQueryOptions::default(),
        None,
        &AllowAll,
        None,
    )
    .await
    .expect("routed query");

    assert_eq!(full.events.len(), 2);
    assert_eq!(full.events[0].event, newest);
    let merged = full
        .events
        .iter()
        .find(|event| event.event == duplicate)
        .expect("deduplicated event");
    assert_eq!(merged.provenance.len(), 2);
    assert_eq!(
        merged
            .provenance
            .iter()
            .map(|provenance| &*provenance.dataset_id)
            .collect::<Vec<_>>(),
        vec!["local", "remote"]
    );

    let limited = query_routes_with_policy(
        &routes,
        vec![Filter::new()],
        RoutedQueryOptions {
            query: QueryOptions { limit: Some(1) },
        },
        None,
        &AllowAll,
        None,
    )
    .await
    .expect("limited routed query");
    assert_eq!(limited.events.len(), 1);
    assert_eq!(limited.events[0].event, newest);
}

#[tokio::test]
async fn a_valid_empty_replica_completes_its_dataset() {
    let empty = Reader::events(Vec::new());
    let should_not_run = Reader::failure("must not run after complete empty result");
    let routes = [
        RouteQuerySource::new(dataset(SourceRoute::local_index("empty"), "shared"), &empty),
        RouteQuerySource::new(
            dataset(SourceRoute::local_index("fallback"), "shared"),
            &should_not_run,
        ),
    ];

    let report = query_routes_with_policy(
        &routes,
        vec![Filter::new()],
        RoutedQueryOptions::default(),
        None,
        &AllowAll,
        None,
    )
    .await
    .expect("empty result is complete");

    assert!(report.complete);
    assert!(report.events.is_empty());
    assert_eq!(empty.calls.load(Ordering::Relaxed), 1);
    assert_eq!(should_not_run.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn route_dataset_identity_cannot_be_empty() {
    assert!(SourceRoute::local_index("index").with_dataset("").is_err());
}

fn dataset(route: SourceRoute, dataset_id: &str) -> SourceRoute {
    route.with_dataset(dataset_id).expect("valid dataset")
}

fn query_event(event: VerifiedEvent, source: &str) -> QueryEvent {
    QueryEvent {
        event,
        source: EventSource::local_index(source),
        priority: 0,
    }
}

fn event(created_at: u64, content: &str) -> VerifiedEvent {
    VerifiedEvent::try_from(
        EventBuilder::text_note(content)
            .custom_created_at(Timestamp::from(created_at))
            .sign_with_keys(&Keys::generate())
            .expect("sign event"),
    )
    .expect("verify event")
}
