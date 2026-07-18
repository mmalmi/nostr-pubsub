use std::{collections::HashMap, future::Future, pin::Pin, task::Poll};

use nostr::Filter;

use crate::{
    EventBus, EventSource, PolicyDecision, PubsubError, PubsubPolicy, QueryEvent, QueryOptions,
    Result, SourceCandidate, SourceHealth, SourcePolicyContext, VerifiedEvent,
};

pub const DEFAULT_ROUTE_DATASET_ID: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRoute {
    pub id: String,
    /// Routes with the same dataset ID are ordered replicas. Different IDs are additive.
    pub dataset_id: String,
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
            dataset_id: DEFAULT_ROUTE_DATASET_ID.to_string(),
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

    pub fn with_dataset(mut self, dataset_id: impl Into<String>) -> Result<Self> {
        let dataset_id = dataset_id.into();
        if dataset_id.is_empty() {
            return Err(PubsubError::Validation(
                "route dataset identity must not be empty".to_string(),
            ));
        }
        self.dataset_id = dataset_id;
        Ok(self)
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
    reader: &'a dyn EventBus,
}

impl<'a> RouteQuerySource<'a> {
    pub fn new<B>(route: SourceRoute, reader: &'a B) -> Self
    where
        B: EventBus + 'a,
    {
        Self { route, reader }
    }

    #[must_use]
    pub fn from_reader(route: SourceRoute, reader: &'a dyn EventBus) -> Self {
        Self { route, reader }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RoutedQueryOptions {
    pub query: QueryOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteAttemptOutcome {
    Success { event_count: usize },
    Failure { message: String },
}

#[derive(Debug, Clone)]
pub struct RouteAttempt {
    pub route: SourceRoute,
    pub dataset_id: String,
    pub decision: PolicyDecision,
    pub outcome: RouteAttemptOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedEventProvenance {
    pub route_id: String,
    pub dataset_id: String,
    pub source: EventSource,
    pub priority: i32,
}

#[derive(Debug, Clone)]
pub struct RoutedQueryEvent {
    pub event: VerifiedEvent,
    pub source: EventSource,
    pub priority: i32,
    pub provenance: Vec<RoutedEventProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedDatasetReport {
    pub dataset_id: String,
    pub complete: bool,
    pub event_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct RoutedQueryReport {
    pub events: Vec<RoutedQueryEvent>,
    pub attempts: Vec<RouteAttempt>,
    pub datasets: Vec<RoutedDatasetReport>,
    pub complete: bool,
}

struct RouteCandidate<'a> {
    effective_priority: i32,
    ordinal: usize,
    source: &'a RouteQuerySource<'a>,
    decision: PolicyDecision,
}

struct DatasetResult {
    report: RoutedDatasetReport,
    attempts: Vec<RouteAttempt>,
    events: Vec<RoutedQueryEvent>,
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
    let candidates = allowed_candidates(routes, author_pubkey, policy, capabilities).await?;
    let groups = group_by_dataset(candidates);
    let limit = options.query.limit.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(RoutedQueryReport {
            datasets: groups
                .into_iter()
                .map(|(dataset_id, _)| RoutedDatasetReport {
                    dataset_id,
                    complete: true,
                    event_count: 0,
                })
                .collect(),
            complete: true,
            ..RoutedQueryReport::default()
        });
    }

    let futures = groups
        .into_iter()
        .map(|(dataset_id, replicas)| query_dataset(dataset_id, replicas, &filters, options.query));
    let results = join_concurrently(futures.collect()).await;
    let mut report = RoutedQueryReport {
        complete: results.iter().all(|result| result.report.complete),
        ..RoutedQueryReport::default()
    };
    for result in results {
        report.attempts.extend(result.attempts);
        report.datasets.push(result.report);
        report.events.extend(result.events);
    }
    report.events = merge_routed_events(report.events);
    report.events.truncate(limit);
    Ok(report)
}

async fn allowed_candidates<'a, P>(
    routes: &'a [RouteQuerySource<'a>],
    author_pubkey: Option<&str>,
    policy: &P,
    capabilities: Option<&[String]>,
) -> Result<Vec<RouteCandidate<'a>>>
where
    P: PubsubPolicy + ?Sized,
{
    let mut candidates = Vec::new();
    for (ordinal, route_source) in routes.iter().enumerate() {
        let route = &route_source.route;
        let route_capabilities = capabilities.unwrap_or(&route.capabilities);
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
                capabilities: route_capabilities,
            })
            .await?;
        if matches!(decision, PolicyDecision::Drop { .. }) {
            continue;
        }
        candidates.push(RouteCandidate {
            effective_priority: route.priority.saturating_add(decision_priority(&decision)),
            ordinal,
            source: route_source,
            decision,
        });
    }
    candidates.sort_by_key(|candidate| {
        (
            std::cmp::Reverse(candidate.effective_priority),
            candidate.ordinal,
        )
    });
    Ok(candidates)
}

fn group_by_dataset<'a>(
    candidates: Vec<RouteCandidate<'a>>,
) -> Vec<(String, Vec<RouteCandidate<'a>>)> {
    let mut groups: Vec<(String, Vec<RouteCandidate<'a>>)> = Vec::new();
    for candidate in candidates {
        let dataset_id = &candidate.source.route.dataset_id;
        if let Some((_, replicas)) = groups.iter_mut().find(|(id, _)| id == dataset_id) {
            replicas.push(candidate);
        } else {
            groups.push((dataset_id.clone(), vec![candidate]));
        }
    }
    groups
}

async fn query_dataset(
    dataset_id: String,
    replicas: Vec<RouteCandidate<'_>>,
    filters: &[Filter],
    options: QueryOptions,
) -> DatasetResult {
    let mut attempts = Vec::new();
    for replica in replicas {
        let result = replica.source.reader.query(filters.to_vec(), options).await;
        match result {
            Ok(report) => {
                let events = merge_routed_events(
                    report
                        .events
                        .into_iter()
                        .map(|event| routed_event(event, &replica, &dataset_id))
                        .collect(),
                );
                attempts.push(RouteAttempt {
                    route: replica.source.route.clone(),
                    dataset_id: dataset_id.clone(),
                    decision: replica.decision,
                    outcome: RouteAttemptOutcome::Success {
                        event_count: events.len(),
                    },
                });
                return DatasetResult {
                    report: RoutedDatasetReport {
                        dataset_id,
                        complete: true,
                        event_count: events.len(),
                    },
                    attempts,
                    events,
                };
            }
            Err(error) => attempts.push(RouteAttempt {
                route: replica.source.route.clone(),
                dataset_id: dataset_id.clone(),
                decision: replica.decision,
                outcome: RouteAttemptOutcome::Failure {
                    message: error.to_string(),
                },
            }),
        }
    }

    DatasetResult {
        report: RoutedDatasetReport {
            dataset_id,
            complete: false,
            event_count: 0,
        },
        attempts,
        events: Vec::new(),
    }
}

fn routed_event(
    event: QueryEvent,
    candidate: &RouteCandidate<'_>,
    dataset_id: &str,
) -> RoutedQueryEvent {
    RoutedQueryEvent {
        event: event.event,
        source: event.source.clone(),
        priority: event.priority,
        provenance: vec![RoutedEventProvenance {
            route_id: candidate.source.route.id.clone(),
            dataset_id: dataset_id.to_string(),
            source: event.source,
            priority: event.priority,
        }],
    }
}

fn merge_routed_events(events: Vec<RoutedQueryEvent>) -> Vec<RoutedQueryEvent> {
    let mut merged: Vec<RoutedQueryEvent> = Vec::new();
    let mut indices: HashMap<nostr::EventId, usize> = HashMap::new();
    for event in events {
        let event_id = event.event.as_event().id;
        if let Some(index) = indices.get(&event_id).copied() {
            merged[index].provenance.extend(event.provenance);
        } else {
            indices.insert(event_id, merged.len());
            merged.push(event);
        }
    }
    for event in &mut merged {
        event.provenance.sort_by(|left, right| {
            (
                &left.dataset_id,
                &left.route_id,
                source_kind_rank(left.source.kind),
                left.source.id.as_str(),
                left.source.url.as_deref(),
                left.priority,
            )
                .cmp(&(
                    &right.dataset_id,
                    &right.route_id,
                    source_kind_rank(right.source.kind),
                    right.source.id.as_str(),
                    right.source.url.as_deref(),
                    right.priority,
                ))
        });
        event.provenance.dedup();
    }
    merged.sort_by(|left, right| {
        right
            .event
            .as_event()
            .created_at
            .cmp(&left.event.as_event().created_at)
            .then_with(|| {
                left.event
                    .as_event()
                    .id
                    .to_hex()
                    .cmp(&right.event.as_event().id.to_hex())
            })
    });
    merged
}

const fn source_kind_rank(kind: crate::EventSourceKind) -> u8 {
    match kind {
        crate::EventSourceKind::LocalIndex => 0,
        crate::EventSourceKind::FipsEndpoint => 1,
        crate::EventSourceKind::Peer => 2,
        crate::EventSourceKind::Relay => 3,
    }
}

async fn join_concurrently<F>(futures: Vec<F>) -> Vec<F::Output>
where
    F: Future,
{
    let mut pending = futures
        .into_iter()
        .map(|future| Some(Box::pin(future)))
        .collect::<Vec<Option<Pin<Box<F>>>>>();
    let mut outputs = std::iter::repeat_with(|| None)
        .take(pending.len())
        .collect::<Vec<Option<F::Output>>>();
    std::future::poll_fn(move |context| {
        let mut waiting = false;
        for (index, future) in pending.iter_mut().enumerate() {
            let Some(active) = future else {
                continue;
            };
            match active.as_mut().poll(context) {
                Poll::Ready(output) => {
                    outputs[index] = Some(output);
                    *future = None;
                }
                Poll::Pending => waiting = true,
            }
        }
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(
                outputs
                    .iter_mut()
                    .map(|output| output.take().expect("completed future has output"))
                    .collect(),
            )
        }
    })
    .await
}

const fn decision_priority(decision: &PolicyDecision) -> i32 {
    match decision {
        PolicyDecision::Allow { priority } | PolicyDecision::Throttle { priority, .. } => *priority,
        PolicyDecision::Drop { .. } => 0,
    }
}
