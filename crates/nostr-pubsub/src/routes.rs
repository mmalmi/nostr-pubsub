use nostr::Filter;

use crate::{
    EventBus, EventSource, PolicyDecision, PubsubPolicy, QueryEvent, QueryOptions, Result,
    SourceCandidate, SourceHealth, SourcePolicyContext,
};

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

fn decision_priority(decision: &PolicyDecision) -> i32 {
    match decision {
        PolicyDecision::Allow { priority } | PolicyDecision::Throttle { priority, .. } => *priority,
        PolicyDecision::Drop { .. } => 0,
    }
}
