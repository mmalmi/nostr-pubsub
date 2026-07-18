use std::sync::Arc;

use async_trait::async_trait;
use nostr::Filter;

use crate::{
    EventBus, EventSource, LiveRouteSource, NostrEventHandler, NostrEventSubscriber,
    NostrEventSubscription, PolicyDecision, PublishReport, PubsubPolicy, PubsubProvider,
    PubsubProviderMode, QueryOptions, QueryReport, Result, RouteQuerySource, RoutedLiveOptions,
    RoutedLiveSubscription, RoutedQueryOptions, RoutedQueryReport, SourceCandidate, SourceHealth,
    SourcePolicyContext, SourceRoute, VerifiedEvent, query_routes_with_policy,
    subscribe_routes_with_policy,
};

#[derive(Clone)]
pub struct RouterQuerySource {
    pub route: SourceRoute,
    reader: Arc<dyn EventBus>,
}

impl RouterQuerySource {
    pub fn new<B>(route: SourceRoute, reader: Arc<B>) -> Self
    where
        B: EventBus + 'static,
    {
        Self { route, reader }
    }

    #[must_use]
    pub fn from_reader(route: SourceRoute, reader: Arc<dyn EventBus>) -> Self {
        Self { route, reader }
    }
}

#[derive(Clone)]
pub struct RouterPublishSource {
    pub route: SourceRoute,
    publisher: Arc<dyn EventBus>,
}

impl RouterPublishSource {
    pub fn new<B>(route: SourceRoute, publisher: Arc<B>) -> Self
    where
        B: EventBus + 'static,
    {
        Self { route, publisher }
    }

    #[must_use]
    pub fn from_publisher(route: SourceRoute, publisher: Arc<dyn EventBus>) -> Self {
        Self { route, publisher }
    }
}

#[derive(Clone)]
pub struct RouterLiveSource {
    pub route: SourceRoute,
    subscriber: Arc<dyn NostrEventSubscriber>,
}

impl RouterLiveSource {
    pub fn new<S>(route: SourceRoute, subscriber: Arc<S>) -> Self
    where
        S: NostrEventSubscriber + 'static,
    {
        Self { route, subscriber }
    }

    #[must_use]
    pub fn from_subscriber(route: SourceRoute, subscriber: Arc<dyn NostrEventSubscriber>) -> Self {
        Self { route, subscriber }
    }
}

/// Owned transport-neutral router for indexes, FIPS peers, and Nostr relays.
pub struct NostrPubsubRouter {
    policy: Arc<dyn PubsubPolicy>,
    query_sources: Vec<RouterQuerySource>,
    publish_sources: Vec<RouterPublishSource>,
    live_sources: Vec<RouterLiveSource>,
}

impl NostrPubsubRouter {
    #[must_use]
    pub fn new(policy: Arc<dyn PubsubPolicy>) -> Self {
        Self {
            policy,
            query_sources: Vec::new(),
            publish_sources: Vec::new(),
            live_sources: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_query_source(mut self, source: RouterQuerySource) -> Self {
        self.query_sources.push(source);
        self
    }

    #[must_use]
    pub fn with_publish_source(mut self, source: RouterPublishSource) -> Self {
        self.publish_sources.push(source);
        self
    }

    #[must_use]
    pub fn with_live_source(mut self, source: RouterLiveSource) -> Self {
        self.live_sources.push(source);
        self
    }

    pub async fn query_with_context(
        &self,
        filters: Vec<Filter>,
        options: RoutedQueryOptions,
        author_pubkey: Option<&str>,
        capabilities: Option<&[String]>,
    ) -> Result<RoutedQueryReport> {
        let sources = self
            .query_sources
            .iter()
            .map(|source| {
                RouteQuerySource::from_reader(source.route.clone(), source.reader.as_ref())
            })
            .collect::<Vec<_>>();
        query_routes_with_policy(
            &sources,
            filters,
            options,
            author_pubkey,
            self.policy.as_ref(),
            capabilities,
        )
        .await
    }

    pub async fn subscribe_with_options(
        &self,
        filters: Vec<Filter>,
        handler: Arc<dyn Fn(crate::RoutedLiveEvent) + Send + Sync>,
        options: RoutedLiveOptions,
    ) -> Result<RoutedLiveSubscription> {
        let sources = self
            .live_sources
            .iter()
            .map(|source| {
                LiveRouteSource::from_subscriber(source.route.clone(), source.subscriber.as_ref())
            })
            .collect::<Vec<_>>();
        subscribe_routes_with_policy(&sources, filters, self.policy.as_ref(), handler, options)
            .await
    }

    async fn publish_to_routes(
        &self,
        event: VerifiedEvent,
        source: EventSource,
    ) -> Result<PublishReport> {
        let mut accepted = 0_usize;
        let mut attempted = 0_usize;
        let mut priority = i32::MIN;
        let mut failures = Vec::new();
        for target in &self.publish_sources {
            let candidate = SourceCandidate {
                source: target.route.source.clone(),
                priority: target.route.priority,
                reason: target.route.reason.clone(),
                freshness_hint: None,
                health: SourceHealth::default(),
            };
            let decision = self
                .policy
                .check_source(SourcePolicyContext {
                    candidate: &candidate,
                    author_pubkey: None,
                    capabilities: &target.route.capabilities,
                })
                .await?;
            if matches!(decision, PolicyDecision::Drop { .. }) {
                continue;
            }
            attempted += 1;
            match target
                .publisher
                .publish(event.clone(), source.clone())
                .await
            {
                Ok(report) if report.accepted => {
                    accepted += 1;
                    priority = priority.max(report.priority);
                }
                Ok(report) => failures.push(format!(
                    "{}: {}",
                    target.route.id,
                    report.reason.unwrap_or_else(|| "rejected".to_string())
                )),
                Err(error) => failures.push(format!("{}: {error}", target.route.id)),
            }
        }
        let reason = if failures.is_empty() {
            None
        } else {
            Some(failures.join("; "))
        };
        Ok(PublishReport {
            accepted: accepted > 0,
            priority: if accepted > 0 { priority } else { 0 },
            reason: reason
                .or_else(|| (attempted == 0).then(|| "no publish route was selected".to_string())),
        })
    }
}

#[async_trait]
impl EventBus for NostrPubsubRouter {
    async fn publish(&self, event: VerifiedEvent, source: EventSource) -> Result<PublishReport> {
        self.publish_to_routes(event, source).await
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let report = self
            .query_with_context(filters, RoutedQueryOptions { query: options }, None, None)
            .await?;
        Ok(QueryReport {
            events: report
                .events
                .into_iter()
                .map(|event| crate::QueryEvent {
                    event: event.event,
                    source: event.source,
                    priority: event.priority,
                })
                .collect(),
        })
    }
}

impl PubsubProvider for NostrPubsubRouter {
    fn mode(&self) -> PubsubProviderMode {
        PubsubProviderMode::Router
    }
}

struct RouterEventSubscription {
    inner: Option<RoutedLiveSubscription>,
}

#[async_trait]
impl NostrEventSubscription for RouterEventSubscription {
    async fn close(mut self: Box<Self>) -> Result<()> {
        if let Some(subscription) = self.inner.take() {
            subscription.close().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl NostrEventSubscriber for NostrPubsubRouter {
    async fn subscribe(
        &self,
        filters: Vec<Filter>,
        handler: NostrEventHandler,
    ) -> Result<Box<dyn NostrEventSubscription>> {
        let subscription = self
            .subscribe_with_options(
                filters,
                Arc::new(move |event| {
                    handler(crate::QueryEvent {
                        event: event.event,
                        source: event.source,
                        priority: event.priority,
                    });
                }),
                RoutedLiveOptions::default(),
            )
            .await?;
        Ok(Box::new(RouterEventSubscription {
            inner: Some(subscription),
        }))
    }
}
