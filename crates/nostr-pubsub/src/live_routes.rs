use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use nostr::Filter;

use crate::{
    EventSource, PolicyDecision, PubsubError, PubsubPolicy, QueryEvent, Result, SourceCandidate,
    SourceHealth, SourcePolicyContext, SourceRoute, VerifiedEvent,
};

pub const DEFAULT_LIVE_DEDUP_EVENTS: usize = 4_096;

pub type NostrEventHandler = Arc<dyn Fn(QueryEvent) + Send + Sync>;

#[async_trait]
pub trait NostrEventSubscription: Send {
    async fn close(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub trait NostrEventSubscriber: Send + Sync {
    async fn subscribe(
        &self,
        filters: Vec<Filter>,
        handler: NostrEventHandler,
    ) -> Result<Box<dyn NostrEventSubscription>>;
}

pub struct LiveRouteSource<'a> {
    pub route: SourceRoute,
    subscriber: &'a dyn NostrEventSubscriber,
}

impl<'a> LiveRouteSource<'a> {
    #[must_use]
    pub fn new<S>(route: SourceRoute, subscriber: &'a S) -> Self
    where
        S: NostrEventSubscriber + 'a,
    {
        Self { route, subscriber }
    }

    #[must_use]
    pub fn from_subscriber(route: SourceRoute, subscriber: &'a dyn NostrEventSubscriber) -> Self {
        Self { route, subscriber }
    }
}

#[derive(Debug, Clone)]
pub struct RoutedLiveEvent {
    pub event: VerifiedEvent,
    pub source: EventSource,
    pub priority: i32,
    pub route: SourceRoute,
}

#[derive(Debug, Clone)]
pub struct RoutedLiveOptions {
    pub author_pubkey: Option<String>,
    pub capabilities: Option<Vec<String>>,
    pub max_seen_events: usize,
}

impl Default for RoutedLiveOptions {
    fn default() -> Self {
        Self {
            author_pubkey: None,
            capabilities: None,
            max_seen_events: DEFAULT_LIVE_DEDUP_EVENTS,
        }
    }
}

pub struct RoutedLiveSubscription {
    route_ids: Vec<String>,
    subscriptions: Vec<Box<dyn NostrEventSubscription>>,
    seen: Arc<Mutex<SeenEvents>>,
}

impl RoutedLiveSubscription {
    #[must_use]
    pub fn route_ids(&self) -> &[String] {
        &self.route_ids
    }

    pub async fn close(mut self) -> Result<()> {
        let result = close_subscriptions(self.subscriptions.drain(..)).await;
        if let Ok(mut seen) = self.seen.lock() {
            seen.clear();
        }
        result
    }
}

pub async fn subscribe_routes_with_policy<P>(
    routes: &[LiveRouteSource<'_>],
    filters: Vec<Filter>,
    policy: &P,
    handler: Arc<dyn Fn(RoutedLiveEvent) + Send + Sync>,
    options: RoutedLiveOptions,
) -> Result<RoutedLiveSubscription>
where
    P: PubsubPolicy + ?Sized,
{
    if options.max_seen_events == 0 {
        return Err(PubsubError::Validation(
            "live route deduplication limit must be positive".to_string(),
        ));
    }

    let seen = Arc::new(Mutex::new(SeenEvents::new(options.max_seen_events)));
    let mut allowed = Vec::new();
    for source in routes {
        let route = &source.route;
        let candidate = SourceCandidate {
            source: route.source.clone(),
            priority: route.priority,
            reason: route.reason.clone(),
            freshness_hint: None,
            health: SourceHealth::default(),
        };
        let capabilities = options
            .capabilities
            .as_deref()
            .unwrap_or(&route.capabilities);
        let decision = policy
            .check_source(SourcePolicyContext {
                candidate: &candidate,
                author_pubkey: options.author_pubkey.as_deref(),
                capabilities,
            })
            .await?;
        if matches!(decision, PolicyDecision::Drop { .. }) {
            continue;
        }
        allowed.push(source);
    }

    let mut active = Vec::new();
    let mut route_ids = Vec::new();
    for source in allowed {
        let route = &source.route;
        let route = route.clone();
        let route_id = route.id.clone();
        let route_for_handler = route.clone();
        let seen_for_handler = Arc::clone(&seen);
        let user_handler = Arc::clone(&handler);
        let incoming_handler: NostrEventHandler = Arc::new(move |incoming| {
            let event_id = incoming.event.as_event().id;
            let should_deliver = seen_for_handler
                .lock()
                .is_ok_and(|mut seen| seen.insert(event_id));
            if should_deliver {
                user_handler(RoutedLiveEvent {
                    event: incoming.event,
                    source: incoming.source,
                    priority: incoming.priority,
                    route: route_for_handler.clone(),
                });
            }
        });
        match source
            .subscriber
            .subscribe(filters.clone(), incoming_handler)
            .await
        {
            Ok(subscription) => {
                active.push(subscription);
                route_ids.push(route_id);
            }
            Err(error) => {
                let close_result = close_subscriptions(active.drain(..)).await;
                return match close_result {
                    Ok(()) => Err(error),
                    Err(close_error) => Err(PubsubError::Storage(format!(
                        "{error}; closing previously opened live routes failed: {close_error}"
                    ))),
                };
            }
        }
    }

    Ok(RoutedLiveSubscription {
        route_ids,
        subscriptions: active,
        seen,
    })
}

async fn close_subscriptions(
    subscriptions: impl IntoIterator<Item = Box<dyn NostrEventSubscription>>,
) -> Result<()> {
    let mut failures = Vec::new();
    for subscription in subscriptions {
        if let Err(error) = subscription.close().await {
            failures.push(error.to_string());
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(PubsubError::Storage(format!(
            "close live routes: {}",
            failures.join("; ")
        )))
    }
}

struct SeenEvents {
    maximum: usize,
    ids: HashSet<nostr::EventId>,
    order: VecDeque<nostr::EventId>,
}

impl SeenEvents {
    fn new(maximum: usize) -> Self {
        Self {
            maximum,
            ids: HashSet::with_capacity(maximum),
            order: VecDeque::with_capacity(maximum),
        }
    }

    fn insert(&mut self, id: nostr::EventId) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        while self.order.len() > self.maximum {
            if let Some(removed) = self.order.pop_front() {
                self.ids.remove(&removed);
            }
        }
        true
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.order.clear();
    }
}
