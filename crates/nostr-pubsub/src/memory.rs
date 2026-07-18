use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use nostr::Filter;

use crate::{
    EventBus, EventPolicyContext, EventSource, NostrEventHandler, NostrEventSubscriber,
    NostrEventSubscription, PolicyDecision, PublishReport, PubsubError, PubsubPolicy, QueryEvent,
    QueryOptions, QueryReport, Result, VerifiedEvent, filters_match, report_parts,
};

#[derive(Clone)]
struct StoredEvent {
    event: VerifiedEvent,
    source: EventSource,
    priority: i32,
}

#[derive(Clone, Default)]
pub struct InMemoryEventBus {
    state: Arc<RwLock<MemoryState>>,
    policy: Option<Arc<dyn PubsubPolicy>>,
}

#[derive(Default)]
struct MemoryState {
    events: Vec<StoredEvent>,
    next_subscription_id: u64,
    subscriptions: HashMap<u64, MemorySubscriber>,
}

struct MemorySubscriber {
    filters: Vec<Filter>,
    handler: NostrEventHandler,
}

impl InMemoryEventBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: Arc<dyn PubsubPolicy>) -> Self {
        Self {
            state: Arc::default(),
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
            let incoming = QueryEvent {
                event: event.clone(),
                source: source.clone(),
                priority,
            };
            let handlers = {
                let mut state = self
                    .state
                    .write()
                    .map_err(|_| PubsubError::Storage("event bus lock poisoned".to_string()))?;
                state.events.push(StoredEvent {
                    event,
                    source,
                    priority,
                });
                state
                    .subscriptions
                    .values()
                    .filter(|subscription| {
                        filters_match(&subscription.filters, incoming.event.as_event())
                    })
                    .map(|subscription| Arc::clone(&subscription.handler))
                    .collect::<Vec<_>>()
            };
            for handler in handlers {
                handler(incoming.clone());
            }
        }

        Ok(PublishReport {
            accepted,
            priority,
            reason,
        })
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let mut stored = self
            .state
            .read()
            .map_err(|_| PubsubError::Storage("event bus lock poisoned".to_string()))?
            .events
            .clone();
        stored.sort_by(|left, right| {
            right
                .event
                .as_event()
                .created_at
                .cmp(&left.event.as_event().created_at)
                .then_with(|| left.event.as_event().id.cmp(&right.event.as_event().id))
        });
        let filters = if filters.is_empty() {
            vec![Filter::new()]
        } else {
            filters
        };
        let mut seen = HashSet::new();
        let mut events = Vec::new();
        for filter in filters {
            let mut matched = 0;
            for stored in &stored {
                if filter.limit.is_some_and(|limit| matched >= limit) {
                    break;
                }
                if !filter.match_event(stored.event.as_event(), crate::MatchEventOptions::new()) {
                    continue;
                }
                matched += 1;
                if seen.insert(stored.event.as_event().id) {
                    events.push(QueryEvent {
                        event: stored.event.clone(),
                        source: stored.source.clone(),
                        priority: stored.priority,
                    });
                }
            }
        }
        events.sort_by(|left, right| {
            right
                .event
                .as_event()
                .created_at
                .cmp(&left.event.as_event().created_at)
                .then_with(|| left.event.as_event().id.cmp(&right.event.as_event().id))
        });
        events.truncate(options.limit.unwrap_or(usize::MAX));

        Ok(QueryReport { events })
    }
}

#[async_trait]
impl NostrEventSubscriber for InMemoryEventBus {
    async fn subscribe(
        &self,
        filters: Vec<Filter>,
        handler: NostrEventHandler,
    ) -> Result<Box<dyn NostrEventSubscription>> {
        let mut state = self
            .state
            .write()
            .map_err(|_| PubsubError::Storage("event bus lock poisoned".to_string()))?;
        let id = state.next_subscription_id;
        state.next_subscription_id = state.next_subscription_id.wrapping_add(1);
        state
            .subscriptions
            .insert(id, MemorySubscriber { filters, handler });
        Ok(Box::new(MemoryEventSubscription {
            state: Arc::clone(&self.state),
            id,
        }))
    }
}

struct MemoryEventSubscription {
    state: Arc<RwLock<MemoryState>>,
    id: u64,
}

#[async_trait]
impl NostrEventSubscription for MemoryEventSubscription {
    async fn close(self: Box<Self>) -> Result<()> {
        self.state
            .write()
            .map_err(|_| PubsubError::Storage("event bus lock poisoned".to_string()))?
            .subscriptions
            .remove(&self.id);
        Ok(())
    }
}
