use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use nostr::Filter;

use crate::{
    EventBus, EventPolicyContext, EventSource, PolicyDecision, PublishReport, PubsubError,
    PubsubPolicy, QueryEvent, QueryOptions, QueryReport, Result, VerifiedEvent, filter_limit,
    filters_match, report_parts,
};

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
