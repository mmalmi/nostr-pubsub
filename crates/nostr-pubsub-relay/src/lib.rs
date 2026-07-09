//! Actual Nostr relay backend for `nostr-pubsub`.

use std::time::Duration;

use async_trait::async_trait;
use nostr_pubsub::{
    EventBus, EventSource, PublishReport, PubsubError, PubsubProvider, PubsubProviderMode,
    QueryEvent, QueryOptions, QueryReport, Result, VerifiedEvent,
};
use nostr_sdk::{Client, Filter, Keys};

#[derive(Clone)]
pub struct RelayEventBus {
    client: Client,
    relays: Vec<String>,
    query_timeout: Duration,
}

impl RelayEventBus {
    pub async fn new(
        relays: impl IntoIterator<Item = impl Into<String>>,
        query_timeout: Duration,
    ) -> Result<Self> {
        Self::with_client(Client::new(Keys::generate()), relays, query_timeout).await
    }

    pub async fn with_client(
        client: Client,
        relays: impl IntoIterator<Item = impl Into<String>>,
        query_timeout: Duration,
    ) -> Result<Self> {
        let relays = relays.into_iter().map(Into::into).collect::<Vec<_>>();
        if relays.is_empty() {
            return Err(PubsubError::Validation(
                "relay event bus requires at least one relay".to_string(),
            ));
        }
        for relay in &relays {
            client
                .add_relay(relay)
                .await
                .map_err(|error| PubsubError::Storage(format!("add relay {relay}: {error}")))?;
        }
        client.connect().await;
        Ok(Self {
            client,
            relays,
            query_timeout,
        })
    }

    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    #[must_use]
    pub fn relays(&self) -> &[String] {
        &self.relays
    }
}

#[async_trait]
impl EventBus for RelayEventBus {
    async fn publish(&self, event: VerifiedEvent, _source: EventSource) -> Result<PublishReport> {
        self.client
            .send_event_to(self.relays.iter().map(String::as_str), event.as_event())
            .await
            .map_err(|error| PubsubError::Storage(format!("send event to relays: {error}")))?;
        Ok(PublishReport {
            accepted: true,
            priority: 0,
            reason: None,
        })
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let mut events = Vec::new();
        let limit = options
            .limit
            .or_else(|| filters.iter().filter_map(|filter| filter.limit).min());
        for filter in filters {
            if limit.is_some_and(|limit| events.len() >= limit) {
                break;
            }
            let fetched = self
                .client
                .fetch_events(filter, self.query_timeout)
                .await
                .map_err(|error| PubsubError::Storage(format!("fetch relay events: {error}")))?;
            for event in fetched {
                if limit.is_some_and(|limit| events.len() >= limit) {
                    break;
                }
                let event = VerifiedEvent::try_from(event)?;
                events.push(QueryEvent {
                    event,
                    source: EventSource::relay(self.relays.join(",")),
                    priority: 0,
                });
            }
        }
        Ok(QueryReport { events })
    }
}

impl PubsubProvider for RelayEventBus {
    fn mode(&self) -> PubsubProviderMode {
        PubsubProviderMode::DirectRelay
    }
}
