//! Actual Nostr relay backend for `nostr-pubsub`.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::Debug,
    time::Duration,
};

use async_trait::async_trait;
use nostr_pubsub::{
    EventBus, EventSource, MatchEventOptions, NostrEventHandler, NostrEventSubscriber,
    NostrEventSubscription, PublishReport, PubsubError, PubsubProvider, PubsubProviderMode,
    QueryEvent, QueryOptions, QueryReport, Result, SOURCE_PRIORITY_RELAY, VerifiedEvent,
};
use nostr_sdk::{
    Client, ClientMessage, Filter, Keys, RelayMessage, RelayPoolNotification, SubscriptionId,
    nostr::message::MachineReadablePrefix, pool::Output,
};
use tokio::task::JoinHandle;

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

    /// Queue an event on every configured relay without waiting for relay `OK`
    /// acknowledgements.
    ///
    /// This is appropriate when the caller owns bounded retry or redundancy
    /// policy and does not require relay `OK` acknowledgements. The report
    /// describes whether each relay accepted the message into its local queue.
    pub async fn enqueue(&self, event: &VerifiedEvent) -> Result<PublishReport> {
        let output = self
            .client
            .send_msg_to(
                self.relays.iter().map(String::as_str),
                ClientMessage::Event(Cow::Borrowed(event.as_event())),
            )
            .await
            .map_err(|error| PubsubError::Storage(format!("queue event for relays: {error}")))?;
        Ok(publish_report(output))
    }
}

#[async_trait]
impl EventBus for RelayEventBus {
    async fn publish(&self, event: VerifiedEvent, _source: EventSource) -> Result<PublishReport> {
        self.enqueue(&event).await
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let filters = if filters.is_empty() {
            vec![Filter::new()]
        } else {
            filters
        };
        let mut events = Vec::new();
        let mut seen = HashSet::new();
        for filter in filters {
            let fetched = self
                .client
                .fetch_events(filter, self.query_timeout)
                .await
                .map_err(|error| PubsubError::Storage(format!("fetch relay events: {error}")))?;
            for event in fetched {
                let event = VerifiedEvent::try_from(event)?;
                if seen.insert(event.as_event().id) {
                    events.push(QueryEvent {
                        event,
                        source: EventSource::relay(self.relays.join(",")),
                        priority: SOURCE_PRIORITY_RELAY,
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

impl PubsubProvider for RelayEventBus {
    fn mode(&self) -> PubsubProviderMode {
        PubsubProviderMode::DirectRelay
    }
}

#[async_trait]
impl NostrEventSubscriber for RelayEventBus {
    async fn subscribe(
        &self,
        filters: Vec<Filter>,
        handler: NostrEventHandler,
    ) -> Result<Box<dyn NostrEventSubscription>> {
        let mut notifications = self.client.notifications();
        let filters = if filters.is_empty() {
            vec![Filter::new()]
        } else {
            filters
        };
        let mut subscriptions = Vec::with_capacity(filters.len());
        for filter in filters {
            let output = match self
                .client
                .subscribe_to(self.relays.iter().map(String::as_str), filter.clone(), None)
                .await
            {
                Ok(output) => output,
                Err(error) => {
                    unsubscribe_pairs(&self.client, &subscriptions).await;
                    return Err(PubsubError::Storage(format!(
                        "subscribe to configured relays: {error}"
                    )));
                }
            };
            if output.success.is_empty() {
                unsubscribe_pairs(&self.client, &subscriptions).await;
                return Err(PubsubError::Storage(
                    "no configured relay accepted live subscription".to_string(),
                ));
            }
            subscriptions.push((output.val, filter));
        }

        let watched_filters = subscriptions.iter().cloned().collect::<HashMap<_, _>>();
        let notifications_task = tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(RelayPoolNotification::Message {
                        relay_url,
                        message:
                            RelayMessage::Event {
                                subscription_id,
                                event,
                            },
                    }) if watched_filters.contains_key(subscription_id.as_ref()) => {
                        if let Ok(event) = VerifiedEvent::try_from(event.into_owned())
                            && watched_filters[subscription_id.as_ref()]
                                .match_event(event.as_event(), MatchEventOptions::new())
                        {
                            handler(QueryEvent {
                                event,
                                source: EventSource::relay(relay_url.to_string()),
                                priority: SOURCE_PRIORITY_RELAY,
                            });
                        }
                    }
                    Ok(RelayPoolNotification::Shutdown)
                    | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
        Ok(Box::new(RelayLiveSubscription {
            client: self.client.clone(),
            subscription_ids: subscriptions.into_iter().map(|(id, _)| id).collect(),
            notifications_task: Some(notifications_task),
        }))
    }
}

struct RelayLiveSubscription {
    client: Client,
    subscription_ids: Vec<SubscriptionId>,
    notifications_task: Option<JoinHandle<()>>,
}

impl Drop for RelayLiveSubscription {
    fn drop(&mut self) {
        if let Some(task) = self.notifications_task.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl NostrEventSubscription for RelayLiveSubscription {
    async fn close(mut self: Box<Self>) -> Result<()> {
        unsubscribe_all(&self.client, &self.subscription_ids).await;
        if let Some(task) = self.notifications_task.take() {
            task.abort();
        }
        Ok(())
    }
}

async fn unsubscribe_all(client: &Client, subscription_ids: &[SubscriptionId]) {
    for subscription_id in subscription_ids {
        client.unsubscribe(subscription_id).await;
    }
}

async fn unsubscribe_pairs(client: &Client, subscriptions: &[(SubscriptionId, Filter)]) {
    for (subscription_id, _) in subscriptions {
        client.unsubscribe(subscription_id).await;
    }
}

fn publish_report<T: Debug>(output: Output<T>) -> PublishReport {
    let accepted_count = output.success.len();
    let failed_count = output.failed.len();
    let attempted_count = accepted_count + failed_count;

    if accepted_count > 0 {
        return PublishReport {
            accepted: true,
            priority: 0,
            reason: (failed_count > 0).then(|| {
                format!(
                    "accepted by {accepted_count} of {attempted_count} relays; {failed_count} {} failed",
                    relay_label(failed_count)
                )
            }),
        };
    }

    if failed_count > 0
        && output
            .failed
            .values()
            .all(|error| is_idempotent_rejection(error))
    {
        return PublishReport {
            accepted: true,
            priority: 0,
            reason: Some(format!(
                "event already present on all {failed_count} {}",
                relay_label(failed_count)
            )),
        };
    }

    let reason = if attempted_count == 0 {
        "no relay reported a publish result".to_string()
    } else {
        let mut failures = output
            .failed
            .into_iter()
            .map(|(url, error)| format!("{url}: {error}"))
            .collect::<Vec<_>>();
        failures.sort_unstable();
        format!(
            "0 of {attempted_count} relays accepted event: {}",
            failures.join("; ")
        )
    };

    PublishReport {
        accepted: false,
        priority: 0,
        reason: Some(reason),
    }
}

fn is_idempotent_rejection(error: &str) -> bool {
    let normalized = error
        .trim()
        .trim_end_matches(['.', '!', ';'])
        .to_ascii_lowercase();

    match MachineReadablePrefix::parse(&normalized) {
        Some(MachineReadablePrefix::Duplicate) => true,
        Some(_) => false,
        None => matches!(
            normalized.as_str(),
            "duplicate"
                | "already have event"
                | "already have the event"
                | "already have this event"
                | "event already exists"
                | "event already present"
                | "event already stored"
        ),
    }
}

const fn relay_label(count: usize) -> &'static str {
    if count == 1 { "relay" } else { "relays" }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use futures_util::{SinkExt, StreamExt};
    use nostr_pubsub::{EventBus, EventSource, NostrEventSubscriber, QueryEvent, VerifiedEvent};
    use nostr_sdk::{
        ClientMessage, Event, EventBuilder, Filter, JsonUtil, Keys, Kind, RelayMessage, RelayUrl,
        SubscriptionId, pool::Output, prelude::RelayStatus,
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::time::{sleep, timeout};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use super::{RelayEventBus, publish_report};

    fn output(success: &[&str], failed: &[(&str, &str)]) -> Output<()> {
        Output {
            val: (),
            success: success
                .iter()
                .map(|url| url.parse::<RelayUrl>().expect("valid relay URL"))
                .collect(),
            failed: failed
                .iter()
                .map(|(url, error)| {
                    (
                        url.parse::<RelayUrl>().expect("valid relay URL"),
                        (*error).to_string(),
                    )
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn publish_returns_after_queueing_without_waiting_for_relay_ok() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test relay");
        let relay_url = format!(
            "ws://{}",
            listener.local_addr().expect("test relay address")
        );
        let relay = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept relay client");
            let mut websocket = accept_async(stream).await.expect("accept websocket");
            timeout(Duration::from_secs(2), async {
                loop {
                    let message = websocket
                        .next()
                        .await
                        .expect("relay websocket remains open")
                        .expect("read relay message");
                    if String::from_utf8_lossy(&message.into_data()).contains("EVENT") {
                        break;
                    }
                }
            })
            .await
            .expect("event should reach relay");
            sleep(Duration::from_secs(1)).await;
        });
        let bus = RelayEventBus::new([relay_url.clone()], Duration::from_secs(5))
            .await
            .expect("start relay event bus");
        timeout(Duration::from_secs(2), async {
            loop {
                let connected = bus
                    .client()
                    .relay(&relay_url)
                    .await
                    .expect("configured relay")
                    .status()
                    == RelayStatus::Connected;
                if connected {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("relay client should connect");
        let event = VerifiedEvent::try_from(
            EventBuilder::new(Kind::Custom(21_060), "queued event")
                .sign_with_keys(&Keys::generate())
                .expect("sign test event"),
        )
        .expect("verify test event");

        let report = timeout(
            Duration::from_millis(500),
            bus.publish(event, EventSource::local_index("test")),
        )
        .await
        .expect("publish must not wait for relay OK")
        .expect("queue event");

        assert!(report.accepted);
        relay.await.expect("test relay task");
        bus.client().shutdown().await;
    }

    #[tokio::test]
    async fn live_subscription_delivers_relay_events_and_sends_close() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test relay");
        let relay_url = format!(
            "ws://{}",
            listener.local_addr().expect("test relay address")
        );
        let expected_event = EventBuilder::new(Kind::TextNote, "live relay event")
            .sign_with_keys(&Keys::generate())
            .expect("sign relay event");
        let wrong_kind_event = EventBuilder::new(Kind::Metadata, "{}")
            .sign_with_keys(&Keys::generate())
            .expect("sign non-matching relay event");
        let event_for_relay = expected_event.clone();
        let relay = tokio::spawn(serve_live_relay(
            listener,
            wrong_kind_event,
            event_for_relay,
        ));
        let bus = RelayEventBus::new([relay_url.clone()], Duration::from_secs(5))
            .await
            .expect("start relay event bus");
        wait_until_connected(&bus, &relay_url).await;
        let (sender, receiver) = oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(sender)));
        let output = Arc::clone(&sender);
        let subscription = bus
            .subscribe(
                vec![Filter::new().kind(Kind::TextNote)],
                Arc::new(move |event: QueryEvent| {
                    if let Some(sender) = output.lock().unwrap().take() {
                        let _ = sender.send(event);
                    }
                }),
            )
            .await
            .expect("subscribe through relay event bus");
        let delivered = timeout(Duration::from_secs(2), receiver)
            .await
            .expect("receive live event")
            .expect("handler remains available");
        assert_eq!(delivered.event.as_event().id, expected_event.id);
        assert_eq!(delivered.priority, nostr_pubsub::SOURCE_PRIORITY_RELAY);
        assert_eq!(
            delivered.source,
            EventSource::relay(relay_url.parse::<RelayUrl>().unwrap().to_string())
        );

        subscription.close().await.expect("close live subscription");
        relay.await.expect("test relay task");
        bus.client().shutdown().await;
    }

    async fn serve_live_relay(listener: TcpListener, wrong_kind_event: Event, live_event: Event) {
        let (stream, _) = listener.accept().await.expect("accept relay client");
        let mut websocket = accept_async(stream).await.expect("accept websocket");
        let subscription_id = timeout(Duration::from_secs(2), async {
            loop {
                let message = websocket
                    .next()
                    .await
                    .expect("relay websocket remains open")
                    .expect("read client message");
                let Ok(client_message) = ClientMessage::from_json(message.into_data()) else {
                    continue;
                };
                if let ClientMessage::Req {
                    subscription_id, ..
                } = client_message
                {
                    break subscription_id.into_owned();
                }
            }
        })
        .await
        .expect("receive subscription request");
        for message in [
            RelayMessage::event(subscription_id.clone(), wrong_kind_event),
            RelayMessage::event(
                SubscriptionId::new("another-client-subscription"),
                live_event.clone(),
            ),
        ] {
            websocket
                .send(Message::Text(message.as_json().into()))
                .await
                .expect("send irrelevant relay event");
        }
        websocket
            .send(Message::Text(
                RelayMessage::event(subscription_id.clone(), live_event)
                    .as_json()
                    .into(),
            ))
            .await
            .expect("send relay event");
        timeout(Duration::from_secs(2), async {
            loop {
                let message = websocket
                    .next()
                    .await
                    .expect("relay websocket remains open")
                    .expect("read client message");
                let Ok(client_message) = ClientMessage::from_json(message.into_data()) else {
                    continue;
                };
                if matches!(
                    client_message,
                    ClientMessage::Close(id) if id.as_ref() == &subscription_id
                ) {
                    break;
                }
            }
        })
        .await
        .expect("receive subscription close");
    }

    async fn wait_until_connected(bus: &RelayEventBus, relay_url: &str) {
        timeout(Duration::from_secs(2), async {
            loop {
                let connected = bus
                    .client()
                    .relay(relay_url)
                    .await
                    .expect("configured relay")
                    .status()
                    == RelayStatus::Connected;
                if connected {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("relay client should connect");
    }

    #[test]
    fn accepts_when_any_relay_succeeds() {
        let report = publish_report(output(
            &["wss://accepted.example"],
            &[("wss://blocked.example", "blocked: publish denied")],
        ));

        assert!(report.accepted);
        assert_eq!(report.priority, 0);
        assert_eq!(
            report.reason.as_deref(),
            Some("accepted by 1 of 2 relays; 1 relay failed")
        );
    }

    #[test]
    fn accepts_when_all_relays_already_have_the_event() {
        let report = publish_report(output(
            &[],
            &[
                ("wss://duplicate.example", "duplicate: event already exists"),
                ("wss://already-have.example", "already have this event"),
            ],
        ));

        assert!(report.accepted);
        assert_eq!(
            report.reason.as_deref(),
            Some("event already present on all 2 relays")
        );
    }

    #[test]
    fn rejects_when_all_relays_report_policy_failures() {
        let report = publish_report(output(
            &[],
            &[
                ("wss://blocked.example", "blocked: already have this event"),
                ("wss://pow.example", "pow: difficulty 30 required"),
            ],
        ));

        assert!(!report.accepted);
        let reason = report.reason.expect("rejection reason");
        assert!(reason.starts_with("0 of 2 relays accepted event: "));
        assert!(reason.contains("blocked: already have this event"));
        assert!(reason.contains("pow: difficulty 30 required"));
    }

    #[test]
    fn rejects_mixed_duplicate_and_other_failures_without_success() {
        let report = publish_report(output(
            &[],
            &[
                ("wss://duplicate.example", "duplicate: already stored"),
                ("wss://offline.example", "relay not connected"),
            ],
        ));

        assert!(!report.accepted);
        assert!(
            report
                .reason
                .as_deref()
                .is_some_and(|reason| reason.starts_with("0 of 2 relays accepted event: "))
        );
    }
}
