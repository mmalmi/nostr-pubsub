//! Actual Nostr relay backend for `nostr-pubsub`.

use std::{fmt::Debug, time::Duration};

use async_trait::async_trait;
use nostr_pubsub::{
    EventBus, EventSource, PublishReport, PubsubError, PubsubProvider, PubsubProviderMode,
    QueryEvent, QueryOptions, QueryReport, Result, VerifiedEvent,
};
use nostr_sdk::{Client, Filter, Keys, nostr::message::MachineReadablePrefix, pool::Output};

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
        let output = self
            .client
            .send_event_to(self.relays.iter().map(String::as_str), event.as_event())
            .await
            .map_err(|error| PubsubError::Storage(format!("send event to relays: {error}")))?;
        Ok(publish_report(output))
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
    use nostr_sdk::{RelayUrl, pool::Output};

    use super::publish_report;

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
