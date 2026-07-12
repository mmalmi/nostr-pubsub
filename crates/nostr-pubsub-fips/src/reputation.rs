use std::sync::Arc;
use std::time::Duration;

use fips_core::FipsEndpoint;
use nostr::Event;
use nostr_pubsub::{EventSource, MeshPeerPolicy, PolicyDecision, PubsubError, Result};
use nostr_pubsub_social_graph::{
    PeerRatingPublisher, PeerRatingPublisherConfig, PeerReputation, PeerReputationConfig,
    PeerReputationPolicies,
};

use crate::storage_error;

#[derive(Debug, Clone, PartialEq)]
pub struct FipsPeerReputationOptions {
    pub evaluation_interval: Duration,
    pub reputation: PeerReputationConfig,
    pub publication: PeerRatingPublisherConfig,
}

pub type FipsPubsubPolicyOptions = FipsPeerReputationOptions;

impl Default for FipsPeerReputationOptions {
    fn default() -> Self {
        Self {
            evaluation_interval: Duration::from_mins(1),
            reputation: PeerReputationConfig::default(),
            publication: PeerRatingPublisherConfig::default(),
        }
    }
}

/// Default peer reputation for pubsub carried by a [`FipsEndpoint`].
///
/// FIPS remains the source of authenticated peer metrics and signed local
/// ratings. This adapter owns the reusable trust projection and publication
/// cadence so applications only need to transport the returned rating events.
pub struct FipsPeerReputation {
    endpoint: Arc<FipsEndpoint>,
    reputation: PeerReputation,
    policies: PeerReputationPolicies,
    publisher: PeerRatingPublisher,
    evaluation_interval: Duration,
}

/// Pubsub-facing policy facade. It hides rating parsing and cadence from the
/// application runtime; callers only observe ordinary events and periodically
/// request due maintenance events for transport.
pub struct FipsPubsubPolicy {
    reputation: FipsPeerReputation,
    next_evaluation_ms: Option<u64>,
}

impl FipsPeerReputation {
    pub fn new<'a>(
        endpoint: Arc<FipsEndpoint>,
        stored_events: impl IntoIterator<Item = &'a Event>,
        options: FipsPeerReputationOptions,
    ) -> Result<Self> {
        if options.evaluation_interval.is_zero() {
            return Err(PubsubError::Validation(
                "FIPS peer reputation evaluation interval must be positive".to_string(),
            ));
        }
        let stored_events = stored_events.into_iter().collect::<Vec<_>>();
        let (mut reputation, policies) = PeerReputation::new(endpoint.npub(), options.reputation)?;
        reputation.replay(stored_events.iter().copied())?;
        let publisher = PeerRatingPublisher::from_events(
            reputation.root(),
            reputation.scope(),
            options.publication,
            stored_events.iter().copied(),
        )?;
        Ok(Self {
            endpoint,
            reputation,
            policies,
            publisher,
            evaluation_interval: options.evaluation_interval,
        })
    }

    #[must_use]
    pub fn peer_policy(&self) -> Arc<dyn MeshPeerPolicy> {
        self.policies.mesh()
    }

    /// Applies the shared author policy to an event from any transport.
    pub async fn check_event(&self, event: &Event, source: &EventSource) -> Result<PolicyDecision> {
        self.policies.check_event(event, source).await
    }

    #[must_use]
    pub const fn evaluation_interval(&self) -> Duration {
        self.evaluation_interval
    }

    #[must_use]
    pub fn scope(&self) -> &str {
        self.reputation.scope()
    }

    pub fn ingest_event(&mut self, event: &Event) -> Result<bool> {
        self.reputation.ingest_event(event)
    }

    pub async fn publication_candidates(&self, now_ms: u64) -> Result<Vec<Event>> {
        let events = self
            .endpoint
            .peer_rating_events(self.reputation.scope())
            .await
            .map_err(|error| storage_error("snapshot signed FIPS peer ratings", error))?;
        Ok(events
            .into_iter()
            .filter(|event| self.publisher.should_publish_event(event, now_ms))
            .take(self.publisher.batch_size())
            .collect())
    }

    pub fn record_published_event(&mut self, event: &Event, now_ms: u64) -> bool {
        self.publisher.record_published_event(event, now_ms)
    }
}

impl FipsPubsubPolicy {
    pub fn new<'a>(
        endpoint: Arc<FipsEndpoint>,
        stored_events: impl IntoIterator<Item = &'a Event>,
        options: FipsPubsubPolicyOptions,
    ) -> Result<Self> {
        Ok(Self {
            reputation: FipsPeerReputation::new(endpoint, stored_events, options)?,
            next_evaluation_ms: None,
        })
    }

    #[must_use]
    pub fn peer_policy(&self) -> Arc<dyn MeshPeerPolicy> {
        self.reputation.peer_policy()
    }

    pub fn observe_event(&mut self, event: &Event) -> Result<bool> {
        self.reputation.ingest_event(event)
    }

    /// Applies transport-neutral author admission before an event enters the
    /// application cache or pubsub fanout.
    pub async fn check_event(&self, event: &Event, source: &EventSource) -> Result<PolicyDecision> {
        self.reputation.check_event(event, source).await
    }

    pub async fn maintenance_events(&mut self, now_ms: u64) -> Result<Vec<Event>> {
        self.reputation.reputation.prune(now_ms / 1_000)?;
        self.reputation.publisher.prune(now_ms);
        let interval_ms = duration_ms(self.reputation.evaluation_interval());
        let Some(next_evaluation_ms) = self.next_evaluation_ms else {
            self.next_evaluation_ms = Some(now_ms.saturating_add(interval_ms));
            return Ok(Vec::new());
        };
        if now_ms < next_evaluation_ms {
            return Ok(Vec::new());
        }
        self.next_evaluation_ms = Some(now_ms.saturating_add(interval_ms));
        self.reputation.publication_candidates(now_ms).await
    }

    pub fn complete_maintenance_event(
        &mut self,
        event: &Event,
        published: bool,
        now_ms: u64,
    ) -> Result<()> {
        if published {
            self.reputation.ingest_event(event)?;
            let _ = self.reputation.record_published_event(event, now_ms);
        }
        Ok(())
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
