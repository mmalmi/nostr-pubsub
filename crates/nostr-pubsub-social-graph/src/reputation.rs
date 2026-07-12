use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use nostr::{Event, Kind};
use nostr_pubsub::{
    EventPolicyContext, EventSource, MeshPeerPolicy, PolicyDecision, PublicKey, PubsubError,
    PubsubPolicy, Result, VerifiedEvent,
};
use nostr_social_graph::{Rating, RatingGraphConfig, SocialGraph};
use nostr_social_memory::{RATING_KIND, rating_from_event};

use crate::{SocialGraphPolicy, SocialGraphPolicyConfig};

pub const DEFAULT_PEER_RATING_SCOPE: &str = "fips.peer";
pub const PEER_RATING_MAX_AGE: Duration = Duration::from_hours(720);
pub const PEER_RATING_MAX_FUTURE_SKEW: Duration = Duration::from_mins(10);
pub const PEER_RATING_MAX_ENTRIES: usize = 4_096;
pub const PEER_RATING_MAX_ENTRIES_PER_RATER: usize = 1_024;

#[derive(Debug, Clone, PartialEq)]
pub struct PeerReputationConfig {
    pub scope: String,
    pub policy: SocialGraphPolicyConfig,
}

impl Default for PeerReputationConfig {
    fn default() -> Self {
        Self {
            scope: DEFAULT_PEER_RATING_SCOPE.to_string(),
            policy: SocialGraphPolicyConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PeerRatingKey {
    rater: String,
    subject: String,
    scope: String,
}

#[derive(Debug, Clone)]
struct StoredPeerRating {
    event_id: String,
    rating: Rating,
}

/// A local, replayable trust projection for authenticated pubsub peers.
///
/// Unknown peers remain eligible under the default policy. Ratings affect the
/// projection only when the signed event author is also its declared rater, so
/// an unknown crawler or Sybil cannot bootstrap itself by asserting a trusted
/// identity in the rating payload.
pub struct PeerReputation {
    root: String,
    scope: String,
    graph: Arc<RwLock<SocialGraph>>,
    latest: BTreeMap<PeerRatingKey, StoredPeerRating>,
}

/// The transport-neutral policies backed by a [`PeerReputation`] projection.
///
/// The same graph classifies authenticated transport peers and signed event
/// authors. Unknown identities remain eligible by default; explicit negative
/// reputation can drop either a peer or an event regardless of its transport.
#[derive(Clone)]
pub struct PeerReputationPolicies {
    mesh: Arc<dyn MeshPeerPolicy>,
    events: Arc<dyn PubsubPolicy>,
}

impl PeerReputationPolicies {
    #[must_use]
    pub fn mesh(&self) -> Arc<dyn MeshPeerPolicy> {
        Arc::clone(&self.mesh)
    }

    #[must_use]
    pub fn events(&self) -> Arc<dyn PubsubPolicy> {
        Arc::clone(&self.events)
    }

    pub fn select_mesh_peer(&self, peer_id: &str) -> Result<Option<nostr_pubsub::MeshPeer>> {
        self.mesh.select_mesh_peer(peer_id)
    }

    pub async fn check_event(&self, event: &Event, source: &EventSource) -> Result<PolicyDecision> {
        let verified = VerifiedEvent::try_from(event.clone())?;
        self.events
            .check_event(EventPolicyContext {
                event: &verified,
                source,
            })
            .await
    }
}

impl PeerReputation {
    pub fn new(
        local_pubkey: &str,
        config: PeerReputationConfig,
    ) -> Result<(Self, PeerReputationPolicies)> {
        if config.scope.trim().is_empty() {
            return Err(validation("peer reputation scope must not be empty"));
        }
        let root = parse_pubkey(local_pubkey, "local peer reputation root")?;
        let graph = Arc::new(RwLock::new(SocialGraph::new(&root)));
        let policy = Arc::new(SocialGraphPolicy::new(Arc::clone(&graph), config.policy));
        let policies = PeerReputationPolicies {
            mesh: policy.clone(),
            events: policy,
        };
        Ok((
            Self {
                root,
                scope: config.scope,
                graph,
                latest: BTreeMap::new(),
            },
            policies,
        ))
    }

    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    pub fn ingest_event(&mut self, event: &Event) -> Result<bool> {
        let now = now_unix_secs();
        let pruned = self.prune_memory(now);
        let accepted = self.consider_event(event, now);
        if pruned > 0 || accepted {
            self.rebuild()?;
        }
        Ok(accepted)
    }

    pub fn replay<'a>(&mut self, events: impl IntoIterator<Item = &'a Event>) -> Result<usize> {
        let now = now_unix_secs();
        let pruned = self.prune_memory(now);
        let mut changed = 0usize;
        for event in events {
            changed += usize::from(self.consider_event(event, now));
        }
        if pruned > 0 || changed > 0 {
            self.rebuild()?;
        }
        Ok(changed)
    }

    /// Removes expired, far-future, and over-budget ratings, then rebuilds the
    /// shared policy projection when anything was forgotten.
    pub fn prune(&mut self, now_secs: u64) -> Result<usize> {
        let removed = self.prune_memory(now_secs);
        if removed > 0 {
            self.rebuild()?;
        }
        Ok(removed)
    }

    fn consider_event(&mut self, event: &Event, now_secs: u64) -> bool {
        if event.kind != Kind::Custom(RATING_KIND) || event.verify().is_err() {
            return false;
        }
        let Ok(mut rating) = rating_from_event(event) else {
            return false;
        };
        if !rating_time_is_valid(rating.created_at, now_secs) {
            return false;
        }
        if rating.scope.as_deref() != Some(self.scope.as_str()) {
            return false;
        }
        let Ok(rater) = PublicKey::parse(&rating.rater) else {
            return false;
        };
        if rater != event.pubkey {
            return false;
        }
        let Ok(subject) = PublicKey::parse(&rating.subject) else {
            return false;
        };
        rating.rater = rater.to_hex();
        rating.subject = subject.to_hex();
        let key = PeerRatingKey {
            rater: rating.rater.clone(),
            subject: rating.subject.clone(),
            scope: self.scope.clone(),
        };
        let event_id = event.id.to_hex();
        if self.latest.get(&key).is_some_and(|existing| {
            (existing.rating.created_at, &existing.event_id) >= (rating.created_at, &event_id)
        }) {
            return false;
        }
        self.latest.insert(
            key.clone(),
            StoredPeerRating {
                event_id: event_id.clone(),
                rating,
            },
        );
        self.enforce_entry_limits();
        self.latest
            .get(&key)
            .is_some_and(|stored| stored.event_id == event_id)
    }

    fn prune_memory(&mut self, now_secs: u64) -> usize {
        let before = self.latest.len();
        self.latest
            .retain(|_, stored| rating_time_is_valid(stored.rating.created_at, now_secs));
        self.enforce_entry_limits();
        before.saturating_sub(self.latest.len())
    }

    fn enforce_entry_limits(&mut self) {
        let mut by_rater = BTreeMap::<String, Vec<(u64, String, PeerRatingKey)>>::new();
        for (key, stored) in &self.latest {
            by_rater.entry(key.rater.clone()).or_default().push((
                stored.rating.created_at,
                stored.event_id.clone(),
                key.clone(),
            ));
        }
        let mut remove = Vec::new();
        for entries in by_rater.values_mut() {
            entries.sort();
            let excess = entries
                .len()
                .saturating_sub(PEER_RATING_MAX_ENTRIES_PER_RATER);
            remove.extend(entries.iter().take(excess).map(|(_, _, key)| key.clone()));
        }
        for key in remove {
            self.latest.remove(&key);
        }

        let excess = self.latest.len().saturating_sub(PEER_RATING_MAX_ENTRIES);
        if excess == 0 {
            return;
        }
        let mut oldest = self
            .latest
            .iter()
            .map(|(key, stored)| {
                (
                    stored.rating.created_at,
                    stored.event_id.clone(),
                    key.clone(),
                )
            })
            .collect::<Vec<_>>();
        oldest.sort();
        for (_, _, key) in oldest.into_iter().take(excess) {
            self.latest.remove(&key);
        }
    }

    fn rebuild(&self) -> Result<()> {
        let mut graph = SocialGraph::new(&self.root);
        let ratings = self
            .latest
            .values()
            .map(|stored| stored.rating.clone())
            .collect::<Vec<_>>();
        graph
            .apply_ratings(
                &ratings,
                &RatingGraphConfig::for_scopes([self.scope.clone()]),
            )
            .map_err(|error| validation(format!("failed to apply peer ratings: {error}")))?;
        *self
            .graph
            .write()
            .map_err(|_| validation("peer reputation graph lock poisoned"))? = graph;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRatingPublisherConfig {
    pub min_publish_interval: Duration,
    pub refresh_interval: Duration,
    pub material_score_delta: i64,
    pub min_non_negative_samples: u64,
    pub batch_size: usize,
}

impl Default for PeerRatingPublisherConfig {
    fn default() -> Self {
        Self {
            min_publish_interval: Duration::from_mins(10),
            refresh_interval: Duration::from_hours(24),
            material_score_delta: 20,
            min_non_negative_samples: 3,
            batch_size: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerRatingClass {
    Negative,
    Neutral,
    Positive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RatingPublication {
    subject: String,
    score: i64,
    class: PeerRatingClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedPeerRating {
    score: i64,
    class: PeerRatingClass,
    published_at_ms: u64,
}

/// Coalesces locally signed peer ratings before they enter the pubsub mesh.
#[derive(Debug)]
pub struct PeerRatingPublisher {
    local_root: String,
    scope: String,
    config: PeerRatingPublisherConfig,
    published: BTreeMap<String, PublishedPeerRating>,
}

impl PeerRatingPublisher {
    pub fn new(
        local_pubkey: &str,
        scope: impl Into<String>,
        config: PeerRatingPublisherConfig,
    ) -> Result<Self> {
        let scope = scope.into();
        if scope.trim().is_empty() {
            return Err(validation(
                "peer rating publication scope must not be empty",
            ));
        }
        if config.min_publish_interval.is_zero()
            || config.refresh_interval.is_zero()
            || config.batch_size == 0
            || config.material_score_delta < 0
        {
            return Err(validation(
                "peer rating publication intervals and batch size must be positive; score delta must be non-negative",
            ));
        }
        Ok(Self {
            local_root: parse_pubkey(local_pubkey, "local peer rating publisher")?,
            scope,
            config,
            published: BTreeMap::new(),
        })
    }

    pub fn from_events<'a>(
        local_pubkey: &str,
        scope: impl Into<String>,
        config: PeerRatingPublisherConfig,
        events: impl IntoIterator<Item = &'a Event>,
    ) -> Result<Self> {
        let mut publisher = Self::new(local_pubkey, scope, config)?;
        for event in events {
            let Some(publication) = publisher.publication(event) else {
                continue;
            };
            let Ok(rating) = rating_from_event(event) else {
                continue;
            };
            let published_at_ms = rating.created_at.saturating_mul(1_000);
            if publisher
                .published
                .get(&publication.subject)
                .is_some_and(|previous| previous.published_at_ms >= published_at_ms)
            {
                continue;
            }
            publisher.record(publication, published_at_ms);
        }
        publisher.prune(now_unix_secs().saturating_mul(1_000));
        Ok(publisher)
    }

    #[must_use]
    pub const fn batch_size(&self) -> usize {
        self.config.batch_size
    }

    #[must_use]
    pub fn should_publish_event(&self, event: &Event, now_ms: u64) -> bool {
        let Some(candidate) = self.publication(event) else {
            return false;
        };
        let Ok(rating) = rating_from_event(event) else {
            return false;
        };
        if candidate.class != PeerRatingClass::Negative
            && rating.sample_count.unwrap_or(0) < self.config.min_non_negative_samples
        {
            return false;
        }
        let Some(previous) = self.published.get(&candidate.subject) else {
            return true;
        };
        let elapsed = now_ms.saturating_sub(previous.published_at_ms);
        if candidate.class == PeerRatingClass::Negative
            && previous.class != PeerRatingClass::Negative
        {
            return true;
        }
        elapsed >= duration_ms(self.config.refresh_interval)
            || (elapsed >= duration_ms(self.config.min_publish_interval)
                && (candidate.class != previous.class
                    || candidate.score.abs_diff(previous.score)
                        >= self.config.material_score_delta.cast_unsigned()))
    }

    pub fn record_published_event(&mut self, event: &Event, now_ms: u64) -> bool {
        let Some(publication) = self.publication(event) else {
            return false;
        };
        self.record(publication, now_ms);
        true
    }

    /// Forgets publication cadence for peers that have not produced a local
    /// rating within the shared rating-retention window.
    pub fn prune(&mut self, now_ms: u64) -> usize {
        let before = self.published.len();
        let max_age_ms = duration_ms(PEER_RATING_MAX_AGE);
        let max_future_skew_ms = duration_ms(PEER_RATING_MAX_FUTURE_SKEW);
        self.published.retain(|_, publication| {
            publication.published_at_ms <= now_ms.saturating_add(max_future_skew_ms)
                && now_ms.saturating_sub(publication.published_at_ms) <= max_age_ms
        });
        let excess = self
            .published
            .len()
            .saturating_sub(PEER_RATING_MAX_ENTRIES_PER_RATER);
        if excess > 0 {
            let mut oldest = self
                .published
                .iter()
                .map(|(subject, publication)| (publication.published_at_ms, subject.clone()))
                .collect::<Vec<_>>();
            oldest.sort();
            for (_, subject) in oldest.into_iter().take(excess) {
                self.published.remove(&subject);
            }
        }
        before.saturating_sub(self.published.len())
    }

    fn publication(&self, event: &Event) -> Option<RatingPublication> {
        if event.kind != Kind::Custom(RATING_KIND)
            || event.verify().is_err()
            || event.pubkey.to_hex() != self.local_root
        {
            return None;
        }
        let rating = rating_from_event(event).ok()?;
        if rating.scope.as_deref() != Some(self.scope.as_str())
            || parse_pubkey(&rating.rater, "rating rater").ok()? != self.local_root
        {
            return None;
        }
        let subject = parse_pubkey(&rating.subject, "rating subject").ok()?;
        let score = rating.normalized_score().ok()?;
        let class = match score.cmp(&0) {
            std::cmp::Ordering::Less => PeerRatingClass::Negative,
            std::cmp::Ordering::Equal => PeerRatingClass::Neutral,
            std::cmp::Ordering::Greater => PeerRatingClass::Positive,
        };
        Some(RatingPublication {
            subject,
            score,
            class,
        })
    }

    fn record(&mut self, publication: RatingPublication, now_ms: u64) {
        self.published.insert(
            publication.subject,
            PublishedPeerRating {
                score: publication.score,
                class: publication.class,
                published_at_ms: now_ms,
            },
        );
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn rating_time_is_valid(created_at: u64, now_secs: u64) -> bool {
    created_at <= now_secs.saturating_add(PEER_RATING_MAX_FUTURE_SKEW.as_secs())
        && now_secs.saturating_sub(created_at) <= PEER_RATING_MAX_AGE.as_secs()
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn parse_pubkey(value: &str, field: &str) -> Result<String> {
    PublicKey::parse(value)
        .map(|pubkey| pubkey.to_hex())
        .map_err(|error| validation(format!("invalid {field}: {error}")))
}

fn validation(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use nostr_sdk::prelude::{EventBuilder, Keys, ToBech32};
    use nostr_social_memory::RatingEventExt;

    use super::*;

    #[test]
    fn default_policy_explores_unknown_prioritizes_good_and_drops_bad() {
        let root = Keys::generate();
        let good = Keys::generate();
        let unknown = Keys::generate();
        let bad = Keys::generate();
        let root_hex = root.public_key().to_hex();
        let now = now_unix_secs();
        let (mut reputation, policy) = PeerReputation::new(
            &root.public_key().to_bech32().expect("root npub"),
            PeerReputationConfig::default(),
        )
        .expect("peer reputation");

        let unknown_peer = policy
            .select_mesh_peer(&unknown.public_key().to_bech32().expect("unknown npub"))
            .expect("unknown policy")
            .expect("unknown remains eligible");
        assert!(unknown_peer.is_unknown());

        assert!(
            reputation
                .ingest_event(&rating_event(
                    &root,
                    &root_hex,
                    &good.public_key().to_hex(),
                    100,
                    now.saturating_sub(1),
                ))
                .expect("good rating")
        );
        let good_peer = policy
            .select_mesh_peer(&good.public_key().to_bech32().expect("good npub"))
            .expect("good policy")
            .expect("good remains eligible");
        assert!(good_peer.quality_score.is_some_and(|score| score > 0));
        assert!(
            reputation
                .ingest_event(&rating_event(
                    &root,
                    &root_hex,
                    &bad.public_key().to_hex(),
                    0,
                    now,
                ))
                .expect("bad rating")
        );
        assert_eq!(
            policy
                .select_mesh_peer(&bad.public_key().to_bech32().expect("bad npub"))
                .expect("bad policy"),
            None
        );
    }

    #[test]
    fn reputation_rejects_forgery_and_newest_rating_allows_recovery() {
        let root = Keys::generate();
        let peer = Keys::generate();
        let attacker = Keys::generate();
        let root_hex = root.public_key().to_hex();
        let peer_hex = peer.public_key().to_hex();
        let peer_npub = peer.public_key().to_bech32().expect("peer npub");
        let now = now_unix_secs();
        let (mut reputation, policy) = PeerReputation::new(
            &root.public_key().to_bech32().expect("root npub"),
            PeerReputationConfig::default(),
        )
        .expect("peer reputation");

        let forged = rating_event(&attacker, &root_hex, &peer_hex, 0, now.saturating_sub(2));
        assert!(!reputation.ingest_event(&forged).expect("forged rating"));
        assert!(
            policy
                .select_mesh_peer(&peer_npub)
                .expect("unknown decision")
                .expect("unknown remains eligible")
                .is_unknown()
        );

        let negative = rating_event(&root, &root_hex, &peer_hex, 0, now.saturating_sub(1));
        assert!(reputation.ingest_event(&negative).expect("negative rating"));
        assert_eq!(policy.select_mesh_peer(&peer_npub).expect("negative"), None);

        let recovered = rating_event(&root, &root_hex, &peer_hex, 100, now);
        assert!(reputation.ingest_event(&recovered).expect("recovery"));
        assert!(
            policy
                .select_mesh_peer(&peer_npub)
                .expect("recovered decision")
                .expect("recovered remains eligible")
                .quality_score
                .is_some_and(|score| score > 0)
        );
        assert!(!reputation.ingest_event(&negative).expect("stale rating"));
    }

    #[tokio::test]
    async fn author_admission_is_transport_neutral_and_allows_unknowns() {
        let root = Keys::generate();
        let good = Keys::generate();
        let unknown = Keys::generate();
        let bad = Keys::generate();
        let root_hex = root.public_key().to_hex();
        let now = now_unix_secs();
        let (mut reputation, policies) = PeerReputation::new(
            &root.public_key().to_bech32().expect("root npub"),
            PeerReputationConfig::default(),
        )
        .expect("peer reputation");
        reputation
            .ingest_event(&rating_event(
                &root,
                &root_hex,
                &good.public_key().to_hex(),
                100,
                now.saturating_sub(1),
            ))
            .expect("good rating");
        reputation
            .ingest_event(&rating_event(
                &root,
                &root_hex,
                &bad.public_key().to_hex(),
                0,
                now,
            ))
            .expect("bad rating");

        let relay = EventSource::relay("wss://bootstrap.example");
        let good_event = EventBuilder::text_note("good")
            .sign_with_keys(&good)
            .expect("good event");
        let unknown_event = EventBuilder::text_note("unknown")
            .sign_with_keys(&unknown)
            .expect("unknown event");
        let bad_event = EventBuilder::text_note("bad")
            .sign_with_keys(&bad)
            .expect("bad event");

        assert!(matches!(
            policies.check_event(&good_event, &relay).await.unwrap(),
            PolicyDecision::Allow { priority } if priority > 0
        ));
        assert!(!matches!(
            policies.check_event(&unknown_event, &relay).await.unwrap(),
            PolicyDecision::Drop { .. }
        ));
        assert!(matches!(
            policies.check_event(&bad_event, &relay).await.unwrap(),
            PolicyDecision::Drop { .. }
        ));

        assert!(matches!(
            policies
                .check_event(&unknown_event, &EventSource::fips_endpoint("peer"))
                .await
                .unwrap(),
            PolicyDecision::Throttle { .. }
        ));
    }

    #[test]
    fn publisher_coalesces_material_changes_and_refreshes() {
        let root = Keys::generate();
        let subject = Keys::generate().public_key().to_hex();
        let root_hex = root.public_key().to_hex();
        let config = PeerRatingPublisherConfig::default();
        let min_interval_ms = duration_ms(config.min_publish_interval);
        let refresh_interval_ms = duration_ms(config.refresh_interval);
        let mut publisher = PeerRatingPublisher::new(&root_hex, DEFAULT_PEER_RATING_SCOPE, config)
            .expect("publisher");

        let first = rating_event_with_samples(&root, &root_hex, &subject, 80, 1, 3);
        assert!(publisher.should_publish_event(&first, 1_000));
        assert!(publisher.record_published_event(&first, 1_000));

        let small = rating_event_with_samples(&root, &root_hex, &subject, 85, 2, 3);
        assert!(!publisher.should_publish_event(&small, 2_000));
        let material = rating_event_with_samples(&root, &root_hex, &subject, 95, 3, 3);
        assert!(!publisher.should_publish_event(&material, 2_000));
        assert!(publisher.should_publish_event(&material, 1_000 + min_interval_ms));
        assert!(publisher.record_published_event(&material, 1_000 + min_interval_ms));

        let negative = rating_event_with_samples(&root, &root_hex, &subject, 0, 4, 1);
        assert!(publisher.should_publish_event(&negative, 1_001 + min_interval_ms));
        assert!(publisher.record_published_event(&negative, 1_001 + min_interval_ms));
        assert!(!publisher.should_publish_event(&negative, 2_000 + min_interval_ms));
        assert!(
            publisher
                .should_publish_event(&negative, 1_001 + min_interval_ms + refresh_interval_ms)
        );
    }

    #[test]
    fn reputation_rejects_expired_and_far_future_ratings() {
        let root = Keys::generate();
        let subject = Keys::generate();
        let root_hex = root.public_key().to_hex();
        let subject_hex = subject.public_key().to_hex();
        let now = now_unix_secs();
        let (mut reputation, policy) =
            PeerReputation::new(&root_hex, PeerReputationConfig::default())
                .expect("peer reputation");

        let expired = rating_event(
            &root,
            &root_hex,
            &subject_hex,
            0,
            now.saturating_sub(PEER_RATING_MAX_AGE.as_secs() + 1),
        );
        assert!(!reputation.ingest_event(&expired).expect("expired rating"));

        let future = rating_event(
            &root,
            &root_hex,
            &subject_hex,
            0,
            now.saturating_add(PEER_RATING_MAX_FUTURE_SKEW.as_secs() + 1),
        );
        assert!(!reputation.ingest_event(&future).expect("future rating"));
        assert!(
            policy
                .select_mesh_peer(&subject_hex)
                .expect("unknown policy")
                .expect("rejected ratings leave subject unknown")
                .is_unknown()
        );
    }

    #[test]
    fn reputation_prune_forgets_stale_policy_state() {
        let root = Keys::generate();
        let subject = Keys::generate();
        let root_hex = root.public_key().to_hex();
        let subject_hex = subject.public_key().to_hex();
        let subject_npub = subject.public_key().to_bech32().expect("subject npub");
        let created_at = now_unix_secs();
        let (mut reputation, policy) =
            PeerReputation::new(&root_hex, PeerReputationConfig::default())
                .expect("peer reputation");

        assert!(
            reputation
                .ingest_event(&rating_event(&root, &root_hex, &subject_hex, 0, created_at,))
                .expect("negative rating")
        );
        assert_eq!(
            policy.select_mesh_peer(&subject_npub).expect("negative"),
            None
        );

        assert_eq!(
            reputation
                .prune(created_at + PEER_RATING_MAX_AGE.as_secs() + 1)
                .expect("prune reputation"),
            1
        );
        assert!(
            policy
                .select_mesh_peer(&subject_npub)
                .expect("forgotten policy")
                .expect("forgotten peer is eligible again")
                .is_unknown()
        );
    }

    #[test]
    fn publisher_prune_forgets_stale_subjects() {
        let root = Keys::generate();
        let subject = Keys::generate().public_key().to_hex();
        let root_hex = root.public_key().to_hex();
        let mut publisher = PeerRatingPublisher::new(
            &root_hex,
            DEFAULT_PEER_RATING_SCOPE,
            PeerRatingPublisherConfig::default(),
        )
        .expect("publisher");
        let event = rating_event_with_samples(&root, &root_hex, &subject, 80, 1, 3);

        assert!(publisher.record_published_event(&event, 1_000));
        assert_eq!(
            publisher.prune(1_000 + duration_ms(PEER_RATING_MAX_AGE) + 1),
            1
        );
        assert!(
            publisher.should_publish_event(&event, 1_000 + duration_ms(PEER_RATING_MAX_AGE) + 1)
        );
    }

    #[test]
    fn reputation_enforces_total_and_per_rater_bounds() {
        let root = Keys::generate().public_key().to_hex();
        let (mut reputation, _) =
            PeerReputation::new(&root, PeerReputationConfig::default()).expect("reputation");

        for index in 0..=PEER_RATING_MAX_ENTRIES_PER_RATER {
            insert_stored_rating(&mut reputation, "one-rater", index, index as u64);
        }
        reputation.enforce_entry_limits();
        assert_eq!(
            reputation
                .latest
                .keys()
                .filter(|key| key.rater == "one-rater")
                .count(),
            PEER_RATING_MAX_ENTRIES_PER_RATER
        );

        reputation.latest.clear();
        for index in 0..=PEER_RATING_MAX_ENTRIES {
            insert_stored_rating(
                &mut reputation,
                &format!("rater-{index}"),
                index,
                index as u64,
            );
        }
        reputation.enforce_entry_limits();
        assert_eq!(reputation.latest.len(), PEER_RATING_MAX_ENTRIES);
    }

    #[test]
    fn publisher_enforces_subject_bound() {
        let root = Keys::generate().public_key().to_hex();
        let mut publisher = PeerRatingPublisher::new(
            &root,
            DEFAULT_PEER_RATING_SCOPE,
            PeerRatingPublisherConfig::default(),
        )
        .expect("publisher");
        let now = duration_ms(PEER_RATING_MAX_AGE);
        for index in 0..=PEER_RATING_MAX_ENTRIES_PER_RATER {
            publisher.published.insert(
                format!("subject-{index}"),
                PublishedPeerRating {
                    score: 0,
                    class: PeerRatingClass::Neutral,
                    published_at_ms: now.saturating_sub(index as u64),
                },
            );
        }

        assert_eq!(publisher.prune(now), 1);
        assert_eq!(publisher.published.len(), PEER_RATING_MAX_ENTRIES_PER_RATER);
    }

    fn insert_stored_rating(
        reputation: &mut PeerReputation,
        rater: &str,
        index: usize,
        created_at: u64,
    ) {
        let subject = format!("subject-{index}");
        let mut rating = Rating::new(rater, &subject, 50, 0, 100);
        rating.scope = Some(DEFAULT_PEER_RATING_SCOPE.to_string());
        rating.created_at = created_at;
        reputation.latest.insert(
            PeerRatingKey {
                rater: rater.to_string(),
                subject,
                scope: DEFAULT_PEER_RATING_SCOPE.to_string(),
            },
            StoredPeerRating {
                event_id: format!("event-{index}"),
                rating,
            },
        );
    }

    fn rating_event(
        signer: &Keys,
        rater: &str,
        subject: &str,
        value: i64,
        created_at: u64,
    ) -> Event {
        rating_event_with_samples(signer, rater, subject, value, created_at, 1)
    }

    fn rating_event_with_samples(
        signer: &Keys,
        rater: &str,
        subject: &str,
        value: i64,
        created_at: u64,
        samples: u64,
    ) -> Event {
        let mut rating = Rating::new(rater, subject, value, 0, 100);
        rating.scope = Some(DEFAULT_PEER_RATING_SCOPE.to_string());
        rating.created_at = created_at;
        rating.sample_count = Some(samples);
        rating.to_event(signer).expect("signed rating")
    }
}
