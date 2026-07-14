use std::collections::{BTreeMap, BTreeSet};
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
pub const PEER_REPUTATION_MAX_TRUSTED_RATERS: usize = 1_024;

#[derive(Debug, Clone, PartialEq)]
pub struct PeerReputationConfig {
    pub scope: String,
    pub policy: SocialGraphPolicyConfig,
    /// Explicit local trust roots whose signed ratings may affect the graph.
    pub trusted_raters: BTreeSet<String>,
}

impl Default for PeerReputationConfig {
    fn default() -> Self {
        Self {
            scope: DEFAULT_PEER_RATING_SCOPE.to_string(),
            policy: SocialGraphPolicyConfig::default(),
            trusted_raters: BTreeSet::new(),
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

/// Raw retained-state and deterministic work counters for [`PeerReputation`].
///
/// The retained counts describe the state held at the instant of the snapshot.
/// Work counters are cumulative and saturating. `graph_rebuild_rating_entries`
/// counts every retained rating supplied to a graph rebuild, so callers can
/// apply platform-specific CPU weights without baking them into this crate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerReputationSnapshot {
    pub retained_ratings: usize,
    pub retained_raters: usize,
    /// Distinct configured trust roots, including the local root.
    pub trusted_roots: usize,
    pub rating_events_considered: u64,
    pub retained_rating_updates: u64,
    pub graph_rebuilds: u64,
    pub graph_rebuild_rating_entries: u64,
}

#[derive(Debug, Default)]
struct PeerReputationWork {
    rating_events_considered: u64,
    retained_rating_updates: u64,
    graph_rebuilds: u64,
    graph_rebuild_rating_entries: u64,
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
    trusted_raters: BTreeSet<String>,
    rating_graph_config: RatingGraphConfig,
    graph: Arc<RwLock<SocialGraph>>,
    latest: BTreeMap<PeerRatingKey, StoredPeerRating>,
    entries_by_rater: BTreeMap<String, usize>,
    next_prune_at_secs: Option<u64>,
    work: PeerReputationWork,
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
        self.check_verified_event(&verified, source).await
    }

    /// Evaluate an event already verified at the transport trust boundary.
    pub async fn check_verified_event(
        &self,
        event: &VerifiedEvent,
        source: &EventSource,
    ) -> Result<PolicyDecision> {
        self.events
            .check_event(EventPolicyContext { event, source })
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
        if config.trusted_raters.len() > PEER_REPUTATION_MAX_TRUSTED_RATERS {
            return Err(validation(format!(
                "peer reputation trusted raters exceed {PEER_REPUTATION_MAX_TRUSTED_RATERS}"
            )));
        }
        let root = parse_pubkey(local_pubkey, "local peer reputation root")?;
        let trusted_raters = config
            .trusted_raters
            .iter()
            .map(|rater| parse_pubkey(rater, "trusted peer reputation rater"))
            .collect::<Result<BTreeSet<_>>>()?;
        let graph = Arc::new(RwLock::new(seeded_graph(&root, &trusted_raters)?));
        let rating_graph_config = RatingGraphConfig::for_scopes([config.scope.clone()]);
        let policy = Arc::new(SocialGraphPolicy::new(Arc::clone(&graph), config.policy));
        let policies = PeerReputationPolicies {
            mesh: policy.clone(),
            events: policy,
        };
        Ok((
            Self {
                root,
                scope: config.scope,
                trusted_raters,
                rating_graph_config,
                graph,
                latest: BTreeMap::new(),
                entries_by_rater: BTreeMap::new(),
                next_prune_at_secs: None,
                work: PeerReputationWork::default(),
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

    /// Returns retained-state gauges and cumulative deterministic work counts.
    #[must_use]
    pub fn snapshot(&self) -> PeerReputationSnapshot {
        PeerReputationSnapshot {
            retained_ratings: self.latest.len(),
            retained_raters: self.entries_by_rater.len(),
            trusted_roots: self.trusted_raters.len()
                + usize::from(!self.trusted_raters.contains(&self.root)),
            rating_events_considered: self.work.rating_events_considered,
            retained_rating_updates: self.work.retained_rating_updates,
            graph_rebuilds: self.work.graph_rebuilds,
            graph_rebuild_rating_entries: self.work.graph_rebuild_rating_entries,
        }
    }

    /// Ingests one rating event using the wall-clock Unix time.
    pub fn ingest_event(&mut self, event: &Event) -> Result<bool> {
        self.ingest_event_at(event, now_unix_secs())
    }

    /// Ingests one rating event at an explicit Unix timestamp.
    ///
    /// Supplying time explicitly keeps retention and future-skew decisions
    /// deterministic for virtual-clock runtimes and simulations.
    pub fn ingest_event_at(&mut self, event: &Event, now_secs: u64) -> Result<bool> {
        let pruned = self.prune_memory(now_secs);
        let (accepted, projection_may_change) = self.consider_event(event, now_secs);
        if pruned > 0 || projection_may_change {
            self.rebuild()?;
        }
        Ok(accepted)
    }

    /// Replays rating events using the wall-clock Unix time.
    pub fn replay<'a>(&mut self, events: impl IntoIterator<Item = &'a Event>) -> Result<usize> {
        self.replay_at(events, now_unix_secs())
    }

    /// Replays rating events at an explicit Unix timestamp.
    ///
    /// All events in the batch are evaluated against the same timestamp.
    pub fn replay_at<'a>(
        &mut self,
        events: impl IntoIterator<Item = &'a Event>,
        now_secs: u64,
    ) -> Result<usize> {
        let pruned = self.prune_memory(now_secs);
        let mut changed = 0usize;
        let mut projection_may_change = false;
        for event in events {
            let (accepted, event_may_change_projection) = self.consider_event(event, now_secs);
            changed += usize::from(accepted);
            projection_may_change |= event_may_change_projection;
        }
        if pruned > 0 || projection_may_change {
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

    fn consider_event(&mut self, event: &Event, now_secs: u64) -> (bool, bool) {
        self.work.rating_events_considered = self.work.rating_events_considered.saturating_add(1);
        if event.kind != Kind::Custom(RATING_KIND) {
            return (false, false);
        }
        // `rating_from_event` verifies the event ID and signature before
        // parsing, so a second verification here would only duplicate CPU.
        let Ok(mut rating) = rating_from_event(event) else {
            return (false, false);
        };
        if !rating_time_is_valid(rating.created_at, now_secs) {
            return (false, false);
        }
        if rating.scope.as_deref() != Some(self.scope.as_str()) {
            return (false, false);
        }
        let Ok(rater) = PublicKey::parse(&rating.rater) else {
            return (false, false);
        };
        if rater != event.pubkey {
            return (false, false);
        }
        let Ok(subject) = PublicKey::parse(&rating.subject) else {
            return (false, false);
        };
        rating.rater = rater.to_hex();
        rating.subject = subject.to_hex();
        let projection_may_change = self.rater_is_reachable(&rating.rater);
        let key = PeerRatingKey {
            rater: rating.rater.clone(),
            subject: rating.subject.clone(),
            scope: self.scope.clone(),
        };
        let event_id = event.id.to_hex();
        if self.latest.get(&key).is_some_and(|existing| {
            (existing.rating.created_at, &existing.event_id) >= (rating.created_at, &event_id)
        }) {
            return (false, false);
        }
        let is_new_key = !self.latest.contains_key(&key);
        let expires_at = rating
            .created_at
            .saturating_add(PEER_RATING_MAX_AGE.as_secs())
            .saturating_add(1);
        self.latest.insert(
            key.clone(),
            StoredPeerRating {
                event_id: event_id.clone(),
                rating,
            },
        );
        if is_new_key {
            *self.entries_by_rater.entry(key.rater.clone()).or_default() += 1;
        }
        self.next_prune_at_secs = Some(
            self.next_prune_at_secs
                .map_or(expires_at, |scheduled| scheduled.min(expires_at)),
        );
        let limits_exceeded = self.latest.len() > PEER_RATING_MAX_ENTRIES
            || self.entries_by_rater.get(&key.rater).copied().unwrap_or(0)
                > PEER_RATING_MAX_ENTRIES_PER_RATER;
        if limits_exceeded {
            self.enforce_entry_limits();
            self.rebuild_entry_counts();
        }
        let retained = self
            .latest
            .get(&key)
            .is_some_and(|stored| stored.event_id == event_id);
        self.work.retained_rating_updates = self
            .work
            .retained_rating_updates
            .saturating_add(u64::from(retained));
        (
            retained,
            (retained && projection_may_change) || limits_exceeded,
        )
    }

    fn prune_memory(&mut self, now_secs: u64) -> usize {
        if self.next_prune_at_secs.is_none_or(|next| now_secs < next) {
            return 0;
        }
        let before = self.latest.len();
        self.latest
            .retain(|_, stored| rating_time_is_valid(stored.rating.created_at, now_secs));
        let removed = before.saturating_sub(self.latest.len());
        if removed > 0 {
            self.rebuild_entry_counts();
        }
        self.next_prune_at_secs = self
            .latest
            .values()
            .map(|stored| {
                stored
                    .rating
                    .created_at
                    .saturating_add(PEER_RATING_MAX_AGE.as_secs())
                    .saturating_add(1)
            })
            .min();
        removed
    }

    fn enforce_entry_limits(&mut self) {
        self.enforce_entry_limits_with(PEER_RATING_MAX_ENTRIES, PEER_RATING_MAX_ENTRIES_PER_RATER);
    }

    fn rebuild_entry_counts(&mut self) {
        self.entries_by_rater.clear();
        for key in self.latest.keys() {
            *self.entries_by_rater.entry(key.rater.clone()).or_default() += 1;
        }
    }

    fn rater_is_reachable(&self, rater: &str) -> bool {
        self.graph.read().map_or(true, |graph| {
            graph.get_follow_distance(rater) <= self.rating_graph_config.max_rater_distance
        })
    }

    fn enforce_entry_limits_with(&mut self, max_entries: usize, max_entries_per_rater: usize) {
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
            let excess = entries.len().saturating_sub(max_entries_per_rater);
            remove.extend(entries.iter().take(excess).map(|(_, _, key)| key.clone()));
        }
        for key in remove {
            self.latest.remove(&key);
        }

        let excess = self.latest.len().saturating_sub(max_entries);
        if excess == 0 {
            return;
        }
        // Untrusted ratings may inform transitive trust while capacity exists,
        // but cannot displace the local root's explicit trust anchors.
        let mut oldest = self
            .latest
            .iter()
            .map(|(key, stored)| {
                (
                    self.root == key.rater || self.trusted_raters.contains(&key.rater),
                    stored.rating.created_at,
                    stored.event_id.clone(),
                    key.clone(),
                )
            })
            .collect::<Vec<_>>();
        oldest.sort();
        for (_, _, _, key) in oldest.into_iter().take(excess) {
            self.latest.remove(&key);
        }
    }

    fn rebuild(&mut self) -> Result<()> {
        self.work.graph_rebuilds = self.work.graph_rebuilds.saturating_add(1);
        self.work.graph_rebuild_rating_entries = self
            .work
            .graph_rebuild_rating_entries
            .saturating_add(u64::try_from(self.latest.len()).unwrap_or(u64::MAX));
        let mut graph = seeded_graph(&self.root, &self.trusted_raters)?;
        let ratings = self
            .latest
            .values()
            .map(|stored| stored.rating.clone())
            .collect::<Vec<_>>();
        graph
            .apply_ratings(&ratings, &self.rating_graph_config)
            .map_err(|error| validation(format!("failed to apply peer ratings: {error}")))?;
        *self
            .graph
            .write()
            .map_err(|_| validation("peer reputation graph lock poisoned"))? = graph;
        Ok(())
    }
}

fn seeded_graph(root: &str, trusted_raters: &BTreeSet<String>) -> Result<SocialGraph> {
    let mut graph = SocialGraph::new(root);
    for rater in trusted_raters {
        graph
            .add_positive_relation(root, rater, 0)
            .map_err(|error| validation(format!("failed to seed trusted rater: {error}")))?;
    }
    Ok(graph)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRatingPublisherConfig {
    pub min_publish_interval: Duration,
    pub refresh_interval: Duration,
    pub material_score_delta: i64,
    pub min_negative_samples: u64,
    pub min_non_negative_samples: u64,
    pub batch_size: usize,
}

impl Default for PeerRatingPublisherConfig {
    fn default() -> Self {
        Self {
            min_publish_interval: Duration::from_mins(10),
            refresh_interval: Duration::from_hours(24),
            material_score_delta: 20,
            min_negative_samples: 3,
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
        Self::from_events_at(
            local_pubkey,
            scope,
            config,
            events,
            now_unix_secs().saturating_mul(1_000),
        )
    }

    /// Reconstructs publication cadence at an explicit Unix timestamp.
    ///
    /// Supplying time explicitly makes replay retention deterministic for
    /// virtual-clock runtimes and simulations.
    pub fn from_events_at<'a>(
        local_pubkey: &str,
        scope: impl Into<String>,
        config: PeerRatingPublisherConfig,
        events: impl IntoIterator<Item = &'a Event>,
        now_ms: u64,
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
        publisher.prune(now_ms);
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
        let minimum_samples = if candidate.class == PeerRatingClass::Negative {
            self.config.min_negative_samples
        } else {
            self.config.min_non_negative_samples
        };
        if rating.sample_count.unwrap_or(0) < minimum_samples {
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
#[path = "reputation_tests.rs"]
mod tests;
