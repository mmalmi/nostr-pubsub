use std::time::Duration;

use nostr::Event;
use nostr_pubsub::VerifiedEvent;
use nostr_pubsub_social_graph::{
    DEFAULT_PEER_RATING_SCOPE, PeerRatingPublisher, PeerRatingPublisherConfig,
    PeerReputationPolicies,
};

use super::machine_wot::{RatingCandidate, RatingEvidence};
use crate::topology::NodeRole;

use super::{
    PeerSelectionMode, ReputationEventMetadata, ReputationEventOrigin, Result, SIM_UNIX_BASE,
    Simulation, SimulationConfig, peer_rating_event_with_samples, pubsub_error,
};

const RATING_BATCH_SIZE: usize = 2;
const RATING_MIN_PUBLISH_INTERVAL_MS: u64 = 50;
const RATING_REFRESH_INTERVAL_MS: u64 = 10_000;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerProjection {
    Unknown,
    Positive,
    Removed,
}

pub(super) fn build_reputation_publishers(
    mode: PeerSelectionMode,
    config: &SimulationConfig,
    peer_ids: &[String],
) -> Result<Vec<Option<PeerRatingPublisher>>> {
    peer_ids
        .iter()
        .enumerate()
        .map(|(node, peer_id)| {
            if mode != PeerSelectionMode::SharedReputation || node < config.attacker_count {
                return Ok(None);
            }
            PeerRatingPublisher::new(
                peer_id,
                DEFAULT_PEER_RATING_SCOPE,
                PeerRatingPublisherConfig {
                    min_publish_interval: Duration::from_millis(RATING_MIN_PUBLISH_INTERVAL_MS),
                    refresh_interval: Duration::from_millis(RATING_REFRESH_INTERVAL_MS),
                    material_score_delta: 100,
                    min_negative_samples: 3,
                    min_non_negative_samples: 3,
                    batch_size: RATING_BATCH_SIZE,
                },
            )
            .map(Some)
            .map_err(pubsub_error)
        })
        .collect()
}

impl Simulation {
    pub(super) fn run_reputation_sweep(&mut self) -> Result<()> {
        if self.mode != PeerSelectionMode::SharedReputation {
            return Ok(());
        }
        let now_ms = self.scheduler.now_ms();
        for node in self.config.attacker_count..self.config.node_count {
            self.nodes[node].mesh.maintain(now_ms);
            self.observe_core_resource_state(node);
        }
        self.publish_admitted_rater_poison_probe()?;
        self.publish_admitted_rater_revocation(now_ms)?;
        for observer in self.config.attacker_count..self.config.node_count {
            let candidates = self.observed_rating_candidates(observer);
            self.publish_observed_ratings(observer, now_ms, candidates)?;
        }
        self.flush_rediscovery_subscriptions()
    }

    fn publish_observed_ratings(
        &mut self,
        observer: usize,
        now_ms: u64,
        candidates: Vec<RatingCandidate>,
    ) -> Result<()> {
        let batch_size = self.reputation_publishers[observer]
            .as_ref()
            .map_or(0, PeerRatingPublisher::batch_size);
        let mut due = Vec::new();
        for (subject, observation, evidence) in candidates {
            let value = i64::from(evidence == RatingEvidence::PositiveService) * 100;
            let event = peer_rating_event_with_samples(
                &self.keys[observer],
                &self.peer_ids[observer],
                &self.peer_ids[subject],
                value,
                u64::from(observation.samples),
                virtual_unix_secs(now_ms),
            )?;
            if self.reputation_publishers[observer]
                .as_ref()
                .is_some_and(|publisher| publisher.should_publish_event(&event, now_ms))
            {
                let observed_at_ms = self
                    .bad_observed_at
                    .get(&(observer, subject))
                    .copied()
                    .unwrap_or(now_ms);
                due.push((subject, observed_at_ms, event, evidence));
            }
            if due.len() >= batch_size {
                break;
            }
        }
        for (subject, observed_at_ms, event, evidence) in due {
            self.publish_observed_rating(observer, subject, observed_at_ms, &event, evidence)?;
        }
        Ok(())
    }

    pub(super) fn publish_observed_rating(
        &mut self,
        observer: usize,
        subject: usize,
        observed_at_ms: u64,
        event: &Event,
        evidence: RatingEvidence,
    ) -> Result<()> {
        let origin = match evidence {
            RatingEvidence::Negative { quiet_blackhole } => {
                ReputationEventOrigin::HonestObservation { quiet_blackhole }
            }
            RatingEvidence::PositiveService => ReputationEventOrigin::PositiveServiceEndorsement,
        };
        self.publish_reputation_event(observer, subject, observed_at_ms, event, origin)?;
        let recorded = self.reputation_publishers[observer]
            .as_mut()
            .is_some_and(|publisher| {
                publisher.record_published_event(event, self.scheduler.now_ms())
            });
        if recorded {
            self.report.machine_ratings_published =
                self.report.machine_ratings_published.saturating_add(1);
            if evidence == RatingEvidence::PositiveService {
                self.positive_endorsements[observer].push(subject);
                self.report.machine_positive_service_endorsements_published = self
                    .report
                    .machine_positive_service_endorsements_published
                    .saturating_add(1);
            }
        }
        Ok(())
    }

    pub(super) fn exercise_adversarial_reputation_probes(&mut self) -> Result<()> {
        self.publish_unconnected_rating_pressure()?;
        self.drain_scheduler()?;
        self.publish_admitted_rater_poison_probe()?;
        self.publish_forged_probe()?;
        self.publish_poisoned_probe()
    }

    pub(super) fn receive_reputation_event(&mut self, node: usize, event: &Event) -> Result<()> {
        let event_id = event.id.to_hex();
        if !self.rating_receipts.insert((node, event_id.clone())) {
            return Ok(());
        }
        self.report.machine_ratings_received =
            self.report.machine_ratings_received.saturating_add(1);
        let metadata = self.reputation_events.get(&event_id).cloned();
        match metadata.as_ref().map(|metadata| metadata.origin) {
            Some(ReputationEventOrigin::PoisonedProbe) => {
                self.report.poisoned_machine_ratings_received = self
                    .report
                    .poisoned_machine_ratings_received
                    .saturating_add(1);
            }
            Some(ReputationEventOrigin::ForgedProbe) => {
                self.report.forged_machine_ratings_received = self
                    .report
                    .forged_machine_ratings_received
                    .saturating_add(1);
            }
            Some(ReputationEventOrigin::AdmittedRaterPoison) => {
                self.report.admitted_rater_poison_received =
                    self.report.admitted_rater_poison_received.saturating_add(1);
                if metadata.as_ref().is_some_and(|metadata| {
                    self.is_admitted_rater_poison_target(node, metadata.subject)
                }) {
                    self.report.admitted_rater_poison_target_received = self
                        .report
                        .admitted_rater_poison_target_received
                        .saturating_add(1);
                }
            }
            Some(ReputationEventOrigin::UnconnectedRatingPressure) => {
                if metadata.as_ref().is_some_and(|metadata| {
                    self.unconnected_rating_pressure_baseline(node, metadata.subject)
                        .is_some()
                }) {
                    self.report.unconnected_rating_pressure_target_received = self
                        .report
                        .unconnected_rating_pressure_target_received
                        .saturating_add(1);
                }
            }
            Some(ReputationEventOrigin::RevokedRaterRating) => {
                if metadata
                    .as_ref()
                    .is_some_and(|metadata| self.is_post_revocation_target(node, metadata.subject))
                {
                    self.report.post_revocation_rating_target_received = self
                        .report
                        .post_revocation_rating_target_received
                        .saturating_add(1);
                }
            }
            Some(
                ReputationEventOrigin::HonestObservation { .. }
                | ReputationEventOrigin::PositiveServiceEndorsement
                | ReputationEventOrigin::MachineLifecycle(_),
            )
            | None => {}
        }
        self.apply_reputation_event(node, event, true)
    }

    pub(super) fn publish_reputation_event(
        &mut self,
        publisher: usize,
        subject: usize,
        observed_at_ms: u64,
        event: &Event,
        origin: ReputationEventOrigin,
    ) -> Result<()> {
        let event_id = event.id.to_hex();
        self.record_cpu_work(publisher, |work| {
            work.signature_checks = work.signature_checks.saturating_add(1);
            work.avoided_signature_checks = work.avoided_signature_checks.saturating_add(1);
        });
        let verified = VerifiedEvent::try_from(event.clone()).map_err(pubsub_error)?;
        self.reputation_events.insert(
            event_id.clone(),
            ReputationEventMetadata {
                subject,
                observed_at_ms,
                origin,
            },
        );
        self.retain_local_event(publisher, event_id, verified.clone())?;
        if !origin.is_spam() {
            self.apply_reputation_event(publisher, event, false)?;
        }
        let peers = self.interested_mesh_peers(publisher, &verified)?;
        self.record_cpu_work(publisher, |work| {
            work.mesh_candidates = work
                .mesh_candidates
                .saturating_add(u64::try_from(peers.len()).unwrap_or(u64::MAX));
        });
        let actions = self.nodes[publisher]
            .mesh
            .publish_verified(verified, &peers, self.scheduler.now_ms())
            .map_err(pubsub_error)?;
        self.observe_core_resource_state(publisher);
        self.dispatch_actions(publisher, actions)
    }

    fn apply_reputation_event(
        &mut self,
        node: usize,
        event: &Event,
        transported: bool,
    ) -> Result<()> {
        let Some(metadata) = self.reputation_events.get(&event.id.to_hex()).cloned() else {
            return Ok(());
        };
        let Some(policies) = self.nodes[node].machine_policies.clone() else {
            return Ok(());
        };
        self.record_cpu_work(node, |work| {
            work.graph_queries = work.graph_queries.saturating_add(1);
        });
        let before = peer_projection(&policies, &self.peer_ids[metadata.subject])?;
        let now_secs = virtual_unix_secs(self.scheduler.now_ms());
        self.record_cpu_work(node, |work| {
            work.signature_checks = work.signature_checks.saturating_add(1);
        });
        let Some(reputation) = self.nodes[node].machine_reputation.as_mut() else {
            return Ok(());
        };
        if transported && metadata.origin == ReputationEventOrigin::ForgedProbe {
            self.report.forged_machine_ratings_evaluated = self
                .report
                .forged_machine_ratings_evaluated
                .saturating_add(1);
        }
        let ingested = reputation
            .ingest_event_at(event, now_secs)
            .map_err(pubsub_error)?;
        self.observe_core_resource_state(node);
        self.record_reputation_ingest(metadata.origin, transported, ingested);
        if transported
            && metadata.origin == ReputationEventOrigin::UnconnectedRatingPressure
            && let Some((ratings, raters, rebuild_entries)) =
                self.unconnected_rating_pressure_baseline(node, metadata.subject)
        {
            if ingested {
                self.report.unconnected_rating_pressure_target_ingested = self
                    .report
                    .unconnected_rating_pressure_target_ingested
                    .saturating_add(1);
            } else {
                self.report.unconnected_rating_pressure_target_rejected = self
                    .report
                    .unconnected_rating_pressure_target_rejected
                    .saturating_add(1);
            }
            let snapshot = self.nodes[node]
                .machine_reputation
                .as_ref()
                .expect("target has machine reputation")
                .snapshot();
            self.report
                .unconnected_rating_pressure_retained_rating_delta =
                snapshot.retained_ratings.saturating_sub(ratings);
            self.report.unconnected_rating_pressure_retained_rater_delta =
                snapshot.retained_raters.saturating_sub(raters);
            self.report.unconnected_rating_pressure_rebuild_entry_delta = snapshot
                .graph_rebuild_rating_entries
                .saturating_sub(rebuild_entries);
        }
        if !ingested {
            return Ok(());
        }
        if transported
            && metadata.origin == ReputationEventOrigin::AdmittedRaterPoison
            && self.is_admitted_rater_poison_target(node, metadata.subject)
        {
            self.report.admitted_rater_poison_target_ingested = self
                .report
                .admitted_rater_poison_target_ingested
                .saturating_add(1);
        }
        if transported
            && metadata.origin == ReputationEventOrigin::RevokedRaterRating
            && self.is_post_revocation_target(node, metadata.subject)
        {
            self.report.post_revocation_rating_target_ingested = self
                .report
                .post_revocation_rating_target_ingested
                .saturating_add(1);
        }
        self.record_cpu_work(node, |work| {
            work.graph_queries = work.graph_queries.saturating_add(1);
        });
        let after = peer_projection(&policies, &self.peer_ids[metadata.subject])?;
        let root_authored = event.pubkey.to_hex() == self.peer_ids[node];
        if root_authored
            && after == PeerProjection::Positive
            && metadata.origin == ReputationEventOrigin::PositiveServiceEndorsement
        {
            self.record_positive_service_admission(node, metadata.subject);
        }
        if root_authored && before != PeerProjection::Removed && after == PeerProjection::Removed {
            self.record_root_rater_revocation(node, metadata.subject);
        }
        self.record_machine_projection_transition(node, &metadata, before, after, transported);
        Ok(())
    }

    fn record_reputation_ingest(
        &mut self,
        origin: ReputationEventOrigin,
        transported: bool,
        ingested: bool,
    ) {
        if transported && ingested {
            self.report.machine_ratings_ingested =
                self.report.machine_ratings_ingested.saturating_add(1);
        }
        if transported && origin == ReputationEventOrigin::ForgedProbe {
            if ingested {
                self.report.forged_machine_ratings_ingested = self
                    .report
                    .forged_machine_ratings_ingested
                    .saturating_add(1);
            } else {
                self.report.forged_machine_ratings_rejected = self
                    .report
                    .forged_machine_ratings_rejected
                    .saturating_add(1);
            }
        }
        if transported && origin == ReputationEventOrigin::PoisonedProbe {
            if ingested {
                self.report.poisoned_machine_ratings_ingested = self
                    .report
                    .poisoned_machine_ratings_ingested
                    .saturating_add(1);
            } else {
                self.report.poisoned_machine_ratings_rejected = self
                    .report
                    .poisoned_machine_ratings_rejected
                    .saturating_add(1);
            }
        }
        if transported && origin == ReputationEventOrigin::AdmittedRaterPoison {
            if ingested {
                self.report.admitted_rater_poison_ingested =
                    self.report.admitted_rater_poison_ingested.saturating_add(1);
            } else {
                self.report.admitted_rater_poison_rejected =
                    self.report.admitted_rater_poison_rejected.saturating_add(1);
            }
        }
    }

    fn record_machine_projection_transition(
        &mut self,
        node: usize,
        metadata: &ReputationEventMetadata,
        before: PeerProjection,
        after: PeerProjection,
        transported: bool,
    ) {
        self.record_machine_lifecycle_transition(node, metadata, before, after, transported);
        if metadata.origin == ReputationEventOrigin::UnconnectedRatingPressure
            && self
                .unconnected_rating_pressure_baseline(node, metadata.subject)
                .is_some()
        {
            if before == PeerProjection::Positive && after == PeerProjection::Positive {
                self.report
                    .unconnected_rating_pressure_anchor_stable_evaluations = self
                    .report
                    .unconnected_rating_pressure_anchor_stable_evaluations
                    .saturating_add(1);
            } else if before != after {
                self.report
                    .unconnected_rating_pressure_anchor_projection_changes = self
                    .report
                    .unconnected_rating_pressure_anchor_projection_changes
                    .saturating_add(1);
            }
        }
        if metadata.origin == ReputationEventOrigin::RevokedRaterRating
            && self.is_post_revocation_target(node, metadata.subject)
            && before != after
        {
            self.report.post_revocation_rating_influence = self
                .report
                .post_revocation_rating_influence
                .saturating_add(1);
        }
        if transported && before != after {
            self.report.machine_transported_transitions = self
                .report
                .machine_transported_transitions
                .saturating_add(1);
        }
        if before != PeerProjection::Positive && after == PeerProjection::Positive {
            self.report.machine_positive_admissions =
                self.report.machine_positive_admissions.saturating_add(1);
            if transported {
                self.report.machine_transported_positive_admissions = self
                    .report
                    .machine_transported_positive_admissions
                    .saturating_add(1);
            }
        }
        if before != PeerProjection::Removed && after == PeerProjection::Removed {
            self.record_machine_removal(node, metadata, transported);
        }
    }

    fn record_machine_removal(
        &mut self,
        node: usize,
        metadata: &ReputationEventMetadata,
        transported: bool,
    ) {
        self.report.machine_removals = self.report.machine_removals.saturating_add(1);
        if transported {
            self.report.machine_transported_removals =
                self.report.machine_transported_removals.saturating_add(1);
        }
        if matches!(
            metadata.origin,
            ReputationEventOrigin::HonestObservation {
                quiet_blackhole: true
            }
        ) {
            self.report.machine_quiet_blackhole_removals = self
                .report
                .machine_quiet_blackhole_removals
                .saturating_add(1);
        }
        if metadata.origin == ReputationEventOrigin::PoisonedProbe {
            self.report.machine_poisoning_removals =
                self.report.machine_poisoning_removals.saturating_add(1);
        }
        if metadata.origin == ReputationEventOrigin::AdmittedRaterPoison {
            self.report.admitted_rater_poison_removals =
                self.report.admitted_rater_poison_removals.saturating_add(1);
            if self.is_admitted_rater_poison_target(node, metadata.subject) {
                self.report.admitted_rater_poison_target_removals = self
                    .report
                    .admitted_rater_poison_target_removals
                    .saturating_add(1);
            }
        }
        if matches!(
            metadata.origin,
            ReputationEventOrigin::HonestObservation { .. }
        ) && self.is_admitted_rater_source(node, metadata.subject)
        {
            self.report.admitted_rater_revocations =
                self.report.admitted_rater_revocations.saturating_add(1);
        }
        let honest_false_positive = matches!(
            metadata.origin,
            ReputationEventOrigin::HonestObservation { .. }
        ) && self.topology.roles[metadata.subject]
            != NodeRole::Attacker
            && !self.is_admitted_rater_publisher(metadata.subject);
        self.report.machine_false_positive_removals = self
            .report
            .machine_false_positive_removals
            .saturating_add(usize::from(honest_false_positive));
        self.reputation_removal_latencies.push(
            self.scheduler
                .now_ms()
                .saturating_sub(metadata.observed_at_ms),
        );
    }
}

pub(super) fn virtual_unix_secs(now_ms: u64) -> u64 {
    SIM_UNIX_BASE.saturating_add(now_ms / 1_000)
}

pub(super) fn peer_projection(
    policies: &PeerReputationPolicies,
    peer_id: &str,
) -> Result<PeerProjection> {
    policies
        .select_mesh_peer(peer_id)
        .map_err(pubsub_error)
        .map(|selected| match selected {
            None => PeerProjection::Removed,
            Some(peer) if peer.quality_score.is_some_and(|score| score > 0) => {
                PeerProjection::Positive
            }
            Some(_) => PeerProjection::Unknown,
        })
}

#[cfg(test)]
#[path = "reputation_flow/test_support.rs"]
mod test_support;

#[cfg(test)]
mod tests {
    use nostr::Keys;
    use nostr_pubsub::{PubsubPeerInterest, SourceId, VerifiedEvent};

    use super::{
        PeerProjection, PeerSelectionMode, ReputationEventOrigin, SIM_UNIX_BASE, Simulation,
        SimulationConfig, peer_projection, test_support::trusted_transport_triangle,
        virtual_unix_secs,
    };
    use crate::simulation::{
        peer_rating_event,
        rating_subscriptions::{reputation_filter, trusted_rater_filter},
        run_simulation,
    };

    #[test]
    fn peer_rating_event_uses_explicit_time_and_is_repeatable() {
        let keys = Keys::generate();
        let rater = keys.public_key().to_hex();
        let subject = Keys::generate().public_key().to_hex();
        let created_at = SIM_UNIX_BASE.saturating_add(17);

        let first = peer_rating_event(&keys, &rater, &subject, 75, created_at).unwrap();
        let second = peer_rating_event(&keys, &rater, &subject, 75, created_at).unwrap();

        assert_eq!(first.created_at.as_secs(), created_at);
        assert_eq!(first.id, second.id);
    }

    #[test]
    fn virtual_milliseconds_respect_nostr_second_resolution() {
        assert_eq!(virtual_unix_secs(0), SIM_UNIX_BASE);
        assert_eq!(virtual_unix_secs(999), SIM_UNIX_BASE);
        assert_eq!(virtual_unix_secs(1_000), SIM_UNIX_BASE + 1);
        assert_eq!(virtual_unix_secs(2_140), SIM_UNIX_BASE + 2);
    }

    #[test]
    fn no_remote_machine_rater_is_synthetic_before_verified_service() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                node_count: 64,
                attacker_count: 12,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        assert_eq!(simulation.machine_lifecycle_plan(), None);
        assert_eq!(simulation.poisoned_probe_plan(), None);
    }

    #[test]
    fn scoped_rating_transport_filter_matches_signed_fips_peer_rating() {
        let keys = Keys::generate();
        let subject = Keys::generate().public_key().to_hex();
        let event = peer_rating_event(
            &keys,
            &keys.public_key().to_hex(),
            &subject,
            100,
            SIM_UNIX_BASE,
        )
        .unwrap();
        let verified = VerifiedEvent::try_from(event).unwrap();
        assert_eq!(
            PubsubPeerInterest::from_filters(
                &[reputation_filter([
                    nostr::PublicKey::parse(&subject).unwrap()
                ])],
                &verified,
            ),
            PubsubPeerInterest::Subscribed
        );
    }

    #[test]
    fn trusted_rater_filter_matches_author_for_an_arbitrary_subject() {
        let rater = Keys::generate();
        let subject = Keys::generate().public_key().to_hex();
        let event = peer_rating_event(
            &rater,
            &rater.public_key().to_hex(),
            &subject,
            0,
            SIM_UNIX_BASE,
        )
        .unwrap();
        let verified = VerifiedEvent::try_from(event).unwrap();
        assert_eq!(
            PubsubPeerInterest::from_filters(
                &[trusted_rater_filter([rater.public_key()])],
                &verified,
            ),
            PubsubPeerInterest::Subscribed
        );
    }

    #[test]
    fn transported_rating_is_received_and_changes_trusted_projection() {
        let config = SimulationConfig {
            node_count: 36,
            attacker_count: 6,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        };
        let mut simulation = Simulation::new(config, PeerSelectionMode::SharedReputation).unwrap();
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        let (publisher, receiver, subject) =
            trusted_transport_triangle(&simulation).expect("connected rating transport triangle");
        let trust = peer_rating_event(
            &simulation.keys[receiver],
            &simulation.peer_ids[receiver],
            &simulation.peer_ids[publisher],
            100,
            SIM_UNIX_BASE,
        )
        .unwrap();
        assert!(
            simulation.nodes[receiver]
                .machine_reputation
                .as_mut()
                .unwrap()
                .ingest_event_at(&trust, SIM_UNIX_BASE)
                .unwrap()
        );
        let negative = peer_rating_event(
            &simulation.keys[publisher],
            &simulation.peer_ids[publisher],
            &simulation.peer_ids[subject],
            0,
            SIM_UNIX_BASE,
        )
        .unwrap();
        simulation
            .publish_reputation_event(
                publisher,
                subject,
                0,
                &negative,
                ReputationEventOrigin::HonestObservation {
                    quiet_blackhole: false,
                },
            )
            .unwrap();
        simulation.drain_scheduler().unwrap();

        assert!(simulation.report.machine_ratings_received > 0);
        assert!(simulation.report.machine_ratings_ingested > 0);
        assert!(simulation.report.machine_transported_transitions > 0);
        assert_eq!(
            simulation.report.retry_inventories,
            0,
            "a successfully received rating must cancel its scheduled inventory retries; inventory={} want={} frame={}",
            simulation.report.inventory_messages,
            simulation.report.want_messages,
            simulation.report.frame_messages,
        );
        assert!(simulation.retry_counts.is_empty());
        assert!(
            simulation.nodes[receiver]
                .machine_policies
                .as_ref()
                .unwrap()
                .select_mesh_peer(&simulation.peer_ids[subject])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn valid_compromised_trusted_rater_is_transported_and_removes_non_neighbor() {
        let config = SimulationConfig {
            node_count: 120,
            attacker_count: 24,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        };
        let mut simulation = Simulation::new(config, PeerSelectionMode::SharedReputation).unwrap();
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        admit_poison_rater_after_verified_service(&mut simulation);
        let (publisher, receiver, subject) = simulation
            .poisoned_probe_plan()
            .expect("a service-admitted machine rater can be compromised");
        assert_poison_plan_is_routable(&mut simulation, publisher, receiver, subject);

        let inventory_before = simulation.report.inventory_messages;
        let want_before = simulation.report.want_messages;
        let frame_before = simulation.report.frame_messages;
        simulation.publish_poisoned_probe().unwrap();
        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.poisoned_machine_ratings_published, 1);
        assert!(simulation.report.poisoned_machine_ratings_received > 0);
        assert!(
            simulation.report.poisoned_machine_ratings_ingested > 0,
            "{:?}",
            simulation.report
        );
        assert!(
            simulation.report.machine_poisoning_removals > 0,
            "{:?}",
            simulation.report
        );
        assert!(simulation.report.inventory_messages > inventory_before);
        assert!(simulation.report.want_messages > want_before);
        assert!(simulation.report.frame_messages > frame_before);
        assert_eq!(simulation.report.machine_false_positive_removals, 0);
        assert_poison_recipients(&simulation, publisher, subject);
    }

    fn admit_poison_rater_after_verified_service(simulation: &mut Simulation) {
        let (publisher, receiver, _) =
            trusted_transport_triangle(simulation).expect("connected rating transport triangle");
        let link = crate::simulation::DirectedServiceLink {
            source: publisher,
            destination: receiver,
        };
        simulation.verified_delivery_credits.insert(link, 3);
        simulation.verified_delivery_bytes.insert(link, 768);
        let event = peer_rating_event(
            &simulation.keys[receiver],
            &simulation.peer_ids[receiver],
            &simulation.peer_ids[publisher],
            100,
            virtual_unix_secs(simulation.scheduler.now_ms()),
        )
        .unwrap();
        simulation
            .publish_reputation_event(
                receiver,
                publisher,
                simulation.scheduler.now_ms(),
                &event,
                ReputationEventOrigin::PositiveServiceEndorsement,
            )
            .unwrap();
        simulation.flush_rediscovery_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        assert!(
            simulation.nodes[receiver]
                .service_admitted_raters
                .contains(&simulation.peer_ids[publisher])
        );
    }

    fn assert_poison_plan_is_routable(
        simulation: &mut Simulation,
        publisher: usize,
        receiver: usize,
        subject: usize,
    ) {
        let poison = peer_rating_event(
            &simulation.keys[publisher],
            &simulation.peer_ids[publisher],
            &simulation.peer_ids[subject],
            0,
            virtual_unix_secs(simulation.scheduler.now_ms()).saturating_add(1),
        )
        .unwrap();
        let verified_poison = VerifiedEvent::try_from(poison).unwrap();
        assert_eq!(
            simulation.nodes[publisher]
                .wire
                .subscriptions()
                .peer_interest(
                    &SourceId::new(&simulation.peer_ids[receiver]),
                    &verified_poison,
                ),
            PubsubPeerInterest::Subscribed,
        );
        assert!(
            simulation
                .candidate_peer(publisher, receiver)
                .unwrap()
                .is_some(),
        );
        assert!(
            simulation.nodes[receiver]
                .service_admitted_raters
                .contains(&simulation.peer_ids[publisher])
        );
        assert!(!simulation.topology.neighbors[receiver].contains(&subject));
        assert_eq!(
            peer_projection(
                simulation.nodes[receiver]
                    .machine_policies
                    .as_ref()
                    .unwrap(),
                &simulation.peer_ids[subject],
            )
            .unwrap(),
            PeerProjection::Unknown,
        );
    }

    fn assert_poison_recipients(simulation: &Simulation, publisher: usize, subject: usize) {
        let poisoned_event_id = simulation
            .reputation_events
            .iter()
            .find_map(|(event_id, metadata)| {
                (metadata.origin == ReputationEventOrigin::PoisonedProbe)
                    .then_some(event_id.as_str())
            })
            .unwrap();
        let recipients = simulation
            .rating_receipts
            .iter()
            .filter_map(|(node, event_id)| (event_id == poisoned_event_id).then_some(*node))
            .collect::<Vec<_>>();
        let mut trusted_recipients = 0usize;
        let mut untrusted_recipients = 0usize;
        let mut removals = 0usize;
        for node in &recipients {
            let trusted = simulation.nodes[*node]
                .service_admitted_raters
                .contains(&simulation.peer_ids[publisher]);
            trusted_recipients = trusted_recipients.saturating_add(usize::from(trusted));
            untrusted_recipients = untrusted_recipients.saturating_add(usize::from(!trusted));
            let removed = simulation.nodes[*node]
                .machine_policies
                .as_ref()
                .unwrap()
                .select_mesh_peer(&simulation.peer_ids[subject])
                .unwrap()
                .is_none();
            assert!(!removed || trusted, "untrusted poison changed projection");
            removals = removals.saturating_add(usize::from(removed));
        }
        assert!(trusted_recipients > 0);
        assert!(untrusted_recipients > 0);
        assert_eq!(
            simulation.report.poisoned_machine_ratings_ingested,
            recipients.len()
        );
        assert_eq!(simulation.report.machine_poisoning_removals, removals);
    }

    #[test]
    fn post_route_sweep_removes_live_blackholes_without_honest_false_positives() {
        let config = SimulationConfig {
            node_count: 48,
            attacker_count: 12,
            loss_basis_points: 0,
            churn_basis_points: 0,
            fake_inventories_per_attack_link: 4,
            ..SimulationConfig::default()
        };
        let report = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();
        assert!(report.machine_removals > 0, "{report:?}");
        assert!(report.machine_quiet_blackhole_removals > 0, "{report:?}");
        assert_eq!(report.machine_false_positive_removals, 0, "{report:?}");
    }
}
