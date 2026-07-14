use std::time::Duration;

use nostr::{Alphabet, Event, Filter, Kind, SingleLetterTag};
use nostr_pubsub::{PeerBehaviorObservation, VerifiedEvent};
use nostr_pubsub_social_graph::{
    DEFAULT_PEER_RATING_SCOPE, PeerRatingPublisher, PeerRatingPublisherConfig,
    PeerReputationPolicies,
};
use nostr_social_memory::RATING_KIND;

use crate::topology::NodeRole;

use super::{
    MachineLifecyclePhase, PeerSelectionMode, ReputationEventMetadata, ReputationEventOrigin,
    Result, SIM_UNIX_BASE, ScheduledAction, Simulation, SimulationConfig, peer_rating_event,
    peer_rating_event_with_samples, pubsub_error,
};

const RATING_BATCH_SIZE: usize = 2;
const RATING_MIN_PUBLISH_INTERVAL_MS: u64 = 50;
const RATING_REFRESH_INTERVAL_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerProjection {
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
    pub(super) fn initialize_rating_filters(&mut self) -> Result<()> {
        for subscriber in 0..self.config.node_count {
            self.nodes[subscriber].rating_filters = self.rating_filters_for(subscriber)?;
        }
        Ok(())
    }

    pub(in crate::simulation) fn subscription_filters_for(&self, subscriber: usize) -> Vec<Filter> {
        let mut filters = self.nodes[subscriber].filters.clone();
        filters.extend(self.nodes[subscriber].rating_filters.clone());
        filters
    }

    fn rating_filters_for(&self, subscriber: usize) -> Result<Vec<Filter>> {
        let mut filters = Vec::new();
        if self.mode != PeerSelectionMode::SharedReputation
            || subscriber < self.config.attacker_count
        {
            return Ok(filters);
        }
        filters.push(reputation_filter(
            std::iter::once(self.keys[subscriber].public_key()).chain(
                self.topology.neighbors[subscriber]
                    .iter()
                    .map(|peer| self.keys[*peer].public_key()),
            ),
        ));
        let trusted_raters = self.nodes[subscriber]
            .machine_trusted_raters
            .iter()
            .map(|rater| nostr::PublicKey::parse(rater).map_err(pubsub_error))
            .collect::<Result<Vec<_>>>()?;
        if !trusted_raters.is_empty() {
            filters.push(trusted_rater_filter(trusted_raters));
        }
        Ok(filters)
    }

    pub(super) fn exercise_machine_lifecycle(&mut self) -> Result<()> {
        if self.mode != PeerSelectionMode::SharedReputation {
            return Ok(());
        }
        let Some((publisher, _receiver, subject)) = self.machine_lifecycle_plan() else {
            return Ok(());
        };
        for (index, (phase, value)) in [
            (MachineLifecyclePhase::Admit, 100),
            (MachineLifecyclePhase::Remove, 0),
            (MachineLifecyclePhase::Readmit, 100),
        ]
        .into_iter()
        .enumerate()
        {
            if index > 0 {
                self.scheduler
                    .schedule_after(1_000, ScheduledAction::AdvanceVirtualTime);
                self.drain_scheduler()?;
            }
            let now_ms = self.scheduler.now_ms();
            let event = peer_rating_event(
                &self.keys[publisher],
                &self.peer_ids[publisher],
                &self.peer_ids[subject],
                value,
                virtual_unix_secs(now_ms),
            )?;
            self.report.machine_lifecycle_ratings_published = self
                .report
                .machine_lifecycle_ratings_published
                .saturating_add(1);
            self.publish_reputation_event(
                publisher,
                subject,
                now_ms,
                &event,
                ReputationEventOrigin::MachineLifecycle(phase),
            )?;
            self.drain_scheduler()?;
        }
        Ok(())
    }

    fn machine_lifecycle_plan(&self) -> Option<(usize, usize, usize)> {
        for receiver in self.config.attacker_count..self.config.node_count {
            for rater in &self.nodes[receiver].machine_trusted_raters {
                let publisher = self.peer_indices.get(rater).copied()?;
                if publisher < self.config.attacker_count
                    || !self.topology.neighbors[receiver].contains(&publisher)
                {
                    continue;
                }
                let subject =
                    (self.config.attacker_count..self.config.node_count).find(|subject| {
                        *subject != publisher
                            && *subject != receiver
                            && self.topology.roles[*subject] == NodeRole::Peer
                    })?;
                return Some((publisher, receiver, subject));
            }
        }
        None
    }

    pub(super) fn run_reputation_sweep(&mut self) -> Result<()> {
        if self.mode != PeerSelectionMode::SharedReputation {
            return Ok(());
        }
        let now_ms = self.scheduler.now_ms();
        for node in self.config.attacker_count..self.config.node_count {
            self.nodes[node].mesh.maintain(now_ms);
            self.observe_core_resource_state(node);
        }
        for observer in self.config.attacker_count..self.config.node_count {
            let mut candidates = self.topology.neighbors[observer]
                .iter()
                .copied()
                .filter_map(|subject| {
                    self.nodes[observer]
                        .mesh
                        .peer_behavior_observation(&self.peer_ids[subject])
                        .filter(|observation| rating_evidence_is_sufficient(*observation))
                        .map(|observation| (subject, observation))
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(subject, _)| *subject);
            let batch_size = self.reputation_publishers[observer]
                .as_ref()
                .map_or(0, PeerRatingPublisher::batch_size);
            let now_secs = virtual_unix_secs(now_ms);
            let publisher_ms = now_ms;
            let mut due = Vec::new();
            for (subject, observation) in candidates {
                let event = peer_rating_event_with_samples(
                    &self.keys[observer],
                    &self.peer_ids[observer],
                    &self.peer_ids[subject],
                    score_to_rating(observation.score),
                    u64::from(observation.samples),
                    now_secs,
                )?;
                if self.reputation_publishers[observer]
                    .as_ref()
                    .is_some_and(|publisher| publisher.should_publish_event(&event, publisher_ms))
                {
                    let observed_at_ms = self
                        .bad_observed_at
                        .get(&(observer, subject))
                        .copied()
                        .unwrap_or(now_ms);
                    let quiet_blackhole = observation.unserved_inventories >= 4
                        && observation.valid_frames == 0
                        && observation.invalid_messages == 0;
                    due.push((subject, observed_at_ms, event, quiet_blackhole));
                }
                if due.len() >= batch_size {
                    break;
                }
            }
            for (subject, observed_at_ms, event, quiet_blackhole) in due {
                self.publish_reputation_event(
                    observer,
                    subject,
                    observed_at_ms,
                    &event,
                    ReputationEventOrigin::HonestObservation { quiet_blackhole },
                )?;
                let recorded =
                    self.reputation_publishers[observer]
                        .as_mut()
                        .is_some_and(|publisher| {
                            publisher.record_published_event(&event, publisher_ms)
                        });
                if recorded {
                    self.report.machine_ratings_published =
                        self.report.machine_ratings_published.saturating_add(1);
                }
            }
        }
        Ok(())
    }

    pub(super) fn exercise_adversarial_reputation_probes(&mut self) -> Result<()> {
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
        match self
            .reputation_events
            .get(&event_id)
            .map(|metadata| metadata.origin)
        {
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
            Some(
                ReputationEventOrigin::HonestObservation { .. }
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
        self.retain_local_event(publisher, event_id, event.clone())?;
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
        if !ingested {
            return Ok(());
        }
        self.record_cpu_work(node, |work| {
            work.graph_queries = work.graph_queries.saturating_add(1);
        });
        let after = peer_projection(&policies, &self.peer_ids[metadata.subject])?;
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
            self.record_machine_removal(metadata, transported);
        }
    }

    fn record_machine_removal(&mut self, metadata: &ReputationEventMetadata, transported: bool) {
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
        let honest_false_positive = matches!(
            metadata.origin,
            ReputationEventOrigin::HonestObservation { .. }
        ) && self.topology.roles[metadata.subject]
            != NodeRole::Attacker;
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

    fn record_machine_lifecycle_transition(
        &mut self,
        node: usize,
        metadata: &ReputationEventMetadata,
        before: PeerProjection,
        after: PeerProjection,
        transported: bool,
    ) {
        let ReputationEventOrigin::MachineLifecycle(phase) = metadata.origin else {
            return;
        };
        if !transported || before == after {
            return;
        }
        let progress = self
            .machine_lifecycle_progress
            .entry((node, metadata.subject))
            .or_default();
        match phase {
            MachineLifecyclePhase::Admit if *progress == 0 && after == PeerProjection::Positive => {
                *progress = 1;
                self.report.machine_lifecycle_admissions =
                    self.report.machine_lifecycle_admissions.saturating_add(1);
            }
            MachineLifecyclePhase::Remove if *progress == 1 && after == PeerProjection::Removed => {
                *progress = 2;
                self.report.machine_lifecycle_removals =
                    self.report.machine_lifecycle_removals.saturating_add(1);
            }
            MachineLifecyclePhase::Readmit
                if *progress == 2 && after == PeerProjection::Positive =>
            {
                *progress = 3;
                self.report.machine_lifecycle_readmissions =
                    self.report.machine_lifecycle_readmissions.saturating_add(1);
                self.report.machine_reversible_lifecycles =
                    self.report.machine_reversible_lifecycles.saturating_add(1);
            }
            _ => {}
        }
    }
}

fn reputation_filter(pubkeys: impl IntoIterator<Item = nostr::PublicKey>) -> Filter {
    Filter::new()
        .kind(Kind::Custom(RATING_KIND))
        .custom_tag(
            SingleLetterTag::lowercase(Alphabet::I),
            DEFAULT_PEER_RATING_SCOPE,
        )
        .pubkeys(pubkeys)
}

fn trusted_rater_filter(authors: impl IntoIterator<Item = nostr::PublicKey>) -> Filter {
    Filter::new()
        .kind(Kind::Custom(RATING_KIND))
        .custom_tag(
            SingleLetterTag::lowercase(Alphabet::I),
            DEFAULT_PEER_RATING_SCOPE,
        )
        .authors(authors)
}

fn score_to_rating(score: i32) -> i64 {
    i64::from(score.clamp(-100, 100)).saturating_add(100) / 2
}

fn rating_evidence_is_sufficient(observation: PeerBehaviorObservation) -> bool {
    observation.invalid_messages >= 3 || observation.unserved_inventories >= 4
}

pub(super) fn virtual_unix_secs(now_ms: u64) -> u64 {
    SIM_UNIX_BASE.saturating_add(now_ms / 1_000)
}

fn peer_projection(policies: &PeerReputationPolicies, peer_id: &str) -> Result<PeerProjection> {
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
fn trusted_transport_triangle(simulation: &Simulation) -> Option<(usize, usize, usize)> {
    for publisher in simulation.config.attacker_count..simulation.config.node_count {
        for receiver in simulation.topology.neighbors[publisher]
            .iter()
            .copied()
            .filter(|peer| *peer >= simulation.config.attacker_count)
        {
            if let Some(subject) = simulation.topology.neighbors[receiver]
                .iter()
                .copied()
                .find(|peer| *peer < simulation.config.attacker_count)
            {
                return Some((publisher, receiver, subject));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use nostr::Keys;
    use nostr_pubsub::{PubsubPeerInterest, SourceId, VerifiedEvent};

    use super::{
        PeerSelectionMode, ReputationEventOrigin, SIM_UNIX_BASE, Simulation, SimulationConfig,
        peer_rating_event, reputation_filter, trusted_rater_filter, trusted_transport_triangle,
        virtual_unix_secs,
    };
    use crate::simulation::run_simulation;

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
    fn production_rating_filter_matches_signed_fips_peer_rating() {
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
        let (publisher, receiver, subject) = simulation
            .poisoned_probe_plan()
            .expect("a configured machine rater can be compromised");
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
                .machine_trusted_raters
                .contains(&simulation.peer_ids[publisher])
        );
        assert!(!simulation.topology.neighbors[receiver].contains(&subject));
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
                .machine_trusted_raters
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
