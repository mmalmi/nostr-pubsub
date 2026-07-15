use std::collections::BTreeSet;

use nostr_pubsub::{PeerBehaviorObservation, PubsubPeerInterest, SourceId, VerifiedEvent};

use super::reputation_flow::{PeerProjection, peer_projection, virtual_unix_secs};
use super::{
    DirectedServiceLink, MachineLifecyclePhase, NodeRole, PeerSelectionMode,
    ReputationEventMetadata, ReputationEventOrigin, Result, ScheduledAction, Simulation, mix64,
    peer_rating_event, peer_rating_event_with_samples, pubsub_error,
};

pub(super) const MAX_POSITIVE_ENDORSEMENTS_PER_NODE: usize = 2;
const POSITIVE_ENDORSER_STRIDE: usize = 16;
const POSITIVE_ENDORSER_DOMAIN: u64 = 0x4d41_4348_574f_5401;
const UNCONNECTED_RATING_PRESSURE_BATCH: usize = 4;
const UNCONNECTED_RATING_KEY_DOMAIN: u64 = 0x5359_4249_4c00_0001;
const ADMITTED_RATER_POISON_TARGETS: usize = 2;
const ADMITTED_RATER_MISBEHAVIOR_FRAMES: usize = 5;
const MIN_POST_TRAINING_SERVICE_RATERS: usize = 2;
const RATER_SUBSCRIPTION_REFRESH_SETTLE_MS: u64 = 550;

struct AdmittedRaterAttackPlan {
    publisher: usize,
    receiver: usize,
    poison_targets: Vec<usize>,
    post_revocation_target: usize,
    service_credits: usize,
    service_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RatingEvidence {
    Negative { quiet_blackhole: bool },
    PositiveService,
}

pub(super) type RatingCandidate = (usize, PeerBehaviorObservation, RatingEvidence);

pub(super) fn rating_evidence(
    observation: PeerBehaviorObservation,
    verified_service_samples: usize,
) -> Option<RatingEvidence> {
    let quiet_blackhole = observation.unserved_inventories >= 4
        && observation.valid_frames == 0
        && observation.invalid_messages == 0;
    if observation.invalid_messages >= 3 || observation.unserved_inventories >= 4 {
        return Some(RatingEvidence::Negative { quiet_blackhole });
    }
    (verified_service_samples >= 3
        && observation.valid_frames >= 3
        && observation.invalid_messages == 0
        && observation.unserved_inventories == 0)
        .then_some(RatingEvidence::PositiveService)
}

pub(super) fn positive_endorsement_enabled(
    seed: u64,
    observer: usize,
    first_honest: usize,
) -> bool {
    let offset = usize::try_from(mix64(seed ^ POSITIVE_ENDORSER_DOMAIN) % 16).unwrap_or(0);
    observer
        .saturating_sub(first_honest)
        .saturating_add(offset)
        .is_multiple_of(POSITIVE_ENDORSER_STRIDE)
}

impl Simulation {
    pub(super) fn exercise_machine_lifecycle(&mut self) -> Result<()> {
        if self.mode != PeerSelectionMode::SharedReputation {
            return Ok(());
        }
        self.bootstrap_machine_lifecycle_rater()?;
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

    fn bootstrap_machine_lifecycle_rater(&mut self) -> Result<()> {
        while self.machine_lifecycle_plan().is_none()
            || self.service_admitted_rater_count() < MIN_POST_TRAINING_SERVICE_RATERS
        {
            let Some((receiver, publisher)) = self.machine_lifecycle_bootstrap_candidate() else {
                break;
            };
            let prior_count = self.service_admitted_rater_count();
            let now_ms = self.scheduler.now_ms();
            let event = peer_rating_event_with_samples(
                &self.keys[receiver],
                &self.peer_ids[receiver],
                &self.peer_ids[publisher],
                100,
                3,
                virtual_unix_secs(now_ms),
            )?;
            self.publish_observed_rating(
                receiver,
                publisher,
                now_ms,
                &event,
                RatingEvidence::PositiveService,
            )?;
            self.flush_rediscovery_subscriptions()?;
            self.drain_scheduler()?;
            if self.service_admitted_rater_count() == prior_count {
                break;
            }
        }
        Ok(())
    }

    fn machine_lifecycle_bootstrap_candidate(&self) -> Option<(usize, usize)> {
        self.verified_delivery_credits
            .iter()
            .filter(|(_, credits)| **credits >= 3)
            .filter_map(|(link, _)| {
                let bytes = self.verified_delivery_bytes.get(link).copied().unwrap_or(0);
                (bytes > 0
                    && link.source >= self.config.attacker_count
                    && link.destination >= self.config.attacker_count
                    && !self.is_admitted_rater_publisher(link.source)
                    && self.topology.roles[link.destination] == NodeRole::Peer
                    && self.topology.neighbors[link.destination].contains(&link.source)
                    && !self.nodes[link.destination]
                        .service_admitted_raters
                        .contains(&self.peer_ids[link.source])
                    && self.nodes[link.destination]
                        .machine_policies
                        .as_ref()
                        .is_some_and(|policies| {
                            peer_projection(policies, &self.peer_ids[link.source])
                                .is_ok_and(|projection| projection != PeerProjection::Removed)
                        }))
                .then_some((link.destination, link.source))
            })
            .min()
    }

    fn service_admitted_rater_count(&self) -> usize {
        self.nodes
            .iter()
            .skip(self.config.attacker_count)
            .map(|node| node.service_admitted_raters.len())
            .sum()
    }

    pub(super) fn machine_lifecycle_plan(&self) -> Option<(usize, usize, usize)> {
        for receiver in self.config.attacker_count..self.config.node_count {
            for rater in &self.nodes[receiver].service_admitted_raters {
                let publisher = self.peer_indices.get(rater).copied()?;
                if publisher < self.config.attacker_count
                    || self.is_admitted_rater_publisher(publisher)
                    || !self.topology.neighbors[receiver].contains(&publisher)
                {
                    continue;
                }
                let policies = self.nodes[receiver].machine_policies.as_ref()?;
                let subject =
                    (self.config.attacker_count..self.config.node_count).find(|subject| {
                        *subject != publisher
                            && *subject != receiver
                            && !self.topology.neighbors[receiver].contains(subject)
                            && peer_projection(policies, &self.peer_ids[*subject])
                                .is_ok_and(|projection| projection == PeerProjection::Unknown)
                    })?;
                return Some((publisher, receiver, subject));
            }
        }
        None
    }

    pub(super) fn record_positive_service_admission(&mut self, receiver: usize, subject: usize) {
        if self.topology.roles[receiver] == NodeRole::Supernode {
            return;
        }
        let link = DirectedServiceLink {
            source: subject,
            destination: receiver,
        };
        let samples = self
            .verified_delivery_credits
            .get(&link)
            .copied()
            .unwrap_or(0);
        let bytes = self
            .verified_delivery_bytes
            .get(&link)
            .copied()
            .unwrap_or(0);
        if samples >= 3
            && bytes > 0
            && self
                .positive_service_admissions
                .insert((receiver, subject), (samples, bytes))
                .is_none()
        {
            self.report.machine_positive_service_admissions = self
                .report
                .machine_positive_service_admissions
                .saturating_add(1);
            let rater = self.peer_ids[subject].clone();
            if self.nodes[receiver].service_admitted_raters.insert(rater) {
                self.mark_rating_subscription_dirty(receiver);
            }
        }
    }

    pub(super) fn record_root_rater_revocation(&mut self, receiver: usize, subject: usize) {
        if self.nodes[receiver]
            .service_admitted_raters
            .remove(&self.peer_ids[subject])
        {
            self.mark_rating_subscription_dirty(receiver);
        }
    }

    pub(super) fn record_machine_lifecycle_transition(
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

    pub(super) fn observed_rating_candidates(&self, observer: usize) -> Vec<RatingCandidate> {
        let mut candidates = self.topology.neighbors[observer]
            .iter()
            .copied()
            .filter_map(|subject| {
                self.nodes[observer]
                    .mesh
                    .peer_behavior_observation(&self.peer_ids[subject])
                    .and_then(|observation| {
                        let service_samples = self
                            .verified_delivery_credits
                            .get(&DirectedServiceLink {
                                source: subject,
                                destination: observer,
                            })
                            .copied()
                            .unwrap_or(0);
                        rating_evidence(observation, service_samples)
                            .map(|evidence| (subject, observation, evidence))
                    })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(subject, _, evidence)| {
            (
                matches!(evidence, RatingEvidence::PositiveService),
                *subject,
            )
        });
        let enabled = self.report.machine_positive_service_endorsements_published == 0
            || positive_endorsement_enabled(self.config.seed, observer, self.config.attacker_count);
        let global_limit = self
            .config
            .node_count
            .saturating_sub(self.config.attacker_count)
            .div_ceil(POSITIVE_ENDORSER_STRIDE)
            .saturating_mul(MAX_POSITIVE_ENDORSEMENTS_PER_NODE);
        let global_slots = global_limit
            .saturating_sub(self.report.machine_positive_service_endorsements_published);
        let node_slots = MAX_POSITIVE_ENDORSEMENTS_PER_NODE
            .saturating_sub(self.positive_endorsements[observer].len());
        let mut positive_slots = usize::from(enabled).saturating_mul(node_slots.min(global_slots));
        candidates.retain(|(subject, _, evidence)| match evidence {
            RatingEvidence::Negative { .. } => true,
            RatingEvidence::PositiveService
                if positive_slots > 0
                    && !self.positive_endorsements[observer].contains(subject) =>
            {
                positive_slots -= 1;
                true
            }
            RatingEvidence::PositiveService => false,
        });
        candidates
    }

    pub(super) fn publish_admitted_rater_poison_probe(&mut self) -> Result<()> {
        if self.report.admitted_rater_poison_published > 0
            || self.report.machine_positive_service_endorsements_published == 0
        {
            return Ok(());
        }
        let Some(plan) = self.admitted_rater_poison_plan()? else {
            return Ok(());
        };
        self.report.admitted_rater_poison_service_admitted_rater = 1;
        self.report.admitted_rater_service_credits = plan.service_credits;
        self.report.admitted_rater_service_bytes = plan.service_bytes;
        self.admitted_rater_poison_source = Some((plan.publisher, plan.receiver));
        self.admitted_rater_post_revocation_target =
            Some((plan.receiver, plan.post_revocation_target));
        for subject in plan.poison_targets {
            if self.topology.neighbors[plan.receiver].contains(&subject) {
                return Err(pubsub_error(
                    "admitted-rater poison target must be a receiver non-neighbor",
                ));
            }
            let unknown_before = self.nodes[plan.receiver]
                .machine_policies
                .as_ref()
                .is_some_and(|policies| {
                    peer_projection(policies, &self.peer_ids[subject])
                        .is_ok_and(|projection| projection == PeerProjection::Unknown)
                });
            if !unknown_before {
                return Err(pubsub_error(
                    "admitted-rater poison target was not Unknown immediately before publication",
                ));
            }
            let event = peer_rating_event(
                &self.keys[plan.publisher],
                &self.peer_ids[plan.publisher],
                &self.peer_ids[subject],
                0,
                virtual_unix_secs(self.scheduler.now_ms()),
            )?;
            self.admitted_rater_poison_targets
                .insert((plan.receiver, subject));
            self.report.admitted_rater_poison_published = self
                .report
                .admitted_rater_poison_published
                .saturating_add(1);
            self.report.admitted_rater_poison_target_unknown_before = self
                .report
                .admitted_rater_poison_target_unknown_before
                .saturating_add(1);
            self.publish_reputation_event(
                plan.publisher,
                subject,
                self.scheduler.now_ms(),
                &event,
                ReputationEventOrigin::AdmittedRaterPoison,
            )?;
        }
        for sample in 0..ADMITTED_RATER_MISBEHAVIOR_FRAMES {
            self.enqueue_raw_packet_at(
                plan.publisher,
                plan.receiver,
                self.scheduler
                    .now_ms()
                    .saturating_add(500)
                    .saturating_add(u64::try_from(sample).unwrap_or(0)),
                format!("compromised-rater-{sample}").into_bytes(),
            );
            self.report.admitted_rater_misbehavior_frames = self
                .report
                .admitted_rater_misbehavior_frames
                .saturating_add(1);
        }
        self.scheduler
            .schedule_after(550, ScheduledAction::ReputationSweep);
        Ok(())
    }

    pub(super) fn publish_admitted_rater_revocation(&mut self, now_ms: u64) -> Result<()> {
        if self.report.admitted_rater_revocations > 0 {
            return self.publish_post_revocation_rating();
        }
        let Some((publisher, receiver)) = self.admitted_rater_poison_source else {
            return Ok(());
        };
        let Some(observation) = self.nodes[receiver]
            .mesh
            .peer_behavior_observation(&self.peer_ids[publisher])
        else {
            return Ok(());
        };
        let Some(evidence @ RatingEvidence::Negative { .. }) =
            rating_evidence(observation, usize::MAX)
        else {
            return Ok(());
        };
        let event = peer_rating_event_with_samples(
            &self.keys[receiver],
            &self.peer_ids[receiver],
            &self.peer_ids[publisher],
            0,
            u64::from(observation.samples),
            virtual_unix_secs(now_ms),
        )?;
        if !self.reputation_publishers[receiver]
            .as_ref()
            .is_some_and(|ratings| ratings.should_publish_event(&event, now_ms))
        {
            return Ok(());
        }
        let removed_targets = self
            .admitted_rater_poison_targets
            .iter()
            .filter_map(|(node, subject)| {
                (*node == receiver
                    && self.nodes[receiver]
                        .machine_policies
                        .as_ref()
                        .is_some_and(|policies| {
                            peer_projection(policies, &self.peer_ids[*subject])
                                .is_ok_and(|state| state == PeerProjection::Removed)
                        }))
                .then_some(*subject)
            })
            .collect::<Vec<_>>();
        let observed_at_ms = self
            .bad_observed_at
            .get(&(receiver, publisher))
            .copied()
            .unwrap_or(now_ms);
        self.publish_observed_rating(receiver, publisher, observed_at_ms, &event, evidence)?;
        if self.report.admitted_rater_revocations == 0 {
            return Ok(());
        }
        if let Some(policies) = self.nodes[receiver].machine_policies.as_ref() {
            self.report.admitted_rater_poison_target_recoveries = removed_targets
                .iter()
                .filter(|subject| {
                    peer_projection(policies, &self.peer_ids[**subject])
                        .is_ok_and(|state| state == PeerProjection::Unknown)
                })
                .count();
        }
        self.scheduler.schedule_after(
            RATER_SUBSCRIPTION_REFRESH_SETTLE_MS,
            ScheduledAction::ReputationSweep,
        );
        Ok(())
    }

    pub(super) fn publish_unconnected_rating_pressure(&mut self) -> Result<()> {
        if self.report.unconnected_rating_pressure_published > 0 {
            return Ok(());
        }
        let Some((publisher, receiver, anchor)) = self.unconnected_rating_pressure_plan() else {
            return Ok(());
        };
        let baseline = self.nodes[receiver]
            .machine_reputation
            .as_ref()
            .expect("honest simulation node has machine reputation")
            .snapshot();
        self.unconnected_rating_pressure_target = Some((
            receiver,
            anchor,
            baseline.retained_ratings,
            baseline.retained_raters,
            baseline.graph_rebuild_rating_entries,
        ));
        self.report
            .unconnected_rating_pressure_anchor_positive_before = 1;
        let mut raters = BTreeSet::new();
        for offset in 0..UNCONNECTED_RATING_PRESSURE_BATCH {
            let signer = nostr::Keys::parse(&format!(
                "{:064x}",
                UNCONNECTED_RATING_KEY_DOMAIN.saturating_add(u64::try_from(offset).unwrap_or(0))
            ))
            .map_err(pubsub_error)?;
            let rater = signer.public_key().to_hex();
            raters.insert(rater.clone());
            let event = peer_rating_event(
                &signer,
                &rater,
                &self.peer_ids[anchor],
                0,
                virtual_unix_secs(self.scheduler.now_ms()),
            )?;
            self.report.unconnected_rating_pressure_published = self
                .report
                .unconnected_rating_pressure_published
                .saturating_add(1);
            self.publish_reputation_event(
                publisher,
                anchor,
                self.scheduler.now_ms(),
                &event,
                ReputationEventOrigin::UnconnectedRatingPressure,
            )?;
        }
        self.report.unconnected_rating_pressure_distinct_raters = raters.len();
        Ok(())
    }

    pub(super) fn unconnected_rating_pressure_baseline(
        &self,
        node: usize,
        subject: usize,
    ) -> Option<(usize, usize, u64)> {
        self.unconnected_rating_pressure_target.and_then(
            |(receiver, anchor, ratings, raters, rebuild_entries)| {
                (receiver == node && anchor == subject).then_some((
                    ratings,
                    raters,
                    rebuild_entries,
                ))
            },
        )
    }

    fn unconnected_rating_pressure_plan(&self) -> Option<(usize, usize, usize)> {
        for receiver in self.config.attacker_count..self.config.node_count {
            let Some(policies) = self.nodes[receiver].machine_policies.as_ref() else {
                continue;
            };
            let anchor = self.nodes[receiver]
                .service_admitted_raters
                .iter()
                .filter_map(|rater| self.peer_indices.get(rater).copied())
                .find(|anchor| {
                    self.topology.neighbors[receiver].contains(anchor)
                        && peer_projection(policies, &self.peer_ids[*anchor])
                            .is_ok_and(|projection| projection == PeerProjection::Positive)
                });
            let Some(anchor) = anchor else {
                continue;
            };
            let publisher = self.topology.neighbors[receiver]
                .iter()
                .copied()
                .find(|publisher| {
                    *publisher != anchor
                        && !self.is_admitted_rater_publisher(*publisher)
                        && peer_projection(policies, &self.peer_ids[*publisher])
                            .is_ok_and(|projection| projection != PeerProjection::Removed)
                });
            if let Some(publisher) = publisher {
                return Some((publisher, receiver, anchor));
            }
        }
        None
    }

    pub(super) fn is_admitted_rater_poison_target(&self, node: usize, subject: usize) -> bool {
        self.admitted_rater_poison_targets
            .contains(&(node, subject))
    }

    pub(super) fn is_admitted_rater_source(&self, node: usize, subject: usize) -> bool {
        self.admitted_rater_poison_source == Some((subject, node))
    }

    pub(super) fn is_admitted_rater_publisher(&self, subject: usize) -> bool {
        self.admitted_rater_poison_source
            .is_some_and(|(publisher, _)| publisher == subject)
    }

    pub(super) fn is_post_revocation_target(&self, node: usize, subject: usize) -> bool {
        self.admitted_rater_post_revocation_target == Some((node, subject))
    }

    pub(super) fn publish_post_revocation_rating(&mut self) -> Result<()> {
        if self.report.post_revocation_rating_published > 0 {
            return Ok(());
        }
        let Some((publisher, receiver)) = self.admitted_rater_poison_source else {
            return Ok(());
        };
        let Some((_, subject)) = self.admitted_rater_post_revocation_target else {
            return Ok(());
        };
        let Some(relay) = self.topology.neighbors[receiver]
            .iter()
            .copied()
            .find(|relay| *relay >= self.config.attacker_count && *relay != publisher)
        else {
            return Ok(());
        };
        let event = peer_rating_event(
            &self.keys[publisher],
            &self.peer_ids[publisher],
            &self.peer_ids[subject],
            0,
            virtual_unix_secs(self.scheduler.now_ms()),
        )?;
        let verified = VerifiedEvent::try_from(event.clone()).map_err(pubsub_error)?;
        let receiver_id = SourceId::new(&self.peer_ids[receiver]);
        if self.nodes[receiver]
            .service_admitted_raters
            .contains(&self.peer_ids[publisher])
            || PubsubPeerInterest::from_filters(&self.nodes[receiver].rating_filters, &verified)
                != PubsubPeerInterest::Unsubscribed
            || self.nodes[relay]
                .wire
                .subscriptions()
                .peer_interest(&receiver_id, &verified)
                != PubsubPeerInterest::Unsubscribed
        {
            return Err(pubsub_error(
                "root revocation must remove the rater-author FIPS subscription before replay",
            ));
        }
        self.report.post_revocation_rating_published = 1;
        self.publish_reputation_event(
            relay,
            subject,
            self.scheduler.now_ms(),
            &event,
            ReputationEventOrigin::RevokedRaterRating,
        )
    }

    fn admitted_rater_poison_plan(&self) -> Result<Option<AdmittedRaterAttackPlan>> {
        let admissions = self
            .positive_service_admissions
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        for (receiver, publisher) in admissions {
            let Some(policies) = self.nodes[receiver].machine_policies.as_ref() else {
                continue;
            };
            if publisher < self.config.attacker_count
                || !self.topology.neighbors[receiver].contains(&publisher)
                || !self.nodes[receiver]
                    .service_admitted_raters
                    .contains(&self.peer_ids[publisher])
                || peer_projection(policies, &self.peer_ids[publisher])? != PeerProjection::Positive
            {
                continue;
            }
            let subjects = (self.config.attacker_count..self.config.node_count)
                .filter(|subject| *subject >= self.config.attacker_count)
                .filter(|subject| {
                    *subject != publisher
                        && *subject != receiver
                        && !self.topology.neighbors[receiver].contains(subject)
                        && peer_projection(policies, &self.peer_ids[*subject])
                            .is_ok_and(|projection| projection == PeerProjection::Unknown)
                })
                .take(ADMITTED_RATER_POISON_TARGETS.saturating_add(1))
                .collect::<Vec<_>>();
            if subjects.len() > ADMITTED_RATER_POISON_TARGETS {
                let (service_credits, service_bytes) = self
                    .positive_service_admissions
                    .get(&(receiver, publisher))
                    .copied()
                    .unwrap_or_default();
                return Ok(Some(AdmittedRaterAttackPlan {
                    publisher,
                    receiver,
                    poison_targets: subjects[..ADMITTED_RATER_POISON_TARGETS].to_vec(),
                    post_revocation_target: subjects[ADMITTED_RATER_POISON_TARGETS],
                    service_credits,
                    service_bytes,
                }));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_endorsement_requires_three_verified_service_frames() {
        let observation = PeerBehaviorObservation {
            score: 30,
            samples: 3,
            valid_frames: 3,
            invalid_messages: 0,
            unserved_inventories: 0,
        };
        assert_eq!(rating_evidence(observation, 2), None);
        assert_eq!(
            rating_evidence(observation, 3),
            Some(RatingEvidence::PositiveService)
        );
    }

    #[test]
    fn negative_evidence_wins_over_valid_service() {
        let observation = PeerBehaviorObservation {
            score: 10,
            samples: 7,
            valid_frames: 4,
            invalid_messages: 3,
            unserved_inventories: 0,
        };
        assert!(matches!(
            rating_evidence(observation, 4),
            Some(RatingEvidence::Negative { .. })
        ));
    }

    #[test]
    fn confirmed_unserved_inventory_overrides_successful_service() {
        let observation = PeerBehaviorObservation {
            score: 100,
            samples: 14,
            valid_frames: 10,
            invalid_messages: 0,
            unserved_inventories: 4,
        };
        assert_eq!(
            rating_evidence(observation, 10),
            Some(RatingEvidence::Negative {
                quiet_blackhole: false
            })
        );
    }

    #[test]
    fn quiet_blackhole_remains_negative_evidence() {
        let observation = PeerBehaviorObservation {
            score: -80,
            samples: 4,
            valid_frames: 0,
            invalid_messages: 0,
            unserved_inventories: 4,
        };
        assert_eq!(
            rating_evidence(observation, 0),
            Some(RatingEvidence::Negative {
                quiet_blackhole: true
            })
        );
    }

    #[test]
    fn repeated_selective_failure_remains_negative_evidence() {
        let observation = PeerBehaviorObservation {
            score: -20,
            samples: 11,
            valid_frames: 5,
            invalid_messages: 0,
            unserved_inventories: 6,
        };
        assert_eq!(
            rating_evidence(observation, 5),
            Some(RatingEvidence::Negative {
                quiet_blackhole: false
            })
        );
    }

    #[test]
    fn positive_endorser_sampling_is_bounded_and_nonempty() {
        let selected = (20..100)
            .filter(|observer| positive_endorsement_enabled(7, *observer, 20))
            .count();
        assert!((5..=6).contains(&selected));
    }
}
