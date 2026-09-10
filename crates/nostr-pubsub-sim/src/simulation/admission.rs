use super::{
    AdmissionDrop, EventSource, NodeRole, PeerSelectionMode, PolicyDecision, Result, Simulation,
    SubscriptionClass, TrafficProvenance, VerifiedEvent, machine_admitted_class, poll_ready,
    pubsub_error,
};

impl Simulation {
    pub(super) fn machine_rejects_ingress(
        &mut self,
        source: usize,
        destination: usize,
        provenance: TrafficProvenance,
        lifecycle_control: bool,
    ) -> bool {
        if self.mode == PeerSelectionMode::SharedReputation {
            self.record_cpu_work(destination, |work| {
                work.graph_queries = work.graph_queries.saturating_add(1);
            });
        }
        let rejected = self.mode == PeerSelectionMode::SharedReputation
            && (self.nodes[destination]
                .mesh
                .peer_behavior_observation(&self.peer_ids[source])
                .is_some_and(confident_local_rejection)
                || self.nodes[destination]
                    .machine_policies
                    .as_ref()
                    .is_some_and(|policies| {
                        policies
                            .select_mesh_peer(&self.peer_ids[source])
                            .is_ok_and(|selected| selected.is_none())
                    }));
        if !rejected {
            return false;
        }
        self.report.machine_ingress_drops = self.report.machine_ingress_drops.saturating_add(1);
        let adversarial_source = self.topology.roles[source] == NodeRole::Attacker
            || self
                .admitted_rater_poison_source
                .is_some_and(|(publisher, _)| publisher == source);
        match (lifecycle_control, provenance, adversarial_source) {
            (true, _, _) => {
                self.report.lifecycle_control_machine_ingress_drops = self
                    .report
                    .lifecycle_control_machine_ingress_drops
                    .saturating_add(1);
            }
            (false, TrafficProvenance::Legitimate, true) => {
                self.report
                    .adversarial_source_legitimate_reference_machine_ingress_drops = self
                    .report
                    .adversarial_source_legitimate_reference_machine_ingress_drops
                    .saturating_add(1);
            }
            (false, TrafficProvenance::Legitimate, false) => {
                self.report.honest_source_legitimate_machine_ingress_drops = self
                    .report
                    .honest_source_legitimate_machine_ingress_drops
                    .saturating_add(1);
            }
            (false, TrafficProvenance::Adversarial, _) => {
                self.report.adversarial_machine_ingress_drops = self
                    .report
                    .adversarial_machine_ingress_drops
                    .saturating_add(1);
            }
        }
        debug_assert!(self.report.machine_ingress_accounting_is_conserved());
        true
    }

    pub(super) fn record_policy_drop(
        &mut self,
        destination: usize,
        author: usize,
        event_id: &str,
        drop: AdmissionDrop,
    ) {
        let legitimate = self
            .reputation_events
            .get(event_id)
            .is_some_and(|metadata| !metadata.origin.is_spam())
            || self
                .events
                .get(event_id)
                .is_some_and(|metadata| metadata.legitimate);
        if legitimate {
            self.report.legitimate_policy_drops =
                self.report.legitimate_policy_drops.saturating_add(1);
            if self.topology.roles[author] == NodeRole::Attacker
                || self.is_admitted_rater_publisher(author)
            {
                self.report
                    .adversarial_author_legitimate_reference_policy_drops = self
                    .report
                    .adversarial_author_legitimate_reference_policy_drops
                    .saturating_add(1);
            }
            if drop == AdmissionDrop::Application {
                self.report.legitimate_application_policy_drops = self
                    .report
                    .legitimate_application_policy_drops
                    .saturating_add(1);
            }
            return;
        }
        match drop {
            AdmissionDrop::MachineReputation => {
                self.report.spam_dropped_by_machine_policy =
                    self.report.spam_dropped_by_machine_policy.saturating_add(1);
                if self
                    .reputation_events
                    .get(event_id)
                    .is_some_and(|metadata| {
                        metadata.origin == super::ReputationEventOrigin::RevokedRaterRating
                            && self.is_post_revocation_target(destination, metadata.subject)
                    })
                {
                    // This probe KPI is a unique target/event policy outcome;
                    // general machine-drop counters still retain route attempts.
                    self.report.post_revocation_rating_target_policy_drops = 1;
                }
            }
            AdmissionDrop::Application => {
                self.report.spam_dropped_by_application_policy = self
                    .report
                    .spam_dropped_by_application_policy
                    .saturating_add(1);
            }
        }
    }

    pub(super) fn admit_event(
        &mut self,
        destination: usize,
        source: usize,
        event: &VerifiedEvent,
    ) -> Result<Option<AdmissionDrop>> {
        if self.mode != PeerSelectionMode::SharedReputation {
            return Ok(None);
        }
        let source = EventSource::fips_endpoint(&self.peer_ids[source]);
        let event_id = event.as_event().id.to_hex();
        let class = self.events.get(&event_id).map(|metadata| metadata.class);
        let is_reputation_rating = self.reputation_events.contains_key(&event_id);
        if class == Some(SubscriptionClass::IrisDriveBroadRoot) {
            return Ok((!self.nodes[destination]
                .app_authorized_authors
                .contains(&event.as_event().pubkey.to_hex()))
            .then_some(AdmissionDrop::Application));
        }
        let machine_admitted = is_reputation_rating || class.is_some_and(machine_admitted_class);
        if !machine_admitted {
            return Ok(None);
        }
        if self.nodes[destination].machine_policies.is_none() {
            return Ok(None);
        }
        self.record_cpu_work(destination, |work| {
            work.graph_queries = work.graph_queries.saturating_add(1);
        });
        self.record_avoided_signature_check(destination);
        let policies = self.nodes[destination]
            .machine_policies
            .as_ref()
            .expect("machine policy checked above");
        let decision =
            poll_ready(policies.check_verified_event(event, &source))?.map_err(pubsub_error)?;
        Ok(matches!(decision, PolicyDecision::Drop { .. })
            .then_some(AdmissionDrop::MachineReputation))
    }
}

fn confident_local_rejection(observation: nostr_pubsub::PeerBehaviorObservation) -> bool {
    observation.invalid_messages >= 3
        || (observation.unserved_inventories >= 6 && observation.valid_frames == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::reputation_flow::{PeerProjection, peer_projection, virtual_unix_secs};
    use crate::simulation::{
        InvWantWireMessage, MachineLifecyclePhase, Packet, ReputationEventMetadata,
        ReputationEventOrigin, SimulationConfig, peer_rating_event,
    };

    #[test]
    fn lifecycle_control_drops_cannot_hide_real_workload_from_the_same_peer() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                node_count: 48,
                attacker_count: 0,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        let workload = simulation
            .events
            .values()
            .filter(|event| event.legitimate)
            .min_by_key(|event| (event.publisher, event.verified.as_event().id))
            .unwrap()
            .clone();
        let source = workload.publisher;
        let destination = simulation.topology.neighbors[source][0];
        let now_ms = simulation.scheduler.now_ms();
        let rating = peer_rating_event(
            &simulation.keys[destination],
            &simulation.peer_ids[destination],
            &simulation.peer_ids[source],
            0,
            virtual_unix_secs(now_ms),
        )
        .unwrap();
        simulation.nodes[destination]
            .machine_reputation
            .as_mut()
            .unwrap()
            .ingest_event_at(&rating, virtual_unix_secs(now_ms))
            .unwrap();
        assert_eq!(
            peer_projection(
                simulation.nodes[destination]
                    .machine_policies
                    .as_ref()
                    .unwrap(),
                &simulation.peer_ids[source],
            )
            .unwrap(),
            PeerProjection::Removed
        );
        simulation.reputation_events.insert(
            rating.id.to_hex(),
            ReputationEventMetadata {
                subject: source,
                observed_at_ms: now_ms,
                origin: ReputationEventOrigin::MachineLifecycle(MachineLifecyclePhase::Remove),
            },
        );
        for message in [
            InvWantWireMessage::Want {
                event_id: rating.id.to_hex(),
            },
            InvWantWireMessage::Frame {
                event_id: workload.verified.as_event().id.to_hex(),
                event: Box::new(workload.verified.as_event().clone()),
            },
        ] {
            simulation
                .process_packet(Packet {
                    source,
                    destination,
                    payload: simulation.codec.encode(&message).unwrap(),
                })
                .unwrap();
        }
        assert_eq!(simulation.report.machine_ingress_drops, 2);
        assert_eq!(simulation.report.lifecycle_control_machine_ingress_drops, 1);
        assert_eq!(
            simulation
                .report
                .honest_source_legitimate_machine_ingress_drops,
            1
        );
        assert_eq!(simulation.report.adversarial_machine_ingress_drops, 0);
        assert!(simulation.report.machine_ingress_accounting_is_conserved());
    }
}
