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
    ) -> bool {
        let rejected = self.mode == PeerSelectionMode::SharedReputation
            && self.nodes[destination]
                .machine_policies
                .as_ref()
                .is_some_and(|policies| {
                    policies
                        .select_mesh_peer(&self.peer_ids[source])
                        .is_ok_and(|selected| selected.is_none())
                });
        if !rejected {
            return false;
        }
        self.report.machine_ingress_drops = self.report.machine_ingress_drops.saturating_add(1);
        match (provenance, self.topology.roles[source]) {
            (TrafficProvenance::Legitimate, NodeRole::Attacker) => {
                self.report
                    .attacker_source_legitimate_reference_machine_ingress_drops = self
                    .report
                    .attacker_source_legitimate_reference_machine_ingress_drops
                    .saturating_add(1);
            }
            (TrafficProvenance::Legitimate, _) => {
                self.report.honest_source_legitimate_machine_ingress_drops = self
                    .report
                    .honest_source_legitimate_machine_ingress_drops
                    .saturating_add(1);
            }
            (TrafficProvenance::Adversarial, _) => {
                self.report.adversarial_machine_ingress_drops = self
                    .report
                    .adversarial_machine_ingress_drops
                    .saturating_add(1);
            }
        }
        debug_assert!(self.report.machine_ingress_accounting_is_conserved());
        true
    }

    pub(super) fn record_policy_drop(&mut self, event_id: &str, drop: AdmissionDrop) {
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
        &self,
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
        let Some(policies) = self.nodes[destination].machine_policies.as_ref() else {
            return Ok(None);
        };
        let decision =
            poll_ready(policies.check_event(event.as_event(), &source))?.map_err(pubsub_error)?;
        Ok(matches!(decision, PolicyDecision::Drop { .. })
            .then_some(AdmissionDrop::MachineReputation))
    }
}
