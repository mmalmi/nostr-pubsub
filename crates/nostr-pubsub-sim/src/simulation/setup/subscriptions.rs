use super::super::{
    Filter, FipsPubsubWireMessage, NodeRole, PubsubPeerInterest, Result, Simulation,
    SimulationError, SpamIdentity, SubscriptionFrame, SubscriptionId, SubscriptionPurpose,
    SubscriptionStore, TrafficProvenance, is_quiet_attacker, machine_admitted_class,
    profile_subscription_id,
};

impl Simulation {
    pub(super) fn populate_interest_sets(&mut self) -> Result<()> {
        for metadata in self.events.values_mut() {
            for node_index in self.config.attacker_count..self.config.node_count {
                if self.topology.roles[node_index] != NodeRole::Peer {
                    continue;
                }
                if PubsubPeerInterest::from_filters(
                    &self.nodes[node_index].filters,
                    &metadata.verified,
                ) == PubsubPeerInterest::Subscribed
                {
                    metadata.interested.insert(node_index);
                }
            }
        }
        self.report.legitimate_events = self
            .events
            .values()
            .filter(|metadata| metadata.legitimate)
            .count();
        self.report.spam_events = self
            .events
            .len()
            .saturating_sub(self.report.legitimate_events);
        self.report.expected_legitimate_deliveries = self
            .events
            .values()
            .filter(|metadata| metadata.legitimate)
            .map(|metadata| metadata.interested.len())
            .sum();
        self.report.expected_signed_spam_deliveries = self
            .events
            .values()
            .filter(|metadata| !metadata.legitimate)
            .map(|metadata| metadata.interested.len())
            .sum();
        for identity in SpamIdentity::ALL {
            let identity = identity.as_str().to_string();
            self.report
                .expected_signed_spam_deliveries_by_identity
                .insert(identity.clone(), 0);
            self.report
                .expected_machine_admitted_spam_deliveries_by_identity
                .insert(identity, 0);
        }
        for metadata in self.events.values().filter(|metadata| !metadata.legitimate) {
            let expected = self
                .report
                .expected_signed_spam_deliveries_by_class
                .entry(super::super::class_name(metadata.class).to_string())
                .or_default();
            *expected = expected.saturating_add(metadata.interested.len());
            if let Some(identity) = metadata.spam_identity {
                let expected = self
                    .report
                    .expected_signed_spam_deliveries_by_identity
                    .entry(identity.as_str().to_string())
                    .or_default();
                *expected = expected.saturating_add(metadata.interested.len());
                if machine_admitted_class(metadata.class) {
                    let expected = self
                        .report
                        .expected_machine_admitted_spam_deliveries_by_identity
                        .entry(identity.as_str().to_string())
                        .or_default();
                    *expected = expected.saturating_add(metadata.interested.len());
                }
            }
        }
        if self.report.expected_legitimate_deliveries == 0 {
            return Err(SimulationError::InvalidConfig(
                "workload has no interested honest peers".to_string(),
            ));
        }
        Ok(())
    }

    pub(in crate::simulation) fn install_subscriptions(&mut self) -> Result<()> {
        for subscriber in 0..self.config.node_count {
            let filters = self.nodes[subscriber].filters.clone();
            let provenance = if subscriber < self.config.attacker_count {
                TrafficProvenance::Adversarial
            } else {
                TrafficProvenance::Legitimate
            };
            for provider in self.topology.neighbors[subscriber].clone() {
                self.schedule_subscription_message(
                    subscriber,
                    provider,
                    &FipsPubsubWireMessage::req(
                        profile_subscription_id(subscriber),
                        filters.clone(),
                    ),
                    SubscriptionStore::Ordinary,
                    SubscriptionPurpose::Install,
                    provenance,
                )?;
            }
        }
        self.inject_subscription_floods()?;
        Ok(())
    }

    fn inject_subscription_floods(&mut self) -> Result<()> {
        if self.config.attacker_count == 0 {
            return Ok(());
        }
        for target in self.config.attacker_count..self.config.node_count {
            let Some(attacker) = self.topology.neighbors[target]
                .iter()
                .copied()
                .find(|peer| *peer < self.config.attacker_count && !is_quiet_attacker(*peer))
            else {
                continue;
            };
            for sequence in 0..12 {
                let message = FipsPubsubWireMessage::req(
                    SubscriptionId::new(format!("flood-{attacker}-{sequence}")),
                    vec![Filter::new()],
                );
                self.schedule_subscription_message(
                    attacker,
                    target,
                    &message,
                    SubscriptionStore::Ordinary,
                    SubscriptionPurpose::Flood,
                    TrafficProvenance::Adversarial,
                )?;
            }
            let oversized = FipsPubsubWireMessage::req(
                SubscriptionId::new(format!("oversized-{attacker}")),
                vec![Filter::new(); 17],
            );
            self.schedule_subscription_message(
                attacker,
                target,
                &oversized,
                SubscriptionStore::Ordinary,
                SubscriptionPurpose::Flood,
                TrafficProvenance::Adversarial,
            )?;
            self.schedule_subscription_frame(SubscriptionFrame::new(
                attacker,
                target,
                br#"["REQ","broken""#.to_vec(),
                SubscriptionStore::Ordinary,
                SubscriptionPurpose::Flood,
                TrafficProvenance::Adversarial,
            ));
        }
        self.inject_unauthorized_subscription_probes()?;
        Ok(())
    }

    fn inject_unauthorized_subscription_probes(&mut self) -> Result<()> {
        for target in self.config.attacker_count..self.config.node_count {
            let Some(attacker) = (0..self.config.attacker_count)
                .find(|attacker| !self.topology.neighbors[target].contains(attacker))
            else {
                continue;
            };
            self.schedule_subscription_message(
                attacker,
                target,
                &FipsPubsubWireMessage::req(
                    SubscriptionId::new(format!("unauthorized-{attacker}-{target}")),
                    vec![Filter::new()],
                ),
                SubscriptionStore::Ordinary,
                SubscriptionPurpose::Flood,
                TrafficProvenance::Adversarial,
            )?;
        }
        Ok(())
    }
}
