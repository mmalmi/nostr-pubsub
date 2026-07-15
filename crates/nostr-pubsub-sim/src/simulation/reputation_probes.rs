use nostr_pubsub::SourceId;

use super::reputation_flow::{PeerProjection, peer_projection, virtual_unix_secs};
use super::{ReputationEventOrigin, Result, Simulation, is_quiet_attacker, peer_rating_event};

impl Simulation {
    pub(super) fn publish_forged_probe(&mut self) -> Result<()> {
        if self.forged_rating_published || self.config.attacker_count == 0 {
            return Ok(());
        }
        let Some(publisher) = (0..self.config.attacker_count).find(|node| {
            is_quiet_attacker(*node)
                && self.topology.neighbors[*node]
                    .iter()
                    .any(|peer| *peer >= self.config.attacker_count)
        }) else {
            return Ok(());
        };
        let Some(rater) = self.topology.neighbors[publisher]
            .iter()
            .copied()
            .find(|peer| *peer >= self.config.attacker_count)
        else {
            return Ok(());
        };
        let subject = (self.config.attacker_count..self.config.node_count)
            .find(|peer| *peer != rater)
            .unwrap_or(rater);
        let event = peer_rating_event(
            &self.keys[publisher],
            &self.peer_ids[rater],
            &self.peer_ids[subject],
            100,
            virtual_unix_secs(self.scheduler.now_ms()),
        )?;
        self.forged_rating_published = true;
        self.report.forged_machine_ratings_published = self
            .report
            .forged_machine_ratings_published
            .saturating_add(1);
        self.publish_reputation_event(
            publisher,
            subject,
            self.scheduler.now_ms(),
            &event,
            ReputationEventOrigin::ForgedProbe,
        )
    }

    pub(super) fn publish_poisoned_probe(&mut self) -> Result<()> {
        if self.poisoned_rating_published {
            return Ok(());
        }
        let Some((publisher, _receiver, subject)) = self.poisoned_probe_plan() else {
            return Ok(());
        };
        let event = peer_rating_event(
            &self.keys[publisher],
            &self.peer_ids[publisher],
            &self.peer_ids[subject],
            0,
            virtual_unix_secs(self.scheduler.now_ms()).saturating_add(1),
        )?;
        self.poisoned_rating_published = true;
        self.report.poisoned_machine_ratings_published = self
            .report
            .poisoned_machine_ratings_published
            .saturating_add(1);
        self.publish_reputation_event(
            publisher,
            subject,
            self.scheduler.now_ms(),
            &event,
            ReputationEventOrigin::PoisonedProbe,
        )
    }

    pub(super) fn poisoned_probe_plan(&self) -> Option<(usize, usize, usize)> {
        let mut best = None;
        for publisher in self.config.attacker_count..self.config.node_count {
            let trusting = self.active_rating_receivers(publisher, true);
            let untrusted = self.active_rating_receivers(publisher, false);
            if trusting.is_empty() || untrusted.is_empty() {
                continue;
            }
            for receiver in trusting.iter().copied() {
                let Some(subject) = self.poisoned_subject(
                    publisher,
                    receiver,
                    trusting.as_slice(),
                    untrusted.as_slice(),
                ) else {
                    continue;
                };
                let interested = trusting.len().saturating_add(
                    untrusted
                        .iter()
                        .filter(|peer| self.topology.neighbors[**peer].contains(&subject))
                        .count(),
                );
                if best
                    .as_ref()
                    .is_none_or(|(score, _, _, _)| interested > *score)
                {
                    best = Some((interested, publisher, receiver, subject));
                }
            }
        }
        best.map(|(_, publisher, receiver, subject)| (publisher, receiver, subject))
    }

    fn active_rating_receivers(&self, publisher: usize, trusted: bool) -> Vec<usize> {
        self.topology.neighbors[publisher]
            .iter()
            .copied()
            .filter(|peer| *peer >= self.config.attacker_count)
            .filter(|peer| self.link_is_active(publisher, *peer))
            .filter(|peer| {
                self.nodes[publisher]
                    .machine_policies
                    .as_ref()
                    .is_none_or(|policies| {
                        policies
                            .select_mesh_peer(&self.peer_ids[*peer])
                            .is_ok_and(|selected| selected.is_some())
                    })
            })
            .filter(|peer| {
                self.nodes[*peer]
                    .service_admitted_raters
                    .contains(&self.peer_ids[publisher])
                    == trusted
            })
            .filter(|peer| {
                !trusted
                    || self.nodes[*peer]
                        .machine_policies
                        .as_ref()
                        .is_some_and(|policies| {
                            peer_projection(policies, &self.peer_ids[publisher])
                                .is_ok_and(|projection| projection == PeerProjection::Positive)
                        })
            })
            .filter(|peer| {
                self.nodes[publisher]
                    .wire
                    .subscriptions()
                    .peer_subscription_count(&SourceId::new(&self.peer_ids[*peer]))
                    > 0
            })
            .collect()
    }

    fn poisoned_subject(
        &self,
        publisher: usize,
        receiver: usize,
        trusting: &[usize],
        untrusted: &[usize],
    ) -> Option<usize> {
        (self.config.attacker_count..self.config.node_count)
            .filter(|subject| {
                *subject != publisher
                    && !trusting.contains(subject)
                    && !self.topology.neighbors[receiver].contains(subject)
                    && !self
                        .admitted_rater_poison_targets
                        .iter()
                        .any(|(_, reserved)| reserved == subject)
                    && self
                        .admitted_rater_post_revocation_target
                        .is_none_or(|(_, reserved)| reserved != *subject)
                    && self.nodes[receiver]
                        .machine_policies
                        .as_ref()
                        .is_some_and(|policies| {
                            peer_projection(policies, &self.peer_ids[*subject])
                                .is_ok_and(|projection| projection == PeerProjection::Unknown)
                        })
                    && untrusted
                        .iter()
                        .any(|peer| self.topology.neighbors[*peer].contains(subject))
            })
            .min_by_key(|subject| {
                (
                    trusting
                        .iter()
                        .filter(|peer| self.topology.neighbors[**peer].contains(subject))
                        .count(),
                    *subject,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use nostr_pubsub::SourceId;

    use super::*;
    use crate::simulation::{
        DirectedServiceLink, PeerSelectionMode, ReputationEventOrigin, SimulationConfig,
    };
    use crate::topology::{NodeRole, TopologyStrategy};

    #[test]
    fn removed_but_still_subscribed_receiver_is_not_an_active_poison_target() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                node_count: 64,
                attacker_count: 8,
                topology: TopologyStrategy::HybridSupernodes,
                supernode_count: 4,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        let (publisher, receiver) = (simulation.config.attacker_count
            ..simulation.config.node_count)
            .find_map(|publisher| {
                simulation.topology.neighbors[publisher]
                    .iter()
                    .copied()
                    .find(|receiver| {
                        *receiver >= simulation.config.attacker_count
                            && simulation.topology.roles[*receiver] == NodeRole::Peer
                    })
                    .map(|receiver| (publisher, receiver))
            })
            .expect("connected honest publisher and ordinary receiver");
        admit_service_rater(&mut simulation, publisher, receiver);
        assert!(
            simulation
                .active_rating_receivers(publisher, true)
                .contains(&receiver)
        );

        let removal = peer_rating_event(
            &simulation.keys[publisher],
            &simulation.peer_ids[publisher],
            &simulation.peer_ids[receiver],
            0,
            virtual_unix_secs(simulation.scheduler.now_ms()).saturating_add(1),
        )
        .unwrap();
        simulation
            .publish_reputation_event(
                publisher,
                receiver,
                simulation.scheduler.now_ms(),
                &removal,
                ReputationEventOrigin::HonestObservation {
                    quiet_blackhole: false,
                },
            )
            .unwrap();

        assert!(
            simulation.nodes[publisher]
                .machine_policies
                .as_ref()
                .unwrap()
                .select_mesh_peer(&simulation.peer_ids[receiver])
                .unwrap()
                .is_none()
        );
        assert!(
            simulation.nodes[publisher]
                .wire
                .subscriptions()
                .peer_subscription_count(&SourceId::new(&simulation.peer_ids[receiver]))
                > 0,
            "the regression requires stale remote subscription state"
        );
        assert!(
            !simulation
                .active_rating_receivers(publisher, true)
                .contains(&receiver),
            "production candidate policy must override stale subscription state"
        );
    }

    #[test]
    fn generic_poison_target_is_disjoint_from_reserved_admitted_rater_targets() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                node_count: 120,
                attacker_count: 24,
                topology: TopologyStrategy::HybridSupernodes,
                supernode_count: 8,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        let (publisher, receiver) = poison_fixture_pair(&simulation);
        admit_service_rater(&mut simulation, publisher, receiver);

        let first = simulation
            .poisoned_probe_plan()
            .expect("service-admitted poison plan");
        assert_eq!((first.0, first.1), (publisher, receiver));
        simulation
            .admitted_rater_poison_targets
            .insert((receiver, first.2));
        let second = simulation
            .poisoned_probe_plan()
            .expect("a distinct generic poison target");
        assert_ne!(second.2, first.2);

        simulation.admitted_rater_post_revocation_target = Some((receiver, second.2));
        let third = simulation
            .poisoned_probe_plan()
            .expect("a target distinct from both admitted-rater controls");
        assert!(![first.2, second.2].contains(&third.2));
    }

    fn poison_fixture_pair(simulation: &Simulation) -> (usize, usize) {
        for publisher in simulation.config.attacker_count..simulation.config.node_count {
            let honest_neighbors = simulation.topology.neighbors[publisher]
                .iter()
                .copied()
                .filter(|peer| *peer >= simulation.config.attacker_count)
                .collect::<Vec<_>>();
            for receiver in honest_neighbors
                .iter()
                .copied()
                .filter(|peer| simulation.topology.roles[*peer] == NodeRole::Peer)
            {
                let untrusted = honest_neighbors
                    .iter()
                    .copied()
                    .filter(|peer| *peer != receiver)
                    .collect::<Vec<_>>();
                let has_remote_subject = (simulation.config.attacker_count
                    ..simulation.config.node_count)
                    .any(|subject| {
                        subject != publisher
                            && !simulation.topology.neighbors[receiver].contains(&subject)
                            && untrusted
                                .iter()
                                .any(|peer| simulation.topology.neighbors[*peer].contains(&subject))
                    });
                if has_remote_subject {
                    return (publisher, receiver);
                }
            }
        }
        panic!("connected admitted-rater poison fixture");
    }

    fn admit_service_rater(simulation: &mut Simulation, publisher: usize, receiver: usize) {
        let link = DirectedServiceLink {
            source: publisher,
            destination: receiver,
        };
        simulation.verified_delivery_credits.insert(link, 3);
        simulation.verified_delivery_bytes.insert(link, 768);
        let admission = peer_rating_event(
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
                &admission,
                ReputationEventOrigin::PositiveServiceEndorsement,
            )
            .unwrap();
        simulation.flush_rediscovery_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
    }
}
