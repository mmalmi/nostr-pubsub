use nostr::{Alphabet, Filter, Kind, SingleLetterTag};
use nostr_pubsub::FipsPubsubWireMessage;
use nostr_pubsub_social_graph::DEFAULT_PEER_RATING_SCOPE;
use nostr_social_memory::RATING_KIND;

use super::{
    NodeRole, PeerSelectionMode, Result, Simulation, SubscriptionPurpose, TrafficProvenance,
    profile_subscription_id, pubsub_error,
};

impl Simulation {
    pub(super) fn initialize_rating_filters(&mut self) -> Result<()> {
        for subscriber in 0..self.config.node_count {
            self.nodes[subscriber].rating_filters = self.rating_filters_for(subscriber)?;
        }
        Ok(())
    }

    pub(super) fn mark_rating_subscription_dirty(&mut self, node: usize) {
        if self.mode == PeerSelectionMode::SharedReputation
            && node >= self.config.attacker_count
            && self.topology.roles[node] != NodeRole::Supernode
        {
            self.rating_subscription_dirty.insert(node);
        }
    }

    pub(super) fn flush_rediscovery_subscriptions(&mut self) -> Result<()> {
        let dirty = std::mem::take(&mut self.rating_subscription_dirty);
        for subscriber in &dirty {
            self.nodes[*subscriber].rating_filters = self.rating_filters_for(*subscriber)?;
            self.refresh_local_filter_resource_state(*subscriber)?;
        }
        let new_links = std::mem::take(&mut self.rediscovery_new_links);
        for (left, right) in &new_links {
            self.restore_link(*left, *right, SubscriptionPurpose::Rediscovery)?;
        }
        self.report.rediscovery_subscription_refresh_nodes = self
            .report
            .rediscovery_subscription_refresh_nodes
            .saturating_add(dirty.len());
        for subscriber in dirty {
            let filters = self.subscription_filters_for(subscriber);
            for provider in self.topology.neighbors[subscriber].clone() {
                if new_links.contains(&ordered_link(subscriber, provider)) {
                    continue;
                }
                self.schedule_subscription_message(
                    subscriber,
                    provider,
                    &FipsPubsubWireMessage::req(
                        profile_subscription_id(subscriber),
                        filters.clone(),
                    ),
                    SubscriptionPurpose::Rediscovery,
                    TrafficProvenance::Legitimate,
                )?;
                self.report.rediscovery_subscription_refresh_targets = self
                    .report
                    .rediscovery_subscription_refresh_targets
                    .saturating_add(1);
            }
        }
        for (left, right) in new_links {
            let (attacker, honest) = if self.topology.roles[left] == NodeRole::Attacker {
                (left, right)
            } else if self.topology.roles[right] == NodeRole::Attacker {
                (right, left)
            } else {
                continue;
            };
            self.schedule_attack_link_pressure(attacker, honest, self.scheduler.now_ms())?;
        }
        Ok(())
    }

    pub(in crate::simulation) fn subscription_filters_for(&self, subscriber: usize) -> Vec<Filter> {
        let mut filters = self.nodes[subscriber].filters.clone();
        filters.extend(self.nodes[subscriber].rating_filters.clone());
        filters
    }

    pub(super) fn rating_filters_for(&self, subscriber: usize) -> Result<Vec<Filter>> {
        let mut filters = Vec::new();
        if self.mode != PeerSelectionMode::SharedReputation
            || subscriber < self.config.attacker_count
        {
            return Ok(filters);
        }
        if self.topology.roles[subscriber] == NodeRole::Supernode {
            return Ok(vec![scoped_rating_filter()]);
        }
        filters.push(reputation_filter(
            std::iter::once(self.keys[subscriber].public_key()).chain(
                self.topology.neighbors[subscriber]
                    .iter()
                    .map(|peer| self.keys[*peer].public_key()),
            ),
        ));
        let trusted_raters = self.nodes[subscriber]
            .service_admitted_raters
            .iter()
            .map(|rater| nostr::PublicKey::parse(rater).map_err(pubsub_error))
            .collect::<Result<Vec<_>>>()?;
        if !trusted_raters.is_empty() {
            filters.push(trusted_rater_filter(trusted_raters));
        }
        Ok(filters)
    }
}

pub(super) fn reputation_filter(pubkeys: impl IntoIterator<Item = nostr::PublicKey>) -> Filter {
    scoped_rating_filter().pubkeys(pubkeys)
}

pub(super) fn trusted_rater_filter(authors: impl IntoIterator<Item = nostr::PublicKey>) -> Filter {
    scoped_rating_filter().authors(authors)
}

fn scoped_rating_filter() -> Filter {
    Filter::new().kind(Kind::Custom(RATING_KIND)).custom_tag(
        SingleLetterTag::lowercase(Alphabet::I),
        DEFAULT_PEER_RATING_SCOPE,
    )
}

pub(super) const fn ordered_link(left: usize, right: usize) -> (usize, usize) {
    if left < right {
        (left, right)
    } else {
        (right, left)
    }
}

#[cfg(test)]
mod tests {
    use nostr_pubsub::{PubsubPeerInterest, SourceId, VerifiedEvent};

    use super::*;
    use crate::simulation::{
        DirectedServiceLink, PeerSelectionMode, ReputationEventOrigin, SIM_UNIX_BASE,
        SimulationConfig, peer_rating_event,
    };
    use crate::topology::{NodeRole, TopologyStrategy};

    fn test_simulation() -> Simulation {
        Simulation::new(
            SimulationConfig {
                node_count: 48,
                attacker_count: 8,
                topology: TopologyStrategy::PeerMesh,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .expect("simulation")
    }

    #[test]
    fn machine_reputation_starts_with_only_each_nodes_local_root() {
        let simulation = test_simulation();
        for node in simulation.config.attacker_count..simulation.config.node_count {
            assert!(simulation.nodes[node].service_admitted_raters.is_empty());
            assert_eq!(
                simulation.nodes[node]
                    .machine_reputation
                    .as_ref()
                    .expect("honest reputation")
                    .snapshot()
                    .trusted_roots,
                1,
            );
        }
        assert_eq!(simulation.report.machine_trust_edges, 0);
    }

    #[test]
    fn verified_service_projection_refreshes_and_root_revocation_removes_author_filter() {
        let (mut simulation, receiver, rater, remote_subject) = service_rater_fixture();
        simulation.record_positive_service_admission(receiver, rater);
        assert!(
            simulation.nodes[receiver]
                .service_admitted_raters
                .is_empty()
        );
        admit_service_rater(&mut simulation, receiver, rater);
        let authored = authored_remote_rating(&simulation, rater, remote_subject);
        assert_author_interest(
            &simulation,
            receiver,
            rater,
            &authored,
            PubsubPeerInterest::Subscribed,
        );
        revoke_service_rater(&mut simulation, receiver, rater);
        assert_author_interest(
            &simulation,
            receiver,
            rater,
            &authored,
            PubsubPeerInterest::Unsubscribed,
        );
    }

    fn service_rater_fixture() -> (Simulation, usize, usize, usize) {
        let mut simulation = test_simulation();
        simulation.install_subscriptions().expect("subscriptions");
        simulation.drain_scheduler().expect("initial REQs");
        let receiver = (simulation.config.attacker_count..simulation.config.node_count)
            .find(|node| simulation.topology.roles[*node] == NodeRole::Peer)
            .expect("ordinary receiver");
        let rater = simulation.topology.neighbors[receiver]
            .iter()
            .copied()
            .find(|peer| *peer >= simulation.config.attacker_count)
            .expect("honest service peer");
        let remote = (simulation.config.attacker_count..simulation.config.node_count)
            .find(|peer| {
                *peer != receiver
                    && *peer != rater
                    && !simulation.topology.neighbors[receiver].contains(peer)
            })
            .expect("receiver non-neighbor");
        (simulation, receiver, rater, remote)
    }

    fn admit_service_rater(simulation: &mut Simulation, receiver: usize, rater: usize) {
        let link = DirectedServiceLink {
            source: rater,
            destination: receiver,
        };
        simulation.verified_delivery_credits.insert(link, 3);
        simulation.verified_delivery_bytes.insert(link, 768);
        let positive = peer_rating_event(
            &simulation.keys[receiver],
            &simulation.peer_ids[receiver],
            &simulation.peer_ids[rater],
            100,
            SIM_UNIX_BASE,
        )
        .expect("positive root rating");
        simulation
            .publish_reputation_event(
                receiver,
                rater,
                0,
                &positive,
                ReputationEventOrigin::PositiveServiceEndorsement,
            )
            .expect("publish positive");
        assert!(simulation.rating_subscription_dirty.contains(&receiver));
        simulation
            .flush_rediscovery_subscriptions()
            .expect("refresh author subscription");
        simulation.drain_scheduler().expect("deliver author REQ");
    }

    fn authored_remote_rating(
        simulation: &Simulation,
        rater: usize,
        remote_subject: usize,
    ) -> VerifiedEvent {
        VerifiedEvent::try_from(
            peer_rating_event(
                &simulation.keys[rater],
                &simulation.peer_ids[rater],
                &simulation.peer_ids[remote_subject],
                0,
                SIM_UNIX_BASE,
            )
            .expect("subject-authored rating"),
        )
        .expect("verified rating")
    }

    fn revoke_service_rater(simulation: &mut Simulation, receiver: usize, rater: usize) {
        let negative = peer_rating_event(
            &simulation.keys[receiver],
            &simulation.peer_ids[receiver],
            &simulation.peer_ids[rater],
            0,
            SIM_UNIX_BASE.saturating_add(1),
        )
        .expect("negative root rating");
        simulation
            .publish_reputation_event(
                receiver,
                rater,
                1_000,
                &negative,
                ReputationEventOrigin::HonestObservation {
                    quiet_blackhole: false,
                },
            )
            .expect("publish revocation");
        assert!(
            simulation.nodes[receiver]
                .service_admitted_raters
                .is_empty()
        );
        assert!(simulation.rating_subscription_dirty.contains(&receiver));
        simulation
            .flush_rediscovery_subscriptions()
            .expect("remove author subscription");
        simulation.drain_scheduler().expect("deliver removal REQ");
    }

    fn assert_author_interest(
        simulation: &Simulation,
        receiver: usize,
        provider: usize,
        authored: &VerifiedEvent,
        expected: PubsubPeerInterest,
    ) {
        assert_eq!(
            PubsubPeerInterest::from_filters(&simulation.nodes[receiver].rating_filters, authored,),
            expected,
        );
        assert_eq!(
            simulation.nodes[provider]
                .wire
                .subscriptions()
                .peer_interest(&SourceId::new(&simulation.peer_ids[receiver]), authored),
            expected,
        );
    }
}
