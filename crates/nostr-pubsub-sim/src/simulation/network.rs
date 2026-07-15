use super::{InvWantWireMessage, ScheduledAction, Simulation, link_key};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum OutageCause {
    Stochastic,
    ForcedSupernode,
}

/// One independently scheduled outage on a canonical undirected link.
///
/// Keeping the cause in the identity makes the outage set a reference-counted
/// link state: a forced outage ending cannot accidentally clear an overlapping
/// stochastic outage on the same link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct LinkOutage {
    link: (usize, usize),
    cause: OutageCause,
}

impl LinkOutage {
    pub(super) fn new(left: usize, right: usize, cause: OutageCause) -> Self {
        Self {
            link: link_key(left, right),
            cause,
        }
    }

    pub(super) const fn endpoints(self) -> (usize, usize) {
        self.link
    }
}

impl Simulation {
    pub(super) fn begin_link_outage(&mut self, outage: LinkOutage) {
        self.down_links.insert(outage);
    }

    /// Ends one outage and reports whether the link became usable.
    pub(super) fn end_link_outage(&mut self, outage: LinkOutage) -> bool {
        self.down_links.remove(&outage)
            && !self
                .down_links
                .iter()
                .any(|active| active.link == outage.link)
    }

    pub(super) fn link_is_active(&self, left: usize, right: usize) -> bool {
        let link = link_key(left, right);
        !self.down_links.iter().any(|outage| outage.link == link)
    }

    pub(super) fn schedule_retry_if_needed(
        &mut self,
        source: usize,
        destination: usize,
        event_id: &str,
    ) {
        if self.config.max_retries == 0 {
            return;
        }
        let key = (source, destination, event_id.to_string());
        let state = self.retry_counts.entry(key.clone()).or_default();
        if state.scheduled {
            return;
        }
        if state.attempts >= self.config.max_retries {
            self.retry_counts.remove(&key);
            return;
        }
        state.scheduled = true;
        self.scheduler.schedule_after(
            self.config.retry_delay_ms,
            ScheduledAction::RetryInventory {
                source,
                destination,
                event_id: event_id.to_string(),
            },
        );
    }

    pub(super) fn note_disrupted_message(
        &mut self,
        source: usize,
        destination: usize,
        message: &InvWantWireMessage,
    ) {
        let disrupted_route = match message {
            InvWantWireMessage::Want { event_id } => Some((source, destination, event_id)),
            InvWantWireMessage::Frame { event_id, .. } => Some((destination, source, event_id)),
            InvWantWireMessage::Inventory { .. } => None,
        };
        if let Some((observer, provider, event_id)) = disrupted_route
            && self.nodes[observer]
                .mesh
                .record_transport_disruption(&self.peer_ids[provider], event_id)
        {
            self.record_cpu_work(observer, |work| {
                work.transport_disruption_updates =
                    work.transport_disruption_updates.saturating_add(1);
            });
            self.observe_core_resource_state(observer);
        }
        let (retry_source, retry_destination, event_id) =
            retry_inventory_route(source, destination, message);
        if self.nodes[retry_source].local_events.contains_key(event_id) {
            self.schedule_retry_if_needed(retry_source, retry_destination, event_id);
        }
        let (target, event_id) = disrupted_delivery_target(source, destination, message);
        let key = (target, event_id.to_string());
        if self
            .events
            .get(event_id)
            .is_some_and(|metadata| metadata.legitimate)
            && !self.nodes[target].local_events.contains_key(event_id)
            && !self.delivery_times.contains_key(&key)
        {
            self.retry_needed.insert(key.clone());
            self.disrupted_transfers.insert(key);
        }
    }

    pub(super) fn note_disrupted_payload(
        &mut self,
        source: usize,
        destination: usize,
        payload: &[u8],
    ) {
        if let Ok(message) = self.codec.decode(payload) {
            self.note_disrupted_message(source, destination, &message);
        }
    }

    pub(super) fn finish_delivery_retries(&mut self, node: usize, event_id: &str) {
        self.retry_counts
            .retain(|(_, destination, candidate), _| *destination != node || candidate != event_id);
    }

    pub(super) fn cancel_delivery_retries(&mut self, node: usize, event_id: &str) {
        self.finish_delivery_retries(node, event_id);
        self.retry_needed.remove(&(node, event_id.to_string()));
    }

    pub(super) fn finish_inventory_retry(
        &mut self,
        source: usize,
        destination: usize,
        event_id: &str,
    ) {
        self.retry_counts
            .remove(&(source, destination, event_id.to_string()));
    }
}

fn retry_inventory_route(
    source: usize,
    destination: usize,
    message: &InvWantWireMessage,
) -> (usize, usize, &str) {
    match message {
        InvWantWireMessage::Inventory { event_id, .. }
        | InvWantWireMessage::Frame { event_id, .. } => (source, destination, event_id),
        InvWantWireMessage::Want { event_id } => (destination, source, event_id),
    }
}

fn disrupted_delivery_target(
    source: usize,
    destination: usize,
    message: &InvWantWireMessage,
) -> (usize, &str) {
    match message {
        InvWantWireMessage::Inventory { event_id, .. }
        | InvWantWireMessage::Frame { event_id, .. } => (destination, event_id),
        InvWantWireMessage::Want { event_id } => (source, event_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{TrafficDirection, TrafficProvenance};
    use crate::simulation::hash_bytes;
    use crate::simulation::{Packet, RetryState};
    use crate::{PeerSelectionMode, SimulationConfig, TopologyStrategy};

    fn config() -> SimulationConfig {
        SimulationConfig {
            node_count: 48,
            attacker_count: 8,
            loss_basis_points: 0,
            churn_basis_points: 0,
            supernode_count: 4,
            adversarial_discovery_candidate_count: 2,
            ..SimulationConfig::default()
        }
    }

    #[test]
    fn overlapping_outages_require_every_cause_to_end() {
        let mut simulation = Simulation::new(config(), PeerSelectionMode::Neutral).unwrap();
        let stochastic = LinkOutage::new(12, 9, OutageCause::Stochastic);
        let forced = LinkOutage::new(9, 12, OutageCause::ForcedSupernode);

        simulation.begin_link_outage(stochastic);
        simulation.begin_link_outage(forced);
        assert!(!simulation.link_is_active(9, 12));
        assert!(!simulation.end_link_outage(forced));
        assert!(!simulation.link_is_active(9, 12));
        assert!(simulation.end_link_outage(stochastic));
        assert!(simulation.link_is_active(9, 12));
    }

    #[test]
    fn forced_supernode_outages_are_included_in_churn_kpi() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                topology: TopologyStrategy::HybridSupernodes,
                churn_basis_points: 1,
                ..config()
            },
            PeerSelectionMode::Neutral,
        )
        .unwrap();
        let supernode = simulation.topology.honest_supernodes[0];
        let forced_outages = simulation.topology.neighbors[supernode].len();

        simulation.schedule_churn();

        let mut forced_down = 0;
        let mut forced_up = 0;
        while let Some(action) = simulation.scheduler.pop_next() {
            match action {
                ScheduledAction::LinkDown(LinkOutage {
                    cause: OutageCause::ForcedSupernode,
                    ..
                }) => forced_down += 1,
                ScheduledAction::LinkUp(LinkOutage {
                    cause: OutageCause::ForcedSupernode,
                    ..
                }) => forced_up += 1,
                _ => {}
            }
        }
        assert_eq!(forced_down, forced_outages);
        assert_eq!(forced_up, forced_outages);
        assert!(simulation.report.churned_links >= forced_outages);
    }

    #[test]
    fn loss_targets_inventory_receiver_want_sender_and_frame_receiver() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                loss_basis_points: 10_000,
                ..config()
            },
            PeerSelectionMode::Neutral,
        )
        .unwrap();
        let metadata = simulation
            .events
            .values()
            .find(|metadata| metadata.legitimate)
            .unwrap()
            .clone();
        let event = metadata.verified.into_event();
        let event_id = event.id.to_hex();
        let inventory = InvWantWireMessage::Inventory {
            event_id: event_id.clone(),
            event_kind: u16::from(event.kind),
            payload_bytes: 512,
            hop_limit: 2,
        };
        let want = InvWantWireMessage::Want {
            event_id: event_id.clone(),
        };
        let frame = InvWantWireMessage::Frame {
            event_id: event_id.clone(),
            event: Box::new(event),
        };

        simulation.enqueue_message_at(9, 10, &inventory, 1).unwrap();
        simulation.enqueue_message_at(11, 9, &want, 1).unwrap();
        simulation.enqueue_message_at(9, 12, &frame, 1).unwrap();

        assert!(simulation.retry_needed.contains(&(10, event_id.clone())));
        assert!(simulation.retry_needed.contains(&(11, event_id.clone())));
        assert!(simulation.retry_needed.contains(&(12, event_id.clone())));
        for node in [10, 11, 12] {
            simulation.record_delivery(node, &event_id, 10);
        }
        assert_eq!(simulation.report.eventual_disrupted_transfer_recoveries, 3);
    }

    #[test]
    fn common_packets_receive_common_seeded_loss_across_policy_modes() {
        let config = SimulationConfig {
            loss_basis_points: 5_000,
            ..config()
        };
        let mut neutral = Simulation::new(config.clone(), PeerSelectionMode::Neutral).unwrap();
        let mut shared = Simulation::new(config, PeerSelectionMode::SharedReputation).unwrap();
        let fault_key = hash_bytes(b"same production packet");
        for _ in 0..64 {
            assert_eq!(
                neutral.packet_is_lost(9, 10, fault_key),
                shared.packet_is_lost(9, 10, fault_key),
            );
        }
    }

    #[test]
    fn policy_rejection_cancels_only_bounded_retry_state() {
        let mut simulation = Simulation::new(config(), PeerSelectionMode::Neutral).unwrap();
        let event_id = "ad".repeat(32);
        let destination = 10;
        simulation
            .retry_counts
            .insert((9, destination, event_id.clone()), RetryState::default());
        simulation
            .retry_needed
            .insert((destination, event_id.clone()));

        simulation.cancel_delivery_retries(destination, &event_id);

        assert!(simulation.retry_counts.is_empty());
        assert!(!simulation.retry_needed.contains(&(destination, event_id)));
    }

    #[test]
    fn unauthorized_data_is_charged_as_received_but_not_decoded() {
        let mut simulation = Simulation::new(config(), PeerSelectionMode::Neutral).unwrap();
        let source = 0;
        let destination = (simulation.config.attacker_count..simulation.config.node_count)
            .find(|candidate| !simulation.topology.neighbors[*candidate].contains(&source))
            .unwrap();
        let payload = simulation
            .codec
            .encode(&InvWantWireMessage::Inventory {
                event_id: "ac".repeat(32),
                event_kind: 1,
                payload_bytes: 1,
                hop_limit: 1,
            })
            .unwrap();

        simulation
            .process_packet(Packet {
                source,
                destination,
                payload,
            })
            .unwrap();

        assert_eq!(simulation.report.unauthorized_source_drops, 1);
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Adversarial)
                .messages,
            1
        );
        assert_eq!(
            simulation.node_resources[destination]
                .work
                .invwant_decode_bytes,
            0
        );
    }

    #[test]
    fn transport_disruption_attribution_is_directional_and_event_deduped() {
        let mut simulation = Simulation::new(config(), PeerSelectionMode::Neutral).unwrap();
        let event_id = "ab".repeat(32);
        simulation.nodes[10]
            .mesh
            .receive(
                &simulation.peer_ids[9],
                InvWantWireMessage::Inventory {
                    event_id: event_id.clone(),
                    event_kind: 1,
                    payload_bytes: 1,
                    hop_limit: 1,
                },
                &[],
                0,
            )
            .unwrap();
        let want = InvWantWireMessage::Want {
            event_id: event_id.clone(),
        };
        simulation.note_disrupted_message(10, 9, &want);
        simulation.note_disrupted_message(10, 9, &want);
        simulation.note_disrupted_message(
            9,
            10,
            &InvWantWireMessage::Inventory {
                event_id,
                event_kind: 1,
                payload_bytes: 1,
                hop_limit: 1,
            },
        );

        assert_eq!(
            simulation.nodes[10]
                .mesh
                .retained_state()
                .transport_disrupted_route_peers,
            1
        );
        assert_eq!(
            simulation.node_resources[10]
                .work
                .transport_disruption_updates,
            1
        );
    }

    #[test]
    fn link_down_drop_marks_the_delivery_for_recovery() {
        let mut simulation = Simulation::new(config(), PeerSelectionMode::Neutral).unwrap();
        let metadata = simulation
            .events
            .values()
            .find(|metadata| metadata.legitimate)
            .unwrap()
            .clone();
        let source = metadata.publisher;
        let destination = simulation.topology.neighbors[source][0];
        let event = metadata.verified.into_event();
        let event_id = event.id.to_hex();
        let message = InvWantWireMessage::Frame {
            event_id: event_id.clone(),
            event: Box::new(event),
        };
        let payload = simulation.codec.encode(&message).unwrap();
        simulation.begin_link_outage(LinkOutage::new(
            source,
            destination,
            OutageCause::Stochastic,
        ));

        simulation
            .process_packet(Packet {
                source,
                destination,
                payload,
            })
            .unwrap();

        assert!(simulation.retry_needed.contains(&(destination, event_id)));
        assert_eq!(simulation.report.dropped_packets, 1);
    }

    #[test]
    fn retries_are_bounded_while_the_link_stays_down() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                max_retries: 3,
                retry_delay_ms: 5,
                ..config()
            },
            PeerSelectionMode::Neutral,
        )
        .unwrap();
        let metadata = simulation
            .events
            .values()
            .find(|metadata| metadata.legitimate)
            .unwrap()
            .clone();
        let source = metadata.publisher;
        let destination = simulation.topology.neighbors[source][0];
        let event_id = metadata.verified.as_event().id.to_hex();
        simulation.nodes[source]
            .local_events
            .insert(event_id.clone(), metadata.verified);
        simulation.begin_link_outage(LinkOutage::new(
            source,
            destination,
            OutageCause::Stochastic,
        ));
        simulation.schedule_retry_if_needed(source, destination, &event_id);

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.retry_inventories, 3);
        assert_eq!(simulation.report.inventory_messages, 3);
        assert_eq!(simulation.report.dropped_packets, 3);
        assert!(simulation.retry_counts.is_empty());
        assert!(simulation.scheduler.is_empty());
    }

    #[test]
    fn final_outage_release_replays_cached_event_through_production_mesh() {
        let mut simulation = Simulation::new(config(), PeerSelectionMode::Neutral).unwrap();
        let mut candidates = simulation
            .events
            .iter()
            .filter(|(_, metadata)| metadata.legitimate)
            .flat_map(|(event_id, metadata)| {
                simulation.topology.neighbors[metadata.publisher]
                    .iter()
                    .copied()
                    .filter(|neighbor| metadata.interested.contains(neighbor))
                    .map(|destination| {
                        (
                            event_id.clone(),
                            metadata.verified.clone(),
                            metadata.publisher,
                            destination,
                        )
                    })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.0.cmp(&right.0).then(left.3.cmp(&right.3)));
        let (event_id, event, source, destination) = candidates
            .into_iter()
            .next()
            .expect("workload must have a directly interested neighbor");
        simulation.nodes[source]
            .local_events
            .insert(event_id.clone(), event);
        let stochastic = LinkOutage::new(source, destination, OutageCause::Stochastic);
        let forced = LinkOutage::new(source, destination, OutageCause::ForcedSupernode);
        simulation
            .scheduler
            .schedule_at(1, ScheduledAction::LinkDown(stochastic));
        simulation
            .scheduler
            .schedule_at(1, ScheduledAction::LinkDown(forced));
        simulation
            .scheduler
            .schedule_at(5, ScheduledAction::LinkUp(forced));
        simulation
            .scheduler
            .schedule_at(10, ScheduledAction::LinkUp(stochastic));
        let work_before = simulation.node_resources[source].work;

        simulation.drain_scheduler().unwrap();
        let work_after = simulation.node_resources[source].work;

        assert!(
            simulation
                .delivery_times
                .contains_key(&(destination, event_id))
        );
        assert!(simulation.link_is_active(source, destination));
        assert_eq!(work_after.signature_checks, work_before.signature_checks);
        assert!(work_after.avoided_signature_checks > work_before.avoided_signature_checks);
    }
}
