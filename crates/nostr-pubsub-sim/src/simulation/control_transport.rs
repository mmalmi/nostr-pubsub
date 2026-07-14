use super::{
    FipsPubsubWireMessage, Result, ScheduledAction, Simulation, SubscriptionFrame,
    SubscriptionPurpose, SubscriptionStore, TrafficDirection, TrafficProvenance, hash_bytes,
    pubsub_error,
};
use nostr_pubsub::SourceId;

impl Simulation {
    pub(in crate::simulation) fn schedule_subscription_message(
        &mut self,
        source: usize,
        destination: usize,
        message: &FipsPubsubWireMessage,
        store: SubscriptionStore,
        purpose: SubscriptionPurpose,
        traffic_provenance: TrafficProvenance,
    ) -> Result<()> {
        let payload = self
            .fips_codec
            .encode_frame(message)
            .map_err(pubsub_error)?;
        self.schedule_subscription_frame(SubscriptionFrame::new(
            source,
            destination,
            payload,
            store,
            purpose,
            traffic_provenance,
        ));
        Ok(())
    }

    pub(in crate::simulation) fn schedule_subscription_frame(&mut self, frame: SubscriptionFrame) {
        self.scheduler
            .schedule_after(0, ScheduledAction::SendSubscription(frame));
    }

    pub(super) fn send_subscription_frame(&mut self, frame: SubscriptionFrame) {
        let bytes = u64::try_from(frame.payload.len()).unwrap_or(u64::MAX);
        self.report.subscription_messages = self.report.subscription_messages.saturating_add(1);
        self.report.control_plane_wire_bytes =
            self.report.control_plane_wire_bytes.saturating_add(bytes);
        self.traffic[frame.source].record_message(
            TrafficDirection::Sent,
            frame.traffic_provenance,
            bytes,
        );
        self.record_link_traffic(
            frame.source,
            frame.destination,
            TrafficDirection::Sent,
            frame.traffic_provenance,
            bytes,
        );

        if self.packet_is_lost(
            frame.source,
            frame.destination,
            subscription_fault_key(&frame),
        ) {
            self.report.dropped_packets = self.report.dropped_packets.saturating_add(1);
            self.schedule_subscription_retry(frame);
            return;
        }
        let latency = self.link_latency_ms(frame.source, frame.destination);
        self.scheduler
            .schedule_after(latency, ScheduledAction::SubscriptionArrived(frame));
    }

    pub(super) fn process_subscription_frame(&mut self, frame: SubscriptionFrame) -> Result<()> {
        if !self.topology.neighbors[frame.destination].contains(&frame.source) {
            self.report.unauthorized_source_drops =
                self.report.unauthorized_source_drops.saturating_add(1);
            return Ok(());
        }
        if !self.link_is_active(frame.source, frame.destination) {
            self.report.dropped_packets = self.report.dropped_packets.saturating_add(1);
            self.schedule_subscription_retry(frame);
            return Ok(());
        }

        let bytes = u64::try_from(frame.payload.len()).unwrap_or(u64::MAX);
        self.traffic[frame.destination].record_message(
            TrafficDirection::Received,
            frame.traffic_provenance,
            bytes,
        );
        self.record_link_traffic(
            frame.source,
            frame.destination,
            TrafficDirection::Received,
            frame.traffic_provenance,
            bytes,
        );
        self.decode_subscription_frame(&frame)
    }

    fn decode_subscription_frame(&mut self, frame: &SubscriptionFrame) -> Result<()> {
        let peer_id = SourceId::new(&self.peer_ids[frame.source]);
        let before = self.subscription_count(frame.destination, frame.store, &peer_id);
        let limit = self.subscription_limit(frame.destination, frame.store);
        let decoded = match frame.store {
            SubscriptionStore::Ordinary => self.nodes[frame.destination]
                .wire
                .decode_inbound(peer_id.clone(), &frame.payload),
            SubscriptionStore::Rating => self.nodes[frame.destination]
                .rating_wire
                .decode_inbound(peer_id.clone(), &frame.payload),
        };
        let inbound = match decoded {
            Ok(inbound) => inbound,
            Err(error) => {
                self.report.subscription_rejections =
                    self.report.subscription_rejections.saturating_add(1);
                return if frame.is_reliable() {
                    Err(pubsub_error(error))
                } else {
                    Ok(())
                };
            }
        };
        let after = self.subscription_count(frame.destination, frame.store, &peer_id);
        let retry_completed = frame.attempt > 0
            && frame.is_reliable()
            && subscription_action_completed(frame.purpose, &inbound.message, before, after);
        if frame.traffic_provenance == TrafficProvenance::Adversarial
            && matches!(inbound.message, FipsPubsubWireMessage::Req { .. })
            && before == limit
            && after == limit
        {
            self.report.subscription_evictions =
                self.report.subscription_evictions.saturating_add(1);
        }
        self.finish_subscription_action(frame, before, after)?;
        if retry_completed {
            self.report.subscription_retry_recoveries =
                self.report.subscription_retry_recoveries.saturating_add(1);
        }
        Ok(())
    }

    fn finish_subscription_action(
        &mut self,
        frame: &SubscriptionFrame,
        before: usize,
        after: usize,
    ) -> Result<()> {
        match frame.purpose {
            SubscriptionPurpose::LifecycleClose => self.schedule_lifecycle_reopen(
                frame.destination,
                frame.source,
                before > 0 && after == 0,
            ),
            SubscriptionPurpose::LifecycleReopen { observed_close } => {
                if observed_close && after > 0 {
                    self.report.subscription_close_reopen_successes = self
                        .report
                        .subscription_close_reopen_successes
                        .saturating_add(1);
                }
                Ok(())
            }
            SubscriptionPurpose::Reconnect => {
                self.replay_link_direction(frame.destination, frame.source, frame.store)
            }
            SubscriptionPurpose::Install | SubscriptionPurpose::Flood => Ok(()),
        }
    }

    fn schedule_subscription_retry(&mut self, mut frame: SubscriptionFrame) {
        if !frame.is_reliable() || frame.attempt >= self.config.max_retries {
            return;
        }
        frame.attempt = frame.attempt.saturating_add(1);
        self.report.subscription_retries = self.report.subscription_retries.saturating_add(1);
        self.scheduler.schedule_after(
            self.config
                .retry_delay_ms
                .saturating_mul(u64::from(frame.attempt)),
            ScheduledAction::SendSubscription(frame),
        );
    }

    fn subscription_count(
        &self,
        provider: usize,
        store: SubscriptionStore,
        peer_id: &SourceId,
    ) -> usize {
        match store {
            SubscriptionStore::Ordinary => self.nodes[provider]
                .wire
                .subscriptions()
                .peer_subscription_count(peer_id),
            SubscriptionStore::Rating => self.nodes[provider]
                .rating_wire
                .subscriptions()
                .peer_subscription_count(peer_id),
        }
    }

    fn subscription_limit(&self, provider: usize, store: SubscriptionStore) -> usize {
        match store {
            SubscriptionStore::Ordinary => {
                self.nodes[provider]
                    .wire
                    .subscriptions()
                    .limits()
                    .max_subscriptions_per_peer
            }
            SubscriptionStore::Rating => {
                self.nodes[provider]
                    .rating_wire
                    .subscriptions()
                    .limits()
                    .max_subscriptions_per_peer
            }
        }
    }
}

fn subscription_action_completed(
    purpose: SubscriptionPurpose,
    message: &FipsPubsubWireMessage,
    before: usize,
    after: usize,
) -> bool {
    match (purpose, message) {
        (
            SubscriptionPurpose::Install
            | SubscriptionPurpose::LifecycleReopen { .. }
            | SubscriptionPurpose::Reconnect,
            FipsPubsubWireMessage::Req { .. },
        ) => after > 0,
        (SubscriptionPurpose::LifecycleClose, FipsPubsubWireMessage::Close { .. }) => {
            before > after
        }
        _ => false,
    }
}

fn subscription_fault_key(frame: &SubscriptionFrame) -> u64 {
    let store = match frame.store {
        SubscriptionStore::Ordinary => 0x4f52_4449_4e41_5259,
        SubscriptionStore::Rating => 0x5241_5449_4e47_0000,
    };
    let purpose = match frame.purpose {
        SubscriptionPurpose::Install => 1,
        SubscriptionPurpose::LifecycleClose => 2,
        SubscriptionPurpose::LifecycleReopen { .. } => 3,
        SubscriptionPurpose::Reconnect => 4,
        SubscriptionPurpose::Flood => 5,
    };
    hash_bytes(&frame.payload) ^ store ^ purpose
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::TrafficCounter;
    use crate::simulation::{
        DirectedServiceLink, FipsPubsubWireMessage, LinkOutage, OutageCause, PeerSelectionMode,
        SubscriptionId,
    };
    use crate::{SimulationConfig, TopologyStrategy};
    use nostr::Filter;

    fn simulation(loss_basis_points: u32, max_retries: u8) -> Simulation {
        Simulation::new(
            SimulationConfig {
                node_count: 32,
                attacker_count: 0,
                topology: TopologyStrategy::PeerMesh,
                false_supernode_count: 0,
                loss_basis_points,
                churn_basis_points: 0,
                max_retries,
                retry_delay_ms: 5,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::Neutral,
        )
        .unwrap()
    }

    fn request(simulation: &Simulation, source: usize, destination: usize) -> SubscriptionFrame {
        let payload = simulation
            .fips_codec
            .encode_frame(&FipsPubsubWireMessage::req(
                SubscriptionId::new(format!("control-test-{source}")),
                vec![Filter::new()],
            ))
            .unwrap();
        SubscriptionFrame::new(
            source,
            destination,
            payload,
            SubscriptionStore::Ordinary,
            SubscriptionPurpose::Install,
            TrafficProvenance::Legitimate,
        )
    }

    #[test]
    fn scheduled_request_updates_both_sides_of_service_ledgers() {
        let mut simulation = simulation(0, 2);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        let frame = request(&simulation, source, destination);
        let bytes = u64::try_from(frame.payload.len()).unwrap();
        simulation.schedule_subscription_frame(frame);

        assert_eq!(simulation.report.subscription_messages, 0);
        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_messages, 1);
        assert_eq!(simulation.report.control_plane_wire_bytes, bytes);
        assert_eq!(simulation.report.subscription_retries, 0);
        assert_eq!(simulation.report.subscription_retry_recoveries, 0);
        assert_eq!(
            simulation.traffic[source]
                .counter(TrafficDirection::Sent, TrafficProvenance::Legitimate),
            TrafficCounter::new(1, bytes)
        );
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Legitimate),
            TrafficCounter::new(1, bytes)
        );
        let link = simulation
            .link_traffic
            .get(&DirectedServiceLink {
                source,
                destination,
            })
            .unwrap();
        assert_eq!(
            link.counter(TrafficDirection::Sent, TrafficProvenance::Legitimate),
            TrafficCounter::new(1, bytes)
        );
        assert_eq!(
            link.counter(TrafficDirection::Received, TrafficProvenance::Legitimate),
            TrafficCounter::new(1, bytes)
        );
        assert_eq!(
            simulation.nodes[destination]
                .wire
                .subscriptions()
                .peer_subscription_count(&SourceId::new(&simulation.peer_ids[source])),
            1
        );
    }

    #[test]
    fn reliable_request_retries_are_bounded_under_total_loss() {
        let mut simulation = simulation(10_000, 2);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        simulation.schedule_subscription_frame(request(&simulation, source, destination));

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_messages, 3);
        assert_eq!(simulation.report.dropped_packets, 3);
        assert_eq!(simulation.report.subscription_retries, 2);
        assert_eq!(simulation.report.subscription_retry_recoveries, 0);
        assert_eq!(
            simulation.traffic[source]
                .counter(TrafficDirection::Sent, TrafficProvenance::Legitimate)
                .messages,
            3
        );
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Legitimate,)
                .messages,
            0
        );
    }

    #[test]
    fn decoded_noop_retry_is_not_counted_as_control_recovery() {
        let mut simulation = simulation(0, 2);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        let payload = simulation
            .fips_codec
            .encode_frame(&FipsPubsubWireMessage::close(SubscriptionId::new(
                "missing-control-test",
            )))
            .unwrap();
        let mut frame = SubscriptionFrame::new(
            source,
            destination,
            payload,
            SubscriptionStore::Ordinary,
            SubscriptionPurpose::LifecycleClose,
            TrafficProvenance::Legitimate,
        );
        frame.attempt = 1;
        simulation.schedule_subscription_frame(frame);

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_messages, 2);
        assert_eq!(simulation.report.subscription_rejections, 0);
        assert_eq!(simulation.report.subscription_retry_recoveries, 0);
        assert_eq!(simulation.report.subscription_close_reopen_successes, 0);
    }

    #[test]
    fn malformed_reliable_retry_is_not_counted_as_control_recovery() {
        let mut simulation = simulation(0, 2);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        let mut frame = SubscriptionFrame::new(
            source,
            destination,
            br#"["REQ","broken""#.to_vec(),
            SubscriptionStore::Ordinary,
            SubscriptionPurpose::Install,
            TrafficProvenance::Legitimate,
        );
        frame.attempt = 1;
        simulation.schedule_subscription_frame(frame);

        assert!(simulation.drain_scheduler().is_err());
        assert_eq!(simulation.report.subscription_rejections, 1);
        assert_eq!(simulation.report.subscription_retry_recoveries, 0);
    }

    #[test]
    fn malformed_spam_is_received_rejected_and_never_retried() {
        let mut simulation = simulation(0, 3);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        let payload = br#"["REQ","broken""#.to_vec();
        let bytes = u64::try_from(payload.len()).unwrap();
        simulation.schedule_subscription_frame(SubscriptionFrame::new(
            source,
            destination,
            payload,
            SubscriptionStore::Ordinary,
            SubscriptionPurpose::Flood,
            TrafficProvenance::Adversarial,
        ));

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_messages, 1);
        assert_eq!(simulation.report.control_plane_wire_bytes, bytes);
        assert_eq!(simulation.report.subscription_rejections, 1);
        assert_eq!(simulation.report.subscription_retries, 0);
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Adversarial),
            TrafficCounter::new(1, bytes)
        );
    }

    #[test]
    fn lost_spam_control_is_best_effort() {
        let mut simulation = simulation(10_000, 3);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        let mut frame = request(&simulation, source, destination);
        frame.purpose = SubscriptionPurpose::Flood;
        frame.traffic_provenance = TrafficProvenance::Adversarial;
        simulation.schedule_subscription_frame(frame);

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_messages, 1);
        assert_eq!(simulation.report.dropped_packets, 1);
        assert_eq!(simulation.report.subscription_retries, 0);
        assert_eq!(simulation.report.subscription_retry_recoveries, 0);
    }

    #[test]
    fn unauthorized_control_is_sent_but_not_received_or_applied() {
        let mut simulation = simulation(0, 3);
        let source = 0;
        let destination = (1..simulation.config.node_count)
            .find(|candidate| !simulation.topology.neighbors[*candidate].contains(&source))
            .unwrap();
        let mut frame = request(&simulation, source, destination);
        frame.traffic_provenance = TrafficProvenance::Adversarial;
        frame.purpose = SubscriptionPurpose::Flood;
        simulation.schedule_subscription_frame(frame);

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.unauthorized_source_drops, 1);
        assert_eq!(
            simulation.traffic[source]
                .counter(TrafficDirection::Sent, TrafficProvenance::Adversarial)
                .messages,
            1
        );
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Adversarial,)
                .messages,
            0
        );
        assert_eq!(
            simulation.nodes[destination]
                .wire
                .subscriptions()
                .peer_subscription_count(&SourceId::new(&simulation.peer_ids[source])),
            0
        );
    }

    #[test]
    fn link_down_control_retries_without_recording_receive_service() {
        let mut simulation = simulation(0, 2);
        let source = 0;
        let destination = simulation.topology.neighbors[source][0];
        simulation.begin_link_outage(LinkOutage::new(
            source,
            destination,
            OutageCause::Stochastic,
        ));
        simulation.schedule_subscription_frame(request(&simulation, source, destination));

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_messages, 3);
        assert_eq!(simulation.report.dropped_packets, 3);
        assert_eq!(simulation.report.subscription_retries, 2);
        assert_eq!(simulation.report.subscription_retry_recoveries, 0);
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Legitimate,)
                .messages,
            0
        );
    }

    #[test]
    fn lifecycle_close_schedules_reopen_after_observed_removal() {
        let mut simulation = simulation(0, 2);
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        simulation.exercise_subscription_lifecycle().unwrap();

        assert_eq!(simulation.report.subscription_close_reopen_successes, 0);
        simulation.drain_scheduler().unwrap();
        assert!(simulation.report.subscription_close_reopen_successes > 0);
    }

    #[test]
    fn successful_resend_is_counted_as_control_recovery() {
        let mut simulation = simulation(5_000, 2);
        let (frame, destination) = (0..simulation.config.node_count)
            .flat_map(|source| {
                simulation.topology.neighbors[source]
                    .iter()
                    .copied()
                    .map(move |destination| (source, destination))
            })
            .find_map(|(source, destination)| {
                let frame = request(&simulation, source, destination);
                (would_lose(&simulation, &frame, 0) && !would_lose(&simulation, &frame, 1))
                    .then_some((frame, destination))
            })
            .expect("topology should contain a first-loss, second-delivery control route");
        simulation.schedule_subscription_frame(frame);

        simulation.drain_scheduler().unwrap();

        assert_eq!(simulation.report.subscription_retries, 1);
        assert_eq!(simulation.report.subscription_retry_recoveries, 1);
        assert_eq!(
            simulation.traffic[destination]
                .counter(TrafficDirection::Received, TrafficProvenance::Legitimate,)
                .messages,
            1
        );
    }

    fn would_lose(simulation: &Simulation, frame: &SubscriptionFrame, attempt: u64) -> bool {
        super::super::mix64(
            simulation.config.seed
                ^ subscription_fault_key(frame).rotate_left(7)
                ^ attempt.rotate_left(17)
                ^ u64::try_from(frame.source)
                    .unwrap_or(u64::MAX)
                    .rotate_left(23)
                ^ u64::try_from(frame.destination)
                    .unwrap_or(u64::MAX)
                    .rotate_left(41),
        ) % 10_000
            < u64::from(simulation.config.loss_basis_points)
    }
}
