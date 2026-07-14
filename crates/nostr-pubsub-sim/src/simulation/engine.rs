use super::{
    CHURN_END_MS, CHURN_START_MS, DirectedServiceLink, FipsPubsubWireMessage, InvWantWireMessage,
    LinkOutage, MALFORMED_TRAINING_SAMPLES, MeshPeer, NodeRole, OutageCause, Packet,
    PeerSelectionMode, PubsubDeliveryAction, PubsubDeliveryPolicy, PubsubPeerInterest,
    REPUTATION_SWEEP_MS, Result, ScheduledAction, Simulation, SimulationError, SourceId,
    SubscriptionClass, SubscriptionPurpose, TopologyStrategy, TrafficDirection, TrafficProvenance,
    VerifiedEvent, hash_bytes, is_fresh_sybil, is_quiet_attacker, machine_admitted_class,
    message_fault_key, message_traffic_provenance, mix64, profile_subscription_id, pubsub_error,
};
use nostr::{Event, Kind};
use nostr_social_memory::{RATING_KIND, rating_from_event};

impl Simulation {
    pub(super) fn schedule_attack_pressure(&mut self) -> Result<()> {
        if self.config.attacker_count == 0 {
            return Ok(());
        }
        let phase_start_ms = self.scheduler.now_ms();
        for destination in self.config.attacker_count..self.config.node_count {
            let attackers = self.topology.neighbors[destination]
                .iter()
                .copied()
                .filter(|source| *source < self.config.attacker_count)
                .collect::<Vec<_>>();
            for source in attackers {
                if is_fresh_sybil(source) {
                    continue;
                }
                if !is_quiet_attacker(source) {
                    for sample in 0..MALFORMED_TRAINING_SAMPLES {
                        self.enqueue_raw_packet_at(
                            source,
                            destination,
                            phase_start_ms
                                .saturating_add(1)
                                .saturating_add(u64::try_from(sample).unwrap_or(0)),
                            br#"{"protocol":"wrong","version":1,"message":{}}"#.to_vec(),
                        );
                    }
                    for sample in 0..2 {
                        self.enqueue_raw_packet_at(
                            source,
                            destination,
                            phase_start_ms
                                .saturating_add(REPUTATION_SWEEP_MS)
                                .saturating_add(3)
                                .saturating_add(u64::try_from(sample).unwrap_or(0)),
                            br#"{"protocol":"wrong","version":1,"message":{}}"#.to_vec(),
                        );
                    }
                }
                let inventory_count = self.config.fake_inventories_per_attack_link;
                for sequence in 0..inventory_count {
                    let event_id = format!(
                        "{:064x}",
                        mix64(
                            self.config.seed
                                ^ u64::try_from(source).unwrap_or(u64::MAX)
                                ^ u64::try_from(destination)
                                    .unwrap_or(u64::MAX)
                                    .rotate_left(17)
                                ^ u64::try_from(sequence).unwrap_or(u64::MAX).rotate_left(31),
                        )
                    );
                    let message = InvWantWireMessage::Inventory {
                        event_id,
                        event_kind: 37_195,
                        payload_bytes: 512,
                        hop_limit: self.config.max_hops.min(3),
                    };
                    self.enqueue_message_at(source, destination, &message, 7)?;
                    self.report.injected_attack_inventories =
                        self.report.injected_attack_inventories.saturating_add(1);
                }
            }
        }
        self.schedule_unauthorized_source_probes()?;
        Ok(())
    }

    fn schedule_unauthorized_source_probes(&mut self) -> Result<()> {
        for destination in self.config.attacker_count..self.config.node_count {
            let Some(source) = (0..self.config.attacker_count)
                .find(|source| !self.topology.neighbors[destination].contains(source))
            else {
                continue;
            };
            let event_id = format!(
                "{:064x}",
                mix64(
                    self.config.seed
                        ^ u64::try_from(source).unwrap_or(u64::MAX).rotate_left(11)
                        ^ u64::try_from(destination)
                            .unwrap_or(u64::MAX)
                            .rotate_left(43)
                        ^ 0x554e_4155_5448,
                )
            );
            self.enqueue_message_at(
                source,
                destination,
                &InvWantWireMessage::Inventory {
                    event_id,
                    event_kind: 37_195,
                    payload_bytes: 512,
                    hop_limit: self.config.max_hops.min(3),
                },
                6,
            )?;
            self.report.injected_attack_inventories =
                self.report.injected_attack_inventories.saturating_add(1);
        }
        Ok(())
    }

    pub(super) fn schedule_churn(&mut self) {
        if self.config.churn_basis_points == 0 {
            return;
        }
        let phase_start_ms = self.scheduler.now_ms();
        for left in 0..self.config.node_count {
            for right in self.topology.neighbors[left]
                .iter()
                .copied()
                .filter(|right| *right > left)
            {
                if left < self.config.attacker_count || right < self.config.attacker_count {
                    continue;
                }
                let sample = mix64(
                    self.config.seed
                        ^ u64::try_from(left).unwrap_or(u64::MAX).rotate_left(13)
                        ^ u64::try_from(right).unwrap_or(u64::MAX).rotate_left(37),
                ) % 10_000;
                if sample < u64::from(self.config.churn_basis_points) {
                    let outage = LinkOutage::new(left, right, OutageCause::Stochastic);
                    self.scheduler.schedule_at(
                        phase_start_ms.saturating_add(CHURN_START_MS),
                        ScheduledAction::LinkDown(outage),
                    );
                    self.scheduler.schedule_at(
                        phase_start_ms.saturating_add(CHURN_END_MS),
                        ScheduledAction::LinkUp(outage),
                    );
                    self.report.churned_links = self.report.churned_links.saturating_add(1);
                }
            }
        }
        if self.config.topology == TopologyStrategy::HybridSupernodes
            && let Some(supernode) = self.topology.honest_supernodes.first().copied()
        {
            for peer in self.topology.neighbors[supernode].clone() {
                let outage = LinkOutage::new(supernode, peer, OutageCause::ForcedSupernode);
                self.scheduler.schedule_at(
                    phase_start_ms
                        .saturating_add(CHURN_START_MS)
                        .saturating_add(4),
                    ScheduledAction::LinkDown(outage),
                );
                self.scheduler.schedule_at(
                    phase_start_ms.saturating_add(CHURN_END_MS.saturating_sub(4)),
                    ScheduledAction::LinkUp(outage),
                );
                self.report.churned_links = self.report.churned_links.saturating_add(1);
            }
        }
    }

    pub(super) fn schedule_publications(&mut self) {
        let phase_start_ms = self.scheduler.now_ms();
        let mut publications = self
            .events
            .iter_mut()
            .map(|(event_id, metadata)| {
                metadata.publish_at_ms = metadata.publish_at_ms.saturating_add(phase_start_ms);
                (metadata.publish_at_ms, event_id.clone())
            })
            .collect::<Vec<_>>();
        publications.sort();
        for (at_ms, event_id) in publications {
            self.scheduler
                .schedule_at(at_ms, ScheduledAction::Publish(event_id));
        }
    }

    pub(super) fn drain_scheduler(&mut self) -> Result<()> {
        while let Some(action) = self.scheduler.pop_next() {
            if self.report.processed_actions >= self.config.max_processed_actions {
                return Err(SimulationError::ActionBudgetExceeded(
                    self.config.max_processed_actions,
                ));
            }
            self.report.processed_actions = self.report.processed_actions.saturating_add(1);
            match action {
                ScheduledAction::Packet(packet) => {
                    self.report.processed_messages =
                        self.report.processed_messages.saturating_add(1);
                    self.process_packet(packet)?;
                }
                ScheduledAction::SendSubscription(frame) => {
                    self.send_subscription_frame(frame);
                }
                ScheduledAction::SubscriptionArrived(frame) => {
                    self.report.processed_messages =
                        self.report.processed_messages.saturating_add(1);
                    self.process_subscription_frame(frame)?;
                }
                ScheduledAction::Publish(event_id) => self.publish_event(&event_id)?,
                ScheduledAction::RetryInventory {
                    source,
                    destination,
                    event_id,
                } => self.retry_inventory(source, destination, &event_id)?,
                ScheduledAction::AdvanceVirtualTime => {}
                ScheduledAction::ReputationSweep => self.run_reputation_sweep()?,
                ScheduledAction::LinkDown(outage) => {
                    self.begin_link_outage(outage);
                }
                ScheduledAction::LinkUp(outage) => {
                    if self.end_link_outage(outage) {
                        let (left, right) = outage.endpoints();
                        self.restore_link(left, right)?;
                    }
                }
            }
        }
        self.report.max_queue_depth = self.scheduler.peak_pending_len();
        self.report.virtual_duration_ms = self.scheduler.now_ms();
        Ok(())
    }

    fn restore_link(&mut self, left: usize, right: usize) -> Result<()> {
        for (provider, subscriber) in [(left, right), (right, left)] {
            let filters = self.subscription_filters_for(subscriber);
            self.schedule_subscription_message(
                subscriber,
                provider,
                &FipsPubsubWireMessage::req(profile_subscription_id(subscriber), filters),
                SubscriptionPurpose::Reconnect,
                TrafficProvenance::Legitimate,
            )?;
        }
        Ok(())
    }

    pub(in crate::simulation) fn replay_link_direction(
        &mut self,
        source: usize,
        destination: usize,
    ) -> Result<()> {
        let mut events = self.nodes[source]
            .local_events
            .values()
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.id);
        for event in events {
            self.record_cpu_work(source, |work| {
                work.signature_checks = work.signature_checks.saturating_add(1);
            });
            let verified = VerifiedEvent::try_from(event.clone()).map_err(pubsub_error)?;
            let is_rating = self.is_rating_event(source, &verified);
            if !self.subscription_announces(source, destination, &verified, is_rating)
                || self.candidate_peer(source, destination)?.is_none()
            {
                continue;
            }
            self.record_avoided_signature_check(source);
            let actions = self.nodes[source]
                .mesh
                .replay_verified_to_peer(
                    verified,
                    &self.peer_ids[destination],
                    self.scheduler.now_ms(),
                )
                .map_err(pubsub_error)?;
            self.observe_core_resource_state(source);
            self.dispatch_actions(source, actions)?;
        }
        Ok(())
    }

    fn publish_event(&mut self, event_id: &str) -> Result<()> {
        let metadata = self
            .events
            .get(event_id)
            .cloned()
            .ok_or_else(|| SimulationError::Pubsub(format!("missing event {event_id}")))?;
        let peers = self.interested_mesh_peers(metadata.publisher, &metadata.verified)?;
        self.retain_local_event(
            metadata.publisher,
            event_id.to_string(),
            metadata.event.clone(),
        )?;
        if metadata.legitimate && metadata.interested.contains(&metadata.publisher) {
            self.report.local_legitimate_deliveries =
                self.report.local_legitimate_deliveries.saturating_add(1);
            self.record_delivery(metadata.publisher, event_id, metadata.publish_at_ms);
        }
        self.record_cpu_work(metadata.publisher, |work| {
            work.mesh_candidates = work
                .mesh_candidates
                .saturating_add(u64::try_from(peers.len()).unwrap_or(u64::MAX));
            work.avoided_signature_checks = work.avoided_signature_checks.saturating_add(1);
        });
        let actions = self.nodes[metadata.publisher]
            .mesh
            .publish_verified(metadata.verified, &peers, self.scheduler.now_ms())
            .map_err(pubsub_error)?;
        self.observe_core_resource_state(metadata.publisher);
        self.dispatch_actions(metadata.publisher, actions)
    }

    fn retry_inventory(&mut self, source: usize, destination: usize, event_id: &str) -> Result<()> {
        if self.nodes[destination].local_events.contains_key(event_id)
            || self.nodes[destination].rejected_events.contains(event_id)
        {
            self.retry_counts
                .remove(&(source, destination, event_id.to_string()));
            return Ok(());
        }
        let Some(event) = self.nodes[source].local_events.get(event_id).cloned() else {
            self.retry_counts
                .remove(&(source, destination, event_id.to_string()));
            return Ok(());
        };
        let key = (source, destination, event_id.to_string());
        let Some(state) = self.retry_counts.get_mut(&key) else {
            return Ok(());
        };
        state.scheduled = false;
        if state.attempts >= self.config.max_retries {
            self.retry_counts.remove(&key);
            return Ok(());
        }
        state.attempts = state.attempts.saturating_add(1);
        self.report.retry_inventories = self.report.retry_inventories.saturating_add(1);
        self.record_cpu_work(source, |work| {
            work.signature_checks = work.signature_checks.saturating_add(1);
            work.avoided_signature_checks = work.avoided_signature_checks.saturating_add(1);
        });
        let verified = VerifiedEvent::try_from(event).map_err(pubsub_error)?;
        let actions = self.nodes[source]
            .mesh
            .replay_verified_to_peer(
                verified,
                &self.peer_ids[destination],
                self.scheduler.now_ms(),
            )
            .map_err(pubsub_error)?;
        self.observe_core_resource_state(source);
        self.dispatch_actions(source, actions)?;
        Ok(())
    }

    pub(super) fn process_packet(&mut self, packet: Packet) -> Result<()> {
        let Packet {
            source,
            destination,
            payload,
        } = packet;
        let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        self.remove_queued_resource_bytes(destination, bytes);
        if !self.topology.neighbors[destination].contains(&source) {
            self.report.unauthorized_source_drops =
                self.report.unauthorized_source_drops.saturating_add(1);
            return Ok(());
        }
        if !self.link_is_active(source, destination) {
            self.report.dropped_packets = self.report.dropped_packets.saturating_add(1);
            self.note_disrupted_payload(source, destination, &payload);
            return Ok(());
        }
        let provenance = self.payload_traffic_provenance(&payload);
        self.traffic[destination].record_message(TrafficDirection::Received, provenance, bytes);
        self.record_link_traffic(
            source,
            destination,
            TrafficDirection::Received,
            provenance,
            bytes,
        );
        if self.topology.roles[destination] == NodeRole::Attacker {
            return self.process_attacker_packet(source, destination, &payload);
        }
        self.process_honest_packet(source, destination, &payload, provenance, bytes)
    }

    fn process_honest_packet(
        &mut self,
        source: usize,
        destination: usize,
        payload: &[u8],
        provenance: TrafficProvenance,
        bytes: u64,
    ) -> Result<()> {
        if self.machine_rejects_ingress(source, destination, provenance) {
            return Ok(());
        }
        self.record_cpu_work(destination, |work| {
            work.invwant_decode_bytes = work.invwant_decode_bytes.saturating_add(bytes);
        });
        let Ok(message) = self.codec.decode(payload) else {
            self.record_invalid_message(destination, source);
            return Ok(());
        };
        if let InvWantWireMessage::Inventory { event_id, .. } = &message {
            self.finish_inventory_retry(source, destination, event_id);
        }
        if let InvWantWireMessage::Want { event_id } = &message {
            self.finish_inventory_retry(destination, source, event_id);
        }
        if self.mode != PeerSelectionMode::Neutral
            && matches!(&message, InvWantWireMessage::Inventory { .. })
        {
            self.bad_observed_at
                .entry((destination, source))
                .or_insert(self.scheduler.now_ms());
        }
        let mut verified_frame = None;
        let peers = match &message {
            InvWantWireMessage::Frame { event_id, event } => {
                self.record_cpu_work(destination, |work| {
                    work.signature_checks = work.signature_checks.saturating_add(1);
                });
                let Ok(verified) = VerifiedEvent::try_from((**event).clone()) else {
                    self.record_invalid_message(destination, source);
                    return Ok(());
                };
                if let Some(drop) = self.admit_event(destination, source, &verified)? {
                    let event_id = event.id.to_hex();
                    self.nodes[destination]
                        .mesh
                        .dismiss_frame(&self.peer_ids[source], &event_id);
                    self.observe_core_resource_state(destination);
                    self.nodes[destination]
                        .rejected_events
                        .insert(event_id.clone());
                    self.record_policy_drop(&event_id, drop);
                    return Ok(());
                }
                let peers = self.interested_mesh_peers(destination, &verified)?;
                verified_frame = Some((event_id.clone(), verified));
                peers
            }
            _ => Vec::new(),
        };
        let source_id = self.peer_ids[source].clone();
        self.record_cpu_work(destination, |work| {
            work.mesh_candidates = work
                .mesh_candidates
                .saturating_add(u64::try_from(peers.len()).unwrap_or(u64::MAX));
        });
        let result = if let Some((event_id, verified)) = verified_frame {
            self.record_avoided_signature_check(destination);
            self.nodes[destination].mesh.receive_verified_frame(
                &source_id,
                &event_id,
                verified,
                &peers,
                self.scheduler.now_ms(),
            )
        } else {
            self.nodes[destination].mesh.receive(
                &source_id,
                message,
                &peers,
                self.scheduler.now_ms(),
            )
        };
        self.observe_core_resource_state(destination);
        let Ok(actions) = result else {
            self.record_mesh_rejection(destination, source);
            return Ok(());
        };
        self.dispatch_actions(destination, actions)
    }

    fn process_attacker_packet(
        &mut self,
        source: usize,
        destination: usize,
        payload: &[u8],
    ) -> Result<()> {
        let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        self.record_cpu_work(destination, |work| {
            work.invwant_decode_bytes = work.invwant_decode_bytes.saturating_add(bytes);
        });
        let Ok(message) = self.codec.decode(payload) else {
            self.report.dropped_at_attackers = self.report.dropped_at_attackers.saturating_add(1);
            return Ok(());
        };
        let serves_owned_event = match &message {
            InvWantWireMessage::Want { event_id } => {
                self.nodes[destination].local_events.contains_key(event_id)
            }
            _ => false,
        };
        if !(is_quiet_attacker(destination) || is_fresh_sybil(destination)) || !serves_owned_event {
            self.report.dropped_at_attackers = self.report.dropped_at_attackers.saturating_add(1);
            return Ok(());
        }
        let result = self.nodes[destination].mesh.receive(
            &self.peer_ids[source],
            message,
            &[],
            self.scheduler.now_ms(),
        );
        self.observe_core_resource_state(destination);
        let actions = result.map_err(pubsub_error)?;
        self.dispatch_actions(destination, actions)
    }

    fn record_invalid_message(&mut self, destination: usize, source: usize) {
        self.report.rejected_malformed_messages =
            self.report.rejected_malformed_messages.saturating_add(1);
        if self.mode != PeerSelectionMode::Neutral {
            self.nodes[destination]
                .mesh
                .record_invalid_message(&self.peer_ids[source]);
            if self.nodes[destination]
                .mesh
                .peer_behavior_score(&self.peer_ids[source])
                .is_some_and(|score| score < 0)
            {
                self.bad_observed_at
                    .entry((destination, source))
                    .or_insert(self.scheduler.now_ms());
            }
        }
        self.observe_core_resource_state(destination);
    }

    fn record_mesh_rejection(&mut self, destination: usize, source: usize) {
        self.report.rejected_malformed_messages =
            self.report.rejected_malformed_messages.saturating_add(1);
        if self.mode != PeerSelectionMode::Neutral
            && self.nodes[destination]
                .mesh
                .peer_behavior_score(&self.peer_ids[source])
                .is_some_and(|score| score < 0)
        {
            self.bad_observed_at
                .entry((destination, source))
                .or_insert(self.scheduler.now_ms());
        }
    }
}

impl Simulation {
    pub(super) fn enqueue_message_at(
        &mut self,
        source: usize,
        destination: usize,
        message: &InvWantWireMessage,
        delay_ms: u64,
    ) -> Result<()> {
        let payload = self.codec.encode(message).map_err(pubsub_error)?;
        let fault_key = message_fault_key(message);
        let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        self.record_cpu_work(source, |work| {
            work.invwant_encode_bytes = work.invwant_encode_bytes.saturating_add(bytes);
        });
        let provenance = message_traffic_provenance(message, &self.events, &self.reputation_events);
        self.traffic[source].record_message(TrafficDirection::Sent, provenance, bytes);
        self.record_link_traffic(
            source,
            destination,
            TrafficDirection::Sent,
            provenance,
            bytes,
        );
        self.report.data_plane_wire_bytes = self.report.data_plane_wire_bytes.saturating_add(bytes);
        match message {
            InvWantWireMessage::Inventory { .. } => {
                self.report.inventory_messages = self.report.inventory_messages.saturating_add(1);
            }
            InvWantWireMessage::Want { .. } => {
                self.report.want_messages = self.report.want_messages.saturating_add(1);
            }
            InvWantWireMessage::Frame { .. } => {
                self.report.frame_messages = self.report.frame_messages.saturating_add(1);
            }
        }
        let arrival_at_ms = self.scheduler.now_ms().saturating_add(delay_ms);
        if self.packet_is_lost(source, destination, fault_key) {
            self.report.dropped_packets = self.report.dropped_packets.saturating_add(1);
            self.note_disrupted_message(source, destination, message);
            return Ok(());
        }
        self.add_queued_resource_bytes(destination, bytes);
        self.scheduler.schedule_at(
            arrival_at_ms,
            ScheduledAction::Packet(Packet {
                source,
                destination,
                payload,
            }),
        );
        Ok(())
    }

    fn enqueue_raw_packet_at(
        &mut self,
        source: usize,
        destination: usize,
        at_ms: u64,
        payload: Vec<u8>,
    ) {
        let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        self.traffic[source].record_message(
            TrafficDirection::Sent,
            TrafficProvenance::Adversarial,
            bytes,
        );
        self.record_link_traffic(
            source,
            destination,
            TrafficDirection::Sent,
            TrafficProvenance::Adversarial,
            bytes,
        );
        self.report.data_plane_wire_bytes = self.report.data_plane_wire_bytes.saturating_add(bytes);
        if self.packet_is_lost(source, destination, hash_bytes(&payload)) {
            self.report.dropped_packets = self.report.dropped_packets.saturating_add(1);
            return;
        }
        self.add_queued_resource_bytes(destination, bytes);
        self.scheduler.schedule_at(
            at_ms,
            ScheduledAction::Packet(Packet {
                source,
                destination,
                payload,
            }),
        );
    }

    pub(in crate::simulation) fn record_link_traffic(
        &mut self,
        source: usize,
        destination: usize,
        direction: TrafficDirection,
        provenance: TrafficProvenance,
        bytes: u64,
    ) {
        self.link_traffic
            .entry(DirectedServiceLink {
                source,
                destination,
            })
            .or_default()
            .record_message(direction, provenance, bytes);
    }

    pub(super) fn record_delivery(&mut self, node: usize, event_id: &str, delivered_at_ms: u64) {
        let key = (node, event_id.to_string());
        if self.delivery_times.contains_key(&key) {
            return;
        }
        self.finish_delivery_retries(node, event_id);
        let Some(metadata) = self.events.get(event_id) else {
            self.report.uninterested_deliveries =
                self.report.uninterested_deliveries.saturating_add(1);
            return;
        };
        self.delivery_times.insert(key.clone(), delivered_at_ms);
        if metadata.legitimate {
            let useful_payload = (metadata.publisher != node
                && metadata.interested.contains(&node)
                && self.topology.roles[node] == NodeRole::Peer)
                .then_some(metadata.payload_bytes);
            if !metadata.interested.contains(&node) && self.topology.roles[node] == NodeRole::Peer {
                self.report.uninterested_deliveries =
                    self.report.uninterested_deliveries.saturating_add(1);
                self.report.uninterested_legitimate_deliveries = self
                    .report
                    .uninterested_legitimate_deliveries
                    .saturating_add(1);
            }
            if self.retry_needed.remove(&key) {
                self.report.eventual_disrupted_transfer_recoveries = self
                    .report
                    .eventual_disrupted_transfer_recoveries
                    .saturating_add(1);
            }
            if let Some(bytes) = useful_payload {
                self.record_useful_payload(node, bytes);
            }
        } else if self.topology.roles[node] == NodeRole::Peer {
            if !metadata.interested.contains(&node) {
                self.report.uninterested_deliveries =
                    self.report.uninterested_deliveries.saturating_add(1);
                self.report.uninterested_spam_deliveries =
                    self.report.uninterested_spam_deliveries.saturating_add(1);
                return;
            }
            self.report.spam_delivered = self.report.spam_delivered.saturating_add(1);
            if let Some(identity) = metadata.spam_identity {
                let identity = identity.as_str().to_string();
                let delivered = self
                    .report
                    .signed_spam_deliveries_by_identity
                    .entry(identity.clone())
                    .or_default();
                *delivered = delivered.saturating_add(1);
                if machine_admitted_class(metadata.class) {
                    let delivered = self
                        .report
                        .machine_admitted_spam_deliveries_by_identity
                        .entry(identity)
                        .or_default();
                    *delivered = delivered.saturating_add(1);
                }
            }
            let delivered = self
                .report
                .signed_spam_deliveries_by_class
                .entry(super::class_name(metadata.class).to_string())
                .or_default();
            *delivered = delivered.saturating_add(1);
            if metadata.class == SubscriptionClass::FipsAdvert {
                self.report.unknown_discovery_adverts_delivered = self
                    .report
                    .unknown_discovery_adverts_delivered
                    .saturating_add(1);
            }
        }
    }

    pub(in crate::simulation) fn interested_mesh_peers(
        &mut self,
        source: usize,
        event: &VerifiedEvent,
    ) -> Result<Vec<MeshPeer>> {
        let signed_spam_class = self
            .events
            .get(&event.as_event().id.to_hex())
            .filter(|metadata| !metadata.legitimate)
            .map(|metadata| metadata.class);
        let is_rating = self.is_rating_event(source, event);
        let mut peers = Vec::new();
        for destination in self.topology.neighbors[source].clone() {
            if !self.link_is_active(source, destination) {
                continue;
            }
            let subscribed = self.subscription_announces(source, destination, event, is_rating);
            if let Some(class) = signed_spam_class
                && self.topology.roles[destination] == NodeRole::Peer
            {
                let class = super::class_name(class).to_string();
                self.report.spam_filter_peer_link_opportunities = self
                    .report
                    .spam_filter_peer_link_opportunities
                    .saturating_add(1);
                let opportunities = self
                    .report
                    .spam_filter_peer_link_opportunities_by_class
                    .entry(class.clone())
                    .or_default();
                *opportunities = opportunities.saturating_add(1);
                if !subscribed {
                    self.report.spam_filter_suppressed_peer_links = self
                        .report
                        .spam_filter_suppressed_peer_links
                        .saturating_add(1);
                    let suppressed = self
                        .report
                        .spam_filter_suppressed_peer_links_by_class
                        .entry(class)
                        .or_default();
                    *suppressed = suppressed.saturating_add(1);
                }
            }
            if !subscribed {
                continue;
            }
            if let Some(peer) = self.candidate_peer(source, destination)? {
                peers.push(peer);
            }
        }
        Ok(peers)
    }

    fn subscription_announces(
        &mut self,
        source: usize,
        destination: usize,
        event: &VerifiedEvent,
        is_rating: bool,
    ) -> bool {
        let peer_id = SourceId::new(&self.peer_ids[destination]);
        let remote_candidates = self.nodes[source]
            .wire
            .subscriptions()
            .peer_filter_count(&peer_id);
        let local_filters = if is_rating {
            &self.nodes[destination].rating_filters
        } else {
            &self.nodes[destination].filters
        };
        let local_candidates = local_filters.len();
        let locally_subscribed = PubsubPeerInterest::from_filters(local_filters, event)
            == PubsubPeerInterest::Subscribed;
        self.record_cpu_work(source, |work| {
            work.filter_queries = work.filter_queries.saturating_add(2);
            work.filter_candidates = work.filter_candidates.saturating_add(
                u64::try_from(remote_candidates.saturating_add(local_candidates))
                    .unwrap_or(u64::MAX),
            );
        });
        PubsubDeliveryPolicy::inventory_to_subscribers().action_for_event(
            self.nodes[source].wire.subscriptions(),
            &peer_id,
            event,
        ) == PubsubDeliveryAction::AnnounceInventory
            && locally_subscribed
    }

    fn is_rating_event(&mut self, source: usize, event: &VerifiedEvent) -> bool {
        let event: &Event = event.as_event();
        if event.kind != Kind::from(RATING_KIND) || !event.content.is_empty() {
            return false;
        }
        self.record_cpu_work(source, |work| {
            work.signature_checks = work.signature_checks.saturating_add(1);
        });
        rating_from_event(event).is_ok()
    }

    pub(in crate::simulation) fn candidate_peer(
        &mut self,
        source: usize,
        destination: usize,
    ) -> Result<Option<MeshPeer>> {
        if self.mode == PeerSelectionMode::SharedReputation {
            self.record_cpu_work(source, |work| {
                work.graph_queries = work.graph_queries.saturating_add(1);
            });
        }
        if self.mode == PeerSelectionMode::SharedReputation {
            let Some(policies) = self.nodes[source].machine_policies.as_ref() else {
                return Ok(Some(MeshPeer::new(&self.peer_ids[destination])));
            };
            let machine = policies
                .select_mesh_peer(&self.peer_ids[destination])
                .map_err(pubsub_error)?;
            match machine {
                None => return Ok(None),
                Some(peer) => return Ok(Some(peer)),
            }
        }
        Ok(Some(MeshPeer::new(&self.peer_ids[destination])))
    }

    pub(super) fn peer_index(&self, peer_id: &str) -> Result<usize> {
        self.peer_indices
            .get(peer_id)
            .copied()
            .ok_or_else(|| SimulationError::Pubsub(format!("invalid simulated peer id {peer_id}")))
    }

    pub(in crate::simulation) fn packet_is_lost(
        &mut self,
        source: usize,
        destination: usize,
        fault_key: u64,
    ) -> bool {
        if self.config.loss_basis_points == 0 {
            return false;
        }
        let attempt = self
            .fault_attempts
            .entry((source, destination, fault_key))
            .or_default();
        let attempt_index = *attempt;
        *attempt = attempt.saturating_add(1);
        mix64(
            self.config.seed
                ^ fault_key.rotate_left(7)
                ^ attempt_index.rotate_left(17)
                ^ u64::try_from(source).unwrap_or(u64::MAX).rotate_left(23)
                ^ u64::try_from(destination)
                    .unwrap_or(u64::MAX)
                    .rotate_left(41),
        ) % 10_000
            < u64::from(self.config.loss_basis_points)
    }

    pub(in crate::simulation) fn link_latency_ms(&self, source: usize, destination: usize) -> u64 {
        2 + mix64(
            self.config.seed
                ^ u64::try_from(source).unwrap_or(u64::MAX).rotate_left(11)
                ^ u64::try_from(destination)
                    .unwrap_or(u64::MAX)
                    .rotate_left(29),
        ) % 9
    }

    fn payload_traffic_provenance(&self, payload: &[u8]) -> TrafficProvenance {
        self.codec
            .decode(payload)
            .map_or(TrafficProvenance::Adversarial, |message| {
                message_traffic_provenance(&message, &self.events, &self.reputation_events)
            })
    }
}
