use std::collections::{BTreeMap, HashSet};

use nostr::Event;

use crate::metrics::{basis_points, summarize_distribution};

use super::{
    DirectedServiceLink, InvWantAction, InvWantWireMessage, NodeRole, Result, Simulation,
    SimulationError,
};

/// One useful edge in an event's first-acceptance dissemination tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDeliveryRecord {
    pub event_id: String,
    pub provider: usize,
    pub receiver: usize,
    pub payload_bytes: u64,
    pub accepted_at_ms: u64,
    /// Exactly matches final interested ordinary-peer service accounting.
    pub final_interested_delivery: bool,
}

impl VerifiedDeliveryRecord {
    #[must_use]
    pub const fn link(&self) -> DirectedServiceLink {
        DirectedServiceLink {
            source: self.provider,
            destination: self.receiver,
        }
    }
}

impl Simulation {
    pub(super) fn finalize_delivery_paths(&mut self) -> Result<()> {
        let hops = delivery_path_hops(&self.report.verified_delivery_records)?;
        let summary = summarize_distribution(&hops);
        self.report.delivery_path_samples = summary.count;
        self.report.multihop_interested_deliveries = hops.iter().filter(|hop| **hop > 1).count();
        self.report.multihop_interested_delivery_basis_points = basis_points(
            u64::try_from(self.report.multihop_interested_deliveries).unwrap_or(u64::MAX),
            u64::try_from(summary.count).unwrap_or(u64::MAX),
        );
        self.report.delivery_path_hops_p50 = summary.p50;
        self.report.delivery_path_hops_p95 = summary.p95;
        self.report.delivery_path_hops_p99 = summary.p99;
        self.report.delivery_path_hops_max = summary.max;
        Ok(())
    }

    pub(in crate::simulation) fn dispatch_actions(
        &mut self,
        receiver: usize,
        actions: Vec<InvWantAction>,
    ) -> Result<()> {
        let forwarded = forwarded_event_ids(&actions);
        for action in actions {
            match action {
                InvWantAction::Deliver { source_peer, event } => {
                    self.dispatch_delivery(receiver, &source_peer, &event, &forwarded)?;
                }
                InvWantAction::Send { peer_id, message } => {
                    let destination = self.peer_index(&peer_id)?;
                    if self
                        .candidate_peer(receiver, destination)?
                        .is_some_and(|peer| peer.is_unknown())
                    {
                        self.report.unknown_candidate_sends =
                            self.report.unknown_candidate_sends.saturating_add(1);
                    }
                    self.enqueue_message(receiver, destination, &message)?;
                }
            }
        }
        Ok(())
    }

    fn dispatch_delivery(
        &mut self,
        receiver: usize,
        source_peer: &str,
        event: &Event,
        forwarded: &HashSet<String>,
    ) -> Result<()> {
        let event_id = event.id.to_hex();
        let first_delivery = !self
            .delivery_times
            .contains_key(&(receiver, event_id.clone()));
        let delivery_metadata = self.events.get(&event_id).and_then(|metadata| {
            let interested = metadata.interested.contains(&receiver);
            is_verified_service(
                first_delivery,
                metadata.legitimate && metadata.publisher != receiver,
                interested || forwarded.contains(&event_id),
            )
            .then_some((metadata.payload_bytes, interested))
        });
        if !self.nodes[receiver].local_events.contains_key(&event_id) {
            return Err(SimulationError::Pubsub(format!(
                "delivered event {event_id} was not retained as verified"
            )));
        }
        if self.reputation_events.contains_key(&event_id) {
            self.finish_delivery_retries(receiver, &event_id);
            return self.receive_reputation_event(receiver, event);
        }

        let accepted_at_ms = self.scheduler.now_ms();
        self.record_delivery(receiver, &event_id, accepted_at_ms);
        if let Some((bytes, interested)) = delivery_metadata {
            let provider = self.peer_index(source_peer)?;
            let final_interested_delivery =
                interested && self.topology.roles[receiver] == NodeRole::Peer;
            self.record_verified_delivery(
                event_id,
                provider,
                receiver,
                bytes,
                accepted_at_ms,
                final_interested_delivery,
            );
            if final_interested_delivery {
                let link = DirectedServiceLink {
                    source: provider,
                    destination: receiver,
                };
                let credit = self.delivery_credits.entry(link).or_default();
                *credit = credit.saturating_add(1);
                let delivered_bytes = self.delivery_bytes.entry(link).or_default();
                *delivered_bytes = delivered_bytes.saturating_add(bytes);
            }
        }
        Ok(())
    }

    fn record_verified_delivery(
        &mut self,
        event_id: String,
        provider: usize,
        receiver: usize,
        payload_bytes: u64,
        accepted_at_ms: u64,
        final_interested_delivery: bool,
    ) {
        let record = VerifiedDeliveryRecord {
            event_id,
            provider,
            receiver,
            payload_bytes,
            accepted_at_ms,
            final_interested_delivery,
        };
        let link = record.link();
        let verified = self.verified_delivery_credits.entry(link).or_default();
        *verified = verified.saturating_add(1);
        let verified_bytes = self.verified_delivery_bytes.entry(link).or_default();
        *verified_bytes = verified_bytes.saturating_add(payload_bytes);
        self.report.verified_delivery_records.push(record);
    }

    fn enqueue_message(
        &mut self,
        source: usize,
        destination: usize,
        message: &InvWantWireMessage,
    ) -> Result<()> {
        let latency = self.link_latency_ms(source, destination);
        self.enqueue_message_at(source, destination, message, latency)
    }
}

fn delivery_path_hops(records: &[VerifiedDeliveryRecord]) -> Result<Vec<u64>> {
    let mut by_event = BTreeMap::<&str, Vec<&VerifiedDeliveryRecord>>::new();
    for record in records {
        by_event.entry(&record.event_id).or_default().push(record);
    }
    let mut hops = Vec::new();
    for (event_id, event_records) in by_event {
        let mut parents = BTreeMap::new();
        for record in &event_records {
            if parents.insert(record.receiver, record.provider).is_some() {
                return Err(SimulationError::Pubsub(format!(
                    "duplicate first-delivery parent for event {event_id} receiver {}",
                    record.receiver
                )));
            }
        }
        for record in event_records
            .iter()
            .filter(|record| record.final_interested_delivery)
        {
            let mut cursor = record.receiver;
            let mut visited = HashSet::new();
            let mut depth = 0_u64;
            while let Some(parent) = parents.get(&cursor).copied() {
                if !visited.insert(cursor) {
                    return Err(SimulationError::Pubsub(format!(
                        "delivery cycle for event {event_id} at node {cursor}"
                    )));
                }
                depth = depth.saturating_add(1);
                cursor = parent;
            }
            hops.push(depth);
        }
    }
    Ok(hops)
}

fn forwarded_event_ids(actions: &[InvWantAction]) -> HashSet<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            InvWantAction::Send {
                message:
                    InvWantWireMessage::Inventory { event_id, .. }
                    | InvWantWireMessage::Frame { event_id, .. },
                ..
            } => Some(event_id.clone()),
            InvWantAction::Send {
                message: InvWantWireMessage::Want { .. },
                ..
            }
            | InvWantAction::Deliver { .. } => None,
        })
        .collect()
}

const fn is_verified_service(
    first_delivery: bool,
    legitimate_non_publisher: bool,
    interested_or_forwarded: bool,
) -> bool {
    first_delivery && legitimate_non_publisher && interested_or_forwarded
}

#[cfg(test)]
mod tests {
    use super::{VerifiedDeliveryRecord, delivery_path_hops, is_verified_service};

    #[test]
    fn useful_service_excludes_spam_duplicates_publishers_and_dead_ends() {
        assert!(!is_verified_service(true, false, true));
        assert!(!is_verified_service(false, true, true));
        assert!(!is_verified_service(true, true, false));
        assert!(is_verified_service(true, true, true));
    }

    #[test]
    fn interested_path_depth_follows_first_acceptance_parents() {
        let records = vec![record(0, 1, false), record(1, 2, true), record(1, 3, true)];
        assert_eq!(delivery_path_hops(&records).unwrap(), vec![2, 2]);
    }

    fn record(provider: usize, receiver: usize, final_delivery: bool) -> VerifiedDeliveryRecord {
        VerifiedDeliveryRecord {
            event_id: "event".to_string(),
            provider,
            receiver,
            payload_bytes: 1,
            accepted_at_ms: u64::try_from(receiver).unwrap(),
            final_interested_delivery: final_delivery,
        }
    }
}
