use std::collections::BTreeMap;

use crate::metrics::{TrafficScope, basis_points, summarize_latencies, summarize_load};
use crate::topology::NodeRole;

use super::{Result, Simulation, SpamIdentity, SubscriptionClass, class_name};

struct DeliveryObservations {
    delivered: usize,
    latencies: Vec<u64>,
    by_class: BTreeMap<String, (usize, usize)>,
}

impl Simulation {
    pub(super) fn finalize_report(&mut self) -> Result<()> {
        let observations = self.delivery_observations();
        let delivered = observations.delivered;
        self.finalize_delivery_metrics(observations);
        self.finalize_adversarial_metrics();
        self.finalize_service_metrics();
        self.finalize_efficiency_metrics(delivered);
        self.finalize_resource_metrics()?;
        Ok(())
    }

    fn delivery_observations(&self) -> DeliveryObservations {
        let mut observations = DeliveryObservations {
            delivered: 0,
            latencies: Vec::new(),
            by_class: BTreeMap::new(),
        };
        for (event_id, metadata) in &self.events {
            if !metadata.legitimate {
                continue;
            }
            let entry = observations
                .by_class
                .entry(class_name(metadata.class).to_string())
                .or_default();
            entry.1 = entry.1.saturating_add(metadata.interested.len());
            for interested in &metadata.interested {
                let Some(delivered_at) = self
                    .delivery_times
                    .get(&(*interested, event_id.clone()))
                    .copied()
                else {
                    continue;
                };
                observations.delivered = observations.delivered.saturating_add(1);
                entry.0 = entry.0.saturating_add(1);
                observations
                    .latencies
                    .push(delivered_at.saturating_sub(metadata.publish_at_ms));
            }
        }
        observations
    }

    fn finalize_delivery_metrics(&mut self, observations: DeliveryObservations) {
        self.report.delivered_legitimate = observations.delivered;
        self.report.delivery_basis_points = basis_points(
            usize_as_u64(observations.delivered),
            usize_as_u64(self.report.expected_legitimate_deliveries),
        );
        self.report.undelivered_legitimate = self
            .report
            .expected_legitimate_deliveries
            .saturating_sub(observations.delivered);
        for (class, (delivered, expected)) in observations.by_class {
            self.report.cohort_delivery_basis_points.insert(
                class,
                basis_points(usize_as_u64(delivered), usize_as_u64(expected)),
            );
        }
        self.report.worst_cohort_delivery_basis_points = self
            .report
            .cohort_delivery_basis_points
            .values()
            .copied()
            .min()
            .unwrap_or(0);
        let latency = summarize_latencies(&observations.latencies);
        self.report.latency_sample_count = latency.sample_count;
        self.report.latency_p50_ms = latency.p50;
        self.report.latency_p95_ms = latency.p95;
        self.report.latency_p99_ms = latency.p99;
        self.report.max_delivered_latency_ms = latency.max;
        self.report.machine_removal_latency_p95_ms =
            summarize_latencies(&self.reputation_removal_latencies).p95;
    }

    fn finalize_adversarial_metrics(&mut self) {
        let expected_spam = self.report.expected_signed_spam_deliveries;
        self.report.signed_spam_delivery_basis_points = basis_points(
            usize_as_u64(self.report.spam_delivered),
            usize_as_u64(expected_spam),
        );
        self.finalize_spam_delivery_by_class();
        self.finalize_spam_delivery_by_identity();
        self.report.spam_suppression_basis_points = basis_points(
            usize_as_u64(expected_spam.saturating_sub(self.report.spam_delivered)),
            usize_as_u64(expected_spam),
        );
        self.finalize_filter_suppression();
        self.report.disrupted_legitimate_transfers = self.disrupted_transfers.len();
        self.report
            .eventual_disrupted_transfer_recovery_basis_points = basis_points(
            usize_as_u64(self.report.eventual_disrupted_transfer_recoveries),
            usize_as_u64(self.report.disrupted_legitimate_transfers),
        );
    }

    fn finalize_spam_delivery_by_class(&mut self) {
        for class in SubscriptionClass::ALL {
            let class = class_name(class).to_string();
            let expected = self
                .report
                .expected_signed_spam_deliveries_by_class
                .get(&class)
                .copied()
                .unwrap_or(0);
            let delivered = self
                .report
                .signed_spam_deliveries_by_class
                .get(&class)
                .copied()
                .unwrap_or(0);
            self.report
                .signed_spam_delivery_basis_points_by_class
                .insert(
                    class,
                    basis_points(usize_as_u64(delivered), usize_as_u64(expected)),
                );
        }
    }

    fn finalize_spam_delivery_by_identity(&mut self) {
        for identity in SpamIdentity::ALL {
            let identity = identity.as_str().to_string();
            let expected = self
                .report
                .expected_signed_spam_deliveries_by_identity
                .get(&identity)
                .copied()
                .unwrap_or(0);
            let delivered = self
                .report
                .signed_spam_deliveries_by_identity
                .get(&identity)
                .copied()
                .unwrap_or(0);
            self.report
                .signed_spam_deliveries_by_identity
                .entry(identity.clone())
                .or_default();
            self.report
                .signed_spam_suppression_basis_points_by_identity
                .insert(
                    identity.clone(),
                    basis_points(
                        usize_as_u64(expected.saturating_sub(delivered)),
                        usize_as_u64(expected),
                    ),
                );
            let expected_machine = self
                .report
                .expected_machine_admitted_spam_deliveries_by_identity
                .get(&identity)
                .copied()
                .unwrap_or(0);
            let delivered_machine = self
                .report
                .machine_admitted_spam_deliveries_by_identity
                .get(&identity)
                .copied()
                .unwrap_or(0);
            self.report
                .machine_admitted_spam_deliveries_by_identity
                .entry(identity.clone())
                .or_default();
            self.report
                .machine_admitted_spam_suppression_basis_points_by_identity
                .insert(
                    identity,
                    basis_points(
                        usize_as_u64(expected_machine.saturating_sub(delivered_machine)),
                        usize_as_u64(expected_machine),
                    ),
                );
        }
    }

    fn finalize_filter_suppression(&mut self) {
        self.report.filter_suppression_basis_points = basis_points(
            usize_as_u64(self.report.spam_filter_suppressed_peer_links),
            usize_as_u64(self.report.spam_filter_peer_link_opportunities),
        );
        for class in SubscriptionClass::ALL {
            let class = class_name(class).to_string();
            let opportunities = self
                .report
                .spam_filter_peer_link_opportunities_by_class
                .get(&class)
                .copied()
                .unwrap_or(0);
            let suppressed = self
                .report
                .spam_filter_suppressed_peer_links_by_class
                .get(&class)
                .copied()
                .unwrap_or(0);
            self.report
                .spam_filter_suppression_basis_points_by_class
                .insert(
                    class,
                    basis_points(usize_as_u64(suppressed), usize_as_u64(opportunities)),
                );
        }
    }

    fn finalize_efficiency_metrics(&mut self, delivered: usize) {
        let load = summarize_load(&self.traffic, TrafficScope::Sent);
        self.report.legitimate_protocol_bytes = load.legitimate.bytes;
        self.report.adversarial_protocol_bytes = load.adversarial.bytes;
        self.report.legitimate_protocol_byte_share_basis_points =
            load.legitimate_byte_share_basis_points;
        self.report.protocol_messages_per_interested_delivery_milli =
            per_delivery(load.total.messages.saturating_mul(1_000), delivered);
        self.report.total_protocol_bytes = self
            .report
            .data_plane_wire_bytes
            .saturating_add(self.report.control_plane_wire_bytes);
        self.report.sent_link_protocol_bytes = self
            .report
            .protocol_service_by_link
            .values()
            .map(|ledger| ledger.total(TrafficScope::Sent).bytes)
            .fold(0, u64::saturating_add);
        self.report.sent_role_protocol_bytes = self
            .report
            .protocol_service_by_role
            .values()
            .map(|ledger| ledger.total(TrafficScope::Sent).bytes)
            .fold(0, u64::saturating_add);
        self.report.protocol_bytes_per_interested_delivery =
            per_delivery(self.report.total_protocol_bytes, delivered);
        debug_assert!(self.report.protocol_accounting_is_conserved());
    }

    fn finalize_service_metrics(&mut self) {
        let supernode_ledgers = self
            .traffic
            .iter()
            .enumerate()
            .filter(|(index, _)| self.topology.roles[*index] == NodeRole::Supernode)
            .map(|(_, ledger)| *ledger)
            .collect::<Vec<_>>();
        let supernode_load = summarize_load(&supernode_ledgers, TrafficScope::Combined);
        self.report.supernode_max_service_bytes = supernode_load.max_load.bytes;
        self.report.supernode_mean_service_bytes = supernode_load.mean_load.bytes;
        self.report.supernode_load_gini_basis_points = supernode_load.byte_gini_basis_points;
        for (role, ledger) in self.topology.roles.iter().copied().zip(&self.traffic) {
            let combined = self
                .report
                .protocol_service_by_role
                .get(&role)
                .copied()
                .unwrap_or_default()
                .saturating_add(*ledger);
            self.report.protocol_service_by_role.insert(role, combined);
        }
        for (link, credits) in &self.delivery_credits {
            let role_credits = self
                .report
                .interested_delivery_credit_by_source_role
                .entry(self.topology.roles[link.source])
                .or_default();
            *role_credits = role_credits.saturating_add(*credits);
        }
        self.report.protocol_service_by_link = std::mem::take(&mut self.link_traffic);
        self.report.interested_delivery_credit_by_link = std::mem::take(&mut self.delivery_credits);
    }
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn per_delivery(numerator: u64, delivered: usize) -> u64 {
    if delivered == 0 {
        0
    } else {
        numerator / usize_as_u64(delivered)
    }
}
