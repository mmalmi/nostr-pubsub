use nostr::{Event, JsonUtil};

use crate::metrics::{
    DistributionSummary, NodeTrafficLedger, TrafficScope, basis_points, summarize_distribution,
};
use crate::topology::NodeRole;

use super::{Result, Simulation, pubsub_error};

/// Primitive production operations used to calibrate CPU cost outside the simulator.
///
/// These counters deliberately have no permanent weights: signature verification,
/// filter matching and graph rebuild costs vary by CPU and workload shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeCpuWork {
    pub invwant_encode_bytes: u64,
    pub invwant_decode_bytes: u64,
    pub fips_encode_bytes: u64,
    pub fips_decode_bytes: u64,
    pub signature_checks: u64,
    /// Signature verifications skipped by production verified-event APIs.
    pub avoided_signature_checks: u64,
    pub filter_queries: u64,
    pub filter_candidates: u64,
    pub mesh_candidates: u64,
    pub graph_queries: u64,
    pub reputation_events_considered: u64,
    pub reputation_rebuild_entries: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeRetainedUsage {
    pub cached_event_bytes: u64,
    pub subscription_state_bytes: u64,
    pub local_filter_bytes: u64,
    pub local_event_bytes: u64,
    pub queued_wire_bytes: u64,
    pub mesh_state_entries: u64,
    pub subscriptions: u64,
    pub filters: u64,
    pub ratings: u64,
    pub raters: u64,
    pub trust_roots: u64,
}

impl NodeRetainedUsage {
    #[must_use]
    pub fn protocol_content_bytes(self) -> u64 {
        self.cached_event_bytes
            .saturating_add(self.subscription_state_bytes)
            .saturating_add(self.queued_wire_bytes)
    }

    #[must_use]
    pub fn exact_content_bytes(self) -> u64 {
        self.cached_event_bytes
            .saturating_add(self.subscription_state_bytes)
            .saturating_add(self.local_filter_bytes)
            .saturating_add(self.local_event_bytes)
            .saturating_add(self.queued_wire_bytes)
    }

    #[must_use]
    pub fn state_entries(self) -> u64 {
        self.mesh_state_entries
            .saturating_add(self.subscriptions)
            .saturating_add(self.filters)
            .saturating_add(self.ratings)
            .saturating_add(self.raters)
            .saturating_add(self.trust_roots)
    }

    fn component_max(self, other: Self) -> Self {
        Self {
            cached_event_bytes: self.cached_event_bytes.max(other.cached_event_bytes),
            subscription_state_bytes: self
                .subscription_state_bytes
                .max(other.subscription_state_bytes),
            local_filter_bytes: self.local_filter_bytes.max(other.local_filter_bytes),
            local_event_bytes: self.local_event_bytes.max(other.local_event_bytes),
            queued_wire_bytes: self.queued_wire_bytes.max(other.queued_wire_bytes),
            mesh_state_entries: self.mesh_state_entries.max(other.mesh_state_entries),
            subscriptions: self.subscriptions.max(other.subscriptions),
            filters: self.filters.max(other.filters),
            ratings: self.ratings.max(other.ratings),
            raters: self.raters.max(other.raters),
            trust_roots: self.trust_roots.max(other.trust_roots),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct NodeResourceLedger {
    pub work: NodeCpuWork,
    pub current: NodeRetainedUsage,
    pub peak: NodeRetainedUsage,
    pub peak_exact_content_bytes: u64,
    pub peak_state_entries: u64,
    pub useful_payload_bytes: u64,
}

impl NodeResourceLedger {
    pub(super) fn observe(&mut self, usage: NodeRetainedUsage) {
        self.current = usage;
        self.peak = self.peak.component_max(usage);
        self.peak_exact_content_bytes = self
            .peak_exact_content_bytes
            .max(usage.exact_content_bytes());
        self.peak_state_entries = self.peak_state_entries.max(usage.state_entries());
    }

    pub(super) fn add_queued_bytes(&mut self, bytes: u64) {
        self.current.queued_wire_bytes = self.current.queued_wire_bytes.saturating_add(bytes);
        self.observe(self.current);
    }

    pub(super) fn remove_queued_bytes(&mut self, bytes: u64) {
        self.current.queued_wire_bytes = self.current.queued_wire_bytes.saturating_sub(bytes);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CpuWorkDistribution {
    pub codec_bytes: DistributionSummary,
    pub signature_checks: DistributionSummary,
    pub avoided_signature_checks: DistributionSummary,
    /// Actual plus avoided checks: the exact no-fast-path counterfactual.
    pub signature_checks_without_verified_paths: DistributionSummary,
    pub filter_queries: DistributionSummary,
    pub filter_candidates: DistributionSummary,
    pub mesh_candidates: DistributionSummary,
    pub graph_queries: DistributionSummary,
    pub reputation_events_considered: DistributionSummary,
    pub reputation_rebuild_entries: DistributionSummary,
}

impl CpuWorkDistribution {
    const EMPTY: Self = Self {
        codec_bytes: EMPTY_DISTRIBUTION,
        signature_checks: EMPTY_DISTRIBUTION,
        avoided_signature_checks: EMPTY_DISTRIBUTION,
        signature_checks_without_verified_paths: EMPTY_DISTRIBUTION,
        filter_queries: EMPTY_DISTRIBUTION,
        filter_candidates: EMPTY_DISTRIBUTION,
        mesh_candidates: EMPTY_DISTRIBUTION,
        graph_queries: EMPTY_DISTRIBUTION,
        reputation_events_considered: EMPTY_DISTRIBUTION,
        reputation_rebuild_entries: EMPTY_DISTRIBUTION,
    };
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedUsageDistribution {
    /// Exact retained encoded content, excluding allocator and container overhead.
    pub exact_content_bytes: DistributionSummary,
    /// Production pubsub content, excluding application filters and event history.
    pub protocol_content_bytes: DistributionSummary,
    pub state_entries: DistributionSummary,
    pub cached_event_bytes: DistributionSummary,
    pub subscription_state_bytes: DistributionSummary,
    pub local_filter_bytes: DistributionSummary,
    pub local_event_bytes: DistributionSummary,
    pub queued_wire_bytes: DistributionSummary,
}

impl RetainedUsageDistribution {
    const EMPTY: Self = Self {
        exact_content_bytes: EMPTY_DISTRIBUTION,
        protocol_content_bytes: EMPTY_DISTRIBUTION,
        state_entries: EMPTY_DISTRIBUTION,
        cached_event_bytes: EMPTY_DISTRIBUTION,
        subscription_state_bytes: EMPTY_DISTRIBUTION,
        local_filter_bytes: EMPTY_DISTRIBUTION,
        local_event_bytes: EMPTY_DISTRIBUTION,
        queued_wire_bytes: EMPTY_DISTRIBUTION,
    };
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceCohortReport {
    pub sent_bytes: DistributionSummary,
    pub received_bytes: DistributionSummary,
    /// Endpoint message handling (`sent + received`), used as a CPU proxy.
    pub combined_messages: DistributionSummary,
    /// Endpoint I/O (`sent + received`), not unique network bytes.
    pub combined_bytes: DistributionSummary,
    pub adversarial_combined_bytes: DistributionSummary,
    pub useful_payload_bytes: DistributionSummary,
    pub cpu_work: CpuWorkDistribution,
    pub peak_retained: RetainedUsageDistribution,
    pub final_retained: RetainedUsageDistribution,
}

impl ResourceCohortReport {
    const EMPTY: Self = Self {
        sent_bytes: EMPTY_DISTRIBUTION,
        received_bytes: EMPTY_DISTRIBUTION,
        combined_messages: EMPTY_DISTRIBUTION,
        combined_bytes: EMPTY_DISTRIBUTION,
        adversarial_combined_bytes: EMPTY_DISTRIBUTION,
        useful_payload_bytes: EMPTY_DISTRIBUTION,
        cpu_work: CpuWorkDistribution::EMPTY,
        peak_retained: RetainedUsageDistribution::EMPTY,
        final_retained: RetainedUsageDistribution::EMPTY,
    };
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SimulationResourceReport {
    pub honest_all: ResourceCohortReport,
    pub honest_peers: ResourceCohortReport,
    pub honest_supernodes: ResourceCohortReport,
    pub attacker_adversarial_sent_bytes: u64,
    pub honest_adversarial_combined_bytes: u64,
    pub victim_bandwidth_amplification_basis_points: u32,
    /// Virtual instant used to run production `maintain` before final gauges.
    pub quiescence_at_ms: u64,
}

impl SimulationResourceReport {
    pub(super) const EMPTY: Self = Self {
        honest_all: ResourceCohortReport::EMPTY,
        honest_peers: ResourceCohortReport::EMPTY,
        honest_supernodes: ResourceCohortReport::EMPTY,
        attacker_adversarial_sent_bytes: 0,
        honest_adversarial_combined_bytes: 0,
        victim_bandwidth_amplification_basis_points: 0,
        quiescence_at_ms: 0,
    };
}

const EMPTY_DISTRIBUTION: DistributionSummary = DistributionSummary {
    count: 0,
    total: 0,
    mean: 0,
    p50: 0,
    p95: 0,
    p99: 0,
    max: 0,
};

impl Simulation {
    pub(super) fn initialize_resource_usage(&mut self) -> Result<()> {
        for node in 0..self.nodes.len() {
            let local_filter_bytes = self.nodes[node]
                .filters
                .iter()
                .chain(self.nodes[node].rating_filters.iter())
                .try_fold(0_u64, |total, filter| {
                    filter
                        .try_as_json()
                        .map(|encoded| {
                            total.saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX))
                        })
                        .map_err(pubsub_error)
                })?;
            self.node_resources[node].current.local_filter_bytes = local_filter_bytes;
            self.observe_core_resource_state(node);
            self.observe_subscription_resource_state(node)?;
        }
        Ok(())
    }

    pub(super) fn observe_core_resource_state(&mut self, node: usize) {
        let mesh = self.nodes[node].mesh.retained_state();
        let mut usage = self.node_resources[node].current;
        usage.cached_event_bytes = mesh.cached_event_bytes;
        usage.mesh_state_entries = usize_as_u64(
            mesh.cached_events
                .saturating_add(mesh.seen_inventories)
                .saturating_add(mesh.delivered_events)
                .saturating_add(mesh.upstream_routes)
                .saturating_add(mesh.pending_events)
                .saturating_add(mesh.pending_peers)
                .saturating_add(mesh.forwarded_wants)
                .saturating_add(mesh.peer_behaviors),
        );
        if let Some(reputation) = self.nodes[node].machine_reputation.as_ref() {
            let snapshot = reputation.snapshot();
            usage.ratings = usize_as_u64(snapshot.retained_ratings);
            usage.raters = usize_as_u64(snapshot.retained_raters);
            usage.trust_roots = usize_as_u64(snapshot.trusted_roots);
            self.node_resources[node].work.reputation_events_considered =
                snapshot.rating_events_considered;
            self.node_resources[node].work.reputation_rebuild_entries =
                snapshot.graph_rebuild_rating_entries;
        }
        self.node_resources[node].observe(usage);
    }

    pub(super) fn observe_subscription_resource_state(&mut self, node: usize) -> Result<()> {
        let snapshot = self.nodes[node]
            .wire
            .subscriptions()
            .retained_snapshot()
            .map_err(pubsub_error)?;
        let mut usage = self.node_resources[node].current;
        usage.subscription_state_bytes = usize_as_u64(snapshot.encoded_req_bytes);
        usage.subscriptions = usize_as_u64(snapshot.subscription_count);
        usage.filters = usize_as_u64(snapshot.filter_count);
        self.node_resources[node].observe(usage);
        Ok(())
    }

    pub(super) fn retain_local_event(
        &mut self,
        node: usize,
        event_id: String,
        event: Event,
    ) -> Result<()> {
        if !self.nodes[node].local_events.contains_key(&event_id) {
            let bytes = event.try_as_json().map_err(pubsub_error)?.len();
            self.node_resources[node].current.local_event_bytes = self.node_resources[node]
                .current
                .local_event_bytes
                .saturating_add(usize_as_u64(bytes));
        }
        self.nodes[node].local_events.insert(event_id, event);
        self.observe_core_resource_state(node);
        Ok(())
    }

    pub(super) fn add_queued_resource_bytes(&mut self, node: usize, bytes: u64) {
        self.node_resources[node].add_queued_bytes(bytes);
    }

    pub(super) fn remove_queued_resource_bytes(&mut self, node: usize, bytes: u64) {
        self.node_resources[node].remove_queued_bytes(bytes);
    }

    pub(super) fn finalize_resource_metrics(&mut self) -> Result<()> {
        let quiescence_at_ms = self
            .nodes
            .iter()
            .map(|node| node.mesh.options().event_ttl_ms)
            .max()
            .unwrap_or(0)
            .saturating_add(self.scheduler.now_ms());
        for node in 0..self.nodes.len() {
            self.nodes[node].mesh.maintain(quiescence_at_ms);
            self.observe_core_resource_state(node);
            self.observe_subscription_resource_state(node)?;
        }
        let honest = self.config.attacker_count..self.config.node_count;
        let honest_peers = honest
            .clone()
            .filter(|node| self.topology.roles[*node] == NodeRole::Peer)
            .collect::<Vec<_>>();
        let honest_supernodes = honest
            .clone()
            .filter(|node| self.topology.roles[*node] == NodeRole::Supernode)
            .collect::<Vec<_>>();
        let attacker_adversarial_sent_bytes = (0..self.config.attacker_count)
            .map(|node| self.traffic[node].adversarial(TrafficScope::Sent).bytes)
            .fold(0, u64::saturating_add);
        let honest_adversarial_combined_bytes = honest
            .clone()
            .map(|node| self.traffic[node].adversarial(TrafficScope::Combined).bytes)
            .fold(0, u64::saturating_add);
        self.report.resource_usage = SimulationResourceReport {
            honest_all: summarize_cohort(honest, &self.traffic, &self.node_resources),
            honest_peers: summarize_cohort(honest_peers, &self.traffic, &self.node_resources),
            honest_supernodes: summarize_cohort(
                honest_supernodes,
                &self.traffic,
                &self.node_resources,
            ),
            attacker_adversarial_sent_bytes,
            honest_adversarial_combined_bytes,
            victim_bandwidth_amplification_basis_points: basis_points(
                honest_adversarial_combined_bytes,
                attacker_adversarial_sent_bytes,
            ),
            quiescence_at_ms,
        };
        Ok(())
    }

    pub(super) fn record_useful_payload(&mut self, node: usize, bytes: u64) {
        self.node_resources[node].useful_payload_bytes = self.node_resources[node]
            .useful_payload_bytes
            .saturating_add(bytes);
    }

    pub(super) fn record_cpu_work(&mut self, node: usize, update: impl FnOnce(&mut NodeCpuWork)) {
        update(&mut self.node_resources[node].work);
    }

    pub(super) fn record_avoided_signature_check(&mut self, node: usize) {
        self.node_resources[node].work.avoided_signature_checks = self.node_resources[node]
            .work
            .avoided_signature_checks
            .saturating_add(1);
    }
}

fn usize_as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn summarize_cohort(
    indices: impl IntoIterator<Item = usize>,
    traffic: &[NodeTrafficLedger],
    resources: &[NodeResourceLedger],
) -> ResourceCohortReport {
    let indices = indices.into_iter().collect::<Vec<_>>();
    let traffic_values = |scope| {
        indices
            .iter()
            .map(|index| traffic[*index].total(scope).bytes)
            .collect::<Vec<_>>()
    };
    let traffic_message_values = |scope| {
        indices
            .iter()
            .map(|index| traffic[*index].total(scope).messages)
            .collect::<Vec<_>>()
    };
    let work_values = |field: fn(NodeCpuWork) -> u64| {
        indices
            .iter()
            .map(|index| field(resources[*index].work))
            .collect::<Vec<_>>()
    };
    let codec_bytes = work_values(|work| {
        work.invwant_encode_bytes
            .saturating_add(work.invwant_decode_bytes)
            .saturating_add(work.fips_encode_bytes)
            .saturating_add(work.fips_decode_bytes)
    });
    ResourceCohortReport {
        sent_bytes: summarize_distribution(&traffic_values(TrafficScope::Sent)),
        received_bytes: summarize_distribution(&traffic_values(TrafficScope::Received)),
        combined_messages: summarize_distribution(&traffic_message_values(TrafficScope::Combined)),
        combined_bytes: summarize_distribution(&traffic_values(TrafficScope::Combined)),
        adversarial_combined_bytes: summarize_distribution(
            &indices
                .iter()
                .map(|index| traffic[*index].adversarial(TrafficScope::Combined).bytes)
                .collect::<Vec<_>>(),
        ),
        useful_payload_bytes: summarize_distribution(
            &indices
                .iter()
                .map(|index| resources[*index].useful_payload_bytes)
                .collect::<Vec<_>>(),
        ),
        cpu_work: CpuWorkDistribution {
            codec_bytes: summarize_distribution(&codec_bytes),
            signature_checks: summarize_distribution(&work_values(|work| work.signature_checks)),
            avoided_signature_checks: summarize_distribution(&work_values(|work| {
                work.avoided_signature_checks
            })),
            signature_checks_without_verified_paths: summarize_distribution(&work_values(|work| {
                work.signature_checks
                    .saturating_add(work.avoided_signature_checks)
            })),
            filter_queries: summarize_distribution(&work_values(|work| work.filter_queries)),
            filter_candidates: summarize_distribution(&work_values(|work| work.filter_candidates)),
            mesh_candidates: summarize_distribution(&work_values(|work| work.mesh_candidates)),
            graph_queries: summarize_distribution(&work_values(|work| work.graph_queries)),
            reputation_events_considered: summarize_distribution(&work_values(|work| {
                work.reputation_events_considered
            })),
            reputation_rebuild_entries: summarize_distribution(&work_values(|work| {
                work.reputation_rebuild_entries
            })),
        },
        peak_retained: summarize_retained_usage(&indices, resources, true),
        final_retained: summarize_retained_usage(&indices, resources, false),
    }
}

fn summarize_retained_usage(
    indices: &[usize],
    resources: &[NodeResourceLedger],
    peak: bool,
) -> RetainedUsageDistribution {
    let usages = |field: fn(NodeRetainedUsage) -> u64| {
        indices
            .iter()
            .map(|index| {
                field(if peak {
                    resources[*index].peak
                } else {
                    resources[*index].current
                })
            })
            .collect::<Vec<_>>()
    };
    RetainedUsageDistribution {
        exact_content_bytes: summarize_distribution(&resource_ledger_values(
            indices,
            resources,
            |ledger| {
                if peak {
                    ledger.peak_exact_content_bytes
                } else {
                    ledger.current.exact_content_bytes()
                }
            },
        )),
        protocol_content_bytes: summarize_distribution(&usages(|usage| {
            usage.protocol_content_bytes()
        })),
        state_entries: summarize_distribution(&resource_ledger_values(
            indices,
            resources,
            |ledger| {
                if peak {
                    ledger.peak_state_entries
                } else {
                    ledger.current.state_entries()
                }
            },
        )),
        cached_event_bytes: summarize_distribution(&usages(|usage| usage.cached_event_bytes)),
        subscription_state_bytes: summarize_distribution(&usages(|usage| {
            usage.subscription_state_bytes
        })),
        local_filter_bytes: summarize_distribution(&usages(|usage| usage.local_filter_bytes)),
        local_event_bytes: summarize_distribution(&usages(|usage| usage.local_event_bytes)),
        queued_wire_bytes: summarize_distribution(&usages(|usage| usage.queued_wire_bytes)),
    }
}

fn resource_ledger_values(
    indices: &[usize],
    resources: &[NodeResourceLedger],
    field: impl Fn(&NodeResourceLedger) -> u64,
) -> Vec<u64> {
    indices
        .iter()
        .map(|index| field(&resources[*index]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{TrafficDirection, TrafficProvenance};
    use crate::{PeerSelectionMode, SimulationConfig, run_simulation};

    #[test]
    fn cohort_summary_keeps_bandwidth_work_and_memory_per_node() {
        let mut traffic = vec![NodeTrafficLedger::default(); 2];
        traffic[0].record(
            TrafficDirection::Sent,
            TrafficProvenance::Adversarial,
            1,
            20,
        );
        traffic[0].record(
            TrafficDirection::Received,
            TrafficProvenance::Legitimate,
            1,
            10,
        );
        traffic[1].record(TrafficDirection::Sent, TrafficProvenance::Legitimate, 1, 40);
        let mut resources = vec![NodeResourceLedger::default(); 2];
        resources[0].work.signature_checks = 7;
        resources[1].work.signature_checks = 3;
        resources[0].work.avoided_signature_checks = 5;
        resources[1].work.avoided_signature_checks = 2;
        resources[0].observe(NodeRetainedUsage {
            cached_event_bytes: 100,
            queued_wire_bytes: 50,
            mesh_state_entries: 2,
            ..NodeRetainedUsage::default()
        });
        resources[0].remove_queued_bytes(50);

        let summary = summarize_cohort(0..2, &traffic, &resources);

        assert_eq!(summary.combined_bytes.total, 70);
        assert_eq!(summary.combined_messages.total, 3);
        assert_eq!(summary.adversarial_combined_bytes.total, 20);
        assert_eq!(summary.cpu_work.signature_checks.p95, 7);
        assert_eq!(summary.cpu_work.avoided_signature_checks.p95, 5);
        assert_eq!(
            summary.cpu_work.signature_checks_without_verified_paths.p95,
            12
        );
        assert_eq!(summary.peak_retained.exact_content_bytes.max, 150);
        assert_eq!(summary.peak_retained.protocol_content_bytes.max, 150);
        assert_eq!(summary.final_retained.exact_content_bytes.max, 100);
        assert_eq!(summary.final_retained.protocol_content_bytes.max, 100);
        assert_eq!(summary.peak_retained.state_entries.max, 2);
    }

    #[test]
    fn adversarial_run_reports_honest_tail_resources_and_quiescent_queues() {
        let config = SimulationConfig {
            node_count: 80,
            attacker_count: 16,
            fake_inventories_per_attack_link: 2,
            signed_spam_rounds: 2,
            loss_basis_points: 100,
            churn_basis_points: 100,
            supernode_count: 4,
            false_supernode_count: 2,
            ..SimulationConfig::default()
        };

        let report = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();
        let resources = report.resource_usage;

        assert_eq!(resources.honest_all.combined_bytes.count, 64);
        assert_eq!(resources.honest_peers.combined_bytes.count, 64);
        assert_eq!(resources.honest_supernodes.combined_bytes.count, 0);
        assert_eq!(
            resources.honest_all.combined_bytes.total,
            resources
                .honest_all
                .sent_bytes
                .total
                .saturating_add(resources.honest_all.received_bytes.total)
        );
        assert_eq!(
            resources.honest_all.adversarial_combined_bytes.total,
            resources.honest_adversarial_combined_bytes
        );
        assert!(resources.honest_peers.cpu_work.codec_bytes.p95 > 0);
        assert!(resources.honest_peers.cpu_work.signature_checks.p95 > 0);
        assert!(resources.honest_peers.cpu_work.filter_candidates.p95 > 0);
        assert!(resources.honest_peers.cpu_work.graph_queries.p95 > 0);
        assert!(resources.honest_peers.peak_retained.exact_content_bytes.p95 > 0);
        assert!(resources.honest_peers.peak_retained.state_entries.p95 > 0);
        assert!(
            resources
                .honest_peers
                .final_retained
                .protocol_content_bytes
                .p95
                > 0
        );
        assert!(resources.quiescence_at_ms >= report.virtual_duration_ms);
        assert_eq!(resources.honest_all.final_retained.queued_wire_bytes.max, 0);
        assert_eq!(
            resources.honest_peers.final_retained.cached_event_bytes.max,
            0
        );
        assert!(resources.honest_peers.useful_payload_bytes.total > 0);
        assert!(resources.attacker_adversarial_sent_bytes > 0);
        assert!(resources.victim_bandwidth_amplification_basis_points > 0);
    }
}
