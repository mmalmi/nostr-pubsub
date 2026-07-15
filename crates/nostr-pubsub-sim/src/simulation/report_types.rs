use std::collections::BTreeMap;

use crate::metrics::NodeTrafficLedger;
use crate::topology::{NodeRole, SupernodeDiscoveryStrategy, TopologyStrategy};

use super::{
    DirectedServiceLink, PeerSelectionMode, SimulationConfig, SimulationResourceReport,
    VerifiedDeliveryRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationReport {
    pub config: SimulationConfig,
    pub mode: PeerSelectionMode,
    pub topology: TopologyStrategy,
    pub discovery: SupernodeDiscoveryStrategy,
    pub node_count: usize,
    pub attacker_count: usize,
    pub honest_node_count: usize,
    pub supernode_count: usize,
    /// Ground-truth role of each simulated node, indexed by node identifier.
    pub node_roles: Vec<NodeRole>,
    pub topology_edges: usize,
    pub max_node_degree: usize,
    pub legitimate_events: usize,
    pub spam_events: usize,
    pub expected_legitimate_deliveries: usize,
    pub expected_signed_spam_deliveries: usize,
    pub expected_signed_spam_deliveries_by_class: BTreeMap<String, usize>,
    pub expected_signed_spam_deliveries_by_identity: BTreeMap<String, usize>,
    pub expected_machine_admitted_spam_deliveries_by_identity: BTreeMap<String, usize>,
    /// Active ordinary-peer links on which production subscription routing
    /// considered a signed spam event.
    pub spam_filter_peer_link_opportunities: usize,
    pub spam_filter_peer_link_opportunities_by_class: BTreeMap<String, usize>,
    /// Those opportunities for which the production subscription store
    /// suppressed inventory delivery.
    pub spam_filter_suppressed_peer_links: usize,
    pub spam_filter_suppressed_peer_links_by_class: BTreeMap<String, usize>,
    pub spam_filter_suppression_basis_points_by_class: BTreeMap<String, u32>,
    pub delivered_legitimate: usize,
    pub local_legitimate_deliveries: usize,
    pub delivery_basis_points: u32,
    pub worst_cohort_delivery_basis_points: u32,
    pub cohort_delivery_basis_points: BTreeMap<String, u32>,
    pub latency_sample_count: usize,
    pub latency_p50_ms: u64,
    pub latency_p95_ms: u64,
    pub latency_p99_ms: u64,
    pub max_delivered_latency_ms: u64,
    /// Remote interested deliveries with reconstructable dissemination paths.
    pub delivery_path_samples: usize,
    pub multihop_interested_deliveries: usize,
    pub multihop_interested_delivery_basis_points: u32,
    pub delivery_path_hops_p50: u64,
    pub delivery_path_hops_p95: u64,
    pub delivery_path_hops_p99: u64,
    pub delivery_path_hops_max: u64,
    pub undelivered_legitimate: usize,
    pub spam_delivered: usize,
    pub signed_spam_deliveries_by_class: BTreeMap<String, usize>,
    pub signed_spam_deliveries_by_identity: BTreeMap<String, usize>,
    pub machine_admitted_spam_deliveries_by_identity: BTreeMap<String, usize>,
    pub signed_spam_delivery_basis_points: u32,
    pub signed_spam_delivery_basis_points_by_class: BTreeMap<String, u32>,
    pub signed_spam_suppression_basis_points_by_identity: BTreeMap<String, u32>,
    pub machine_admitted_spam_suppression_basis_points_by_identity: BTreeMap<String, u32>,
    pub unknown_discovery_adverts_delivered: usize,
    pub spam_dropped_by_machine_policy: usize,
    pub spam_dropped_by_application_policy: usize,
    pub spam_suppression_basis_points: u32,
    pub uninterested_deliveries: usize,
    pub uninterested_legitimate_deliveries: usize,
    pub uninterested_spam_deliveries: usize,
    pub filter_suppression_basis_points: u32,
    pub processed_actions: usize,
    pub processed_messages: usize,
    pub inventory_messages: usize,
    pub want_messages: usize,
    pub frame_messages: usize,
    pub data_plane_wire_bytes: u64,
    pub legitimate_protocol_bytes: u64,
    pub adversarial_protocol_bytes: u64,
    pub legitimate_protocol_byte_share_basis_points: u32,
    pub protocol_messages_per_interested_delivery_milli: u64,
    pub dropped_packets: usize,
    pub dropped_at_attackers: usize,
    /// Retry inventories actually sent through the production replay path.
    pub retry_inventories: usize,
    /// Disrupted legitimate transfers that were eventually delivered by any path.
    pub eventual_disrupted_transfer_recoveries: usize,
    pub disrupted_legitimate_transfers: usize,
    pub eventual_disrupted_transfer_recovery_basis_points: u32,
    pub max_queue_depth: usize,
    pub virtual_duration_ms: u64,
    pub injected_attack_inventories: usize,
    pub rejected_malformed_messages: usize,
    pub unauthorized_source_drops: usize,
    pub machine_ingress_drops: usize,
    /// Legitimate-provenance packets blocked when carried by an honest peer.
    pub honest_source_legitimate_machine_ingress_drops: usize,
    /// Legitimate-reference packets blocked from either a static attacker or
    /// a peer that deliberately defected after earning machine trust.
    pub adversarial_source_legitimate_reference_machine_ingress_drops: usize,
    /// Adversarial-provenance packets blocked regardless of carrier role.
    pub adversarial_machine_ingress_drops: usize,
    pub machine_ratings_published: usize,
    pub machine_ratings_received: usize,
    /// Structurally valid ratings retained by `PeerReputation`.
    pub machine_ratings_ingested: usize,
    pub machine_positive_service_endorsements_published: usize,
    pub machine_positive_service_admissions: usize,
    pub machine_positive_endorsement_state_entries: usize,
    pub machine_rating_protocol_messages: usize,
    pub machine_rating_protocol_bytes: u64,
    pub machine_reputation_retained_ratings: usize,
    pub machine_reputation_retained_raters: usize,
    pub machine_reputation_trusted_roots: usize,
    pub poisoned_machine_ratings_published: usize,
    pub poisoned_machine_ratings_received: usize,
    pub poisoned_machine_ratings_ingested: usize,
    pub poisoned_machine_ratings_rejected: usize,
    /// Historically legitimate frames rejected after their author became the
    /// explicit post-service defector, regardless of which peer relayed them.
    pub adversarial_author_legitimate_reference_policy_drops: usize,
    pub admitted_rater_poison_published: usize,
    pub admitted_rater_poison_received: usize,
    pub admitted_rater_poison_ingested: usize,
    pub admitted_rater_poison_rejected: usize,
    pub admitted_rater_poison_removals: usize,
    pub admitted_rater_poison_service_admitted_rater: usize,
    pub admitted_rater_service_credits: usize,
    pub admitted_rater_service_bytes: u64,
    pub admitted_rater_poison_target_unknown_before: usize,
    pub admitted_rater_poison_target_received: usize,
    pub admitted_rater_poison_target_ingested: usize,
    pub admitted_rater_poison_target_removals: usize,
    pub admitted_rater_poison_target_recoveries: usize,
    pub admitted_rater_misbehavior_frames: usize,
    pub admitted_rater_revocations: usize,
    pub post_revocation_rating_published: usize,
    pub post_revocation_rating_target_policy_drops: usize,
    pub post_revocation_rating_target_received: usize,
    pub post_revocation_rating_target_ingested: usize,
    pub post_revocation_rating_influence: usize,
    pub unconnected_rating_pressure_published: usize,
    pub unconnected_rating_pressure_target_received: usize,
    pub unconnected_rating_pressure_target_ingested: usize,
    pub unconnected_rating_pressure_target_rejected: usize,
    pub unconnected_rating_pressure_distinct_raters: usize,
    pub unconnected_rating_pressure_retained_rating_delta: usize,
    pub unconnected_rating_pressure_retained_rater_delta: usize,
    pub unconnected_rating_pressure_rebuild_entry_delta: u64,
    pub unconnected_rating_pressure_anchor_positive_before: usize,
    pub unconnected_rating_pressure_anchor_stable_evaluations: usize,
    pub unconnected_rating_pressure_anchor_projection_changes: usize,
    pub machine_transported_transitions: usize,
    pub machine_transported_positive_admissions: usize,
    pub machine_transported_removals: usize,
    pub machine_lifecycle_ratings_published: usize,
    pub machine_lifecycle_admissions: usize,
    pub machine_lifecycle_removals: usize,
    pub machine_lifecycle_readmissions: usize,
    pub machine_reversible_lifecycles: usize,
    pub machine_positive_admissions: usize,
    pub machine_removals: usize,
    pub machine_quiet_blackhole_removals: usize,
    pub machine_poisoning_removals: usize,
    pub machine_false_positive_removals: usize,
    pub machine_removal_latency_p95_ms: u64,
    pub forged_machine_ratings_published: usize,
    pub forged_machine_ratings_received: usize,
    pub forged_machine_ratings_evaluated: usize,
    pub forged_machine_ratings_ingested: usize,
    pub forged_machine_ratings_rejected: usize,
    pub legitimate_policy_drops: usize,
    pub legitimate_application_policy_drops: usize,
    pub machine_trust_edges: usize,
    pub subscription_messages: usize,
    pub control_plane_wire_bytes: u64,
    pub subscription_retries: usize,
    pub subscription_retry_recoveries: usize,
    pub subscription_rejections: usize,
    pub subscription_evictions: usize,
    pub subscription_close_reopen_successes: usize,
    /// Inv/WANT actions sent while the local policy still classified the peer as unknown.
    pub unknown_candidate_sends: usize,
    /// Scheduled link-outage episodes, including forced supernode outages.
    pub churned_links: usize,
    pub discovery_links: usize,
    /// Selected links classified against hidden high-capacity ground truth.
    pub selected_high_capacity_links: usize,
    pub selected_adversarial_candidate_links: usize,
    pub high_capacity_selection_precision_basis_points: u32,
    pub high_capacity_selection_coverage_basis_points: u32,
    pub peers_without_high_capacity_selection: usize,
    pub rediscovery_sweeps: usize,
    pub rediscovery_candidate_attempts: usize,
    pub rediscovery_links_removed: usize,
    pub rediscovery_links_added: usize,
    pub rediscovery_adversarial_links_removed: usize,
    pub rediscovery_unavailable_links_removed: usize,
    pub rediscovery_high_capacity_links_added: usize,
    pub rediscovery_state_entries: usize,
    pub rediscovery_subscription_refresh_nodes: usize,
    pub rediscovery_subscription_refresh_targets: usize,
    pub rediscovery_subscription_messages: usize,
    pub rediscovery_control_plane_wire_bytes: u64,
    pub supernode_max_service_bytes: u64,
    pub supernode_mean_service_bytes: u64,
    pub supernode_load_gini_basis_points: u32,
    pub total_protocol_bytes: u64,
    pub sent_link_protocol_bytes: u64,
    pub sent_role_protocol_bytes: u64,
    pub protocol_bytes_per_interested_delivery: u64,
    pub resource_usage: SimulationResourceReport,
    /// Attempted and received Inv/WANT and FIPS control traffic per directed link.
    pub protocol_service_by_link: BTreeMap<DirectedServiceLink, NodeTrafficLedger>,
    /// Inv/WANT and FIPS control service aggregated by the carrier's node role.
    pub protocol_service_by_role: BTreeMap<NodeRole, NodeTrafficLedger>,
    /// Successful interested application deliveries credited to their final directed hop.
    pub interested_delivery_credit_by_link: BTreeMap<DirectedServiceLink, usize>,
    /// Final-hop interested delivery credits aggregated by the carrier's role.
    pub interested_delivery_credit_by_source_role: BTreeMap<NodeRole, usize>,
    /// Exact useful application payload bytes credited to each final directed hop.
    pub interested_delivery_bytes_by_link: BTreeMap<DirectedServiceLink, u64>,
    /// Final-hop useful payload bytes aggregated by the carrier's role.
    pub interested_delivery_bytes_by_source_role: BTreeMap<NodeRole, u64>,
    /// Interested final-hop deliveries carried by a hidden-role supernode for
    /// an event whose original publisher was another node.
    pub supernode_third_party_interested_delivery_credits: usize,
    /// Useful payload bytes for those third-party supernode deliveries.
    pub supernode_third_party_interested_delivery_bytes: u64,
    /// First accepted legitimate frame deliveries on every directed transport hop.
    pub verified_delivery_credit_by_link: BTreeMap<DirectedServiceLink, usize>,
    /// Exact application payload bytes carried by first accepted legitimate frames.
    pub verified_delivery_bytes_by_link: BTreeMap<DirectedServiceLink, u64>,
    /// Verified-hop payload bytes aggregated by the sending node's role.
    pub verified_delivery_bytes_by_source_role: BTreeMap<NodeRole, u64>,
    /// Per-event edges for first accepted legitimate frames that served an
    /// interested receiver or were forwarded onward.
    pub verified_delivery_records: Vec<VerifiedDeliveryRecord>,
}

impl SimulationReport {
    /// Whether independently accumulated protocol-byte ledgers agree exactly.
    #[must_use]
    pub fn protocol_accounting_is_conserved(&self) -> bool {
        self.data_plane_wire_bytes
            .saturating_add(self.control_plane_wire_bytes)
            == self.total_protocol_bytes
            && self
                .legitimate_protocol_bytes
                .saturating_add(self.adversarial_protocol_bytes)
                == self.total_protocol_bytes
            && self.sent_link_protocol_bytes == self.total_protocol_bytes
            && self.sent_role_protocol_bytes == self.total_protocol_bytes
    }

    /// Whether every machine-ingress drop has exactly one ground-truth class.
    #[must_use]
    pub fn machine_ingress_accounting_is_conserved(&self) -> bool {
        self.honest_source_legitimate_machine_ingress_drops
            .saturating_add(self.adversarial_source_legitimate_reference_machine_ingress_drops)
            .saturating_add(self.adversarial_machine_ingress_drops)
            == self.machine_ingress_drops
    }
}
