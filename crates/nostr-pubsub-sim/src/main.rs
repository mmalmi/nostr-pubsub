use nostr_pubsub_sim::{
    NodeRole, PeerSelectionMode, SimulationConfig, SimulationReport, SupernodeDiscoveryStrategy,
    TopologyStrategy, TrafficDirection, TrafficProvenance, run_simulation,
};

const COHORT_AUTHOR_FEED: &str = "author-feed";
const COHORT_HASHTAG_TOPIC: &str = "hashtag-topic";
const COHORT_HASHTREE_UPDATE: &str = "hashtree-update";
const COHORT_TARGETED_APPROVAL_RATING: &str = "targeted-approval-rating";
const COHORT_IRIS_DRIVE_BROAD_ROOT: &str = "iris-drive-broad-root";
const COHORT_FIPS_ADVERT: &str = "fips-advert";
const COHORT_FIPS_PAID_OFFER: &str = "fips-paid-offer";
const COHORT_GIT_REPO_ANNOUNCEMENT: &str = "git-repo-announcement";

const CSV_COLUMNS: &[&str] = &[
    "simulator_version",
    "topology",
    "discovery",
    "mode",
    "nodes",
    "attackers",
    "honest_nodes",
    "fanout",
    "unknown_peer_reserve",
    "max_hops",
    "fake_inventories_per_attack_link",
    "signed_spam_rounds",
    "action_budget",
    "supernode_fanout",
    "loss_bps",
    "churn_bps",
    "retry_ms",
    "max_retries",
    "seed",
    "configured_supernodes",
    "configured_false_supernodes",
    "supernode_links_per_peer",
    "supernodes",
    "edges",
    "max_degree",
    "legitimate_events",
    "spam_events",
    "expected_legitimate_deliveries",
    "delivered_legitimate",
    "local_legitimate_deliveries",
    "delivery_bps",
    "cohort_author_feed_bps",
    "cohort_hashtag_topic_bps",
    "cohort_hashtree_update_bps",
    "cohort_targeted_approval_rating_bps",
    "cohort_iris_drive_broad_root_bps",
    "cohort_fips_advert_bps",
    "cohort_fips_paid_offer_bps",
    "cohort_git_repo_announcement_bps",
    "worst_cohort_bps",
    "undelivered_legitimate",
    "expected_signed_spam_deliveries",
    "signed_spam_delivered",
    "signed_spam_delivery_bps",
    "expected_persistent_identity_spam_deliveries",
    "persistent_identity_spam_delivered",
    "persistent_identity_spam_suppression_bps",
    "expected_fresh_sybil_spam_deliveries",
    "fresh_sybil_spam_delivered",
    "fresh_sybil_spam_suppression_bps",
    "expected_persistent_machine_admitted_spam_deliveries",
    "persistent_machine_admitted_spam_delivered",
    "persistent_machine_admitted_spam_suppression_bps",
    "expected_fresh_sybil_machine_admitted_spam_deliveries",
    "fresh_sybil_machine_admitted_spam_delivered",
    "fresh_sybil_machine_admitted_spam_suppression_bps",
    "spam_author_feed_bps",
    "spam_hashtag_topic_bps",
    "spam_hashtree_update_bps",
    "spam_targeted_approval_rating_bps",
    "spam_iris_drive_broad_root_bps",
    "spam_fips_advert_bps",
    "spam_fips_paid_offer_bps",
    "spam_git_repo_announcement_bps",
    "signed_spam_suppression_bps",
    "spam_filter_peer_link_opportunities",
    "spam_filter_suppressed_peer_links",
    "filter_suppression_bps",
    "filter_author_feed_suppression_bps",
    "filter_hashtag_topic_suppression_bps",
    "filter_hashtree_update_suppression_bps",
    "filter_targeted_approval_rating_suppression_bps",
    "filter_iris_drive_broad_root_suppression_bps",
    "filter_fips_advert_suppression_bps",
    "filter_fips_paid_offer_suppression_bps",
    "filter_git_repo_announcement_suppression_bps",
    "unknown_discovery_adverts",
    "spam_graph_drops",
    "spam_machine_drops",
    "spam_application_drops",
    "legitimate_policy_drops",
    "legitimate_application_policy_drops",
    "machine_ingress_drops",
    "honest_source_legitimate_machine_ingress_drops",
    "attacker_source_legitimate_reference_machine_ingress_drops",
    "adversarial_machine_ingress_drops",
    "machine_ingress_accounting_conserved",
    "uninterested_deliveries",
    "uninterested_legitimate_deliveries",
    "uninterested_spam_deliveries",
    "injected_attack_inventories",
    "rejected_malformed_messages",
    "unauthorized_source_drops",
    "latency_samples",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_delivered_latency_ms",
    "processed_actions",
    "processed_messages",
    "inventory",
    "want",
    "frame",
    "data_plane_wire_bytes",
    "legitimate_protocol_bytes",
    "adversarial_protocol_bytes",
    "legitimate_protocol_byte_share_bps",
    "protocol_messages_per_interested_delivery_milli",
    "dropped_packets",
    "dropped_at_attackers",
    "retries",
    "eventual_disrupted_transfer_recoveries",
    "disrupted_legitimate_transfers",
    "eventual_disrupted_transfer_recovery_bps",
    "max_queue_depth",
    "machine_ratings_published",
    "machine_ratings_received",
    "machine_ratings_ingested",
    "poisoned_machine_ratings_published",
    "poisoned_machine_ratings_received",
    "poisoned_machine_ratings_ingested",
    "poisoned_machine_ratings_rejected",
    "machine_transported_transitions",
    "machine_transported_positive_admissions",
    "machine_transported_removals",
    "machine_lifecycle_ratings_published",
    "machine_lifecycle_admissions",
    "machine_lifecycle_removals",
    "machine_lifecycle_readmissions",
    "machine_reversible_lifecycles",
    "machine_positive_admissions",
    "machine_removals",
    "machine_quiet_blackhole_removals",
    "machine_poisoning_removals",
    "machine_false_positive_removals",
    "machine_removal_p95_ms",
    "forged_ratings_published",
    "forged_ratings_received",
    "forged_ratings_evaluated",
    "forged_ratings_ingested",
    "forged_ratings_rejected",
    "human_signed_graph_updates_ingested",
    "human_lifecycle_successes",
    "human_lifecycle_checks",
    "human_follow_admissions",
    "human_follow_removals",
    "human_stale_update_rejections",
    "human_follow_readmissions",
    "human_mute_removals",
    "human_trust_edges",
    "machine_trust_edges",
    "human_machine_trust_overlap_edges",
    "subscription_messages",
    "control_plane_wire_bytes",
    "subscription_retries",
    "subscription_retry_recoveries",
    "subscription_rejections",
    "subscription_evictions",
    "subscription_reopens",
    "unknown_candidate_sends",
    "churned_links",
    "discovery_links",
    "honest_supernode_links",
    "false_supernode_links",
    "discovery_precision_bps",
    "honest_supernode_coverage_bps",
    "false_only_supernode_peers",
    "supernode_max_bytes",
    "supernode_mean_bytes",
    "load_gini_bps",
    "total_protocol_bytes",
    "sent_link_protocol_bytes",
    "sent_role_protocol_bytes",
    "protocol_accounting_conserved",
    "protocol_bytes_per_interested_delivery",
    "interested_delivery_credits",
    "peer_interested_delivery_credits",
    "supernode_interested_delivery_credits",
    "attacker_interested_delivery_credits",
    "peer_sent_legitimate_bytes",
    "peer_sent_adversarial_bytes",
    "peer_received_legitimate_bytes",
    "peer_received_adversarial_bytes",
    "supernode_sent_legitimate_bytes",
    "supernode_sent_adversarial_bytes",
    "supernode_received_legitimate_bytes",
    "supernode_received_adversarial_bytes",
    "attacker_sent_legitimate_bytes",
    "attacker_sent_adversarial_bytes",
    "attacker_received_legitimate_bytes",
    "attacker_received_adversarial_bytes",
    "virtual_ms",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (config, topologies, modes) = parse_config(std::env::args().skip(1))?;
    println!("{}", csv_header());
    for topology in topologies {
        let mut scenario = config.clone();
        scenario.topology = topology;
        for mode in modes.iter().copied() {
            let report = run_simulation(scenario.clone(), mode)?;
            println!("{}", report_csv(&report));
        }
    }
    Ok(())
}

fn report_csv(report: &SimulationReport) -> String {
    let mut values = identity_config_values(report);
    values.extend(delivery_values(report));
    values.extend(transport_values(report));
    values.extend(reputation_values(report));
    values.extend(subscription_topology_values(report));
    values.extend(role_service_values(report));
    values.push(report.virtual_duration_ms.to_string());
    assert_eq!(CSV_COLUMNS.len(), values.len(), "CSV schema/row mismatch");
    values.join(",")
}

fn identity_config_values(report: &SimulationReport) -> Vec<String> {
    vec![
        env!("CARGO_PKG_VERSION").to_string(),
        topology_name(report.topology).to_string(),
        discovery_name(report.discovery).to_string(),
        report.mode.as_str().to_string(),
        report.node_count.to_string(),
        report.attacker_count.to_string(),
        report.honest_node_count.to_string(),
        report.config.fanout.to_string(),
        report.config.unknown_peer_reserve.to_string(),
        report.config.max_hops.to_string(),
        report.config.fake_inventories_per_attack_link.to_string(),
        report.config.signed_spam_rounds.to_string(),
        report.config.max_processed_actions.to_string(),
        report.config.supernode_fanout.to_string(),
        report.config.loss_basis_points.to_string(),
        report.config.churn_basis_points.to_string(),
        report.config.retry_delay_ms.to_string(),
        report.config.max_retries.to_string(),
        report.config.seed.to_string(),
        report.config.supernode_count.to_string(),
        report.config.false_supernode_count.to_string(),
        report.config.supernode_links_per_peer.to_string(),
        report.supernode_count.to_string(),
        report.topology_edges.to_string(),
        report.max_node_degree.to_string(),
    ]
}

fn delivery_values(report: &SimulationReport) -> Vec<String> {
    vec![
        report.legitimate_events.to_string(),
        report.spam_events.to_string(),
        report.expected_legitimate_deliveries.to_string(),
        report.delivered_legitimate.to_string(),
        report.local_legitimate_deliveries.to_string(),
        report.delivery_basis_points.to_string(),
        cohort_delivery_bps(report, COHORT_AUTHOR_FEED).to_string(),
        cohort_delivery_bps(report, COHORT_HASHTAG_TOPIC).to_string(),
        cohort_delivery_bps(report, COHORT_HASHTREE_UPDATE).to_string(),
        cohort_delivery_bps(report, COHORT_TARGETED_APPROVAL_RATING).to_string(),
        cohort_delivery_bps(report, COHORT_IRIS_DRIVE_BROAD_ROOT).to_string(),
        cohort_delivery_bps(report, COHORT_FIPS_ADVERT).to_string(),
        cohort_delivery_bps(report, COHORT_FIPS_PAID_OFFER).to_string(),
        cohort_delivery_bps(report, COHORT_GIT_REPO_ANNOUNCEMENT).to_string(),
        report.worst_cohort_delivery_basis_points.to_string(),
        report.undelivered_legitimate.to_string(),
        report.expected_signed_spam_deliveries.to_string(),
        report.spam_delivered.to_string(),
        report.signed_spam_delivery_basis_points.to_string(),
        identity_expected(report, "persistent").to_string(),
        identity_delivered(report, "persistent").to_string(),
        identity_suppression_bps(report, "persistent").to_string(),
        identity_expected(report, "fresh-sybil").to_string(),
        identity_delivered(report, "fresh-sybil").to_string(),
        identity_suppression_bps(report, "fresh-sybil").to_string(),
        machine_identity_expected(report, "persistent").to_string(),
        machine_identity_delivered(report, "persistent").to_string(),
        machine_identity_suppression_bps(report, "persistent").to_string(),
        machine_identity_expected(report, "fresh-sybil").to_string(),
        machine_identity_delivered(report, "fresh-sybil").to_string(),
        machine_identity_suppression_bps(report, "fresh-sybil").to_string(),
        spam_delivery_bps(report, COHORT_AUTHOR_FEED).to_string(),
        spam_delivery_bps(report, COHORT_HASHTAG_TOPIC).to_string(),
        spam_delivery_bps(report, COHORT_HASHTREE_UPDATE).to_string(),
        spam_delivery_bps(report, COHORT_TARGETED_APPROVAL_RATING).to_string(),
        spam_delivery_bps(report, COHORT_IRIS_DRIVE_BROAD_ROOT).to_string(),
        spam_delivery_bps(report, COHORT_FIPS_ADVERT).to_string(),
        spam_delivery_bps(report, COHORT_FIPS_PAID_OFFER).to_string(),
        spam_delivery_bps(report, COHORT_GIT_REPO_ANNOUNCEMENT).to_string(),
        report.spam_suppression_basis_points.to_string(),
        report.spam_filter_peer_link_opportunities.to_string(),
        report.spam_filter_suppressed_peer_links.to_string(),
        report.filter_suppression_basis_points.to_string(),
        filter_suppression_bps(report, COHORT_AUTHOR_FEED).to_string(),
        filter_suppression_bps(report, COHORT_HASHTAG_TOPIC).to_string(),
        filter_suppression_bps(report, COHORT_HASHTREE_UPDATE).to_string(),
        filter_suppression_bps(report, COHORT_TARGETED_APPROVAL_RATING).to_string(),
        filter_suppression_bps(report, COHORT_IRIS_DRIVE_BROAD_ROOT).to_string(),
        filter_suppression_bps(report, COHORT_FIPS_ADVERT).to_string(),
        filter_suppression_bps(report, COHORT_FIPS_PAID_OFFER).to_string(),
        filter_suppression_bps(report, COHORT_GIT_REPO_ANNOUNCEMENT).to_string(),
        report.unknown_discovery_adverts_delivered.to_string(),
        report.spam_dropped_by_social_graph.to_string(),
        report.spam_dropped_by_machine_policy.to_string(),
        report.spam_dropped_by_application_policy.to_string(),
        report.legitimate_policy_drops.to_string(),
        report.legitimate_application_policy_drops.to_string(),
        report.machine_ingress_drops.to_string(),
        report
            .honest_source_legitimate_machine_ingress_drops
            .to_string(),
        report
            .attacker_source_legitimate_reference_machine_ingress_drops
            .to_string(),
        report.adversarial_machine_ingress_drops.to_string(),
        report.machine_ingress_accounting_is_conserved().to_string(),
        report.uninterested_deliveries.to_string(),
        report.uninterested_legitimate_deliveries.to_string(),
        report.uninterested_spam_deliveries.to_string(),
        report.injected_attack_inventories.to_string(),
        report.rejected_malformed_messages.to_string(),
        report.unauthorized_source_drops.to_string(),
        report.latency_sample_count.to_string(),
        report.latency_p50_ms.to_string(),
        report.latency_p95_ms.to_string(),
        report.latency_p99_ms.to_string(),
        report.max_delivered_latency_ms.to_string(),
    ]
}

fn transport_values(report: &SimulationReport) -> Vec<String> {
    vec![
        report.processed_actions.to_string(),
        report.processed_messages.to_string(),
        report.inventory_messages.to_string(),
        report.want_messages.to_string(),
        report.frame_messages.to_string(),
        report.data_plane_wire_bytes.to_string(),
        report.legitimate_protocol_bytes.to_string(),
        report.adversarial_protocol_bytes.to_string(),
        report
            .legitimate_protocol_byte_share_basis_points
            .to_string(),
        report
            .protocol_messages_per_interested_delivery_milli
            .to_string(),
        report.dropped_packets.to_string(),
        report.dropped_at_attackers.to_string(),
        report.retry_inventories.to_string(),
        report.eventual_disrupted_transfer_recoveries.to_string(),
        report.disrupted_legitimate_transfers.to_string(),
        report
            .eventual_disrupted_transfer_recovery_basis_points
            .to_string(),
        report.max_queue_depth.to_string(),
    ]
}

fn reputation_values(report: &SimulationReport) -> Vec<String> {
    vec![
        report.machine_ratings_published.to_string(),
        report.machine_ratings_received.to_string(),
        report.machine_ratings_ingested.to_string(),
        report.poisoned_machine_ratings_published.to_string(),
        report.poisoned_machine_ratings_received.to_string(),
        report.poisoned_machine_ratings_ingested.to_string(),
        report.poisoned_machine_ratings_rejected.to_string(),
        report.machine_transported_transitions.to_string(),
        report.machine_transported_positive_admissions.to_string(),
        report.machine_transported_removals.to_string(),
        report.machine_lifecycle_ratings_published.to_string(),
        report.machine_lifecycle_admissions.to_string(),
        report.machine_lifecycle_removals.to_string(),
        report.machine_lifecycle_readmissions.to_string(),
        report.machine_reversible_lifecycles.to_string(),
        report.machine_positive_admissions.to_string(),
        report.machine_removals.to_string(),
        report.machine_quiet_blackhole_removals.to_string(),
        report.machine_poisoning_removals.to_string(),
        report.machine_false_positive_removals.to_string(),
        report.machine_removal_latency_p95_ms.to_string(),
        report.forged_machine_ratings_published.to_string(),
        report.forged_machine_ratings_received.to_string(),
        report.forged_machine_ratings_evaluated.to_string(),
        report.forged_machine_ratings_ingested.to_string(),
        report.forged_machine_ratings_rejected.to_string(),
        report.human_signed_graph_updates_ingested.to_string(),
        report.human_lifecycle_successes.to_string(),
        report.human_lifecycle_checks.to_string(),
        report.human_follow_admissions.to_string(),
        report.human_follow_removals.to_string(),
        report.human_stale_update_rejections.to_string(),
        report.human_follow_readmissions.to_string(),
        report.human_mute_removals.to_string(),
        report.human_trust_edges.to_string(),
        report.machine_trust_edges.to_string(),
        report.human_machine_trust_overlap_edges.to_string(),
    ]
}

fn subscription_topology_values(report: &SimulationReport) -> Vec<String> {
    vec![
        report.subscription_messages.to_string(),
        report.control_plane_wire_bytes.to_string(),
        report.subscription_retries.to_string(),
        report.subscription_retry_recoveries.to_string(),
        report.subscription_rejections.to_string(),
        report.subscription_evictions.to_string(),
        report.subscription_close_reopen_successes.to_string(),
        report.unknown_candidate_sends.to_string(),
        report.churned_links.to_string(),
        report.discovery_links.to_string(),
        report.honest_supernode_links.to_string(),
        report.false_supernode_links.to_string(),
        report
            .supernode_discovery_precision_basis_points
            .to_string(),
        report.honest_supernode_coverage_basis_points.to_string(),
        report.false_only_supernode_peers.to_string(),
        report.supernode_max_service_bytes.to_string(),
        report.supernode_mean_service_bytes.to_string(),
        report.supernode_load_gini_basis_points.to_string(),
        report.total_protocol_bytes.to_string(),
        report.sent_link_protocol_bytes.to_string(),
        report.sent_role_protocol_bytes.to_string(),
        report.protocol_accounting_is_conserved().to_string(),
        report.protocol_bytes_per_interested_delivery.to_string(),
        report
            .interested_delivery_credit_by_link
            .values()
            .copied()
            .fold(0, usize::saturating_add)
            .to_string(),
        interested_delivery_credits(report, NodeRole::Peer).to_string(),
        interested_delivery_credits(report, NodeRole::Supernode).to_string(),
        interested_delivery_credits(report, NodeRole::Attacker).to_string(),
    ]
}

fn role_service_values(report: &SimulationReport) -> Vec<String> {
    [NodeRole::Peer, NodeRole::Supernode, NodeRole::Attacker]
        .into_iter()
        .flat_map(|role| {
            [
                role_service_bytes(
                    report,
                    role,
                    TrafficDirection::Sent,
                    TrafficProvenance::Legitimate,
                ),
                role_service_bytes(
                    report,
                    role,
                    TrafficDirection::Sent,
                    TrafficProvenance::Adversarial,
                ),
                role_service_bytes(
                    report,
                    role,
                    TrafficDirection::Received,
                    TrafficProvenance::Legitimate,
                ),
                role_service_bytes(
                    report,
                    role,
                    TrafficDirection::Received,
                    TrafficProvenance::Adversarial,
                ),
            ]
            .map(|bytes| bytes.to_string())
        })
        .collect()
}

fn interested_delivery_credits(report: &SimulationReport, role: NodeRole) -> usize {
    report
        .interested_delivery_credit_by_source_role
        .get(&role)
        .copied()
        .unwrap_or(0)
}

fn role_service_bytes(
    report: &SimulationReport,
    role: NodeRole,
    direction: TrafficDirection,
    provenance: TrafficProvenance,
) -> u64 {
    report
        .protocol_service_by_role
        .get(&role)
        .map_or(0, |ledger| ledger.counter(direction, provenance).bytes)
}

fn csv_header() -> String {
    CSV_COLUMNS.join(",")
}

fn cohort_delivery_bps(report: &SimulationReport, cohort: &str) -> u32 {
    report
        .cohort_delivery_basis_points
        .get(cohort)
        .copied()
        .unwrap_or_else(|| panic!("simulation report omitted {cohort:?} cohort"))
}

fn spam_delivery_bps(report: &SimulationReport, cohort: &str) -> u32 {
    report
        .signed_spam_delivery_basis_points_by_class
        .get(cohort)
        .copied()
        .unwrap_or_else(|| panic!("simulation report omitted {cohort:?} spam cohort"))
}

fn identity_expected(report: &SimulationReport, identity: &str) -> usize {
    report
        .expected_signed_spam_deliveries_by_identity
        .get(identity)
        .copied()
        .unwrap_or(0)
}

fn identity_delivered(report: &SimulationReport, identity: &str) -> usize {
    report
        .signed_spam_deliveries_by_identity
        .get(identity)
        .copied()
        .unwrap_or(0)
}

fn identity_suppression_bps(report: &SimulationReport, identity: &str) -> u32 {
    report
        .signed_spam_suppression_basis_points_by_identity
        .get(identity)
        .copied()
        .unwrap_or(0)
}

fn machine_identity_expected(report: &SimulationReport, identity: &str) -> usize {
    report
        .expected_machine_admitted_spam_deliveries_by_identity
        .get(identity)
        .copied()
        .unwrap_or(0)
}

fn machine_identity_delivered(report: &SimulationReport, identity: &str) -> usize {
    report
        .machine_admitted_spam_deliveries_by_identity
        .get(identity)
        .copied()
        .unwrap_or(0)
}

fn machine_identity_suppression_bps(report: &SimulationReport, identity: &str) -> u32 {
    report
        .machine_admitted_spam_suppression_basis_points_by_identity
        .get(identity)
        .copied()
        .unwrap_or(0)
}

fn filter_suppression_bps(report: &SimulationReport, cohort: &str) -> u32 {
    report
        .spam_filter_suppression_basis_points_by_class
        .get(cohort)
        .copied()
        .unwrap_or_else(|| panic!("simulation report omitted {cohort:?} filter cohort"))
}

fn parse_config(
    args: impl Iterator<Item = String>,
) -> Result<
    (
        SimulationConfig,
        Vec<TopologyStrategy>,
        Vec<PeerSelectionMode>,
    ),
    String,
> {
    let mut config = SimulationConfig::default();
    let mut topologies = vec![
        TopologyStrategy::PeerMesh,
        TopologyStrategy::HybridSupernodes,
    ];
    let mut modes = vec![
        PeerSelectionMode::Neutral,
        PeerSelectionMode::LocalBehavior,
        PeerSelectionMode::SharedReputation,
    ];
    let mut args = args;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {flag}"))?;
        match flag.as_str() {
            "--nodes" => config.node_count = parse_number(&flag, &value)?,
            "--attackers" => config.attacker_count = parse_number(&flag, &value)?,
            "--fanout" => config.fanout = parse_number(&flag, &value)?,
            "--unknown-reserve" => {
                config.unknown_peer_reserve = parse_number(&flag, &value)?;
            }
            "--max-hops" => config.max_hops = parse_number(&flag, &value)?,
            "--fake-inventories-per-attack-link" | "--spam-per-honest" => {
                config.fake_inventories_per_attack_link = parse_number(&flag, &value)?;
            }
            "--signed-spam-rounds" => {
                config.signed_spam_rounds = parse_number(&flag, &value)?;
            }
            "--action-budget" | "--message-budget" => {
                config.max_processed_actions = parse_number(&flag, &value)?;
            }
            "--seed" => config.seed = parse_number(&flag, &value)?,
            "--loss-bps" => config.loss_basis_points = parse_number(&flag, &value)?,
            "--churn-bps" => config.churn_basis_points = parse_number(&flag, &value)?,
            "--retry-ms" => config.retry_delay_ms = parse_number(&flag, &value)?,
            "--max-retries" => config.max_retries = parse_number(&flag, &value)?,
            "--supernodes" => config.supernode_count = parse_number(&flag, &value)?,
            "--false-supernodes" => {
                config.false_supernode_count = parse_number(&flag, &value)?;
            }
            "--supernode-links" => {
                config.supernode_links_per_peer = parse_number(&flag, &value)?;
            }
            "--supernode-fanout" => {
                config.supernode_fanout = parse_number(&flag, &value)?;
            }
            "--topology" => {
                topologies = match value.as_str() {
                    "peer" | "peer-mesh" => vec![TopologyStrategy::PeerMesh],
                    "supernodes" | "hybrid-supernodes" => {
                        vec![TopologyStrategy::HybridSupernodes]
                    }
                    "all" => vec![
                        TopologyStrategy::PeerMesh,
                        TopologyStrategy::HybridSupernodes,
                    ],
                    _ => return Err(format!("invalid topology {value:?}")),
                };
            }
            "--mode" => {
                modes = match value.as_str() {
                    "neutral" => vec![PeerSelectionMode::Neutral],
                    "local" | "local-behavior" => vec![PeerSelectionMode::LocalBehavior],
                    "shared" | "shared-reputation" => {
                        vec![PeerSelectionMode::SharedReputation]
                    }
                    "all" => vec![
                        PeerSelectionMode::Neutral,
                        PeerSelectionMode::LocalBehavior,
                        PeerSelectionMode::SharedReputation,
                    ],
                    _ => return Err(format!("invalid peer-selection mode {value:?}")),
                };
            }
            "--discovery" => {
                config.supernode_discovery = match value.as_str() {
                    "bootstrap" => SupernodeDiscoveryStrategy::Bootstrap,
                    "affinity" | "interest-affinity" | "social" | "social-graph" => {
                        SupernodeDiscoveryStrategy::InterestAffinity
                    }
                    "exploration" => SupernodeDiscoveryStrategy::Exploration,
                    "mixed" => SupernodeDiscoveryStrategy::Mixed,
                    _ => return Err(format!("invalid discovery strategy {value:?}")),
                };
            }
            _ => return Err(format!("unknown argument {flag}")),
        }
    }
    Ok((config, topologies, modes))
}

fn parse_number<T>(flag: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| format!("invalid numeric value {value:?} for {flag}"))
}

const fn topology_name(strategy: TopologyStrategy) -> &'static str {
    match strategy {
        TopologyStrategy::PeerMesh => "peer-mesh",
        TopologyStrategy::HybridSupernodes => "hybrid-supernodes",
    }
}

const fn discovery_name(strategy: SupernodeDiscoveryStrategy) -> &'static str {
    match strategy {
        SupernodeDiscoveryStrategy::Bootstrap => "bootstrap",
        SupernodeDiscoveryStrategy::InterestAffinity => "interest-affinity",
        SupernodeDiscoveryStrategy::Exploration => "exploration",
        SupernodeDiscoveryStrategy::Mixed => "mixed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_header_and_report_row_have_the_same_columns() {
        let report = run_simulation(
            SimulationConfig {
                node_count: 120,
                attacker_count: 24,
                supernode_count: 8,
                false_supernode_count: 4,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        let header = csv_header();
        let row = report_csv(&report);

        assert_eq!(CSV_COLUMNS.len(), header.split(',').count());
        assert_eq!(CSV_COLUMNS.len(), row.split(',').count());
    }

    #[test]
    fn parses_canonical_attack_controls_and_legacy_inventory_alias() {
        let (canonical, _, _) = parse_config(
            [
                "--fake-inventories-per-attack-link",
                "11",
                "--signed-spam-rounds",
                "5",
            ]
            .map(String::from)
            .into_iter(),
        )
        .unwrap();
        let (legacy, _, _) =
            parse_config(["--spam-per-honest", "7"].map(String::from).into_iter()).unwrap();

        assert_eq!(canonical.fake_inventories_per_attack_link, 11);
        assert_eq!(canonical.signed_spam_rounds, 5);
        assert_eq!(legacy.fake_inventories_per_attack_link, 7);
    }

    #[test]
    fn reports_interest_affinity_for_canonical_and_legacy_discovery_names() {
        for name in ["interest-affinity", "social-graph"] {
            let (config, _, _) =
                parse_config(["--discovery", name].map(String::from).into_iter()).unwrap();
            assert_eq!(
                config.supernode_discovery,
                SupernodeDiscoveryStrategy::InterestAffinity
            );
            assert_eq!(
                discovery_name(config.supernode_discovery),
                "interest-affinity"
            );
        }
    }
}
