use nostr_pubsub_sim::SimulationReport;

pub const REDISCOVERY_CSV_COLUMNS: &[&str] = &[
    "rediscovery_sweeps",
    "rediscovery_candidate_attempts",
    "rediscovery_links_removed",
    "rediscovery_links_added",
    "rediscovery_adversarial_links_removed",
    "rediscovery_unavailable_links_removed",
    "rediscovery_high_capacity_links_added",
    "rediscovery_state_entries",
    "rediscovery_subscription_refresh_nodes",
    "rediscovery_subscription_refresh_targets",
    "rediscovery_subscription_messages",
    "rediscovery_control_plane_wire_bytes",
];

pub fn rediscovery_values(report: &SimulationReport) -> Vec<String> {
    vec![
        report.rediscovery_sweeps.to_string(),
        report.rediscovery_candidate_attempts.to_string(),
        report.rediscovery_links_removed.to_string(),
        report.rediscovery_links_added.to_string(),
        report.rediscovery_adversarial_links_removed.to_string(),
        report.rediscovery_unavailable_links_removed.to_string(),
        report.rediscovery_high_capacity_links_added.to_string(),
        report.rediscovery_state_entries.to_string(),
        report.rediscovery_subscription_refresh_nodes.to_string(),
        report.rediscovery_subscription_refresh_targets.to_string(),
        report.rediscovery_subscription_messages.to_string(),
        report.rediscovery_control_plane_wire_bytes.to_string(),
    ]
}
