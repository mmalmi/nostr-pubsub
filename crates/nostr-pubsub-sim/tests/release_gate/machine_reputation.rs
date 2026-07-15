use nostr_pubsub_sim::SimulationReport;

use super::report_context;

pub(super) fn assert_machine_reputation_used_real_transport(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert!(report.machine_ratings_published > 0, "{context}");
    assert!(report.machine_ratings_received > 0, "{context}");
    assert!(report.machine_ratings_ingested > 0, "{context}");
    assert_eq!(
        report.machine_reputation_trusted_roots, report.honest_node_count,
        "only each node's local root may be configured: {context}",
    );
    assert!(report.poisoned_machine_ratings_published > 0, "{context}");
    assert!(report.poisoned_machine_ratings_received > 0, "{context}");
    assert!(report.poisoned_machine_ratings_ingested > 0, "{context}");
    assert!(
        report.poisoned_machine_ratings_received >= report.poisoned_machine_ratings_ingested,
        "{context}"
    );
    assert!(report.forged_machine_ratings_published > 0, "{context}");
    assert!(report.forged_machine_ratings_received > 0, "{context}");
    assert!(report.forged_machine_ratings_evaluated > 0, "{context}");
    assert_eq!(report.forged_machine_ratings_ingested, 0, "{context}");
    assert_eq!(
        report.forged_machine_ratings_rejected, report.forged_machine_ratings_evaluated,
        "{context}"
    );
    assert!(report.machine_transported_transitions > 0, "{context}");
    assert!(
        report.machine_transported_positive_admissions > 0,
        "{context}"
    );
    assert!(report.machine_transported_removals > 0, "{context}");
    assert_eq!(report.machine_lifecycle_ratings_published, 3, "{context}");
    assert!(report.machine_lifecycle_admissions > 0, "{context}");
    assert!(report.machine_lifecycle_removals > 0, "{context}");
    assert!(report.machine_lifecycle_readmissions > 0, "{context}");
    assert!(report.machine_reversible_lifecycles > 0, "{context}");
    assert!(report.machine_positive_admissions > 0, "{context}");
    assert!(report.machine_removals > 0, "{context}");
    assert!(report.machine_poisoning_removals > 0, "{context}");
    assert!(report.machine_removal_latency_p95_ms > 0, "{context}");
    assert!(report.machine_trust_edges > 0, "{context}");

    assert_eq!(report.admitted_rater_poison_published, 2, "{context}");
    assert_eq!(
        report.admitted_rater_poison_service_admitted_rater, 1,
        "{context}"
    );
    assert!(report.admitted_rater_service_credits >= 3, "{context}");
    assert!(report.admitted_rater_service_bytes > 0, "{context}");
    assert_eq!(
        report.admitted_rater_poison_target_unknown_before, 2,
        "{context}"
    );
    assert_eq!(report.admitted_rater_poison_target_received, 2, "{context}");
    assert_eq!(report.admitted_rater_poison_target_ingested, 2, "{context}");
    assert_eq!(report.admitted_rater_poison_target_removals, 2, "{context}");
    assert_eq!(report.admitted_rater_misbehavior_frames, 5, "{context}");
    assert_eq!(report.admitted_rater_revocations, 1, "{context}");
    assert_eq!(
        report.admitted_rater_poison_target_recoveries, 2,
        "{context}"
    );
    assert_eq!(report.post_revocation_rating_published, 1, "{context}");
    assert_eq!(
        report.post_revocation_rating_target_policy_drops, 0,
        "{context}"
    );
    assert_eq!(
        report.post_revocation_rating_target_received, 0,
        "{context}"
    );
    assert_eq!(
        report.post_revocation_rating_target_ingested, 0,
        "{context}"
    );
    assert_eq!(report.post_revocation_rating_influence, 0, "{context}");

    // This batch is only a bounded retained-state/CPU pressure control. Its
    // graph-unconnected raters must remain inert; it is not a spam-defense KPI.
    assert_eq!(report.unconnected_rating_pressure_published, 4, "{context}");
    assert_eq!(
        report.unconnected_rating_pressure_target_ingested, 4,
        "{context}"
    );
    assert_eq!(
        report.unconnected_rating_pressure_distinct_raters, 4,
        "{context}"
    );
    assert_eq!(
        report.unconnected_rating_pressure_rebuild_entry_delta, 0,
        "{context}"
    );
    assert_eq!(
        report.unconnected_rating_pressure_anchor_projection_changes, 0,
        "{context}"
    );
}
