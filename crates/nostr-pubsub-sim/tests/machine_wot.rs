use nostr_pubsub_sim::{PeerSelectionMode, SimulationConfig, TopologyStrategy, run_simulation};
use nostr_pubsub_social_graph::PEER_RATING_MAX_ENTRIES;

#[test]
fn bounded_service_endorsements_transport_and_classify_admitted_rater_poison() {
    let report = run_simulation(
        SimulationConfig {
            node_count: 48,
            attacker_count: 8,
            fake_inventories_per_attack_link: 3,
            signed_spam_rounds: 3,
            legitimate_publication_rounds: 20,
            topology: TopologyStrategy::PeerMesh,
            supernode_count: 4,
            adversarial_discovery_candidate_count: 2,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::SharedReputation,
    )
    .expect("machine-WoT simulation");
    let endorser_bound = report.honest_node_count.div_ceil(16).saturating_mul(2);

    assert!(report.machine_positive_service_endorsements_published > 0);
    assert!(report.machine_positive_service_admissions > 0);
    assert!(report.machine_positive_service_endorsements_published <= endorser_bound);
    assert_eq!(
        report.machine_positive_endorsement_state_entries,
        report.machine_positive_service_endorsements_published
    );
    assert!((1..=8_000).contains(&report.machine_rating_protocol_messages));
    assert!((1..=4_000_000).contains(&report.machine_rating_protocol_bytes));
    assert!(report.machine_reputation_retained_ratings > 0);
    assert!(report.machine_reputation_retained_raters > 0);
    assert_eq!(
        report.machine_reputation_trusted_roots,
        report.honest_node_count,
    );
    assert!(
        report.machine_reputation_retained_ratings
            <= report
                .honest_node_count
                .saturating_mul(PEER_RATING_MAX_ENTRIES)
    );
    assert!(
        report.machine_reputation_retained_raters
            <= report
                .honest_node_count
                .saturating_mul(PEER_RATING_MAX_ENTRIES)
    );

    assert_eq!(report.unconnected_rating_pressure_published, 4);
    assert_eq!(report.unconnected_rating_pressure_target_received, 4);
    assert_eq!(report.unconnected_rating_pressure_target_ingested, 4);
    assert_eq!(report.unconnected_rating_pressure_target_rejected, 0);
    assert_eq!(report.unconnected_rating_pressure_distinct_raters, 4);
    assert_eq!(report.unconnected_rating_pressure_retained_rating_delta, 4);
    assert_eq!(report.unconnected_rating_pressure_retained_rater_delta, 4);
    assert_eq!(report.unconnected_rating_pressure_rebuild_entry_delta, 0);
    assert_eq!(report.unconnected_rating_pressure_anchor_positive_before, 1);
    assert_eq!(
        report.unconnected_rating_pressure_anchor_stable_evaluations,
        4
    );
    assert_eq!(
        report.unconnected_rating_pressure_anchor_projection_changes,
        0
    );
    let cpu = report.resource_usage.honest_all.cpu_work;
    assert!(
        cpu.reputation_events_considered.total
            >= u64::try_from(report.unconnected_rating_pressure_target_ingested).unwrap()
    );
    assert!(
        cpu.reputation_rebuild_entries.total
            <= cpu
                .reputation_events_considered
                .total
                .saturating_mul(u64::try_from(PEER_RATING_MAX_ENTRIES).unwrap())
    );

    assert_eq!(report.admitted_rater_poison_published, 2);
    assert!(report.admitted_rater_poison_received > 0);
    assert_eq!(
        report.admitted_rater_poison_ingested + report.admitted_rater_poison_rejected,
        report.admitted_rater_poison_received
    );
    assert!(report.admitted_rater_poison_ingested > 0);
    assert_eq!(report.admitted_rater_poison_service_admitted_rater, 1);
    assert!(report.admitted_rater_service_credits >= 3);
    assert!(report.admitted_rater_service_bytes > 0);
    assert_eq!(report.admitted_rater_poison_target_unknown_before, 2);
    assert_eq!(report.admitted_rater_poison_target_received, 2);
    assert_eq!(report.admitted_rater_poison_target_ingested, 2);
    assert_eq!(report.admitted_rater_poison_target_removals, 2);
    assert!(report.admitted_rater_poison_removals >= 2);
    assert_eq!(report.admitted_rater_misbehavior_frames, 5);
    assert_eq!(report.admitted_rater_revocations, 1);
    assert_eq!(report.admitted_rater_poison_target_recoveries, 2);
    assert_eq!(report.post_revocation_rating_published, 1);
    assert_eq!(report.post_revocation_rating_target_policy_drops, 0);
    assert_eq!(report.post_revocation_rating_target_received, 0);
    assert_eq!(report.post_revocation_rating_target_ingested, 0);
    assert_eq!(report.post_revocation_rating_influence, 0);
    assert_eq!(report.machine_false_positive_removals, 0);
}
