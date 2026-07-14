use nostr_pubsub_sim::{PeerSelectionMode, SimulationConfig, run_simulation};

#[test]
fn one_subject_is_admitted_removed_and_readmitted_over_production_transport() {
    let report = run_simulation(
        SimulationConfig {
            node_count: 48,
            attacker_count: 8,
            fake_inventories_per_attack_link: 0,
            signed_spam_rounds: 0,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::SharedReputation,
    )
    .unwrap();

    assert_eq!(report.machine_lifecycle_ratings_published, 3);
    assert!(report.machine_lifecycle_admissions > 0, "{report:?}");
    assert!(report.machine_lifecycle_removals > 0, "{report:?}");
    assert!(report.machine_lifecycle_readmissions > 0, "{report:?}");
    assert!(report.machine_reversible_lifecycles > 0, "{report:?}");
    assert_eq!(
        report.human_signed_graph_updates_ingested,
        report.human_lifecycle_checks
    );
    assert_eq!(
        report.human_lifecycle_successes,
        report.human_lifecycle_checks
    );
    assert!(report.human_follow_admissions > 0);
    assert!(report.human_follow_removals > 0);
    assert!(report.human_stale_update_rejections > 0);
    assert!(report.human_follow_readmissions > 0);
    assert!(report.human_mute_removals > 0);
}
