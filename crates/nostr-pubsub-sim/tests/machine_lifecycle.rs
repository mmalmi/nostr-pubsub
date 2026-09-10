use nostr_pubsub_sim::{PeerSelectionMode, SimulationConfig, run_simulation};

#[test]
fn service_only_ratings_do_not_change_remote_authority() {
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
    assert_eq!(report.machine_lifecycle_admissions, 0, "{report:?}");
    assert_eq!(report.machine_lifecycle_removals, 0, "{report:?}");
    assert_eq!(report.machine_lifecycle_readmissions, 0, "{report:?}");
    assert_eq!(report.machine_reversible_lifecycles, 0, "{report:?}");
}

#[test]
fn configured_authorities_admit_remove_and_readmit_over_production_transport() {
    let report = run_simulation(
        SimulationConfig {
            node_count: 48,
            attacker_count: 8,
            // Fixed scenario inputs include an attacker; authority is not selected by role.
            trusted_raters: [0, 8, 16, 24, 32, 40].into_iter().collect(),
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
}
