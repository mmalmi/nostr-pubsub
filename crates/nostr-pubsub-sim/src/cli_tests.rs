use super::*;

#[test]
fn csv_header_and_report_row_have_the_same_columns() {
    let report = run_simulation(
        SimulationConfig {
            node_count: 120,
            attacker_count: 24,
            trusted_raters: [0, 17].into_iter().collect(),
            supernode_count: 8,
            adversarial_discovery_candidate_count: 4,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::SharedReputation,
    )
    .unwrap();
    let header = csv_header();
    let row = report_csv(&report);

    assert_eq!(csv_column_count(), header.split(',').count());
    assert_eq!(csv_column_count(), row.split(',').count());
    let trust_column = header
        .split(',')
        .position(|name| name == "trusted_raters")
        .unwrap();
    assert_eq!(row.split(',').nth(trust_column), Some("0;17"));
}

#[test]
fn parses_canonical_attack_controls_and_legacy_inventory_alias() {
    let (canonical, _, _) = parse_config(
        [
            "--fake-inventories-per-attack-link",
            "11",
            "--signed-spam-rounds",
            "5",
            "--legitimate-publication-rounds",
            "7",
            "--adversarial-discovery-candidates",
            "9",
            "--trusted-raters",
            "17,0,17",
        ]
        .map(String::from)
        .into_iter(),
    )
    .unwrap();
    let (legacy, _, _) =
        parse_config(["--spam-per-honest", "7"].map(String::from).into_iter()).unwrap();

    assert_eq!(canonical.fake_inventories_per_attack_link, 11);
    assert_eq!(canonical.signed_spam_rounds, 5);
    assert_eq!(canonical.legitimate_publication_rounds, 7);
    assert_eq!(canonical.adversarial_discovery_candidate_count, 9);
    assert_eq!(canonical.trusted_raters, [0, 17].into_iter().collect());
    assert!(legacy.trusted_raters.is_empty());
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
