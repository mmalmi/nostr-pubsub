use nostr_pubsub_sim::{
    IncentiveConfig, IncentiveStrategy, PeerSelectionMode, SimulationConfig,
    SupernodeDiscoveryStrategy, TopologyStrategy, basis_points, compare_incentive_strategies,
    run_simulation,
};

#[test]
#[ignore = "production-scale deterministic incentive gate"]
fn thousand_node_adversarial_incentive_gate() {
    for topology in [
        TopologyStrategy::PeerMesh,
        TopologyStrategy::HybridSupernodes,
    ] {
        let simulation = run_simulation(
            thousand_node_config(topology),
            PeerSelectionMode::SharedReputation,
        )
        .unwrap_or_else(|error| panic!("topology={topology:?} failed: {error}"));
        assert_eq!(simulation.legitimate_events, 160);
        let config = IncentiveConfig {
            bytes_per_sat: 128,
            batch_interval_ms: 10_000,
            cashu_min_settlement_sat: 10,
            offline_credit_cap_sat: 16,
            outage_basis_points: 1_000,
            // Hash-selected active payers fluctuate around their configured
            // rate; retain two points of margin for the 95% value gate.
            payment_defaulter_basis_points: 300,
            max_payment_retries: 3,
            settlement_deadline_ms: 20_000,
            ..IncentiveConfig::default()
        };
        let reports = compare_incentive_strategies(&simulation, &config)
            .expect("verified production-scale delivery records must produce incentive plans");
        let batch = strategy(&reports, IncentiveStrategy::AcceptedMintBatch);
        let peer = strategy(&reports, IncentiveStrategy::OfflinePeerCredit);
        eprintln!(
            "topology={topology:?} deliveries={} useful_bytes={} baseline_state_p95={} baseline_messages_p95={} baseline_bytes_p95={} accepted_batch={batch:?}",
            simulation.verified_delivery_records.len(),
            batch.useful_bytes,
            simulation
                .resource_usage
                .honest_peers
                .peak_retained
                .state_entries
                .p95,
            simulation.resource_usage.honest_peers.combined_messages.p95,
            simulation.resource_usage.honest_peers.combined_bytes.p95,
        );

        assert!(simulation.spam_events > 0);
        assert!(simulation.spam_delivered > 0);
        assert!(simulation.rejected_malformed_messages > 0);
        for report in &reports {
            assert!(
                report.honest_earned_settled_by_deadline_basis_points >= 9_500,
                "topology={topology:?}: {report:?}"
            );
            assert_eq!(
                report.fake_claim_cashout_sat, 0,
                "topology={topology:?}: {report:?}"
            );
            assert!(report.adversarial_payer_value_sat > 0, "{report:?}");
        }
        assert!(peer.max_pair_unpaid_exposure_sat <= config.offline_credit_cap_sat);
        assert!(batch.reciprocal_service_settled_sat > 0, "{batch:?}");
        assert!(
            batch.payment_byte_overhead_basis_points <= 500,
            "topology={topology:?}: {batch:?}"
        );
        assert_selected_resource_gates(&simulation, batch, topology);
    }
}

#[test]
fn adversarial_pubsub_delivery_compares_four_bounded_payment_plans() {
    let simulation = run_simulation(
        SimulationConfig {
            node_count: 96,
            attacker_count: 16,
            fake_inventories_per_attack_link: 3,
            signed_spam_rounds: 3,
            legitimate_publication_rounds: 20,
            supernode_count: 8,
            false_supernode_count: 4,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::SharedReputation,
    )
    .expect("adversarial pubsub simulation must complete");
    assert_eq!(simulation.legitimate_events, 160);
    assert!(simulation.spam_events > 0);
    assert!(simulation.spam_delivered > 0);
    assert!(simulation.rejected_malformed_messages > 0);
    let config = IncentiveConfig {
        bytes_per_sat: 128,
        batch_interval_ms: 10_000,
        offline_credit_cap_sat: 16,
        outage_basis_points: 1_000,
        payment_defaulter_basis_points: 500,
        max_payment_retries: 3,
        settlement_deadline_ms: 20_000,
        ..IncentiveConfig::default()
    };
    let reports = compare_incentive_strategies(&simulation, &config)
        .expect("verified delivery records must produce incentive plans");
    eprintln!(
        "honest peer baseline: state_p95={} codec_bytes_p95={} messages_p95={} combined_bytes_p95={}",
        simulation
            .resource_usage
            .honest_peers
            .peak_retained
            .state_entries
            .p95,
        simulation
            .resource_usage
            .honest_peers
            .cpu_work
            .codec_bytes
            .p95,
        simulation.resource_usage.honest_peers.combined_messages.p95,
        simulation.resource_usage.honest_peers.combined_bytes.p95,
    );
    assert_common_reports(&simulation, &config, &reports);
    assert_strategy_comparison(&simulation, &config, &reports);
}

fn assert_strategy_comparison(
    simulation: &nostr_pubsub_sim::SimulationReport,
    config: &IncentiveConfig,
    reports: &[nostr_pubsub_sim::IncentiveReport],
) {
    let direct = strategy(reports, IncentiveStrategy::DirectVerifiedCashu);
    let peer = strategy(reports, IncentiveStrategy::OfflinePeerCredit);
    let spilman = strategy(reports, IncentiveStrategy::PrefundedSpilman);
    let batch = strategy(reports, IncentiveStrategy::AcceptedMintBatch);
    assert_eq!(direct.unpaid_exposure_sat, 0);
    assert_eq!(spilman.unpaid_exposure_sat, 0);
    assert_eq!(batch.unpaid_exposure_sat, 0);
    assert!(peer.default_exposure_sat > 0, "{peer:?}");
    assert_eq!(peer.unpaid_exposure_sat, peer.default_exposure_sat);
    assert!(peer.max_pair_unpaid_exposure_sat <= config.offline_credit_cap_sat);
    assert!(
        batch.payment_messages < direct.payment_messages,
        "{direct:?}\n{batch:?}"
    );
    assert!(
        batch.payment_bytes < direct.payment_bytes,
        "{direct:?}\n{batch:?}"
    );
    assert!(
        batch.payment_byte_overhead_basis_points <= 500,
        "accepted-mint batching must stay within the 5% modeled overhead gate: {batch:?}"
    );
    assert!(batch.same_mint_settlements > 0, "{batch:?}");
    assert!(batch.cross_mint_settlements > 0, "{batch:?}");
    assert_selected_resource_gates(simulation, batch, simulation.topology);
    assert!(
        batch.honest_node_payment_messages.p95 < direct.honest_node_payment_messages.p95,
        "{direct:?}\n{batch:?}"
    );
    assert!(
        batch.honest_node_payment_state_entries.p95 < direct.honest_node_payment_state_entries.p95,
        "{direct:?}\n{batch:?}"
    );
    assert!(
        batch.honest_node_payment_endpoint_bytes.p95
            < direct.honest_node_payment_endpoint_bytes.p95,
        "{direct:?}\n{batch:?}"
    );
}

fn assert_common_reports(
    simulation: &nostr_pubsub_sim::SimulationReport,
    config: &IncentiveConfig,
    reports: &[nostr_pubsub_sim::IncentiveReport],
) {
    assert_eq!(reports.len(), 4);
    let useful_bytes = simulation
        .verified_delivery_records
        .iter()
        .map(|record| record.payload_bytes)
        .sum::<u64>();
    for report in reports {
        eprintln!("{report:?}");
        assert!(report.figures_are_modeled);
        assert_eq!(report.useful_bytes, useful_bytes);
        assert!(report.useful_value_sat > 0, "{report:?}");
        assert_eq!(
            report
                .useful_value_sat
                .saturating_mul(config.bytes_per_sat)
                .saturating_add(report.pricing_dust_bytes),
            report.useful_bytes,
            "priced value and retained pair dust must conserve: {report:?}"
        );
        assert!(
            report.honest_earned_settled_by_deadline_basis_points >= 9_500,
            "{report:?}"
        );
        assert!(report.fake_claims_attempted > 0, "{report:?}");
        assert!(report.fake_claimed_value_sat > 0, "{report:?}");
        assert_eq!(report.fake_claim_cashout_sat, 0, "{report:?}");
        assert_eq!(
            report.honest_node_payment_state_entries.count,
            simulation.honest_node_count
        );
        assert_eq!(
            report.honest_node_payment_messages.count,
            simulation.honest_node_count
        );
        assert_eq!(
            report.honest_node_payment_endpoint_bytes.count,
            simulation.honest_node_count
        );
        assert!(report.adversarial_payer_count > 0, "{report:?}");
        assert!(report.adversarial_payer_value_sat > 0, "{report:?}");
    }
    assert!(reports.iter().any(|report| report.payment_retries > 0));
}

fn strategy(
    reports: &[nostr_pubsub_sim::IncentiveReport],
    strategy: IncentiveStrategy,
) -> &nostr_pubsub_sim::IncentiveReport {
    reports
        .iter()
        .find(|report| report.strategy == strategy)
        .expect("all strategies are present")
}

fn assert_selected_resource_gates(
    simulation: &nostr_pubsub_sim::SimulationReport,
    batch: &nostr_pubsub_sim::IncentiveReport,
    topology: TopologyStrategy,
) {
    let baseline = simulation.resource_usage.honest_peers;
    for (name, extra, measured) in [
        (
            "state",
            batch.honest_node_payment_state_entries.p95,
            baseline.peak_retained.state_entries.p95,
        ),
        (
            "message work",
            batch.honest_node_payment_messages.p95,
            baseline.combined_messages.p95,
        ),
        (
            "endpoint bytes",
            batch.honest_node_payment_endpoint_bytes.p95,
            baseline.combined_bytes.p95,
        ),
    ] {
        assert!(
            basis_points(extra, measured) <= 1_000,
            "accepted-mint modeled honest p95 {name} must add at most 10% for topology={topology:?}: {batch:?}"
        );
    }
}

fn thousand_node_config(topology: TopologyStrategy) -> SimulationConfig {
    SimulationConfig {
        node_count: 1_000,
        attacker_count: 200,
        fanout: 6,
        unknown_peer_reserve: 1,
        max_hops: 16,
        fake_inventories_per_attack_link: 6,
        signed_spam_rounds: 8,
        legitimate_publication_rounds: 20,
        max_processed_actions: 10_000_000,
        seed: 0x4e4f_5354_5250_5542,
        topology,
        supernode_discovery: SupernodeDiscoveryStrategy::Mixed,
        supernode_count: 16,
        false_supernode_count: 8,
        supernode_links_per_peer: 3,
        supernode_fanout: 192,
        loss_basis_points: 200,
        churn_basis_points: 300,
        retry_delay_ms: 80,
        max_retries: 3,
    }
}
