use nostr_pubsub_sim::{
    IncentiveConfig, IncentiveStrategy, IncentiveUseCase, PeerSelectionMode, SimulationConfig,
    SupernodeDiscoveryStrategy, TopologyStrategy, basis_points, compare_incentive_strategies,
    recommend_incentive_strategy, run_simulation,
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
            assert!(report.adversarial_payer_value_sat > 0, "{report:?}");
        }
        assert!(peer.max_pair_unpaid_exposure_sat <= config.offline_credit_cap_sat);
        assert!(batch.reciprocal_service_settled_sat > 0, "{batch:?}");
        assert!(batch.peer_credit_accepted_sat > 0, "{batch:?}");
        assert!(
            batch.max_pair_peer_credit_sat <= config.offline_credit_cap_sat,
            "topology={topology:?}: {batch:?}"
        );
        assert!(
            batch.payment_byte_overhead_basis_points <= 500,
            "topology={topology:?}: {batch:?}"
        );
        assert_selected_resource_gates(&simulation, batch, topology);
    }
}

#[test]
fn adversarial_pubsub_delivery_compares_five_bounded_payment_plans() {
    let simulation = run_simulation(
        SimulationConfig {
            node_count: 96,
            attacker_count: 16,
            fake_inventories_per_attack_link: 3,
            signed_spam_rounds: 3,
            legitimate_publication_rounds: 20,
            supernode_count: 8,
            adversarial_discovery_candidate_count: 4,
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
    assert_workload_aware_exposure_policy(&simulation, &config, &reports);
}

fn assert_workload_aware_exposure_policy(
    simulation: &nostr_pubsub_sim::SimulationReport,
    one_shot_config: &IncentiveConfig,
    one_shot_reports: &[nostr_pubsub_sim::IncentiveReport],
) {
    let selected =
        recommend_incentive_strategy(one_shot_reports, IncentiveUseCase::VerifiedOneShot)
            .expect("verified one-shot delivery must have an eligible strategy");
    assert_eq!(selected.strategy, IncentiveStrategy::AcceptedMintBatch);

    let streaming_config = IncentiveConfig {
        use_case: IncentiveUseCase::UntrustedStreaming,
        provider_failure_basis_points: 10_000,
        spilman_update_sat: 1,
        spilman_open_fee_sat: 1,
        spilman_close_fee_sat: 1,
        spilman_channel_lifetime_ms: 60_000,
        ..one_shot_config.clone()
    };
    let streaming_reports = compare_incentive_strategies(simulation, &streaming_config)
        .expect("streaming exposure plans must use the same verified delivery trail");
    let selected =
        recommend_incentive_strategy(&streaming_reports, IncentiveUseCase::UntrustedStreaming)
            .expect("untrusted streaming must have an eligible strategy");
    assert_eq!(selected.strategy, IncentiveStrategy::IncrementalSpilman);

    let spilman = strategy(&streaming_reports, IncentiveStrategy::IncrementalSpilman);
    let fixed = strategy(&streaming_reports, IncentiveStrategy::FixedPrepaidCashu);
    let batch = strategy(&streaming_reports, IncentiveStrategy::AcceptedMintBatch);
    let peer = strategy(&streaming_reports, IncentiveStrategy::OfflinePeerCredit);
    assert_eq!(spilman.buyer_prepaid_exposure_sat, 0);
    assert!(fixed.buyer_prepaid_exposure_sat > 0, "{fixed:?}");
    assert!(fixed.buyer_counterparty_loss_sat > 0, "{fixed:?}");
    assert_eq!(fixed.honest_earned_sat, 0, "{fixed:?}");
    assert_eq!(batch.buyer_prepaid_exposure_sat, 0, "{batch:?}");
    assert!(batch.provider_unpaid_exposure_sat > 0, "{batch:?}");
    assert!(spilman.provider_unpaid_exposure_sat <= streaming_config.spilman_update_sat);
    assert!(peer.provider_unpaid_exposure_sat <= streaming_config.offline_credit_cap_sat);
    assert!(spilman.locked_capital_sat_ms > 0, "{spilman:?}");
    assert!(spilman.peak_locked_capital_sat > 0, "{spilman:?}");
    assert!(spilman.channel_open_fees_sat > 0, "{spilman:?}");
    assert!(spilman.channel_close_fees_sat > 0, "{spilman:?}");
    assert!(spilman.channel_value_is_conserved(), "{spilman:?}");

    let refund_failure_config = IncentiveConfig {
        spilman_refund_failure_basis_points: 10_000,
        ..streaming_config
    };
    let refund_reports = compare_incentive_strategies(simulation, &refund_failure_config)
        .expect("refund failure scenario must remain deterministic");
    let failed_refund = strategy(&refund_reports, IncentiveStrategy::IncrementalSpilman);
    assert!(failed_refund.refund_failures > 0, "{failed_refund:?}");
    assert!(failed_refund.refund_loss_sat > 0, "{failed_refund:?}");
    assert!(
        failed_refund.channel_value_is_conserved(),
        "{failed_refund:?}"
    );
}

fn assert_strategy_comparison(
    simulation: &nostr_pubsub_sim::SimulationReport,
    config: &IncentiveConfig,
    reports: &[nostr_pubsub_sim::IncentiveReport],
) {
    let direct = strategy(reports, IncentiveStrategy::DirectVerifiedCashu);
    let peer = strategy(reports, IncentiveStrategy::OfflinePeerCredit);
    let spilman = strategy(reports, IncentiveStrategy::IncrementalSpilman);
    let batch = strategy(reports, IncentiveStrategy::AcceptedMintBatch);
    assert!(direct.unpaid_exposure_sat > 0, "{direct:?}");
    assert!(spilman.unpaid_exposure_sat > 0, "{spilman:?}");
    assert!(
        spilman.max_pair_unpaid_exposure_sat <= config.spilman_update_sat,
        "{spilman:?}"
    );
    assert!(batch.unpaid_exposure_sat > 0, "{batch:?}");
    assert!(batch.peer_credit_accepted_sat > 0, "{batch:?}");
    assert!(batch.peer_credit_outstanding_sat > 0, "{batch:?}");
    assert!(batch.max_pair_peer_credit_sat <= config.offline_credit_cap_sat);
    assert!(direct.provider_default_loss_sat > 0, "{direct:?}");
    assert!(batch.provider_default_loss_sat > 0, "{batch:?}");
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
    assert!(peer.meets_strategy_selection_gate(), "{peer:?}");
    assert!(batch.meets_strategy_selection_gate(), "{batch:?}");
    assert!(!direct.meets_payment_overhead_goal(), "{direct:?}");
    assert!(!spilman.meets_payment_overhead_goal(), "{spilman:?}");
    let recommended = reports
        .iter()
        .filter(|report| report.meets_strategy_selection_gate())
        .min_by_key(|report| {
            (
                report.authorization_denied_sat,
                report.unpaid_exposure_sat,
                report.payment_bytes,
            )
        })
        .expect("at least one strategy must pass settlement and overhead goals");
    eprintln!(
        "recommended modeled strategy: {}",
        recommended.strategy_name
    );
    assert_eq!(recommended.strategy, IncentiveStrategy::AcceptedMintBatch);
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
    assert_eq!(reports.len(), 5);
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
        assert!(report.prepaid_value_is_conserved(), "{report:?}");
        assert!(report.channel_value_is_conserved(), "{report:?}");
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
        adversarial_discovery_candidate_count: 8,
        supernode_links_per_peer: 3,
        supernode_fanout: 192,
        loss_basis_points: 200,
        churn_basis_points: 300,
        retry_delay_ms: 80,
        max_retries: 3,
    }
}
