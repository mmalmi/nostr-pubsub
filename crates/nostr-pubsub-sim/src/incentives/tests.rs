use super::*;

fn record(
    event_id: &str,
    provider: usize,
    receiver: usize,
    payload_bytes: u64,
    accepted_at_ms: u64,
) -> VerifiedDeliveryRecord {
    VerifiedDeliveryRecord {
        event_id: event_id.to_string(),
        provider,
        receiver,
        payload_bytes,
        accepted_at_ms,
        final_interested_delivery: true,
    }
}

#[test]
fn pricing_retains_pair_dust_exactly() {
    let records = vec![
        record("a", 0, 1, 600, 1),
        record("b", 0, 1, 600, 2),
        record("c", 1, 2, 300, 3),
    ];
    let priced = price_records(&records, 1_000);
    assert_eq!(priced.useful_bytes, 1_500);
    assert_eq!(priced.useful_value_sat, 1);
    assert_eq!(priced.pricing_dust_bytes, 500);
    assert_eq!(priced.obligations.len(), 1);
}

#[test]
fn adversarial_peer_credit_loss_is_pair_capped_and_fake_claims_pay_zero() {
    let records = vec![record("a", 0, 1, 100, 1), record("b", 0, 1, 100, 2)];
    let roles = vec![NodeRole::Peer, NodeRole::Attacker];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        offline_credit_cap_sat: 7,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let peer = plan_peer_credit(&workload, &roles, &config);
    assert_eq!(peer.adversarial_payer_count, 1);
    assert_eq!(peer.adversarial_payer_value_sat, 200);
    assert_eq!(peer.unpaid_exposure_sat, 7);
    assert_eq!(peer.default_exposure_sat, 7);
    assert_eq!(peer.max_pair_unpaid_exposure_sat, 7);
    assert_eq!(peer.provider_unpaid_exposure_sat, 7);
    assert_eq!(peer.provider_default_loss_sat, 7);
    assert_eq!(peer.authorization_denied_sat, 193);

    let direct = plan_direct(&workload, &roles, &config);
    assert_eq!(direct.unpaid_exposure_sat, 200);
    assert_eq!(direct.default_exposure_sat, 200);
    assert_eq!(direct.provider_unpaid_exposure_sat, 200);
    assert_eq!(direct.max_pair_unpaid_exposure_sat, 200);
    assert_eq!(direct.provider_default_loss_sat, 200);
    assert_eq!(direct.honest_earned_sat, 200);
    assert_eq!(direct.honest_settled_by_deadline_sat, 0);
    assert_eq!(direct.authorization_denied_sat, 0);
}

#[test]
fn peer_credit_nets_verified_reciprocal_service_before_applying_the_cap() {
    let records = vec![record("a", 0, 1, 100, 1), record("b", 1, 0, 100, 2)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        offline_credit_cap_sat: 7,
        outage_basis_points: 0,
        payment_defaulter_basis_points: 0,
        fake_claims_per_attacker: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let peer = plan_peer_credit(&workload, &roles, &config);

    assert_eq!(peer.honest_earned_sat, 200);
    assert_eq!(peer.honest_settled_by_deadline_sat, 200);
    assert_eq!(peer.reciprocal_service_settled_sat, 200);
    assert_eq!(peer.authorization_denied_sat, 0);
    assert_eq!(peer.provider_unpaid_exposure_sat, 0);
    assert_eq!(peer.payment_messages, 0);
}

#[test]
fn post_delivery_default_is_direct_loss_but_batch_credit_is_pair_capped() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        payment_defaulter_basis_points: 10_000,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);

    let direct = plan_direct(&workload, &roles, &config);
    assert_eq!(direct.honest_earned_sat, 100, "{direct:?}");
    assert_eq!(direct.honest_settled_by_deadline_sat, 0, "{direct:?}");
    assert_eq!(direct.provider_default_loss_sat, 100, "{direct:?}");
    assert_eq!(direct.default_exposure_sat, 100, "{direct:?}");
    assert_eq!(direct.authorization_denied_sat, 0, "{direct:?}");

    let batch = plan_accepted_mint_batch(&workload, &roles, &config);
    assert_eq!(batch.honest_earned_sat, 32, "{batch:?}");
    assert_eq!(batch.honest_settled_by_deadline_sat, 0, "{batch:?}");
    assert_eq!(batch.provider_default_loss_sat, 32, "{batch:?}");
    assert_eq!(batch.default_exposure_sat, 32, "{batch:?}");
    assert_eq!(batch.peer_credit_accepted_sat, 32, "{batch:?}");
    assert_eq!(batch.peer_credit_outstanding_sat, 32, "{batch:?}");
    assert_eq!(batch.max_pair_peer_credit_sat, 32, "{batch:?}");
    assert_eq!(batch.authorization_denied_sat, 68, "{batch:?}");
    assert_eq!(
        batch
            .honest_earned_sat
            .saturating_add(batch.authorization_denied_sat),
        batch.useful_value_sat,
        "accepted plus stopped service must conserve the priced workload"
    );
}

#[test]
fn post_delivery_outage_is_direct_pending_but_batch_stops_at_credit_cap() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        payment_defaulter_basis_points: 0,
        outage_basis_points: 10_000,
        cashu_min_settlement_sat: 1,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);

    let direct = plan_direct(&workload, &roles, &config);
    assert_eq!(direct.honest_earned_sat, 100, "{direct:?}");
    assert_eq!(direct.honest_settled_by_deadline_sat, 0, "{direct:?}");
    assert_eq!(direct.pending_settlement_sat, 100, "{direct:?}");
    assert_eq!(direct.unpaid_exposure_sat, 100, "{direct:?}");
    assert_eq!(direct.default_exposure_sat, 0, "{direct:?}");
    assert_eq!(direct.authorization_denied_sat, 0, "{direct:?}");

    let batch = plan_accepted_mint_batch(&workload, &roles, &config);
    assert_eq!(batch.honest_earned_sat, 32, "{batch:?}");
    assert_eq!(batch.honest_settled_by_deadline_sat, 32, "{batch:?}");
    assert_eq!(batch.pending_settlement_sat, 0, "{batch:?}");
    assert_eq!(batch.unpaid_exposure_sat, 32, "{batch:?}");
    assert_eq!(batch.default_exposure_sat, 0, "{batch:?}");
    assert_eq!(batch.peer_credit_accepted_sat, 32, "{batch:?}");
    assert_eq!(batch.peer_credit_outstanding_sat, 32, "{batch:?}");
    assert_eq!(batch.max_pair_peer_credit_sat, 32, "{batch:?}");
    assert_eq!(batch.authorization_denied_sat, 68, "{batch:?}");
    assert_eq!(batch.same_mint_settlements, 0, "{batch:?}");
    assert_eq!(batch.cross_mint_settlements, 0, "{batch:?}");
}

#[test]
fn outage_exposes_post_delivery_direct_but_prevents_channel_service() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        outage_basis_points: 10_000,
        max_payment_retries: 2,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let direct = plan_direct(&workload, &roles, &config);
    let spilman = plan_spilman(&workload, &roles, &config);
    assert_eq!(direct.unpaid_exposure_sat, 100);
    assert_eq!(spilman.unpaid_exposure_sat, 0);
    assert_eq!(direct.honest_earned_sat, 100);
    assert_eq!(spilman.honest_earned_sat, 0);
    assert_eq!(direct.settlement_attempts, 3);
    assert_eq!(spilman.settlement_attempts, 3);
}

#[test]
fn funded_spilman_defaulter_can_steal_at_most_one_update() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        payment_defaulter_basis_points: 10_000,
        spilman_prefund_sat: 1_000,
        spilman_update_sat: 16,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_spilman(&workload, &roles, &config);

    assert_eq!(report.honest_earned_sat, 16);
    assert_eq!(report.honest_settled_by_deadline_sat, 0);
    assert_eq!(report.provider_unpaid_exposure_sat, 16);
    assert_eq!(report.provider_default_loss_sat, 16);
    assert_eq!(report.unpaid_exposure_sat, 16);
    assert_eq!(report.authorization_denied_sat, 84);
    assert_eq!(report.channel_refunded_sat, 1_000);
    assert!(report.channel_value_is_conserved(), "{report:?}");
}

#[test]
fn seeded_outages_and_private_mint_assignment_are_deterministic() {
    let records = vec![
        record("a", 0, 1, 100, 1),
        record("b", 2, 1, 100, 2),
        record("c", 1, 2, 100, 3),
    ];
    let roles = vec![NodeRole::Peer, NodeRole::Peer, NodeRole::Attacker];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        outage_basis_points: 5_000,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    assert_eq!(
        plan_accepted_mint_batch(&workload, &roles, &config),
        plan_accepted_mint_batch(&workload, &roles, &config)
    );
}

#[test]
fn connected_mint_discovery_is_smaller_than_public_broadcast() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 3];
    let mut config = IncentiveConfig {
        bytes_per_sat: 1,
        outage_basis_points: 0,
        payment_defaulter_basis_points: 0,
        fake_claims_per_attacker: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let connected = plan_accepted_mint_batch(&workload, &roles, &config);
    config.mint_discovery_scope = MintDiscoveryScope::Public;
    let public = plan_accepted_mint_batch(&workload, &roles, &config);
    assert_eq!(connected.mint_advertisement_messages, 0);
    assert_eq!(public.mint_advertisement_messages, 6);
    assert!(connected.payment_bytes < public.payment_bytes);
    assert_eq!(connected.mint_accepting_nodes.len(), config.mint_count);
    assert_eq!(connected.mint_accepting_nodes[0], roles.len() as u64);
}

#[test]
fn full_spilman_channel_has_no_refund_to_fail() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        provider_failure_basis_points: 10_000,
        spilman_prefund_sat: 100,
        spilman_refund_failure_basis_points: 10_000,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_spilman(&workload, &roles, &config);

    assert_eq!(report.channel_unused_capacity_sat, 0);
    assert_eq!(report.refund_failures, 0);
    assert_eq!(report.refund_loss_sat, 0);
}

#[test]
fn fixed_prepay_exposes_the_upfront_lease_not_hindsight_usage() {
    let records = vec![record("a", 0, 1, 87, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        fixed_prepay_sat: 4_096,
        provider_failure_basis_points: 10_000,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_fixed_prepay(&workload, &roles, &config);

    assert_eq!(report.buyer_prepaid_exposure_sat, 4_096);
    assert_eq!(report.buyer_counterparty_loss_sat, 4_096);
    assert_eq!(report.honest_earned_sat, 0);
    assert!(report.prepaid_value_is_conserved(), "{report:?}");
}

#[test]
fn successful_fixed_prepay_retains_unused_service_credit() {
    let records = vec![record("a", 0, 1, 87, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        fixed_prepay_sat: 4_096,
        provider_failure_basis_points: 0,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_fixed_prepay(&workload, &roles, &config);

    assert_eq!(report.fixed_prepay_paid_sat, 4_096);
    assert_eq!(report.fixed_prepay_used_sat, 87);
    assert_eq!(report.fixed_prepay_unused_credit_sat, 4_009);
    assert_eq!(report.buyer_counterparty_loss_sat, 0);
    assert!(report.prepaid_value_is_conserved(), "{report:?}");
}

#[test]
fn adversarial_streaming_provider_fails_even_when_honest_failure_rate_is_zero() {
    let records = vec![record("a", 0, 1, 87, 1)];
    let roles = vec![NodeRole::Attacker, NodeRole::Peer];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        fixed_prepay_sat: 4_096,
        provider_failure_basis_points: 0,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_fixed_prepay(&workload, &roles, &config);

    assert_eq!(report.buyer_counterparty_loss_sat, 4_096);
    assert!(report.prepaid_value_is_conserved(), "{report:?}");
}

#[test]
fn cooperative_spilman_close_never_enters_refund_failure_path() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        spilman_prefund_sat: 1_000,
        spilman_channel_lifetime_ms: 60_000,
        spilman_refund_failure_basis_points: 10_000,
        provider_failure_basis_points: 0,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_spilman(&workload, &roles, &config);

    assert_eq!(report.channel_unused_capacity_sat, 900);
    assert_eq!(report.refund_failures, 0);
    assert_eq!(report.refund_loss_sat, 0);
    assert_eq!(report.channel_refunded_sat, 900);
    assert_eq!(report.locked_capital_sat_ms, 1_000);
    assert!(report.channel_value_is_conserved(), "{report:?}");
}

#[test]
fn uncooperative_spilman_provider_uses_full_timelock_and_refunds_unused_value() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        spilman_prefund_sat: 1_000,
        spilman_channel_lifetime_ms: 60_000,
        provider_failure_basis_points: 10_000,
        spilman_refund_failure_basis_points: 0,
        outage_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let report = plan_spilman(&workload, &roles, &config);

    assert_eq!(report.locked_capital_sat_ms, 1_000 * 60_000);
    assert_eq!(report.channel_refunded_sat, 900);
    assert_eq!(report.refund_failures, 0);
    assert_eq!(report.honest_settled_by_deadline_sat, 100);
    assert_eq!(report.pending_settlement_sat, 0);
    assert!(report.channel_value_is_conserved(), "{report:?}");
}

#[test]
fn recommendation_rejects_reports_modeled_for_a_different_use_case() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        outage_basis_points: 0,
        payment_defaulter_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let reports = [plan_direct(&workload, &roles, &config)];

    assert!(recommend_incentive_strategy(&reports, IncentiveUseCase::UntrustedStreaming).is_none());
}

#[test]
fn streaming_recommendation_compares_fees_before_wire_bytes() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let config = IncentiveConfig {
        bytes_per_sat: 1,
        use_case: IncentiveUseCase::UntrustedStreaming,
        outage_basis_points: 0,
        payment_defaulter_basis_points: 0,
        ..IncentiveConfig::default()
    };
    let workload = price_records(&records, config.bytes_per_sat);
    let mut expensive = plan_direct(&workload, &roles, &config);
    expensive.modeled_fees_sat = 1_000;
    expensive.payment_bytes = 1;
    let mut efficient = expensive.clone();
    efficient.modeled_fees_sat = 1;
    efficient.payment_bytes = 10_000;
    let reports = [expensive, efficient];

    let selected = recommend_incentive_strategy(&reports, IncentiveUseCase::UntrustedStreaming)
        .expect("both reports meet the settlement gate");
    assert_eq!(selected.modeled_fees_sat, 1);
    assert_eq!(selected.payment_bytes, 10_000);
}

#[test]
fn failed_spilman_close_locks_capacity_until_refund_timelock() {
    let records = vec![record("a", 0, 1, 100, 1)];
    let roles = vec![NodeRole::Peer; 2];
    let workload = price_records(&records, 1);
    let report = (0..10_000)
        .find_map(|seed| {
            let config = IncentiveConfig {
                bytes_per_sat: 1,
                use_case: IncentiveUseCase::UntrustedStreaming,
                spilman_prefund_sat: 1_000,
                spilman_channel_lifetime_ms: 60_000,
                provider_failure_basis_points: 0,
                spilman_refund_failure_basis_points: 0,
                outage_basis_points: 5_000,
                max_payment_retries: 0,
                seed,
                ..IncentiveConfig::default()
            };
            let report = plan_spilman(&workload, &roles, &config);
            (report.channel_capacity_sat > 0 && report.locked_capital_sat_ms == 1_000 * 60_000)
                .then_some(report)
        })
        .expect("at least one deterministic seed opens but cannot cooperatively close");

    assert_eq!(report.locked_capital_sat_ms, 1_000 * 60_000);
}
