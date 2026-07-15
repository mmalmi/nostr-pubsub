use nostr_pubsub_sim::{
    IncentiveConfig, IncentiveReport, IncentiveStrategy, IncentiveUseCase, SimulationReport,
    compare_incentive_strategies, recommend_incentive_strategy,
};

pub const INCENTIVE_CSV_COLUMNS: &[&str] = &[
    "one_shot_incentive_strategy",
    "one_shot_settlement_bps",
    "one_shot_payment_overhead_bps",
    "one_shot_buyer_prepaid_exposure_sat",
    "one_shot_buyer_counterparty_loss_sat",
    "one_shot_provider_unpaid_exposure_sat",
    "one_shot_provider_default_loss_sat",
    "one_shot_peer_credit_accepted_sat",
    "one_shot_peer_credit_outstanding_sat",
    "one_shot_max_pair_peer_credit_sat",
    "one_shot_locked_capital_sat_ms",
    "one_shot_peak_locked_capital_sat",
    "one_shot_channel_capacity_sat",
    "one_shot_channel_signed_balance_sat",
    "one_shot_channel_unused_capacity_sat",
    "one_shot_channel_refunded_sat",
    "one_shot_channel_value_conserved",
    "one_shot_channel_open_fees_sat",
    "one_shot_channel_close_fees_sat",
    "one_shot_refund_failures",
    "one_shot_refund_loss_sat",
    "streaming_incentive_strategy",
    "streaming_settlement_bps",
    "streaming_payment_overhead_bps",
    "streaming_buyer_prepaid_exposure_sat",
    "streaming_buyer_counterparty_loss_sat",
    "streaming_provider_unpaid_exposure_sat",
    "streaming_provider_default_loss_sat",
    "streaming_peer_credit_accepted_sat",
    "streaming_peer_credit_outstanding_sat",
    "streaming_max_pair_peer_credit_sat",
    "streaming_locked_capital_sat_ms",
    "streaming_peak_locked_capital_sat",
    "streaming_channel_capacity_sat",
    "streaming_channel_signed_balance_sat",
    "streaming_channel_unused_capacity_sat",
    "streaming_channel_refunded_sat",
    "streaming_channel_value_conserved",
    "streaming_channel_open_fees_sat",
    "streaming_channel_close_fees_sat",
    "streaming_refund_failures",
    "streaming_refund_loss_sat",
    "streaming_fixed_buyer_prepaid_exposure_sat",
    "streaming_fixed_buyer_counterparty_loss_sat",
    "streaming_fixed_prepay_paid_sat",
    "streaming_fixed_prepay_used_sat",
    "streaming_fixed_prepay_unused_credit_sat",
    "streaming_fixed_prepay_value_conserved",
];

pub fn incentive_values(simulation: &SimulationReport) -> Vec<String> {
    let one_shot_config = IncentiveConfig {
        bytes_per_sat: 128,
        batch_interval_ms: 10_000,
        cashu_min_settlement_sat: 10,
        offline_credit_cap_sat: 16,
        outage_basis_points: 1_000,
        payment_defaulter_basis_points: 300,
        ..IncentiveConfig::default()
    };
    let one_shot_reports = compare_incentive_strategies(simulation, &one_shot_config)
        .expect("simulation delivery trail must support one-shot incentive planning");
    let one_shot =
        recommend_incentive_strategy(&one_shot_reports, IncentiveUseCase::VerifiedOneShot)
            .expect("one-shot incentive strategy must pass its release gates");

    let streaming_config = IncentiveConfig {
        use_case: IncentiveUseCase::UntrustedStreaming,
        // One-sat cooperative increments deliberately trade wire bytes for a
        // one-sat provider grace bound. Capacity itself remains refundable.
        spilman_update_sat: 1,
        ..one_shot_config
    };
    let streaming_reports = compare_incentive_strategies(simulation, &streaming_config)
        .expect("simulation delivery trail must support streaming incentive planning");
    let streaming =
        recommend_incentive_strategy(&streaming_reports, IncentiveUseCase::UntrustedStreaming)
            .expect("streaming incentive strategy must meet its settlement goal");
    let fixed = streaming_reports
        .iter()
        .find(|report| report.strategy == IncentiveStrategy::FixedPrepaidCashu)
        .expect("fixed prepayment baseline must be present");

    let mut values = report_values(one_shot);
    values.extend(report_values(streaming));
    values.push(fixed.buyer_prepaid_exposure_sat.to_string());
    values.push(fixed.buyer_counterparty_loss_sat.to_string());
    values.push(fixed.fixed_prepay_paid_sat.to_string());
    values.push(fixed.fixed_prepay_used_sat.to_string());
    values.push(fixed.fixed_prepay_unused_credit_sat.to_string());
    values.push(fixed.prepaid_value_is_conserved().to_string());
    values
}

fn report_values(report: &IncentiveReport) -> Vec<String> {
    vec![
        report.strategy_name.to_string(),
        report
            .honest_earned_settled_by_deadline_basis_points
            .to_string(),
        report.payment_byte_overhead_basis_points.to_string(),
        report.buyer_prepaid_exposure_sat.to_string(),
        report.buyer_counterparty_loss_sat.to_string(),
        report.provider_unpaid_exposure_sat.to_string(),
        report.provider_default_loss_sat.to_string(),
        report.peer_credit_accepted_sat.to_string(),
        report.peer_credit_outstanding_sat.to_string(),
        report.max_pair_peer_credit_sat.to_string(),
        report.locked_capital_sat_ms.to_string(),
        report.peak_locked_capital_sat.to_string(),
        report.channel_capacity_sat.to_string(),
        report.channel_signed_balance_sat.to_string(),
        report.channel_unused_capacity_sat.to_string(),
        report.channel_refunded_sat.to_string(),
        report.channel_value_is_conserved().to_string(),
        report.channel_open_fees_sat.to_string(),
        report.channel_close_fees_sat.to_string(),
        report.refund_failures.to_string(),
        report.refund_loss_sat.to_string(),
    ]
}
