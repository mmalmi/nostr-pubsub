//! Counterparty-risk accounting and workload-aware strategy selection.

use crate::topology::NodeRole;

use super::{IncentiveReport, IncentiveStrategy, Pair, Plan, mix64, usize_u64};

/// How useful service and payment can be ordered safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncentiveUseCase {
    /// A small result can be verified before its Cashu settlement is released.
    VerifiedOneShot,
    /// Service is continuous, so fixed-token prepayment trusts an unknown seller.
    UntrustedStreaming,
}

/// Select a strategy using the security properties of the service workload.
///
/// One-shot work retains the global payment-byte gate. Streaming work instead
/// minimizes the worse of buyer prepayment and provider grace exposure, then
/// loss and wire cost. It may therefore choose a channel over the global byte
/// preference: locked capacity is liquidity cost, not seller-claimable value.
#[must_use]
pub fn recommend_incentive_strategy(
    reports: &[IncentiveReport],
    use_case: IncentiveUseCase,
) -> Option<&IncentiveReport> {
    match use_case {
        IncentiveUseCase::VerifiedOneShot => reports
            .iter()
            .filter(|report| {
                report.use_case == use_case
                    && report.meets_strategy_selection_gate()
                    && is_one_shot_strategy(report.strategy)
            })
            .min_by_key(|report| one_shot_score(report))
            .or_else(|| {
                // Small smoke scenarios may not reach a production-scale gate.
                // Keep the semantic choice observable without claiming it passed.
                reports
                    .iter()
                    .filter(|report| {
                        report.use_case == use_case && is_one_shot_strategy(report.strategy)
                    })
                    .min_by_key(|report| {
                        (
                            !report.meets_settlement_goal(),
                            !report.meets_payment_overhead_goal(),
                            one_shot_score(report),
                        )
                    })
            }),
        IncentiveUseCase::UntrustedStreaming => reports
            .iter()
            .filter(|report| report.use_case == use_case && report.meets_settlement_goal())
            .min_by_key(|report| {
                (
                    report
                        .buyer_prepaid_exposure_sat
                        .max(report.provider_unpaid_exposure_sat),
                    counterparty_loss(report),
                    report.locked_capital_sat_ms,
                    total_fees(report),
                    report.payment_bytes,
                )
            }),
    }
}

fn is_one_shot_strategy(strategy: IncentiveStrategy) -> bool {
    matches!(
        strategy,
        IncentiveStrategy::DirectVerifiedCashu
            | IncentiveStrategy::OfflinePeerCredit
            | IncentiveStrategy::AcceptedMintBatch
    )
}

fn one_shot_score(report: &IncentiveReport) -> (u64, u64, u64, u64) {
    (
        report.authorization_denied_sat,
        report.unpaid_exposure_sat,
        report.payment_bytes,
        counterparty_loss(report),
    )
}

fn counterparty_loss(report: &IncentiveReport) -> u64 {
    report
        .buyer_counterparty_loss_sat
        .saturating_add(report.provider_default_loss_sat)
        .saturating_add(report.refund_loss_sat)
}

fn total_fees(report: &IncentiveReport) -> u64 {
    report
        .modeled_fees_sat
        .saturating_add(report.channel_open_fees_sat)
        .saturating_add(report.channel_close_fees_sat)
}

impl Plan<'_> {
    pub(super) fn provider_will_fail(&self, pair: Pair) -> bool {
        if self.config.use_case != IncentiveUseCase::UntrustedStreaming {
            return false;
        }
        if self.roles[pair.provider] == NodeRole::Attacker {
            return true;
        }
        let sample = mix64(
            self.config.seed
                ^ usize_u64(pair.provider).rotate_left(23)
                ^ usize_u64(pair.payer).rotate_left(41)
                ^ 0x5345_4c4c_4552_4641,
        ) % 10_000;
        sample < u64::from(self.config.provider_failure_basis_points)
    }

    pub(super) fn refund_will_fail(&self, pair: Pair) -> bool {
        let sample = mix64(
            self.config.seed
                ^ usize_u64(pair.provider).rotate_left(13)
                ^ usize_u64(pair.payer).rotate_left(37)
                ^ 0x5245_4655_4e44_4641,
        ) % 10_000;
        sample < u64::from(self.config.spilman_refund_failure_basis_points)
    }

    pub(super) fn record_fixed_prepay(&mut self, prepaid_sat: u64, used_sat: u64, lost: bool) {
        assert!(used_sat <= prepaid_sat);
        self.report.buyer_prepaid_exposure_sat =
            self.report.buyer_prepaid_exposure_sat.max(prepaid_sat);
        self.report.fixed_prepay_paid_sat = self
            .report
            .fixed_prepay_paid_sat
            .saturating_add(prepaid_sat);
        if lost {
            self.report.buyer_counterparty_loss_sat = self
                .report
                .buyer_counterparty_loss_sat
                .saturating_add(prepaid_sat);
        } else {
            self.report.fixed_prepay_used_sat =
                self.report.fixed_prepay_used_sat.saturating_add(used_sat);
            self.report.fixed_prepay_unused_credit_sat = self
                .report
                .fixed_prepay_unused_credit_sat
                .saturating_add(prepaid_sat - used_sat);
        }
    }

    pub(super) fn record_provider_exposure(&mut self, amount_sat: u64, lost: bool) {
        self.report.provider_unpaid_exposure_sat =
            self.report.provider_unpaid_exposure_sat.max(amount_sat);
        if lost {
            self.report.provider_default_loss_sat = self
                .report
                .provider_default_loss_sat
                .saturating_add(amount_sat);
        }
    }

    pub(super) fn record_channel_open(
        &mut self,
        capacity_sat: u64,
        signed_balance_sat: u64,
        lifetime_ms: u64,
    ) {
        assert!(signed_balance_sat <= capacity_sat);
        let unused_capacity_sat = capacity_sat - signed_balance_sat;
        self.report.peak_locked_capital_sat = self.report.peak_locked_capital_sat.max(capacity_sat);
        self.report.channel_capacity_sat = self
            .report
            .channel_capacity_sat
            .saturating_add(capacity_sat);
        self.report.channel_signed_balance_sat = self
            .report
            .channel_signed_balance_sat
            .saturating_add(signed_balance_sat);
        self.report.channel_unused_capacity_sat = self
            .report
            .channel_unused_capacity_sat
            .saturating_add(unused_capacity_sat);
        self.report.locked_capital_sat_ms = self
            .report
            .locked_capital_sat_ms
            .saturating_add(capacity_sat.saturating_mul(lifetime_ms));
        self.report.channel_open_fees_sat = self
            .report
            .channel_open_fees_sat
            .saturating_add(self.config.spilman_open_fee_sat);
    }

    pub(super) fn record_channel_close(&mut self) {
        self.report.channel_close_fees_sat = self
            .report
            .channel_close_fees_sat
            .saturating_add(self.config.spilman_close_fee_sat);
    }

    pub(super) fn record_channel_refund(&mut self, amount_sat: u64) {
        self.report.channel_refunded_sat =
            self.report.channel_refunded_sat.saturating_add(amount_sat);
    }

    pub(super) fn record_refund_failure(&mut self, unused_capacity_sat: u64) {
        if unused_capacity_sat == 0 {
            return;
        }
        self.report.refund_failures = self.report.refund_failures.saturating_add(1);
        self.report.refund_loss_sat = self
            .report
            .refund_loss_sat
            .saturating_add(unused_capacity_sat);
    }
}

impl IncentiveReport {
    #[must_use]
    pub const fn prepaid_value_is_conserved(&self) -> bool {
        self.fixed_prepay_paid_sat
            == self
                .fixed_prepay_used_sat
                .saturating_add(self.fixed_prepay_unused_credit_sat)
                .saturating_add(self.buyer_counterparty_loss_sat)
    }

    #[must_use]
    pub const fn channel_value_is_conserved(&self) -> bool {
        self.channel_capacity_sat
            == self
                .channel_signed_balance_sat
                .saturating_add(self.channel_unused_capacity_sat)
            && self.channel_unused_capacity_sat
                == self
                    .channel_refunded_sat
                    .saturating_add(self.refund_loss_sat)
    }
}
