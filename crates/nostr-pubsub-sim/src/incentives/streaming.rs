//! Fixed prepayment and incremental channel strategy models.

use crate::topology::NodeRole;

use super::{
    IncentiveConfig, IncentiveReport, IncentiveStrategy, Plan, PricedWorkload, pair_streams,
};

pub(super) fn plan_fixed_prepay(
    workload: &PricedWorkload,
    roles: &[NodeRole],
    config: &IncentiveConfig,
) -> IncentiveReport {
    let mut plan = Plan::new(
        IncentiveStrategy::FixedPrepaidCashu,
        workload,
        roles,
        config,
    );
    for stream in pair_streams(&workload.obligations) {
        plan.record_state(stream.pair);
        if plan.payer_will_default(stream.pair) {
            plan.reject_unfunded(stream.pair, stream.amount_sat);
            continue;
        }
        if !plan.try_settlement(stream.pair, stream.first_at_ms, 6) {
            plan.deny(stream.amount_sat);
            continue;
        }
        let admitted = stream.amount_sat.min(config.fixed_prepay_sat);
        let provider_fails = plan.provider_will_fail(stream.pair);
        plan.record_fixed_prepay(config.fixed_prepay_sat, admitted, provider_fails);
        plan.deny(stream.amount_sat.saturating_sub(admitted));
        plan.record_route_settlement(stream.pair);
        if !provider_fails {
            plan.earn(stream.pair, admitted);
            plan.mark_settled(stream.pair, admitted);
        }
    }
    plan.finish()
}

pub(super) fn plan_spilman(
    workload: &PricedWorkload,
    roles: &[NodeRole],
    config: &IncentiveConfig,
) -> IncentiveReport {
    let mut plan = Plan::new(
        IncentiveStrategy::IncrementalSpilman,
        workload,
        roles,
        config,
    );
    for stream in pair_streams(&workload.obligations) {
        plan.record_state(stream.pair);
        if !plan.try_settlement(stream.pair, stream.first_at_ms, 3) {
            plan.deny(stream.amount_sat);
            continue;
        }

        // Capacity is locked in the 2-of-2 channel. The seller can claim only
        // the latest increment signed for metered useful service.
        let capacity_sat = config.spilman_prefund_sat;
        if plan.payer_will_default(stream.pair) {
            // A strategic payer can fund a real channel, receive at most one
            // cooperative update of service, then withhold the next signature.
            let unpaid_grace_sat = stream
                .amount_sat
                .min(config.spilman_update_sat)
                .min(capacity_sat);
            plan.earn(stream.pair, unpaid_grace_sat);
            plan.record_provider_exposure(unpaid_grace_sat, true);
            plan.record_unpaid(stream.pair, unpaid_grace_sat);
            plan.deny(stream.amount_sat.saturating_sub(unpaid_grace_sat));
            plan.record_route_settlement(stream.pair);
            plan.record_channel_open(capacity_sat, 0, config.spilman_channel_lifetime_ms);
            plan.record_messages(stream.pair, 1);
            plan.record_channel_close();
            if plan.refund_will_fail(stream.pair) {
                plan.record_refund_failure(capacity_sat);
            } else {
                plan.record_channel_refund(capacity_sat);
            }
            continue;
        }
        let signed_balance_sat = stream.amount_sat.min(capacity_sat);
        let unused_capacity_sat = capacity_sat - signed_balance_sat;
        assert_eq!(capacity_sat, signed_balance_sat + unused_capacity_sat);
        plan.deny(stream.amount_sat.saturating_sub(signed_balance_sat));
        plan.earn(stream.pair, signed_balance_sat);
        // The latest increment is seller-claimable without a later buyer
        // signature. Cooperative close only shortens the buyer's liquidity
        // lock; it does not decide whether this service is secured.
        plan.mark_settled(stream.pair, signed_balance_sat);
        plan.record_route_settlement(stream.pair);
        let provider_fails = plan.provider_will_fail(stream.pair);
        let observed_lifetime_ms = stream.last_at_ms.saturating_sub(stream.first_at_ms).max(1);
        let closed = if provider_fails {
            plan.record_messages(stream.pair, 1);
            false
        } else {
            plan.try_settlement(stream.pair, stream.last_at_ms, 4)
        };
        let recovery_needed = provider_fails || !closed;
        let lock_lifetime_ms = if recovery_needed {
            config.spilman_channel_lifetime_ms
        } else {
            observed_lifetime_ms.min(config.spilman_channel_lifetime_ms)
        };
        plan.record_channel_open(capacity_sat, signed_balance_sat, lock_lifetime_ms);
        plan.record_provider_exposure(signed_balance_sat.min(config.spilman_update_sat), false);
        plan.record_messages(
            stream.pair,
            signed_balance_sat.div_ceil(config.spilman_update_sat),
        );

        plan.record_channel_close();
        if recovery_needed && plan.refund_will_fail(stream.pair) {
            plan.record_refund_failure(unused_capacity_sat);
        } else {
            plan.record_channel_refund(unused_capacity_sat);
        }
    }
    plan.finish()
}
