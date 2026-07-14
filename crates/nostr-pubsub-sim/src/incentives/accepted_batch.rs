use std::collections::BTreeMap;

use crate::topology::NodeRole;

use super::{
    IncentiveConfig, IncentiveReport, IncentiveStrategy, Pair, Plan, PricedWorkload, batches,
};

#[derive(Default)]
struct ReciprocalTotals {
    low_to_high_sat: u64,
    high_to_low_sat: u64,
}

pub(super) fn plan_accepted_mint_batch(
    workload: &PricedWorkload,
    roles: &[NodeRole],
    config: &IncentiveConfig,
) -> IncentiveReport {
    let mut plan = Plan::new(
        IncentiveStrategy::AcceptedMintBatch,
        workload,
        roles,
        config,
    );
    let mut reciprocal = BTreeMap::<(u64, usize, usize), ReciprocalTotals>::new();
    for batch in batches(&workload.obligations, config.batch_interval_ms) {
        plan.record_state(batch.pair);
        if plan.payer_will_default(batch.pair) {
            plan.reject_unfunded(batch.pair, batch.amount_sat);
            continue;
        }
        plan.earn(batch.pair, batch.amount_sat);
        let low = batch.pair.payer.min(batch.pair.provider);
        let high = batch.pair.payer.max(batch.pair.provider);
        let totals = reciprocal.entry((batch.due_ms, low, high)).or_default();
        if batch.pair.payer == low {
            totals.low_to_high_sat = totals.low_to_high_sat.saturating_add(batch.amount_sat);
        } else {
            totals.high_to_low_sat = totals.high_to_low_sat.saturating_add(batch.amount_sat);
        }
    }

    for ((due_ms, low, high), totals) in reciprocal {
        let offset = totals.low_to_high_sat.min(totals.high_to_low_sat);
        if offset > 0 {
            plan.mark_settled(
                Pair {
                    payer: low,
                    provider: high,
                },
                offset,
            );
            plan.mark_settled(
                Pair {
                    payer: high,
                    provider: low,
                },
                offset,
            );
            plan.report.reciprocal_service_settled_sat = plan
                .report
                .reciprocal_service_settled_sat
                .saturating_add(offset.saturating_mul(2));
        }
        let (pair, net_sat) = if totals.low_to_high_sat >= totals.high_to_low_sat {
            (
                Pair {
                    payer: low,
                    provider: high,
                },
                totals.low_to_high_sat - totals.high_to_low_sat,
            )
        } else {
            (
                Pair {
                    payer: high,
                    provider: low,
                },
                totals.high_to_low_sat - totals.low_to_high_sat,
            )
        };
        settle_net(&mut plan, pair, net_sat, due_ms);
    }
    plan.finish()
}

fn settle_net(plan: &mut Plan<'_>, pair: Pair, amount_sat: u64, due_ms: u64) {
    if amount_sat == 0 {
        return;
    }
    if amount_sat < plan.config.cashu_min_settlement_sat {
        plan.report.pending_settlement_sat = plan
            .report
            .pending_settlement_sat
            .saturating_add(amount_sat);
    } else if plan.try_settlement(pair, due_ms, 5) {
        plan.settle_with_route(pair, amount_sat);
    } else {
        plan.report.pending_settlement_sat = plan
            .report
            .pending_settlement_sat
            .saturating_add(amount_sat);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VerifiedDeliveryRecord;

    #[test]
    fn reciprocal_service_settles_without_external_payment() {
        let records = vec![record("a", 0, 1), record("b", 1, 0)];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            outage_basis_points: 0,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);
        assert_eq!(report.honest_earned_sat, 200);
        assert_eq!(report.honest_settled_by_deadline_sat, 200);
        assert_eq!(report.reciprocal_service_settled_sat, 200);
        assert_eq!(report.same_mint_settlements, 0);
        assert_eq!(report.cross_mint_settlements, 0);
    }

    fn record(event_id: &str, provider: usize, receiver: usize) -> VerifiedDeliveryRecord {
        VerifiedDeliveryRecord {
            event_id: event_id.to_string(),
            provider,
            receiver,
            payload_bytes: 100,
            accepted_at_ms: 1,
            final_interested_delivery: true,
        }
    }
}
