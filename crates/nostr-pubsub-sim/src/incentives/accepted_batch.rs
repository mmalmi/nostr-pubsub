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
    let mut peer_credit = BTreeMap::<Pair, u64>::new();
    for batch in batches(&workload.obligations, config.batch_interval_ms) {
        plan.record_state(batch.pair);
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
            let low_pair = Pair {
                payer: low,
                provider: high,
            };
            let high_pair = Pair {
                payer: high,
                provider: low,
            };
            plan.earn(low_pair, offset);
            plan.earn(high_pair, offset);
            plan.mark_settled(low_pair, offset);
            plan.mark_settled(high_pair, offset);
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
        settle_net(&mut plan, pair, net_sat, due_ms, &mut peer_credit);
    }
    for (pair, amount_sat) in peer_credit {
        plan.report.peer_credit_outstanding_sat = plan
            .report
            .peer_credit_outstanding_sat
            .saturating_add(amount_sat);
        let outstanding_sat = plan.record_unpaid(pair, amount_sat);
        plan.record_provider_exposure(outstanding_sat, plan.payer_will_default(pair));
    }
    plan.finish()
}

fn settle_net(
    plan: &mut Plan<'_>,
    pair: Pair,
    mut amount_sat: u64,
    due_ms: u64,
    peer_credit: &mut BTreeMap<Pair, u64>,
) {
    if amount_sat == 0 {
        return;
    }
    let reverse = Pair {
        payer: pair.provider,
        provider: pair.payer,
    };
    let reverse_sat = peer_credit.remove(&reverse).unwrap_or_default();
    let reciprocal_sat = amount_sat.min(reverse_sat);
    if reciprocal_sat > 0 {
        plan.earn(pair, reciprocal_sat);
        plan.mark_settled(pair, reciprocal_sat);
        if plan.payer_will_default(reverse) {
            plan.mark_settled(reverse, reciprocal_sat);
        }
        plan.report.reciprocal_service_settled_sat = plan
            .report
            .reciprocal_service_settled_sat
            .saturating_add(reciprocal_sat.saturating_mul(2));
        amount_sat -= reciprocal_sat;
    }
    let reverse_remaining_sat = reverse_sat - reciprocal_sat;
    if reverse_remaining_sat > 0 {
        peer_credit.insert(reverse, reverse_remaining_sat);
    }
    if amount_sat == 0 {
        return;
    }
    let existing_sat = peer_credit.get(&pair).copied().unwrap_or_default();
    let combined_sat = existing_sat.saturating_add(amount_sat);
    let requires_cashu = combined_sat >= plan.config.cashu_min_settlement_sat
        || combined_sat >= plan.config.offline_credit_cap_sat;
    if requires_cashu && !plan.payer_will_default(pair) && plan.try_settlement(pair, due_ms, 5) {
        peer_credit.remove(&pair);
        plan.record_route_settlement(pair);
        plan.earn(pair, amount_sat);
        plan.mark_settled(pair, amount_sat);
        return;
    }
    if requires_cashu && plan.payer_will_default(pair) {
        plan.record_mint_discovery_pair(pair);
        plan.report.settlement_attempts = plan.report.settlement_attempts.saturating_add(1);
        plan.record_messages(pair, 1);
    }
    accept_peer_credit(plan, pair, amount_sat, peer_credit);
}

fn accept_peer_credit(
    plan: &mut Plan<'_>,
    pair: Pair,
    amount_sat: u64,
    peer_credit: &mut BTreeMap<Pair, u64>,
) {
    let balance = peer_credit.entry(pair).or_default();
    let admitted_sat = amount_sat.min(plan.config.offline_credit_cap_sat.saturating_sub(*balance));
    *balance = balance.saturating_add(admitted_sat);
    plan.report.peer_credit_accepted_sat = plan
        .report
        .peer_credit_accepted_sat
        .saturating_add(admitted_sat);
    plan.report.max_pair_peer_credit_sat = plan.report.max_pair_peer_credit_sat.max(*balance);
    plan.record_provider_exposure(*balance, false);
    plan.earn(pair, admitted_sat);
    if !plan.payer_will_default(pair) {
        plan.mark_settled(pair, admitted_sat);
    }
    plan.deny(amount_sat.saturating_sub(admitted_sat));
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

    #[test]
    fn sub_threshold_pair_value_carries_into_the_next_batch() {
        let records = vec![
            timed_record("a", 0, 1, 4, 1),
            timed_record("b", 0, 1, 4, 11),
        ];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            batch_interval_ms: 10,
            cashu_min_settlement_sat: 5,
            outage_basis_points: 0,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.honest_earned_sat, 8);
        assert_eq!(report.honest_settled_by_deadline_sat, 8);
        assert_eq!(report.pending_settlement_sat, 0);
        assert_eq!(report.peer_credit_accepted_sat, 4);
        assert_eq!(report.peer_credit_outstanding_sat, 0);
        assert_eq!(
            report.same_mint_settlements + report.cross_mint_settlements,
            1
        );
    }

    #[test]
    fn repeated_defaulted_batches_accumulate_pair_exposure() {
        let records = vec![
            timed_record("a", 0, 1, 4, 1),
            timed_record("b", 0, 1, 4, 11),
        ];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            batch_interval_ms: 10,
            offline_credit_cap_sat: 5,
            payment_defaulter_basis_points: 10_000,
            outage_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.honest_earned_sat, 5);
        assert_eq!(report.authorization_denied_sat, 3);
        assert_eq!(report.peer_credit_accepted_sat, 5);
        assert_eq!(report.peer_credit_outstanding_sat, 5);
        assert_eq!(report.max_pair_peer_credit_sat, 5);
        assert_eq!(report.provider_unpaid_exposure_sat, 5);
        assert_eq!(report.max_pair_unpaid_exposure_sat, 5);
        assert_eq!(report.provider_default_loss_sat, 5);
        assert_eq!(report.honest_earned_settled_by_deadline_basis_points, 0);
    }

    #[test]
    fn opposite_sub_threshold_batches_net_across_windows() {
        let records = vec![
            timed_record("a", 0, 1, 4, 1),
            timed_record("b", 1, 0, 4, 11),
        ];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            batch_interval_ms: 10,
            cashu_min_settlement_sat: 5,
            outage_basis_points: 0,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.honest_earned_sat, 8);
        assert_eq!(report.honest_settled_by_deadline_sat, 8);
        assert_eq!(report.reciprocal_service_settled_sat, 8);
        assert_eq!(report.pending_settlement_sat, 0);
        assert_eq!(report.unpaid_exposure_sat, 0);
        assert_eq!(report.peer_credit_accepted_sat, 4);
        assert_eq!(report.peer_credit_outstanding_sat, 0);
        assert_eq!(report.same_mint_settlements, 0);
        assert_eq!(report.cross_mint_settlements, 0);
    }

    #[test]
    fn residual_dust_is_truthful_bounded_credit_not_a_cashu_settlement() {
        let records = vec![timed_record("dust", 0, 1, 9, 1)];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            cashu_min_settlement_sat: 10,
            offline_credit_cap_sat: 16,
            outage_basis_points: 0,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.honest_earned_sat, 9);
        assert_eq!(report.honest_settled_by_deadline_sat, 9);
        assert_eq!(report.peer_credit_accepted_sat, 9);
        assert_eq!(report.peer_credit_outstanding_sat, 9);
        assert_eq!(report.max_pair_peer_credit_sat, 9);
        assert_eq!(report.unpaid_exposure_sat, 9);
        assert_eq!(report.same_mint_settlements, 0);
        assert_eq!(report.cross_mint_settlements, 0);
    }

    #[test]
    fn reaching_credit_cap_requires_one_cashu_settlement() {
        let records = vec![
            timed_record("first", 0, 1, 8, 1),
            timed_record("second", 0, 1, 8, 11),
        ];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            batch_interval_ms: 10,
            cashu_min_settlement_sat: 100,
            offline_credit_cap_sat: 16,
            outage_basis_points: 0,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.honest_settled_by_deadline_sat, 16);
        assert_eq!(report.peer_credit_accepted_sat, 8);
        assert_eq!(report.peer_credit_outstanding_sat, 0);
        assert_eq!(report.max_pair_peer_credit_sat, 8);
        assert_eq!(report.unpaid_exposure_sat, 0);
        assert_eq!(
            report.same_mint_settlements + report.cross_mint_settlements,
            1
        );
    }

    #[test]
    fn cashu_outage_never_extends_credit_past_the_cap() {
        let records = vec![
            timed_record("first", 0, 1, 12, 1),
            timed_record("second", 0, 1, 12, 11),
        ];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            batch_interval_ms: 10,
            cashu_min_settlement_sat: 100,
            offline_credit_cap_sat: 16,
            outage_basis_points: 10_000,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.peer_credit_accepted_sat, 16);
        assert_eq!(report.peer_credit_outstanding_sat, 16);
        assert_eq!(report.max_pair_peer_credit_sat, 16);
        assert_eq!(report.authorization_denied_sat, 8);
        assert!(report.settlement_attempts > 0);
        assert_eq!(report.same_mint_settlements, 0);
        assert_eq!(report.cross_mint_settlements, 0);
    }

    #[test]
    fn zero_credit_cap_requires_cashu_from_the_first_sat() {
        let records = vec![timed_record("cashu-only", 0, 1, 1, 1)];
        let config = IncentiveConfig {
            bytes_per_sat: 1,
            cashu_min_settlement_sat: 100,
            offline_credit_cap_sat: 0,
            outage_basis_points: 0,
            payment_defaulter_basis_points: 0,
            fake_claims_per_attacker: 0,
            ..IncentiveConfig::default()
        };
        let workload = super::super::price_records(&records, config.bytes_per_sat);
        let report = plan_accepted_mint_batch(&workload, &[NodeRole::Peer; 2], &config);

        assert_eq!(report.honest_earned_sat, 1);
        assert_eq!(report.honest_settled_by_deadline_sat, 1);
        assert_eq!(report.peer_credit_accepted_sat, 0);
        assert_eq!(report.peer_credit_outstanding_sat, 0);
        assert_eq!(
            report.same_mint_settlements + report.cross_mint_settlements,
            1
        );
    }

    fn record(event_id: &str, provider: usize, receiver: usize) -> VerifiedDeliveryRecord {
        timed_record(event_id, provider, receiver, 100, 1)
    }

    fn timed_record(
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
}
