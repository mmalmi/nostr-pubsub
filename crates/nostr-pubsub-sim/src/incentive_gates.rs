use crate::incentives::IncentiveReport;

pub const MAX_RECOMMENDED_PAYMENT_OVERHEAD_BASIS_POINTS: u32 = 500;
pub const MIN_RECOMMENDED_SETTLEMENT_BASIS_POINTS: u32 = 9_500;

impl IncentiveReport {
    #[must_use]
    pub const fn meets_settlement_goal(&self) -> bool {
        self.honest_earned_settled_by_deadline_basis_points
            >= MIN_RECOMMENDED_SETTLEMENT_BASIS_POINTS
    }

    #[must_use]
    pub const fn meets_payment_overhead_goal(&self) -> bool {
        self.payment_byte_overhead_basis_points <= MAX_RECOMMENDED_PAYMENT_OVERHEAD_BASIS_POINTS
    }

    #[must_use]
    pub const fn meets_strategy_selection_gate(&self) -> bool {
        self.meets_settlement_goal() && self.meets_payment_overhead_goal()
    }
}
