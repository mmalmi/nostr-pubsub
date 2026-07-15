//! Deterministic incentive planning over verified pubsub delivery edges.
//!
//! Each record is treated as an application-authorized pull: its receiver pays
//! its immediate provider. This models the local subcontract chain behind an
//! application/FSP service authorization; it never prices raw FMP traffic.
//! Cashu fees, mint conversion, message sizes, outages, retained state, and
//! Spilman updates below are deliberately modeled figures. They are not CDK or
//! cdk-spilman executor measurements and must be replaced or calibrated by the
//! real optional settlement executor before production capacity decisions.

use std::collections::{BTreeMap, BTreeSet};

use crate::metrics::{DistributionSummary, basis_points, summarize_distribution};
use crate::simulation::{SimulationReport, VerifiedDeliveryRecord};
use crate::topology::NodeRole;

mod accepted_batch;
use accepted_batch::plan_accepted_mint_batch;
mod exposure;
pub use exposure::{IncentiveUseCase, recommend_incentive_strategy};
mod streaming;
use streaming::{plan_fixed_prepay, plan_spilman};

const ATTEMPT_MESSAGES: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncentiveStrategy {
    DirectVerifiedCashu,
    OfflinePeerCredit,
    FixedPrepaidCashu,
    IncrementalSpilman,
    AcceptedMintBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MintDiscoveryScope {
    /// Piggyback accepted mints and local limits on an active peer handshake.
    ConnectedPeers,
    /// Broadcast the same capability to every simulated node.
    Public,
}

impl IncentiveStrategy {
    pub const ALL: [Self; 5] = [
        Self::DirectVerifiedCashu,
        Self::OfflinePeerCredit,
        Self::FixedPrepaidCashu,
        Self::IncrementalSpilman,
        Self::AcceptedMintBatch,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectVerifiedCashu => "modeled-direct-verified-cashu",
            Self::OfflinePeerCredit => "modeled-offline-peer-credit",
            Self::FixedPrepaidCashu => "modeled-fixed-prepaid-cashu",
            Self::IncrementalSpilman => "modeled-incremental-spilman",
            Self::AcceptedMintBatch => "modeled-accepted-mint-cashu-batch",
        }
    }

    const fn domain(self) -> u64 {
        match self {
            Self::DirectVerifiedCashu => 1,
            Self::OfflinePeerCredit => 2,
            Self::FixedPrepaidCashu => 3,
            Self::IncrementalSpilman => 4,
            Self::AcceptedMintBatch => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncentiveConfig {
    pub use_case: IncentiveUseCase,
    pub bytes_per_sat: u64,
    pub mint_count: usize,
    pub gateway_mint: Option<usize>,
    pub offline_credit_cap_sat: u64,
    pub batch_interval_ms: u64,
    /// Carry smaller pair balances forward instead of exchanging Cashu dust.
    pub cashu_min_settlement_sat: u64,
    pub fixed_prepay_sat: u64,
    /// Cashu value locked in each incremental channel, not paid to its seller.
    pub spilman_prefund_sat: u64,
    pub spilman_update_sat: u64,
    /// Maximum time capacity remains unavailable when expiry recovery is needed.
    pub spilman_channel_lifetime_ms: u64,
    pub spilman_open_fee_sat: u64,
    pub spilman_close_fee_sat: u64,
    /// Mint, storage, or implementation failure after the refund timelock.
    pub spilman_refund_failure_basis_points: u32,
    pub same_mint_fee_sat: u64,
    pub cross_mint_fee_sat: u64,
    pub payment_message_bytes: u64,
    pub mint_advertisement_bytes: u64,
    pub mint_discovery_scope: MintDiscoveryScope,
    pub outage_basis_points: u32,
    /// Nodes that carry legitimate traffic but deliberately refuse payment.
    /// This is independent of spammer roles so real delivery trails exercise
    /// economic defaults even when spam identities have no subscriptions.
    pub payment_defaulter_basis_points: u32,
    /// Unknown streaming sellers that stop serving after fixed prepayment.
    pub provider_failure_basis_points: u32,
    pub max_payment_retries: u8,
    pub payment_retry_ms: u64,
    pub settlement_deadline_ms: u64,
    pub fake_claims_per_attacker: u32,
    pub fake_claim_value_sat: u64,
    pub seed: u64,
}

impl Default for IncentiveConfig {
    fn default() -> Self {
        Self {
            use_case: IncentiveUseCase::VerifiedOneShot,
            bytes_per_sat: 512,
            mint_count: 4,
            gateway_mint: Some(0),
            offline_credit_cap_sat: 32,
            batch_interval_ms: 1_000,
            cashu_min_settlement_sat: 1,
            fixed_prepay_sat: 4_096,
            spilman_prefund_sat: 4_096,
            spilman_update_sat: 16,
            spilman_channel_lifetime_ms: 60_000,
            spilman_open_fee_sat: 1,
            spilman_close_fee_sat: 1,
            spilman_refund_failure_basis_points: 0,
            same_mint_fee_sat: 0,
            cross_mint_fee_sat: 2,
            payment_message_bytes: 96,
            mint_advertisement_bytes: 96,
            mint_discovery_scope: MintDiscoveryScope::ConnectedPeers,
            outage_basis_points: 800,
            payment_defaulter_basis_points: 500,
            provider_failure_basis_points: 500,
            max_payment_retries: 3,
            payment_retry_ms: 50,
            settlement_deadline_ms: 20_000,
            fake_claims_per_attacker: 2,
            fake_claim_value_sat: 16,
            seed: 0x5041_594d_454e_5453,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncentiveReport {
    pub strategy: IncentiveStrategy,
    pub strategy_name: &'static str,
    pub use_case: IncentiveUseCase,
    pub figures_are_modeled: bool,
    pub useful_bytes: u64,
    pub useful_value_sat: u64,
    pub pricing_dust_bytes: u64,
    pub priced_obligations: usize,
    pub adversarial_payer_count: u64,
    pub adversarial_payer_value_sat: u64,
    pub honest_earned_sat: u64,
    /// Service-level settlement, including accepted bounded peer credit;
    /// external Cashu settlements remain separately counted by mint route.
    pub honest_settled_by_deadline_sat: u64,
    pub honest_earned_settled_by_deadline_basis_points: u32,
    pub authorization_denied_sat: u64,
    /// Authorized/prefunded value not finalized by the modeled deadline.
    pub pending_settlement_sat: u64,
    /// Maximum fixed prepayment exposed to one unverified streaming seller.
    pub buyer_prepaid_exposure_sat: u64,
    /// Aggregate fixed-lease Cashu transferred before useful service.
    pub fixed_prepay_paid_sat: u64,
    /// Fixed-lease value matched by verified useful service.
    pub fixed_prepay_used_sat: u64,
    /// Unused prepaid service credit retained after an honest window.
    pub fixed_prepay_unused_credit_sat: u64,
    /// Fixed prepayment lost when its seller fails before useful service.
    pub buyer_counterparty_loss_sat: u64,
    /// Maximum useful service one provider supplied before secured payment.
    pub provider_unpaid_exposure_sat: u64,
    /// Useful service lost when a payer exhausted or abandoned that exposure.
    pub provider_default_loss_sat: u64,
    pub unpaid_exposure_sat: u64,
    pub default_exposure_sat: u64,
    pub max_pair_unpaid_exposure_sat: u64,
    /// Verified service accepted as bounded, unbacked relationship credit.
    pub peer_credit_accepted_sat: u64,
    /// Relationship credit still outstanding at the modeled deadline.
    pub peer_credit_outstanding_sat: u64,
    /// Peak relationship-credit balance for one directed pair.
    pub max_pair_peer_credit_sat: u64,
    pub same_mint_settlements: u64,
    pub cross_mint_settlements: u64,
    /// Value extinguished by reciprocal useful service before external payment.
    pub reciprocal_service_settled_sat: u64,
    pub modeled_fees_sat: u64,
    /// Capacity multiplied by modeled lock duration, in sat-milliseconds.
    pub locked_capital_sat_ms: u64,
    /// Largest individual channel capacity unavailable at one time.
    pub peak_locked_capital_sat: u64,
    pub channel_capacity_sat: u64,
    pub channel_signed_balance_sat: u64,
    pub channel_unused_capacity_sat: u64,
    /// Unused channel capacity returned by cooperative close or refund.
    pub channel_refunded_sat: u64,
    pub channel_open_fees_sat: u64,
    pub channel_close_fees_sat: u64,
    pub refund_failures: u64,
    pub refund_loss_sat: u64,
    /// Standalone advertisement messages; private handshake piggybacks add none.
    pub mint_advertisement_messages: u64,
    pub mint_advertisement_bytes: u64,
    /// Number of nodes that privately accept each configured mint.
    pub mint_accepting_nodes: Vec<u64>,
    pub settlement_attempts: u64,
    pub payment_retries: u64,
    pub payment_messages: u64,
    pub payment_bytes: u64,
    pub payment_byte_overhead_basis_points: u32,
    pub fake_claims_attempted: u64,
    pub fake_claimed_value_sat: u64,
    /// Modeled plan entries at honest nodes, not measured heap allocations.
    pub honest_node_payment_state_entries: DistributionSummary,
    /// Modeled payment messages processed by honest endpoints: a CPU proxy to
    /// calibrate against real CDK and cdk-spilman benchmark costs.
    pub honest_node_payment_messages: DistributionSummary,
    /// Combined send/receive bytes at honest endpoints. Unlike total wire bytes,
    /// adversarial endpoints are intentionally excluded.
    pub honest_node_payment_endpoint_bytes: DistributionSummary,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IncentiveError {
    #[error("invalid incentive configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid simulation delivery report: {0}")]
    InvalidReport(String),
}

pub fn compare_incentive_strategies(
    report: &SimulationReport,
    config: &IncentiveConfig,
) -> Result<Vec<IncentiveReport>, IncentiveError> {
    IncentiveStrategy::ALL
        .into_iter()
        .map(|strategy| plan_incentive_strategy(report, config, strategy))
        .collect()
}

pub fn plan_incentive_strategy(
    report: &SimulationReport,
    config: &IncentiveConfig,
    strategy: IncentiveStrategy,
) -> Result<IncentiveReport, IncentiveError> {
    validate_inputs(report, config)?;
    let workload = price_records(&report.verified_delivery_records, config.bytes_per_sat);
    Ok(match strategy {
        IncentiveStrategy::DirectVerifiedCashu => {
            plan_direct(&workload, &report.node_roles, config)
        }
        IncentiveStrategy::OfflinePeerCredit => {
            plan_peer_credit(&workload, &report.node_roles, config)
        }
        IncentiveStrategy::FixedPrepaidCashu => {
            plan_fixed_prepay(&workload, &report.node_roles, config)
        }
        IncentiveStrategy::IncrementalSpilman => {
            plan_spilman(&workload, &report.node_roles, config)
        }
        IncentiveStrategy::AcceptedMintBatch => {
            plan_accepted_mint_batch(&workload, &report.node_roles, config)
        }
    })
}

fn validate_inputs(
    report: &SimulationReport,
    config: &IncentiveConfig,
) -> Result<(), IncentiveError> {
    if config.bytes_per_sat == 0 {
        return Err(IncentiveError::InvalidConfig(
            "bytes_per_sat must be nonzero",
        ));
    }
    if config.mint_count == 0 {
        return Err(IncentiveError::InvalidConfig("mint_count must be nonzero"));
    }
    if config
        .gateway_mint
        .is_some_and(|mint| mint >= config.mint_count)
    {
        return Err(IncentiveError::InvalidConfig(
            "gateway mint is out of range",
        ));
    }
    if config.batch_interval_ms == 0
        || config.fixed_prepay_sat == 0
        || config.spilman_prefund_sat == 0
        || config.spilman_update_sat == 0
        || config.spilman_channel_lifetime_ms == 0
        || config.cashu_min_settlement_sat == 0
        || config.payment_message_bytes == 0
        || config.mint_advertisement_bytes == 0
    {
        return Err(IncentiveError::InvalidConfig(
            "batch, Spilman, and payment message sizes must be nonzero",
        ));
    }
    if config.outage_basis_points > 10_000
        || config.payment_defaulter_basis_points > 10_000
        || config.provider_failure_basis_points > 10_000
        || config.spilman_refund_failure_basis_points > 10_000
    {
        return Err(IncentiveError::InvalidConfig(
            "outage, default, provider-failure, or refund-failure basis points exceed 100%",
        ));
    }
    if report.node_roles.len() != report.node_count {
        return Err(IncentiveError::InvalidReport(
            "node role table length does not match node count".to_string(),
        ));
    }
    validate_records(&report.verified_delivery_records, report.node_count)
}

fn validate_records(
    records: &[VerifiedDeliveryRecord],
    node_count: usize,
) -> Result<(), IncentiveError> {
    let mut accepted = BTreeSet::new();
    for record in records {
        if record.provider >= node_count || record.receiver >= node_count {
            return Err(IncentiveError::InvalidReport(
                "delivery record node is out of range".to_string(),
            ));
        }
        if record.provider == record.receiver || record.payload_bytes == 0 {
            return Err(IncentiveError::InvalidReport(
                "delivery record has no remote useful service".to_string(),
            ));
        }
        if !accepted.insert((&record.event_id, record.receiver)) {
            return Err(IncentiveError::InvalidReport(
                "duplicate first-acceptance delivery record".to_string(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Pair {
    payer: usize,
    provider: usize,
}

#[derive(Debug, Clone)]
struct Obligation {
    pair: Pair,
    amount_sat: u64,
    accepted_at_ms: u64,
}

struct PricedWorkload {
    obligations: Vec<Obligation>,
    useful_bytes: u64,
    useful_value_sat: u64,
    pricing_dust_bytes: u64,
}

fn price_records(records: &[VerifiedDeliveryRecord], bytes_per_sat: u64) -> PricedWorkload {
    let mut ordered = records.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|record| {
        (
            record.accepted_at_ms,
            &record.event_id,
            record.receiver,
            record.provider,
        )
    });
    let mut remainders = BTreeMap::<Pair, u64>::new();
    let mut obligations = Vec::new();
    let mut useful_bytes = 0_u64;
    let mut useful_value_sat = 0_u64;
    for record in ordered {
        let pair = Pair {
            payer: record.receiver,
            provider: record.provider,
        };
        useful_bytes = useful_bytes.saturating_add(record.payload_bytes);
        let bytes = remainders
            .entry(pair)
            .or_default()
            .saturating_add(record.payload_bytes);
        let amount_sat = bytes / bytes_per_sat;
        *remainders.get_mut(&pair).expect("pair remainder exists") = bytes % bytes_per_sat;
        if amount_sat == 0 {
            continue;
        }
        useful_value_sat = useful_value_sat.saturating_add(amount_sat);
        obligations.push(Obligation {
            pair,
            amount_sat,
            accepted_at_ms: record.accepted_at_ms,
        });
    }
    PricedWorkload {
        obligations,
        useful_bytes,
        useful_value_sat,
        pricing_dust_bytes: remainders.values().copied().sum(),
    }
}

fn plan_direct(
    workload: &PricedWorkload,
    roles: &[NodeRole],
    config: &IncentiveConfig,
) -> IncentiveReport {
    let mut plan = Plan::new(
        IncentiveStrategy::DirectVerifiedCashu,
        workload,
        roles,
        config,
    );
    for obligation in &workload.obligations {
        plan.record_state(obligation.pair);
        plan.earn(obligation.pair, obligation.amount_sat);
        plan.record_provider_exposure(obligation.amount_sat, false);
        if plan.payer_will_default(obligation.pair) {
            plan.record_defaulted_settlement(obligation.pair, obligation.amount_sat);
        } else if plan.try_settlement(obligation.pair, obligation.accepted_at_ms, 1) {
            plan.settle_with_route(obligation.pair, obligation.amount_sat);
        } else {
            plan.report.pending_settlement_sat = plan
                .report
                .pending_settlement_sat
                .saturating_add(obligation.amount_sat);
            let outstanding_sat = plan.record_unpaid(obligation.pair, obligation.amount_sat);
            plan.record_provider_exposure(outstanding_sat, false);
        }
    }
    plan.finish()
}

fn plan_peer_credit(
    workload: &PricedWorkload,
    roles: &[NodeRole],
    config: &IncentiveConfig,
) -> IncentiveReport {
    let mut plan = Plan::new(
        IncentiveStrategy::OfflinePeerCredit,
        workload,
        roles,
        config,
    );
    // Verified reciprocal service is bilateral setoff. Only the remaining
    // direction consumes unsecured credit or needs a Cashu settlement.
    let mut reciprocal = BTreeMap::<(u64, usize, usize), [u64; 2]>::new();
    for batch in batches(&workload.obligations, config.batch_interval_ms) {
        plan.record_state(batch.pair);
        let low = batch.pair.payer.min(batch.pair.provider);
        let high = batch.pair.payer.max(batch.pair.provider);
        let direction = usize::from(batch.pair.payer != low);
        let totals = reciprocal.entry((batch.due_ms, low, high)).or_default();
        totals[direction] = totals[direction].saturating_add(batch.amount_sat);
    }
    let mut outstanding = BTreeMap::<Pair, u64>::new();
    for ((due_ms, low, high), [low_to_high, high_to_low]) in reciprocal {
        let offset = low_to_high.min(high_to_low);
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
        let (pair, amount_sat) = if low_to_high >= high_to_low {
            (
                Pair {
                    payer: low,
                    provider: high,
                },
                low_to_high - high_to_low,
            )
        } else {
            (
                Pair {
                    payer: high,
                    provider: low,
                },
                high_to_low - low_to_high,
            )
        };
        if amount_sat == 0 {
            continue;
        }
        let (admitted, debt_now) = {
            let debt = outstanding.entry(pair).or_default();
            let admitted = amount_sat.min(config.offline_credit_cap_sat.saturating_sub(*debt));
            *debt = debt.saturating_add(admitted);
            (admitted, *debt)
        };
        plan.record_provider_exposure(debt_now, false);
        plan.earn(pair, admitted);
        plan.deny(amount_sat.saturating_sub(admitted));
        if plan.payer_will_default(pair) {
            plan.record_messages(pair, 1);
        } else if debt_now > 0 && plan.try_settlement(pair, due_ms, 2) {
            let settled = outstanding.remove(&pair).unwrap_or_default();
            plan.settle_with_route(pair, settled);
        }
    }
    for (pair, debt) in outstanding {
        let defaults = plan.payer_will_default(pair);
        plan.record_provider_exposure(debt, defaults);
        plan.record_unpaid(pair, debt);
    }
    plan.finish()
}

#[derive(Clone, Copy)]
struct Batch {
    pair: Pair,
    amount_sat: u64,
    due_ms: u64,
}

fn batches(obligations: &[Obligation], interval_ms: u64) -> Vec<Batch> {
    let mut grouped = BTreeMap::<(u64, Pair), u64>::new();
    for obligation in obligations {
        let window = obligation.accepted_at_ms / interval_ms;
        let due_ms = window.saturating_add(1).saturating_mul(interval_ms);
        let amount = grouped.entry((due_ms, obligation.pair)).or_default();
        *amount = amount.saturating_add(obligation.amount_sat);
    }
    grouped
        .into_iter()
        .map(|((due_ms, pair), amount_sat)| Batch {
            pair,
            amount_sat,
            due_ms,
        })
        .collect()
}

#[derive(Clone, Copy)]
struct PairStream {
    pair: Pair,
    amount_sat: u64,
    first_at_ms: u64,
    last_at_ms: u64,
}

fn pair_streams(obligations: &[Obligation]) -> Vec<PairStream> {
    let mut grouped = BTreeMap::<Pair, PairStream>::new();
    for obligation in obligations {
        let stream = grouped.entry(obligation.pair).or_insert(PairStream {
            pair: obligation.pair,
            amount_sat: 0,
            first_at_ms: obligation.accepted_at_ms,
            last_at_ms: obligation.accepted_at_ms,
        });
        stream.amount_sat = stream.amount_sat.saturating_add(obligation.amount_sat);
        stream.first_at_ms = stream.first_at_ms.min(obligation.accepted_at_ms);
        stream.last_at_ms = stream.last_at_ms.max(obligation.accepted_at_ms);
    }
    grouped.into_values().collect()
}

#[derive(Clone, Copy)]
enum MintRoute {
    Same,
    Cross,
}

struct Plan<'a> {
    strategy: IncentiveStrategy,
    roles: &'a [NodeRole],
    config: &'a IncentiveConfig,
    report: IncentiveReport,
    state_entries: Vec<u64>,
    endpoint_messages: Vec<u64>,
    endpoint_bytes: Vec<u64>,
    unpaid_by_pair: BTreeMap<Pair, u64>,
    mint_discovery_pairs: BTreeSet<Pair>,
    sequence: u64,
}

impl<'a> Plan<'a> {
    fn new(
        strategy: IncentiveStrategy,
        workload: &PricedWorkload,
        roles: &'a [NodeRole],
        config: &'a IncentiveConfig,
    ) -> Self {
        let adversarial_payers = workload
            .obligations
            .iter()
            .filter(|obligation| payer_will_default(obligation.pair.payer, roles, config))
            .map(|obligation| obligation.pair.payer)
            .collect::<BTreeSet<_>>();
        let adversarial_payer_value_sat = workload
            .obligations
            .iter()
            .filter(|obligation| adversarial_payers.contains(&obligation.pair.payer))
            .map(|obligation| obligation.amount_sat)
            .sum();
        let mut plan = Self {
            strategy,
            roles,
            config,
            report: IncentiveReport {
                strategy,
                strategy_name: strategy.as_str(),
                use_case: config.use_case,
                figures_are_modeled: true,
                useful_bytes: workload.useful_bytes,
                useful_value_sat: workload.useful_value_sat,
                pricing_dust_bytes: workload.pricing_dust_bytes,
                priced_obligations: workload.obligations.len(),
                adversarial_payer_count: adversarial_payers.len() as u64,
                adversarial_payer_value_sat,
                honest_earned_sat: 0,
                honest_settled_by_deadline_sat: 0,
                honest_earned_settled_by_deadline_basis_points: 0,
                authorization_denied_sat: 0,
                pending_settlement_sat: 0,
                buyer_prepaid_exposure_sat: 0,
                fixed_prepay_paid_sat: 0,
                fixed_prepay_used_sat: 0,
                fixed_prepay_unused_credit_sat: 0,
                buyer_counterparty_loss_sat: 0,
                provider_unpaid_exposure_sat: 0,
                provider_default_loss_sat: 0,
                unpaid_exposure_sat: 0,
                default_exposure_sat: 0,
                max_pair_unpaid_exposure_sat: 0,
                peer_credit_accepted_sat: 0,
                peer_credit_outstanding_sat: 0,
                max_pair_peer_credit_sat: 0,
                same_mint_settlements: 0,
                cross_mint_settlements: 0,
                reciprocal_service_settled_sat: 0,
                modeled_fees_sat: 0,
                locked_capital_sat_ms: 0,
                peak_locked_capital_sat: 0,
                channel_capacity_sat: 0,
                channel_signed_balance_sat: 0,
                channel_unused_capacity_sat: 0,
                channel_refunded_sat: 0,
                channel_open_fees_sat: 0,
                channel_close_fees_sat: 0,
                refund_failures: 0,
                refund_loss_sat: 0,
                mint_advertisement_messages: 0,
                mint_advertisement_bytes: 0,
                mint_accepting_nodes: mint_accepting_nodes(roles.len(), config),
                settlement_attempts: 0,
                payment_retries: 0,
                payment_messages: 0,
                payment_bytes: 0,
                payment_byte_overhead_basis_points: 0,
                fake_claims_attempted: 0,
                fake_claimed_value_sat: 0,
                honest_node_payment_state_entries: DistributionSummary::default(),
                honest_node_payment_messages: DistributionSummary::default(),
                honest_node_payment_endpoint_bytes: DistributionSummary::default(),
            },
            state_entries: vec![0; roles.len()],
            endpoint_messages: vec![0; roles.len()],
            endpoint_bytes: vec![0; roles.len()],
            unpaid_by_pair: BTreeMap::new(),
            mint_discovery_pairs: BTreeSet::new(),
            sequence: 0,
        };
        plan.record_public_mint_discovery();
        plan
    }

    fn payer_will_default(&self, pair: Pair) -> bool {
        payer_will_default(pair.payer, self.roles, self.config)
    }

    fn provider_is_honest(&self, pair: Pair) -> bool {
        self.roles[pair.provider] != NodeRole::Attacker
    }

    fn earn(&mut self, pair: Pair, amount_sat: u64) {
        if self.provider_is_honest(pair) {
            self.report.honest_earned_sat =
                self.report.honest_earned_sat.saturating_add(amount_sat);
        }
    }

    fn deny(&mut self, amount_sat: u64) {
        self.report.authorization_denied_sat = self
            .report
            .authorization_denied_sat
            .saturating_add(amount_sat);
    }

    fn reject_unfunded(&mut self, pair: Pair, amount_sat: u64) {
        self.record_messages(pair, 1);
        self.deny(amount_sat);
    }

    fn record_defaulted_settlement(&mut self, pair: Pair, amount_sat: u64) {
        self.record_mint_discovery_pair(pair);
        self.record_messages(pair, 1);
        let outstanding_sat = self.record_unpaid(pair, amount_sat);
        self.record_provider_exposure(outstanding_sat, false);
        self.report.provider_default_loss_sat = self
            .report
            .provider_default_loss_sat
            .saturating_add(amount_sat);
    }

    fn try_settlement(&mut self, pair: Pair, due_ms: u64, phase: u64) -> bool {
        self.record_mint_discovery_pair(pair);
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        for retry in 0..=self.config.max_payment_retries {
            let at_ms = due_ms
                .saturating_add(u64::from(retry).saturating_mul(self.config.payment_retry_ms));
            if at_ms > self.config.settlement_deadline_ms {
                break;
            }
            self.report.settlement_attempts = self.report.settlement_attempts.saturating_add(1);
            if retry > 0 {
                self.report.payment_retries = self.report.payment_retries.saturating_add(1);
            }
            self.record_messages(pair, ATTEMPT_MESSAGES);
            if !self.payment_is_out(pair, at_ms, phase, sequence, retry) {
                return true;
            }
        }
        false
    }

    fn payment_is_out(&self, pair: Pair, at_ms: u64, phase: u64, sequence: u64, retry: u8) -> bool {
        let sample = mix64(
            self.config.seed
                ^ self.strategy.domain().rotate_left(7)
                ^ usize_u64(pair.payer).rotate_left(17)
                ^ usize_u64(pair.provider).rotate_left(31)
                ^ at_ms.rotate_left(43)
                ^ phase.rotate_left(53)
                ^ sequence
                ^ u64::from(retry).rotate_left(11),
        ) % 10_000;
        sample < u64::from(self.config.outage_basis_points)
    }

    fn settle_with_route(&mut self, pair: Pair, amount_sat: u64) {
        self.record_route_settlement(pair);
        self.mark_settled(pair, amount_sat);
    }

    fn record_route_settlement(&mut self, pair: Pair) {
        match mint_route(pair, self.config) {
            MintRoute::Same => {
                self.report.same_mint_settlements =
                    self.report.same_mint_settlements.saturating_add(1);
                self.report.modeled_fees_sat = self
                    .report
                    .modeled_fees_sat
                    .saturating_add(self.config.same_mint_fee_sat);
            }
            MintRoute::Cross => {
                self.report.cross_mint_settlements =
                    self.report.cross_mint_settlements.saturating_add(1);
                self.report.modeled_fees_sat = self
                    .report
                    .modeled_fees_sat
                    .saturating_add(self.config.cross_mint_fee_sat);
            }
        }
    }

    fn mark_settled(&mut self, pair: Pair, amount_sat: u64) {
        if self.provider_is_honest(pair) {
            self.report.honest_settled_by_deadline_sat = self
                .report
                .honest_settled_by_deadline_sat
                .saturating_add(amount_sat);
        }
    }

    fn record_unpaid(&mut self, pair: Pair, amount_sat: u64) -> u64 {
        let defaults = self.payer_will_default(pair);
        let outstanding_sat = {
            let outstanding = self.unpaid_by_pair.entry(pair).or_default();
            *outstanding = outstanding.saturating_add(amount_sat);
            *outstanding
        };
        self.report.unpaid_exposure_sat =
            self.report.unpaid_exposure_sat.saturating_add(amount_sat);
        self.report.max_pair_unpaid_exposure_sat = self
            .report
            .max_pair_unpaid_exposure_sat
            .max(outstanding_sat);
        if defaults {
            self.report.default_exposure_sat =
                self.report.default_exposure_sat.saturating_add(amount_sat);
        }
        outstanding_sat
    }

    fn record_state(&mut self, pair: Pair) {
        self.state_entries[pair.payer] = self.state_entries[pair.payer].saturating_add(1);
        self.state_entries[pair.provider] = self.state_entries[pair.provider].saturating_add(1);
    }

    fn record_messages(&mut self, pair: Pair, messages: u64) {
        let bytes = messages.saturating_mul(self.config.payment_message_bytes);
        self.record_endpoint_traffic(pair, messages, bytes);
    }

    fn record_endpoint_traffic(&mut self, pair: Pair, messages: u64, bytes: u64) {
        self.report.payment_messages = self.report.payment_messages.saturating_add(messages);
        self.report.payment_bytes = self.report.payment_bytes.saturating_add(bytes);
        self.endpoint_messages[pair.payer] =
            self.endpoint_messages[pair.payer].saturating_add(messages);
        self.endpoint_messages[pair.provider] =
            self.endpoint_messages[pair.provider].saturating_add(messages);
        self.endpoint_bytes[pair.payer] = self.endpoint_bytes[pair.payer].saturating_add(bytes);
        self.endpoint_bytes[pair.provider] =
            self.endpoint_bytes[pair.provider].saturating_add(bytes);
    }

    fn record_public_mint_discovery(&mut self) {
        if self.config.mint_discovery_scope != MintDiscoveryScope::Public {
            return;
        }
        for provider in 0..self.roles.len() {
            for payer in 0..self.roles.len() {
                if payer != provider {
                    self.record_mint_discovery_pair(Pair { payer, provider });
                }
            }
        }
    }

    fn record_mint_discovery_pair(&mut self, pair: Pair) {
        if !self.mint_discovery_pairs.insert(pair) {
            return;
        }
        let messages = u64::from(self.config.mint_discovery_scope == MintDiscoveryScope::Public);
        self.report.mint_advertisement_messages = self
            .report
            .mint_advertisement_messages
            .saturating_add(messages);
        self.report.mint_advertisement_bytes = self
            .report
            .mint_advertisement_bytes
            .saturating_add(self.config.mint_advertisement_bytes);
        self.record_endpoint_traffic(pair, messages, self.config.mint_advertisement_bytes);
    }

    fn finish(mut self) -> IncentiveReport {
        self.add_fake_claim_pressure();
        self.report.honest_earned_settled_by_deadline_basis_points = basis_points(
            self.report.honest_settled_by_deadline_sat,
            self.report.honest_earned_sat,
        );
        self.report.payment_byte_overhead_basis_points =
            basis_points(self.report.payment_bytes, self.report.useful_bytes);
        self.report.honest_node_payment_state_entries =
            summarize_honest(&self.state_entries, self.roles);
        self.report.honest_node_payment_messages =
            summarize_honest(&self.endpoint_messages, self.roles);
        self.report.honest_node_payment_endpoint_bytes =
            summarize_honest(&self.endpoint_bytes, self.roles);
        self.report
    }

    fn add_fake_claim_pressure(&mut self) {
        let Some(honest_target) = self
            .roles
            .iter()
            .position(|role| *role != NodeRole::Attacker)
        else {
            return;
        };
        for (attacker, role) in self.roles.iter().enumerate() {
            if *role != NodeRole::Attacker {
                continue;
            }
            let claims = u64::from(self.config.fake_claims_per_attacker);
            self.report.fake_claims_attempted =
                self.report.fake_claims_attempted.saturating_add(claims);
            self.report.fake_claimed_value_sat = self
                .report
                .fake_claimed_value_sat
                .saturating_add(claims.saturating_mul(self.config.fake_claim_value_sat));
            self.record_messages(
                Pair {
                    payer: honest_target,
                    provider: attacker,
                },
                claims,
            );
        }
    }
}

fn payer_will_default(payer: usize, roles: &[NodeRole], config: &IncentiveConfig) -> bool {
    if roles[payer] == NodeRole::Attacker {
        return true;
    }
    mix64(config.seed ^ usize_u64(payer).rotate_left(29) ^ 0x4445_4641_554c_5453) % 10_000
        < u64::from(config.payment_defaulter_basis_points)
}

fn summarize_honest(values: &[u64], roles: &[NodeRole]) -> DistributionSummary {
    let honest = values
        .iter()
        .zip(roles)
        .filter_map(|(value, role)| (*role != NodeRole::Attacker).then_some(*value))
        .collect::<Vec<_>>();
    summarize_distribution(&honest)
}

fn mint_accepting_nodes(node_count: usize, config: &IncentiveConfig) -> Vec<u64> {
    let mut accepting = vec![0_u64; config.mint_count];
    for node in 0..node_count {
        let home = home_mint(node, config);
        accepting[home] = accepting[home].saturating_add(1);
        if let Some(gateway) = config.gateway_mint.filter(|gateway| *gateway != home) {
            accepting[gateway] = accepting[gateway].saturating_add(1);
        }
    }
    accepting
}

fn mint_route(pair: Pair, config: &IncentiveConfig) -> MintRoute {
    // Each node's modeled acceptance set is private: its deterministic home
    // mint plus the optional shared gateway mint.
    let payer_home = home_mint(pair.payer, config);
    let provider_home = home_mint(pair.provider, config);
    if payer_home == provider_home || config.gateway_mint == Some(payer_home) {
        MintRoute::Same
    } else {
        // Cross means one modeled Lightning-backed source melt and destination
        // mint quote into a privately accepted mint, never Cashu multihop.
        MintRoute::Cross
    }
}

fn home_mint(node: usize, config: &IncentiveConfig) -> usize {
    usize::try_from(mix64(config.seed ^ usize_u64(node)) % usize_u64(config.mint_count))
        .unwrap_or_default()
}

const fn usize_u64(value: usize) -> u64 {
    value as u64
}

const fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests;
