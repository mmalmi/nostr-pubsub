const BASIS_POINTS_SCALE: u128 = 10_000;

/// Convert a ratio to basis points using integer truncation.
///
/// A zero denominator returns zero. Ratios larger than `u32::MAX` basis points
/// saturate at `u32::MAX`.
#[must_use]
pub fn basis_points(numerator: u64, denominator: u64) -> u32 {
    basis_points_wide(u128::from(numerator), u128::from(denominator))
}

/// Nearest-rank summary of an unordered non-negative integer distribution.
///
/// The total saturates at `u64::MAX`; the mean is calculated from the wider
/// unsaturated total using integer truncation. Every field is zero for an
/// empty input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DistributionSummary {
    pub count: usize,
    pub total: u64,
    pub mean: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

#[must_use]
pub fn summarize_distribution(samples: &[u64]) -> DistributionSummary {
    if samples.is_empty() {
        return DistributionSummary::default();
    }

    let wide_total = samples.iter().fold(0_u128, |total, sample| {
        total.saturating_add(u128::from(*sample))
    });
    let count = u128::try_from(samples.len()).unwrap_or(u128::MAX);
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    DistributionSummary {
        count: samples.len(),
        total: saturating_u64(wide_total),
        mean: saturating_u64(wide_total.checked_div(count).unwrap_or_default()),
        p50: nearest_rank(&sorted, 50),
        p95: nearest_rank(&sorted, 95),
        p99: nearest_rank(&sorted, 99),
        max: sorted.last().copied().unwrap_or_default(),
    }
}

/// Nearest-rank latency percentiles over an unordered sample set.
///
/// All fields, including `sample_count`, are zero when the input is empty.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencySummary {
    pub sample_count: usize,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

#[must_use]
pub fn summarize_latencies(samples: &[u64]) -> LatencySummary {
    let summary = summarize_distribution(samples);
    LatencySummary {
        sample_count: summary.count,
        p50: summary.p50,
        p95: summary.p95,
        p99: summary.p99,
        max: summary.max,
    }
}

/// A message and byte count. All additions saturate independently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrafficCounter {
    pub messages: u64,
    pub bytes: u64,
}

impl TrafficCounter {
    #[must_use]
    pub const fn new(messages: u64, bytes: u64) -> Self {
        Self { messages, bytes }
    }

    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            messages: self.messages.saturating_add(other.messages),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }

    fn record(&mut self, messages: u64, bytes: u64) {
        self.messages = self.messages.saturating_add(messages);
        self.bytes = self.bytes.saturating_add(bytes);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficDirection {
    Sent,
    Received,
}

/// Workload provenance, independent of the role carrying the traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficProvenance {
    Legitimate,
    Adversarial,
}

/// Select which side of each node's ledger contributes to a load summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrafficScope {
    Sent,
    Received,
    Combined,
}

/// Per-node traffic split by direction and workload provenance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeTrafficLedger {
    sent_legitimate: TrafficCounter,
    sent_adversarial: TrafficCounter,
    received_legitimate: TrafficCounter,
    received_adversarial: TrafficCounter,
}

impl NodeTrafficLedger {
    /// Combine two ledgers while saturating every counter independently.
    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            sent_legitimate: self.sent_legitimate.saturating_add(other.sent_legitimate),
            sent_adversarial: self.sent_adversarial.saturating_add(other.sent_adversarial),
            received_legitimate: self
                .received_legitimate
                .saturating_add(other.received_legitimate),
            received_adversarial: self
                .received_adversarial
                .saturating_add(other.received_adversarial),
        }
    }

    /// Record an arbitrary number of messages and their aggregate wire bytes.
    pub fn record(
        &mut self,
        direction: TrafficDirection,
        provenance: TrafficProvenance,
        messages: u64,
        bytes: u64,
    ) {
        self.counter_mut(direction, provenance)
            .record(messages, bytes);
    }

    /// Record one message and its wire bytes.
    pub fn record_message(
        &mut self,
        direction: TrafficDirection,
        provenance: TrafficProvenance,
        bytes: u64,
    ) {
        self.record(direction, provenance, 1, bytes);
    }

    #[must_use]
    pub fn counter(
        &self,
        direction: TrafficDirection,
        provenance: TrafficProvenance,
    ) -> TrafficCounter {
        match (direction, provenance) {
            (TrafficDirection::Sent, TrafficProvenance::Legitimate) => self.sent_legitimate,
            (TrafficDirection::Sent, TrafficProvenance::Adversarial) => self.sent_adversarial,
            (TrafficDirection::Received, TrafficProvenance::Legitimate) => self.received_legitimate,
            (TrafficDirection::Received, TrafficProvenance::Adversarial) => {
                self.received_adversarial
            }
        }
    }

    #[must_use]
    pub fn legitimate(&self, scope: TrafficScope) -> TrafficCounter {
        self.scoped(scope, TrafficProvenance::Legitimate)
    }

    #[must_use]
    pub fn adversarial(&self, scope: TrafficScope) -> TrafficCounter {
        self.scoped(scope, TrafficProvenance::Adversarial)
    }

    #[must_use]
    pub fn total(&self, scope: TrafficScope) -> TrafficCounter {
        self.legitimate(scope)
            .saturating_add(self.adversarial(scope))
    }

    fn scoped(&self, scope: TrafficScope, provenance: TrafficProvenance) -> TrafficCounter {
        match scope {
            TrafficScope::Sent => self.counter(TrafficDirection::Sent, provenance),
            TrafficScope::Received => self.counter(TrafficDirection::Received, provenance),
            TrafficScope::Combined => self
                .counter(TrafficDirection::Sent, provenance)
                .saturating_add(self.counter(TrafficDirection::Received, provenance)),
        }
    }

    fn counter_mut(
        &mut self,
        direction: TrafficDirection,
        provenance: TrafficProvenance,
    ) -> &mut TrafficCounter {
        match (direction, provenance) {
            (TrafficDirection::Sent, TrafficProvenance::Legitimate) => &mut self.sent_legitimate,
            (TrafficDirection::Sent, TrafficProvenance::Adversarial) => &mut self.sent_adversarial,
            (TrafficDirection::Received, TrafficProvenance::Legitimate) => {
                &mut self.received_legitimate
            }
            (TrafficDirection::Received, TrafficProvenance::Adversarial) => {
                &mut self.received_adversarial
            }
        }
    }
}

/// Aggregate load and concentration metrics for a set of node ledgers.
///
/// `max_load` is component-wise: its message and byte maxima may come from
/// different nodes. Means use integer truncation. An empty ledger slice yields
/// zero for every numeric field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadSummary {
    pub scope: TrafficScope,
    pub node_count: usize,
    pub legitimate: TrafficCounter,
    pub adversarial: TrafficCounter,
    pub total: TrafficCounter,
    pub max_load: TrafficCounter,
    pub mean_load: TrafficCounter,
    pub legitimate_message_share_basis_points: u32,
    pub legitimate_byte_share_basis_points: u32,
    pub message_gini_basis_points: u32,
    pub byte_gini_basis_points: u32,
}

#[must_use]
pub fn summarize_load(nodes: &[NodeTrafficLedger], scope: TrafficScope) -> LoadSummary {
    let mut legitimate_messages = 0_u128;
    let mut legitimate_bytes = 0_u128;
    let mut adversarial_messages = 0_u128;
    let mut adversarial_bytes = 0_u128;
    let mut message_loads = Vec::with_capacity(nodes.len());
    let mut byte_loads = Vec::with_capacity(nodes.len());
    let mut max_load = TrafficCounter::default();

    for node in nodes {
        let legitimate = node.legitimate(scope);
        let adversarial = node.adversarial(scope);
        let total = legitimate.saturating_add(adversarial);
        legitimate_messages = legitimate_messages.saturating_add(u128::from(legitimate.messages));
        legitimate_bytes = legitimate_bytes.saturating_add(u128::from(legitimate.bytes));
        adversarial_messages =
            adversarial_messages.saturating_add(u128::from(adversarial.messages));
        adversarial_bytes = adversarial_bytes.saturating_add(u128::from(adversarial.bytes));
        max_load.messages = max_load.messages.max(total.messages);
        max_load.bytes = max_load.bytes.max(total.bytes);
        message_loads.push(total.messages);
        byte_loads.push(total.bytes);
    }

    let total_messages = legitimate_messages.saturating_add(adversarial_messages);
    let total_bytes = legitimate_bytes.saturating_add(adversarial_bytes);
    let node_count = u128::try_from(nodes.len()).unwrap_or(u128::MAX);
    let mean_load = TrafficCounter::new(
        saturating_u64(total_messages.checked_div(node_count).unwrap_or_default()),
        saturating_u64(total_bytes.checked_div(node_count).unwrap_or_default()),
    );

    LoadSummary {
        scope,
        node_count: nodes.len(),
        legitimate: TrafficCounter::new(
            saturating_u64(legitimate_messages),
            saturating_u64(legitimate_bytes),
        ),
        adversarial: TrafficCounter::new(
            saturating_u64(adversarial_messages),
            saturating_u64(adversarial_bytes),
        ),
        total: TrafficCounter::new(saturating_u64(total_messages), saturating_u64(total_bytes)),
        max_load,
        mean_load,
        legitimate_message_share_basis_points: basis_points_wide(
            legitimate_messages,
            total_messages,
        ),
        legitimate_byte_share_basis_points: basis_points_wide(legitimate_bytes, total_bytes),
        message_gini_basis_points: gini_basis_points(&message_loads),
        byte_gini_basis_points: gini_basis_points(&byte_loads),
    }
}

/// Gini load concentration in basis points for non-negative integer samples.
///
/// Empty and all-zero inputs return zero. Equal loads return zero; a value near
/// 10,000 indicates that load is concentrated on very few nodes.
#[must_use]
pub fn gini_basis_points(values: &[u64]) -> u32 {
    if values.is_empty() {
        return 0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mut prefix = 0_u128;
    let mut pairwise_difference = 0_u128;
    for (index, value) in sorted.into_iter().enumerate() {
        let value = u128::from(value);
        let index = u128::try_from(index).unwrap_or(u128::MAX);
        pairwise_difference =
            pairwise_difference.saturating_add(value.saturating_mul(index).saturating_sub(prefix));
        prefix = prefix.saturating_add(value);
    }
    if prefix == 0 {
        return 0;
    }

    let count = u128::try_from(values.len()).unwrap_or(u128::MAX);
    let denominator = count.saturating_mul(prefix);
    basis_points_wide(pairwise_difference, denominator)
        .min(u32::try_from(BASIS_POINTS_SCALE).unwrap_or(u32::MAX))
}

fn nearest_rank(sorted: &[u64], percentile: u128) -> u64 {
    let count = u128::try_from(sorted.len()).unwrap_or(u128::MAX);
    let rank = count
        .saturating_mul(percentile)
        .saturating_add(99)
        .checked_div(100)
        .unwrap_or_default();
    let index = usize::try_from(rank.saturating_sub(1))
        .unwrap_or(usize::MAX)
        .min(sorted.len().saturating_sub(1));
    sorted[index]
}

fn basis_points_wide(numerator: u128, denominator: u128) -> u32 {
    if denominator == 0 {
        return 0;
    }
    let scaled = numerator
        .saturating_mul(BASIS_POINTS_SCALE)
        .checked_div(denominator)
        .unwrap_or_default();
    u32::try_from(scaled).unwrap_or(u32::MAX)
}

fn saturating_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        DistributionSummary, LatencySummary, NodeTrafficLedger, TrafficCounter, TrafficDirection,
        TrafficProvenance, TrafficScope, basis_points, gini_basis_points, summarize_distribution,
        summarize_latencies, summarize_load,
    };

    #[test]
    fn basis_points_is_deterministic_and_defines_zero_denominator() {
        assert_eq!(basis_points(0, 0), 0);
        assert_eq!(basis_points(1, 0), 0);
        assert_eq!(basis_points(1, 3), 3_333);
        assert_eq!(basis_points(2, 3), 6_666);
        assert_eq!(basis_points(5, 4), 12_500);
        assert_eq!(basis_points(u64::MAX, u64::MAX), 10_000);
        assert_eq!(basis_points(u64::MAX, 1), u32::MAX);
    }

    #[test]
    fn latency_summary_defines_empty_and_single_sample_behavior() {
        assert_eq!(summarize_latencies(&[]), LatencySummary::default());
        assert_eq!(
            summarize_latencies(&[42]),
            LatencySummary {
                sample_count: 1,
                p50: 42,
                p95: 42,
                p99: 42,
                max: 42,
            }
        );
    }

    #[test]
    fn latency_summary_uses_nearest_rank_on_unsorted_samples() {
        let samples = (1..=20).rev().collect::<Vec<_>>();
        assert_eq!(
            summarize_latencies(&samples),
            LatencySummary {
                sample_count: 20,
                p50: 10,
                p95: 19,
                p99: 20,
                max: 20,
            }
        );
        assert_eq!(summarize_latencies(&[20, 10]).p50, 10);
    }

    #[test]
    fn distribution_summary_defines_empty_and_single_sample_behavior() {
        assert_eq!(summarize_distribution(&[]), DistributionSummary::default());
        assert_eq!(
            summarize_distribution(&[42]),
            DistributionSummary {
                count: 1,
                total: 42,
                mean: 42,
                p50: 42,
                p95: 42,
                p99: 42,
                max: 42,
            }
        );
    }

    #[test]
    fn distribution_summary_uses_nearest_rank_on_unsorted_samples() {
        let samples = (1..=100).rev().collect::<Vec<_>>();
        assert_eq!(
            summarize_distribution(&samples),
            DistributionSummary {
                count: 100,
                total: 5_050,
                mean: 50,
                p50: 50,
                p95: 95,
                p99: 99,
                max: 100,
            }
        );

        let two = summarize_distribution(&[20, 10]);
        assert_eq!((two.p50, two.p95, two.p99), (10, 20, 20));
    }

    #[test]
    fn distribution_summary_truncates_mean_and_preserves_zeroes() {
        assert_eq!(
            summarize_distribution(&[0, 0, 0]),
            DistributionSummary {
                count: 3,
                ..DistributionSummary::default()
            }
        );

        let summary = summarize_distribution(&[0, 1, 2, 2]);
        assert_eq!(summary.total, 5);
        assert_eq!(summary.mean, 1);
        assert_eq!(
            (summary.p50, summary.p95, summary.p99, summary.max),
            (1, 2, 2, 2)
        );
    }

    #[test]
    fn distribution_summary_saturates_total_without_corrupting_mean() {
        let summary = summarize_distribution(&[u64::MAX, u64::MAX]);
        assert_eq!(summary.count, 2);
        assert_eq!(summary.total, u64::MAX);
        assert_eq!(summary.mean, u64::MAX);
        assert_eq!(summary.p50, u64::MAX);
        assert_eq!(summary.p95, u64::MAX);
        assert_eq!(summary.p99, u64::MAX);
        assert_eq!(summary.max, u64::MAX);
    }

    #[test]
    fn node_ledger_keeps_direction_and_provenance_separate() {
        let mut ledger = NodeTrafficLedger::default();
        ledger.record(
            TrafficDirection::Sent,
            TrafficProvenance::Legitimate,
            2,
            100,
        );
        ledger.record_message(TrafficDirection::Sent, TrafficProvenance::Adversarial, 75);
        ledger.record(
            TrafficDirection::Received,
            TrafficProvenance::Legitimate,
            3,
            200,
        );
        ledger.record_message(
            TrafficDirection::Received,
            TrafficProvenance::Adversarial,
            50,
        );

        assert_eq!(
            ledger.counter(TrafficDirection::Sent, TrafficProvenance::Legitimate),
            TrafficCounter::new(2, 100)
        );
        assert_eq!(
            ledger.counter(TrafficDirection::Sent, TrafficProvenance::Adversarial),
            TrafficCounter::new(1, 75)
        );
        assert_eq!(
            ledger.legitimate(TrafficScope::Combined),
            TrafficCounter::new(5, 300)
        );
        assert_eq!(
            ledger.adversarial(TrafficScope::Combined),
            TrafficCounter::new(2, 125)
        );
        assert_eq!(
            ledger.total(TrafficScope::Combined),
            TrafficCounter::new(7, 425)
        );
    }

    #[test]
    fn ledger_arithmetic_saturates_messages_and_bytes() {
        let mut ledger = NodeTrafficLedger::default();
        ledger.record(
            TrafficDirection::Sent,
            TrafficProvenance::Legitimate,
            u64::MAX - 1,
            u64::MAX - 2,
        );
        ledger.record(
            TrafficDirection::Sent,
            TrafficProvenance::Legitimate,
            10,
            10,
        );

        assert_eq!(
            ledger.legitimate(TrafficScope::Sent),
            TrafficCounter::new(u64::MAX, u64::MAX)
        );
        assert_eq!(
            TrafficCounter::new(u64::MAX, 1).saturating_add(TrafficCounter::new(1, u64::MAX)),
            TrafficCounter::new(u64::MAX, u64::MAX)
        );

        let mut other = NodeTrafficLedger::default();
        other.record(
            TrafficDirection::Received,
            TrafficProvenance::Adversarial,
            2,
            20,
        );
        let combined = ledger.saturating_add(other);
        assert_eq!(
            combined.adversarial(TrafficScope::Received),
            TrafficCounter::new(2, 20)
        );
    }

    #[test]
    fn gini_defines_empty_equal_and_concentrated_loads() {
        assert_eq!(gini_basis_points(&[]), 0);
        assert_eq!(gini_basis_points(&[0, 0, 0]), 0);
        assert_eq!(gini_basis_points(&[10, 10, 10]), 0);
        assert_eq!(gini_basis_points(&[0, 1, 3]), 5_000);
        assert_eq!(gini_basis_points(&[0, 0, 0, 100]), 7_500);
        assert_eq!(gini_basis_points(&[0, u64::MAX]), 5_000);
    }

    #[test]
    fn empty_load_summary_is_all_zero() {
        let summary = summarize_load(&[], TrafficScope::Combined);

        assert_eq!(summary.scope, TrafficScope::Combined);
        assert_eq!(summary.node_count, 0);
        assert_eq!(summary.legitimate, TrafficCounter::default());
        assert_eq!(summary.adversarial, TrafficCounter::default());
        assert_eq!(summary.total, TrafficCounter::default());
        assert_eq!(summary.max_load, TrafficCounter::default());
        assert_eq!(summary.mean_load, TrafficCounter::default());
        assert_eq!(summary.legitimate_message_share_basis_points, 0);
        assert_eq!(summary.legitimate_byte_share_basis_points, 0);
        assert_eq!(summary.message_gini_basis_points, 0);
        assert_eq!(summary.byte_gini_basis_points, 0);
    }

    #[test]
    fn load_summary_reports_provenance_shares_mean_max_and_concentration() {
        let mut first = NodeTrafficLedger::default();
        first.record(
            TrafficDirection::Sent,
            TrafficProvenance::Legitimate,
            10,
            1_000,
        );
        first.record(
            TrafficDirection::Received,
            TrafficProvenance::Adversarial,
            5,
            500,
        );
        let mut second = NodeTrafficLedger::default();
        second.record(
            TrafficDirection::Sent,
            TrafficProvenance::Adversarial,
            10,
            3_000,
        );

        let sent = summarize_load(&[first, second], TrafficScope::Sent);
        assert_eq!(sent.total, TrafficCounter::new(20, 4_000));
        assert_eq!(sent.max_load, TrafficCounter::new(10, 3_000));
        assert_eq!(sent.mean_load, TrafficCounter::new(10, 2_000));
        assert_eq!(sent.legitimate_message_share_basis_points, 5_000);
        assert_eq!(sent.legitimate_byte_share_basis_points, 2_500);
        assert_eq!(sent.message_gini_basis_points, 0);
        assert_eq!(sent.byte_gini_basis_points, 2_500);

        let received = summarize_load(&[first, second], TrafficScope::Received);
        assert_eq!(received.total, TrafficCounter::new(5, 500));
        assert_eq!(received.legitimate_message_share_basis_points, 0);
        assert_eq!(received.message_gini_basis_points, 5_000);

        let combined = summarize_load(&[first, second], TrafficScope::Combined);
        assert_eq!(combined.total, TrafficCounter::new(25, 4_500));
        assert_eq!(combined.mean_load, TrafficCounter::new(12, 2_250));
        assert_eq!(combined.legitimate_message_share_basis_points, 4_000);
        assert_eq!(combined.legitimate_byte_share_basis_points, 2_222);
        assert_eq!(combined.message_gini_basis_points, 1_000);
        assert_eq!(combined.byte_gini_basis_points, 1_666);
    }

    #[test]
    fn summary_saturates_public_totals_without_corrupting_provenance_shares() {
        let mut node = NodeTrafficLedger::default();
        node.record(
            TrafficDirection::Sent,
            TrafficProvenance::Legitimate,
            u64::MAX,
            u64::MAX,
        );
        node.record(
            TrafficDirection::Sent,
            TrafficProvenance::Adversarial,
            u64::MAX,
            u64::MAX,
        );

        let summary = summarize_load(&[node], TrafficScope::Sent);
        assert_eq!(summary.legitimate, TrafficCounter::new(u64::MAX, u64::MAX));
        assert_eq!(summary.adversarial, TrafficCounter::new(u64::MAX, u64::MAX));
        assert_eq!(summary.total, TrafficCounter::new(u64::MAX, u64::MAX));
        assert_eq!(summary.max_load, TrafficCounter::new(u64::MAX, u64::MAX));
        assert_eq!(summary.mean_load, TrafficCounter::new(u64::MAX, u64::MAX));
        assert_eq!(summary.legitimate_message_share_basis_points, 5_000);
        assert_eq!(summary.legitimate_byte_share_basis_points, 5_000);
        assert_eq!(summary.message_gini_basis_points, 0);
        assert_eq!(summary.byte_gini_basis_points, 0);
    }
}
