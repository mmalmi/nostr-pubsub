use std::collections::BTreeSet;

use nostr_pubsub_sim::{
    NodeRole, PeerSelectionMode, SimulationConfig, SimulationReport, SupernodeDiscoveryStrategy,
    TopologyStrategy, TrafficScope, basis_points, run_simulation,
};

#[path = "release_gate/service_accounting.rs"]
mod service_accounting;
use service_accounting::assert_service_accounting_is_populated;
#[path = "release_gate/delivery_trails.rs"]
mod delivery_trails;

const RELEASE_SEEDS: [u64; 3] = [
    0x4e4f_5354_5250_5542,
    0x9e37_79b9_7f4a_7c15,
    0x0000_0000_0000_0001,
];
const MIN_DELIVERY_BPS: u32 = 9_500;
const MIN_WORST_COHORT_BPS: u32 = 9_000;
const MIN_EVENTUAL_DISRUPTED_TRANSFER_RECOVERY_BPS: u32 = 3_000;
// Fresh machine-admitted Sybils intentionally remain an open cold-start control.
const MIN_PEER_MESH_SPAM_SUPPRESSION_BPS: u32 = 5_000;
const MIN_HYBRID_SPAM_SUPPRESSION_BPS: u32 = 5_000;
const MIN_PERSISTENT_MACHINE_SPAM_SUPPRESSION_BPS: u32 = 5_000;
const MIN_VERIFIED_PATH_SIGNATURE_REDUCTION_BPS: u32 = 2_000;
const SPAM_IDENTITIES: [&str; 2] = ["persistent", "fresh-sybil"];
const SUBSCRIPTION_CLASSES: [&str; 8] = [
    "author-feed",
    "hashtag-topic",
    "hashtree-update",
    "targeted-approval-rating",
    "iris-drive-broad-root",
    "fips-advert",
    "fips-paid-offer",
    "git-repo-announcement",
];
const EXACT_FILTER_CLASSES: [&str; 3] = ["author-feed", "hashtree-update", "git-repo-announcement"];
const MACHINE_ADMITTED_CLASSES: [&str; 3] =
    ["targeted-approval-rating", "fips-advert", "fips-paid-offer"];

struct ModeReports {
    neutral: SimulationReport,
    local: SimulationReport,
    shared: SimulationReport,
}

impl ModeReports {
    fn all(&self) -> [&SimulationReport; 3] {
        [&self.neutral, &self.local, &self.shared]
    }
}

#[test]
#[ignore = "production-scale deterministic adversarial release gate"]
fn production_like_thousand_node_adversarial_matrix() {
    let mut quiet_blackhole_cases = [0usize; 2];
    for seed in RELEASE_SEEDS {
        for topology in [
            TopologyStrategy::PeerMesh,
            TopologyStrategy::HybridSupernodes,
        ] {
            let reports = run_modes(release_config(seed, topology));
            let case = format!("seed={seed:#x} topology={topology:?}");

            for report in reports.all() {
                eprintln!("{}", report_context(report, &case));
                assert_adversarial_scenario_was_exercised(report, &case);
                assert_signed_spam_class_metrics(report, &case);
                assert_signed_spam_identity_metrics(report, &case);
                assert_legitimate_delivery_is_safe(report, &case);
                assert_service_accounting_is_populated(report, &case);
                assert_honest_resource_accounting(report, &case);
            }
            assert_shared_reputation_improves_spam_resistance(&reports, &case);
            assert_machine_reputation_used_real_transport(&reports.shared, &case);
            let topology_index = usize::from(topology == TopologyStrategy::HybridSupernodes);
            quiet_blackhole_cases[topology_index] = quiet_blackhole_cases[topology_index]
                .saturating_add(usize::from(
                    reports.shared.machine_quiet_blackhole_removals > 0,
                ));
            if topology == TopologyStrategy::HybridSupernodes {
                assert_supernode_discovery_and_service(&reports, &case);
            }
        }
    }
    assert!(
        quiet_blackhole_cases.into_iter().all(|cases| cases >= 2),
        "quiet-blackhole removal must be exercised in at least two seeds per topology: {quiet_blackhole_cases:?}"
    );
}

#[test]
#[ignore = "production-scale supernode discovery comparison"]
fn thousand_node_supernode_discovery_strategy_matrix() {
    let mut outcomes = BTreeSet::new();
    for discovery in [
        SupernodeDiscoveryStrategy::Bootstrap,
        SupernodeDiscoveryStrategy::InterestAffinity,
        SupernodeDiscoveryStrategy::Exploration,
        SupernodeDiscoveryStrategy::Mixed,
    ] {
        let mut config = release_config(RELEASE_SEEDS[0], TopologyStrategy::HybridSupernodes);
        config.supernode_discovery = discovery;
        let report = run(config, PeerSelectionMode::SharedReputation);
        let case = format!("discovery={discovery:?}");
        eprintln!("{}", report_context(&report, &case));
        assert_legitimate_delivery_is_safe(&report, &case);
        assert_service_accounting_is_populated(&report, &case);
        assert_honest_resource_accounting(&report, &case);
        assert_supernode_report(&report, &case);
        outcomes.insert((
            report.honest_supernode_links,
            report.false_supernode_links,
            report.false_only_supernode_peers,
        ));
        if discovery == SupernodeDiscoveryStrategy::Bootstrap {
            assert_eq!(report.false_supernode_links, 0, "{case}");
            assert_eq!(report.false_only_supernode_peers, 0, "{case}");
        }
        if discovery == SupernodeDiscoveryStrategy::Exploration {
            assert!(report.false_supernode_links > 0, "{case}");
            assert!(
                report.honest_supernode_coverage_basis_points < 10_000,
                "{case}"
            );
        }
    }
    assert!(
        outcomes.len() >= 3,
        "discovery strategies must produce materially different selections: {outcomes:?}"
    );
}

#[test]
#[ignore = "bounded retained-state gate with tenfold adversarial duration and load"]
fn protocol_state_is_bounded_after_tenfold_spam() {
    let baseline = run(
        retained_state_stress_config(1),
        PeerSelectionMode::SharedReputation,
    );
    let stressed = run(
        retained_state_stress_config(10),
        PeerSelectionMode::SharedReputation,
    );
    let baseline_resources = baseline.resource_usage.honest_peers;
    let stressed_resources = stressed.resource_usage.honest_peers;
    let context = format!(
        "baseline={{spam_events:{} fake_inv:{} state_p95:{} state_max:{} protocol_p95:{} protocol_max:{}}} stressed={{spam_events:{} fake_inv:{} state_p95:{} state_max:{} protocol_p95:{} protocol_max:{}}}",
        baseline.spam_events,
        baseline.injected_attack_inventories,
        baseline_resources.final_retained.state_entries.p95,
        baseline_resources.final_retained.state_entries.max,
        baseline_resources.final_retained.protocol_content_bytes.p95,
        baseline_resources.final_retained.protocol_content_bytes.max,
        stressed.spam_events,
        stressed.injected_attack_inventories,
        stressed_resources.final_retained.state_entries.p95,
        stressed_resources.final_retained.state_entries.max,
        stressed_resources.final_retained.protocol_content_bytes.p95,
        stressed_resources.final_retained.protocol_content_bytes.max,
    );
    eprintln!("{context}");

    assert_eq!(stressed.spam_events, baseline.spam_events * 10, "{context}");
    assert!(
        stressed.injected_attack_inventories >= baseline.injected_attack_inventories * 8,
        "fixed unauthorized probes make total injected inventories slightly less than 10x: {context}"
    );
    assert_legitimate_delivery_is_safe(&stressed, "tenfold-retained-state");
    for report in [&baseline, &stressed] {
        let resources = report.resource_usage;
        assert!(resources.quiescence_at_ms >= report.virtual_duration_ms);
        assert_eq!(resources.honest_all.final_retained.queued_wire_bytes.max, 0);
        assert_eq!(
            resources.honest_peers.final_retained.cached_event_bytes.max,
            0
        );
        assert!(resources.honest_peers.peak_retained.cached_event_bytes.max <= 16 * 1024 * 1024);
    }
    assert_ten_percent_growth_bound(
        baseline_resources.final_retained.protocol_content_bytes.p95,
        stressed_resources.final_retained.protocol_content_bytes.p95,
        &context,
    );
    assert_ten_percent_growth_bound(
        baseline_resources.final_retained.state_entries.p95,
        stressed_resources.final_retained.state_entries.p95,
        &context,
    );
    assert_ten_percent_growth_bound(
        baseline_resources.final_retained.state_entries.max,
        stressed_resources.final_retained.state_entries.max,
        &context,
    );
}

fn retained_state_stress_config(multiplier: usize) -> SimulationConfig {
    SimulationConfig {
        node_count: 64,
        attacker_count: 16,
        fanout: 6,
        fake_inventories_per_attack_link: 6 * multiplier,
        signed_spam_rounds: 8 * multiplier,
        max_processed_actions: 5_000_000,
        seed: RELEASE_SEEDS[0],
        topology: TopologyStrategy::PeerMesh,
        loss_basis_points: 0,
        churn_basis_points: 0,
        supernode_count: 4,
        false_supernode_count: 2,
        ..SimulationConfig::default()
    }
}

fn assert_ten_percent_growth_bound(baseline: u64, stressed: u64, context: &str) {
    let allowed = baseline.saturating_add(baseline.saturating_add(9) / 10);
    assert!(
        stressed <= allowed,
        "tenfold spam grew quiescent retained state more than 10% ({baseline} -> {stressed}, allowed {allowed}): {context}"
    );
}

fn release_config(seed: u64, topology: TopologyStrategy) -> SimulationConfig {
    SimulationConfig {
        node_count: 1_000,
        attacker_count: 200,
        fanout: 6,
        unknown_peer_reserve: 1,
        max_hops: 16,
        fake_inventories_per_attack_link: 6,
        signed_spam_rounds: 8,
        legitimate_publication_rounds: 1,
        max_processed_actions: 10_000_000,
        seed,
        topology,
        supernode_discovery: SupernodeDiscoveryStrategy::Mixed,
        supernode_count: 16,
        false_supernode_count: 8,
        supernode_links_per_peer: 3,
        supernode_fanout: 192,
        loss_basis_points: 200,
        churn_basis_points: 300,
        retry_delay_ms: 80,
        max_retries: 3,
    }
}

fn run_modes(config: SimulationConfig) -> ModeReports {
    ModeReports {
        neutral: run(config.clone(), PeerSelectionMode::Neutral),
        local: run(config.clone(), PeerSelectionMode::LocalBehavior),
        shared: run(config, PeerSelectionMode::SharedReputation),
    }
}

fn run(config: SimulationConfig, mode: PeerSelectionMode) -> SimulationReport {
    let topology = config.topology;
    let seed = config.seed;
    run_simulation(config, mode).unwrap_or_else(|error| {
        panic!("seed={seed:#x} topology={topology:?} mode={mode:?} failed: {error}")
    })
}

fn assert_adversarial_scenario_was_exercised(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert_eq!(report.node_count, 1_000, "{context}");
    assert_eq!(report.attacker_count, 200, "{context}");
    assert!(report.spam_events > 0, "{context}");
    assert!(report.processed_actions > 0, "{context}");
    assert!(report.processed_messages > 0, "{context}");
    assert!(
        report.processed_messages <= report.processed_actions,
        "{context}"
    );
    assert!(
        report.processed_actions <= report.config.max_processed_actions,
        "{context}"
    );
    assert!(report.expected_signed_spam_deliveries > 0, "{context}");
    assert!(report.injected_attack_inventories > 0, "{context}");
    assert!(report.unauthorized_source_drops > 0, "{context}");
    assert!(report.rejected_malformed_messages > 0, "{context}");
    assert!(report.spam_filter_peer_link_opportunities > 0, "{context}");
    assert!(report.spam_filter_suppressed_peer_links > 0, "{context}");
    assert!(
        report.filter_suppression_basis_points > 0
            && report.filter_suppression_basis_points < 10_000,
        "filter gate must exercise both in-scope and near-miss spam: {context}"
    );
    assert!(report.dropped_packets > 0, "{context}");
    assert!(report.churned_links > 0, "{context}");
    assert!(report.subscription_retries > 0, "{context}");
    assert!(report.subscription_retry_recoveries > 0, "{context}");
    assert!(
        report.subscription_retry_recoveries <= report.subscription_retries,
        "{context}"
    );
    assert!(report.subscription_rejections > 0, "{context}");
    assert!(report.subscription_evictions > 0, "{context}");
    assert!(report.subscription_close_reopen_successes > 0, "{context}");
    assert!(report.retry_inventories > 0, "{context}");
    assert!(report.disrupted_legitimate_transfers > 0, "{context}");
    assert!(
        report.eventual_disrupted_transfer_recoveries > 0,
        "{context}"
    );
    assert!(
        report.eventual_disrupted_transfer_recovery_basis_points
            >= MIN_EVENTUAL_DISRUPTED_TRANSFER_RECOVERY_BPS,
        "{context}"
    );
}

fn assert_signed_spam_class_metrics(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert_signed_spam_class_shapes(report, &context);

    let (expected_total, delivered_total) = SUBSCRIPTION_CLASSES
        .into_iter()
        .map(|class| assert_signed_spam_class(report, class, &context))
        .fold((0usize, 0usize), |totals, class| {
            (
                totals.0.saturating_add(class.0),
                totals.1.saturating_add(class.1),
            )
        });
    assert_eq!(
        expected_total, report.expected_signed_spam_deliveries,
        "{context}"
    );
    assert_eq!(delivered_total, report.spam_delivered, "{context}");
    assert_eq!(
        class_total(
            &report.spam_filter_peer_link_opportunities_by_class,
            &SUBSCRIPTION_CLASSES,
        ),
        report.spam_filter_peer_link_opportunities,
        "{context}"
    );
    assert_eq!(
        class_total(
            &report.spam_filter_suppressed_peer_links_by_class,
            &SUBSCRIPTION_CLASSES,
        ),
        report.spam_filter_suppressed_peer_links,
        "{context}"
    );
    assert_exact_filter_classes(report, &context);
}

fn assert_signed_spam_class_shapes(report: &SimulationReport, context: &str) {
    assert_eq!(
        report.expected_signed_spam_deliveries_by_class.len(),
        SUBSCRIPTION_CLASSES.len(),
        "{context}"
    );
    assert_eq!(
        report.signed_spam_delivery_basis_points_by_class.len(),
        SUBSCRIPTION_CLASSES.len(),
        "{context}"
    );
    assert_eq!(
        report.spam_filter_suppression_basis_points_by_class.len(),
        SUBSCRIPTION_CLASSES.len(),
        "{context}"
    );
    assert!(
        report.signed_spam_deliveries_by_class.len() <= SUBSCRIPTION_CLASSES.len(),
        "{context}"
    );
}

fn assert_signed_spam_class(
    report: &SimulationReport,
    class: &str,
    context: &str,
) -> (usize, usize) {
    let expected = identity_value(
        &report.expected_signed_spam_deliveries_by_class,
        class,
        "expected signed-spam class",
        context,
    );
    let delivered = report
        .signed_spam_deliveries_by_class
        .get(class)
        .copied()
        .unwrap_or(0);
    let delivery_bps = identity_value(
        &report.signed_spam_delivery_basis_points_by_class,
        class,
        "signed-spam class basis points",
        context,
    );
    let filter_opportunities = report
        .spam_filter_peer_link_opportunities_by_class
        .get(class)
        .copied()
        .unwrap_or(0);
    let filter_suppressed = report
        .spam_filter_suppressed_peer_links_by_class
        .get(class)
        .copied()
        .unwrap_or(0);
    let filter_bps = identity_value(
        &report.spam_filter_suppression_basis_points_by_class,
        class,
        "filter class basis points",
        context,
    );

    assert!(delivered <= expected, "class={class}: {context}");
    assert!(delivery_bps <= 10_000, "class={class}: {context}");
    assert!(filter_opportunities > 0, "class={class}: {context}");
    assert!(
        filter_suppressed <= filter_opportunities,
        "class={class}: {context}"
    );
    assert_eq!(
        filter_bps,
        basis_points(filter_suppressed as u64, filter_opportunities as u64),
        "class={class}: {context}"
    );
    assert_eq!(
        delivery_bps,
        basis_points(delivered as u64, expected as u64),
        "class={class}: {context}"
    );
    (expected, delivered)
}

fn assert_exact_filter_classes(report: &SimulationReport, context: &str) {
    for class in EXACT_FILTER_CLASSES {
        assert_eq!(
            report
                .expected_signed_spam_deliveries_by_class
                .get(class)
                .copied(),
            Some(0),
            "exact filter accepted signed spam for class={class}: {context}"
        );
        assert_eq!(
            report
                .signed_spam_deliveries_by_class
                .get(class)
                .copied()
                .unwrap_or(0),
            0,
            "signed spam reached an exact-filter class={class}: {context}"
        );
        assert_eq!(
            report
                .spam_filter_suppression_basis_points_by_class
                .get(class)
                .copied(),
            Some(10_000),
            "exact-author filter did not suppress every adversarial link opportunity for class={class}: {context}"
        );
    }
}

fn assert_signed_spam_identity_metrics(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    let mut expected_total = 0usize;
    let mut delivered_total = 0usize;
    let mut expected_machine_total = 0usize;
    let mut delivered_machine_total = 0usize;

    for identity in SPAM_IDENTITIES {
        let expected = identity_value(
            &report.expected_signed_spam_deliveries_by_identity,
            identity,
            "expected signed-spam",
            &context,
        );
        let delivered = identity_value(
            &report.signed_spam_deliveries_by_identity,
            identity,
            "delivered signed-spam",
            &context,
        );
        let suppression = identity_value(
            &report.signed_spam_suppression_basis_points_by_identity,
            identity,
            "signed-spam suppression",
            &context,
        );
        let expected_machine = identity_value(
            &report.expected_machine_admitted_spam_deliveries_by_identity,
            identity,
            "expected machine-admitted spam",
            &context,
        );
        let delivered_machine = identity_value(
            &report.machine_admitted_spam_deliveries_by_identity,
            identity,
            "delivered machine-admitted spam",
            &context,
        );
        let machine_suppression = identity_value(
            &report.machine_admitted_spam_suppression_basis_points_by_identity,
            identity,
            "machine-admitted spam suppression",
            &context,
        );

        assert!(expected > 0, "identity={identity}: {context}");
        assert!(delivered <= expected, "identity={identity}: {context}");
        assert_eq!(
            suppression,
            basis_points(expected.saturating_sub(delivered) as u64, expected as u64),
            "identity={identity}: {context}"
        );
        assert!(expected_machine > 0, "identity={identity}: {context}");
        assert!(
            delivered_machine <= expected_machine,
            "identity={identity}: {context}"
        );
        assert_eq!(
            machine_suppression,
            basis_points(
                expected_machine.saturating_sub(delivered_machine) as u64,
                expected_machine as u64,
            ),
            "identity={identity}: {context}"
        );
        assert!(
            expected_machine <= expected && delivered_machine <= delivered,
            "machine-admitted identity lane must be a subset: identity={identity}: {context}"
        );

        expected_total = expected_total.saturating_add(expected);
        delivered_total = delivered_total.saturating_add(delivered);
        expected_machine_total = expected_machine_total.saturating_add(expected_machine);
        delivered_machine_total = delivered_machine_total.saturating_add(delivered_machine);
    }

    assert_eq!(
        expected_total, report.expected_signed_spam_deliveries,
        "identity expected deliveries must conserve: {context}"
    );
    assert_eq!(
        delivered_total, report.spam_delivered,
        "identity delivered spam must conserve: {context}"
    );
    assert_eq!(
        expected_machine_total,
        class_total(
            &report.expected_signed_spam_deliveries_by_class,
            &MACHINE_ADMITTED_CLASSES
        ),
        "machine identity expectations must conserve across machine-admitted classes: {context}"
    );
    assert_eq!(
        delivered_machine_total,
        class_total(
            &report.signed_spam_deliveries_by_class,
            &MACHINE_ADMITTED_CLASSES
        ),
        "machine identity deliveries must conserve across machine-admitted classes: {context}"
    );
}

fn identity_value<T: Copy>(
    values: &std::collections::BTreeMap<String, T>,
    identity: &str,
    metric: &str,
    context: &str,
) -> T {
    values
        .get(identity)
        .copied()
        .unwrap_or_else(|| panic!("missing {metric} identity={identity}: {context}"))
}

fn class_total(values: &std::collections::BTreeMap<String, usize>, classes: &[&str]) -> usize {
    classes
        .iter()
        .map(|class| values.get(*class).copied().unwrap_or(0))
        .sum()
}

fn assert_legitimate_delivery_is_safe(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert!(
        report.delivery_basis_points >= MIN_DELIVERY_BPS,
        "{context}"
    );
    assert!(
        report.worst_cohort_delivery_basis_points >= MIN_WORST_COHORT_BPS,
        "{context}"
    );
    assert_eq!(
        report.latency_sample_count, report.delivered_legitimate,
        "{context}"
    );
    assert_eq!(report.uninterested_deliveries, 0, "{context}");
    assert_eq!(report.uninterested_legitimate_deliveries, 0, "{context}");
    assert_eq!(report.uninterested_spam_deliveries, 0, "{context}");
    assert_eq!(report.legitimate_policy_drops, 0, "{context}");
    assert_eq!(report.legitimate_application_policy_drops, 0, "{context}");
    assert!(
        report.machine_ingress_accounting_is_conserved(),
        "{context}"
    );
    assert_eq!(report.machine_false_positive_removals, 0, "{context}");
}

fn assert_shared_reputation_improves_spam_resistance(reports: &ModeReports, case: &str) {
    let shared = &reports.shared;
    let local = &reports.local;
    let neutral = &reports.neutral;
    let context = report_context(shared, case);

    assert!(
        local.spam_delivered > 0,
        "local baseline had no spam: {case}"
    );
    let relative_improvement_holds = match shared.topology {
        TopologyStrategy::PeerMesh => {
            shared.spam_delivered.saturating_mul(5) <= local.spam_delivered.saturating_mul(4)
        }
        TopologyStrategy::HybridSupernodes => {
            shared.spam_delivered.saturating_mul(10) <= local.spam_delivered.saturating_mul(9)
        }
    };
    assert!(
        relative_improvement_holds,
        "shared reputation did not meet the topology-specific improvement floor: {context}; local_spam={}",
        local.spam_delivered
    );
    assert!(
        shared.spam_delivered < neutral.spam_delivered,
        "shared reputation did not improve the neutral baseline: {context}; neutral_spam={}",
        neutral.spam_delivered
    );
    let minimum_suppression = match shared.topology {
        TopologyStrategy::PeerMesh => MIN_PEER_MESH_SPAM_SUPPRESSION_BPS,
        TopologyStrategy::HybridSupernodes => MIN_HYBRID_SPAM_SUPPRESSION_BPS,
    };
    assert!(
        shared.spam_suppression_basis_points >= minimum_suppression,
        "{context}"
    );
    assert!(
        shared.delivery_basis_points >= neutral.delivery_basis_points
            && shared.delivery_basis_points >= local.delivery_basis_points,
        "shared policy regressed legitimate delivery: {context}; neutral_delivery={}; local_delivery={}",
        neutral.delivery_basis_points,
        local.delivery_basis_points
    );
    assert!(
        shared
            .spam_dropped_by_machine_policy
            .saturating_add(shared.adversarial_machine_ingress_drops)
            > 0,
        "machine reputation did not reject adversarial ingress or events: {context}"
    );
    assert!(shared.spam_dropped_by_application_policy > 0, "{context}");
    assert_machine_identity_learning_and_fresh_control(shared, local, &context);
}

fn assert_machine_identity_learning_and_fresh_control(
    shared: &SimulationReport,
    local: &SimulationReport,
    context: &str,
) {
    let shared_persistent_expected =
        shared.expected_machine_admitted_spam_deliveries_by_identity["persistent"];
    let shared_persistent = shared.machine_admitted_spam_deliveries_by_identity["persistent"];
    let local_persistent_expected =
        local.expected_machine_admitted_spam_deliveries_by_identity["persistent"];
    let local_persistent = local.machine_admitted_spam_deliveries_by_identity["persistent"];
    assert!(
        (shared_persistent as u128)
            .saturating_mul(local_persistent_expected as u128)
            .saturating_mul(2)
            <= (local_persistent as u128).saturating_mul(shared_persistent_expected as u128),
        "shared persistent machine-spam delivery rate must be at most half local: {context}; local={local_persistent}/{local_persistent_expected}; shared={shared_persistent}/{shared_persistent_expected}"
    );
    let persistent_suppression =
        shared.machine_admitted_spam_suppression_basis_points_by_identity["persistent"];
    assert!(
        persistent_suppression >= MIN_PERSISTENT_MACHINE_SPAM_SUPPRESSION_BPS,
        "persistent machine-spam suppression did not meet the learned-identity floor: {context}"
    );

    let fresh_expected =
        shared.expected_machine_admitted_spam_deliveries_by_identity["fresh-sybil"];
    let fresh = shared.machine_admitted_spam_deliveries_by_identity["fresh-sybil"];
    match shared.topology {
        TopologyStrategy::PeerMesh => {
            let local_fresh_expected =
                local.expected_machine_admitted_spam_deliveries_by_identity["fresh-sybil"];
            let local_fresh = local.machine_admitted_spam_deliveries_by_identity["fresh-sybil"];
            assert!(
                (fresh as u128).saturating_mul(local_fresh_expected as u128)
                    >= (local_fresh as u128).saturating_mul(fresh_expected as u128),
                "shared policy must not suppress fresh unknowns beyond the lossy local control: {context}; local={local_fresh}/{local_fresh_expected}; shared={fresh}/{fresh_expected}"
            );
        }
        TopologyStrategy::HybridSupernodes => assert!(
            fresh > 0
                && (fresh as u128).saturating_mul(shared_persistent_expected as u128)
                    > (shared_persistent as u128).saturating_mul(fresh_expected as u128),
            "hybrid fresh-unknown control must remain exercised and more open than learned persistent identities: {context}; persistent={shared_persistent}/{shared_persistent_expected}; fresh={fresh}/{fresh_expected}"
        ),
    }
}

fn assert_machine_reputation_used_real_transport(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert!(report.machine_ratings_published > 0, "{context}");
    assert!(report.machine_ratings_received > 0, "{context}");
    assert!(report.machine_ratings_ingested > 0, "{context}");
    assert!(report.poisoned_machine_ratings_published > 0, "{context}");
    assert!(report.poisoned_machine_ratings_received > 0, "{context}");
    assert!(report.poisoned_machine_ratings_ingested > 0, "{context}");
    assert!(
        report.poisoned_machine_ratings_received >= report.poisoned_machine_ratings_ingested,
        "{context}"
    );
    assert!(report.forged_machine_ratings_published > 0, "{context}");
    assert!(report.forged_machine_ratings_received > 0, "{context}");
    assert!(report.forged_machine_ratings_evaluated > 0, "{context}");
    assert_eq!(report.forged_machine_ratings_ingested, 0, "{context}");
    assert_eq!(
        report.forged_machine_ratings_rejected, report.forged_machine_ratings_evaluated,
        "{context}"
    );
    assert!(report.machine_transported_transitions > 0, "{context}");
    assert!(
        report.machine_transported_positive_admissions > 0,
        "{context}"
    );
    assert!(report.machine_transported_removals > 0, "{context}");
    assert_eq!(report.machine_lifecycle_ratings_published, 3, "{context}");
    assert!(report.machine_lifecycle_admissions > 0, "{context}");
    assert!(report.machine_lifecycle_removals > 0, "{context}");
    assert!(report.machine_lifecycle_readmissions > 0, "{context}");
    assert!(report.machine_reversible_lifecycles > 0, "{context}");
    assert!(report.machine_positive_admissions > 0, "{context}");
    assert!(report.machine_removals > 0, "{context}");
    assert!(report.machine_poisoning_removals > 0, "{context}");
    assert!(report.machine_removal_latency_p95_ms > 0, "{context}");
    assert!(report.machine_trust_edges > 0, "{context}");
}

fn assert_honest_resource_accounting(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    let resources = report.resource_usage;
    let all = resources.honest_all;
    let peers = resources.honest_peers;
    let supernodes = resources.honest_supernodes;
    assert_eq!(
        all.combined_bytes.count, report.honest_node_count,
        "{context}"
    );
    assert_eq!(
        peers
            .combined_bytes
            .count
            .saturating_add(supernodes.combined_bytes.count),
        report.honest_node_count,
        "{context}"
    );
    assert_eq!(
        all.combined_bytes.total,
        all.sent_bytes
            .total
            .saturating_add(all.received_bytes.total),
        "endpoint I/O must conserve: {context}"
    );
    assert!(peers.combined_messages.p95 > 0, "{context}");
    assert_eq!(
        all.adversarial_combined_bytes.total, resources.honest_adversarial_combined_bytes,
        "honest adversarial I/O must conserve: {context}"
    );
    assert!(peers.cpu_work.codec_bytes.p95 > 0, "{context}");
    assert!(peers.cpu_work.signature_checks.p95 > 0, "{context}");
    assert!(peers.cpu_work.avoided_signature_checks.p95 > 0, "{context}");
    if report.mode == PeerSelectionMode::SharedReputation {
        let without_fast_paths = peers.cpu_work.signature_checks_without_verified_paths.p95;
        let reduction = without_fast_paths.saturating_sub(peers.cpu_work.signature_checks.p95);
        assert!(
            basis_points(reduction, without_fast_paths)
                >= MIN_VERIFIED_PATH_SIGNATURE_REDUCTION_BPS,
            "verified paths must reduce honest-peer p95 signature work by at least 20%: {context}"
        );
    }
    assert!(peers.cpu_work.filter_candidates.p95 > 0, "{context}");
    assert!(peers.peak_retained.exact_content_bytes.p95 > 0, "{context}");
    assert!(peers.peak_retained.state_entries.p95 > 0, "{context}");
    assert_eq!(all.final_retained.queued_wire_bytes.max, 0, "{context}");
    assert!(
        peers.peak_retained.exact_content_bytes.max >= peers.final_retained.exact_content_bytes.max,
        "{context}"
    );
    assert!(
        peers.peak_retained.cached_event_bytes.max <= 16 * 1024 * 1024,
        "ordinary peer cache cap exceeded: {context}"
    );
    assert!(resources.attacker_adversarial_sent_bytes > 0, "{context}");
    assert!(
        resources.victim_bandwidth_amplification_basis_points > 0,
        "{context}"
    );
    if report.topology == TopologyStrategy::HybridSupernodes {
        assert_eq!(supernodes.combined_bytes.count, report.supernode_count);
        assert!(supernodes.cpu_work.codec_bytes.p95 > 0, "{context}");
        assert!(
            supernodes.peak_retained.cached_event_bytes.max <= 256 * 1024 * 1024,
            "supernode cache cap exceeded: {context}"
        );
    }
}

fn assert_supernode_discovery_and_service(reports: &ModeReports, case: &str) {
    for report in reports.all() {
        assert_supernode_report(report, case);
        let context = report_context(report, case);
        assert!(report.false_supernode_links > 0, "{context}");
    }
}

fn assert_supernode_report(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert_eq!(report.supernode_count, 16, "{context}");
    assert!(report.discovery_links > 0, "{context}");
    assert!(report.honest_supernode_links > 0, "{context}");
    assert!(
        report.supernode_discovery_precision_basis_points > 0,
        "{context}"
    );
    assert!(
        report.honest_supernode_coverage_basis_points > 0,
        "{context}"
    );
    assert!(report.supernode_max_service_bytes > 0, "{context}");
    assert!(report.supernode_mean_service_bytes > 0, "{context}");
    assert!(
        role_sent_legitimate_bytes(report, NodeRole::Supernode) > 0,
        "{context}"
    );
    assert!(
        report
            .interested_delivery_credit_by_source_role
            .get(&NodeRole::Supernode)
            .copied()
            .unwrap_or(0)
            > 0,
        "{context}"
    );
    assert!(
        role_service_bytes(report, NodeRole::Supernode) > 0,
        "{context}"
    );
}

fn role_service_bytes(report: &SimulationReport, role: NodeRole) -> u64 {
    report
        .protocol_service_by_role
        .get(&role)
        .map_or(0, |ledger| ledger.total(TrafficScope::Combined).bytes)
}

fn role_sent_legitimate_bytes(report: &SimulationReport, role: NodeRole) -> u64 {
    report
        .protocol_service_by_role
        .get(&role)
        .map_or(0, |ledger| {
            ledger
                .counter(
                    nostr_pubsub_sim::TrafficDirection::Sent,
                    nostr_pubsub_sim::TrafficProvenance::Legitimate,
                )
                .bytes
        })
}

fn report_context(report: &SimulationReport, case: &str) -> String {
    format!(
        "{case} mode={} delivery={} worst={} spam={} suppression={} recovery={} processed={} honest_ingress_drops={} transported_transitions={} quiet_blackhole_removals={} poison_ingests={} poisoning_removals={} false_removals={}",
        report.mode.as_str(),
        report.delivery_basis_points,
        report.worst_cohort_delivery_basis_points,
        report.spam_delivered,
        report.spam_suppression_basis_points,
        report.eventual_disrupted_transfer_recovery_basis_points,
        report.processed_messages,
        report.honest_source_legitimate_machine_ingress_drops,
        report.machine_transported_transitions,
        report.machine_quiet_blackhole_removals,
        report.poisoned_machine_ratings_ingested,
        report.machine_poisoning_removals,
        report.machine_false_positive_removals,
    )
}
