use std::collections::{BTreeMap, HashMap, HashSet};

use nostr::Keys;
use nostr_pubsub::{
    DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES, DEFAULT_INV_WANT_MAX_WIRE_BYTES, FipsPubsubWireCodec,
    InvWantCodec,
};
use nostr_pubsub_social_graph::PeerRatingPublisher;

#[cfg(test)]
use super::WorkloadPair;
use super::{
    EventMetadata, NodeResourceLedger, POST_RECONNECT_REPUTATION_SWEEP_MS,
    POST_ROUTE_REPUTATION_SWEEP_MS, PeerSelectionMode, REPUTATION_SWEEP_MS, Result, SIM_PROTOCOL,
    SIM_VERSION, ScheduledAction, SimNode, Simulation, SimulationConfig, SimulationReport,
    basis_points, mix64, pubsub_error, simulation_keys,
};
use crate::clock::VirtualScheduler;
use crate::metrics::NodeTrafficLedger;
use crate::topology::{SupernodeDiscoveryStrategy, TopologyResult, TopologyStrategy};
use crate::workload::SubscriptionClass;

#[path = "setup/config.rs"]
mod config;
#[path = "setup/network.rs"]
mod network;
#[path = "setup/nodes.rs"]
mod nodes;
#[path = "setup/subscriptions.rs"]
mod subscriptions;
#[cfg(test)]
#[path = "setup/tests.rs"]
mod tests;
#[path = "setup/workloads.rs"]
mod workloads;

use super::reputation_flow::build_reputation_publishers;
use config::validate_config;
use network::build_sim_topology;
use nodes::{build_node, observed_established_history};
use workloads::{build_workloads, node_filters};

impl Simulation {
    pub(super) fn new(config: SimulationConfig, mode: PeerSelectionMode) -> Result<Self> {
        validate_config(&config)?;
        let prepared = prepare_simulation(&config, mode)?;
        let mut simulation = assemble_simulation(config, mode, prepared)?;
        simulation.initialize_rating_filters()?;
        simulation.initialize_resource_usage()?;
        simulation.populate_interest_sets()?;
        Ok(simulation)
    }

    pub(super) fn run(mut self) -> Result<SimulationReport> {
        self.install_subscriptions()?;
        self.drain_scheduler()?;
        self.exercise_subscription_lifecycle()?;
        self.drain_scheduler()?;
        self.exercise_machine_lifecycle()?;
        self.exercise_adversarial_reputation_probes()?;
        self.drain_scheduler()?;
        self.schedule_attack_pressure()?;
        let workload_start_ms = self.scheduler.now_ms();
        self.scheduler.schedule_at(
            workload_start_ms.saturating_add(REPUTATION_SWEEP_MS),
            ScheduledAction::ReputationSweep,
        );
        self.scheduler.schedule_at(
            workload_start_ms.saturating_add(POST_ROUTE_REPUTATION_SWEEP_MS),
            ScheduledAction::ReputationSweep,
        );
        self.scheduler.schedule_at(
            workload_start_ms.saturating_add(POST_RECONNECT_REPUTATION_SWEEP_MS),
            ScheduledAction::ReputationSweep,
        );
        self.schedule_churn();
        self.schedule_publications();
        self.drain_scheduler()?;
        self.finalize_report()?;
        Ok(self.report)
    }
}

struct PreparedSimulation {
    keys: Vec<Keys>,
    peer_ids: Vec<String>,
    peer_indices: HashMap<String, usize>,
    topology: TopologyResult,
    nodes: Vec<SimNode>,
    events: HashMap<String, EventMetadata>,
    #[cfg(test)]
    workload_pairs: Vec<WorkloadPair>,
    reputation_publishers: Vec<Option<PeerRatingPublisher>>,
}

fn prepare_simulation(
    config: &SimulationConfig,
    mode: PeerSelectionMode,
) -> Result<PreparedSimulation> {
    let keys = simulation_keys(config.node_count)?;
    let peer_ids = keys
        .iter()
        .map(|keys| keys.public_key().to_hex())
        .collect::<Vec<_>>();
    let peer_indices = peer_ids
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, peer_id)| (peer_id, index))
        .collect::<HashMap<_, _>>();
    let topology = build_sim_topology(config, claimed_cohort_ids(config))?;
    let (events, workload_pairs) = build_workloads(config, &keys, &topology, &peer_ids)?;
    let signed_history = events
        .values()
        .map(|metadata| metadata.verified.clone())
        .collect::<Vec<_>>();
    let established_history = observed_established_history(&workload_pairs, &signed_history);
    let filters = node_filters(config, &topology, &workload_pairs);
    let nodes = filters
        .into_iter()
        .enumerate()
        .map(|(node_index, filters)| {
            build_node(
                config,
                mode,
                node_index,
                &peer_ids,
                &topology,
                filters,
                &established_history,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let reputation_publishers = build_reputation_publishers(mode, config, &peer_ids)?;
    Ok(PreparedSimulation {
        keys,
        peer_ids,
        peer_indices,
        topology,
        nodes,
        events,
        #[cfg(test)]
        workload_pairs,
        reputation_publishers,
    })
}

fn assemble_simulation(
    config: SimulationConfig,
    mode: PeerSelectionMode,
    prepared: PreparedSimulation,
) -> Result<Simulation> {
    let PreparedSimulation {
        keys,
        peer_ids,
        peer_indices,
        topology,
        nodes,
        events,
        #[cfg(test)]
        workload_pairs,
        reputation_publishers,
    } = prepared;
    let report = initial_report(&config, mode, &topology, &nodes);
    let traffic = vec![NodeTrafficLedger::default(); config.node_count];
    let node_resources = vec![NodeResourceLedger::default(); config.node_count];
    Ok(Simulation {
        codec: InvWantCodec::new(SIM_PROTOCOL, SIM_VERSION, DEFAULT_INV_WANT_MAX_WIRE_BYTES),
        fips_codec: FipsPubsubWireCodec::new(DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES)
            .map_err(pubsub_error)?,
        scheduler: VirtualScheduler::default(),
        delivery_times: HashMap::new(),
        down_links: HashSet::new(),
        fault_attempts: HashMap::new(),
        retry_counts: HashMap::new(),
        retry_needed: HashSet::new(),
        disrupted_transfers: HashSet::new(),
        bad_observed_at: HashMap::new(),
        reputation_events: HashMap::new(),
        reputation_publishers,
        rating_receipts: HashSet::new(),
        machine_lifecycle_progress: HashMap::new(),
        reputation_removal_latencies: Vec::new(),
        forged_rating_published: false,
        poisoned_rating_published: false,
        traffic,
        node_resources,
        link_traffic: BTreeMap::new(),
        delivery_credits: BTreeMap::new(),
        report,
        config,
        mode,
        keys,
        peer_ids,
        peer_indices,
        topology,
        nodes,
        events,
        #[cfg(test)]
        workload_pairs,
    })
}

fn initial_report(
    config: &SimulationConfig,
    mode: PeerSelectionMode,
    topology: &TopologyResult,
    nodes: &[SimNode],
) -> SimulationReport {
    let discovery = &topology.discovery_selections;
    let discovered_supernode_links = discovery
        .honest_supernode_links
        .saturating_add(discovery.false_supernode_links);
    let machine_trust_edges = nodes
        .iter()
        .skip(config.attacker_count)
        .map(|node| node.machine_trusted_raters.len())
        .sum();
    SimulationReport {
        config: config.clone(),
        mode,
        topology: config.topology,
        discovery: config.supernode_discovery,
        node_count: config.node_count,
        attacker_count: config.attacker_count,
        honest_node_count: config.node_count - config.attacker_count,
        supernode_count: topology.honest_supernodes.len(),
        topology_edges: topology.edge_count(),
        max_node_degree: topology.neighbors.iter().map(Vec::len).max().unwrap_or(0),
        discovery_links: discovery.total_links(),
        honest_supernode_links: discovery.honest_supernode_links,
        false_supernode_links: discovery.false_supernode_links,
        supernode_discovery_precision_basis_points: basis_points(
            u64::try_from(discovery.honest_supernode_links).unwrap_or(u64::MAX),
            u64::try_from(discovered_supernode_links).unwrap_or(u64::MAX),
        ),
        honest_supernode_coverage_basis_points: discovery.honest_coverage_basis_points(),
        false_only_supernode_peers: discovery
            .candidate_peer_count
            .saturating_sub(discovery.peers_with_honest_supernode),
        machine_trust_edges,
        ..EMPTY_REPORT_TEMPLATE.clone()
    }
}

const EMPTY_REPORT_CONFIG: SimulationConfig = SimulationConfig {
    node_count: 0,
    attacker_count: 0,
    fanout: 0,
    unknown_peer_reserve: 0,
    max_hops: 0,
    fake_inventories_per_attack_link: 0,
    signed_spam_rounds: 0,
    max_processed_actions: 0,
    seed: 0,
    topology: TopologyStrategy::PeerMesh,
    supernode_discovery: SupernodeDiscoveryStrategy::Mixed,
    supernode_count: 0,
    false_supernode_count: 0,
    supernode_links_per_peer: 0,
    supernode_fanout: 0,
    loss_basis_points: 0,
    churn_basis_points: 0,
    retry_delay_ms: 0,
    max_retries: 0,
};

const EMPTY_REPORT_TEMPLATE: SimulationReport = SimulationReport {
    config: EMPTY_REPORT_CONFIG,
    mode: PeerSelectionMode::Neutral,
    topology: TopologyStrategy::PeerMesh,
    discovery: SupernodeDiscoveryStrategy::Mixed,
    node_count: 0,
    attacker_count: 0,
    honest_node_count: 0,
    supernode_count: 0,
    topology_edges: 0,
    max_node_degree: 0,
    legitimate_events: 0,
    spam_events: 0,
    expected_legitimate_deliveries: 0,
    expected_signed_spam_deliveries: 0,
    expected_signed_spam_deliveries_by_class: BTreeMap::new(),
    expected_signed_spam_deliveries_by_identity: BTreeMap::new(),
    expected_machine_admitted_spam_deliveries_by_identity: BTreeMap::new(),
    spam_filter_peer_link_opportunities: 0,
    spam_filter_peer_link_opportunities_by_class: BTreeMap::new(),
    spam_filter_suppressed_peer_links: 0,
    spam_filter_suppressed_peer_links_by_class: BTreeMap::new(),
    spam_filter_suppression_basis_points_by_class: BTreeMap::new(),
    delivered_legitimate: 0,
    local_legitimate_deliveries: 0,
    delivery_basis_points: 0,
    worst_cohort_delivery_basis_points: 0,
    cohort_delivery_basis_points: BTreeMap::new(),
    latency_sample_count: 0,
    latency_p50_ms: 0,
    latency_p95_ms: 0,
    latency_p99_ms: 0,
    max_delivered_latency_ms: 0,
    undelivered_legitimate: 0,
    spam_delivered: 0,
    signed_spam_deliveries_by_class: BTreeMap::new(),
    signed_spam_deliveries_by_identity: BTreeMap::new(),
    machine_admitted_spam_deliveries_by_identity: BTreeMap::new(),
    signed_spam_delivery_basis_points: 0,
    signed_spam_delivery_basis_points_by_class: BTreeMap::new(),
    signed_spam_suppression_basis_points_by_identity: BTreeMap::new(),
    machine_admitted_spam_suppression_basis_points_by_identity: BTreeMap::new(),
    unknown_discovery_adverts_delivered: 0,
    spam_dropped_by_machine_policy: 0,
    spam_dropped_by_application_policy: 0,
    spam_suppression_basis_points: 0,
    uninterested_deliveries: 0,
    uninterested_legitimate_deliveries: 0,
    uninterested_spam_deliveries: 0,
    filter_suppression_basis_points: 0,
    processed_actions: 0,
    processed_messages: 0,
    inventory_messages: 0,
    want_messages: 0,
    frame_messages: 0,
    data_plane_wire_bytes: 0,
    legitimate_protocol_bytes: 0,
    adversarial_protocol_bytes: 0,
    legitimate_protocol_byte_share_basis_points: 0,
    protocol_messages_per_interested_delivery_milli: 0,
    dropped_packets: 0,
    dropped_at_attackers: 0,
    retry_inventories: 0,
    eventual_disrupted_transfer_recoveries: 0,
    disrupted_legitimate_transfers: 0,
    eventual_disrupted_transfer_recovery_basis_points: 0,
    max_queue_depth: 0,
    virtual_duration_ms: 0,
    injected_attack_inventories: 0,
    rejected_malformed_messages: 0,
    unauthorized_source_drops: 0,
    machine_ingress_drops: 0,
    honest_source_legitimate_machine_ingress_drops: 0,
    attacker_source_legitimate_reference_machine_ingress_drops: 0,
    adversarial_machine_ingress_drops: 0,
    machine_ratings_published: 0,
    machine_ratings_received: 0,
    machine_ratings_ingested: 0,
    poisoned_machine_ratings_published: 0,
    poisoned_machine_ratings_received: 0,
    poisoned_machine_ratings_ingested: 0,
    poisoned_machine_ratings_rejected: 0,
    machine_transported_transitions: 0,
    machine_transported_positive_admissions: 0,
    machine_transported_removals: 0,
    machine_lifecycle_ratings_published: 0,
    machine_lifecycle_admissions: 0,
    machine_lifecycle_removals: 0,
    machine_lifecycle_readmissions: 0,
    machine_reversible_lifecycles: 0,
    machine_positive_admissions: 0,
    machine_removals: 0,
    machine_quiet_blackhole_removals: 0,
    machine_poisoning_removals: 0,
    machine_false_positive_removals: 0,
    machine_removal_latency_p95_ms: 0,
    forged_machine_ratings_published: 0,
    forged_machine_ratings_received: 0,
    forged_machine_ratings_evaluated: 0,
    forged_machine_ratings_ingested: 0,
    forged_machine_ratings_rejected: 0,
    legitimate_policy_drops: 0,
    legitimate_application_policy_drops: 0,
    machine_trust_edges: 0,
    subscription_messages: 0,
    control_plane_wire_bytes: 0,
    subscription_retries: 0,
    subscription_retry_recoveries: 0,
    subscription_rejections: 0,
    subscription_evictions: 0,
    subscription_close_reopen_successes: 0,
    unknown_candidate_sends: 0,
    churned_links: 0,
    discovery_links: 0,
    honest_supernode_links: 0,
    false_supernode_links: 0,
    supernode_discovery_precision_basis_points: 0,
    honest_supernode_coverage_basis_points: 0,
    false_only_supernode_peers: 0,
    supernode_max_service_bytes: 0,
    supernode_mean_service_bytes: 0,
    supernode_load_gini_basis_points: 0,
    total_protocol_bytes: 0,
    sent_link_protocol_bytes: 0,
    sent_role_protocol_bytes: 0,
    protocol_bytes_per_interested_delivery: 0,
    resource_usage: super::SimulationResourceReport::EMPTY,
    protocol_service_by_link: BTreeMap::new(),
    protocol_service_by_role: BTreeMap::new(),
    interested_delivery_credit_by_link: BTreeMap::new(),
    interested_delivery_credit_by_source_role: BTreeMap::new(),
};

fn claimed_cohort_ids(config: &SimulationConfig) -> Vec<u32> {
    let cohort_count = u64::try_from(SubscriptionClass::ALL.len()).unwrap_or(1);
    (0..config.node_count)
        .map(|index| {
            if index < config.attacker_count {
                let identity = u64::try_from(index).unwrap_or(u64::MAX);
                u32::try_from(mix64(config.seed ^ identity.rotate_left(19)) % cohort_count)
                    .unwrap_or_default()
            } else {
                u32::try_from((index - config.attacker_count) % SubscriptionClass::ALL.len())
                    .unwrap_or_default()
            }
        })
        .collect()
}
