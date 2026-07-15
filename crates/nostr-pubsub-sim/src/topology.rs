//! Deterministic bounded topology construction for pubsub simulations.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyStrategy {
    PeerMesh,
    HybridSupernodes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupernodeDiscoveryStrategy {
    Bootstrap,
    InterestAffinity,
    Exploration,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeRole {
    Peer,
    Supernode,
    Attacker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerMeshConfig {
    pub same_cohort_shortcuts_per_node: usize,
    pub cross_cohort_links_per_node: usize,
    pub attacker_links_per_honest_node: usize,
    pub max_peer_degree: usize,
    pub max_attacker_degree: usize,
}

impl Default for PeerMeshConfig {
    fn default() -> Self {
        Self {
            same_cohort_shortcuts_per_node: 2,
            cross_cohort_links_per_node: 1,
            attacker_links_per_honest_node: 2,
            max_peer_degree: 16,
            max_attacker_degree: 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridSupernodeConfig {
    pub discovery: SupernodeDiscoveryStrategy,
    pub honest_supernode_count: usize,
    pub adversarial_discovery_candidate_count: usize,
    pub candidate_links_per_peer: usize,
    pub exploration_links_per_peer: usize,
    pub max_peer_degree: usize,
    pub max_supernode_degree: usize,
    pub max_attacker_degree: usize,
}

impl Default for HybridSupernodeConfig {
    fn default() -> Self {
        Self {
            discovery: SupernodeDiscoveryStrategy::Mixed,
            honest_supernode_count: 8,
            adversarial_discovery_candidate_count: 4,
            candidate_links_per_peer: 3,
            exploration_links_per_peer: 1,
            max_peer_degree: 16,
            max_supernode_degree: 128,
            max_attacker_degree: 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyConfig {
    pub node_count: usize,
    pub attacker_count: usize,
    pub cohort_ids: Vec<u32>,
    pub seed: u64,
    pub strategy: TopologyStrategy,
    pub peer_mesh: PeerMeshConfig,
    pub hybrid: HybridSupernodeConfig,
}

impl TopologyConfig {
    #[must_use]
    pub fn new(
        node_count: usize,
        attacker_count: usize,
        cohort_ids: Vec<u32>,
        seed: u64,
        strategy: TopologyStrategy,
    ) -> Self {
        Self {
            node_count,
            attacker_count,
            cohort_ids,
            seed,
            strategy,
            peer_mesh: PeerMeshConfig::default(),
            hybrid: HybridSupernodeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoverySelectionCounts {
    pub bootstrap_links: usize,
    pub interest_affinity_links: usize,
    pub exploration_links: usize,
    /// Selected links whose remote endpoint was high-capacity in hidden ground truth.
    pub selected_high_capacity_links: usize,
    pub selected_adversarial_candidate_links: usize,
    pub candidate_peer_count: usize,
    pub peers_selecting_high_capacity: usize,
}

impl DiscoverySelectionCounts {
    #[must_use]
    pub const fn total_links(&self) -> usize {
        self.bootstrap_links
            .saturating_add(self.interest_affinity_links)
            .saturating_add(self.exploration_links)
    }

    #[must_use]
    pub fn high_capacity_selection_coverage_basis_points(&self) -> u32 {
        if self.candidate_peer_count == 0 {
            return 0;
        }
        let covered = self.peers_selecting_high_capacity.saturating_mul(10_000);
        u32::try_from(covered / self.candidate_peer_count).unwrap_or(u32::MAX)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyResult {
    pub neighbors: Vec<Vec<usize>>,
    /// Directed ownership of role-blind endpoint connections. This is routing
    /// provenance, not an endpoint role or capacity advertisement.
    pub outbound_discovery_neighbors: Vec<Vec<usize>>,
    pub roles: Vec<NodeRole>,
    pub cohort_ids: Vec<u32>,
    pub honest_supernodes: Vec<usize>,
    pub adversarial_discovery_candidates: Vec<usize>,
    pub discovery_selections: DiscoverySelectionCounts,
}

impl TopologyResult {
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.neighbors.iter().map(Vec::len).sum::<usize>() / 2
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyError {
    InvalidConfig(String),
    InsufficientCapacity(String),
}

impl fmt::Display for TopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid topology configuration: {message}")
            }
            Self::InsufficientCapacity(message) => {
                write!(formatter, "insufficient topology capacity: {message}")
            }
        }
    }
}

impl std::error::Error for TopologyError {}

/// Builds one deterministic, bounded topology from `config`.
///
/// # Errors
///
/// Returns [`TopologyError::InvalidConfig`] when the requested strategy cannot
/// satisfy its invariants, or [`TopologyError::InsufficientCapacity`] when the
/// configured degree bounds cannot hold required connectivity edges.
pub fn build_topology(config: &TopologyConfig) -> Result<TopologyResult, TopologyError> {
    validate_common(config)?;
    match config.strategy {
        TopologyStrategy::PeerMesh => build_peer_mesh(config),
        TopologyStrategy::HybridSupernodes => build_hybrid_supernodes(config),
    }
}

#[derive(Debug, Clone, Copy)]
struct DegreeCaps {
    peer: usize,
    supernode: usize,
    attacker: usize,
}

struct BoundedGraph {
    neighbors: Vec<BTreeSet<usize>>,
    outbound_discovery_neighbors: Vec<BTreeSet<usize>>,
    discovery_inbound: Vec<usize>,
    roles: Vec<NodeRole>,
    caps: DegreeCaps,
}

impl BoundedGraph {
    fn new(roles: Vec<NodeRole>, caps: DegreeCaps) -> Self {
        Self {
            neighbors: vec![BTreeSet::new(); roles.len()],
            outbound_discovery_neighbors: vec![BTreeSet::new(); roles.len()],
            discovery_inbound: vec![0; roles.len()],
            roles,
            caps,
        }
    }

    fn cap(&self, node: usize) -> usize {
        match self.roles[node] {
            NodeRole::Peer => self.caps.peer,
            NodeRole::Supernode => self.caps.supernode,
            NodeRole::Attacker => self.caps.attacker,
        }
    }

    fn can_add(&self, left: usize, right: usize) -> bool {
        left != right
            && !self.neighbors[left].contains(&right)
            && self.neighbors[left].len() < self.cap(left)
            && self.neighbors[right].len() < self.cap(right)
    }

    fn add(&mut self, left: usize, right: usize) -> bool {
        if !self.can_add(left, right) {
            return false;
        }
        self.neighbors[left].insert(right);
        self.neighbors[right].insert(left);
        true
    }

    /// Attempt a role-blind outbound connection. The selector sees only the
    /// boolean result; the remote endpoint privately enforces its local limit.
    fn add_discovered(&mut self, source: usize, candidate: usize) -> bool {
        let inbound_limit = self.cap(candidate);
        if self.discovery_inbound[candidate] >= inbound_limit || !self.add(source, candidate) {
            return false;
        }
        self.discovery_inbound[candidate] += 1;
        self.outbound_discovery_neighbors[source].insert(candidate);
        true
    }

    fn finish(self) -> (Vec<Vec<usize>>, Vec<Vec<usize>>, Vec<NodeRole>) {
        (
            self.neighbors
                .into_iter()
                .map(|neighbors| neighbors.into_iter().collect())
                .collect(),
            self.outbound_discovery_neighbors
                .into_iter()
                .map(|neighbors| neighbors.into_iter().collect())
                .collect(),
            self.roles,
        )
    }
}

fn validate_common(config: &TopologyConfig) -> Result<(), TopologyError> {
    if config.node_count == 0 {
        return Err(invalid("node_count must be non-zero"));
    }
    if config.attacker_count >= config.node_count {
        return Err(invalid(
            "attacker_count must leave at least one honest node",
        ));
    }
    if config.cohort_ids.len() != config.node_count {
        return Err(invalid(format!(
            "cohort_ids has {} entries for {} nodes",
            config.cohort_ids.len(),
            config.node_count
        )));
    }
    Ok(())
}

fn build_peer_mesh(config: &TopologyConfig) -> Result<TopologyResult, TopologyError> {
    let peer = &config.peer_mesh;
    validate_peer_mesh(config, peer)?;
    let roles = base_roles(config.node_count, config.attacker_count);
    let mut graph = BoundedGraph::new(
        roles,
        DegreeCaps {
            peer: peer.max_peer_degree,
            supernode: peer.max_peer_degree,
            attacker: peer.max_attacker_degree,
        },
    );
    let mut rng = DeterministicRng::new(config.seed);
    let mut cohorts = honest_cohorts(config);

    for nodes in cohorts.values_mut() {
        rng.shuffle(nodes);
        connect_ring(&mut graph, nodes)?;
    }
    connect_cohort_backbone(&mut graph, &cohorts, &mut rng)?;
    add_same_cohort_shortcuts(
        &mut graph,
        &cohorts,
        peer.same_cohort_shortcuts_per_node,
        &mut rng,
    );
    add_cross_cohort_links(
        &mut graph,
        config,
        peer.cross_cohort_links_per_node,
        &mut rng,
    );
    add_attacker_exposure(
        &mut graph,
        config,
        peer.attacker_links_per_honest_node,
        &mut rng,
    );

    let (neighbors, outbound_discovery_neighbors, roles) = graph.finish();
    Ok(TopologyResult {
        neighbors,
        outbound_discovery_neighbors,
        roles,
        cohort_ids: config.cohort_ids.clone(),
        honest_supernodes: Vec::new(),
        adversarial_discovery_candidates: Vec::new(),
        discovery_selections: DiscoverySelectionCounts::default(),
    })
}

fn validate_peer_mesh(config: &TopologyConfig, peer: &PeerMeshConfig) -> Result<(), TopologyError> {
    let cohort_count = honest_cohorts(config).len();
    let minimum_peer_degree = if cohort_count > 1 { 4 } else { 2 };
    if peer.max_peer_degree < minimum_peer_degree {
        return Err(invalid(format!(
            "PeerMesh max_peer_degree must be at least {minimum_peer_degree}"
        )));
    }
    if config.attacker_count > 0
        && peer.attacker_links_per_honest_node > 0
        && peer.max_attacker_degree == 0
    {
        return Err(invalid(
            "PeerMesh attacker exposure requires max_attacker_degree > 0",
        ));
    }
    Ok(())
}

fn connect_ring(graph: &mut BoundedGraph, nodes: &[usize]) -> Result<(), TopologyError> {
    match nodes {
        [] | [_] => Ok(()),
        [left, right] => add_required(graph, *left, *right, "two-node cohort ring"),
        _ => {
            for (index, node) in nodes.iter().enumerate() {
                add_required(
                    graph,
                    *node,
                    nodes[(index + 1) % nodes.len()],
                    "cohort ring",
                )?;
            }
            Ok(())
        }
    }
}

fn connect_cohort_backbone(
    graph: &mut BoundedGraph,
    cohorts: &BTreeMap<u32, Vec<usize>>,
    rng: &mut DeterministicRng,
) -> Result<(), TopologyError> {
    let groups = cohorts.values().collect::<Vec<_>>();
    for pair in groups.windows(2) {
        let mut left = pair[0].clone();
        let mut right = pair[1].clone();
        rng.shuffle(&mut left);
        rng.shuffle(&mut right);
        if !connect_first_available(graph, &left, &right) {
            return Err(capacity("unable to connect honest cohort backbone"));
        }
    }
    Ok(())
}

fn add_same_cohort_shortcuts(
    graph: &mut BoundedGraph,
    cohorts: &BTreeMap<u32, Vec<usize>>,
    shortcuts_per_node: usize,
    rng: &mut DeterministicRng,
) {
    if shortcuts_per_node == 0 {
        return;
    }
    for nodes in cohorts.values() {
        for node in nodes {
            let mut candidates = nodes.clone();
            rng.shuffle(&mut candidates);
            add_optional_links(graph, *node, &candidates, shortcuts_per_node);
        }
    }
}

fn add_cross_cohort_links(
    graph: &mut BoundedGraph,
    config: &TopologyConfig,
    links_per_node: usize,
    rng: &mut DeterministicRng,
) {
    if links_per_node == 0 {
        return;
    }
    let honest = honest_nodes(config);
    for node in &honest {
        let cohort = config.cohort_ids[*node];
        let mut candidates = honest
            .iter()
            .copied()
            .filter(|candidate| config.cohort_ids[*candidate] != cohort)
            .collect::<Vec<_>>();
        rng.shuffle(&mut candidates);
        add_optional_links(graph, *node, &candidates, links_per_node);
    }
}

fn add_attacker_exposure(
    graph: &mut BoundedGraph,
    config: &TopologyConfig,
    links_per_node: usize,
    rng: &mut DeterministicRng,
) {
    if links_per_node == 0 || config.attacker_count == 0 {
        return;
    }
    let attackers = (0..config.attacker_count).collect::<Vec<_>>();
    for node in honest_nodes(config) {
        let mut candidates = attackers.clone();
        rng.shuffle(&mut candidates);
        add_optional_links(graph, node, &candidates, links_per_node);
    }
}

fn build_hybrid_supernodes(config: &TopologyConfig) -> Result<TopologyResult, TopologyError> {
    let hybrid = &config.hybrid;
    let (
        mut roles,
        honest_supernodes,
        adversarial_candidates,
        generic_candidates,
        normal_peers,
        mut rng,
    ) = hybrid_ground_truth_and_candidates(config, hybrid);
    validate_hybrid(
        config,
        hybrid,
        generic_candidates.len(),
        adversarial_candidates.len(),
        normal_peers.len(),
    )?;
    for supernode in &honest_supernodes {
        roles[*supernode] = NodeRole::Supernode;
    }
    let mut graph = BoundedGraph::new(
        roles,
        DegreeCaps {
            peer: hybrid.max_peer_degree,
            supernode: hybrid.max_supernode_degree,
            attacker: hybrid.max_attacker_degree,
        },
    );

    connect_supernode_mesh(&mut graph, &honest_supernodes)?;
    let adversarial_set = adversarial_candidates
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut discovery = DiscoverySelectionCounts::default();
    if hybrid.discovery == SupernodeDiscoveryStrategy::Mixed {
        connect_mixed_bootstraps(
            &mut graph,
            &normal_peers,
            &generic_candidates,
            &adversarial_set,
            &mut discovery,
            &mut rng,
        )?;
    }
    for peer in &normal_peers {
        connect_discovered_candidates(
            &mut graph,
            config,
            hybrid,
            *peer,
            &generic_candidates,
            &adversarial_set,
            &mut discovery,
            &mut rng,
        )?;
    }
    discovery.candidate_peer_count = normal_peers.len();
    discovery.peers_selecting_high_capacity = normal_peers
        .iter()
        .filter(|peer| {
            graph.neighbors[**peer]
                .iter()
                .any(|neighbor| honest_supernodes.contains(neighbor))
        })
        .count();

    let (neighbors, outbound_discovery_neighbors, roles) = graph.finish();
    Ok(TopologyResult {
        neighbors,
        outbound_discovery_neighbors,
        roles,
        cohort_ids: config.cohort_ids.clone(),
        honest_supernodes,
        adversarial_discovery_candidates: adversarial_candidates,
        discovery_selections: discovery,
    })
}

#[allow(clippy::type_complexity)]
fn hybrid_ground_truth_and_candidates(
    config: &TopologyConfig,
    hybrid: &HybridSupernodeConfig,
) -> (
    Vec<NodeRole>,
    Vec<usize>,
    Vec<usize>,
    Vec<usize>,
    Vec<usize>,
    DeterministicRng,
) {
    let roles = base_roles(config.node_count, config.attacker_count);
    let mut role_rng = DeterministicRng::new(config.seed ^ 0x4859_4252_4944_524F);
    let mut honest = honest_nodes(config);
    role_rng.shuffle(&mut honest);
    let honest_supernodes = honest
        .iter()
        .take(hybrid.honest_supernode_count.min(honest.len()))
        .copied()
        .collect::<Vec<_>>();
    let supernode_set = honest_supernodes.iter().copied().collect::<BTreeSet<_>>();
    let normal_peers = honest
        .into_iter()
        .filter(|node| !supernode_set.contains(node))
        .collect::<Vec<_>>();
    let mut attackers = (0..config.attacker_count).collect::<Vec<_>>();
    let mut discovery_rng = DeterministicRng::new(config.seed ^ 0x4449_5343_4F56_4552);
    discovery_rng.shuffle(&mut attackers);
    let adversarial_candidates = attackers
        .into_iter()
        .take(
            hybrid
                .adversarial_discovery_candidate_count
                .min(config.attacker_count),
        )
        .collect::<Vec<_>>();
    let mut generic_candidates = honest_nodes(config);
    generic_candidates.extend(adversarial_candidates.iter().copied());
    (
        roles,
        honest_supernodes,
        adversarial_candidates,
        generic_candidates,
        normal_peers,
        discovery_rng,
    )
}

fn validate_hybrid(
    config: &TopologyConfig,
    hybrid: &HybridSupernodeConfig,
    generic_candidate_count: usize,
    adversarial_candidate_count: usize,
    normal_peer_count: usize,
) -> Result<(), TopologyError> {
    if normal_peer_count == 0 {
        return Ok(());
    }
    if hybrid.candidate_links_per_peer == 0 {
        return Err(invalid(
            "HybridSupernodes candidate_links_per_peer must be non-zero",
        ));
    }
    if hybrid.max_peer_degree < hybrid.candidate_links_per_peer {
        return Err(invalid(
            "HybridSupernodes max_peer_degree is below candidate_links_per_peer",
        ));
    }
    let candidate_count = generic_candidate_count.saturating_sub(1);
    if candidate_count < hybrid.candidate_links_per_peer {
        return Err(invalid(format!(
            "HybridSupernodes needs at least {} candidates but has {candidate_count}",
            hybrid.candidate_links_per_peer
        )));
    }
    if hybrid.discovery == SupernodeDiscoveryStrategy::Mixed
        && (hybrid.candidate_links_per_peer < 2 || hybrid.exploration_links_per_peer == 0)
    {
        return Err(invalid(
            "mixed discovery requires a bootstrap link and at least one exploration link",
        ));
    }
    if config.attacker_count > 0
        && adversarial_candidate_count > 0
        && hybrid.max_attacker_degree == 0
    {
        return Err(invalid(
            "adversarial discovery candidates require max_attacker_degree > 0",
        ));
    }
    Ok(())
}

fn connect_supernode_mesh(
    graph: &mut BoundedGraph,
    honest_supernodes: &[usize],
) -> Result<(), TopologyError> {
    for (index, left) in honest_supernodes.iter().enumerate() {
        for right in honest_supernodes.iter().skip(index + 1) {
            if !graph.add(*left, *right) {
                return Err(capacity("unable to connect honest supernode mesh"));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum DiscoverySource {
    Bootstrap,
    InterestAffinity,
    Exploration,
}

fn connect_mixed_bootstraps(
    graph: &mut BoundedGraph,
    normal_peers: &[usize],
    generic_candidates: &[usize],
    adversarial_set: &BTreeSet<usize>,
    discovery: &mut DiscoverySelectionCounts,
    rng: &mut DeterministicRng,
) -> Result<(), TopologyError> {
    for peer in normal_peers {
        let candidates = generic_candidates_for_peer(generic_candidates, *peer, rng);
        add_discovery_links(
            graph,
            *peer,
            &candidates,
            1,
            DiscoverySource::Bootstrap,
            adversarial_set,
            discovery,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn connect_discovered_candidates(
    graph: &mut BoundedGraph,
    config: &TopologyConfig,
    hybrid: &HybridSupernodeConfig,
    peer: usize,
    generic_candidates: &[usize],
    adversarial_set: &BTreeSet<usize>,
    discovery: &mut DiscoverySelectionCounts,
    rng: &mut DeterministicRng,
) -> Result<(), TopologyError> {
    let target = hybrid.candidate_links_per_peer;

    match hybrid.discovery {
        SupernodeDiscoveryStrategy::Bootstrap => {
            let candidates = generic_candidates_for_peer(generic_candidates, peer, rng);
            add_discovery_links(
                graph,
                peer,
                &candidates,
                target,
                DiscoverySource::Bootstrap,
                adversarial_set,
                discovery,
            )?;
        }
        SupernodeDiscoveryStrategy::InterestAffinity => {
            let ordered = interest_affinity_candidates(config, peer, generic_candidates, rng);
            add_discovery_links(
                graph,
                peer,
                &ordered,
                target,
                DiscoverySource::InterestAffinity,
                adversarial_set,
                discovery,
            )?;
        }
        SupernodeDiscoveryStrategy::Exploration => {
            let candidates = generic_candidates_for_peer(generic_candidates, peer, rng);
            add_discovery_links(
                graph,
                peer,
                &candidates,
                target,
                DiscoverySource::Exploration,
                adversarial_set,
                discovery,
            )?;
        }
        SupernodeDiscoveryStrategy::Mixed => {
            let exploration_target = hybrid
                .exploration_links_per_peer
                .min(target.saturating_sub(1));
            let exploration_candidates = generic_candidates_for_peer(generic_candidates, peer, rng);
            add_discovery_links(
                graph,
                peer,
                &exploration_candidates,
                exploration_target,
                DiscoverySource::Exploration,
                adversarial_set,
                discovery,
            )?;
            let social_target = target.saturating_sub(1 + exploration_target);
            if social_target > 0 {
                let ordered = interest_affinity_candidates(config, peer, generic_candidates, rng);
                add_discovery_links(
                    graph,
                    peer,
                    &ordered,
                    social_target,
                    DiscoverySource::InterestAffinity,
                    adversarial_set,
                    discovery,
                )?;
            }
        }
    }
    Ok(())
}

fn add_discovery_links(
    graph: &mut BoundedGraph,
    peer: usize,
    candidates: &[usize],
    target: usize,
    source: DiscoverySource,
    adversarial_set: &BTreeSet<usize>,
    discovery: &mut DiscoverySelectionCounts,
) -> Result<(), TopologyError> {
    if target == 0 {
        return Ok(());
    }
    let mut added = 0usize;
    for candidate in candidates {
        if graph.add_discovered(peer, *candidate) {
            record_discovery(
                source,
                graph.roles[*candidate],
                adversarial_set.contains(candidate),
                discovery,
            );
            added += 1;
            if added == target {
                return Ok(());
            }
        }
    }
    Err(capacity(format!(
        "node {peer} selected only {added} of {target} requested candidates"
    )))
}

fn generic_candidates_for_peer(
    values: &[usize],
    peer: usize,
    rng: &mut DeterministicRng,
) -> Vec<usize> {
    let mut shuffled = values.to_vec();
    rng.shuffle(&mut shuffled);
    shuffled.retain(|candidate| *candidate != peer);
    shuffled
}

fn interest_affinity_candidates(
    config: &TopologyConfig,
    peer: usize,
    candidates: &[usize],
    rng: &mut DeterministicRng,
) -> Vec<usize> {
    let mut ordered = generic_candidates_for_peer(candidates, peer, rng);
    ordered.sort_by_key(|candidate| config.cohort_ids[*candidate] != config.cohort_ids[peer]);
    ordered
}

fn record_discovery(
    source: DiscoverySource,
    candidate_role: NodeRole,
    adversarial: bool,
    discovery: &mut DiscoverySelectionCounts,
) {
    match source {
        DiscoverySource::Bootstrap => discovery.bootstrap_links += 1,
        DiscoverySource::InterestAffinity => discovery.interest_affinity_links += 1,
        DiscoverySource::Exploration => discovery.exploration_links += 1,
    }
    if candidate_role == NodeRole::Supernode {
        discovery.selected_high_capacity_links += 1;
    }
    if adversarial {
        discovery.selected_adversarial_candidate_links += 1;
    }
}

fn base_roles(node_count: usize, attacker_count: usize) -> Vec<NodeRole> {
    (0..node_count)
        .map(|node| {
            if node < attacker_count {
                NodeRole::Attacker
            } else {
                NodeRole::Peer
            }
        })
        .collect()
}

fn honest_nodes(config: &TopologyConfig) -> Vec<usize> {
    (config.attacker_count..config.node_count).collect()
}

fn honest_cohorts(config: &TopologyConfig) -> BTreeMap<u32, Vec<usize>> {
    let mut cohorts = BTreeMap::<u32, Vec<usize>>::new();
    for node in honest_nodes(config) {
        cohorts
            .entry(config.cohort_ids[node])
            .or_default()
            .push(node);
    }
    cohorts
}

fn add_optional_links(graph: &mut BoundedGraph, node: usize, candidates: &[usize], target: usize) {
    let mut added = 0usize;
    for candidate in candidates {
        if graph.add(node, *candidate) {
            added += 1;
            if added == target {
                break;
            }
        }
    }
}

fn connect_first_available(graph: &mut BoundedGraph, left: &[usize], right: &[usize]) -> bool {
    for left_node in left {
        for right_node in right {
            if graph.add(*left_node, *right_node) {
                return true;
            }
        }
    }
    false
}

fn add_required(
    graph: &mut BoundedGraph,
    left: usize,
    right: usize,
    context: &str,
) -> Result<(), TopologyError> {
    if graph.add(left, right) {
        Ok(())
    } else {
        Err(capacity(format!(
            "unable to add required {context} edge {left}<->{right}"
        )))
    }
}

fn invalid(message: impl Into<String>) -> TopologyError {
    TopologyError::InvalidConfig(message.into())
}

fn capacity(message: impl Into<String>) -> TopologyError {
    TopologyError::InsufficientCapacity(message.into())
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0xA076_1D64_78BD_642F,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn shuffle<T>(&mut self, values: &mut [T]) {
        for index in (1..values.len()).rev() {
            let upper = u64::try_from(index + 1).unwrap_or(u64::MAX);
            let selected = usize::try_from(self.next_u64() % upper).unwrap_or(0);
            values.swap(index, selected);
        }
    }
}

#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;
