use std::collections::{BTreeSet, VecDeque};

use super::*;

#[test]
fn peer_mesh_is_deterministic_and_seeded() {
    let config = peer_mesh_config(80, 16, 4, 7);
    let first = build_topology(&config).expect("first topology");
    let second = build_topology(&config).expect("second topology");
    assert_eq!(first, second);

    let mut other_seed = config;
    other_seed.seed = 8;
    assert_ne!(
        first.neighbors,
        build_topology(&other_seed)
            .expect("other seeded topology")
            .neighbors
    );
}

#[test]
fn peer_mesh_connects_cohorts_and_bounds_symmetric_edges() {
    let config = peer_mesh_config(96, 16, 5, 42);
    let topology = build_topology(&config).expect("peer mesh");
    assert_valid_edges(&topology, &config);

    for cohort in 0..5 {
        let cohort_nodes = (config.attacker_count..config.node_count)
            .filter(|node| config.cohort_ids[*node] == cohort)
            .collect::<BTreeSet<_>>();
        assert_induced_connected(&topology, &cohort_nodes);
    }
    let honest = (config.attacker_count..config.node_count).collect::<BTreeSet<_>>();
    assert_induced_connected(&topology, &honest);
    assert!(
        topology.neighbors[..config.attacker_count]
            .iter()
            .any(|neighbors| !neighbors.is_empty())
    );
}

#[test]
fn hybrid_bootstrap_selects_generic_endpoints_with_hidden_capacity_ground_truth() {
    let mut config = hybrid_config(120, 20, 6, 3, 99);
    config.hybrid.discovery = SupernodeDiscoveryStrategy::Bootstrap;
    config.hybrid.candidate_links_per_peer = 3;
    let topology = build_topology(&config).expect("hybrid bootstrap");
    assert_valid_edges(&topology, &config);
    assert_eq!(topology.honest_supernodes.len(), 6);
    assert_eq!(topology.adversarial_discovery_candidates.len(), 3);
    assert_eq!(
        topology.discovery_selections.bootstrap_links,
        (config.node_count - config.attacker_count - 6) * 3
    );
    assert!(
        topology
            .honest_supernodes
            .iter()
            .all(|node| topology.roles[*node] == NodeRole::Supernode)
    );
    assert!(topology.discovery_selections.selected_high_capacity_links > 0);
    assert!(
        topology.discovery_selections.selected_high_capacity_links
            < topology.discovery_selections.total_links()
    );
}

#[test]
fn mixed_discovery_is_role_blind_and_exposes_adversarial_candidates() {
    let mut config = hybrid_config(30, 5, 1, 4, 2026);
    config.hybrid.discovery = SupernodeDiscoveryStrategy::Mixed;
    config.hybrid.candidate_links_per_peer = 2;
    config.hybrid.exploration_links_per_peer = 1;
    config.hybrid.max_supernode_degree = 64;
    config.hybrid.max_attacker_degree = 64;
    let topology = build_topology(&config).expect("mixed hybrid");
    let normal_peer_count = config.node_count - config.attacker_count - 1;

    assert_eq!(
        topology.discovery_selections.bootstrap_links,
        normal_peer_count
    );
    assert_eq!(
        topology.discovery_selections.exploration_links,
        normal_peer_count
    );
    assert!(topology.discovery_selections.selected_high_capacity_links > 0);
    assert!(
        topology
            .discovery_selections
            .selected_adversarial_candidate_links
            > 0
    );
    assert_eq!(
        topology.discovery_selections.candidate_peer_count,
        normal_peer_count
    );
    assert!(
        topology
            .discovery_selections
            .high_capacity_selection_coverage_basis_points()
            < 10_000
    );
    assert_eq!(
        topology.discovery_selections.total_links(),
        normal_peer_count * config.hybrid.candidate_links_per_peer
    );
    assert_valid_edges(&topology, &config);
}

#[test]
fn untrusted_exploration_reports_high_capacity_selection_after_the_fact() {
    let mut config = hybrid_config(40, 4, 1, 4, 17);
    config.hybrid.discovery = SupernodeDiscoveryStrategy::Exploration;
    config.hybrid.candidate_links_per_peer = 1;
    config.hybrid.max_attacker_degree = 64;

    let topology = build_topology(&config).expect("untrusted exploration");
    assert_eq!(topology.discovery_selections.candidate_peer_count, 35);
    assert!(topology.discovery_selections.peers_selecting_high_capacity > 0);
    assert!(
        topology
            .discovery_selections
            .high_capacity_selection_coverage_basis_points()
            < 10_000
    );
    assert!(
        topology
            .discovery_selections
            .selected_adversarial_candidate_links
            > 0
    );
    assert_eq!(
        topology.discovery_selections.total_links(),
        topology.discovery_selections.candidate_peer_count
    );
    assert_valid_edges(&topology, &config);
}

#[test]
fn discovery_selection_does_not_require_hidden_supernode_roles() {
    for discovery in [
        SupernodeDiscoveryStrategy::Bootstrap,
        SupernodeDiscoveryStrategy::InterestAffinity,
        SupernodeDiscoveryStrategy::Exploration,
        SupernodeDiscoveryStrategy::Mixed,
    ] {
        let mut config = hybrid_config(80, 8, 0, 4, 73);
        config.hybrid.discovery = discovery;
        config.hybrid.candidate_links_per_peer = 3;
        config.hybrid.exploration_links_per_peer = 1;
        let topology = build_topology(&config)
            .unwrap_or_else(|error| panic!("{discovery:?} used hidden roles: {error}"));

        assert!(topology.honest_supernodes.is_empty());
        assert_eq!(
            topology.discovery_selections.selected_high_capacity_links,
            0
        );
        assert_eq!(
            topology.discovery_selections.peers_selecting_high_capacity,
            0
        );
        assert!(topology.discovery_selections.total_links() > 0);
        assert_valid_edges(&topology, &config);
    }
}

#[test]
fn connection_acceptance_does_not_classify_endpoint_capacity() {
    let roles = vec![
        NodeRole::Peer,
        NodeRole::Peer,
        NodeRole::Supernode,
        NodeRole::Peer,
    ];
    let mut graph = BoundedGraph::new(
        roles,
        DegreeCaps {
            peer: 4,
            supernode: 16,
            attacker: 8,
        },
    );

    assert!(graph.add_discovered(0, 1));
    assert!(graph.add_discovered(3, 2));
    assert_eq!(graph.discovery_inbound[1], 1);
    assert_eq!(graph.discovery_inbound[2], 1);
}

fn peer_mesh_config(
    node_count: usize,
    attacker_count: usize,
    cohort_count: u32,
    seed: u64,
) -> TopologyConfig {
    let cohorts = (0..node_count)
        .map(|node| {
            if node < attacker_count {
                u32::try_from(node).unwrap_or_default() % cohort_count
            } else {
                u32::try_from(node - attacker_count).unwrap_or_default() % cohort_count
            }
        })
        .collect();
    TopologyConfig::new(
        node_count,
        attacker_count,
        cohorts,
        seed,
        TopologyStrategy::PeerMesh,
    )
}

fn hybrid_config(
    node_count: usize,
    attacker_count: usize,
    honest_supernodes: usize,
    adversarial_discovery_candidates: usize,
    seed: u64,
) -> TopologyConfig {
    let mut config = TopologyConfig::new(
        node_count,
        attacker_count,
        (0..node_count)
            .map(|node| u32::try_from(node % 4).unwrap_or_default())
            .collect(),
        seed,
        TopologyStrategy::HybridSupernodes,
    );
    config.hybrid.honest_supernode_count = honest_supernodes;
    config.hybrid.adversarial_discovery_candidate_count = adversarial_discovery_candidates;
    config
}

fn assert_valid_edges(topology: &TopologyResult, config: &TopologyConfig) {
    for (node, neighbors) in topology.neighbors.iter().enumerate() {
        assert!(!neighbors.contains(&node));
        assert_eq!(
            neighbors.iter().copied().collect::<BTreeSet<_>>().len(),
            neighbors.len()
        );
        for neighbor in neighbors {
            assert!(topology.neighbors[*neighbor].contains(&node));
        }
        let cap = match (config.strategy, topology.roles[node]) {
            (TopologyStrategy::PeerMesh, NodeRole::Attacker) => {
                config.peer_mesh.max_attacker_degree
            }
            (TopologyStrategy::PeerMesh, NodeRole::Peer | NodeRole::Supernode) => {
                config.peer_mesh.max_peer_degree
            }
            (TopologyStrategy::HybridSupernodes, NodeRole::Peer) => config.hybrid.max_peer_degree,
            (TopologyStrategy::HybridSupernodes, NodeRole::Supernode) => {
                config.hybrid.max_supernode_degree
            }
            (TopologyStrategy::HybridSupernodes, NodeRole::Attacker) => {
                config.hybrid.max_attacker_degree
            }
        };
        assert!(neighbors.len() <= cap);
    }
}

fn assert_induced_connected(topology: &TopologyResult, expected: &BTreeSet<usize>) {
    let Some(start) = expected.iter().next().copied() else {
        return;
    };
    let mut visited = BTreeSet::from([start]);
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        for neighbor in &topology.neighbors[node] {
            if expected.contains(neighbor) && visited.insert(*neighbor) {
                queue.push_back(*neighbor);
            }
        }
    }
    assert_eq!(&visited, expected);
}
