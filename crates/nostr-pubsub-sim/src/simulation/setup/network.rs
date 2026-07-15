use super::super::{
    BTreeSet, NodeRole, Result, SimulationConfig, SimulationError, TopologyConfig, TopologyResult,
    TopologyStrategy, build_topology, is_fresh_sybil, is_quiet_attacker,
};

pub(super) fn build_sim_topology(
    config: &SimulationConfig,
    cohort_ids: Vec<u32>,
) -> Result<TopologyResult> {
    let mut topology = TopologyConfig::new(
        config.node_count,
        config.attacker_count,
        cohort_ids,
        config.seed,
        config.topology,
    );
    topology.peer_mesh.attacker_links_per_honest_node = 4.min(config.attacker_count);
    topology.peer_mesh.max_peer_degree = 20;
    topology.peer_mesh.max_attacker_degree = 128;
    topology.hybrid.discovery = config.supernode_discovery;
    topology.hybrid.honest_supernode_count = config.supernode_count;
    topology.hybrid.adversarial_discovery_candidate_count = config
        .adversarial_discovery_candidate_count
        .min(config.attacker_count);
    topology.hybrid.candidate_links_per_peer = config.supernode_links_per_peer;
    topology.hybrid.exploration_links_per_peer = usize::from(config.supernode_links_per_peer > 1);
    topology.hybrid.max_peer_degree = config
        .supernode_links_per_peer
        .saturating_mul(4)
        .saturating_add(4)
        .max(8);
    topology.hybrid.max_supernode_degree = 512.min(config.node_count.saturating_sub(1)).max(32);
    topology.hybrid.max_attacker_degree = 256.min(config.node_count.saturating_sub(1)).max(16);
    let mut result = build_topology(&topology)
        .map_err(|error| SimulationError::InvalidConfig(error.to_string()))?;
    ensure_adversarial_exposure(config, &mut result)?;
    Ok(result)
}

fn ensure_adversarial_exposure(
    config: &SimulationConfig,
    topology: &mut TopologyResult,
) -> Result<()> {
    if config.attacker_count == 0 {
        return Ok(());
    }
    if config.topology == TopologyStrategy::HybridSupernodes {
        for attacker in [
            (0..config.attacker_count).find(|attacker| is_quiet_attacker(*attacker)),
            (0..config.attacker_count).find(|attacker| is_fresh_sybil(*attacker)),
        ]
        .into_iter()
        .flatten()
        {
            ensure_supernode_ingress(config, topology, attacker)?;
        }
        return Ok(());
    }
    if connected_quiet_attackers(config, topology).next().is_some() {
        return Ok(());
    }
    if let Some(connected_attacker) =
        (0..config.attacker_count).find(|attacker| has_honest_neighbor(config, topology, *attacker))
    {
        let quiet_attacker = (0..config.attacker_count)
            .find(|attacker| is_quiet_attacker(*attacker))
            .expect("attacker zero is always a quiet adversary");
        swap_topology_nodes(topology, connected_attacker, quiet_attacker);
        return Ok(());
    }

    let quiet_attacker = (0..config.attacker_count)
        .find(|attacker| is_quiet_attacker(*attacker))
        .expect("attacker zero is always a quiet adversary");
    if topology.neighbors[quiet_attacker].len() >= node_degree_cap(config, topology, quiet_attacker)
    {
        return Err(SimulationError::InvalidConfig(
            "topology has no attacker capacity for adversarial pubsub exposure".to_string(),
        ));
    }
    let target = (config.attacker_count..config.node_count)
        .find(|target| {
            topology.neighbors[*target].len() < node_degree_cap(config, topology, *target)
        })
        .ok_or_else(|| {
            SimulationError::InvalidConfig(
                "topology has no capacity for adversarial pubsub exposure".to_string(),
            )
        })?;
    connect(topology, quiet_attacker, target);
    Ok(())
}

fn ensure_supernode_ingress(
    config: &SimulationConfig,
    topology: &mut TopologyResult,
    attacker: usize,
) -> Result<()> {
    if topology.neighbors[attacker]
        .iter()
        .any(|neighbor| topology.honest_supernodes.contains(neighbor))
    {
        return Ok(());
    }
    if topology.neighbors[attacker].len() >= node_degree_cap(config, topology, attacker) {
        return Err(SimulationError::InvalidConfig(
            "topology has no attacker capacity for adversarial supernode ingress".to_string(),
        ));
    }
    let supernode = topology
        .honest_supernodes
        .iter()
        .copied()
        .find(|supernode| {
            topology.neighbors[*supernode].len() < node_degree_cap(config, topology, *supernode)
        })
        .ok_or_else(|| {
            SimulationError::InvalidConfig(
                "topology has no supernode capacity for adversarial pubsub ingress".to_string(),
            )
        })?;
    connect(topology, attacker, supernode);
    Ok(())
}

fn connect(topology: &mut TopologyResult, left: usize, right: usize) {
    topology.neighbors[left].push(right);
    topology.neighbors[left].sort_unstable();
    topology.neighbors[right].push(left);
    topology.neighbors[right].sort_unstable();
}

pub(super) fn connected_quiet_attackers<'a>(
    config: &'a SimulationConfig,
    topology: &'a TopologyResult,
) -> impl Iterator<Item = usize> + 'a {
    let adversarial_candidates = topology
        .adversarial_discovery_candidates
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut attackers = (0..config.attacker_count)
        .filter(|attacker| {
            is_quiet_attacker(*attacker) && has_honest_neighbor(config, topology, *attacker)
        })
        .collect::<Vec<_>>();
    attackers.sort_by_key(|attacker| (!adversarial_candidates.contains(attacker), *attacker));
    attackers.into_iter()
}

fn has_honest_neighbor(
    config: &SimulationConfig,
    topology: &TopologyResult,
    attacker: usize,
) -> bool {
    topology.neighbors[attacker]
        .iter()
        .any(|neighbor| *neighbor >= config.attacker_count)
}

fn swap_topology_nodes(topology: &mut TopologyResult, left: usize, right: usize) {
    if left == right {
        return;
    }
    topology.neighbors.swap(left, right);
    for neighbors in &mut topology.neighbors {
        for neighbor in neighbors.iter_mut() {
            *neighbor = match *neighbor {
                value if value == left => right,
                value if value == right => left,
                value => value,
            };
        }
        neighbors.sort_unstable();
    }
    topology.roles.swap(left, right);
    topology.cohort_ids.swap(left, right);
    replace_node(&mut topology.honest_supernodes, left, right);
    replace_node(&mut topology.adversarial_discovery_candidates, left, right);
}

fn replace_node(nodes: &mut [usize], left: usize, right: usize) {
    for node in nodes {
        *node = match *node {
            value if value == left => right,
            value if value == right => left,
            value => value,
        };
    }
}

pub(super) fn node_degree_cap(
    config: &SimulationConfig,
    topology: &TopologyResult,
    node: usize,
) -> usize {
    match (config.topology, topology.roles[node]) {
        (TopologyStrategy::PeerMesh, NodeRole::Peer | NodeRole::Supernode) => 20,
        (TopologyStrategy::PeerMesh, NodeRole::Attacker) => 128,
        (TopologyStrategy::HybridSupernodes, NodeRole::Peer) => config
            .supernode_links_per_peer
            .saturating_mul(4)
            .saturating_add(4)
            .max(8),
        (TopologyStrategy::HybridSupernodes, NodeRole::Supernode) => {
            512.min(config.node_count.saturating_sub(1)).max(32)
        }
        (TopologyStrategy::HybridSupernodes, NodeRole::Attacker) => {
            256.min(config.node_count.saturating_sub(1)).max(16)
        }
    }
}

pub(super) fn endpoint_connection_limits(
    config: &SimulationConfig,
    topology: &TopologyResult,
) -> Vec<usize> {
    (0..config.node_count)
        .map(|node| {
            if topology.roles[node] == NodeRole::Attacker
                && !topology.adversarial_discovery_candidates.contains(&node)
            {
                0
            } else {
                node_degree_cap(config, topology, node)
            }
        })
        .collect()
}
