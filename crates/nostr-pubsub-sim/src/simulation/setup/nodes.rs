use super::super::{
    Arc, BTreeSet, DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES, Filter, FipsPubsubWireAdapter,
    FipsPubsubWireCodec, GraphDistanceAction, HashMap, HashSet, InvWantMesh, InvWantMeshOptions,
    Kind, NodeRole, PeerReputation, PeerReputationConfig, PeerSelectionMode, PubsubPeerInterest,
    PubsubPeerSubscriptionStore, PubsubSubscriptionLimits, Result, RwLock, SIM_UNIX_BASE, SimNode,
    SimulationConfig, SimulationError, SocialGraph, SocialGraphPolicy, SocialGraphPolicyConfig,
    TopologyResult, VerifiedEvent, WorkloadPair, contact_list_event, mix64, pubsub_error,
};

const HUMAN_FOLLOW_DOMAIN: u64 = 0x4855_4d41_4e00_0001;
const MACHINE_RATER_DOMAIN: u64 = 0x4d41_4348_494e_4502;

pub(super) fn build_node(
    config: &SimulationConfig,
    mode: PeerSelectionMode,
    node_index: usize,
    peer_ids: &[String],
    topology: &TopologyResult,
    filters: Vec<Filter>,
    established_history: &[VerifiedEvent],
) -> Result<SimNode> {
    let app_authorized_authors = established_history
        .iter()
        .filter(|event| {
            event.as_event().kind == Kind::Custom(30_078)
                && PubsubPeerInterest::from_filters(&filters, event)
                    == PubsubPeerInterest::Subscribed
        })
        .map(|event| event.as_event().pubkey.to_hex())
        .collect::<BTreeSet<_>>();
    let established_human_contacts = established_history
        .iter()
        .filter(|event| {
            PubsubPeerInterest::from_filters(&filters, event) == PubsubPeerInterest::Subscribed
        })
        .map(|event| event.as_event().pubkey.to_hex())
        .collect::<BTreeSet<_>>();
    let (human_peer_follows, machine_trusted_raters) = trust_assignments(
        mode,
        config.seed,
        node_index,
        &topology.neighbors[node_index],
        peer_ids,
        &established_human_contacts,
    );
    let graph = Arc::new(RwLock::new(SocialGraph::new(&peer_ids[node_index])));
    if !human_peer_follows.is_empty() {
        graph
            .write()
            .map_err(|_| SimulationError::Pubsub("social graph lock poisoned".to_string()))?
            .handle_event(
                &contact_list_event(
                    &peer_ids[node_index],
                    SIM_UNIX_BASE,
                    human_peer_follows.iter().map(String::as_str),
                ),
                true,
                1.0,
            );
    }
    let policy = SocialGraphPolicy::new(
        Arc::clone(&graph),
        SocialGraphPolicyConfig {
            max_follow_distance: Some(2),
            outside_graph_action: GraphDistanceAction::Drop,
            missing_author_action: GraphDistanceAction::Drop,
            ..SocialGraphPolicyConfig::default()
        },
    );
    let mesh_policy = SocialGraphPolicy::new(
        Arc::clone(&graph),
        SocialGraphPolicyConfig {
            max_follow_distance: Some(2),
            outside_graph_action: GraphDistanceAction::Throttle,
            ..SocialGraphPolicyConfig::default()
        },
    );
    let (machine_reputation, machine_policies) = if node_index >= config.attacker_count {
        let (reputation, policies) = PeerReputation::new(
            &peer_ids[node_index],
            PeerReputationConfig {
                trusted_raters: machine_trusted_raters.clone(),
                ..PeerReputationConfig::default()
            },
        )
        .map_err(pubsub_error)?;
        (Some(reputation), Some(policies))
    } else {
        (None, None)
    };
    let limits = subscription_limits(topology.neighbors[node_index].len());
    Ok(SimNode {
        mesh: InvWantMesh::new(mesh_options(config, topology.roles[node_index])),
        wire: FipsPubsubWireAdapter::new(
            FipsPubsubWireCodec::new(DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES).map_err(pubsub_error)?,
            PubsubPeerSubscriptionStore::new(limits),
        ),
        rating_wire: FipsPubsubWireAdapter::new(
            FipsPubsubWireCodec::new(DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES).map_err(pubsub_error)?,
            PubsubPeerSubscriptionStore::new(limits),
        ),
        filters,
        human_policy: policy,
        mesh_policy,
        machine_reputation,
        machine_policies,
        human_peer_follows,
        machine_trusted_raters,
        app_authorized_authors,
        local_events: HashMap::new(),
        rejected_events: HashSet::new(),
    })
}

fn subscription_limits(neighbor_count: usize) -> PubsubSubscriptionLimits {
    PubsubSubscriptionLimits {
        max_peers: neighbor_count.saturating_add(4).max(1),
        max_subscriptions_per_peer: 8,
        max_filters_per_subscription: 16,
    }
}

pub(super) fn observed_established_history(
    pairs: &[WorkloadPair],
    signed_history: &[VerifiedEvent],
) -> Vec<VerifiedEvent> {
    pairs
        .iter()
        .filter_map(|pair| {
            signed_history
                .iter()
                .filter(|event| {
                    PubsubPeerInterest::from_filters(std::slice::from_ref(&pair.filter), event)
                        == PubsubPeerInterest::Subscribed
                })
                .min_by_key(|event| {
                    (
                        event.as_event().created_at.as_secs(),
                        event.as_event().id.to_hex(),
                    )
                })
                .cloned()
        })
        .collect()
}

fn deterministic_peer_selection(
    seed: u64,
    domain: u64,
    node: usize,
    neighbors: &[usize],
    excluded: &BTreeSet<usize>,
) -> Option<usize> {
    let node = u64::try_from(node).unwrap_or(u64::MAX);
    let mut candidates = neighbors
        .iter()
        .copied()
        .filter(|peer| !excluded.contains(peer))
        .collect::<Vec<_>>();
    candidates.sort_by_key(|peer| {
        let peer = u64::try_from(*peer).unwrap_or(u64::MAX);
        (
            mix64(seed ^ domain ^ node.rotate_left(17) ^ peer.rotate_left(41)),
            peer,
        )
    });
    candidates.into_iter().next()
}

fn trust_assignments(
    mode: PeerSelectionMode,
    seed: u64,
    node: usize,
    neighbors: &[usize],
    peer_ids: &[String],
    established_human_contacts: &BTreeSet<String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    if mode != PeerSelectionMode::SharedReputation {
        return (BTreeSet::new(), BTreeSet::new());
    }
    let explicit_human =
        deterministic_peer_selection(seed, HUMAN_FOLLOW_DOMAIN, node, neighbors, &BTreeSet::new());
    let mut human = established_human_contacts.clone();
    human.extend(explicit_human.map(|peer| peer_ids[peer].clone()));
    human.remove(&peer_ids[node]);
    let human_indices = peer_ids
        .iter()
        .enumerate()
        .filter_map(|(peer, peer_id)| human.contains(peer_id).then_some(peer))
        .collect::<BTreeSet<_>>();
    let machine =
        deterministic_peer_selection(seed, MACHINE_RATER_DOMAIN, node, neighbors, &human_indices)
            .into_iter()
            .map(|peer| peer_ids[peer].clone())
            .collect();
    (human, machine)
}

pub(super) fn mesh_options(config: &SimulationConfig, role: NodeRole) -> InvWantMeshOptions {
    let fanout = if role == NodeRole::Supernode {
        config
            .supernode_fanout
            .max(config.fanout)
            .min(config.node_count.saturating_sub(1))
    } else {
        config.fanout
    };
    InvWantMeshOptions {
        fanout,
        unknown_peer_reserve: config.unknown_peer_reserve,
        max_hops: config.max_hops,
        max_event_bytes: 256 * 1024,
        max_cached_events: 128,
        max_seen_events: 8_192,
        max_pending_peers_per_event: fanout.max(128),
        route_ttl_ms: 60,
        event_ttl_ms: 2_000,
        allowed_kinds: (role != NodeRole::Supernode)
            .then(|| BTreeSet::from([1, 3, 7_368, 10_000, 30_064, 30_078, 30_617, 37_195, 37_196])),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BTreeSet, HUMAN_FOLLOW_DOMAIN, MACHINE_RATER_DOMAIN, NodeRole, PeerSelectionMode,
        SimulationConfig, deterministic_peer_selection, mesh_options,
    };
    use crate::simulation::Simulation;

    #[test]
    fn supernodes_accept_all_signed_kinds_while_peers_bound_known_production_kinds() {
        let config = SimulationConfig::default();
        assert_eq!(
            mesh_options(&config, NodeRole::Supernode).allowed_kinds,
            None
        );
        assert_eq!(
            mesh_options(&config, NodeRole::Peer).allowed_kinds,
            Some(BTreeSet::from([
                1, 3, 7_368, 10_000, 30_064, 30_078, 30_617, 37_195, 37_196,
            ]))
        );
    }

    #[test]
    fn human_and_machine_trust_are_domain_separated_and_disjoint() {
        let neighbors = [0, 2, 5, 9, 12];
        let first =
            deterministic_peer_selection(7, HUMAN_FOLLOW_DOMAIN, 8, &neighbors, &BTreeSet::new());
        let second =
            deterministic_peer_selection(7, HUMAN_FOLLOW_DOMAIN, 8, &neighbors, &BTreeSet::new());
        assert_eq!(first, second);
        assert!(first.is_some_and(|peer| neighbors.contains(&peer)));

        let human = BTreeSet::from_iter(first);
        let machine = deterministic_peer_selection(7, MACHINE_RATER_DOMAIN, 8, &neighbors, &human);
        assert!(machine.is_some_and(|peer| neighbors.contains(&peer)));
        assert!(human.is_disjoint(&BTreeSet::from_iter(machine)));
    }

    #[test]
    fn configured_machine_raters_are_disjoint_from_the_actual_human_contact_graph() {
        let simulation = Simulation::new(
            SimulationConfig {
                node_count: 120,
                attacker_count: 24,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        let mut human_edges = 0usize;
        let mut machine_edges = 0usize;

        for node in simulation.config.attacker_count..simulation.config.node_count {
            let state = &simulation.nodes[node];
            assert!(
                state
                    .human_peer_follows
                    .is_disjoint(&state.machine_trusted_raters)
            );
            let root = &simulation.peer_ids[node];
            let graph = state.human_policy.graph();
            let graph = graph.read().unwrap();
            let actual_contacts = graph
                .get_followed_by_user(root)
                .into_iter()
                .collect::<BTreeSet<_>>();
            assert_eq!(actual_contacts, state.human_peer_follows);
            assert!(
                state
                    .machine_trusted_raters
                    .iter()
                    .all(|rater| !graph.is_following(root, rater))
            );
            human_edges = human_edges.saturating_add(state.human_peer_follows.len());
            machine_edges = machine_edges.saturating_add(state.machine_trusted_raters.len());
        }

        assert!(human_edges > 0);
        assert!(machine_edges > 0);
        assert_eq!(simulation.report.human_trust_edges, human_edges);
        assert_eq!(simulation.report.machine_trust_edges, machine_edges);
        assert_eq!(simulation.report.human_machine_trust_overlap_edges, 0);
    }
}
