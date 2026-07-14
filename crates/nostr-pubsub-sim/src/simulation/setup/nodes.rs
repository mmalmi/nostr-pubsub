use super::super::{
    BTreeSet, DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES, Filter, FipsPubsubWireAdapter,
    FipsPubsubWireCodec, HashMap, HashSet, InvWantMesh, InvWantMeshOptions, Kind, NodeRole,
    PeerReputation, PeerReputationConfig, PeerSelectionMode, PubsubPeerInterest,
    PubsubPeerSubscriptionStore, PubsubSubscriptionLimits, Result, SimNode, SimulationConfig,
    TopologyResult, VerifiedEvent, WorkloadPair, mix64, pubsub_error,
};

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
    let machine_trusted_raters = trusted_machine_raters(
        mode,
        config.seed,
        node_index,
        &topology.neighbors[node_index],
        peer_ids,
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
        filters,
        rating_filters: Vec::new(),
        machine_reputation,
        machine_policies,
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

fn trusted_machine_raters(
    mode: PeerSelectionMode,
    seed: u64,
    node: usize,
    neighbors: &[usize],
    peer_ids: &[String],
) -> BTreeSet<String> {
    if mode != PeerSelectionMode::SharedReputation {
        return BTreeSet::new();
    }
    deterministic_peer_selection(
        seed,
        MACHINE_RATER_DOMAIN,
        node,
        neighbors,
        &BTreeSet::new(),
    )
    .into_iter()
    .map(|peer| peer_ids[peer].clone())
    .collect()
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
        max_cached_event_bytes: if role == NodeRole::Supernode {
            256 * 1024 * 1024
        } else {
            16 * 1024 * 1024
        },
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
        BTreeSet, MACHINE_RATER_DOMAIN, NodeRole, SimulationConfig, deterministic_peer_selection,
        mesh_options,
    };

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
    fn machine_rater_selection_is_deterministic_and_neighbor_bounded() {
        let neighbors = [0, 2, 5, 9, 12];
        let first =
            deterministic_peer_selection(7, MACHINE_RATER_DOMAIN, 8, &neighbors, &BTreeSet::new());
        let second =
            deterministic_peer_selection(7, MACHINE_RATER_DOMAIN, 8, &neighbors, &BTreeSet::new());
        assert_eq!(first, second);
        assert!(first.is_some_and(|peer| neighbors.contains(&peer)));
    }
}
