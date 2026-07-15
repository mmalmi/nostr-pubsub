use super::super::{
    BTreeSet, DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES, Filter, FipsPubsubWireAdapter,
    FipsPubsubWireCodec, HashMap, InvWantMesh, InvWantMeshOptions, Kind, NodeRole, PeerReputation,
    PeerReputationConfig, PeerSelectionMode, PubsubPeerInterest, PubsubPeerSubscriptionStore,
    PubsubSubscriptionLimits, Result, SimNode, SimulationConfig, TopologyResult, VerifiedEvent,
    WorkloadPair, pubsub_error,
};

pub(super) fn build_node(
    config: &SimulationConfig,
    _mode: PeerSelectionMode,
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
    let (machine_reputation, machine_policies) = if node_index >= config.attacker_count {
        let (reputation, policies) =
            PeerReputation::new(&peer_ids[node_index], PeerReputationConfig::default())
                .map_err(pubsub_error)?;
        (Some(reputation), Some(policies))
    } else {
        (None, None)
    };
    let connection_limit = super::network::node_degree_cap(config, topology, node_index);
    let limits = subscription_limits(connection_limit.max(topology.neighbors[node_index].len()));
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
        service_admitted_raters: BTreeSet::new(),
        app_authorized_authors,
        local_events: HashMap::new(),
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
    use super::{BTreeSet, NodeRole, SimulationConfig, mesh_options};

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
}
