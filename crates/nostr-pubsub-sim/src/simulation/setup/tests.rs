use super::super::{
    BTreeSet, NodeRole, PeerSelectionMode, PubsubPeerInterest, Simulation, SimulationConfig,
    SpamIdentity, SubscriptionClass, SupernodeDiscoveryStrategy, TopologyStrategy, class_name,
    is_fresh_sybil, is_quiet_attacker, run_simulation,
};
use super::claimed_cohort_ids;
use super::workloads::signed_spam_publish_at;

fn adversarial_config(topology: TopologyStrategy) -> SimulationConfig {
    SimulationConfig {
        node_count: 120,
        attacker_count: 24,
        topology,
        supernode_count: 8,
        false_supernode_count: 4,
        loss_basis_points: 0,
        churn_basis_points: 0,
        ..SimulationConfig::default()
    }
}

#[test]
fn scoped_spam_alternates_between_organic_interest_and_near_misses() {
    for topology in [
        TopologyStrategy::PeerMesh,
        TopologyStrategy::HybridSupernodes,
    ] {
        let simulation = Simulation::new(
            adversarial_config(topology),
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        for (class_index, class) in SubscriptionClass::ALL.iter().copied().enumerate() {
            let pair = simulation
                .workload_pairs
                .iter()
                .find(|pair| pair.class == class)
                .unwrap();
            let mut spam = simulation
                .events
                .values()
                .filter(|event| !event.legitimate && event.class == class)
                .collect::<Vec<_>>();
            spam.sort_by_key(|event| event.event.created_at);
            assert_eq!(spam.len(), simulation.config.signed_spam_rounds);

            for (round, spam) in spam.into_iter().enumerate() {
                assert!(spam.publisher < simulation.config.attacker_count);
                let cohort = u32::try_from(class_index).unwrap();
                let attack_ingress_route = |publisher: usize| {
                    simulation.topology.neighbors[publisher]
                        .iter()
                        .any(|target| {
                            simulation.topology.roles[*target] == NodeRole::Supernode
                                || (simulation.topology.roles[*target] == NodeRole::Peer
                                    && simulation.topology.cohort_ids[*target] == cohort)
                        })
                };
                assert!(attack_ingress_route(spam.publisher));
                match spam.spam_identity.expect("signed spam identity") {
                    SpamIdentity::Persistent => {
                        if (0..simulation.config.attacker_count).any(|publisher| {
                            is_quiet_attacker(publisher) && attack_ingress_route(publisher)
                        }) {
                            assert!(is_quiet_attacker(spam.publisher));
                        }
                    }
                    SpamIdentity::FreshSybil => {
                        if (0..simulation.config.attacker_count).any(|publisher| {
                            is_fresh_sybil(publisher) && attack_ingress_route(publisher)
                        }) {
                            assert!(is_fresh_sybil(spam.publisher));
                        }
                    }
                }
                let exact_author = matches!(
                    class,
                    SubscriptionClass::AuthorFeed
                        | SubscriptionClass::HashtreeUpdate
                        | SubscriptionClass::GitRepoAnnouncement
                );
                let kind_wide = class == SubscriptionClass::FipsPaidOffer;
                let expected_interest = !exact_author && (round.is_multiple_of(2) || kind_wide);
                assert_eq!(
                    PubsubPeerInterest::from_filters(
                        std::slice::from_ref(&pair.filter),
                        &spam.verified,
                    ) == PubsubPeerInterest::Subscribed,
                    expected_interest,
                    "{} round {round} organic filter mismatch in {topology:?}",
                    class_name(class),
                );

                let routed_organic_interest = (simulation.config.attacker_count
                    ..simulation.config.node_count)
                    .filter(|target| {
                        simulation.topology.roles[*target] == NodeRole::Peer
                            && simulation.topology.cohort_ids[*target] == cohort
                    })
                    .filter(|target| attack_path_exists(&simulation, spam.publisher, *target))
                    .any(|target| {
                        PubsubPeerInterest::from_filters(
                            &simulation.nodes[target].filters,
                            &spam.verified,
                        ) == PubsubPeerInterest::Subscribed
                    });
                assert_eq!(
                    routed_organic_interest,
                    expected_interest,
                    "{} round {round} routed cohort mismatch in {topology:?}",
                    class_name(class),
                );
            }
        }
    }
}

fn attack_path_exists(simulation: &Simulation, attacker: usize, target: usize) -> bool {
    if simulation.topology.neighbors[attacker].contains(&target) {
        return true;
    }
    simulation.topology.neighbors[attacker]
        .iter()
        .copied()
        .filter(|ingress| simulation.topology.roles[*ingress] == NodeRole::Supernode)
        .any(|ingress| {
            simulation.topology.neighbors[ingress].contains(&target)
                || simulation
                    .topology
                    .honest_supernodes
                    .iter()
                    .copied()
                    .any(|relay| {
                        simulation.topology.neighbors[ingress].contains(&relay)
                            && simulation.topology.neighbors[relay].contains(&target)
                    })
        })
}

#[test]
fn hybrid_attack_ingress_covers_both_identities_without_changing_discovery_counts() {
    for discovery in [
        SupernodeDiscoveryStrategy::Bootstrap,
        SupernodeDiscoveryStrategy::InterestAffinity,
        SupernodeDiscoveryStrategy::Exploration,
        SupernodeDiscoveryStrategy::Mixed,
    ] {
        let mut config = adversarial_config(TopologyStrategy::HybridSupernodes);
        config.supernode_discovery = discovery;
        let simulation = Simulation::new(config.clone(), PeerSelectionMode::SharedReputation)
            .expect("hybrid adversarial ingress");
        for attacker in [
            (0..config.attacker_count)
                .find(|attacker| is_quiet_attacker(*attacker))
                .expect("persistent attacker"),
            (0..config.attacker_count)
                .find(|attacker| is_fresh_sybil(*attacker))
                .expect("fresh Sybil attacker"),
        ] {
            assert!(
                simulation.topology.neighbors[attacker]
                    .iter()
                    .any(|neighbor| simulation.topology.honest_supernodes.contains(neighbor))
            );
        }

        let normal_peer_count = simulation
            .topology
            .roles
            .iter()
            .filter(|role| **role == NodeRole::Peer)
            .count();
        assert_eq!(
            simulation.topology.discovery_selections.total_links(),
            normal_peer_count.saturating_mul(config.supernode_links_per_peer),
            "attack ingress leaked into {discovery:?} discovery counts"
        );
        if discovery == SupernodeDiscoveryStrategy::Bootstrap {
            assert_eq!(
                simulation
                    .topology
                    .discovery_selections
                    .false_supernode_links,
                0
            );
        }
    }
}

#[test]
fn exact_author_spam_is_not_added_to_ordinary_peer_filters() {
    for topology in [
        TopologyStrategy::PeerMesh,
        TopologyStrategy::HybridSupernodes,
    ] {
        let simulation = Simulation::new(
            adversarial_config(topology),
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        for pair in simulation.workload_pairs.iter().filter(|pair| {
            matches!(
                pair.class,
                SubscriptionClass::AuthorFeed
                    | SubscriptionClass::HashtreeUpdate
                    | SubscriptionClass::GitRepoAnnouncement
            )
        }) {
            for spam in simulation
                .events
                .values()
                .filter(|event| !event.legitimate && event.class == pair.class)
            {
                assert_eq!(
                    PubsubPeerInterest::from_filters(
                        std::slice::from_ref(&pair.filter),
                        &spam.verified,
                    ),
                    PubsubPeerInterest::Unsubscribed,
                    "{} adversary unexpectedly matches the established-author filter",
                    class_name(pair.class),
                );
                for (node, role) in simulation.topology.roles.iter().enumerate() {
                    if *role == NodeRole::Peer {
                        assert_eq!(
                            PubsubPeerInterest::from_filters(
                                &simulation.nodes[node].filters,
                                &spam.verified,
                            ),
                            PubsubPeerInterest::Unsubscribed,
                            "{} adversary was explicitly injected into peer {node} in {topology:?}",
                            class_name(pair.class),
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn signed_spam_rounds_cover_lifecycle_phases_and_multiple_identities() {
    let mut config = adversarial_config(TopologyStrategy::PeerMesh);
    config.signed_spam_rounds = 8;
    let simulation = Simulation::new(config, PeerSelectionMode::SharedReputation).unwrap();
    let spam = simulation
        .events
        .values()
        .filter(|event| !event.legitimate)
        .collect::<Vec<_>>();

    assert_eq!(spam.len(), SubscriptionClass::ALL.len() * 8);
    assert!(
        spam.iter()
            .map(|event| event.publisher)
            .collect::<BTreeSet<_>>()
            .len()
            > 1
    );
    let signed_timestamps = spam
        .iter()
        .map(|event| event.event.created_at.as_secs())
        .collect::<BTreeSet<_>>();
    assert_eq!(signed_timestamps.len(), spam.len());
    for phase in [12_u64, 75, 250, 1_150, 1_300, 2_250, 2_400, 2_550] {
        assert!(spam.iter().any(|event| {
            event.publish_at_ms >= phase && event.publish_at_ms <= phase.saturating_add(3)
        }));
    }
    assert!(simulation.workload_pairs.iter().all(|pair| {
        pair.spam_event_id.as_ref().is_some_and(|event_id| {
            simulation
                .events
                .get(event_id)
                .is_some_and(|event| !event.legitimate && event.class == pair.class)
        })
    }));
}

#[test]
fn signed_spam_cycles_remain_monotonic_beyond_the_default_round_count() {
    for class_index in 0..SubscriptionClass::ALL.len() {
        let times = (0..24)
            .map(|round| signed_spam_publish_at(class_index, round))
            .collect::<Vec<_>>();
        assert!(times.windows(2).all(|pair| pair[0] < pair[1]), "{times:?}");
    }
}

#[test]
fn signed_spam_mix_repeats_persistent_attackers_and_introduces_fresh_sybils() {
    let simulation = Simulation::new(
        SimulationConfig {
            node_count: 180,
            attacker_count: 36,
            topology: TopologyStrategy::PeerMesh,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::SharedReputation,
    )
    .unwrap();
    for (class_index, class) in SubscriptionClass::ALL.iter().copied().enumerate() {
        let publisher_at = |round| {
            let publish_at = signed_spam_publish_at(class_index, round);
            simulation
                .events
                .values()
                .find(|event| {
                    !event.legitimate && event.class == class && event.publish_at_ms == publish_at
                })
                .map(|event| event.publisher)
                .expect("scheduled signed spam round")
        };
        let persistent = publisher_at(0);
        assert_eq!(publisher_at(2), persistent, "class={class:?}");
        assert_eq!(publisher_at(6), persistent, "class={class:?}");
        let fresh_sybil = publisher_at(4);
        assert_ne!(fresh_sybil, persistent, "class={class:?}");
        assert!(is_fresh_sybil(fresh_sybil), "class={class:?}");
    }
}

#[test]
fn clean_and_disabled_attack_configs_generate_no_signed_spam() {
    let clean = Simulation::new(
        SimulationConfig {
            node_count: 32,
            attacker_count: 0,
            topology: TopologyStrategy::PeerMesh,
            false_supernode_count: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::Neutral,
    )
    .unwrap();
    assert_eq!(clean.events.len(), SubscriptionClass::ALL.len());
    assert!(clean.events.values().all(|event| event.legitimate));
    assert!(
        clean
            .workload_pairs
            .iter()
            .all(|pair| pair.spam_event_id.is_none())
    );

    let mut disabled_config = adversarial_config(TopologyStrategy::PeerMesh);
    disabled_config.signed_spam_rounds = 0;
    let disabled = Simulation::new(disabled_config, PeerSelectionMode::SharedReputation).unwrap();
    assert!(disabled.events.values().all(|event| event.legitimate));
    assert!(
        disabled
            .workload_pairs
            .iter()
            .all(|pair| pair.spam_event_id.is_none())
    );
}

#[test]
fn ordinary_peers_keep_only_their_organic_profile_filters() {
    for topology in [
        TopologyStrategy::PeerMesh,
        TopologyStrategy::HybridSupernodes,
    ] {
        let simulation = Simulation::new(
            adversarial_config(topology),
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        let fips_filter = simulation
            .workload_pairs
            .iter()
            .find(|pair| pair.class == SubscriptionClass::FipsAdvert)
            .unwrap()
            .filter
            .clone();
        for (node, role) in simulation.topology.roles.iter().enumerate() {
            if *role != NodeRole::Peer {
                continue;
            }
            let class_index =
                (node - simulation.config.attacker_count) % SubscriptionClass::ALL.len();
            let pair = &simulation.workload_pairs[class_index];
            let mut expected = vec![pair.filter.clone()];
            if topology == TopologyStrategy::HybridSupernodes
                && pair.class != SubscriptionClass::FipsAdvert
            {
                expected.push(fips_filter.clone());
            }
            assert_eq!(
                simulation.nodes[node].filters, expected,
                "peer {node} received a non-organic filter in {topology:?}"
            );
        }
    }
}

#[test]
fn attacker_cohorts_are_deterministic_claims_not_a_truth_sentinel() {
    let config = adversarial_config(TopologyStrategy::HybridSupernodes);
    let first = claimed_cohort_ids(&config);
    let second = claimed_cohort_ids(&config);
    assert_eq!(first, second);
    assert!(
        first[..config.attacker_count]
            .iter()
            .all(|cohort| usize::try_from(*cohort).unwrap() < SubscriptionClass::ALL.len())
    );
    assert!(
        first[..config.attacker_count]
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            > 1
    );
}

#[test]
fn hybrid_no_loss_exercises_filter_and_graph_layers() {
    let report = run_simulation(
        adversarial_config(TopologyStrategy::HybridSupernodes),
        PeerSelectionMode::SharedReputation,
    )
    .unwrap();

    assert!(report.spam_filter_peer_link_opportunities > 0, "{report:?}");
    assert!(report.spam_filter_suppressed_peer_links > 0, "{report:?}");
    assert!(
        report.filter_suppression_basis_points > 0
            && report.filter_suppression_basis_points < 10_000,
        "{report:?}"
    );
    assert!(
        report.spam_dropped_by_social_graph > 0
            || report.spam_delivered > 0
            || report.unknown_discovery_adverts_delivered > 0,
        "{report:?}",
    );
}
