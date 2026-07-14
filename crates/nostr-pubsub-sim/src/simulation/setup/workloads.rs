use super::super::{
    BTreeSet, Event, EventMetadata, Filter, HashMap, Keys, LEGITIMATE_PUBLISH_BASE_MS, NodeRole,
    Result, SIM_UNIX_BASE, SPAM_PUBLISH_BASE_MS, SimulationConfig, SimulationError, SpamIdentity,
    SubscriptionClass, SubscriptionWorkload, TopologyResult, TopologyStrategy, VerifiedEvent,
    WorkloadPair, build_author_feed, build_fips_advert, build_hashtag_topic, build_hashtree_update,
    build_iris_drive_broad_root, build_targeted_approval_rating, class_name, is_fresh_sybil,
    is_quiet_attacker, pubsub_error,
};
use nostr::JsonUtil;

type BuiltWorkloads = (HashMap<String, EventMetadata>, Vec<WorkloadPair>);

const SIGNED_SPAM_PHASES_MS: [u64; 8] = [
    12,
    SPAM_PUBLISH_BASE_MS,
    250,
    1_150,
    1_300,
    2_250,
    2_400,
    2_550,
];
const SIGNED_SPAM_CYCLE_STRIDE_MS: usize = 3_000;
const HASHTAG_TOPIC: &str = "decentralized-nostr";
const HASHTREE_NAME: &str = "iris-chat-releases";
const IRIS_DRIVE_ROOT: &str = "iris-drive/default/root";
const FIPS_ADVERT_SCOPE: &str = "nostr-pubsub";
const FIPS_PAID_OFFER: &str = "nostr-vpn-paid-exit";
const GIT_REPOSITORY: &str = "nostr-pubsub";

pub(super) fn build_workloads(
    config: &SimulationConfig,
    keys: &[Keys],
    topology: &TopologyResult,
    peer_ids: &[String],
) -> Result<BuiltWorkloads> {
    let mut events = HashMap::new();
    let mut pairs = Vec::new();

    for (class_index, class) in SubscriptionClass::ALL.iter().copied().enumerate() {
        let publisher = publisher_for_class(config, topology, class_index, class)?;
        let recipient_index = if class == SubscriptionClass::TargetedApprovalRating {
            (config.attacker_count..config.node_count)
                .find(|candidate| {
                    *candidate != publisher
                        && (*candidate - config.attacker_count) % SubscriptionClass::ALL.len()
                            == class_index
                })
                .unwrap_or_else(|| (publisher + 1).min(config.node_count.saturating_sub(1)))
        } else {
            publisher
        };
        let recipient = keys[recipient_index].public_key();
        let created_at = SIM_UNIX_BASE.saturating_add(u64::try_from(class_index).unwrap_or(0));
        let legitimate = build_class_workload(class, &keys[publisher], recipient, created_at)?;
        #[cfg(test)]
        let legitimate_event_id = legitimate.event.id.to_hex();
        insert_event_metadata(
            &mut events,
            legitimate.event,
            class,
            true,
            None,
            publisher,
            LEGITIMATE_PUBLISH_BASE_MS
                .saturating_add(u64::try_from(class_index).unwrap_or(0).saturating_mul(4)),
        )?;
        #[cfg(test)]
        let mut spam_event_id = None;
        if config.attacker_count > 0 && config.signed_spam_rounds > 0 {
            for round in 0..config.signed_spam_rounds {
                let event_sequence = class_index
                    .saturating_mul(config.signed_spam_rounds)
                    .saturating_add(round);
                let (spam_publisher, _, spam_identity) =
                    adversarial_route(config, topology, class_index, class, round)?;
                let spam_created_at = SIM_UNIX_BASE
                    .saturating_add(10_000)
                    .saturating_add(u64::try_from(event_sequence).unwrap_or(u64::MAX));
                let spam = build_adversarial_workload(
                    class,
                    &keys[spam_publisher],
                    recipient,
                    spam_created_at,
                    round.is_multiple_of(2),
                )?;
                #[cfg(test)]
                spam_event_id.get_or_insert_with(|| spam.event.id.to_hex());
                insert_event_metadata(
                    &mut events,
                    spam.event,
                    class,
                    false,
                    Some(spam_identity),
                    spam_publisher,
                    signed_spam_publish_at(class_index, round),
                )?;
            }
        }
        pairs.push(WorkloadPair {
            class,
            filter: legitimate.filter,
            #[cfg(test)]
            legitimate_event_id,
            #[cfg(test)]
            spam_event_id,
        });
    }
    if events
        .values()
        .any(|metadata| metadata.publisher >= peer_ids.len())
    {
        return Err(SimulationError::Pubsub(
            "workload publisher is outside peer identity table".to_string(),
        ));
    }
    Ok((events, pairs))
}

fn adversarial_route(
    config: &SimulationConfig,
    topology: &TopologyResult,
    class_index: usize,
    class: SubscriptionClass,
    round: usize,
) -> Result<(usize, usize, SpamIdentity)> {
    let claimed_cohort = u32::try_from(class_index).unwrap_or_default();
    let cohort = (!has_exact_author_filter(class)).then_some(claimed_cohort);
    let mut routes = adversarial_routes(config, topology, cohort);
    if routes.is_empty() {
        return Err(SimulationError::InvalidConfig(format!(
            "signed adversarial workloads have no attacker ingress for cohort {class_index}"
        )));
    }
    let requested_identity = signed_spam_identity(round);
    let matches_identity = |publisher: usize| match requested_identity {
        SpamIdentity::Persistent => is_quiet_attacker(publisher),
        SpamIdentity::FreshSybil => is_fresh_sybil(publisher),
    };
    if routes
        .iter()
        .any(|(publisher, _)| matches_identity(*publisher))
    {
        routes.retain(|(publisher, _)| matches_identity(*publisher));
    }
    if routes
        .iter()
        .any(|(_, subscriber)| topology.roles[*subscriber] == NodeRole::Peer)
    {
        routes.retain(|(_, subscriber)| topology.roles[*subscriber] == NodeRole::Peer);
    }
    routes.sort_unstable();
    routes.dedup_by_key(|(publisher, _)| *publisher);

    let cycle = round / SIGNED_SPAM_PHASES_MS.len();
    let phase = round % SIGNED_SPAM_PHASES_MS.len();
    let rotating_sybil = matches!(phase, 4 | 5);
    let identity_offset = usize::from(rotating_sybil).saturating_mul(cycle.saturating_add(1));
    let (publisher, subscriber) =
        routes[(class_index.saturating_add(identity_offset)) % routes.len()];
    let identity = if requested_identity == SpamIdentity::FreshSybil && is_fresh_sybil(publisher) {
        SpamIdentity::FreshSybil
    } else {
        SpamIdentity::Persistent
    };
    Ok((publisher, subscriber, identity))
}

fn adversarial_routes(
    config: &SimulationConfig,
    topology: &TopologyResult,
    claimed_cohort: Option<u32>,
) -> Vec<(usize, usize)> {
    (0..config.attacker_count)
        .flat_map(|publisher| {
            topology.neighbors[publisher]
                .iter()
                .copied()
                .filter(move |subscriber| {
                    topology.roles[*subscriber] == NodeRole::Supernode
                        || (topology.roles[*subscriber] == NodeRole::Peer
                            && claimed_cohort
                                .is_none_or(|cohort| topology.cohort_ids[*subscriber] == cohort))
                })
                .map(move |subscriber| (publisher, subscriber))
        })
        .collect()
}

fn has_exact_author_filter(class: SubscriptionClass) -> bool {
    matches!(
        class,
        SubscriptionClass::AuthorFeed
            | SubscriptionClass::HashtreeUpdate
            | SubscriptionClass::GitRepoAnnouncement
    )
}

pub(super) fn signed_spam_publish_at(class_index: usize, round: usize) -> u64 {
    let phase = SIGNED_SPAM_PHASES_MS[round % SIGNED_SPAM_PHASES_MS.len()];
    let cycle = round / SIGNED_SPAM_PHASES_MS.len();
    phase
        .saturating_add(
            u64::try_from(cycle.saturating_mul(SIGNED_SPAM_CYCLE_STRIDE_MS)).unwrap_or(u64::MAX),
        )
        .saturating_add(u64::try_from(class_index % 4).unwrap_or(0))
}

const fn signed_spam_identity(round: usize) -> SpamIdentity {
    match round % SIGNED_SPAM_PHASES_MS.len() {
        4 | 5 => SpamIdentity::FreshSybil,
        _ => SpamIdentity::Persistent,
    }
}

fn publisher_for_class(
    config: &SimulationConfig,
    topology: &TopologyResult,
    class_index: usize,
    class: SubscriptionClass,
) -> Result<usize> {
    if class == SubscriptionClass::FipsAdvert
        && config.topology == TopologyStrategy::HybridSupernodes
        && let Some(supernode) = topology.honest_supernodes.first()
    {
        return Ok(*supernode);
    }
    topology
        .roles
        .iter()
        .enumerate()
        .find(|(index, role)| {
            **role == NodeRole::Peer
                && *index >= config.attacker_count
                && (*index - config.attacker_count) % SubscriptionClass::ALL.len() == class_index
        })
        .map(|(index, _)| index)
        .or_else(|| {
            (config.attacker_count..config.node_count).find(|index| {
                (*index - config.attacker_count) % SubscriptionClass::ALL.len() == class_index
            })
        })
        .ok_or_else(|| {
            SimulationError::InvalidConfig(format!(
                "no honest publisher for subscription class {}",
                class_name(class)
            ))
        })
}

fn build_class_workload(
    class: SubscriptionClass,
    signer: &Keys,
    recipient: nostr::PublicKey,
    created_at: u64,
) -> Result<SubscriptionWorkload> {
    let result = match class {
        SubscriptionClass::AuthorFeed => build_author_feed(signer, created_at),
        SubscriptionClass::HashtagTopic => build_hashtag_topic(signer, HASHTAG_TOPIC, created_at),
        SubscriptionClass::HashtreeUpdate => {
            build_hashtree_update(signer, HASHTREE_NAME, created_at)
        }
        SubscriptionClass::TargetedApprovalRating => {
            build_targeted_approval_rating(signer, recipient, "fips.peer", created_at)
        }
        SubscriptionClass::IrisDriveBroadRoot => {
            build_iris_drive_broad_root(signer, IRIS_DRIVE_ROOT, created_at)
        }
        SubscriptionClass::FipsAdvert => build_fips_advert(signer, FIPS_ADVERT_SCOPE, created_at),
        SubscriptionClass::FipsPaidOffer => {
            crate::workload::build_fips_paid_offer(signer, FIPS_PAID_OFFER, created_at)
        }
        SubscriptionClass::GitRepoAnnouncement => {
            crate::workload::build_git_repo_announcement(signer, GIT_REPOSITORY, created_at)
        }
    };
    result.map_err(pubsub_error)
}

fn build_adversarial_workload(
    class: SubscriptionClass,
    signer: &Keys,
    recipient: nostr::PublicKey,
    created_at: u64,
    in_scope: bool,
) -> Result<SubscriptionWorkload> {
    let result = match class {
        SubscriptionClass::AuthorFeed => build_author_feed(signer, created_at),
        SubscriptionClass::HashtagTopic => {
            let topic = if in_scope {
                HASHTAG_TOPIC
            } else {
                "adversarial-noise"
            };
            build_hashtag_topic(signer, topic, created_at)
        }
        SubscriptionClass::HashtreeUpdate => {
            let tree = if in_scope {
                HASHTREE_NAME
            } else {
                "adversarial-releases"
            };
            build_hashtree_update(signer, tree, created_at)
        }
        SubscriptionClass::TargetedApprovalRating => {
            let recipient = if in_scope {
                recipient
            } else {
                signer.public_key()
            };
            build_targeted_approval_rating(signer, recipient, "fips.peer", created_at)
        }
        SubscriptionClass::IrisDriveBroadRoot => {
            let root = if in_scope {
                IRIS_DRIVE_ROOT
            } else {
                "iris-drive/adversarial/root"
            };
            build_iris_drive_broad_root(signer, root, created_at)
        }
        SubscriptionClass::FipsAdvert => {
            let scope = if in_scope {
                FIPS_ADVERT_SCOPE
            } else {
                "unknown-pubsub"
            };
            build_fips_advert(signer, scope, created_at)
        }
        SubscriptionClass::FipsPaidOffer => {
            let offer = if in_scope {
                FIPS_PAID_OFFER
            } else {
                "untrusted-paid-exit"
            };
            crate::workload::build_fips_paid_offer(signer, offer, created_at)
        }
        SubscriptionClass::GitRepoAnnouncement => {
            let repository = if in_scope {
                GIT_REPOSITORY
            } else {
                "adversarial-pubsub"
            };
            crate::workload::build_git_repo_announcement(signer, repository, created_at)
        }
    };
    result.map_err(pubsub_error)
}

fn insert_event_metadata(
    events: &mut HashMap<String, EventMetadata>,
    event: Event,
    class: SubscriptionClass,
    legitimate: bool,
    spam_identity: Option<SpamIdentity>,
    publisher: usize,
    publish_at_ms: u64,
) -> Result<()> {
    let verified = VerifiedEvent::try_from(event.clone()).map_err(pubsub_error)?;
    let payload_bytes =
        u64::try_from(event.try_as_json().map_err(pubsub_error)?.len()).unwrap_or(u64::MAX);
    events.insert(
        event.id.to_hex(),
        EventMetadata {
            class,
            legitimate,
            spam_identity,
            publisher,
            event,
            verified,
            payload_bytes,
            publish_at_ms,
            interested: BTreeSet::new(),
        },
    );
    Ok(())
}

pub(super) fn node_filters(
    config: &SimulationConfig,
    topology: &TopologyResult,
    pairs: &[WorkloadPair],
) -> Vec<Vec<Filter>> {
    let fips_filter = pairs
        .iter()
        .find(|pair| pair.class == SubscriptionClass::FipsAdvert)
        .map(|pair| pair.filter.clone());
    (0..config.node_count)
        .map(|node| match topology.roles[node] {
            NodeRole::Attacker | NodeRole::Supernode => vec![Filter::new()],
            NodeRole::Peer => {
                let class_index = (node - config.attacker_count) % SubscriptionClass::ALL.len();
                let mut filters = vec![pairs[class_index].filter.clone()];
                if config.topology == TopologyStrategy::HybridSupernodes
                    && pairs[class_index].class != SubscriptionClass::FipsAdvert
                    && let Some(filter) = fips_filter.clone()
                {
                    filters.push(filter);
                }
                filters
            }
        })
        .collect()
}
