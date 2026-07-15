mod admission;
mod control_transport;
mod delivery;
mod engine;
mod error;
mod lifecycle;
mod machine_wot;
mod mode;
mod network;
mod rating_subscriptions;
mod rediscovery;
mod report;
mod report_types;
mod reputation_flow;
mod reputation_probes;
mod resources;
mod setup;

pub use report_types::SimulationReport;
use resources::NodeResourceLedger;
pub use resources::{
    CpuWorkDistribution, NodeCpuWork, NodeRetainedUsage, ResourceCohortReport,
    RetainedUsageDistribution, SimulationResourceReport,
};
pub use {delivery::VerifiedDeliveryRecord, error::SimulationError, mode::PeerSelectionMode};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use nostr::{Event, EventBuilder, Filter, Keys, Kind, Timestamp};
use nostr_pubsub::{
    DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES, EventSource, FipsPubsubWireAdapter, FipsPubsubWireCodec,
    FipsPubsubWireMessage, InvWantAction, InvWantCodec, InvWantMesh, InvWantMeshOptions,
    InvWantWireMessage, MeshPeer, PolicyDecision, PubsubDeliveryAction, PubsubDeliveryPolicy,
    PubsubPeerInterest, PubsubPeerSubscriptionStore, PubsubSubscriptionLimits, SourceId,
    SubscriptionId, VerifiedEvent,
};
use nostr_pubsub_social_graph::{
    PeerRatingPublisher, PeerReputation, PeerReputationConfig, PeerReputationPolicies,
};
use nostr_social_graph::Rating;
use nostr_social_memory::RatingEventExt;

use crate::clock::VirtualScheduler;
use crate::metrics::{NodeTrafficLedger, TrafficDirection, TrafficProvenance, basis_points};
use crate::topology::{
    NodeRole, SupernodeDiscoveryStrategy, TopologyConfig, TopologyResult, TopologyStrategy,
    build_topology,
};
use crate::workload::{
    SubscriptionClass, SubscriptionWorkload, build_author_feed, build_fips_advert,
    build_hashtag_topic, build_hashtree_update, build_iris_drive_broad_root,
    build_targeted_approval_rating,
};
use network::{LinkOutage, OutageCause};
use rediscovery::RediscoveryState;

const SIM_PROTOCOL: &str = "nostr.pubsub.sim";
const SIM_VERSION: u8 = 1;
const SIM_UNIX_BASE: u64 = 1_700_000_000;
const MALFORMED_TRAINING_SAMPLES: usize = 5;
// The first sweep follows the 60 ms route expiry, the 110 ms churn window,
// and one 80 ms data retry. This keeps transient outages from becoming shared
// machine-removal evidence while silent blackholes remain observable.
const REPUTATION_SWEEP_MS: u64 = 150;
const POST_TRAINING_REDISCOVERY_MS: u64 = 250;
const POST_ROUTE_REPUTATION_SWEEP_MS: u64 = 1_075;
const POST_RECONNECT_REPUTATION_SWEEP_MS: u64 = 2_140;
const CHURN_START_MS: u64 = 30;
const CHURN_END_MS: u64 = 110;
const LEGITIMATE_PUBLISH_BASE_MS: u64 = 40;
const SPAM_PUBLISH_BASE_MS: u64 = 75;

pub type Result<T> = std::result::Result<T, SimulationError>;

/// One directed transport link used for service accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DirectedServiceLink {
    pub source: usize,
    pub destination: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationConfig {
    pub node_count: usize,
    pub attacker_count: usize,
    pub fanout: usize,
    pub unknown_peer_reserve: usize,
    pub max_hops: u8,
    /// Syntactically valid inventories that each connected attacker sends per link.
    pub fake_inventories_per_attack_link: usize,
    /// Signed adversarial publications generated for every subscription class.
    pub signed_spam_rounds: usize,
    /// Signed legitimate publications generated for every subscription class.
    pub legitimate_publication_rounds: usize,
    pub max_processed_actions: usize,
    pub seed: u64,
    pub topology: TopologyStrategy,
    pub supernode_discovery: SupernodeDiscoveryStrategy,
    pub supernode_count: usize,
    pub adversarial_discovery_candidate_count: usize,
    pub supernode_links_per_peer: usize,
    pub supernode_fanout: usize,
    pub loss_basis_points: u32,
    pub churn_basis_points: u32,
    pub retry_delay_ms: u64,
    pub max_retries: u8,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            node_count: 1_000,
            attacker_count: 200,
            fanout: 6,
            unknown_peer_reserve: 1,
            max_hops: 16,
            fake_inventories_per_attack_link: 6,
            signed_spam_rounds: 8,
            legitimate_publication_rounds: 1,
            max_processed_actions: 10_000_000,
            seed: 0x4e4f_5354_5250_5542,
            topology: TopologyStrategy::PeerMesh,
            supernode_discovery: SupernodeDiscoveryStrategy::Mixed,
            supernode_count: 16,
            adversarial_discovery_candidate_count: 8,
            supernode_links_per_peer: 3,
            supernode_fanout: 192,
            loss_basis_points: 200,
            churn_basis_points: 300,
            retry_delay_ms: 80,
            max_retries: 3,
        }
    }
}

struct SimNode {
    mesh: InvWantMesh,
    wire: FipsPubsubWireAdapter,
    filters: Vec<Filter>,
    rating_filters: Vec<Filter>,
    machine_reputation: Option<PeerReputation>,
    machine_policies: Option<PeerReputationPolicies>,
    /// Rating authors admitted by this node's root after verified service.
    service_admitted_raters: BTreeSet<String>,
    app_authorized_authors: BTreeSet<String>,
    local_events: HashMap<String, VerifiedEvent>,
}

#[derive(Debug, Clone)]
struct Packet {
    source: usize,
    destination: usize,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionPurpose {
    Install,
    LifecycleClose,
    LifecycleReopen { observed_close: bool },
    Reconnect,
    Rediscovery,
    Flood,
}

#[derive(Debug, Clone)]
struct SubscriptionFrame {
    source: usize,
    destination: usize,
    payload: Vec<u8>,
    purpose: SubscriptionPurpose,
    traffic_provenance: TrafficProvenance,
    attempt: u8,
}

impl SubscriptionFrame {
    fn new(
        source: usize,
        destination: usize,
        payload: Vec<u8>,
        purpose: SubscriptionPurpose,
        traffic_provenance: TrafficProvenance,
    ) -> Self {
        Self {
            source,
            destination,
            payload,
            purpose,
            traffic_provenance,
            attempt: 0,
        }
    }

    const fn is_reliable(&self) -> bool {
        matches!(self.traffic_provenance, TrafficProvenance::Legitimate)
    }
}

#[derive(Debug, Clone)]
enum ScheduledAction {
    Packet(Packet),
    SendSubscription(SubscriptionFrame),
    SubscriptionArrived(SubscriptionFrame),
    Publish(String),
    RetryInventory {
        source: usize,
        destination: usize,
        event_id: String,
    },
    AdvanceVirtualTime,
    ReputationSweep,
    RediscoverySweep,
    LinkDown(LinkOutage),
    LinkUp(LinkOutage),
}

#[derive(Debug, Clone)]
struct EventMetadata {
    class: SubscriptionClass,
    legitimate: bool,
    spam_identity: Option<SpamIdentity>,
    publisher: usize,
    verified: VerifiedEvent,
    payload_bytes: u64,
    publish_at_ms: u64,
    interested: BTreeSet<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpamIdentity {
    Persistent,
    FreshSybil,
}

impl SpamIdentity {
    const ALL: [Self; 2] = [Self::Persistent, Self::FreshSybil];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::FreshSybil => "fresh-sybil",
        }
    }
}

#[derive(Debug, Clone)]
struct ReputationEventMetadata {
    subject: usize,
    observed_at_ms: u64,
    origin: ReputationEventOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReputationEventOrigin {
    HonestObservation { quiet_blackhole: bool },
    PositiveServiceEndorsement,
    MachineLifecycle(MachineLifecyclePhase),
    UnconnectedRatingPressure,
    RevokedRaterRating,
    ForgedProbe,
    PoisonedProbe,
    AdmittedRaterPoison,
}

impl ReputationEventOrigin {
    const fn is_spam(self) -> bool {
        !matches!(
            self,
            Self::HonestObservation { .. }
                | Self::PositiveServiceEndorsement
                | Self::MachineLifecycle(_)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MachineLifecyclePhase {
    Admit,
    Remove,
    Readmit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionDrop {
    MachineReputation,
    Application,
}

struct WorkloadPair {
    class: SubscriptionClass,
    filter: Filter,
    #[cfg(test)]
    legitimate_event_id: String,
    #[cfg(test)]
    spam_event_id: Option<String>,
}

#[derive(Debug, Default)]
struct RetryState {
    attempts: u8,
    scheduled: bool,
}

struct Simulation {
    config: SimulationConfig,
    mode: PeerSelectionMode,
    codec: InvWantCodec,
    fips_codec: FipsPubsubWireCodec,
    keys: Vec<Keys>,
    peer_ids: Vec<String>,
    peer_indices: HashMap<String, usize>,
    topology: TopologyResult,
    nodes: Vec<SimNode>,
    scheduler: VirtualScheduler<ScheduledAction>,
    events: HashMap<String, EventMetadata>,
    routing_probes: Vec<VerifiedEvent>,
    reputation_events: HashMap<String, ReputationEventMetadata>,
    reputation_publishers: Vec<Option<PeerRatingPublisher>>,
    rating_receipts: HashSet<(usize, String)>,
    machine_lifecycle_progress: HashMap<(usize, usize), u8>,
    reputation_removal_latencies: Vec<u64>,
    forged_rating_published: bool,
    poisoned_rating_published: bool,
    #[cfg(test)]
    workload_pairs: Vec<WorkloadPair>,
    delivery_times: HashMap<(usize, String), u64>,
    down_links: HashSet<LinkOutage>,
    fault_attempts: HashMap<(usize, usize, u64), u64>,
    retry_counts: HashMap<(usize, usize, String), RetryState>,
    retry_needed: HashSet<(usize, String)>,
    disrupted_transfers: HashSet<(usize, String)>,
    bad_observed_at: HashMap<(usize, usize), u64>,
    traffic: Vec<NodeTrafficLedger>,
    node_resources: Vec<NodeResourceLedger>,
    link_traffic: BTreeMap<DirectedServiceLink, NodeTrafficLedger>,
    delivery_credits: BTreeMap<DirectedServiceLink, usize>,
    delivery_bytes: BTreeMap<DirectedServiceLink, u64>,
    verified_delivery_credits: BTreeMap<DirectedServiceLink, usize>,
    verified_delivery_bytes: BTreeMap<DirectedServiceLink, u64>,
    endpoint_connection_limits: Vec<usize>,
    rediscovery: Vec<RediscoveryState>,
    rediscovery_new_links: BTreeSet<(usize, usize)>,
    rating_subscription_dirty: BTreeSet<usize>,
    positive_endorsements: Vec<Vec<usize>>,
    positive_service_admissions: HashMap<(usize, usize), (usize, u64)>,
    admitted_rater_poison_targets: BTreeSet<(usize, usize)>,
    admitted_rater_poison_source: Option<(usize, usize)>,
    admitted_rater_post_revocation_target: Option<(usize, usize)>,
    unconnected_rating_pressure_target: Option<(usize, usize, usize, usize, u64)>,
    report: SimulationReport,
}

pub fn run_simulation(
    config: SimulationConfig,
    mode: PeerSelectionMode,
) -> Result<SimulationReport> {
    Simulation::new(config, mode)?.run()
}

fn simulation_keys(node_count: usize) -> Result<Vec<Keys>> {
    let mut keys = (0..node_count)
        .map(|index| {
            Keys::parse(&format!("{:064x}", index.saturating_add(1)))
                .map_err(|error| SimulationError::Pubsub(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    keys.sort_by_key(|keys| keys.public_key().to_hex());
    Ok(keys)
}

fn peer_rating_event(
    signer: &Keys,
    rater: &str,
    subject: &str,
    value: i64,
    created_at: u64,
) -> Result<Event> {
    peer_rating_event_with_samples(signer, rater, subject, value, 3, created_at)
}

fn peer_rating_event_with_samples(
    signer: &Keys,
    rater: &str,
    subject: &str,
    value: i64,
    sample_count: u64,
    created_at: u64,
) -> Result<Event> {
    let mut rating = Rating::new(rater, subject, value, 0, 100);
    rating.id = deterministic_rating_id(rater, subject, value, created_at);
    rating.scope = Some(PeerReputationConfig::default().scope);
    rating.created_at = created_at;
    rating.sample_count = Some(sample_count);
    let encoded = rating.to_event(signer).map_err(pubsub_error)?;
    EventBuilder::new(encoded.kind, encoded.content)
        .tags(encoded.tags)
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(signer)
        .map_err(pubsub_error)
}

fn poll_ready<F>(future: F) -> Result<F::Output>
where
    F: Future,
{
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => Ok(output),
        Poll::Pending => Err(SimulationError::Pubsub(
            "simulation policy unexpectedly waited on external I/O".to_string(),
        )),
    }
}

fn profile_subscription_id(node: usize) -> SubscriptionId {
    SubscriptionId::new(format!("profile-{node}"))
}

fn message_traffic_provenance(
    message: &InvWantWireMessage,
    events: &HashMap<String, EventMetadata>,
    reputation_events: &HashMap<String, ReputationEventMetadata>,
) -> TrafficProvenance {
    let event_id = wire_event_id(message);
    if reputation_events
        .get(event_id)
        .is_some_and(|metadata| !metadata.origin.is_spam())
        || events
            .get(event_id)
            .is_some_and(|metadata| metadata.legitimate)
    {
        TrafficProvenance::Legitimate
    } else {
        TrafficProvenance::Adversarial
    }
}

fn wire_event_id(message: &InvWantWireMessage) -> &str {
    match message {
        InvWantWireMessage::Inventory { event_id, .. }
        | InvWantWireMessage::Want { event_id }
        | InvWantWireMessage::Frame { event_id, .. } => event_id,
    }
}

fn message_fault_key(message: &InvWantWireMessage) -> u64 {
    let kind = match message {
        InvWantWireMessage::Inventory { .. } => 0x494e_5600_0000_0001,
        InvWantWireMessage::Want { .. } => 0x5741_4e54_0000_0002,
        InvWantWireMessage::Frame { .. } => 0x4652_414d_4500_0003,
    };
    kind ^ hash_bytes(wire_event_id(message).as_bytes())
}

fn class_name(class: SubscriptionClass) -> &'static str {
    match class {
        SubscriptionClass::AuthorFeed => "author-feed",
        SubscriptionClass::HashtagTopic => "hashtag-topic",
        SubscriptionClass::HashtreeUpdate => "hashtree-update",
        SubscriptionClass::TargetedApprovalRating => "targeted-approval-rating",
        SubscriptionClass::IrisDriveBroadRoot => "iris-drive-broad-root",
        SubscriptionClass::FipsAdvert => "fips-advert",
        SubscriptionClass::FipsPaidOffer => "fips-paid-offer",
        SubscriptionClass::GitRepoAnnouncement => "git-repo-announcement",
    }
}

const fn machine_admitted_class(class: SubscriptionClass) -> bool {
    matches!(
        class,
        SubscriptionClass::TargetedApprovalRating
            | SubscriptionClass::FipsAdvert
            | SubscriptionClass::FipsPaidOffer
    )
}

fn is_quiet_attacker(index: usize) -> bool {
    index.is_multiple_of(4)
}

fn is_fresh_sybil(index: usize) -> bool {
    index % 4 == 1
}

fn link_key(left: usize, right: usize) -> (usize, usize) {
    if left < right {
        (left, right)
    } else {
        (right, left)
    }
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn deterministic_rating_id(rater: &str, subject: &str, value: i64, created_at: u64) -> String {
    let seed = hash_bytes(rater.as_bytes())
        ^ hash_bytes(subject.as_bytes()).rotate_left(23)
        ^ value.cast_unsigned().rotate_left(41)
        ^ created_at;
    let high = mix64(seed);
    let low = mix64(seed ^ 0xa076_1d64_78bd_642f);
    format!(
        "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
        high >> 32,
        (high >> 16) & 0xffff,
        high & 0x0fff,
        (low >> 48) & 0x0fff,
        low & 0xffff_ffff_ffff,
    )
}

fn pubsub_error(error: impl std::fmt::Display) -> SimulationError {
    SimulationError::Pubsub(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TrafficScope;

    #[test]
    fn every_workload_class_has_interested_peers() {
        let simulation = Simulation::new(
            SimulationConfig {
                node_count: 120,
                attacker_count: 24,
                supernode_count: 8,
                adversarial_discovery_candidate_count: 4,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .unwrap();
        let targeted_pair = simulation
            .workload_pairs
            .iter()
            .find(|pair| pair.class == SubscriptionClass::TargetedApprovalRating)
            .unwrap();
        let targeted_event = simulation
            .events
            .get(&targeted_pair.legitimate_event_id)
            .unwrap();
        assert_eq!(
            PubsubPeerInterest::from_filters(
                std::slice::from_ref(&targeted_pair.filter),
                &targeted_event.verified,
            ),
            PubsubPeerInterest::Subscribed
        );
        let targeted_node = simulation.config.attacker_count + 3;
        assert_eq!(
            PubsubPeerInterest::from_filters(
                &simulation.nodes[targeted_node].filters,
                &targeted_event.verified,
            ),
            PubsubPeerInterest::Subscribed,
            "node filters: {:?}",
            simulation.nodes[targeted_node].filters
        );
        for metadata in simulation.events.values().filter(|event| event.legitimate) {
            assert!(
                !metadata.interested.is_empty(),
                "{} has no interested peers",
                class_name(metadata.class)
            );
        }
    }

    #[test]
    fn adversarial_simulation_is_deterministic() {
        let config = SimulationConfig {
            node_count: 120,
            attacker_count: 24,
            loss_basis_points: 100,
            churn_basis_points: 200,
            fake_inventories_per_attack_link: 2,
            signed_spam_rounds: 2,
            supernode_count: 8,
            adversarial_discovery_candidate_count: 4,
            ..SimulationConfig::default()
        };
        let mut first =
            run_simulation(config.clone(), PeerSelectionMode::SharedReputation).unwrap();
        let mut second = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();
        let first_link_service = std::mem::take(&mut first.protocol_service_by_link);
        let second_link_service = std::mem::take(&mut second.protocol_service_by_link);
        let first_role_service = std::mem::take(&mut first.protocol_service_by_role);
        let second_role_service = std::mem::take(&mut second.protocol_service_by_role);
        let first_credits = std::mem::take(&mut first.interested_delivery_credit_by_link);
        let second_credits = std::mem::take(&mut second.interested_delivery_credit_by_link);
        assert_eq!(first, second);
        assert_eq!(first_link_service, second_link_service);
        assert_eq!(first_role_service, second_role_service);
        assert_eq!(first_credits, second_credits);
        assert!(first.protocol_accounting_is_conserved(), "{first:?}");
        assert!(first.forged_machine_ratings_published > 0);
        assert!(first.forged_machine_ratings_received > 0);
        assert!(first.forged_machine_ratings_evaluated > 0);
        assert_eq!(first.forged_machine_ratings_ingested, 0);
        assert_eq!(
            first.forged_machine_ratings_rejected,
            first.forged_machine_ratings_evaluated
        );
    }

    #[test]
    fn shared_machine_reputation_filters_spam_without_losing_interested_delivery() {
        let config = SimulationConfig {
            node_count: 180,
            attacker_count: 36,
            loss_basis_points: 0,
            churn_basis_points: 0,
            supernode_count: 8,
            adversarial_discovery_candidate_count: 4,
            ..SimulationConfig::default()
        };
        let local = run_simulation(config.clone(), PeerSelectionMode::LocalBehavior).unwrap();
        let shared = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();
        assert!(shared.delivery_basis_points >= 9_500, "{shared:?}");
        assert!(
            shared.worst_cohort_delivery_basis_points >= 9_500,
            "{shared:?}"
        );
        assert!(
            shared.spam_delivered.saturating_mul(5) <= local.spam_delivered.saturating_mul(4),
            "{local:?}\n{shared:?}"
        );
        assert!(shared.spam_suppression_basis_points >= 5_000, "{shared:?}");
        let shared_persistent = shared.machine_admitted_spam_deliveries_by_identity["persistent"];
        let local_persistent = local.machine_admitted_spam_deliveries_by_identity["persistent"];
        assert!(
            shared_persistent.saturating_mul(2) <= local_persistent,
            "{local:?}\n{shared:?}"
        );
        assert!(
            shared.machine_admitted_spam_suppression_basis_points_by_identity["persistent"]
                >= 5_000,
            "{shared:?}"
        );
        let expected_fresh =
            shared.expected_machine_admitted_spam_deliveries_by_identity["fresh-sybil"];
        assert!(expected_fresh > 0, "{shared:?}");
        assert_eq!(
            shared.machine_admitted_spam_deliveries_by_identity["fresh-sybil"], expected_fresh,
            "fresh unknown identities must exercise the open-admission gap: {shared:?}"
        );
        assert!(
            shared.filter_suppression_basis_points > 0
                && shared.filter_suppression_basis_points < 10_000,
            "in-scope and near-miss spam must both exercise subscription filters: {shared:?}"
        );
        assert_eq!(shared.uninterested_deliveries, 0, "{shared:?}");
        assert!(shared.unauthorized_source_drops > 0, "{shared:?}");
        assert_eq!(
            shared.honest_source_legitimate_machine_ingress_drops, 0,
            "{shared:?}"
        );
        assert!(shared.machine_ingress_accounting_is_conserved());
        assert_eq!(shared.machine_false_positive_removals, 0, "{shared:?}");
        assert!(shared.forged_machine_ratings_published > 0, "{shared:?}");
        assert!(shared.forged_machine_ratings_received > 0, "{shared:?}");
        assert!(shared.forged_machine_ratings_evaluated > 0, "{shared:?}");
        assert_eq!(shared.forged_machine_ratings_ingested, 0, "{shared:?}");
        assert_eq!(
            shared.forged_machine_ratings_rejected, shared.forged_machine_ratings_evaluated,
            "{shared:?}"
        );
        assert!(shared.machine_transported_transitions > 0, "{shared:?}");
        assert!(shared.machine_positive_admissions > 0, "{shared:?}");
        assert!(shared.machine_removals > 0, "{shared:?}");
        let attacker_traffic = shared
            .protocol_service_by_role
            .get(&NodeRole::Attacker)
            .expect("adversarial workload must exercise attacker-role traffic");
        assert!(
            attacker_traffic
                .counter(TrafficDirection::Sent, TrafficProvenance::Adversarial,)
                .bytes
                > 0,
            "attacker bootstrap and flood controls must remain adversarial: {shared:?}"
        );
        assert!(
            attacker_traffic
                .counter(TrafficDirection::Received, TrafficProvenance::Legitimate,)
                .bytes
                > 0,
            "legitimate workload provenance must not change at an attacker carrier: {shared:?}"
        );
        assert!(shared.protocol_accounting_is_conserved(), "{shared:?}");
    }

    #[test]
    fn hybrid_supernodes_expose_discovery_and_service_load_kpis() {
        let config = SimulationConfig {
            node_count: 180,
            attacker_count: 36,
            topology: TopologyStrategy::HybridSupernodes,
            supernode_count: 8,
            adversarial_discovery_candidate_count: 4,
            supernode_links_per_peer: 3,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        };
        let report = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();
        assert_eq!(report.supernode_count, 8);
        assert!(report.discovery_links > 0, "{report:?}");
        assert!(report.selected_high_capacity_links > 0, "{report:?}");
        assert!(
            report.selected_adversarial_candidate_links > 0,
            "{report:?}"
        );
        assert!(report.supernode_max_service_bytes > 0, "{report:?}");
        assert!(!report.protocol_service_by_link.is_empty(), "{report:?}");
        assert!(
            report
                .protocol_service_by_role
                .get(&NodeRole::Supernode)
                .is_some_and(|ledger| ledger.total(TrafficScope::Combined).bytes > 0),
            "{report:?}"
        );
        assert!(report.protocol_accounting_is_conserved(), "{report:?}");
        assert!(report.delivery_basis_points >= 9_000, "{report:?}");
    }
}
