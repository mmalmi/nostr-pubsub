mod admission;
mod control_transport;
mod delivery;
mod engine;
mod lifecycle;
mod network;
mod report;
mod reputation_flow;
mod reputation_probes;
mod resources;
mod setup;

pub use delivery::VerifiedDeliveryRecord;
use resources::NodeResourceLedger;
pub use resources::{
    CpuWorkDistribution, NodeCpuWork, NodeRetainedUsage, ResourceCohortReport,
    RetainedUsageDistribution, SimulationResourceReport,
};

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

const SIM_PROTOCOL: &str = "nostr.pubsub.sim";
const SIM_VERSION: u8 = 1;
const SIM_UNIX_BASE: u64 = 1_700_000_000;
const MALFORMED_TRAINING_SAMPLES: usize = 5;
// The first sweep follows the simulator's 60 ms Inv/Want route expiry so
// silent inventory blackholes have become production peer-behavior evidence.
const REPUTATION_SWEEP_MS: u64 = 100;
const POST_ROUTE_REPUTATION_SWEEP_MS: u64 = 1_075;
const POST_RECONNECT_REPUTATION_SWEEP_MS: u64 = 2_140;
const CHURN_START_MS: u64 = 30;
const CHURN_END_MS: u64 = 110;
const LEGITIMATE_PUBLISH_BASE_MS: u64 = 40;
const SPAM_PUBLISH_BASE_MS: u64 = 75;

pub type Result<T> = std::result::Result<T, SimulationError>;

#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("invalid simulation configuration: {0}")]
    InvalidConfig(String),
    #[error("pubsub simulation failed: {0}")]
    Pubsub(String),
    #[error("simulation exceeded its {0} scheduled-action processing budget")]
    ActionBudgetExceeded(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSelectionMode {
    Neutral,
    LocalBehavior,
    SharedReputation,
}

impl PeerSelectionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Neutral => "neutral",
            Self::LocalBehavior => "local-behavior",
            Self::SharedReputation => "shared-reputation",
        }
    }
}

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
    pub false_supernode_count: usize,
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
            false_supernode_count: 8,
            supernode_links_per_peer: 3,
            supernode_fanout: 192,
            loss_basis_points: 200,
            churn_basis_points: 300,
            retry_delay_ms: 80,
            max_retries: 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationReport {
    pub config: SimulationConfig,
    pub mode: PeerSelectionMode,
    pub topology: TopologyStrategy,
    pub discovery: SupernodeDiscoveryStrategy,
    pub node_count: usize,
    pub attacker_count: usize,
    pub honest_node_count: usize,
    pub supernode_count: usize,
    /// Ground-truth role of each simulated node, indexed by node identifier.
    pub node_roles: Vec<NodeRole>,
    pub topology_edges: usize,
    pub max_node_degree: usize,
    pub legitimate_events: usize,
    pub spam_events: usize,
    pub expected_legitimate_deliveries: usize,
    pub expected_signed_spam_deliveries: usize,
    pub expected_signed_spam_deliveries_by_class: BTreeMap<String, usize>,
    pub expected_signed_spam_deliveries_by_identity: BTreeMap<String, usize>,
    pub expected_machine_admitted_spam_deliveries_by_identity: BTreeMap<String, usize>,
    /// Active ordinary-peer links on which production subscription routing
    /// considered a signed spam event.
    pub spam_filter_peer_link_opportunities: usize,
    pub spam_filter_peer_link_opportunities_by_class: BTreeMap<String, usize>,
    /// Those opportunities for which the production subscription store
    /// suppressed inventory delivery.
    pub spam_filter_suppressed_peer_links: usize,
    pub spam_filter_suppressed_peer_links_by_class: BTreeMap<String, usize>,
    pub spam_filter_suppression_basis_points_by_class: BTreeMap<String, u32>,
    pub delivered_legitimate: usize,
    pub local_legitimate_deliveries: usize,
    pub delivery_basis_points: u32,
    pub worst_cohort_delivery_basis_points: u32,
    pub cohort_delivery_basis_points: BTreeMap<String, u32>,
    pub latency_sample_count: usize,
    pub latency_p50_ms: u64,
    pub latency_p95_ms: u64,
    pub latency_p99_ms: u64,
    pub max_delivered_latency_ms: u64,
    /// Remote interested deliveries with reconstructable dissemination paths.
    pub delivery_path_samples: usize,
    pub multihop_interested_deliveries: usize,
    pub multihop_interested_delivery_basis_points: u32,
    pub delivery_path_hops_p50: u64,
    pub delivery_path_hops_p95: u64,
    pub delivery_path_hops_p99: u64,
    pub delivery_path_hops_max: u64,
    pub undelivered_legitimate: usize,
    pub spam_delivered: usize,
    pub signed_spam_deliveries_by_class: BTreeMap<String, usize>,
    pub signed_spam_deliveries_by_identity: BTreeMap<String, usize>,
    pub machine_admitted_spam_deliveries_by_identity: BTreeMap<String, usize>,
    pub signed_spam_delivery_basis_points: u32,
    pub signed_spam_delivery_basis_points_by_class: BTreeMap<String, u32>,
    pub signed_spam_suppression_basis_points_by_identity: BTreeMap<String, u32>,
    pub machine_admitted_spam_suppression_basis_points_by_identity: BTreeMap<String, u32>,
    pub unknown_discovery_adverts_delivered: usize,
    pub spam_dropped_by_machine_policy: usize,
    pub spam_dropped_by_application_policy: usize,
    pub spam_suppression_basis_points: u32,
    pub uninterested_deliveries: usize,
    pub uninterested_legitimate_deliveries: usize,
    pub uninterested_spam_deliveries: usize,
    pub filter_suppression_basis_points: u32,
    pub processed_actions: usize,
    pub processed_messages: usize,
    pub inventory_messages: usize,
    pub want_messages: usize,
    pub frame_messages: usize,
    pub data_plane_wire_bytes: u64,
    pub legitimate_protocol_bytes: u64,
    pub adversarial_protocol_bytes: u64,
    pub legitimate_protocol_byte_share_basis_points: u32,
    pub protocol_messages_per_interested_delivery_milli: u64,
    pub dropped_packets: usize,
    pub dropped_at_attackers: usize,
    /// Retry inventories actually sent through the production replay path.
    pub retry_inventories: usize,
    /// Disrupted legitimate transfers that were eventually delivered by any path.
    pub eventual_disrupted_transfer_recoveries: usize,
    pub disrupted_legitimate_transfers: usize,
    pub eventual_disrupted_transfer_recovery_basis_points: u32,
    pub max_queue_depth: usize,
    pub virtual_duration_ms: u64,
    pub injected_attack_inventories: usize,
    pub rejected_malformed_messages: usize,
    pub unauthorized_source_drops: usize,
    pub machine_ingress_drops: usize,
    /// Legitimate-provenance packets blocked when carried by an honest peer.
    pub honest_source_legitimate_machine_ingress_drops: usize,
    /// Attacker packets that referenced a legitimate event ID when blocked.
    pub attacker_source_legitimate_reference_machine_ingress_drops: usize,
    /// Adversarial-provenance packets blocked regardless of carrier role.
    pub adversarial_machine_ingress_drops: usize,
    pub machine_ratings_published: usize,
    pub machine_ratings_received: usize,
    /// Structurally valid ratings retained by `PeerReputation`.
    pub machine_ratings_ingested: usize,
    pub poisoned_machine_ratings_published: usize,
    pub poisoned_machine_ratings_received: usize,
    pub poisoned_machine_ratings_ingested: usize,
    pub poisoned_machine_ratings_rejected: usize,
    pub machine_transported_transitions: usize,
    pub machine_transported_positive_admissions: usize,
    pub machine_transported_removals: usize,
    pub machine_lifecycle_ratings_published: usize,
    pub machine_lifecycle_admissions: usize,
    pub machine_lifecycle_removals: usize,
    pub machine_lifecycle_readmissions: usize,
    pub machine_reversible_lifecycles: usize,
    pub machine_positive_admissions: usize,
    pub machine_removals: usize,
    pub machine_quiet_blackhole_removals: usize,
    pub machine_poisoning_removals: usize,
    pub machine_false_positive_removals: usize,
    pub machine_removal_latency_p95_ms: u64,
    pub forged_machine_ratings_published: usize,
    pub forged_machine_ratings_received: usize,
    pub forged_machine_ratings_evaluated: usize,
    pub forged_machine_ratings_ingested: usize,
    pub forged_machine_ratings_rejected: usize,
    pub legitimate_policy_drops: usize,
    pub legitimate_application_policy_drops: usize,
    pub machine_trust_edges: usize,
    pub subscription_messages: usize,
    pub control_plane_wire_bytes: u64,
    pub subscription_retries: usize,
    pub subscription_retry_recoveries: usize,
    pub subscription_rejections: usize,
    pub subscription_evictions: usize,
    pub subscription_close_reopen_successes: usize,
    /// Inv/WANT actions sent while the local policy still classified the peer as unknown.
    pub unknown_candidate_sends: usize,
    /// Scheduled link-outage episodes, including forced supernode outages.
    pub churned_links: usize,
    pub discovery_links: usize,
    pub honest_supernode_links: usize,
    pub false_supernode_links: usize,
    pub supernode_discovery_precision_basis_points: u32,
    pub honest_supernode_coverage_basis_points: u32,
    pub false_only_supernode_peers: usize,
    pub supernode_max_service_bytes: u64,
    pub supernode_mean_service_bytes: u64,
    pub supernode_load_gini_basis_points: u32,
    pub total_protocol_bytes: u64,
    pub sent_link_protocol_bytes: u64,
    pub sent_role_protocol_bytes: u64,
    pub protocol_bytes_per_interested_delivery: u64,
    pub resource_usage: SimulationResourceReport,
    /// Attempted and received Inv/WANT and FIPS control traffic per directed link.
    pub protocol_service_by_link: BTreeMap<DirectedServiceLink, NodeTrafficLedger>,
    /// Inv/WANT and FIPS control service aggregated by the carrier's node role.
    pub protocol_service_by_role: BTreeMap<NodeRole, NodeTrafficLedger>,
    /// Successful interested application deliveries credited to their final directed hop.
    pub interested_delivery_credit_by_link: BTreeMap<DirectedServiceLink, usize>,
    /// Final-hop interested delivery credits aggregated by the carrier's role.
    pub interested_delivery_credit_by_source_role: BTreeMap<NodeRole, usize>,
    /// Exact useful application payload bytes credited to each final directed hop.
    pub interested_delivery_bytes_by_link: BTreeMap<DirectedServiceLink, u64>,
    /// Final-hop useful payload bytes aggregated by the carrier's role.
    pub interested_delivery_bytes_by_source_role: BTreeMap<NodeRole, u64>,
    /// First accepted legitimate frame deliveries on every directed transport hop.
    pub verified_delivery_credit_by_link: BTreeMap<DirectedServiceLink, usize>,
    /// Exact application payload bytes carried by first accepted legitimate frames.
    pub verified_delivery_bytes_by_link: BTreeMap<DirectedServiceLink, u64>,
    /// Verified-hop payload bytes aggregated by the sending node's role.
    pub verified_delivery_bytes_by_source_role: BTreeMap<NodeRole, u64>,
    /// Per-event edges for first accepted legitimate frames that served an
    /// interested receiver or were forwarded onward.
    pub verified_delivery_records: Vec<VerifiedDeliveryRecord>,
}

impl SimulationReport {
    /// Whether independently accumulated protocol-byte ledgers agree exactly.
    #[must_use]
    pub fn protocol_accounting_is_conserved(&self) -> bool {
        self.data_plane_wire_bytes
            .saturating_add(self.control_plane_wire_bytes)
            == self.total_protocol_bytes
            && self
                .legitimate_protocol_bytes
                .saturating_add(self.adversarial_protocol_bytes)
                == self.total_protocol_bytes
            && self.sent_link_protocol_bytes == self.total_protocol_bytes
            && self.sent_role_protocol_bytes == self.total_protocol_bytes
    }

    /// Whether every machine-ingress drop has exactly one ground-truth class.
    #[must_use]
    pub fn machine_ingress_accounting_is_conserved(&self) -> bool {
        self.honest_source_legitimate_machine_ingress_drops
            .saturating_add(self.attacker_source_legitimate_reference_machine_ingress_drops)
            .saturating_add(self.adversarial_machine_ingress_drops)
            == self.machine_ingress_drops
    }
}

struct SimNode {
    mesh: InvWantMesh,
    wire: FipsPubsubWireAdapter,
    filters: Vec<Filter>,
    rating_filters: Vec<Filter>,
    machine_reputation: Option<PeerReputation>,
    machine_policies: Option<PeerReputationPolicies>,
    machine_trusted_raters: BTreeSet<String>,
    app_authorized_authors: BTreeSet<String>,
    local_events: HashMap<String, Event>,
    rejected_events: HashSet<String>,
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
    LinkDown(LinkOutage),
    LinkUp(LinkOutage),
}

#[derive(Debug, Clone)]
struct EventMetadata {
    class: SubscriptionClass,
    legitimate: bool,
    spam_identity: Option<SpamIdentity>,
    publisher: usize,
    event: Event,
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
    MachineLifecycle(MachineLifecyclePhase),
    ForgedProbe,
    PoisonedProbe,
}

impl ReputationEventOrigin {
    const fn is_spam(self) -> bool {
        !matches!(
            self,
            Self::HonestObservation { .. } | Self::MachineLifecycle(_)
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
                false_supernode_count: 4,
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
            false_supernode_count: 4,
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
            false_supernode_count: 4,
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
            false_supernode_count: 4,
            supernode_links_per_peer: 3,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        };
        let report = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();
        assert_eq!(report.supernode_count, 8);
        assert!(report.discovery_links > 0, "{report:?}");
        assert!(report.honest_supernode_links > 0, "{report:?}");
        assert!(report.false_supernode_links > 0, "{report:?}");
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
