use std::collections::{BTreeSet, HashMap, VecDeque};

use nostr::{Event, EventBuilder, Keys, Kind, Timestamp};
use nostr_pubsub::{
    DEFAULT_INV_WANT_MAX_WIRE_BYTES, InvWantAction, InvWantCodec, InvWantMesh, InvWantMeshOptions,
    InvWantWireMessage, MeshPeer,
};
use nostr_pubsub_social_graph::{PeerReputation, PeerReputationConfig};
use nostr_social_graph::Rating;
use nostr_social_memory::RatingEventExt;

const SIM_PROTOCOL: &str = "nostr.pubsub.sim";
const SIM_VERSION: u8 = 1;
const SIM_EVENT_KIND: u16 = 37_195;
const ATTACK_PEERS_PER_HONEST_NODE: usize = 4;
const MALFORMED_SAMPLES_PER_ATTACK_LINK: usize = 3;

pub type Result<T> = std::result::Result<T, SimulationError>;

#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("invalid simulation configuration: {0}")]
    InvalidConfig(String),
    #[error("pubsub simulation failed: {0}")]
    Pubsub(String),
    #[error("simulation exceeded its {0} message processing budget")]
    MessageBudgetExceeded(usize),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationConfig {
    pub node_count: usize,
    pub attacker_count: usize,
    pub fanout: usize,
    pub unknown_peer_reserve: usize,
    pub max_hops: u8,
    pub attack_inventories_per_honest_node: usize,
    pub max_processed_messages: usize,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            node_count: 1_000,
            attacker_count: 200,
            fanout: 4,
            unknown_peer_reserve: 1,
            max_hops: 12,
            attack_inventories_per_honest_node: 1,
            max_processed_messages: 5_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationReport {
    pub mode: PeerSelectionMode,
    pub node_count: usize,
    pub attacker_count: usize,
    pub honest_node_count: usize,
    pub delivered_honest_nodes: usize,
    pub delivery_basis_points: u32,
    pub processed_messages: usize,
    pub injected_attack_inventories: usize,
    pub rejected_malformed_messages: usize,
    pub inventory_messages: usize,
    pub want_messages: usize,
    pub frame_messages: usize,
    pub wire_bytes: u64,
    pub dropped_at_attackers: usize,
    pub sends_to_unknown_peers: usize,
}

struct SimNode {
    mesh: InvWantMesh,
    peers: Vec<MeshPeer>,
    attacker: bool,
}

struct Packet {
    source: usize,
    destination: usize,
    payload: Vec<u8>,
}

struct Simulation {
    config: SimulationConfig,
    mode: PeerSelectionMode,
    codec: InvWantCodec,
    nodes: Vec<SimNode>,
    peer_ids: Vec<String>,
    peer_indices: HashMap<String, usize>,
    queue: VecDeque<Packet>,
    delivered: BTreeSet<usize>,
    report: SimulationReport,
    now_ms: u64,
}

pub fn run_simulation(
    config: SimulationConfig,
    mode: PeerSelectionMode,
) -> Result<SimulationReport> {
    Simulation::new(config, mode)?.run()
}

impl Simulation {
    fn new(config: SimulationConfig, mode: PeerSelectionMode) -> Result<Self> {
        validate_config(&config)?;
        let honest_node_count = config.node_count - config.attacker_count;
        let keys = simulation_keys(config.node_count)?;
        let peer_ids = keys
            .iter()
            .map(|keys| keys.public_key().to_hex())
            .collect::<Vec<_>>();
        let peer_indices = peer_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, peer_id)| (peer_id, index))
            .collect();
        let mut nodes = Vec::with_capacity(config.node_count);
        for node_index in 0..config.node_count {
            nodes.push(SimNode {
                mesh: InvWantMesh::new(mesh_options(&config)),
                peers: peers_for_node(&config, mode, node_index, &peer_ids, &keys)?,
                attacker: node_index < config.attacker_count,
            });
        }
        Ok(Self {
            codec: InvWantCodec::new(SIM_PROTOCOL, SIM_VERSION, DEFAULT_INV_WANT_MAX_WIRE_BYTES),
            queue: VecDeque::new(),
            delivered: BTreeSet::new(),
            report: SimulationReport {
                mode,
                node_count: config.node_count,
                attacker_count: config.attacker_count,
                honest_node_count,
                delivered_honest_nodes: 0,
                delivery_basis_points: 0,
                processed_messages: 0,
                injected_attack_inventories: 0,
                rejected_malformed_messages: 0,
                inventory_messages: 0,
                want_messages: 0,
                frame_messages: 0,
                wire_bytes: 0,
                dropped_at_attackers: 0,
                sends_to_unknown_peers: 0,
            },
            mode,
            config,
            nodes,
            peer_ids,
            peer_indices,
            now_ms: 1,
        })
    }

    fn run(mut self) -> Result<SimulationReport> {
        self.inject_attacker_pressure()?;
        self.drain_queue()?;
        self.publish_real_event()?;
        self.drain_queue()?;

        self.report.delivered_honest_nodes = self.delivered.len();
        self.report.delivery_basis_points = basis_points(
            self.report.delivered_honest_nodes,
            self.report.honest_node_count,
        );
        Ok(self.report)
    }

    fn drain_queue(&mut self) -> Result<()> {
        while let Some(packet) = self.queue.pop_front() {
            if self.report.processed_messages >= self.config.max_processed_messages {
                return Err(SimulationError::MessageBudgetExceeded(
                    self.config.max_processed_messages,
                ));
            }
            self.report.processed_messages += 1;
            self.now_ms = self.now_ms.saturating_add(1);
            self.process_packet(&packet)?;
        }
        Ok(())
    }

    fn inject_attacker_pressure(&mut self) -> Result<()> {
        if self.config.attacker_count == 0 {
            return Ok(());
        }
        let honest_start = self.config.attacker_count;
        for honest_position in 0..self.report.honest_node_count {
            let destination = honest_start + honest_position;
            for sequence in 0..self.config.attack_inventories_per_honest_node {
                let source =
                    attack_peer_index(honest_position, sequence, self.config.attacker_count);
                let event_id = format!(
                    "{:064x}",
                    1_u128
                        .saturating_add(source as u128)
                        .saturating_mul(1_000_003)
                        .saturating_add(sequence as u128)
                );
                let message = InvWantWireMessage::Inventory {
                    event_id,
                    event_kind: SIM_EVENT_KIND,
                    payload_bytes: 512,
                    hop_limit: self.config.max_hops.min(3),
                };
                self.enqueue(source, destination, &message)?;
                self.report.injected_attack_inventories += 1;
            }
        }

        let attack_peer_count = ATTACK_PEERS_PER_HONEST_NODE.min(self.config.attacker_count);
        for honest_position in 0..self.report.honest_node_count {
            let destination = honest_start + honest_position;
            for sequence in 0..attack_peer_count {
                let source =
                    attack_peer_index(honest_position, sequence, self.config.attacker_count);
                for _ in 0..MALFORMED_SAMPLES_PER_ATTACK_LINK {
                    self.queue.push_back(Packet {
                        source,
                        destination,
                        payload: br#"{"protocol":"wrong","version":1,"message":{}}"#.to_vec(),
                    });
                }
            }
        }
        Ok(())
    }

    fn publish_real_event(&mut self) -> Result<()> {
        let publisher = self.config.attacker_count;
        self.delivered.insert(publisher);
        let peers = self.nodes[publisher].peers.clone();
        let actions = self.nodes[publisher]
            .mesh
            .publish(signed_sim_event(), &peers, self.now_ms)
            .map_err(pubsub_error)?;
        self.dispatch_actions(publisher, actions)
    }

    fn process_packet(&mut self, packet: &Packet) -> Result<()> {
        if self.nodes[packet.destination].attacker {
            self.report.dropped_at_attackers += 1;
            return Ok(());
        }
        let source_id = self.peer_ids[packet.source].clone();
        let Ok(message) = self.codec.decode(&packet.payload) else {
            self.report.rejected_malformed_messages += 1;
            if self.mode != PeerSelectionMode::Neutral {
                self.nodes[packet.destination]
                    .mesh
                    .record_invalid_message(&source_id);
            }
            return Ok(());
        };
        let peers = self.nodes[packet.destination].peers.clone();
        let actions = self.nodes[packet.destination]
            .mesh
            .receive(&source_id, message, &peers, self.now_ms)
            .map_err(pubsub_error)?;
        self.dispatch_actions(packet.destination, actions)
    }

    fn dispatch_actions(&mut self, source: usize, actions: Vec<InvWantAction>) -> Result<()> {
        for action in actions {
            match action {
                InvWantAction::Deliver { .. } => {
                    if !self.nodes[source].attacker {
                        self.delivered.insert(source);
                    }
                }
                InvWantAction::Send { peer_id, message } => {
                    let destination = self.peer_index(&peer_id)?;
                    if self.nodes[source]
                        .peers
                        .iter()
                        .find(|peer| peer.id == peer_id)
                        .is_some_and(MeshPeer::is_unknown)
                    {
                        self.report.sends_to_unknown_peers += 1;
                    }
                    self.enqueue(source, destination, &message)?;
                }
            }
        }
        Ok(())
    }

    fn enqueue(
        &mut self,
        source: usize,
        destination: usize,
        message: &InvWantWireMessage,
    ) -> Result<()> {
        let payload = self.codec.encode(message).map_err(pubsub_error)?;
        self.report.wire_bytes = self
            .report
            .wire_bytes
            .saturating_add(u64::try_from(payload.len()).unwrap_or(u64::MAX));
        match message {
            InvWantWireMessage::Inventory { .. } => self.report.inventory_messages += 1,
            InvWantWireMessage::Want { .. } => self.report.want_messages += 1,
            InvWantWireMessage::Frame { .. } => self.report.frame_messages += 1,
        }
        self.queue.push_back(Packet {
            source,
            destination,
            payload,
        });
        Ok(())
    }

    fn peer_index(&self, peer_id: &str) -> Result<usize> {
        self.peer_indices
            .get(peer_id)
            .copied()
            .ok_or_else(|| SimulationError::Pubsub(format!("invalid simulated peer id {peer_id}")))
    }
}

fn validate_config(config: &SimulationConfig) -> Result<()> {
    if config.node_count < 3 {
        return Err(SimulationError::InvalidConfig(
            "node_count must be at least 3".to_string(),
        ));
    }
    if config.attacker_count >= config.node_count {
        return Err(SimulationError::InvalidConfig(
            "attacker_count must leave at least one honest node".to_string(),
        ));
    }
    if config.node_count - config.attacker_count < 2 {
        return Err(SimulationError::InvalidConfig(
            "at least two honest nodes are required".to_string(),
        ));
    }
    if config.fanout == 0 || config.max_hops == 0 || config.max_processed_messages == 0 {
        return Err(SimulationError::InvalidConfig(
            "fanout, max_hops and max_processed_messages must be non-zero".to_string(),
        ));
    }
    Ok(())
}

fn mesh_options(config: &SimulationConfig) -> InvWantMeshOptions {
    InvWantMeshOptions {
        fanout: config.fanout,
        unknown_peer_reserve: config.unknown_peer_reserve,
        max_hops: config.max_hops,
        max_event_bytes: 64 * 1024,
        max_cached_events: 64,
        max_seen_events: 4_096,
        max_pending_peers_per_event: 64,
        route_ttl_ms: 10_000_000,
        event_ttl_ms: 10_000_000,
        allowed_kinds: Some(BTreeSet::from([SIM_EVENT_KIND])),
    }
}

fn peers_for_node(
    config: &SimulationConfig,
    mode: PeerSelectionMode,
    node_index: usize,
    peer_ids: &[String],
    keys: &[Keys],
) -> Result<Vec<MeshPeer>> {
    if node_index < config.attacker_count {
        return Ok(Vec::new());
    }
    let honest_count = config.node_count - config.attacker_count;
    let honest_position = node_index - config.attacker_count;
    let mut candidates = honest_neighbors(honest_position, honest_count)
        .into_iter()
        .map(|position| config.attacker_count + position)
        .collect::<Vec<_>>();
    let attack_peer_count = ATTACK_PEERS_PER_HONEST_NODE.min(config.attacker_count);
    candidates.extend(
        (0..attack_peer_count)
            .map(|sequence| attack_peer_index(honest_position, sequence, config.attacker_count)),
    );
    candidates.sort_unstable();
    candidates.dedup();

    match mode {
        PeerSelectionMode::Neutral => Ok(candidates
            .into_iter()
            .map(|peer| MeshPeer::new(&peer_ids[peer]))
            .collect()),
        PeerSelectionMode::LocalBehavior => {
            let first_honest_unknown = candidates
                .iter()
                .copied()
                .find(|peer| *peer >= config.attacker_count);
            Ok(candidates
                .into_iter()
                .map(|peer| {
                    if peer < config.attacker_count || Some(peer) == first_honest_unknown {
                        MeshPeer::new(&peer_ids[peer])
                    } else {
                        MeshPeer::observed(&peer_ids[peer], honest_link_score(node_index, peer))
                    }
                })
                .collect())
        }
        PeerSelectionMode::SharedReputation => {
            shared_reputation_peers(config, node_index, &candidates, peer_ids, keys)
        }
    }
}

fn shared_reputation_peers(
    config: &SimulationConfig,
    node_index: usize,
    candidates: &[usize],
    peer_ids: &[String],
    keys: &[Keys],
) -> Result<Vec<MeshPeer>> {
    let (mut reputation, policies) =
        PeerReputation::new(&peer_ids[node_index], PeerReputationConfig::default())
            .map_err(pubsub_error)?;
    let first_honest_unknown = candidates
        .iter()
        .copied()
        .find(|peer| *peer >= config.attacker_count);
    let first_attacker_unknown = candidates
        .iter()
        .copied()
        .find(|peer| *peer < config.attacker_count);
    let mut ratings = Vec::new();
    let rating_time_base = Timestamp::now()
        .as_secs()
        .saturating_sub(u64::try_from(config.node_count).unwrap_or(u64::MAX));
    for peer in candidates.iter().copied() {
        if Some(peer) == first_honest_unknown || Some(peer) == first_attacker_unknown {
            continue;
        }
        ratings.push(peer_rating_event(
            &keys[node_index],
            &peer_ids[node_index],
            &peer_ids[peer],
            if peer < config.attacker_count { 0 } else { 100 },
            rating_time_base.saturating_add(u64::try_from(peer).unwrap_or(u64::MAX)),
        )?);
    }
    reputation.replay(&ratings).map_err(pubsub_error)?;

    let mut peers = Vec::with_capacity(candidates.len());
    for peer in candidates {
        if let Some(mut selected) = policies
            .select_mesh_peer(&peer_ids[*peer])
            .map_err(pubsub_error)?
        {
            if let Some(shared_score) = selected.quality_score {
                selected = MeshPeer::observed(
                    &selected.id,
                    shared_score
                        .saturating_sub((100 - honest_link_score(node_index, *peer)) / 5)
                        .clamp(-100, 100),
                );
            }
            peers.push(selected);
        }
    }
    Ok(peers)
}

fn peer_rating_event(
    signer: &Keys,
    rater: &str,
    subject: &str,
    value: i64,
    created_at: u64,
) -> Result<Event> {
    let mut rating = Rating::new(rater, subject, value, 0, 100);
    rating.scope = Some(PeerReputationConfig::default().scope);
    rating.created_at = created_at;
    rating.sample_count = Some(3);
    rating.to_event(signer).map_err(pubsub_error)
}

fn honest_neighbors(position: usize, honest_count: usize) -> BTreeSet<usize> {
    let mut neighbors = BTreeSet::new();
    for offset in [1, 7, 31, 127] {
        let offset = offset % honest_count;
        neighbors.insert((position + offset) % honest_count);
        neighbors.insert((position + honest_count - offset) % honest_count);
    }
    neighbors.remove(&position);
    neighbors
}

fn honest_link_score(source: usize, destination: usize) -> i32 {
    let mixed = (source as u64)
        .wrapping_mul(0x9E37_79B1_85EB_CA87)
        .rotate_left(17)
        ^ (destination as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    50 + i32::try_from(mixed % 51).unwrap_or(0)
}

fn attack_peer_index(position: usize, sequence: usize, attacker_count: usize) -> usize {
    if attacker_count == 0 {
        return 0;
    }
    position
        .wrapping_add(sequence.wrapping_mul(17))
        .wrapping_rem(attacker_count)
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

fn signed_sim_event() -> Event {
    let keys = Keys::parse(&format!("{:064x}", 1)).expect("fixed simulation key is valid");
    EventBuilder::new(
        Kind::Custom(SIM_EVENT_KIND),
        "deterministic pubsub simulation",
    )
    .custom_created_at(Timestamp::from(1_u64))
    .sign_with_keys(&keys)
    .expect("fixed simulation event signs")
}

fn basis_points(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        return 0;
    }
    u32::try_from(numerator.saturating_mul(10_000) / denominator).unwrap_or(u32::MAX)
}

fn pubsub_error(error: impl std::fmt::Display) -> SimulationError {
    SimulationError::Pubsub(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thousand_node_local_behavior_priority_resists_sybil_eclipse() {
        let config = SimulationConfig::default();
        let neutral = run_simulation(config.clone(), PeerSelectionMode::Neutral).unwrap();
        let learned = run_simulation(config.clone(), PeerSelectionMode::LocalBehavior).unwrap();
        let shared = run_simulation(config, PeerSelectionMode::SharedReputation).unwrap();

        assert!(neutral.delivery_basis_points < 100, "{neutral:?}");
        assert!(learned.delivery_basis_points > 9_500, "{learned:?}");
        assert!(shared.delivery_basis_points > 9_500, "{shared:?}");
        assert!(learned.sends_to_unknown_peers > 0);
        assert!(shared.sends_to_unknown_peers > 0);
        assert_eq!(learned.injected_attack_inventories, 800);
        assert_eq!(learned.rejected_malformed_messages, 9_600);
    }

    #[test]
    fn adversarial_simulation_is_deterministic() {
        let config = SimulationConfig {
            node_count: 120,
            attacker_count: 24,
            attack_inventories_per_honest_node: 1,
            ..SimulationConfig::default()
        };

        let first = run_simulation(config.clone(), PeerSelectionMode::LocalBehavior).unwrap();
        let second = run_simulation(config, PeerSelectionMode::LocalBehavior).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.injected_attack_inventories, 96);
        assert_eq!(first.rejected_malformed_messages, 1_152);
    }
}
