use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use nostr::Event;
use serde::{Deserialize, Serialize};

use crate::{DEFAULT_INV_WANT_HOP_LIMIT, PubsubError, Result};

pub const DEFAULT_INV_WANT_FANOUT: usize = 8;
pub const DEFAULT_INV_WANT_MAX_EVENT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_INV_WANT_MAX_WIRE_BYTES: usize = DEFAULT_INV_WANT_MAX_EVENT_BYTES + 4096;

const DEFAULT_ROUTE_TTL_MS: u64 = 2 * 60 * 1_000;
const DEFAULT_EVENT_TTL_MS: u64 = 10 * 60 * 1_000;
const MAX_UPSTREAM_PROVIDERS_PER_EVENT: usize = 3;
const MAX_TRACKED_PEER_BEHAVIORS: usize = 4_096;
const MIN_PEER_BEHAVIOR_SAMPLES: u32 = 3;
const VALID_FRAME_REWARD: i32 = 20;
const INVALID_MESSAGE_PENALTY: i32 = -40;
const UNSERVED_INVENTORY_PENALTY: i32 = -20;

/// Network-neutral inventory/want/frame messages for signed Nostr events.
///
/// The containing transport chooses the protocol name, version and framing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InvWantWireMessage {
    Inventory {
        event_id: String,
        event_kind: u16,
        payload_bytes: u32,
        hop_limit: u8,
    },
    Want {
        event_id: String,
    },
    Frame {
        event_id: String,
        event: Box<Event>,
    },
}

#[derive(Debug, Serialize)]
struct InvWantEnvelopeRef<'a> {
    protocol: &'a str,
    version: u8,
    message: &'a InvWantWireMessage,
}

#[derive(Debug, Deserialize)]
struct InvWantEnvelope {
    protocol: String,
    version: u8,
    message: InvWantWireMessage,
}

/// A bounded JSON envelope codec. Transport adapters can preserve their own
/// deployed namespace and version while sharing the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvWantCodec {
    protocol: String,
    version: u8,
    max_wire_bytes: usize,
}

impl InvWantCodec {
    #[must_use]
    pub fn new(protocol: impl Into<String>, version: u8, max_wire_bytes: usize) -> Self {
        Self {
            protocol: protocol.into(),
            version,
            max_wire_bytes: max_wire_bytes.max(1),
        }
    }

    #[must_use]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    pub fn encode(&self, message: &InvWantWireMessage) -> Result<Vec<u8>> {
        let encoded = serde_json::to_vec(&InvWantEnvelopeRef {
            protocol: &self.protocol,
            version: self.version,
            message,
        })
        .map_err(|error| validation(format!("invalid inv/want JSON: {error}")))?;
        self.check_wire_len(encoded.len())?;
        Ok(encoded)
    }

    pub fn decode(&self, payload: &[u8]) -> Result<InvWantWireMessage> {
        self.check_wire_len(payload.len())?;
        let envelope: InvWantEnvelope = serde_json::from_slice(payload)
            .map_err(|error| validation(format!("invalid inv/want JSON: {error}")))?;
        if envelope.protocol != self.protocol {
            return Err(validation(format!(
                "unsupported inv/want protocol {:?}",
                envelope.protocol
            )));
        }
        if envelope.version != self.version {
            return Err(validation(format!(
                "unsupported inv/want version {}",
                envelope.version
            )));
        }
        Ok(envelope.message)
    }

    fn check_wire_len(&self, len: usize) -> Result<()> {
        if len > self.max_wire_bytes {
            return Err(validation(format!(
                "inv/want wire payload is {len} bytes, maximum is {}",
                self.max_wire_bytes
            )));
        }
        Ok(())
    }
}

/// A connected transport peer plus an optional locally observed quality score.
///
/// `None` deliberately means unknown. It is not equivalent to a poor score and
/// lets selection reserve exploration capacity for new peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshPeer {
    pub id: String,
    pub quality_score: Option<i32>,
}

impl MeshPeer {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            quality_score: None,
        }
    }

    #[must_use]
    pub fn observed(id: impl Into<String>, quality_score: i32) -> Self {
        Self {
            id: id.into(),
            quality_score: Some(quality_score),
        }
    }

    #[must_use]
    pub const fn is_unknown(&self) -> bool {
        self.quality_score.is_none()
    }

    fn effective_score(&self) -> i32 {
        self.quality_score.unwrap_or(0)
    }
}

/// Selects the connected peers eligible for mesh traffic and assigns optional
/// local quality scores. Unknown peers should normally remain eligible so a
/// new node can explore beyond its existing trust graph.
pub trait MeshPeerPolicy: Send + Sync {
    fn select_mesh_peer(&self, peer_id: &str) -> Result<Option<MeshPeer>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvWantMeshOptions {
    pub fanout: usize,
    pub unknown_peer_reserve: usize,
    pub max_hops: u8,
    pub max_event_bytes: usize,
    pub max_cached_events: usize,
    pub max_seen_events: usize,
    pub max_pending_peers_per_event: usize,
    pub route_ttl_ms: u64,
    pub event_ttl_ms: u64,
    /// `None` accepts every signed Nostr kind. `Some` is an explicit allowlist.
    pub allowed_kinds: Option<BTreeSet<u16>>,
}

impl Default for InvWantMeshOptions {
    fn default() -> Self {
        Self {
            fanout: DEFAULT_INV_WANT_FANOUT,
            unknown_peer_reserve: 1,
            max_hops: DEFAULT_INV_WANT_HOP_LIMIT,
            max_event_bytes: DEFAULT_INV_WANT_MAX_EVENT_BYTES,
            max_cached_events: 1_024,
            max_seen_events: 4_096,
            max_pending_peers_per_event: 64,
            route_ttl_ms: DEFAULT_ROUTE_TTL_MS,
            event_ttl_ms: DEFAULT_EVENT_TTL_MS,
            allowed_kinds: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvWantAction {
    Send {
        peer_id: String,
        message: InvWantWireMessage,
    },
    Deliver {
        source_peer: String,
        event: Event,
    },
}

#[derive(Debug, Clone)]
struct CachedEvent {
    event: Event,
    expires_at_ms: u64,
}

#[derive(Debug, Clone)]
struct UpstreamRoute {
    peer_id: String,
    alternate_peers: BTreeSet<String>,
    event_kind: u16,
    payload_bytes: u32,
    hop_limit: u8,
    expires_at_ms: u64,
    fulfilled: bool,
}

#[derive(Debug, Clone)]
struct PendingPeers {
    peers: BTreeSet<String>,
    expires_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerBehavior {
    score: i32,
    samples: u32,
    valid_frames: u32,
    invalid_messages: u32,
    unserved_inventories: u32,
}

/// Locally observed evidence behind a mesh peer's behavior score.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerBehaviorObservation {
    pub score: i32,
    pub samples: u32,
    pub valid_frames: u32,
    pub invalid_messages: u32,
    pub unserved_inventories: u32,
}

#[derive(Debug, Clone, Copy)]
enum PeerBehaviorEvidence {
    ValidFrame,
    InvalidMessage,
    UnservedInventory,
}

struct ReceivedInventory {
    event_id: String,
    event_kind: u16,
    payload_bytes: u32,
    hop_limit: u8,
}

/// Synchronous, transport-neutral production state machine for bounded Nostr
/// inventory/want/frame propagation.
pub struct InvWantMesh {
    options: InvWantMeshOptions,
    cached_events: HashMap<String, CachedEvent>,
    cache_order: VecDeque<String>,
    seen_inventories: HashMap<String, u64>,
    seen_order: VecDeque<String>,
    delivered_events: HashSet<String>,
    delivered_order: VecDeque<String>,
    upstream_routes: HashMap<String, UpstreamRoute>,
    pending_downstream: HashMap<String, PendingPeers>,
    want_forwarded: HashMap<String, u64>,
    next_prune_at_ms: Option<u64>,
    peer_behaviors: HashMap<String, PeerBehavior>,
    peer_behavior_order: VecDeque<String>,
}

impl InvWantMesh {
    #[must_use]
    pub fn new(mut options: InvWantMeshOptions) -> Self {
        options.fanout = options.fanout.max(1);
        options.unknown_peer_reserve = options.unknown_peer_reserve.min(options.fanout);
        options.max_hops = options.max_hops.max(1);
        options.max_event_bytes = options.max_event_bytes.max(1);
        options.max_cached_events = options.max_cached_events.max(1);
        options.max_seen_events = options.max_seen_events.max(1);
        options.max_pending_peers_per_event = options.max_pending_peers_per_event.max(1);
        options.route_ttl_ms = options.route_ttl_ms.max(1);
        options.event_ttl_ms = options.event_ttl_ms.max(options.route_ttl_ms);
        Self {
            options,
            cached_events: HashMap::new(),
            cache_order: VecDeque::new(),
            seen_inventories: HashMap::new(),
            seen_order: VecDeque::new(),
            delivered_events: HashSet::new(),
            delivered_order: VecDeque::new(),
            upstream_routes: HashMap::new(),
            pending_downstream: HashMap::new(),
            want_forwarded: HashMap::new(),
            next_prune_at_ms: None,
            peer_behaviors: HashMap::new(),
            peer_behavior_order: VecDeque::new(),
        }
    }

    #[must_use]
    pub fn options(&self) -> &InvWantMeshOptions {
        &self.options
    }

    /// Return a locally observed pubsub behavior score once enough evidence is
    /// available. A peer with fewer samples remains unknown.
    #[must_use]
    pub fn peer_behavior_score(&self, peer_id: &str) -> Option<i32> {
        self.peer_behavior_observation(peer_id)
            .map(|observation| observation.score)
    }

    /// Return the score and evidence count once the local confidence floor is met.
    #[must_use]
    pub fn peer_behavior_observation(&self, peer_id: &str) -> Option<PeerBehaviorObservation> {
        self.peer_behaviors
            .get(peer_id)
            .filter(|behavior| behavior.samples >= MIN_PEER_BEHAVIOR_SAMPLES)
            .map(|behavior| PeerBehaviorObservation {
                score: behavior.score,
                samples: behavior.samples,
                valid_frames: behavior.valid_frames,
                invalid_messages: behavior.invalid_messages,
                unserved_inventories: behavior.unserved_inventories,
            })
    }

    /// Record a malformed or otherwise invalid wire message rejected by the
    /// transport adapter before it reached [`Self::receive`].
    pub fn record_invalid_message(&mut self, peer_id: &str) {
        self.record_peer_behavior(
            peer_id,
            INVALID_MESSAGE_PENALTY,
            PeerBehaviorEvidence::InvalidMessage,
        );
    }

    /// Run time-based route and cache maintenance without manufacturing
    /// transport traffic. Expired unanswered routes contribute to the same
    /// peer behavior score used by normal receive and publish operations.
    pub fn maintain(&mut self, now_ms: u64) {
        self.prune(now_ms);
    }

    /// Close a requested route when the transport or application rejects an
    /// otherwise served frame under local admission policy. This gives no
    /// provider credit and avoids later treating the peer as if it never
    /// answered the want.
    pub fn dismiss_frame(&mut self, peer_id: &str, event_id: &str) {
        if let Some(route) = self.upstream_routes.get_mut(event_id)
            && (route.peer_id == peer_id || route.alternate_peers.contains(peer_id))
        {
            route.fulfilled = true;
            self.pending_downstream.remove(event_id);
        }
    }

    pub fn publish(
        &mut self,
        event: Event,
        peers: &[MeshPeer],
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        let (event_id, payload_bytes) = self.validate_event(&event)?;
        let event_kind = u16::from(event.kind);
        self.store_event(event, now_ms);
        if !self.remember_inventory(&event_id, now_ms) {
            return Ok(Vec::new());
        }
        let inventory = InvWantWireMessage::Inventory {
            event_id,
            event_kind,
            payload_bytes,
            hop_limit: self.options.max_hops,
        };
        Ok(self.send_to_selected_peers(peers, None, &inventory))
    }

    /// Cache a verified event and advertise it directly to a peer that joined
    /// after the original publication. The normal WANT/FRAME exchange still
    /// proves interest before the event payload is sent.
    pub fn replay_to_peer(
        &mut self,
        event: Event,
        peer_id: &str,
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        let (event_id, payload_bytes) = self.validate_event(&event)?;
        let event_kind = u16::from(event.kind);
        self.store_event(event, now_ms);
        Ok(vec![InvWantAction::Send {
            peer_id: peer_id.to_string(),
            message: InvWantWireMessage::Inventory {
                event_id,
                event_kind,
                payload_bytes,
                hop_limit: self.options.max_hops,
            },
        }])
    }

    pub fn receive(
        &mut self,
        source_peer: &str,
        message: InvWantWireMessage,
        peers: &[MeshPeer],
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        let result = match message {
            InvWantWireMessage::Inventory {
                event_id,
                event_kind,
                payload_bytes,
                hop_limit,
            } => self.receive_inventory(
                source_peer,
                ReceivedInventory {
                    event_id,
                    event_kind,
                    payload_bytes,
                    hop_limit,
                },
                now_ms,
            ),
            InvWantWireMessage::Want { event_id } => {
                self.receive_want(source_peer, event_id, now_ms)
            }
            InvWantWireMessage::Frame { event_id, event } => {
                self.receive_frame(source_peer, &event_id, &event, peers, now_ms)
            }
        };
        if result.is_err() {
            self.record_invalid_message(source_peer);
        }
        result
    }

    fn receive_inventory(
        &mut self,
        source_peer: &str,
        inventory: ReceivedInventory,
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        let ReceivedInventory {
            event_id,
            event_kind,
            payload_bytes,
            hop_limit,
        } = inventory;
        validate_event_id(&event_id)?;
        self.validate_kind(event_kind)?;
        self.validate_event_len(payload_bytes as usize)?;
        if hop_limit == 0 {
            return Ok(Vec::new());
        }
        if hop_limit > self.options.max_hops {
            return Err(validation(format!(
                "inv/want hop limit {hop_limit} exceeds local maximum {}",
                self.options.max_hops
            )));
        }
        if self.cached_events.contains_key(&event_id) {
            return Ok(Vec::new());
        }
        if !self.remember_inventory(&event_id, now_ms) {
            let Some(route) = self.upstream_routes.get_mut(&event_id) else {
                return Ok(Vec::new());
            };
            if route.fulfilled {
                return Ok(Vec::new());
            }
            if route.event_kind != event_kind || route.payload_bytes != payload_bytes {
                return Err(validation(
                    "retried inv/want inventory changed kind or size",
                ));
            }
            route.hop_limit = route.hop_limit.max(hop_limit);
            if route.peer_id != source_peer && !route.alternate_peers.contains(source_peer) {
                if route.alternate_peers.len() + 1 >= MAX_UPSTREAM_PROVIDERS_PER_EVENT {
                    return Ok(Vec::new());
                }
                route.alternate_peers.insert(source_peer.to_string());
                let extended_expiry = now_ms.saturating_add(self.options.route_ttl_ms);
                route.expires_at_ms = extended_expiry;
                self.seen_inventories
                    .insert(event_id.clone(), extended_expiry);
                self.want_forwarded
                    .insert(event_id.clone(), extended_expiry);
            }
            return Ok(vec![InvWantAction::Send {
                peer_id: source_peer.to_string(),
                message: InvWantWireMessage::Want { event_id },
            }]);
        }

        let route_expiry = now_ms.saturating_add(self.options.route_ttl_ms);
        self.upstream_routes
            .entry(event_id.clone())
            .or_insert_with(|| UpstreamRoute {
                peer_id: source_peer.to_string(),
                alternate_peers: BTreeSet::new(),
                event_kind,
                payload_bytes,
                hop_limit,
                expires_at_ms: route_expiry,
                fulfilled: false,
            });
        self.want_forwarded.insert(event_id.clone(), route_expiry);

        Ok(vec![InvWantAction::Send {
            peer_id: source_peer.to_string(),
            message: InvWantWireMessage::Want { event_id },
        }])
    }

    fn receive_want(
        &mut self,
        source_peer: &str,
        event_id: String,
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        validate_event_id(&event_id)?;
        if let Some(cached) = self.cached_events.get(&event_id) {
            return Ok(vec![InvWantAction::Send {
                peer_id: source_peer.to_string(),
                message: InvWantWireMessage::Frame {
                    event_id,
                    event: Box::new(cached.event.clone()),
                },
            }]);
        }

        let Some(route) = self.upstream_routes.get(&event_id).cloned() else {
            return Ok(Vec::new());
        };
        if route.fulfilled {
            return Ok(Vec::new());
        }

        let pending = self
            .pending_downstream
            .entry(event_id.clone())
            .or_insert_with(|| PendingPeers {
                peers: BTreeSet::new(),
                expires_at_ms: now_ms.saturating_add(self.options.route_ttl_ms),
            });
        if pending.peers.len() < self.options.max_pending_peers_per_event {
            pending.peers.insert(source_peer.to_string());
        }

        let already_forwarded = self
            .want_forwarded
            .get(&event_id)
            .is_some_and(|expiry| *expiry > now_ms);
        if already_forwarded {
            return Ok(Vec::new());
        }
        self.want_forwarded
            .insert(event_id.clone(), route.expires_at_ms);
        Ok(vec![InvWantAction::Send {
            peer_id: route.peer_id,
            message: InvWantWireMessage::Want { event_id },
        }])
    }

    fn receive_frame(
        &mut self,
        source_peer: &str,
        event_id: &str,
        event: &Event,
        peers: &[MeshPeer],
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        validate_event_id(event_id)?;
        let route = self
            .upstream_routes
            .get(event_id)
            .cloned()
            .ok_or_else(|| validation("unsolicited inv/want frame"))?;
        if !self.want_forwarded.contains_key(event_id)
            || (route.peer_id != source_peer && !route.alternate_peers.contains(source_peer))
        {
            return Err(validation("inv/want frame was not requested from source"));
        }
        let (verified_id, payload_bytes) = self.validate_event(event)?;
        if verified_id != event_id {
            return Err(validation(
                "inv/want frame id does not match signed event id",
            ));
        }
        if route.event_kind != u16::from(event.kind) || route.payload_bytes != payload_bytes {
            return Err(validation(
                "inv/want frame does not match announced kind or payload size",
            ));
        }
        if route.fulfilled {
            return Ok(Vec::new());
        }
        self.record_peer_behavior(
            source_peer,
            VALID_FRAME_REWARD,
            PeerBehaviorEvidence::ValidFrame,
        );
        self.store_event(event.clone(), now_ms);
        if let Some(route) = self.upstream_routes.get_mut(event_id) {
            route.fulfilled = true;
        }

        let mut actions = Vec::new();
        if self.remember_delivered(event_id) {
            actions.push(InvWantAction::Deliver {
                source_peer: source_peer.to_string(),
                event: event.clone(),
            });
        }
        if let Some(pending) = self.pending_downstream.remove(event_id) {
            actions.extend(
                pending
                    .peers
                    .into_iter()
                    .map(|peer_id| InvWantAction::Send {
                        peer_id,
                        message: InvWantWireMessage::Frame {
                            event_id: event_id.to_string(),
                            event: Box::new(event.clone()),
                        },
                    }),
            );
        }
        if route.hop_limit > 1 {
            actions.extend(self.send_to_selected_peers(
                peers,
                Some(source_peer),
                &InvWantWireMessage::Inventory {
                    event_id: event_id.to_string(),
                    event_kind: u16::from(event.kind),
                    payload_bytes,
                    hop_limit: route.hop_limit - 1,
                },
            ));
        }
        Ok(actions)
    }

    fn validate_event(&self, event: &Event) -> Result<(String, u32)> {
        event
            .verify()
            .map_err(|error| validation(format!("invalid signed Nostr event: {error}")))?;
        self.validate_kind(u16::from(event.kind))?;
        let payload = serde_json::to_vec(event)
            .map_err(|error| validation(format!("invalid inv/want JSON: {error}")))?;
        self.validate_event_len(payload.len())?;
        let payload_bytes = u32::try_from(payload.len()).map_err(|_| {
            validation(format!(
                "inv/want event is {} bytes, maximum is {}",
                payload.len(),
                self.options.max_event_bytes
            ))
        })?;
        Ok((event.id.to_hex(), payload_bytes))
    }

    fn validate_kind(&self, event_kind: u16) -> Result<()> {
        if self
            .options
            .allowed_kinds
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&event_kind))
        {
            return Err(validation(format!(
                "unsupported Nostr event kind {event_kind}"
            )));
        }
        Ok(())
    }

    fn validate_event_len(&self, len: usize) -> Result<()> {
        if len > self.options.max_event_bytes {
            return Err(validation(format!(
                "inv/want event is {len} bytes, maximum is {}",
                self.options.max_event_bytes
            )));
        }
        Ok(())
    }

    fn store_event(&mut self, event: Event, now_ms: u64) {
        let event_id = event.id.to_hex();
        let expires_at_ms = now_ms.saturating_add(self.options.event_ttl_ms);
        if !self.cached_events.contains_key(&event_id) {
            while self.cached_events.len() >= self.options.max_cached_events {
                let Some(oldest) = self.cache_order.pop_front() else {
                    break;
                };
                self.cached_events.remove(&oldest);
            }
            self.cache_order.push_back(event_id.clone());
        }
        self.cached_events.insert(
            event_id,
            CachedEvent {
                event,
                expires_at_ms,
            },
        );
        self.track_expiry(expires_at_ms);
    }

    fn remember_inventory(&mut self, event_id: &str, now_ms: u64) -> bool {
        if self
            .seen_inventories
            .get(event_id)
            .is_some_and(|expiry| *expiry > now_ms)
        {
            return false;
        }
        if !self.seen_inventories.contains_key(event_id) {
            while self.seen_inventories.len() >= self.options.max_seen_events {
                let Some(oldest) = self.seen_order.pop_front() else {
                    break;
                };
                self.seen_inventories.remove(&oldest);
                self.upstream_routes.remove(&oldest);
                self.pending_downstream.remove(&oldest);
                self.want_forwarded.remove(&oldest);
            }
            self.seen_order.push_back(event_id.to_string());
        }
        let expires_at_ms = now_ms.saturating_add(self.options.route_ttl_ms);
        self.seen_inventories
            .insert(event_id.to_string(), expires_at_ms);
        self.track_expiry(expires_at_ms);
        true
    }

    fn remember_delivered(&mut self, event_id: &str) -> bool {
        if !self.delivered_events.insert(event_id.to_string()) {
            return false;
        }
        self.delivered_order.push_back(event_id.to_string());
        while self.delivered_events.len() > self.options.max_seen_events {
            let Some(oldest) = self.delivered_order.pop_front() else {
                break;
            };
            self.delivered_events.remove(&oldest);
        }
        true
    }

    fn send_to_selected_peers(
        &self,
        peers: &[MeshPeer],
        excluded_peer: Option<&str>,
        message: &InvWantWireMessage,
    ) -> Vec<InvWantAction> {
        let peers = self.peers_with_behavior(peers);
        select_peers(
            &peers,
            excluded_peer,
            self.options.fanout,
            self.options.unknown_peer_reserve,
        )
        .into_iter()
        .map(|peer| InvWantAction::Send {
            peer_id: peer.id,
            message: message.clone(),
        })
        .collect()
    }

    fn peers_with_behavior(&self, peers: &[MeshPeer]) -> Vec<MeshPeer> {
        peers
            .iter()
            .map(|peer| {
                let local_score = self.peer_behavior_score(&peer.id);
                let quality_score = match (peer.quality_score, local_score) {
                    (Some(external), Some(local)) => {
                        Some(external.saturating_add(local).clamp(-100, 100))
                    }
                    (external, local) => external.or(local),
                };
                match quality_score {
                    Some(score) => MeshPeer::observed(&peer.id, score),
                    None => MeshPeer::new(&peer.id),
                }
            })
            .collect()
    }

    fn record_peer_behavior(
        &mut self,
        peer_id: &str,
        score_delta: i32,
        evidence: PeerBehaviorEvidence,
    ) {
        if !self.peer_behaviors.contains_key(peer_id) {
            while self.peer_behaviors.len() >= MAX_TRACKED_PEER_BEHAVIORS {
                let Some(oldest) = self.peer_behavior_order.pop_front() else {
                    break;
                };
                self.peer_behaviors.remove(&oldest);
            }
            self.peer_behavior_order.push_back(peer_id.to_string());
        }
        let behavior = self.peer_behaviors.entry(peer_id.to_string()).or_default();
        behavior.samples = behavior.samples.saturating_add(1);
        behavior.score = behavior.score.saturating_add(score_delta).clamp(-100, 100);
        match evidence {
            PeerBehaviorEvidence::ValidFrame => {
                behavior.valid_frames = behavior.valid_frames.saturating_add(1);
            }
            PeerBehaviorEvidence::InvalidMessage => {
                behavior.invalid_messages = behavior.invalid_messages.saturating_add(1);
            }
            PeerBehaviorEvidence::UnservedInventory => {
                behavior.unserved_inventories = behavior.unserved_inventories.saturating_add(1);
            }
        }
    }

    fn prune(&mut self, now_ms: u64) {
        let Some(next_prune_at_ms) = self.next_prune_at_ms else {
            return;
        };
        if now_ms < next_prune_at_ms {
            return;
        }
        let expired_routes = self
            .upstream_routes
            .values()
            .filter(|route| !route.fulfilled && route.expires_at_ms <= now_ms)
            .flat_map(|route| {
                std::iter::once(route.peer_id.clone()).chain(route.alternate_peers.iter().cloned())
            })
            .collect::<Vec<_>>();
        for peer_id in expired_routes {
            self.record_peer_behavior(
                &peer_id,
                UNSERVED_INVENTORY_PENALTY,
                PeerBehaviorEvidence::UnservedInventory,
            );
        }
        self.cached_events
            .retain(|_, cached| cached.expires_at_ms > now_ms);
        self.cache_order
            .retain(|event_id| self.cached_events.contains_key(event_id));
        self.seen_inventories
            .retain(|_, expires_at_ms| *expires_at_ms > now_ms);
        self.seen_order
            .retain(|event_id| self.seen_inventories.contains_key(event_id));
        self.upstream_routes
            .retain(|_, route| route.expires_at_ms > now_ms);
        let upstream_routes = &self.upstream_routes;
        self.pending_downstream.retain(|event_id, pending| {
            pending.expires_at_ms > now_ms && upstream_routes.contains_key(event_id)
        });
        self.want_forwarded.retain(|event_id, expires_at_ms| {
            *expires_at_ms > now_ms && upstream_routes.contains_key(event_id)
        });
        self.next_prune_at_ms = self
            .cached_events
            .values()
            .map(|cached| cached.expires_at_ms)
            .chain(self.seen_inventories.values().copied())
            .chain(
                self.upstream_routes
                    .values()
                    .map(|route| route.expires_at_ms),
            )
            .min();
    }

    fn track_expiry(&mut self, expires_at_ms: u64) {
        self.next_prune_at_ms = Some(
            self.next_prune_at_ms
                .map_or(expires_at_ms, |scheduled| scheduled.min(expires_at_ms)),
        );
    }
}

fn select_peers(
    peers: &[MeshPeer],
    excluded_peer: Option<&str>,
    fanout: usize,
    unknown_peer_reserve: usize,
) -> Vec<MeshPeer> {
    let mut deduplicated = BTreeMap::<String, MeshPeer>::new();
    for peer in peers
        .iter()
        .filter(|peer| excluded_peer != Some(peer.id.as_str()))
    {
        deduplicated
            .entry(peer.id.clone())
            .and_modify(|existing| {
                if existing.quality_score.is_none() && peer.quality_score.is_some() {
                    *existing = peer.clone();
                }
            })
            .or_insert_with(|| peer.clone());
    }

    let mut candidates = deduplicated.into_values().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .effective_score()
            .cmp(&left.effective_score())
            .then_with(|| left.is_unknown().cmp(&right.is_unknown()))
            .then_with(|| left.id.cmp(&right.id))
    });

    let target = fanout.min(candidates.len());
    let required_unknown = unknown_peer_reserve
        .min(target)
        .min(candidates.iter().filter(|peer| peer.is_unknown()).count());
    let mut selected = candidates.iter().take(target).cloned().collect::<Vec<_>>();
    let selected_ids = selected
        .iter()
        .map(|peer| peer.id.clone())
        .collect::<BTreeSet<_>>();
    let mut replacement_unknowns = candidates
        .iter()
        .filter(|peer| peer.is_unknown() && !selected_ids.contains(&peer.id))
        .cloned();

    while selected.iter().filter(|peer| peer.is_unknown()).count() < required_unknown {
        let Some(replacement) = replacement_unknowns.next() else {
            break;
        };
        let Some(replace_index) = selected.iter().rposition(|peer| !peer.is_unknown()) else {
            break;
        };
        selected[replace_index] = replacement;
    }
    selected
}

fn validate_event_id(event_id: &str) -> Result<()> {
    if event_id.len() == 64
        && event_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Ok(());
    }
    Err(validation(format!("invalid inv/want event id {event_id}")))
}

fn validation(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(message.into())
}
