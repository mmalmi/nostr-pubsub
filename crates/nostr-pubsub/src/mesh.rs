use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use nostr::Event;
use serde::{Deserialize, Serialize};

use crate::{DEFAULT_INV_WANT_HOP_LIMIT, PubsubError, Result};

pub const DEFAULT_INV_WANT_FANOUT: usize = 8;
pub const DEFAULT_INV_WANT_MAX_EVENT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_INV_WANT_MAX_WIRE_BYTES: usize = DEFAULT_INV_WANT_MAX_EVENT_BYTES + 4096;

const DEFAULT_ROUTE_TTL_MS: u64 = 2 * 60 * 1_000;
const DEFAULT_EVENT_TTL_MS: u64 = 10 * 60 * 1_000;

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
    expires_at_ms: u64,
}

#[derive(Debug, Clone)]
struct PendingPeers {
    peers: BTreeSet<String>,
    expires_at_ms: u64,
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
        }
    }

    #[must_use]
    pub fn options(&self) -> &InvWantMeshOptions {
        &self.options
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

    pub fn receive(
        &mut self,
        source_peer: &str,
        message: InvWantWireMessage,
        peers: &[MeshPeer],
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        match message {
            InvWantWireMessage::Inventory {
                event_id,
                event_kind,
                payload_bytes,
                hop_limit,
            } => self.receive_inventory(
                source_peer,
                peers,
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
                self.receive_frame(source_peer, &event_id, &event, now_ms)
            }
        }
    }

    fn receive_inventory(
        &mut self,
        source_peer: &str,
        peers: &[MeshPeer],
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
        if hop_limit == 0 || !self.remember_inventory(&event_id, now_ms) {
            return Ok(Vec::new());
        }

        let route_expiry = now_ms.saturating_add(self.options.route_ttl_ms);
        self.upstream_routes
            .entry(event_id.clone())
            .or_insert_with(|| UpstreamRoute {
                peer_id: source_peer.to_string(),
                expires_at_ms: route_expiry,
            });
        self.want_forwarded.insert(event_id.clone(), route_expiry);

        let mut actions = vec![InvWantAction::Send {
            peer_id: source_peer.to_string(),
            message: InvWantWireMessage::Want {
                event_id: event_id.clone(),
            },
        }];
        if hop_limit > 1 {
            actions.extend(self.send_to_selected_peers(
                peers,
                Some(source_peer),
                &InvWantWireMessage::Inventory {
                    event_id,
                    event_kind,
                    payload_bytes,
                    hop_limit: hop_limit - 1,
                },
            ));
        }
        Ok(actions)
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

        let Some(route) = self.upstream_routes.get(&event_id) else {
            return Ok(Vec::new());
        };
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
            peer_id: route.peer_id.clone(),
            message: InvWantWireMessage::Want { event_id },
        }])
    }

    fn receive_frame(
        &mut self,
        source_peer: &str,
        event_id: &str,
        event: &Event,
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        validate_event_id(event_id)?;
        let (verified_id, _) = self.validate_event(event)?;
        if verified_id != event_id {
            return Err(validation(
                "inv/want frame id does not match signed event id",
            ));
        }
        self.store_event(event.clone(), now_ms);

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
                expires_at_ms: now_ms.saturating_add(self.options.event_ttl_ms),
            },
        );
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
            }
            self.seen_order.push_back(event_id.to_string());
        }
        self.seen_inventories.insert(
            event_id.to_string(),
            now_ms.saturating_add(self.options.route_ttl_ms),
        );
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
        select_peers(
            peers,
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

    fn prune(&mut self, now_ms: u64) {
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
        self.pending_downstream
            .retain(|_, pending| pending.expires_at_ms > now_ms);
        self.want_forwarded
            .retain(|_, expires_at_ms| *expires_at_ms > now_ms);
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
    if event_id.len() == 64 && event_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(());
    }
    Err(validation(format!("invalid inv/want event id {event_id}")))
}

fn validation(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(message.into())
}
