use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use fips_core::discovery::local::LocalInstanceCapability;
use fips_core::{FipsEndpoint, PeerIdentity};
use fips_tcp::{Config as TcpConfig, ConnectionId, State};
use fips_tcp_endpoint::FipsTcpEndpoint;
use nostr_pubsub::{PubsubError, Result};

use crate::{FIPS_NOSTR_PUBSUB_CAPABILITY, FIPS_NOSTR_PUBSUB_SERVICE_PORT};

const IO_CHUNK_BYTES: usize = 16 * 1024;
const MAX_READY_INPUT_TURNS: usize = 16;

pub(crate) struct WireTcpOptions {
    pub frame_capacity: usize,
    pub peer_capacity: usize,
    pub queue_records_per_peer: usize,
    pub queue_bytes_per_peer: usize,
    pub drive_io_bytes: usize,
    pub drive_frames: usize,
}

pub(crate) struct WireTcpReport {
    pub frames: Vec<(PeerIdentity, Vec<u8>)>,
    pub newly_connected: Vec<PeerIdentity>,
    pub connected_peers: usize,
    pub tcp_datagrams: usize,
    pub rejected_tcp_datagrams: usize,
}

pub(crate) struct WireTcpDriver {
    local_npub: String,
    tcp: FipsTcpEndpoint,
    options: WireTcpOptions,
    connections: HashMap<ConnectionId, TrackedConnection>,
    active: BTreeMap<String, ConnectionId>,
    queues: BTreeMap<String, PeerQueue>,
    inputs: HashMap<String, RecordDecoder>,
}

impl WireTcpDriver {
    pub async fn bind(
        endpoint: Arc<FipsEndpoint>,
        options: WireTcpOptions,
        isn_seed: u64,
    ) -> Result<Self> {
        let max_connections = options
            .peer_capacity
            .checked_mul(2)
            .ok_or_else(|| storage("TCP connection limit overflows"))?;
        let tcp_config = TcpConfig {
            receive_buffer: u16::MAX as usize,
            send_buffer: options.queue_bytes_per_peer,
            max_connections,
            max_connections_per_peer: 2,
            ..TcpConfig::default()
        };
        let capability = LocalInstanceCapability::service(
            FIPS_NOSTR_PUBSUB_CAPABILITY,
            FIPS_NOSTR_PUBSUB_SERVICE_PORT,
        );
        let local_npub = endpoint.npub().to_string();
        let tcp = FipsTcpEndpoint::bind_with_capability(endpoint, capability, tcp_config, isn_seed)
            .await
            .map_err(|error| storage_error("bind TCP/FIPS Nostr pubsub service", error))?;
        Ok(Self {
            local_npub,
            tcp,
            options,
            connections: HashMap::new(),
            active: BTreeMap::new(),
            queues: BTreeMap::new(),
            inputs: HashMap::new(),
        })
    }

    pub async fn connect_peer(&mut self, peer: PeerIdentity, now_ms: u64) -> Result<()> {
        let peer_npub = peer.npub();
        if self.connections.iter().any(|(id, connection)| {
            connection.peer == peer_npub
                && matches!(
                    self.tcp.state(*id),
                    Some(
                        State::SynSent | State::SynReceived | State::Established | State::CloseWait
                    )
                )
        }) {
            return Ok(());
        }
        self.ensure_peer_capacity(&peer_npub)?;
        let id = self
            .tcp
            .connect(peer, now_ms)
            .await
            .map_err(|error| storage_error("connect TCP/FIPS Nostr pubsub peer", error))?;
        self.connections.insert(
            id,
            TrackedConnection {
                peer: peer_npub,
                direction: Direction::Outbound,
            },
        );
        Ok(())
    }

    pub fn queue_frame(&mut self, peer: PeerIdentity, frame: &[u8]) -> Result<()> {
        if frame.len() > self.options.frame_capacity {
            return Err(storage(format!(
                "Nostr pubsub frame is {} bytes, maximum is {}",
                frame.len(),
                self.options.frame_capacity
            )));
        }
        let record = encode_record(frame)?;
        let peer_npub = peer.npub();
        let is_new = !self.queues.contains_key(&peer_npub);
        if is_new && self.queues.len() >= self.options.peer_capacity {
            return Err(storage("TCP/FIPS Nostr pubsub queue peer limit reached"));
        }
        let queue = self.queues.entry(peer_npub.clone()).or_default();
        if queue.records.len() >= self.options.queue_records_per_peer {
            return Err(storage(format!(
                "TCP/FIPS Nostr pubsub record queue is full for {peer_npub}"
            )));
        }
        if queue.bytes.saturating_add(record.len()) > self.options.queue_bytes_per_peer {
            return Err(storage(format!(
                "TCP/FIPS Nostr pubsub byte queue is full for {peer_npub}"
            )));
        }
        queue.bytes = queue.bytes.saturating_add(record.len());
        queue.records.push_back(QueuedRecord {
            bytes: record,
            offset: 0,
        });
        Ok(())
    }

    pub async fn receive(&mut self, now_ms: u64) -> Result<WireTcpReport> {
        let received = self
            .tcp
            .receive_report(now_ms)
            .await
            .map_err(|error| storage_error("receive TCP/FIPS Nostr pubsub batch", error))?;
        let mut report = self.drive_ready(now_ms).await?;
        report.tcp_datagrams = received.datagrams;
        report.rejected_tcp_datagrams = received.rejected();
        Ok(report)
    }

    pub async fn poll(&mut self, now_ms: u64) -> Result<WireTcpReport> {
        self.tcp
            .poll(now_ms)
            .await
            .map_err(|error| storage_error("poll TCP/FIPS Nostr pubsub transport", error))?;
        self.drive_ready(now_ms).await
    }

    pub async fn abort_peer(&mut self, peer: PeerIdentity) -> Result<()> {
        let peer_npub = peer.npub();
        let ids = self
            .connections
            .iter()
            .filter_map(|(id, connection)| (connection.peer == peer_npub).then_some(*id))
            .collect::<Vec<_>>();
        for id in ids {
            if self.tcp.state(id).is_some() {
                self.tcp
                    .abort(id)
                    .await
                    .map_err(|error| storage_error("abort TCP/FIPS Nostr pubsub peer", error))?;
            }
            self.connections.remove(&id);
        }
        self.active.remove(&peer_npub);
        self.inputs.remove(&peer_npub);
        if let Some(queue) = self.queues.get_mut(&peer_npub) {
            queue.restart();
        }
        Ok(())
    }

    pub(crate) fn connection_count(&self) -> usize {
        self.connections.len()
    }

    async fn drive_ready(&mut self, now_ms: u64) -> Result<WireTcpReport> {
        self.accept_connections().await?;
        let newly_connected = self.refresh_active().await?;
        let frames = self.read_active(now_ms).await?;
        self.flush_queues(now_ms).await?;
        self.finish_remote_closes(now_ms).await?;
        let more_connected = self.refresh_active().await?;
        let mut newly_connected = newly_connected;
        newly_connected.extend(more_connected);
        newly_connected.sort_unstable_by_key(PeerIdentity::npub);
        newly_connected.dedup_by_key(|peer| peer.npub());
        Ok(WireTcpReport {
            frames,
            newly_connected,
            connected_peers: self.active.len(),
            tcp_datagrams: 0,
            rejected_tcp_datagrams: 0,
        })
    }

    async fn accept_connections(&mut self) -> Result<()> {
        while let Some(id) = self.tcp.accept() {
            let peer = self
                .tcp
                .peer(id)
                .ok_or_else(|| storage("accepted TCP/FIPS stream has no authenticated peer"))?
                .npub();
            if self.ensure_peer_capacity(&peer).is_err() {
                self.tcp
                    .abort(id)
                    .await
                    .map_err(|error| storage_error("reject excess TCP/FIPS peer", error))?;
                continue;
            }
            self.connections.entry(id).or_insert(TrackedConnection {
                peer,
                direction: Direction::Inbound,
            });
        }
        Ok(())
    }

    async fn refresh_active(&mut self) -> Result<Vec<PeerIdentity>> {
        self.connections
            .retain(|id, _| self.tcp.state(*id).is_some());
        let mut candidates = BTreeMap::<String, Vec<(ConnectionId, Direction)>>::new();
        for (id, connection) in &self.connections {
            if matches!(
                self.tcp.state(*id),
                Some(State::Established | State::CloseWait)
            ) {
                candidates
                    .entry(connection.peer.clone())
                    .or_default()
                    .push((*id, connection.direction));
            }
        }
        let mut next_active = BTreeMap::new();
        let mut extras = Vec::new();
        for (peer, mut streams) in candidates {
            let prefer_outbound = self.local_npub < peer;
            streams.sort_by_key(|(id, direction)| {
                let preferred = matches!(direction, Direction::Outbound) == prefer_outbound;
                (!preferred, id.get())
            });
            let (selected, _) = streams.remove(0);
            next_active.insert(peer, selected);
            extras.extend(streams.into_iter().map(|(id, _)| id));
        }
        for id in extras {
            self.tcp
                .abort(id)
                .await
                .map_err(|error| storage_error("deduplicate TCP/FIPS pubsub stream", error))?;
            self.connections.remove(&id);
        }
        let changed = self
            .active
            .keys()
            .chain(next_active.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|peer| self.active.get(peer) != next_active.get(peer))
            .collect::<Vec<_>>();
        let newly_connected = changed
            .iter()
            .filter(|peer| next_active.contains_key(*peer))
            .filter_map(|peer| PeerIdentity::from_npub(peer).ok())
            .collect::<Vec<_>>();
        self.active = next_active;
        for peer in changed {
            self.inputs.remove(&peer);
            if let Some(queue) = self.queues.get_mut(&peer) {
                queue.restart();
            }
        }
        Ok(newly_connected)
    }

    async fn read_active(&mut self, now_ms: u64) -> Result<Vec<(PeerIdentity, Vec<u8>)>> {
        let streams = self
            .active
            .iter()
            .map(|(peer, id)| (peer.clone(), *id))
            .collect::<Vec<_>>();
        let mut budget = self.options.drive_io_bytes;
        let mut frames = Vec::new();
        for (peer, id) in streams {
            let mut turns = 0;
            while turns < MAX_READY_INPUT_TURNS && frames.len() < self.options.drive_frames {
                if self
                    .inputs
                    .get(&peer)
                    .is_some_and(RecordDecoder::has_complete_record)
                {
                    let decoded = self
                        .inputs
                        .get_mut(&peer)
                        .expect("decoder exists")
                        .take(self.options.drive_frames - frames.len())?;
                    let identity = PeerIdentity::from_npub(&peer)
                        .map_err(|error| storage_error("decode authenticated peer", error))?;
                    frames.extend(decoded.into_iter().map(|frame| (identity, frame)));
                    turns += 1;
                    continue;
                }
                if budget == 0 {
                    break;
                }
                let decoder = self
                    .inputs
                    .entry(peer.clone())
                    .or_insert_with(|| RecordDecoder::new(self.options.frame_capacity));
                let read_max = decoder.remaining_capacity().min(IO_CHUNK_BYTES).min(budget);
                if read_max == 0 {
                    break;
                }
                let bytes = self
                    .tcp
                    .read(id, read_max, now_ms)
                    .await
                    .map_err(|error| storage_error("read TCP/FIPS pubsub stream", error))?;
                if bytes.is_empty() {
                    break;
                }
                budget -= bytes.len();
                decoder.push(&bytes)?;
                turns += 1;
            }
        }
        Ok(frames)
    }

    async fn flush_queues(&mut self, now_ms: u64) -> Result<()> {
        let streams = self
            .active
            .iter()
            .map(|(peer, id)| (peer.clone(), *id))
            .collect::<Vec<_>>();
        let mut budget = self.options.drive_io_bytes;
        for (peer, id) in streams {
            while budget > 0 {
                let chunk = self
                    .queues
                    .get(&peer)
                    .and_then(|queue| queue.records.front())
                    .map(|record| {
                        let end = record
                            .offset
                            .saturating_add(IO_CHUNK_BYTES.min(budget))
                            .min(record.bytes.len());
                        record.bytes[record.offset..end].to_vec()
                    });
                let Some(chunk) = chunk else {
                    break;
                };
                let accepted = self
                    .tcp
                    .write(id, &chunk, now_ms)
                    .await
                    .map_err(|error| storage_error("write TCP/FIPS pubsub stream", error))?;
                if accepted == 0 {
                    break;
                }
                budget -= accepted;
                let queue = self.queues.get_mut(&peer).expect("queue exists");
                let record = queue.records.front_mut().expect("record exists");
                record.offset += accepted;
                queue.bytes = queue.bytes.saturating_sub(accepted);
                if record.offset == record.bytes.len() {
                    queue.records.pop_front();
                }
            }
            if self
                .queues
                .get(&peer)
                .is_some_and(|queue| queue.records.is_empty())
            {
                self.queues.remove(&peer);
            }
        }
        Ok(())
    }

    async fn finish_remote_closes(&mut self, now_ms: u64) -> Result<()> {
        let ids = self
            .active
            .iter()
            .filter_map(|(peer, id)| {
                (self.tcp.is_read_closed(*id) && !self.queues.contains_key(peer)).then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in ids {
            self.tcp
                .close(id, now_ms)
                .await
                .map_err(|error| storage_error("close TCP/FIPS pubsub stream", error))?;
        }
        Ok(())
    }

    fn ensure_peer_capacity(&self, peer: &str) -> Result<()> {
        let peers = self
            .connections
            .values()
            .map(|connection| connection.peer.as_str())
            .collect::<BTreeSet<_>>();
        if !peers.contains(peer) && peers.len() >= self.options.peer_capacity {
            return Err(storage(format!(
                "TCP/FIPS pubsub peer limit is {}",
                self.options.peer_capacity
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Inbound,
    Outbound,
}

struct TrackedConnection {
    peer: String,
    direction: Direction,
}

#[derive(Default)]
struct PeerQueue {
    records: VecDeque<QueuedRecord>,
    bytes: usize,
}

impl PeerQueue {
    fn restart(&mut self) {
        for record in &mut self.records {
            record.offset = 0;
        }
        self.bytes = self.records.iter().map(|record| record.bytes.len()).sum();
    }
}

struct QueuedRecord {
    bytes: Vec<u8>,
    offset: usize,
}

struct RecordDecoder {
    max_frame_bytes: usize,
    buffer: Vec<u8>,
}

impl RecordDecoder {
    fn new(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes,
            buffer: Vec::new(),
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<()> {
        if self.buffer.len().saturating_add(bytes.len()) > self.max_frame_bytes + 4 {
            self.buffer.clear();
            return Err(storage("TCP/FIPS pubsub input exceeds frame bound"));
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn take(&mut self, max_frames: usize) -> Result<Vec<Vec<u8>>> {
        let mut frames = Vec::new();
        let mut consumed = 0;
        while frames.len() < max_frames && self.buffer.len().saturating_sub(consumed) >= 4 {
            let declared = u32::from_be_bytes(
                self.buffer[consumed..consumed + 4]
                    .try_into()
                    .expect("record prefix is complete"),
            ) as usize;
            if declared > self.max_frame_bytes {
                self.buffer.clear();
                return Err(storage("TCP/FIPS pubsub frame exceeds configured bound"));
            }
            let record_bytes = 4 + declared;
            if self.buffer.len() - consumed < record_bytes {
                break;
            }
            frames.push(self.buffer[consumed + 4..consumed + record_bytes].to_vec());
            consumed += record_bytes;
        }
        if consumed == self.buffer.len() {
            self.buffer.clear();
        } else if consumed > 0 {
            self.buffer.drain(..consumed);
        }
        Ok(frames)
    }

    fn has_complete_record(&self) -> bool {
        if self.buffer.len() < 4 {
            return false;
        }
        let declared = u32::from_be_bytes(
            self.buffer[..4]
                .try_into()
                .expect("record prefix is complete"),
        ) as usize;
        declared <= self.max_frame_bytes && self.buffer.len() >= 4 + declared
    }

    fn remaining_capacity(&self) -> usize {
        self.max_frame_bytes
            .saturating_add(4)
            .saturating_sub(self.buffer.len())
    }
}

fn encode_record(frame: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(frame.len()).map_err(|_| storage("pubsub frame is too large"))?;
    let mut record = Vec::with_capacity(frame.len() + 4);
    record.extend_from_slice(&length.to_be_bytes());
    record.extend_from_slice(frame);
    Ok(record)
}

fn storage(message: impl Into<String>) -> PubsubError {
    PubsubError::Storage(message.into())
}

fn storage_error(context: &str, error: impl std::fmt::Display) -> PubsubError {
    storage(format!("{context}: {error}"))
}
