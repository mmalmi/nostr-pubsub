use super::{BTreeMap, BTreeSet, Duration, Instant, PeerIdentity, WireTcpOptions};
use fips_tcp::wire::{FIPS_VERSION, Flags, Segment};

// Silent or abandoned public clients release their bounded slot. Expiry
// aborts every TCP tuple for that identity before the slot can be reused.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct InboundPeers(BTreeMap<String, Instant>);

impl InboundPeers {
    pub fn contains(&self, peer: &str) -> bool {
        self.0.contains_key(peer)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn remove(&mut self, peer: &str) {
        self.0.remove(peer);
    }

    pub fn admit(
        &mut self,
        peer: &PeerIdentity,
        bytes: &[u8],
        selected: &BTreeMap<String, Option<Instant>>,
        options: &WireTcpOptions,
        now: Instant,
    ) -> bool {
        let identity = peer.npub();
        if selected.contains_key(&identity) {
            return true;
        }
        if let Some(last_seen) = self.0.get_mut(&identity) {
            *last_seen = now;
            return true;
        }
        if self.0.len() >= options.inbound_peer_capacity
            || self.0.len() + selected.len() >= options.peer_capacity
            || !initial_syn(bytes)
        {
            return false;
        }
        self.0.insert(identity, now);
        true
    }

    pub fn retire(&mut self, selected: &BTreeSet<String>, now: Instant) -> Vec<PeerIdentity> {
        // Promotion restarts the stream: selected peers then receive local
        // subscriptions, which public read-only clients never receive.
        let retired = self
            .0
            .iter()
            .filter(|(peer, last)| {
                selected.contains(*peer) || now.duration_since(**last) >= IDLE_TIMEOUT
            })
            .map(|(peer, _)| peer.clone())
            .collect::<Vec<_>>();
        retired
            .into_iter()
            .filter_map(|peer| {
                self.0.remove(&peer);
                PeerIdentity::from_npub(&peer).ok()
            })
            .collect()
    }
}

fn initial_syn(bytes: &[u8]) -> bool {
    // Header-only SYN, bounded before decoding (which owns a payload Vec).
    if !(20..=60).contains(&bytes.len()) {
        return false;
    }
    Segment::decode(bytes).is_ok_and(|segment| {
        segment.dst_port == crate::FIPS_NOSTR_PUBSUB_SERVICE_PORT
            && segment.flags == Flags::SYN
            && segment.ack.is_none()
            && segment.payload.is_empty()
            && segment.supports_fips_version(FIPS_VERSION)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fips_tcp::wire::TcpOption;

    fn peer(byte: u8) -> PeerIdentity {
        PeerIdentity::from_npub(
            &fips_core::Identity::from_secret_bytes(&[byte; 32])
                .unwrap()
                .npub(),
        )
        .unwrap()
    }
    fn syn() -> Segment {
        let mut segment = Segment::new(50_001, crate::FIPS_NOSTR_PUBSUB_SERVICE_PORT, 7);
        segment.flags = Flags::SYN;
        segment.options.push(TcpOption::FipsVersion {
            version: FIPS_VERSION,
            reserved: 0,
        });
        segment
    }
    fn options() -> WireTcpOptions {
        WireTcpOptions {
            frame_capacity: 1024,
            peer_capacity: 2,
            inbound_peer_capacity: 1,
            queue_records_per_peer: 4,
            queue_bytes_per_peer: 4096,
            drive_io_bytes: 4096,
            drive_frames: 4,
        }
    }
    #[test]
    fn only_initial_service_syn_can_reserve_a_slot() {
        let mut invalid = vec![vec![], vec![0; 61]];
        for flags in [
            Flags::ACK,
            Flags::RST,
            Flags::SYN | Flags::ACK,
            Flags::SYN | Flags::FIN,
        ] {
            let mut segment = syn();
            segment.flags = flags;
            if flags.contains(Flags::ACK) {
                segment.ack = Some(1);
            }
            if let Ok(bytes) = segment.encode() {
                invalid.push(bytes);
            }
        }
        let mut wrong_port = syn();
        wrong_port.dst_port += 1;
        invalid.push(wrong_port.encode().unwrap());
        let mut payload = syn();
        payload.payload.push(1);
        invalid.push(payload.encode().unwrap());
        let mut version = syn();
        version.options.clear();
        invalid.push(version.encode().unwrap());
        let mut peers = InboundPeers::default();
        let now = Instant::now();
        for bytes in invalid {
            assert!(!peers.admit(&peer(101), &bytes, &BTreeMap::new(), &options(), now));
        }
        assert_eq!(peers.len(), 0);
        assert!(peers.admit(
            &peer(101),
            &syn().encode().unwrap(),
            &BTreeMap::new(),
            &options(),
            now
        ));
    }
    #[test]
    fn bounds_defaults_selected_priority_and_idle_expiry() {
        let (a, b, c) = (peer(101), peer(102), peer(103));
        let now = Instant::now();
        let bytes = syn().encode().unwrap();
        let mut peers = InboundPeers::default();
        let mut opts = options();
        opts.inbound_peer_capacity = 0;
        assert!(!peers.admit(&a, &bytes, &BTreeMap::new(), &opts, now));
        opts.inbound_peer_capacity = 1;
        let selected = BTreeMap::from([(b.npub(), None)]);
        assert!(peers.admit(&a, &bytes, &selected, &opts, now));
        assert!(!peers.admit(&c, &bytes, &selected, &opts, now));
        assert!(peers.admit(&b, &[], &selected, &opts, now));
        assert_eq!(
            peers
                .retire(&BTreeSet::from([b.npub()]), now + IDLE_TIMEOUT)
                .len(),
            1
        );
        assert!(peers.admit(&c, &bytes, &selected, &opts, now + IDLE_TIMEOUT));
        assert_eq!(
            peers
                .retire(&BTreeSet::from([c.npub(), b.npub()]), now + IDLE_TIMEOUT)
                .len(),
            1
        );
        assert_eq!(peers.len(), 0);
    }
}
