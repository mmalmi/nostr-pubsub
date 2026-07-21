use std::sync::Weak;

use super::{
    ClientInner, ConnectedPeerLink, HashMap, HashSet, Ordering, PeerIdentity, TCP_POLL_INTERVAL,
    TransportCommand, WireTcpDriver, mpsc, now_ms,
};
use crate::wire_tcp::WireTcpReport;

pub(super) async fn transport_loop(
    inner: Weak<ClientInner>,
    mut driver: WireTcpDriver,
    mut commands: mpsc::Receiver<TransportCommand>,
) {
    let mut poll_tick = tokio::time::interval(TCP_POLL_INTERVAL);
    poll_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut known_links = HashMap::new();
    loop {
        let Some(inner) = inner.upgrade() else {
            break;
        };
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                match command {
                    TransportCommand::Send { peer, frame } => {
                        if inner.peer_is_in_cooldown(&peer.npub(), now_ms()) {
                            continue;
                        }
                        let _ = driver.queue_frame(peer, &frame);
                        let _ = driver.connect_peer(peer, now_ms()).await;
                    }
                    TransportCommand::Cooldown { peer } => {
                        known_links.remove(&peer.npub());
                        let _ = driver.abort_peer(peer).await;
                    }
                }
            }
            report = driver.receive(now_ms()) => {
                if let Ok(report) = report {
                    inner.tcp_receive_batches.fetch_add(1, Ordering::Relaxed);
                    process_wire_report(&inner, &mut driver, report).await;
                }
            }
            _ = poll_tick.tick() => {
                inner.tcp_poll_turns.fetch_add(1, Ordering::Relaxed);
                sync_transport_peers(&inner, &mut driver, &mut known_links).await;
                if let Ok(report) = driver.poll(now_ms()).await {
                    process_wire_report(&inner, &mut driver, report).await;
                }
            }
        }
    }
}

async fn sync_transport_peers(
    inner: &ClientInner,
    driver: &mut WireTcpDriver,
    known_links: &mut HashMap<String, u64>,
) {
    let Ok(peers) = inner.connected_peer_links().await else {
        return;
    };
    let next_links = peers
        .iter()
        .filter(|peer| !inner.peer_is_in_cooldown(&peer.npub, now_ms()))
        .map(|peer| (peer.npub.clone(), peer.link_id))
        .collect::<HashMap<_, _>>();
    let changed = known_links
        .iter()
        .filter(|(npub, link_id)| next_links.get(*npub) != Some(*link_id))
        .filter_map(|(npub, _)| PeerIdentity::from_npub(npub).ok())
        .collect::<Vec<_>>();
    for peer in changed {
        let _ = driver.abort_peer(peer).await;
    }
    for peer in peers {
        if inner.peer_is_in_cooldown(&peer.npub, now_ms()) {
            continue;
        }
        let Some(identity) = peer_identity_for_connect(known_links, &peer) else {
            continue;
        };
        let _ = driver.connect_peer(identity, now_ms()).await;
    }
    *known_links = next_links;
}

pub(super) fn peer_identity_for_connect(
    known_links: &HashMap<String, u64>,
    peer: &ConnectedPeerLink,
) -> Option<PeerIdentity> {
    peer_link_needs_connect(known_links, &peer.npub, peer.link_id)
        .then(|| PeerIdentity::from_npub(&peer.npub).ok())
        .flatten()
}

pub(super) fn peer_link_needs_connect(
    known_links: &HashMap<String, u64>,
    peer_npub: &str,
    link_id: u64,
) -> bool {
    known_links.get(peer_npub) != Some(&link_id)
}

async fn process_wire_report(
    inner: &ClientInner,
    driver: &mut WireTcpDriver,
    report: WireTcpReport,
) {
    inner
        .tcp_datagrams_received
        .fetch_add(report.tcp_datagrams as u64, Ordering::Relaxed);
    inner
        .tcp_datagrams_rejected
        .fetch_add(report.rejected_tcp_datagrams as u64, Ordering::Relaxed);
    inner
        .connected_transport_peers
        .store(report.connected_peers, Ordering::Relaxed);
    let mut cooled_peers = HashSet::new();
    for peer in report.newly_connected {
        if inner.peer_is_in_cooldown(&peer.npub(), now_ms()) {
            cooled_peers.insert(peer.npub());
            let _ = driver.abort_peer(peer).await;
            continue;
        }
        inner.reset_peer_epoch(&peer.npub());
        for frame in inner.replay_frames_for_peer(&peer.npub()) {
            let _ = driver.queue_frame(peer, &frame);
        }
    }
    for (peer, frame) in report.frames {
        if cooled_peers.contains(&peer.npub()) || inner.peer_is_in_cooldown(&peer.npub(), now_ms())
        {
            if cooled_peers.insert(peer.npub()) {
                let _ = driver.abort_peer(peer).await;
            }
            continue;
        }
        inner.handle_frame(peer, &frame).await;
    }
    for (peer, frame) in inner.retry_pending_frames(now_ms()) {
        if driver.queue_frame(peer, &frame).is_ok() {
            inner.want_frames_sent.fetch_add(1, Ordering::Relaxed);
        }
    }
}
