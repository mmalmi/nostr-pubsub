use std::sync::Weak;

use super::{
    ClientInner, HashMap, Ordering, PeerIdentity, TCP_POLL_INTERVAL, TransportCommand,
    WireTcpDriver, mpsc, now_ms,
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
                let Some(TransportCommand::Send { peer, frame }) = command else {
                    break;
                };
                let _ = driver.queue_frame(peer, &frame);
                let _ = driver.connect_peer(peer, now_ms()).await;
            }
            report = driver.receive(now_ms()) => {
                if let Ok(report) = report {
                    process_wire_report(&inner, &mut driver, report);
                }
            }
            _ = poll_tick.tick() => {
                sync_transport_peers(&inner, &mut driver, &mut known_links).await;
                if let Ok(report) = driver.poll(now_ms()).await {
                    process_wire_report(&inner, &mut driver, report);
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
    let Ok(peers) = inner.connected_peers().await else {
        return;
    };
    let next_links = peers
        .iter()
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
        let _ = driver.connect_peer(peer.identity, now_ms()).await;
    }
    *known_links = next_links;
}

fn process_wire_report(inner: &ClientInner, driver: &mut WireTcpDriver, report: WireTcpReport) {
    inner
        .connected_transport_peers
        .store(report.connected_peers, Ordering::Relaxed);
    for peer in report.newly_connected {
        for frame in inner.replay_frames_for_peer(&peer.npub()) {
            let _ = driver.queue_frame(peer, &frame);
        }
    }
    for (peer, frame) in report.frames {
        inner.handle_frame(peer, &frame);
    }
    for (peer, frame) in inner.retry_pending_frames(now_ms()) {
        if driver.queue_frame(peer, &frame).is_ok() {
            inner.want_frames_sent.fetch_add(1, Ordering::Relaxed);
        }
    }
}
