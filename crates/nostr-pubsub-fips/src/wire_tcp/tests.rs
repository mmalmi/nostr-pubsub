use super::*;
use fips_core::config::{IdentityConfig, SimTransportConfig, TransportInstances};
use fips_core::{Config, Identity, SimNetwork, register_sim_network, unregister_sim_network};

fn options(peer_capacity: usize) -> WireTcpOptions {
    WireTcpOptions {
        frame_capacity: 1024,
        peer_capacity,
        inbound_peer_capacity: 0,
        queue_records_per_peer: 4,
        queue_bytes_per_peer: 4096,
        drive_io_bytes: 4096,
        drive_frames: 4,
    }
}

#[test]
fn oversized_record_prefix_is_ready_for_rejection_without_filling_the_buffer() {
    for declared in [1025_u32, u32::MAX] {
        let mut decoder = RecordDecoder::new(1024);
        let prefix = declared.to_be_bytes();
        decoder.push(&prefix[..3]).unwrap();
        assert!(!decoder.has_complete_record());
        decoder.push(&prefix[3..]).unwrap();
        // This is the driver's readiness predicate: it must let take()
        // reject the prefix before more peer bytes fill the bounded input.
        assert!(decoder.has_complete_record());
        assert!(decoder.take(1).is_err());
        assert_eq!(decoder.remaining_capacity(), 1028);
        assert!(!decoder.has_complete_record());
        decoder.push(&encode_record(b"valid").unwrap()).unwrap();
        assert_eq!(decoder.take(1).unwrap(), vec![b"valid".to_vec()]);
    }
}

#[tokio::test]
async fn oversized_record_aborts_only_its_stream_and_preserves_other_frames() {
    let mut config = Config::new();
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.local.enabled = false;
    config.node.discovery.lan.enabled = false;
    let endpoint = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .without_system_tun()
                .bind(),
        )
        .await
        .unwrap(),
    );
    let mut peers = [74, 75].map(|value| {
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[value; 32]).unwrap().npub()).unwrap()
    });
    peers.sort_by_key(PeerIdentity::npub);
    let [healthy, malformed] = peers;
    let mut driver = WireTcpDriver::bind(endpoint.clone(), options(2), 99)
        .await
        .unwrap();
    driver
        .select_peers(peers.map(|peer| peer.npub()).into())
        .await;
    for peer in peers {
        driver.connect_peer(&peer.npub(), 0).await.unwrap();
        let id = driver
            .connections
            .iter()
            .find(|(_, connection)| connection.peer == peer.npub())
            .unwrap()
            .0;
        driver.active.insert(peer.npub(), *id);
        let mut decoder = RecordDecoder::new(1024);
        decoder
            .push(&if peer == healthy {
                encode_record(b"valid").unwrap()
            } else {
                u32::MAX.to_be_bytes().to_vec()
            })
            .unwrap();
        driver.inputs.insert(peer.npub(), decoder);
    }
    let malformed_id = driver.active[&malformed.npub()];
    assert_eq!(
        driver.read_active(1).await.unwrap(),
        (vec![(healthy, b"valid".to_vec())], 1)
    );
    assert!(!driver.inputs.contains_key(&malformed.npub()));
    assert!(!driver.active.contains_key(&malformed.npub()));
    assert_eq!(connection_count(&driver), 1);
    assert!(driver.tcp.state(malformed_id).is_none());
    assert_eq!(driver.read_active(2).await.unwrap(), (Vec::new(), 0));
    drop(driver);
    endpoint.shutdown().await.unwrap();
}

#[tokio::test]
async fn connection_admission_defers_validation_and_preserves_queued_recovery() {
    let network = format!("pubsub-wire-queue-{}", std::process::id());
    register_sim_network(&network, SimNetwork::new(7375));
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode([71; 32])),
        persistent: false,
    };
    config.node.discovery.nostr.enabled = false;
    config.transports.sim = TransportInstances::Single(SimTransportConfig {
        network: Some(network.clone()),
        addr: Some("queue-local".to_string()),
        auto_connect: Some(false),
        ..Default::default()
    });
    let endpoint = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .without_system_tun()
                .bind(),
        )
        .await
        .unwrap(),
    );
    let mut driver = WireTcpDriver::bind(endpoint.clone(), options(1), 73)
        .await
        .unwrap();
    let early =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[72; 32]).unwrap().npub()).unwrap();
    let late =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[73; 32]).unwrap().npub()).unwrap();
    driver.select_peers(BTreeSet::from([early.npub()])).await;
    driver.connect_peer(&early.npub(), 100).await.unwrap();
    driver.queue_frame(early, b"partly-written").unwrap();
    let queued = driver.queues.get_mut(&early.npub()).unwrap();
    queued.records.front_mut().unwrap().offset = 3;
    queued.bytes -= 3;

    driver.abort_peer(early).await.unwrap();
    let queued = driver.queues.get(&early.npub()).unwrap();
    assert_eq!(queued.records.front().unwrap().offset, 0);
    assert_eq!(queued.bytes, b"partly-written".len() + 4);

    // A link refresh keeps queued data and the remaining retry deadline.
    driver.select_peers(BTreeSet::from([early.npub()])).await;
    driver.connect_peer(&early.npub(), u64::MAX).await.unwrap();
    assert_eq!(connection_count(&driver), 0);
    driver.connect_peer(&early.npub(), 0).await.unwrap();
    assert_eq!(connection_count(&driver), 0);
    tokio::time::sleep(SERVICE_RETRY_INTERVAL).await;
    driver.connect_peer(&early.npub(), 0).await.unwrap();
    assert_eq!(connection_count(&driver), 1);

    assert_failed_handshake_defers_redial(&mut driver, early).await;
    assert_eq!(
        driver.queues[&early.npub()].bytes,
        b"partly-written".len() + 4
    );

    driver.select_peers(BTreeSet::from([late.npub()])).await;
    assert!(!driver.queues.contains_key(&early.npub()));
    assert!(driver.queue_frame(early, b"delayed old command").is_err());
    assert!(driver.connect_peer(&early.npub(), 0).await.is_err());
    driver.queue_frame(late, b"replacement request").unwrap();
    assert_eq!(driver.queues.len(), 1);
    assert!(driver.queues.contains_key(&late.npub()));
    assert_eq!(driver.selected_peers.len(), 1);
    driver.connect_peer(&late.npub(), 1_100).await.unwrap();
    assert_eq!(connection_count(&driver), 1);

    // An invalid sentinel makes decoding observable without a counter or
    // timing assertion: admission must reject/skip it before validation.
    let invalid = "not-an-npub";
    let error = driver.connect_peer(invalid, 0).await.unwrap_err();
    assert!(error.to_string().contains("no longer selected"));
    driver
        .select_peers(BTreeSet::from([invalid.to_owned()]))
        .await;
    driver
        .selected_peers
        .get_mut(invalid)
        .unwrap()
        .next_attempt_at = Some(Instant::now() + SERVICE_RETRY_INTERVAL);
    driver.connect_peer(invalid, 0).await.unwrap();
    assert_eq!(connection_count(&driver), 0);
    driver
        .selected_peers
        .insert(invalid.to_owned(), ServiceRetry::default());
    let error = driver.connect_peer(invalid, 0).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("decode TCP/FIPS Nostr pubsub peer")
    );
    assert_eq!(connection_count(&driver), 0);
    assert!(driver.queues.is_empty());
    drop(driver);
    endpoint.shutdown().await.unwrap();
    unregister_sim_network(&network);
}

async fn assert_failed_handshake_defers_redial(driver: &mut WireTcpDriver, peer: PeerIdentity) {
    // TCP's unanswered SYN can outlive the initial service retry deadline.
    // Drive that real timeout, with its old monotonic deadline expired, and
    // verify that peer sync/application sends cannot immediately redial it.
    let expiring = *driver.connections.keys().next().unwrap();
    driver
        .selected_peers
        .get_mut(&peer.npub())
        .unwrap()
        .next_attempt_at = Instant::now().checked_sub(SERVICE_RETRY_INTERVAL);
    for tick in 1..=10 {
        driver.poll(tick * 60_000).await.unwrap();
        if driver.tcp.state(expiring).is_none() {
            break;
        }
    }
    assert!(driver.tcp.state(expiring).is_none());
    assert_eq!(connection_count(driver), 0);
    driver.connect_peer(&peer.npub(), 700_000).await.unwrap();
    assert_eq!(
        connection_count(driver),
        0,
        "a failed service needs a fresh cooldown after the TCP timeout"
    );
}

pub(super) fn connection_count(driver: &WireTcpDriver) -> usize {
    driver.connections.len().max(driver.inbound.len())
}
