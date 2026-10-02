use super::*;
use std::net::SocketAddrV4;

pub(super) fn private_rendezvous_addr() -> SocketAddrV4 {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("reserve private rendezvous address");
    let SocketAddr::V4(addr) = socket.local_addr().unwrap() else {
        panic!("expected IPv4 loopback address");
    };
    addr
}

pub(super) async fn local_endpoint(addr: SocketAddrV4, secret: [u8; 32]) -> Arc<FipsEndpoint> {
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode(secret)),
        persistent: false,
    };
    config.node.routing.mode = fips_core::config::RoutingMode::ReplyLearned;
    config.node.discovery.local.rendezvous_addr = addr;
    config.node.discovery.local.retry_interval_ms = 20;
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.lan.enabled = false;
    Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .local_rendezvous()
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind local rendezvous endpoint"),
    )
}

pub(super) async fn wait_for_local_link(endpoint: &FipsEndpoint, npub: &str) {
    timeout(Duration::from_secs(20), async {
        loop {
            if let Some(peer) = endpoint
                .peers()
                .await
                .unwrap()
                .iter()
                .find(|peer| peer.connected && peer.npub == npub)
            {
                assert_eq!(peer.transport_type.as_deref(), Some("udp"));
                assert!(
                    peer.transport_addr
                        .as_ref()
                        .unwrap()
                        .parse::<SocketAddr>()
                        .unwrap()
                        .ip()
                        .is_loopback()
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("local rendezvous link should authenticate");
}

pub(super) async fn wait_for_selected_peers(
    client: &FipsPubsubClient,
    expected: &[&str],
) -> Vec<ConnectedPeerLink> {
    timeout(Duration::from_secs(20), async {
        loop {
            let peers = client.inner.connected_peer_links().await.unwrap();
            if peers
                .iter()
                .map(|peer| peer.npub.as_str())
                .collect::<Vec<_>>()
                == expected
            {
                return peers;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("selected local pubsub services should converge")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_service_peers_exchange_events_and_survive_anchor_exit_without_routed_peers() {
    let addr = private_rendezvous_addr();
    // This endpoint owns only the rendezvous socket and has no pubsub client.
    let anchor = local_endpoint(addr, [91; 32]).await;
    let a = local_endpoint(addr, [92; 32]).await;
    let c = local_endpoint(addr, [93; 32]).await;
    for endpoint in [&a, &c] {
        wait_for_local_link(endpoint, anchor.npub()).await;
    }
    let options = FipsPubsubClientOptions {
        max_connected_peers: 1,
        fanout: 1,
        ..Default::default()
    };
    assert!(options.routed_peers.is_empty());
    let client_a = FipsPubsubClient::start(a.clone(), options.clone())
        .await
        .unwrap();
    let mut client_c = FipsPubsubClient::start(c.clone(), options.clone())
        .await
        .unwrap();
    let filter = Filter::new().kind(Kind::TextNote);
    let mut subscription_a = client_a.subscribe(vec![filter.clone()]).await.unwrap();
    let mut subscription_c = client_c.subscribe(vec![filter]).await.unwrap();
    wait_for_selected_peers(&client_a, &[c.npub()]).await;
    wait_for_selected_peers(&client_c, &[a.npub()]).await;
    wait_for_peer_subscription_count(&client_a, 2).await;
    wait_for_peer_subscription_count(&client_c, 2).await;

    for phase in ["through anchor", "after automatic anchor replacement"] {
        for (sender, receiver, direction) in [
            (&client_a, &mut subscription_c, "A to C"),
            (&client_c, &mut subscription_a, "C to A"),
        ] {
            let event = signed_note(&format!("{direction} {phase}"));
            sender
                .publish(event.clone(), EventSource::local_index("local-test"))
                .await
                .unwrap();
            assert_eq!(
                timeout(Duration::from_secs(10), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .event,
                event,
            );
        }
        if phase == "through anchor" {
            for endpoint in [&a, &c] {
                let peers = endpoint.peers().await.unwrap();
                let connected = peers
                    .iter()
                    .filter(|peer| peer.connected)
                    .collect::<Vec<_>>();
                assert_eq!(connected.len(), 1, "leaf has only its physical anchor link");
                assert_eq!(connected[0].npub, anchor.npub());
            }
            anchor.shutdown().await.unwrap();
            wait_for_local_link(&a, c.npub()).await;
            wait_for_local_link(&c, a.npub()).await;
            // The same identities are now both direct and advertised. They
            // still consume one slot, with the original subscriptions intact.
            wait_for_selected_peers(&client_a, &[c.npub()]).await;
            wait_for_selected_peers(&client_c, &[a.npub()]).await;
            wait_for_peer_subscription_count(&client_a, 2).await;
            wait_for_peer_subscription_count(&client_c, 2).await;
        }
    }
    drop(subscription_c);
    client_c.shutdown().await;
    wait_for_selected_peers(&client_a, &[]).await;
    client_c = FipsPubsubClient::start(c.clone(), options).await.unwrap();
    wait_for_selected_peers(&client_a, &[c.npub()]).await;
    wait_for_peer_subscription_count(&client_c, 2).await;
    let event = signed_note("original subscription after local service restart");
    client_c
        .publish(event.clone(), EventSource::local_index("local-test"))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(10), subscription_a.recv())
            .await
            .unwrap()
            .unwrap()
            .event,
        event
    );
    drop(subscription_a);
    client_a.shutdown().await;
    client_c.shutdown().await;
    a.shutdown().await.unwrap();
    c.shutdown().await.unwrap();
}

#[test]
fn routed_roster_is_validated_bounded_and_canonical() {
    use crate::client_peers::validate_routed_peers;
    let local = Identity::from_secret_bytes(&[71; 32]).unwrap().npub();
    let remote = Identity::from_secret_bytes(&[73; 32]).unwrap().npub();
    assert_eq!(
        validate_routed_peers(
            vec![local.clone(), remote.clone(), remote.clone()],
            3,
            &local,
            false
        )
        .unwrap(),
        vec![remote.clone()]
    );
    assert!(validate_routed_peers(vec![remote.clone()], 0, &local, false).is_err());
    assert!(validate_routed_peers(vec![remote], 1, &local, true).is_err());
    assert!(validate_routed_peers(vec!["invalid".into()], 1, &local, false).is_err());
    assert!(
        validate_routed_peers(vec![], 1, &local, true)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_service_peers_exchange_events_through_an_uninterested_router() {
    let network_id = format!("nostr-pubsub-routed-{}", std::process::id());
    register_sim_network(&network_id, SimNetwork::new(7385));
    let b_identity = Identity::from_secret_bytes(&[72; 32]).unwrap();
    let b = live_endpoint(&network_id, "b", [72; 32], []).await;
    let a = live_endpoint(&network_id, "a", [71; 32], [(b_identity.npub(), "b")]).await;
    let c = live_endpoint(&network_id, "c", [73; 32], [(b_identity.npub(), "b")]).await;
    wait_for_connected_peer(&a, b.npub()).await;
    wait_for_connected_peer(&c, b.npub()).await;
    check_routed_exchange(a, b, c).await;
    unregister_sim_network(&network_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_service_peers_exchange_events_over_real_udp_without_discovery() {
    let b = udp_endpoint([82; 32], Vec::new()).await;
    let address = b.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let a = udp_endpoint([81; 32], vec![PeerConfig::new(b.npub(), "udp", &address)]).await;
    let c = udp_endpoint([83; 32], vec![PeerConfig::new(b.npub(), "udp", &address)]).await;
    check_routed_exchange(a, b, c).await;
}

pub(super) async fn udp_endpoint(secret: [u8; 32], peers: Vec<PeerConfig>) -> Arc<FipsEndpoint> {
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode(secret)),
        persistent: false,
    };
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.local.enabled = false;
    config.node.discovery.lan.enabled = false;
    config.transports.udp = TransportInstances::Single(fips_core::config::UdpConfig {
        bind_addr: Some("127.0.0.1:0".into()),
        advertise_on_nostr: Some(false),
        accept_connections: Some(true),
        ..Default::default()
    });
    config.peers = peers;
    Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .without_system_tun()
                .bind(),
        )
        .await
        .unwrap(),
    )
}

async fn check_routed_exchange(a: Arc<FipsEndpoint>, b: Arc<FipsEndpoint>, c: Arc<FipsEndpoint>) {
    let options = |npub: &str| FipsPubsubClientOptions {
        routed_peers: vec![npub.to_owned()],
        max_connected_peers: 1,
        fanout: 1,
        ..Default::default()
    };
    let client_a = FipsPubsubClient::start(a.clone(), options(c.npub()))
        .await
        .unwrap();
    assert!(client_a.set_routed_peers(vec!["invalid".into()]).is_err());
    assert_eq!(
        client_a.inner.routed_peers.lock().unwrap().as_slice(),
        &[c.npub()]
    );
    let client_c = FipsPubsubClient::start(c.clone(), options(a.npub()))
        .await
        .unwrap();
    let filter = Filter::new().kind(Kind::TextNote);
    let mut subscription_a = client_a.subscribe(vec![filter.clone()]).await.unwrap();
    let mut subscription_c = client_c.subscribe(vec![filter]).await.unwrap();
    wait_for_peer_subscription_count(&client_a, 2).await;
    wait_for_peer_subscription_count(&client_c, 2).await;
    for (sender, receiver, content) in [
        (&client_a, &mut subscription_c, "A through B to C"),
        (&client_c, &mut subscription_a, "C through B to A"),
    ] {
        let event = signed_note(content);
        sender
            .publish(event.clone(), EventSource::local_index("routed-test"))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(5), receiver.recv())
                .await
                .unwrap()
                .unwrap()
                .event,
            event
        );
    }
    for endpoint in [&a, &c] {
        let peers = endpoint.peers().await.unwrap();
        assert_eq!(peers.iter().filter(|peer| peer.connected).count(), 1);
        assert!(
            peers
                .iter()
                .filter(|peer| peer.connected)
                .all(|peer| peer.npub == b.npub())
        );
    }
    // The router has no pubsub client at all and cannot inspect either stream.
    assert_eq!(client_a.connected_peer_count().unwrap(), 1);
    client_a.set_routed_peers(Vec::new()).unwrap();
    timeout(Duration::from_secs(5), async {
        while client_a.peer_subscription_count().unwrap() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("removed identity loses retained subscription state");
    client_a
        .set_routed_peers(vec![c.npub().to_owned()])
        .unwrap();
    wait_for_peer_subscription_count(&client_a, 2).await;
    let event = signed_note("same subscription after roster rejoin");
    client_c
        .publish(event.clone(), EventSource::local_index("routed-test"))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), subscription_a.recv())
            .await
            .unwrap()
            .unwrap()
            .event,
        event
    );
    drop((subscription_a, subscription_c));
    client_a.shutdown().await;
    client_c.shutdown().await;
    for endpoint in [a, b, c] {
        endpoint.shutdown().await.unwrap();
    }
}

pub(super) fn signed_note(content: &str) -> VerifiedEvent {
    VerifiedEvent::try_from(
        EventBuilder::text_note(content)
            .sign_with_keys(&Keys::generate())
            .unwrap(),
    )
    .unwrap()
}
