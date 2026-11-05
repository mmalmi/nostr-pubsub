use super::*;
use fips_core::config::{PeerAddress, PeerAdmissionConfig, UdpConfig};

async fn local_provider_endpoint(
    rendezvous: std::net::SocketAddrV4,
    secret: [u8; 32],
) -> Arc<FipsEndpoint> {
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode(secret)),
        persistent: false,
    };
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.lan.enabled = false;
    config.node.discovery.local.rendezvous_addr = rendezvous;
    config.node.discovery.local.retry_interval_ms = 20;
    config.transports.udp = TransportInstances::Single(UdpConfig {
        bind_addr: Some("127.0.0.1:0".into()),
        advertise_on_nostr: Some(false),
        accept_connections: Some(true),
        ..Default::default()
    });
    Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .local_rendezvous()
                .without_system_tun()
                .bind(),
        )
        .await
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strict_reader_uses_accepting_provider_hint_and_revokes_its_only_path() {
    let rendezvous = routed::private_rendezvous_addr();
    let anchor = local_provider_endpoint(rendezvous, [230; 32]).await;
    let identified = local_provider_endpoint(rendezvous, [231; 32]).await;
    let provider_endpoint = local_provider_endpoint(rendezvous, [232; 32]).await;
    let provider = FipsPubsubClient::start(
        provider_endpoint.clone(),
        FipsPubsubClientOptions {
            max_connected_peers: 1,
            max_inbound_routed_peers: 1,
            fanout: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(15), async {
        loop {
            let ready = identified
                .local_instance_advertisements()
                .unwrap()
                .iter()
                .any(|advert| {
                    advert.npub == provider_endpoint.npub()
                        && advert.capability(FIPS_NOSTR_PUBSUB_CAPABILITY).is_some()
                });
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("identified endpoint discovers the public provider");
    assert!(
        identified
            .peers()
            .await
            .unwrap()
            .iter()
            .all(|peer| { !peer.connected || peer.npub != provider_endpoint.npub() }),
        "listener hints must also work through the authenticated anchor roster"
    );
    let provider_identity = PeerIdentity::from_npub(provider_endpoint.npub()).unwrap();
    let addresses = identified
        .authenticated_local_udp_listen_addrs(provider_identity)
        .await
        .unwrap();
    assert!(
        !addresses.is_empty(),
        "first lookup must return accepting listeners"
    );
    let listening = provider_endpoint.bound_udp_listen_addrs().await.unwrap();
    assert!(
        addresses
            .iter()
            .all(|address| { address.ip().is_loopback() && listening.contains(address) })
    );

    let keys = Keys::generate();
    let event = VerifiedEvent::try_from(
        EventBuilder::new(Kind::Metadata, "{\"name\":\"retained provider metadata\"}")
            .sign_with_keys(&keys)
            .unwrap(),
    )
    .unwrap();
    let store = Arc::new(InMemoryEventBus::default());
    store
        .publish(event.clone(), EventSource::local_index("strict-reader"))
        .await
        .unwrap();
    provider.set_replay_source(Some(store)).unwrap();

    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode([233; 32])),
        persistent: false,
    };
    config.node.strict_client_only = true;
    config.node.peer_admission = PeerAdmissionConfig {
        configured_only: true,
        denied_npubs: vec![identified.npub().to_owned(), anchor.npub().to_owned()],
    };
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.local.enabled = false;
    config.node.discovery.lan.enabled = false;
    config.transports.udp = TransportInstances::Single(UdpConfig {
        bind_addr: Some("127.0.0.1:0".into()),
        advertise_on_nostr: Some(false),
        accept_connections: Some(false),
        ..Default::default()
    });
    let mut provider_peer =
        PeerConfig::new(provider_endpoint.npub(), "udp", addresses[0].to_string());
    provider_peer.addresses = addresses
        .iter()
        .map(|address| PeerAddress::new("udp", address.to_string()).authenticated())
        .collect();
    config.peers = vec![provider_peer];
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
    let reader = FipsPubsubClient::start_anonymous_discovery(
        endpoint.clone(),
        FipsPubsubClientOptions {
            routed_peers: vec![provider_endpoint.npub().to_owned()],
            max_connected_peers: 1,
            ..Default::default()
        },
        FipsDiscoveryScope::Author(keys.public_key()),
    )
    .await
    .unwrap();
    let mut subscription = reader
        .subscribe(vec![
            Filter::new().kind(Kind::Metadata).author(keys.public_key()),
        ])
        .await
        .unwrap();
    let delivered = timeout(Duration::from_secs(15), subscription.recv())
        .await
        .expect("anonymous reader receives the first stored event")
        .unwrap();
    assert_eq!(delivered.event, event);
    assert!(endpoint.routing_status().await.unwrap().client_only);
    assert!(endpoint.bound_udp_listen_addrs().await.unwrap().is_empty());
    let connected = endpoint
        .peers()
        .await
        .unwrap()
        .into_iter()
        .filter(|peer| peer.connected)
        .map(|peer| peer.npub)
        .collect::<Vec<_>>();
    assert_eq!(connected, vec![provider_endpoint.npub().to_owned()]);
    assert_eq!(reader.delivery_snapshot().req_frames_received, 0);
    endpoint
        .update_peer_admission(PeerAdmissionConfig {
            configured_only: true,
            denied_npubs: vec![
                identified.npub().to_owned(),
                anchor.npub().to_owned(),
                provider_endpoint.npub().to_owned(),
            ],
        })
        .await
        .unwrap();
    assert!(
        endpoint
            .peers()
            .await
            .unwrap()
            .iter()
            .all(|peer| !peer.connected)
    );
    drop(subscription);
    reader.shutdown().await;
    provider.shutdown().await;
    for endpoint in [endpoint, provider_endpoint, identified, anchor] {
        endpoint.shutdown().await.unwrap();
    }
}
