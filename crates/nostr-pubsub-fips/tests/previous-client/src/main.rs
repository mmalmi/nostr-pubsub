//! Separate process because Cargo cannot unify the old/new exact TCP pins.
use fips_core::config::{PeerConfig, TransportInstances, UdpConfig};
use fips_core::{Config, FipsEndpoint};
use nostr::{Filter, Kind};
use nostr_pubsub_fips::{FipsPubsubClient, FipsPubsubClientOptions};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut config = Config::new();
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.local.enabled = false;
    config.node.discovery.lan.enabled = false;
    config.transports.udp = TransportInstances::Single(UdpConfig {
        bind_addr: Some("127.0.0.1:0".into()),
        advertise_on_nostr: Some(false),
        accept_connections: Some(true),
        ..Default::default()
    });
    config.peers = vec![PeerConfig::new(&args[0], "udp", &args[1])];
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .config(config)
            .without_system_tun()
            .bind()
            .await
            .unwrap(),
    );
    let client = FipsPubsubClient::start(
        endpoint.clone(),
        FipsPubsubClientOptions {
            routed_peers: vec![args[2].clone()],
            max_connected_peers: 1,
            fanout: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut subscription = client
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(8), subscription.recv()).await;
    let success = if let Ok(Some(event)) = event {
        println!("{}", event.event.as_event().id);
        assert_eq!(client.delivery_snapshot().req_frames_received, 0);
        true
    } else {
        false
    };
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
    std::process::exit(if success { 0 } else { 2 });
}
