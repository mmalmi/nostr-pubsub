use std::sync::Arc;
use std::time::Duration;

use fips_core::config::{PeerConfig, TransportInstances};
use fips_core::{
    Config, FipsEndpoint, Identity, IdentityConfig, SimNetwork, SimTransportConfig,
    register_sim_network, unregister_sim_network,
};
use nostr::{EventBuilder, Filter, Keys, Kind};
use nostr_pubsub::{EventBus, EventSourceKind, PubsubProvider, QueryOptions, VerifiedEvent};
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::*;

#[test]
fn client_limits_reject_unbounded_fips_frames() {
    let options = FipsPubsubClientOptions {
        max_frame_bytes: FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES + 1,
        ..FipsPubsubClientOptions::default()
    };

    assert!(options.validate().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_fips_endpoints_query_publish_and_close_over_service_port() {
    let network_id = format!("nostr-pubsub-fips-{}", std::process::id());
    register_sim_network(&network_id, SimNetwork::new(7368));

    let identity_a = Identity::from_secret_bytes(&[1; 32]).expect("identity A");
    let identity_b = Identity::from_secret_bytes(&[2; 32]).expect("identity B");
    let endpoint_a = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(endpoint_config(
                    &network_id,
                    "endpoint-a",
                    [1; 32],
                    identity_b.npub(),
                    "endpoint-b",
                ))
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind endpoint A"),
    );
    let endpoint_b = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(endpoint_config(
                    &network_id,
                    "endpoint-b",
                    [2; 32],
                    identity_a.npub(),
                    "endpoint-a",
                ))
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind endpoint B"),
    );
    wait_for_connected_peer(&endpoint_a, endpoint_b.npub()).await;
    wait_for_connected_peer(&endpoint_b, endpoint_a.npub()).await;

    endpoint_b
        .register_service(FIPS_NOSTR_PUBSUB_SERVICE_PORT)
        .await
        .expect("register peer pubsub service");
    let replay_event = VerifiedEvent::try_from(
        EventBuilder::text_note("replayed from local peer")
            .sign_with_keys(&Keys::generate())
            .expect("sign replay event"),
    )
    .expect("verify replay event");
    let (service_tx, mut service_rx) = mpsc::unbounded_channel();
    let service_task =
        spawn_peer_service(Arc::clone(&endpoint_b), replay_event.clone(), service_tx);

    let client = FipsPubsubClient::start_for_transport(
        Arc::clone(&endpoint_a),
        FipsPubsubClientOptions {
            query_timeout: Duration::from_secs(2),
            ..FipsPubsubClientOptions::default()
        },
        "sim",
    )
    .await
    .expect("start FIPS pubsub client");
    assert_eq!(client.mode(), PubsubProviderMode::LocalOnly);
    assert_eq!(client.connected_peer_count().await.expect("peer count"), 1);

    let report = client
        .query(
            vec![Filter::new().kind(Kind::TextNote)],
            QueryOptions { limit: Some(1) },
        )
        .await
        .expect("query local FIPS peer");
    assert_eq!(report.events.len(), 1);
    assert_eq!(report.events[0].event, replay_event);
    assert_eq!(report.events[0].source.kind, EventSourceKind::FipsEndpoint);
    assert_eq!(report.events[0].source.id.as_str(), endpoint_b.npub());
    assert_eq!(
        client.active_subscription_count().expect("subscriptions"),
        0
    );

    let req_id = recv_req_id(&mut service_rx).await;
    recv_close(&mut service_rx, &req_id).await;

    let published = VerifiedEvent::try_from(
        EventBuilder::text_note("published to local peer")
            .sign_with_keys(&Keys::generate())
            .expect("sign published event"),
    )
    .expect("verify published event");
    let publish_report = client
        .publish(published.clone(), EventSource::local_index("test"))
        .await
        .expect("publish to local FIPS peer");
    assert!(publish_report.accepted);
    recv_publish(&mut service_rx, &published).await;

    client.shutdown().await;
    service_task.abort();
    let _ = service_task.await;
    endpoint_a.shutdown().await.expect("shutdown endpoint A");
    endpoint_b.shutdown().await.expect("shutdown endpoint B");
    unregister_sim_network(&network_id);
}

fn endpoint_config(
    network_id: &str,
    local_addr: &str,
    secret: [u8; 32],
    peer_npub: String,
    peer_addr: &str,
) -> Config {
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode(secret)),
        persistent: false,
    };
    config.node.rate_limit.handshake_burst = 1_000;
    config.node.rate_limit.handshake_rate = 1_000.0;
    config.node.retry.base_interval_secs = 1;
    config.node.retry.max_backoff_secs = 1;
    config.transports.sim = TransportInstances::Single(SimTransportConfig {
        network: Some(network_id.to_string()),
        addr: Some(local_addr.to_string()),
        mtu: Some(1280),
        auto_connect: Some(false),
        accept_connections: Some(true),
    });
    config.peers = vec![PeerConfig::new(peer_npub, "sim", peer_addr)];
    config
}

async fn wait_for_connected_peer(endpoint: &FipsEndpoint, expected_npub: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            let connected = endpoint
                .peers()
                .await
                .expect("peer snapshot")
                .into_iter()
                .any(|peer| {
                    peer.connected
                        && peer.npub == expected_npub
                        && peer.transport_type.as_deref() == Some("sim")
                });
            if connected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("FIPS endpoints connect");
}

fn spawn_peer_service(
    endpoint: Arc<FipsEndpoint>,
    replay_event: VerifiedEvent,
    messages: mpsc::UnboundedSender<FipsPubsubWireMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let codec =
            FipsPubsubWireCodec::new(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES).expect("service codec");
        let mut datagrams = Vec::new();
        loop {
            let Some(_) = endpoint
                .recv_service_datagram_batch_into(&mut datagrams, 16)
                .await
            else {
                return;
            };
            for datagram in datagrams.drain(..) {
                assert_eq!(datagram.source_port, FIPS_NOSTR_PUBSUB_SERVICE_PORT);
                assert_eq!(datagram.destination_port, FIPS_NOSTR_PUBSUB_SERVICE_PORT);
                let message = codec
                    .decode_frame(datagram.data.as_slice())
                    .expect("decode service request");
                messages
                    .send(message.clone())
                    .expect("record service message");
                if let FipsPubsubWireMessage::Req {
                    subscription_id, ..
                } = message
                {
                    let unrelated = codec
                        .encode_frame(&FipsPubsubWireMessage::deliver(
                            SubscriptionId::new("not-subscribed"),
                            replay_event.clone(),
                        ))
                        .expect("encode unrelated reply");
                    endpoint
                        .send_datagram(
                            datagram.source_peer,
                            FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                            datagram.source_port,
                            unrelated,
                        )
                        .await
                        .expect("send unrelated reply");
                    let reply = codec
                        .encode_frame(&FipsPubsubWireMessage::deliver(
                            subscription_id,
                            replay_event.clone(),
                        ))
                        .expect("encode subscribed reply");
                    endpoint
                        .send_datagram(
                            datagram.source_peer,
                            FIPS_NOSTR_PUBSUB_SERVICE_PORT,
                            datagram.source_port,
                            reply,
                        )
                        .await
                        .expect("send subscribed reply");
                }
            }
        }
    })
}

async fn recv_req_id(
    messages: &mut mpsc::UnboundedReceiver<FipsPubsubWireMessage>,
) -> SubscriptionId {
    loop {
        let message = timeout(Duration::from_secs(2), messages.recv())
            .await
            .expect("REQ timeout")
            .expect("service channel open");
        if let FipsPubsubWireMessage::Req {
            subscription_id, ..
        } = message
        {
            return subscription_id;
        }
    }
}

async fn recv_close(
    messages: &mut mpsc::UnboundedReceiver<FipsPubsubWireMessage>,
    expected: &SubscriptionId,
) {
    loop {
        let message = timeout(Duration::from_secs(2), messages.recv())
            .await
            .expect("CLOSE timeout")
            .expect("service channel open");
        if let FipsPubsubWireMessage::Close { subscription_id } = message {
            assert_eq!(&subscription_id, expected);
            return;
        }
    }
}

async fn recv_publish(
    messages: &mut mpsc::UnboundedReceiver<FipsPubsubWireMessage>,
    expected: &VerifiedEvent,
) {
    loop {
        let message = timeout(Duration::from_secs(2), messages.recv())
            .await
            .expect("publish timeout")
            .expect("service channel open");
        if let FipsPubsubWireMessage::Event {
            subscription_id: None,
            event,
        } = message
        {
            assert_eq!(&event, expected);
            return;
        }
    }
}
