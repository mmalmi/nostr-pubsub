use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use fips_core::config::{
    NostrDiscoveryConfig, NostrPeerfindingSource, PeerConfig, TcpConfig, TransportInstances,
};
use fips_core::{
    Config, FipsEndpoint, Identity, IdentityConfig, SimNetwork, SimTransportConfig,
    register_sim_network, unregister_sim_network,
};
use nostr::{
    Alphabet, Event, EventBuilder, Filter, JsonUtil, Keys, Kind, SingleLetterTag, Tag, TagKind,
    Timestamp, ToBech32,
};
use nostr_pubsub::{
    EventBus, EventSourceKind, InMemoryEventBus, PolicyDecision, PubsubPeerSubscriptionSnapshot,
    PubsubProvider, QueryOptions, VerifiedEvent,
};
use nostr_social_graph::Rating;
use nostr_social_memory::RatingEventExt;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::*;

#[tokio::test]
async fn client_advertises_only_while_its_fsp_service_is_registered() {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("reserve local rendezvous address");
    let SocketAddr::V4(rendezvous_addr) = socket.local_addr().expect("reserved address") else {
        panic!("reserved rendezvous address should be IPv4");
    };
    drop(socket);

    let mut config = Config::new();
    config.node.discovery.local.rendezvous_addr = rendezvous_addr;
    config.node.discovery.nostr.enabled = false;
    config.node.discovery.lan.enabled = false;
    let endpoint = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config)
                .local_rendezvous()
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind local FIPS endpoint"),
    );

    let client = FipsPubsubClient::start(Arc::clone(&endpoint), FipsPubsubClientOptions::default())
        .await
        .expect("start FIPS pubsub client");
    wait_for_local_capability(&endpoint, true).await;

    client.shutdown().await;
    wait_for_local_capability(&endpoint, false).await;
    endpoint.shutdown().await.expect("shutdown endpoint");
}

async fn wait_for_local_capability(endpoint: &FipsEndpoint, expected: bool) {
    timeout(Duration::from_secs(5), async {
        loop {
            let advertised = endpoint
                .local_instance_advertisements()
                .expect("local capability snapshot")
                .iter()
                .any(|advert| {
                    advert.npub == endpoint.npub()
                        && advert.capability(FIPS_NOSTR_PUBSUB_CAPABILITY).is_some_and(
                            |capability| {
                                capability.fsp_port == Some(FIPS_NOSTR_PUBSUB_SERVICE_PORT)
                            },
                        )
                });
            if advertised == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("local pubsub capability state should converge");
}

#[test]
fn fips_discovery_config_converts_to_bounded_pubsub_retention() {
    let config = NostrDiscoveryConfig {
        enabled: true,
        peerfinding_source: NostrPeerfindingSource::External,
        app: "fips-test".to_string(),
        advert_cache_max_entries: 17,
        ..NostrDiscoveryConfig::default()
    };
    let policy = fips_discovery_retention_policy(&config).expect("enabled discovery policy");
    let matching = VerifiedEvent::try_from(
        EventBuilder::new(Kind::Custom(37195), "advert")
            .tags([Tag::identifier("fips-test")])
            .sign_with_keys(&Keys::generate())
            .expect("signed matching event"),
    )
    .expect("verified matching event");
    let other_app = VerifiedEvent::try_from(
        EventBuilder::new(Kind::Custom(37195), "advert")
            .tags([Tag::identifier("other-app")])
            .sign_with_keys(&Keys::generate())
            .expect("signed other-app event"),
    )
    .expect("verified other-app event");

    assert_eq!(policy.max_events, 17);
    assert!(policy.accepts(&matching));
    assert!(!policy.accepts(&other_app));
    assert!(fips_discovery_retention_policy(&NostrDiscoveryConfig::default()).is_none());
}

#[tokio::test]
async fn verified_pubsub_event_ingests_through_neutral_fips_api() {
    let mut config = Config::new();
    config.node.discovery.nostr.enabled = true;
    config.node.discovery.nostr.peerfinding_source = NostrPeerfindingSource::External;
    config.node.discovery.nostr.advertise = false;
    config.node.discovery.nostr.advert_relays.clear();
    config.node.discovery.nostr.app = "fips-test".to_string();
    let endpoint = Box::pin(
        FipsEndpoint::builder()
            .config(config)
            .without_system_tun()
            .bind(),
    )
    .await
    .expect("endpoint with Nostr discovery");
    let now = Timestamp::now().as_secs();
    let event = EventBuilder::new(
        Kind::Custom(37195),
        r#"{"identifier":"fips-overlay-v1","version":1,"endpoints":[{"transport":"tcp","addr":"8.8.8.8:443"}]}"#,
    )
    .tags([
        Tag::identifier("fips-test"),
        Tag::custom(TagKind::custom("protocol"), ["fips-test"]),
        Tag::custom(TagKind::custom("version"), ["1"]),
        Tag::expiration(Timestamp::from(now.saturating_add(3_600))),
    ])
    .custom_created_at(Timestamp::from(now))
    .sign_with_keys(&Keys::generate())
    .expect("signed FIPS advert");

    assert!(
        ingest_fips_discovery_event(
            &endpoint,
            VerifiedEvent::try_from(event).expect("verified FIPS advert"),
        )
        .await
        .expect("FIPS discovery ingest")
    );
    endpoint.shutdown().await.expect("endpoint shutdown");
}

#[tokio::test]
async fn peerfinder_routes_publication_and_lookup_only_through_event_bus() {
    let (config_a, discovery_a) = peerfinding_endpoint_config([21; 32], "8.8.8.8:443");
    let (config_b, discovery_b) = peerfinding_endpoint_config([22; 32], "8.8.4.4:443");
    let endpoint_a = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config_a)
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind peerfinder publisher"),
    );
    let endpoint_b = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(config_b)
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind peerfinder consumer"),
    );
    let publisher =
        FipsPeerfinder::new(Arc::clone(&endpoint_a), &discovery_a).expect("external publisher");
    let consumer =
        FipsPeerfinder::new(Arc::clone(&endpoint_b), &discovery_b).expect("external consumer");
    let bus = InMemoryEventBus::new();

    let publish_report = publisher
        .publish_local(&bus)
        .await
        .expect("publish through selected bus")
        .expect("local endpoint is advert eligible");
    assert!(publish_report.accepted);
    let refreshed = consumer
        .refresh(&bus)
        .await
        .expect("query through selected bus");

    assert_eq!(
        refreshed,
        FipsPeerfindingRefresh {
            received: 1,
            accepted: 1,
        }
    );
    assert!(
        endpoint_a
            .relay_statuses()
            .await
            .expect("publisher relays")
            .is_empty()
    );
    assert!(
        endpoint_b
            .relay_statuses()
            .await
            .expect("consumer relays")
            .is_empty()
    );
    endpoint_a.shutdown().await.expect("publisher shutdown");
    endpoint_b.shutdown().await.expect("consumer shutdown");
}

#[tokio::test]
async fn peerfinder_rejects_embedded_relay_peerfinding_mode() {
    let endpoint = Arc::new(
        Box::pin(FipsEndpoint::builder().without_system_tun().bind())
            .await
            .expect("bind endpoint"),
    );
    let config = NostrDiscoveryConfig {
        enabled: true,
        peerfinding_source: NostrPeerfindingSource::Relays,
        ..Default::default()
    };

    assert!(FipsPeerfinder::new(Arc::clone(&endpoint), &config).is_err());
    endpoint.shutdown().await.expect("endpoint shutdown");
}

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

    exercise_default_reputation(&endpoint_a, endpoint_b.npub()).await;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connected_fips_peers_subscribe_and_receive_cached_update_announcements() {
    let network_id = format!("nostr-pubsub-fips-updates-{}", std::process::id());
    let (endpoint_a, endpoint_b, client_a, client_b) = connected_update_clients(&network_id).await;

    let release_keys = Keys::generate();
    let tree_name = "iris-chat-releases";
    let announcement = VerifiedEvent::try_from(
        EventBuilder::new(Kind::Custom(30_064), "")
            .tags([
                Tag::identifier(tree_name),
                Tag::custom(
                    TagKind::SingleLetter(SingleLetterTag::lowercase(Alphabet::L)),
                    ["hashtree"],
                ),
                Tag::custom(TagKind::Custom("hash".into()), ["11".repeat(32)]),
            ])
            .sign_with_keys(&release_keys)
            .expect("sign update announcement"),
    )
    .expect("verify update announcement");
    client_b
        .publish(
            announcement.clone(),
            nostr_pubsub::EventSource::local_index("release"),
        )
        .await
        .expect("publish update announcement");

    let filter = Filter::new()
        .kind(Kind::Custom(30_064))
        .author(release_keys.public_key())
        .custom_tag(
            SingleLetterTag::lowercase(Alphabet::D),
            tree_name.to_string(),
        );
    let mut subscription = client_a
        .subscribe(vec![filter.clone()])
        .await
        .expect("subscribe to release tree");
    let received = timeout(Duration::from_secs(2), subscription.recv())
        .await
        .expect("update announcement timeout")
        .expect("subscription remains open");

    assert_eq!(received.event, announcement);
    assert_eq!(received.source.kind, EventSourceKind::FipsEndpoint);
    assert_eq!(received.source.id.as_str(), endpoint_b.npub());

    timeout(Duration::from_secs(2), async {
        loop {
            if client_b
                .peer_subscription_count()
                .expect("peer subscription count")
                == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("publisher observes peer subscription");

    let retained_filter = filter.clone().limit(FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS);
    assert_peer_subscription_snapshot(&client_b, subscription.id(), &retained_filter);

    let next_announcement = VerifiedEvent::try_from(
        EventBuilder::new(Kind::Custom(30_064), "")
            .tags([
                Tag::identifier(tree_name),
                Tag::custom(
                    TagKind::SingleLetter(SingleLetterTag::lowercase(Alphabet::L)),
                    ["hashtree"],
                ),
                Tag::custom(TagKind::Custom("hash".into()), ["22".repeat(32)]),
            ])
            .sign_with_keys(&release_keys)
            .expect("sign next update announcement"),
    )
    .expect("verify next update announcement");
    client_b
        .publish(
            next_announcement.clone(),
            nostr_pubsub::EventSource::local_index("release"),
        )
        .await
        .expect("publish next update announcement");
    let received = timeout(Duration::from_secs(2), subscription.recv())
        .await
        .expect("next update announcement timeout")
        .expect("subscription remains open");
    assert_eq!(received.event, next_announcement);

    subscription.close();
    wait_for_no_peer_subscriptions(&client_b).await;
    client_a.shutdown().await;
    client_b.shutdown().await;
    endpoint_a.shutdown().await.expect("shutdown endpoint A");
    endpoint_b.shutdown().await.expect("shutdown endpoint B");
    unregister_sim_network(&network_id);
}

fn assert_peer_subscription_snapshot(
    client: &FipsPubsubClient,
    subscription_id: &nostr_pubsub::SubscriptionId,
    filter: &Filter,
) {
    let encoded_req_bytes = FipsPubsubWireCodec::new(FIPS_NOSTR_PUBSUB_MAX_DATAGRAM_BYTES)
        .expect("FIPS codec")
        .encode_frame(&FipsPubsubWireMessage::req(
            subscription_id.clone(),
            vec![filter.clone()],
        ))
        .expect("encode canonical REQ")
        .len();
    assert_eq!(
        client
            .peer_subscription_snapshot()
            .expect("retained peer subscriptions"),
        PubsubPeerSubscriptionSnapshot {
            peer_count: 1,
            subscription_count: 1,
            filter_count: 1,
            encoded_filter_bytes: filter.as_json().len(),
            encoded_req_bytes,
        }
    );
}

async fn wait_for_no_peer_subscriptions(client: &FipsPubsubClient) {
    timeout(Duration::from_secs(2), async {
        loop {
            if client
                .peer_subscription_snapshot()
                .expect("retained peer subscriptions")
                == PubsubPeerSubscriptionSnapshot::default()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("publisher removes closed peer subscription");
}

#[tokio::test]
async fn reputation_facade_uses_explicit_virtual_time() {
    let network_id = format!("nostr-pubsub-fips-virtual-time-{}", std::process::id());
    register_sim_network(&network_id, SimNetwork::new(7370));
    let subject = Keys::parse(&hex::encode([8; 32])).expect("subject key");
    let subject_npub = subject.public_key().to_bech32().expect("subject npub");
    let endpoint = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(endpoint_config(
                    &network_id,
                    "virtual-time",
                    [7; 32],
                    subject_npub.clone(),
                    "absent-peer",
                ))
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind virtual-time endpoint"),
    );

    exercise_explicit_time_reputation(&endpoint, &subject_npub, [7; 32]);

    endpoint.shutdown().await.expect("shutdown endpoint");
    unregister_sim_network(&network_id);
}

async fn connected_update_clients(
    network_id: &str,
) -> (
    Arc<FipsEndpoint>,
    Arc<FipsEndpoint>,
    FipsPubsubClient,
    FipsPubsubClient,
) {
    register_sim_network(network_id, SimNetwork::new(7369));
    let identity_a = Identity::from_secret_bytes(&[3; 32]).expect("identity A");
    let identity_b = Identity::from_secret_bytes(&[4; 32]).expect("identity B");
    let endpoint_a = Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(endpoint_config(
                    network_id,
                    "updates-a",
                    [3; 32],
                    identity_b.npub(),
                    "updates-b",
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
                    network_id,
                    "updates-b",
                    [4; 32],
                    identity_a.npub(),
                    "updates-a",
                ))
                .without_system_tun()
                .bind(),
        )
        .await
        .expect("bind endpoint B"),
    );
    wait_for_connected_peer(&endpoint_a, endpoint_b.npub()).await;
    wait_for_connected_peer(&endpoint_b, endpoint_a.npub()).await;
    let client_a = FipsPubsubClient::start_for_transport(
        Arc::clone(&endpoint_a),
        FipsPubsubClientOptions::default(),
        "sim",
    )
    .await
    .expect("start subscriber");
    let client_b = FipsPubsubClient::start_for_transport(
        Arc::clone(&endpoint_b),
        FipsPubsubClientOptions::default(),
        "sim",
    )
    .await
    .expect("start publisher");
    (endpoint_a, endpoint_b, client_a, client_b)
}

async fn exercise_default_reputation(endpoint: &Arc<FipsEndpoint>, peer_npub: &str) {
    let reputation = FipsPeerReputation::new(
        Arc::clone(endpoint),
        std::iter::empty(),
        FipsPeerReputationOptions::default(),
    )
    .expect("start default FIPS peer reputation");
    assert!(
        reputation
            .peer_policy()
            .select_mesh_peer(peer_npub)
            .expect("unknown peer policy")
            .expect("unknown peer remains eligible")
            .is_unknown()
    );
    reputation
        .publication_candidates(1_000)
        .await
        .expect("snapshot signed FIPS ratings");

    let mut policy = FipsPubsubPolicy::new(
        Arc::clone(endpoint),
        std::iter::empty(),
        FipsPubsubPolicyOptions::default(),
    )
    .expect("start pubsub policy");
    let event = EventBuilder::text_note("ordinary event")
        .sign_with_keys(&Keys::generate())
        .expect("sign ordinary event");
    assert!(
        !policy
            .observe_event(&event)
            .expect("observe ordinary event")
    );
    assert!(!matches!(
        policy
            .check_event(&event, &EventSource::relay("wss://bootstrap.example"))
            .await
            .expect("check relay event author"),
        PolicyDecision::Drop { .. }
    ));
    assert!(policy.maintenance_events(1_000).await.unwrap().is_empty());
}

fn exercise_explicit_time_reputation(
    endpoint: &Arc<FipsEndpoint>,
    peer_npub: &str,
    secret: [u8; 32],
) {
    const NOW_SECS: u64 = 1_000_000;
    let signer = Keys::parse(&hex::encode(secret)).expect("FIPS identity signing key");
    assert_eq!(
        signer.public_key().to_bech32().expect("FIPS identity npub"),
        endpoint.npub()
    );
    let event = rating_event(&signer, peer_npub, 0, NOW_SECS);

    let stored = [event.clone()];
    let replayed = FipsPeerReputation::new_at(
        Arc::clone(endpoint),
        &stored,
        FipsPeerReputationOptions::default(),
        NOW_SECS,
    )
    .expect("replay at virtual time");
    assert_eq!(
        replayed
            .peer_policy()
            .select_mesh_peer(peer_npub)
            .expect("replayed peer policy"),
        None
    );

    let mut reputation = FipsPeerReputation::new_at(
        Arc::clone(endpoint),
        std::iter::empty(),
        FipsPeerReputationOptions::default(),
        NOW_SECS,
    )
    .expect("start reputation at virtual time");
    assert!(
        reputation
            .ingest_event_at(&event, NOW_SECS)
            .expect("ingest at virtual time")
    );

    let mut policy = FipsPubsubPolicy::new_at(
        Arc::clone(endpoint),
        std::iter::empty(),
        FipsPubsubPolicyOptions::default(),
        NOW_SECS,
    )
    .expect("start policy at virtual time");
    assert!(
        policy
            .observe_event_at(&event, NOW_SECS)
            .expect("observe at virtual time")
    );

    let mut completion = FipsPubsubPolicy::new_at(
        Arc::clone(endpoint),
        std::iter::empty(),
        FipsPubsubPolicyOptions::default(),
        NOW_SECS,
    )
    .expect("start completion policy at virtual time");
    completion
        .complete_maintenance_event(&event, true, NOW_SECS * 1_000)
        .expect("complete maintenance at virtual time");
    assert_eq!(
        completion
            .peer_policy()
            .select_mesh_peer(peer_npub)
            .expect("completed peer policy"),
        None
    );
}

fn rating_event(signer: &Keys, subject: &str, value: i64, created_at: u64) -> Event {
    let mut rating = Rating::new(signer.public_key().to_hex(), subject, value, 0, 100);
    rating.scope = Some("fips.peer".to_string());
    rating.created_at = created_at;
    rating.sample_count = Some(3);
    let encoded = rating.to_event(signer).expect("encode peer rating");
    EventBuilder::new(encoded.kind, encoded.content)
        .tags(encoded.tags)
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(signer)
        .expect("sign peer rating at virtual time")
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

fn peerfinding_endpoint_config(
    secret: [u8; 32],
    external_addr: &str,
) -> (Config, NostrDiscoveryConfig) {
    let mut config = Config::new();
    config.node.identity = IdentityConfig {
        nsec: Some(hex::encode(secret)),
        persistent: false,
    };
    config.node.discovery.nostr.enabled = true;
    config.node.discovery.nostr.advertise = true;
    config.node.discovery.nostr.peerfinding_source = NostrPeerfindingSource::External;
    config.node.discovery.nostr.advert_relays.clear();
    config.node.discovery.nostr.app = "fips-pubsub-peerfinding-test".to_string();
    config.transports.tcp = TransportInstances::Single(TcpConfig {
        bind_addr: Some("127.0.0.1:0".to_string()),
        advertise_on_nostr: Some(true),
        external_addr: Some(external_addr.to_string()),
        ..Default::default()
    });
    let discovery = config.node.discovery.nostr.clone();
    (config, discovery)
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
