use fips_core::{
    FipsEndpoint,
    config::{IdentityConfig, RoutingMode},
};
use nostr::{Filter, Keys, Kind, Timestamp, nips::nip19::ToBech32};
use nostr_pubsub::{EventBus, QueryOptions, QueryReport};
use nostr_pubsub_fips::{FipsPubsubClient, FipsPubsubClientOptions};
use std::{sync::Arc, time::Duration};
type BoundFipsEndpoint = Arc<FipsEndpoint>;
struct Config {
    addr: String,
    scope: String,
}

use nostr::{Event, EventBuilder};
use nostr_pubsub::{EventSource, InMemoryEventBus, PublishReport, VerifiedEvent};
use std::sync::atomic::{AtomicUsize, Ordering};

struct History {
    bus: InMemoryEventBus,
    witness: Event,
    requests: AtomicUsize,
    completed: AtomicUsize,
    delay: Duration,
}

#[async_trait::async_trait]
impl EventBus for History {
    async fn publish(
        &self,
        event: VerifiedEvent,
        source: EventSource,
    ) -> nostr_pubsub::Result<PublishReport> {
        self.bus.publish(event, source).await
    }
    async fn query(
        &self,
        filters: Vec<Filter>,
        options: QueryOptions,
    ) -> nostr_pubsub::Result<QueryReport> {
        if filters.iter().any(|filter| {
            filter.match_event(&self.witness, nostr::filter::MatchEventOptions::default())
        }) {
            self.requests.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
        }
        let report = self.bus.query(filters, options).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        report
    }
}

async fn peer(
    config: &Config,
    events: &[Event],
    delay: Duration,
) -> (BoundFipsEndpoint, FipsPubsubClient, Arc<History>) {
    let endpoint = endpoint(config).await;
    let client = FipsPubsubClient::start(
        endpoint.clone(),
        FipsPubsubClientOptions {
            max_replay_events: 128,
            max_inbound_routed_peers: 8,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let history = Arc::new(History {
        bus: InMemoryEventBus::new(),
        witness: events[0].clone(),
        requests: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
        delay,
    });
    for event in events {
        history
            .bus
            .publish(
                VerifiedEvent::try_from(event.clone()).unwrap(),
                EventSource::local_index("fixture"),
            )
            .await
            .unwrap();
    }
    client.set_replay_source(Some(history.clone())).unwrap();
    (endpoint, client, history)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovers_multiple_fips_peers_without_a_provider_roster() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let config = Config {
        addr: addr.to_string(),
        scope: format!("archive-repro-{}", addr.port()),
    };
    let keys = Keys::generate();
    let event = |label: &str| {
        EventBuilder::new(Kind::TextNote, label)
            .custom_created_at(Timestamp::from_secs(20))
            .sign_with_keys(&keys)
            .unwrap()
    };
    let shared = event("same signed ID at both peers");
    let first_only = event("first peer only");
    let (first_endpoint, first, first_history) = peer(
        &config,
        &[shared.clone(), first_only.clone()],
        Duration::from_millis(300),
    )
    .await;
    let second_only = event("second peer only");
    let (other_endpoint, other, other_history) = peer(
        &config,
        &[shared.clone(), second_only.clone()],
        Duration::ZERO,
    )
    .await;
    let runtime_endpoint = endpoint(&config).await;
    let runtime_client = FipsPubsubClient::start(
        runtime_endpoint.clone(),
        FipsPubsubClientOptions {
            max_connected_peers: 16,
            max_inbound_routed_peers: 4,
            query_timeout: Duration::from_secs(2),
            max_active_subscriptions: 4,
            max_replay_events: 128,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let result = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let adverts = runtime_endpoint.local_instance_advertisements().unwrap();
                let visible = [
                    &first_endpoint.npub().to_string(),
                    &other_endpoint.npub().to_string(),
                ]
                .iter()
                .all(|npub| {
                    adverts.iter().any(|advert| {
                        &advert.npub == *npub
                            && advert
                                .capability(nostr_pubsub_fips::FIPS_NOSTR_PUBSUB_CAPABILITY)
                                .is_some()
                    })
                });
                if visible {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        let filter = Filter::new()
            .author(keys.public_key())
            .kind(Kind::TextNote)
            .limit(128);
        let report = runtime_client
            .query(vec![filter], QueryOptions { limit: Some(128) })
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(report)
    }
    .await;
    runtime_client.shutdown_shared().await;
    runtime_endpoint.shutdown().await.unwrap();
    first.shutdown_shared().await;
    other.shutdown_shared().await;
    first_endpoint.shutdown().await.unwrap();
    other_endpoint.shutdown().await.unwrap();
    let report = result.unwrap();
    assert!(first_history.requests.load(Ordering::SeqCst) > 0);
    assert!(other_history.requests.load(Ordering::SeqCst) > 0);
    assert_eq!(
        report.events.len(),
        3,
        "both slow and fast peers must contribute distinct events"
    );
    for expected in [shared.id, first_only.id, second_only.id] {
        assert!(
            report
                .events
                .iter()
                .any(|entry| entry.event.as_event().id == expected)
        );
    }
}

async fn endpoint(config: &Config) -> Arc<FipsEndpoint> {
    let mut native = fips_core::Config::new();
    native.node.identity = IdentityConfig {
        nsec: Some(Keys::generate().secret_key().to_bech32().unwrap()),
        persistent: false,
    };
    native.node.routing.mode = RoutingMode::ReplyLearned;
    native.node.limits.max_peers = 8;
    native.node.limits.max_links = 16;
    native.node.limits.max_connections = 16;
    native.node.limits.max_pending_inbound = 32;
    native.node.control.enabled = false;
    native.tun.enabled = false;
    native.dns.enabled = false;
    native.node.system_files_enabled = false;
    native.node.discovery.lan.enabled = false;
    native.node.discovery.nostr.enabled = false;
    native.node.discovery.nostr.advertise = false;
    native.node.discovery.local.rendezvous_addr = config.addr.parse().unwrap();
    Arc::new(
        Box::pin(
            FipsEndpoint::builder()
                .config(native)
                .discovery_scope(config.scope.clone())
                .without_system_tun()
                .local_rendezvous()
                .bind(),
        )
        .await
        .unwrap(),
    )
}
