use std::collections::BTreeSet;

use super::routed::udp_endpoint;
use super::*;

fn drive_feed() -> Filter {
    Filter::new().kinds([Kind::Custom(7368), Kind::Custom(30078)])
}

fn roots(keys: &Keys, count: usize, label: &str) -> Vec<VerifiedEvent> {
    (0..count)
        .map(|index| {
            VerifiedEvent::try_from(
                EventBuilder::new(Kind::Custom(30078), format!("{label} {index}"))
                    .tags([Tag::identifier(format!("{label}-{index}"))])
                    .custom_created_at(Timestamp::from(1_700_000_000 + index as u64))
                    .sign_with_keys(keys)
                    .unwrap(),
            )
            .unwrap()
        })
        .collect()
}

async fn publish_all(client: &FipsPubsubClient, events: &[VerifiedEvent]) {
    for event in events {
        client
            .publish(event.clone(), EventSource::local_index("drive"))
            .await
            .unwrap();
    }
}

/// Every delivery until the feed is quiet for half a second.
async fn received(feed: &mut FipsPubsubSubscription, expected: usize) -> BTreeSet<EventId> {
    let mut ids = BTreeSet::new();
    while ids.len() < expected {
        let delivery = timeout(Duration::from_secs(10), feed.recv())
            .await
            .expect("feed delivery")
            .expect("open feed");
        ids.insert(delivery.event.as_event().id);
    }
    while let Ok(Some(delivery)) = timeout(Duration::from_millis(500), feed.recv()).await {
        ids.insert(delivery.event.as_event().id);
    }
    ids
}

fn ids(events: &[VerifiedEvent]) -> BTreeSet<EventId> {
    events.iter().map(|event| event.as_event().id).collect()
}

async fn linked_pair(
    seed: u8,
) -> (
    Arc<FipsEndpoint>,
    Arc<FipsEndpoint>,
    FipsPubsubClient,
    FipsPubsubClient,
) {
    let a = udp_endpoint([seed; 32], Vec::new()).await;
    let address = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint(
        [seed + 1; 32],
        vec![PeerConfig::new(a.npub(), "udp", &address)],
    )
    .await;
    let alice = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let bob = FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    // Each side holds the other's peer-finding subscription once linked.
    wait_for_peer_subscription_count(&alice, 1).await;
    wait_for_peer_subscription_count(&bob, 1).await;
    (a, b, alice, bob)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_feeds_reconcile_and_announce_only_the_gap_to_their_peers() {
    let (a, b, alice, bob) = linked_pair(141).await;
    let keys = Keys::generate();
    let shared = roots(&keys, 10, "shared");
    let only_bob = roots(&keys, 1, "bob");

    let mut alice_feed = alice
        .subscribe_feed(drive_feed(), Some(Vec::new()))
        .await
        .unwrap();
    let mut bob_feed = bob
        .subscribe_feed(drive_feed(), Some(Vec::new()))
        .await
        .unwrap();
    publish_all(&alice, &shared).await;
    publish_all(&bob, &shared[..7]).await;
    publish_all(&bob, &only_bob).await;
    assert_eq!(
        alice.peer_subscription_count().unwrap(),
        1,
        "no peer is admitted yet"
    );

    bob_feed
        .set_feed_peers(Some(vec![a.npub().to_string()]))
        .await
        .unwrap();
    assert_eq!(received(&mut bob_feed, 3).await, ids(&shared[7..]));

    alice_feed
        .set_feed_peers(Some(vec![b.npub().to_string()]))
        .await
        .unwrap();
    assert_eq!(received(&mut alice_feed, 1).await, ids(&only_bob));

    let live = roots(&keys, 1, "live");
    publish_all(&alice, &live).await;
    assert_eq!(received(&mut bob_feed, 1).await, ids(&live));

    alice_feed.set_feed_peers(Some(Vec::new())).await.unwrap();
    wait_for_peer_subscription_count(&bob, 1).await;
    assert_eq!(bob.delivery_snapshot().provider_cooldowns, 0);
    assert_eq!(alice.delivery_snapshot().provider_cooldowns, 0);

    drop((alice_feed, bob_feed));
    alice.shutdown().await;
    bob.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cold_feed_gets_the_latest_events_and_ordinary_requests_are_unchanged() {
    let (a, b, alice, bob) = linked_pair(151).await;
    let keys = Keys::generate();
    let history = roots(&keys, 12, "history");
    let _alice_feed = alice
        .subscribe_feed(drive_feed(), Some(Vec::new()))
        .await
        .unwrap();
    publish_all(&alice, &history).await;

    let mut bob_feed = bob
        .subscribe_feed(drive_feed(), Some(vec![a.npub().to_string()]))
        .await
        .unwrap();
    let latest = &history[history.len() - FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS..];
    assert_eq!(
        received(&mut bob_feed, latest.len()).await,
        ids(latest),
        "a cold feed request gets the bounded latest replay"
    );

    // An ordinary subscription keeps its own replay and is not fed by the
    // feed's window.
    let mut ordinary = bob.subscribe(vec![drive_feed()]).await.unwrap();
    let delivery = timeout(Duration::from_secs(10), ordinary.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(ids(&history).contains(&delivery.event.as_event().id));
    assert!(
        bob.subscribe_feed(Filter::new().kind(Kind::Custom(1059)), None)
            .await
            .is_err()
    );
    assert!(
        bob.subscribe_feed(drive_feed().limit(1), None)
            .await
            .is_err()
    );

    drop((bob_feed, ordinary));
    alice.shutdown().await;
    bob.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
