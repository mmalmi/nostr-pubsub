use super::routed::{signed_note, udp_endpoint};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_outbox_retry_restores_a_payload_evicted_before_peer_arrival() {
    let a = udp_endpoint([91; 32], Vec::new()).await;
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let events = (0..=FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS)
        .map(|i| signed_note(&format!("offline event {i}")))
        .collect::<Vec<_>>();
    for event in &events {
        publisher
            .publish(event.clone(), EventSource::local_index("durable-outbox"))
            .await
            .unwrap();
    }
    let addr = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint([92; 32], vec![PeerConfig::new(a.npub(), "udp", &addr)]).await;
    let receiver = FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let mut subscription = receiver
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    // The bounded live window only contains the last eight payloads.
    let mut delivered_ids = HashSet::new();
    timeout(Duration::from_secs(5), async {
        while delivered_ids.len() < FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS {
            delivered_ids.insert(subscription.recv().await.unwrap().event.as_event().id);
        }
    })
    .await
    .unwrap();
    assert!(!delivered_ids.contains(&events[0].as_event().id));
    // A durable application retries the exact original signed event. Seen-ID
    // dedup must not permanently discard a payload that was never delivered.
    publisher
        .publish(
            events[0].clone(),
            EventSource::local_index("durable-outbox"),
        )
        .await
        .unwrap();
    let delivery = timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("explicit local retry restores and advertises the evicted payload")
        .unwrap();
    assert_eq!(delivery.event, events[0]);
    assert_eq!(
        publisher.inner.recent_events.lock().unwrap().entries.len(),
        FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS
    );
    drop(subscription);
    publisher.shutdown().await;
    receiver.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_subscriber_reobserves_cached_events_and_stays_quiet_offline() {
    let a = udp_endpoint([93; 32], Vec::new()).await;
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let event = signed_note("retained signed announcement");
    publisher
        .publish(event.clone(), EventSource::local_index("release"))
        .await
        .unwrap();
    let addr = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint([94; 32], vec![PeerConfig::new(a.npub(), "udp", &addr)]).await;
    let receiver = Arc::new(
        FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
            .await
            .unwrap(),
    );
    let filter = Filter::new().kind(Kind::TextNote);
    let baseline = receiver.active_subscription_count().unwrap();
    let mut cached = receiver.subscribe(vec![filter.clone()]).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), cached.recv())
            .await
            .unwrap()
            .unwrap()
            .event,
        event
    );
    // Keep the ordinary app subscription alive: cached peer replies must not
    // get lost to its preexisting event-ID deduplication.
    // Each check uses the same transport and preexisting cache. A fresh REQ
    // must get a fresh peer response even when the signed event has not changed.
    for _ in 0..2 {
        let (sender, mut deliveries) = mpsc::unbounded_channel();
        let subscription = receiver
            .fresh_subscriber()
            .subscribe(
                vec![filter.clone()],
                Arc::new(move |incoming| {
                    let _ = sender.send(incoming);
                }),
            )
            .await
            .unwrap();
        let incoming = timeout(Duration::from_secs(5), deliveries.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(incoming.event, event);
        assert_eq!(incoming.source, EventSource::fips_endpoint(a.npub()));
        assert!(
            timeout(Duration::from_millis(50), deliveries.recv())
                .await
                .is_err()
        );
        subscription.close().await.unwrap();
        assert_eq!(receiver.active_subscription_count().unwrap(), baseline + 1);
        assert!(
            timeout(Duration::from_millis(50), cached.recv())
                .await
                .is_err()
        );
    }
    drop(cached);
    publisher.shutdown().await;
    a.shutdown().await.unwrap();
    // Ordinary subscription replay is unchanged, including the original source.
    let mut cached = receiver.subscribe(vec![filter.clone()]).await.unwrap();
    assert_eq!(cached.recv().await.unwrap().event, event);
    drop(cached);
    let (sender, mut deliveries) = mpsc::unbounded_channel();
    let subscription = receiver
        .fresh_subscriber()
        .subscribe(
            vec![filter],
            Arc::new(move |incoming| {
                let _ = sender.send(incoming);
            }),
        )
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(300), deliveries.recv())
            .await
            .is_err()
    );
    subscription.close().await.unwrap();
    assert_eq!(receiver.active_subscription_count().unwrap(), baseline);
    receiver.shutdown_shared().await;
    b.shutdown().await.unwrap();
}
