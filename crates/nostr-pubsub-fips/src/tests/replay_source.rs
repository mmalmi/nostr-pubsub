use super::routed::{signed_note, udp_endpoint};
use super::*;

#[test]
fn replay_budget_combines_filter_limits_without_overflow() {
    use crate::replay_source::query_limit;

    assert_eq!(query_limit(&[], 8), 8);
    assert_eq!(query_limit(&[Filter::new().limit(0)], 8), 0);
    assert_eq!(
        query_limit(&[Filter::new().limit(1), Filter::new().limit(1)], 8),
        2
    );
    assert_eq!(query_limit(&[Filter::new().limit(1), Filter::new()], 8), 8);
    assert_eq!(
        query_limit(
            &[Filter::new().limit(usize::MAX), Filter::new().limit(1)],
            8
        ),
        8
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_filter_limits_replay_hot_and_durable_events_together() {
    let a = udp_endpoint([107; 32], Vec::new()).await;
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let store = Arc::new(InMemoryEventBus::new());
    let events = (0..4)
        .map(|i| signed_note(&format!("independent historical filter {i}")))
        .collect::<Vec<_>>();
    for (index, event) in events.iter().enumerate() {
        store
            .publish(event.clone(), EventSource::local_index("history"))
            .await
            .unwrap();
        if index < 2 {
            publisher
                .publish(event.clone(), EventSource::local_index("history"))
                .await
                .unwrap();
        }
    }
    publisher.set_replay_source(Some(store)).unwrap();
    let addr = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint([108; 32], vec![PeerConfig::new(a.npub(), "udp", &addr)]).await;
    let reader = FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let filters = events
        .iter()
        .map(|event| Filter::new().id(event.as_event().id).limit(1))
        .collect();
    let mut subscription = reader.subscribe(filters).await.unwrap();
    let mut actual = HashSet::new();
    let replay = timeout(Duration::from_secs(5), async {
        while actual.len() < events.len() {
            actual.insert(subscription.recv().await.unwrap().event.as_event().id);
        }
    })
    .await;
    drop(subscription);
    reader.shutdown().await;
    publisher.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    assert!(
        replay.is_ok(),
        "received {}/{} independently requested events",
        actual.len(),
        events.len()
    );
    assert_eq!(
        actual,
        events.iter().map(|event| event.as_event().id).collect()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_provider_serves_external_history_beyond_the_hot_window() {
    let a = udp_endpoint([101; 32], Vec::new()).await;
    let store = Arc::new(InMemoryEventBus::new());
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let events = (0..24)
        .map(|i| signed_note(&format!("durable {i}")))
        .collect::<Vec<_>>();
    for event in &events {
        store
            .publish(event.clone(), EventSource::local_index("history"))
            .await
            .unwrap();
        publisher
            .publish(event.clone(), EventSource::local_index("history"))
            .await
            .unwrap();
    }
    assert!(
        publisher
            .inner
            .recent_events
            .lock()
            .unwrap()
            .event(&events[0].as_event().id.to_hex())
            .is_none()
    );
    publisher.shutdown().await;
    a.shutdown().await.unwrap();
    let a = udp_endpoint([101; 32], Vec::new()).await;
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    publisher.set_replay_source(Some(store)).unwrap();
    let addr = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint([102; 32], vec![PeerConfig::new(a.npub(), "udp", &addr)]).await;
    let reader = Arc::new(
        FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
            .await
            .unwrap(),
    );
    let mut held = reader
        .subscribe(vec![Filter::new().id(events[0].as_event().id)])
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), held.recv())
            .await
            .unwrap()
            .unwrap()
            .event,
        events[0]
    );

    // Repeat once the reader has evicted the first payloads but still remembers
    // their IDs: a fresh check must obtain a new authenticated observation.
    for (page_number, page) in events.chunks(8).chain(events.chunks(8)).enumerate() {
        let expected = page.iter().map(|e| e.as_event().id).collect::<HashSet<_>>();
        let (sender, mut received) = mpsc::unbounded_channel();
        let subscription = reader
            .fresh_subscriber()
            .subscribe(
                vec![Filter::new().ids(expected.iter().copied()).limit(8)],
                Arc::new(move |event| {
                    let _ = sender.send(event);
                }),
            )
            .await
            .unwrap();
        let mut actual = HashSet::new();
        timeout(Duration::from_secs(5), async {
            while actual.len() < expected.len() {
                let delivered = received.recv().await.unwrap();
                assert_eq!(delivered.source, EventSource::fips_endpoint(a.npub()));
                actual.insert(delivered.event.as_event().id);
            }
        })
        .await
        .unwrap_or_else(|_| panic!("late peer replay page {page_number}: received {}/{} events; reader={:?}, publisher={:?}", actual.len(), expected.len(), reader.delivery_snapshot(), publisher.delivery_snapshot()));
        assert_eq!(actual, expected);
        subscription.close().await.unwrap();
    }
    // Durable queries do not enlarge or repopulate the transport payload cache.
    assert!(
        publisher.inner.recent_events.lock().unwrap().entries.len()
            <= FIPS_NOSTR_PUBSUB_MAX_REPLAY_EVENTS
    );
    assert!(
        timeout(Duration::from_millis(50), held.recv())
            .await
            .is_err()
    );
    drop(held);
    reader.shutdown_shared().await;
    publisher.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

struct PausedStore {
    event: VerifiedEvent,
    started: mpsc::UnboundedSender<Vec<Filter>>,
    gate: tokio::sync::Semaphore,
    active: AtomicUsize,
}

struct ActiveQuery<'a>(&'a AtomicUsize);
impl Drop for ActiveQuery<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl EventBus for PausedStore {
    async fn publish(&self, _: VerifiedEvent, _: EventSource) -> Result<PublishReport> {
        unreachable!("replay source is read-only")
    }

    async fn query(&self, filters: Vec<Filter>, _: QueryOptions) -> Result<QueryReport> {
        if PubsubPeerInterest::from_filters(&filters, &self.event) != PubsubPeerInterest::Subscribed
        {
            return Ok(QueryReport::default());
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveQuery(&self.active);
        let _ = self.started.send(filters);
        let _permit = self.gate.acquire().await.unwrap();
        Ok(QueryReport {
            events: vec![QueryEvent {
                event: self.event.clone(),
                source: EventSource::local_index("history"),
                priority: 0,
            }],
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_history_never_blocks_live_delivery_and_shutdown_cancels_queries() {
    let a = udp_endpoint([103; 32], Vec::new()).await;
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let (started, mut observed) = mpsc::unbounded_channel();
    let store = Arc::new(PausedStore {
        event: signed_note("historical"),
        started,
        gate: tokio::sync::Semaphore::new(0),
        active: AtomicUsize::new(0),
    });
    publisher.set_replay_source(Some(store.clone())).unwrap();
    let hot = signed_note("already cached before subscription");
    publisher
        .publish(hot.clone(), EventSource::local_index("live"))
        .await
        .unwrap();
    let addr = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint([104; 32], vec![PeerConfig::new(a.npub(), "udp", &addr)]).await;
    let reader = FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let mut subscription = reader
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    timeout(Duration::from_secs(5), observed.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), subscription.recv())
            .await
            .expect("slow history must not suppress the hot replay window")
            .unwrap()
            .event,
        hot
    );
    let live = signed_note("live while disk is busy");
    publisher
        .publish(live.clone(), EventSource::local_index("live"))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), subscription.recv())
            .await
            .unwrap()
            .unwrap()
            .event,
        live
    );
    assert!(store.active.load(Ordering::SeqCst) <= 4);
    let pending = reader
        .subscribe(vec![Filter::new().id(store.event.as_event().id)])
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        while let Some(filters) = observed.recv().await {
            if filters.iter().any(|filter| {
                filter
                    .ids
                    .as_ref()
                    .is_some_and(|ids| ids.contains(&store.event.as_event().id))
            }) {
                return;
            }
        }
        panic!("history query channel closed");
    })
    .await
    .unwrap();
    assert!(store.active.load(Ordering::SeqCst) > 0);
    publisher.shutdown_shared().await;
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert!(publisher.set_replay_source(None).is_err());
    drop(subscription);
    drop(pending);
    reader.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_completion_respects_closed_subscriptions_and_detached_sources() {
    let a = udp_endpoint([105; 32], Vec::new()).await;
    let publisher = FipsPubsubClient::start(a.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let (started, mut observed) = mpsc::unbounded_channel();
    let store = Arc::new(PausedStore {
        event: signed_note("must remain private after detach"),
        started,
        gate: tokio::sync::Semaphore::new(0),
        active: AtomicUsize::new(0),
    });
    publisher.set_replay_source(Some(store.clone())).unwrap();
    let addr = a.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let b = udp_endpoint([106; 32], vec![PeerConfig::new(a.npub(), "udp", &addr)]).await;
    let reader = FipsPubsubClient::start(b.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    for detach in [false, true] {
        let subscription = reader
            .subscribe(vec![Filter::new().kind(Kind::TextNote)])
            .await
            .unwrap();
        timeout(Duration::from_secs(5), observed.recv())
            .await
            .unwrap()
            .unwrap();
        if detach {
            publisher.set_replay_source(None).unwrap();
        } else {
            subscription.close();
            wait_for_default_peer_subscription(&publisher).await;
        }
        let before = reader.inner.inv_frames_received.load(Ordering::Relaxed);
        store.gate.add_permits(1);
        timeout(Duration::from_secs(1), async {
            while store.active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            reader.inner.inv_frames_received.load(Ordering::Relaxed),
            before
        );
        store.gate.forget_permits(1);
    }
    reader.shutdown().await;
    publisher.shutdown().await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
