use super::routed::{signed_note, udp_endpoint};
use super::*;

async fn fixture() -> (Arc<FipsEndpoint>, FipsPubsubClient, PeerIdentity) {
    let endpoint = udp_endpoint([125; 32], Vec::new()).await;
    let client = FipsPubsubClient::start(
        endpoint.clone(),
        FipsPubsubClientOptions {
            max_replay_events: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let peer =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[126; 32]).unwrap().npub()).unwrap();
    (endpoint, client, peer)
}

fn attach(client: &FipsPubsubClient, subscription: &FipsPubsubSubscription, peer: PeerIdentity) {
    client
        .inner
        .lock_subscriptions()
        .unwrap()
        .get_mut(&subscription.key)
        .unwrap()
        .peers
        .insert(peer.npub());
}

async fn deliver(
    client: &FipsPubsubClient,
    peer: PeerIdentity,
    id: &SubscriptionId,
    event: &VerifiedEvent,
) {
    let frame = client
        .inner
        .codec
        .encode_frame(&FipsPubsubWireMessage::deliver(id.clone(), event.clone()))
        .unwrap();
    client.inner.handle_frame(peer, &frame).await;
}

struct BlockedCallback {
    handler: NostrEventHandler,
    started: oneshot::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
    delivered: mpsc::UnboundedReceiver<QueryEvent>,
}

fn blocked_callback(yield_after_first: bool) -> BlockedCallback {
    let (started_tx, started) = oneshot::channel();
    let start = Mutex::new(Some(started_tx));
    let (release, release_rx) = std::sync::mpsc::channel();
    let resume = Mutex::new(release_rx);
    let (deliveries, delivered) = mpsc::unbounded_channel();
    let handler = Arc::new(move |event| {
        if let Some(started) = start.lock().unwrap().take() {
            started.send(()).unwrap();
            resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            if yield_after_first {
                // Force a scheduling boundary after this callback, so CLOSE
                // cancellation cannot hide inside one uninterrupted FIFO poll.
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                for _ in 0..128 {
                    let mut budget = std::pin::pin!(tokio::task::consume_budget());
                    let _ = std::future::Future::poll(budget.as_mut(), &mut context);
                }
            }
        }
        deliveries.send(event).unwrap();
    });
    BlockedCallback {
        handler,
        started,
        release,
        delivered,
    }
}

#[tokio::test]
async fn pending_delivery_deduplicates_and_precedes_a_racing_new_arrival() {
    let (endpoint, client, peer) = fixture().await;
    let mut subscription = client
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    attach(&client, &subscription, peer);
    let events = ["channel A", "pending B", "racing C"].map(signed_note);
    for event in &events[..2] {
        deliver(&client, peer, subscription.id(), event).await;
    }
    client.inner.reset_peer_epoch(&peer.npub());
    deliver(&client, peer, subscription.id(), &events[1]).await;

    // Precisely expose the boundary between taking A from the channel and
    // acquiring the refill mutex. A concurrent signed C must still follow B.
    assert_eq!(subscription.receiver.try_recv().unwrap().event, events[0]);
    deliver(&client, peer, subscription.id(), &events[2]).await;
    assert_eq!(subscription.try_recv().unwrap().event, events[1]);
    assert_eq!(subscription.recv().await.unwrap().event, events[2]);
    assert_eq!(
        subscription.delivery_status(),
        SubscriptionDeliveryStatus::Active
    );
    assert!(matches!(
        subscription.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    subscription.close();
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_fresh_inventory_keeps_peer_provenance_after_cache_eviction() {
    let (endpoint, client, peer) = fixture().await;
    let mut subscription = client
        .inner
        .subscribe(vec![Filter::new().kind(Kind::TextNote)], false)
        .await
        .unwrap();
    attach(&client, &subscription, peer);
    let first = signed_note("fresh channel A");
    let pending = signed_note("fresh advertised B");
    deliver(&client, peer, subscription.id(), &first).await;
    client
        .inner
        .remember_event(pending.clone(), EventSource::local_index("outbox"), 0)
        .unwrap();
    let inventory = client
        .inner
        .inventory_frame(vec![subscription.id().clone()], &pending, 2)
        .unwrap();
    client.inner.handle_frame(peer, &inventory).await;
    client.inner.reset_peer_epoch(&peer.npub());
    client.inner.handle_frame(peer, &inventory).await;
    client
        .inner
        .remember_event(
            signed_note("evicts cached B"),
            EventSource::local_index("outbox"),
            0,
        )
        .unwrap();
    assert!(
        client
            .inner
            .recent_events
            .lock()
            .unwrap()
            .event(&pending.as_event().id.to_hex())
            .is_none()
    );
    assert_eq!(subscription.try_recv().unwrap().event, first);
    let recovered = subscription.recv().await.unwrap();
    assert_eq!(recovered.event, pending);
    assert_eq!(recovered.source, EventSource::fips_endpoint(peer.npub()));
    assert_eq!(recovered.priority, SOURCE_PRIORITY_FIPS_ENDPOINT);
    assert_eq!(
        subscription.delivery_status(),
        SubscriptionDeliveryStatus::Active
    );
    assert!(matches!(
        subscription.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    subscription.close();
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
}

#[tokio::test]
async fn overflow_stops_only_the_lagged_wire_subscription_and_drains_admitted_bodies() {
    let (endpoint, client, peer) = fixture().await;
    let mut slow = client
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    let mut healthy = client
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    attach(&client, &slow, peer);
    attach(&client, &healthy, peer);
    let events = ["overflow A", "overflow B", "overflow C", "healthy D"].map(signed_note);
    for event in &events[..3] {
        deliver(&client, peer, slow.id(), event).await;
        assert_eq!(healthy.try_recv().unwrap().event, *event);
    }
    assert_eq!(slow.delivery_status(), SubscriptionDeliveryStatus::Lagged);
    assert!(
        !client
            .inner
            .lock_subscriptions()
            .unwrap()
            .contains_key(&slow.key)
    );
    assert_eq!(
        healthy.delivery_status(),
        SubscriptionDeliveryStatus::Active
    );
    deliver(&client, peer, healthy.id(), &events[3]).await;
    assert_eq!(healthy.try_recv().unwrap().event, events[3]);
    client.shutdown_shared().await;
    assert_eq!(slow.delivery_status(), SubscriptionDeliveryStatus::Lagged);
    assert_eq!(
        healthy.delivery_status(),
        SubscriptionDeliveryStatus::Closed
    );
    assert_eq!(slow.recv().await.unwrap().event, events[0]);
    assert_eq!(slow.try_recv().unwrap().event, events[1]);
    assert!(slow.recv().await.is_none());
    assert!(matches!(
        slow.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    let state = Arc::clone(&slow.delivery);
    slow.close();
    assert_eq!(state.status(), SubscriptionDeliveryStatus::Lagged);
    healthy.close();
    endpoint.shutdown().await.unwrap();
}

#[tokio::test]
async fn close_and_shutdown_keep_pending_bodies_until_the_receiver_drains() {
    for shutdown in [false, true] {
        let (endpoint, client, peer) = fixture().await;
        let mut subscription = client
            .subscribe(vec![Filter::new().kind(Kind::TextNote)])
            .await
            .unwrap();
        attach(&client, &subscription, peer);
        let events = ["closing A", "closing B"].map(signed_note);
        for event in &events {
            deliver(&client, peer, subscription.id(), event).await;
        }
        if shutdown {
            client.shutdown_shared().await;
        } else {
            client.inner.close_subscription(&subscription.key);
        }
        assert_eq!(
            subscription.delivery_status(),
            SubscriptionDeliveryStatus::Closed
        );
        for event in events {
            assert_eq!(subscription.recv().await.unwrap().event, event);
        }
        assert!(subscription.recv().await.is_none());
        subscription.close();
        client.shutdown().await;
        endpoint.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_adapter_reports_sticky_lag_and_drains_before_finishing() {
    let (endpoint, client, peer) = fixture().await;
    let subscription = client
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    attach(&client, &subscription, peer);
    let id = subscription.id().clone();
    let state = Arc::clone(&subscription.delivery);
    let mut callback = blocked_callback(false);
    let routed = forward_subscription(subscription, callback.handler);
    assert_eq!(
        routed.delivery_status(),
        Some(SubscriptionDeliveryStatus::Active)
    );
    let events = [
        "callback A",
        "callback B",
        "callback C",
        "callback overflow D",
    ]
    .map(signed_note);
    deliver(&client, peer, &id, &events[0]).await;
    timeout(Duration::from_secs(5), callback.started)
        .await
        .unwrap()
        .unwrap();
    for event in &events[1..] {
        deliver(&client, peer, &id, event).await;
    }
    assert_eq!(
        routed.delivery_status(),
        Some(SubscriptionDeliveryStatus::Lagged)
    );
    callback.release.send(()).unwrap();
    for event in &events[..3] {
        assert_eq!(
            timeout(Duration::from_secs(5), callback.delivered.recv())
                .await
                .unwrap()
                .unwrap()
                .event,
            *event
        );
    }
    assert!(
        timeout(Duration::from_secs(5), callback.delivered.recv())
            .await
            .unwrap()
            .is_none()
    );
    client.shutdown_shared().await;
    assert_eq!(
        routed.delivery_status(),
        Some(SubscriptionDeliveryStatus::Lagged)
    );
    routed.close().await.unwrap();
    assert_eq!(state.status(), SubscriptionDeliveryStatus::Lagged);
    endpoint.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_async_close_drains_bodies_after_lag_or_shutdown() {
    use std::future::poll_fn;
    use std::task::Poll;

    for overflow in [false, true] {
        let (endpoint, client, peer) = fixture().await;
        let subscription = client
            .subscribe(vec![Filter::new().kind(Kind::TextNote)])
            .await
            .unwrap();
        attach(&client, &subscription, peer);
        let id = subscription.id().clone();
        let state = Arc::clone(&subscription.delivery);
        let mut callback = blocked_callback(true);
        let routed = forward_subscription(subscription, callback.handler);
        let events = [
            "closing callback A",
            "closing callback B",
            "closing callback C",
        ]
        .map(signed_note);
        deliver(&client, peer, &id, &events[0]).await;
        timeout(Duration::from_secs(5), callback.started)
            .await
            .unwrap()
            .unwrap();
        for event in &events[1..] {
            deliver(&client, peer, &id, event).await;
        }
        let terminal = if overflow {
            deliver(
                &client,
                peer,
                &id,
                &signed_note("closing callback overflow D"),
            )
            .await;
            SubscriptionDeliveryStatus::Lagged
        } else {
            client.shutdown_shared().await;
            SubscriptionDeliveryStatus::Closed
        };
        assert_eq!(routed.delivery_status(), Some(terminal));
        let mut closing = routed.close();
        assert!(
            poll_fn(|context| Poll::Ready(closing.as_mut().poll(context)))
                .await
                .is_pending()
        );
        callback.release.send(()).unwrap();
        for event in events {
            assert_eq!(
                timeout(Duration::from_secs(5), callback.delivered.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .event,
                event
            );
        }
        timeout(Duration::from_secs(5), closing)
            .await
            .unwrap()
            .unwrap();
        assert!(callback.delivered.recv().await.is_none());
        assert_eq!(state.status(), terminal);
        client.shutdown().await;
        endpoint.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn callback_adapter_reports_closed_on_close_and_shutdown() {
    for shutdown in [false, true] {
        let (endpoint, client, _) = fixture().await;
        let subscription = client
            .subscribe(vec![Filter::new().kind(Kind::TextNote)])
            .await
            .unwrap();
        let state = Arc::clone(&subscription.delivery);
        let routed = forward_subscription(subscription, Arc::new(|_| {}));
        assert_eq!(
            routed.delivery_status(),
            Some(SubscriptionDeliveryStatus::Active)
        );
        if shutdown {
            client.shutdown_shared().await;
            assert_eq!(
                routed.delivery_status(),
                Some(SubscriptionDeliveryStatus::Closed)
            );
        }
        routed.close().await.unwrap();
        assert_eq!(state.status(), SubscriptionDeliveryStatus::Closed);
        client.shutdown().await;
        endpoint.shutdown().await.unwrap();
    }
}
