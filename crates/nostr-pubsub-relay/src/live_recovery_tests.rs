use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nostr_pubsub::{EventBus, NostrEventSubscriber, QueryEvent, QueryOptions};
use nostr_sdk::{
    Client, ClientMessage, ClientOptions, Event, EventBuilder, Filter, JsonUtil, Keys, Kind,
    RelayMessage, RelayPoolNotification, SubscriptionId, Timestamp, pool::RelayPoolOptions,
};
use tokio::net::TcpListener;
use tokio::sync::{Notify, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use super::{LiveReplay, RelayEventBus};

const DEADLINE: Duration = Duration::from_secs(3);

struct RelayFixture {
    url: String,
    send: mpsc::Sender<Vec<RelayMessage<'static>>>,
    requests: mpsc::Receiver<(SubscriptionId, Vec<Filter>)>,
    closes: mpsc::Receiver<SubscriptionId>,
    task: JoinHandle<()>,
}

impl RelayFixture {
    async fn start(events: Vec<Event>, manual_first_request: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test relay");
        let url = format!("ws://{}", listener.local_addr().expect("test address"));
        let (send, mut sends) = mpsc::channel::<Vec<RelayMessage<'static>>>(4);
        let (request_tx, requests) = mpsc::channel(32);
        let (close_tx, closes) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(stream).await.expect("accept websocket");
            let mut first = true;
            loop {
                let messages = tokio::select! {
                    messages = sends.recv() => {
                        let Some(messages) = messages else { return };
                        messages
                    }
                    frame = socket.next() => {
                        let Some(Ok(frame)) = frame else { return };
                        let (subscription_id, filters) = match ClientMessage::from_json(frame.into_data()) {
                            Ok(ClientMessage::Req { subscription_id, filters }) => (subscription_id, filters),
                            Ok(ClientMessage::Close(id)) => {
                                let _ = close_tx.send(id.into_owned()).await;
                                continue;
                            }
                            _ => continue,
                        };
                        let id = subscription_id.into_owned();
                        let _ = request_tx.send((id.clone(), filters.into_iter().map(std::borrow::Cow::into_owned).collect())).await;
                        let skip = first && manual_first_request;
                        first = false;
                        if skip { continue; }
                        events.iter().cloned().map(|event| RelayMessage::event(id.clone(), event))
                            .chain(std::iter::once(RelayMessage::eose(id.clone()))).collect()
                    }
                };
                for message in messages {
                    if socket
                        .send(Message::Text(message.as_json().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        Self {
            url,
            send,
            requests,
            closes,
            task,
        }
    }

    async fn bus(&self, notification_capacity: usize) -> RelayEventBus {
        let client =
            Client::builder()
                .opts(ClientOptions::new().pool(
                    RelayPoolOptions::default().notification_channel_size(notification_capacity),
                ))
                .build();
        let bus = RelayEventBus::with_client(client, [self.url.clone()], DEADLINE)
            .await
            .expect("relay provider");
        super::tests::wait_until_connected(&bus, &self.url).await;
        bus
    }

    async fn request(&mut self) -> SubscriptionId {
        self.request_with_filters().await.0
    }

    async fn request_with_filters(&mut self) -> (SubscriptionId, Vec<Filter>) {
        timeout(DEADLINE, self.requests.recv())
            .await
            .expect("REQ deadline")
            .expect("REQ")
    }
}

impl Drop for RelayFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn event(content: &str) -> Event {
    EventBuilder::text_note(content)
        .sign_with_keys(&Keys::generate())
        .expect("signed event")
}

#[derive(Default)]
struct HandlerGate {
    entered: Notify,
    released: Mutex<bool>,
    changed: Condvar,
}

impl HandlerGate {
    fn wait(&self) {
        self.entered.notify_one();
        let guard = self.released.lock().expect("gate lock");
        drop(
            self.changed
                .wait_while(guard, |released| !*released)
                .expect("gate wait"),
        );
    }

    fn release(&self) {
        *self.released.lock().expect("gate lock") = true;
        self.changed.notify_all();
    }
}

struct ReleaseOnDrop(Arc<HandlerGate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn notification_fence(receiver: &mut broadcast::Receiver<RelayPoolNotification>) {
    timeout(DEADLINE, async {
        loop {
            match receiver.recv().await {
                Ok(RelayPoolNotification::Message {
                    message: RelayMessage::Notice(message),
                    ..
                }) if message == "overflow fence" => break,
                Err(broadcast::error::RecvError::Closed) => panic!("notification stream closed"),
                _ => {}
            }
        }
    })
    .await
    .expect("SDK has processed the whole overflow burst");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_subscription_recovers_an_event_lost_from_the_sdk_notification_queue() {
    let first = event("blocks the live consumer");
    let lost = event("accepted by SDK while the live consumer is blocked");
    let first_id = first.id;
    let lost_id = lost.id;
    let mut relay = RelayFixture::start(vec![first.clone(), lost.clone()], true).await;
    let bus = relay.bus(8).await;
    let mut unrelated = RelayFixture::start(Vec::new(), false).await;
    bus.client()
        .add_relay(&unrelated.url)
        .await
        .expect("shared client's other relay");
    bus.client().connect().await;
    super::tests::wait_until_connected(&bus, &unrelated.url).await;
    let mut observer = bus.client().notifications();
    let gate = Arc::new(HandlerGate::default());
    let _release_on_failure = ReleaseOnDrop(Arc::clone(&gate));
    let handler_gate = Arc::clone(&gate);
    let (delivered_tx, mut delivered) = mpsc::channel(32);
    let filter = Filter::new()
        .ids([first_id, lost_id])
        .kind(Kind::TextNote)
        .since(Timestamp::from(1))
        .until(Timestamp::now())
        .limit(2);
    let subscription = bus
        .subscribe(
            vec![filter.clone()],
            Arc::new(move |delivery| {
                if delivery.event.as_event().id == first_id {
                    handler_gate.wait();
                }
                delivered_tx
                    .try_send(delivery.event.as_event().id)
                    .expect("bounded test output");
            }),
        )
        .await
        .expect("live subscription");
    let (id, filters) = relay.request_with_filters().await;
    assert_eq!(filters, vec![filter.clone()]);
    relay
        .send
        .send(vec![RelayMessage::event(id.clone(), first)])
        .await
        .expect("first event");
    timeout(DEADLINE, gate.entered.notified())
        .await
        .expect("handler is blocked");
    let mut burst = vec![RelayMessage::event(id.clone(), lost)];
    burst.extend((0..32).map(|n| RelayMessage::notice(format!("overflow filler {n}"))));
    burst.push(RelayMessage::notice("overflow fence"));
    relay.send.send(burst).await.expect("overflow burst");
    notification_fence(&mut observer).await;
    assert!(
        bus.client()
            .database()
            .event_by_id(&lost_id)
            .await
            .expect("database lookup")
            .is_none(),
        "the default SDK database does not retain event bodies"
    );
    gate.release();

    let recovered = timeout(DEADLINE, async {
        while let Some(id) = delivered.recv().await {
            if id == lost_id {
                return;
            }
        }
        panic!("live subscription ended");
    })
    .await;
    if recovered.is_ok() {
        let (replayed_id, filters) = relay.request_with_filters().await;
        assert_eq!(
            replayed_id, id,
            "replay does not accumulate SDK subscriptions"
        );
        assert_eq!(
            filters,
            vec![filter],
            "original time bounds, IDs and limit survive"
        );
        assert!(
            unrelated.requests.try_recv().is_err(),
            "replay stays on configured relays"
        );
    }
    subscription.close().await.expect("close subscription");
    bus.client().shutdown().await;
    assert!(
        recovered.is_ok(),
        "a live subscriber must recover the known notification gap"
    );
}

fn drain_event_notifications(
    receiver: &mut broadcast::Receiver<RelayPoolNotification>,
) -> (usize, usize) {
    let (mut events, mut messages) = (0, 0);
    loop {
        match receiver.try_recv() {
            Ok(RelayPoolNotification::Event { .. }) => events += 1,
            Ok(RelayPoolNotification::Message {
                message: RelayMessage::Event { .. },
                ..
            }) => messages += 1,
            Ok(_) => {}
            Err(broadcast::error::TryRecvError::Empty) => return (events, messages),
            Err(error) => panic!("unexpected notification loss in dedup control: {error}"),
        }
    }
}

#[tokio::test]
async fn refetch_recovers_bodies_through_messages_without_duplicate_sdk_event_notifications() {
    let expected = event("body retained at the relay, ID retained by the SDK");
    let relay = RelayFixture::start(vec![expected.clone()], false).await;
    let bus = relay.bus(128).await;
    let mut observer = bus.client().notifications();
    for replay in [false, true] {
        let report = bus
            .query(vec![Filter::new().id(expected.id)], QueryOptions::default())
            .await
            .expect("bounded explicit reconciliation");
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].event.as_event().id, expected.id);
        let (events, messages) = drain_event_notifications(&mut observer);
        assert_eq!(
            events,
            usize::from(!replay),
            "SDK Event notifications are ID-deduplicated"
        );
        assert_eq!(messages, 1, "Message::Event retains the replayed body");
    }
    assert!(
        bus.client()
            .database()
            .event_by_id(&expected.id)
            .await
            .expect("database lookup")
            .is_none()
    );
    bus.client().shutdown().await;
}

#[tokio::test]
async fn query_reconciliation_recovers_events_rejected_by_a_full_ingress_queue() {
    let first = event("fills the application ingress queue");
    let second = event("rejected by bounded application ingress");
    let mut relay = RelayFixture::start(vec![first.clone(), second.clone()], true).await;
    let bus = relay.bus(128).await;
    let (ingress_tx, mut ingress) = mpsc::channel::<QueryEvent>(1);
    let dropped = Arc::new(Notify::new());
    let rejected = Arc::clone(&dropped);
    let subscription = bus
        .subscribe(
            vec![Filter::new().kind(Kind::TextNote)],
            Arc::new(move |delivery| {
                // Same nonblocking, non-acknowledging boundary as VPN RelayProvider.
                if ingress_tx.try_send(delivery).is_err() {
                    rejected.notify_one();
                }
            }),
        )
        .await
        .expect("live subscription");
    let id = relay.request().await;
    relay
        .send
        .send(vec![
            RelayMessage::event(id.clone(), first.clone()),
            RelayMessage::event(id, second.clone()),
        ])
        .await
        .expect("two live events");
    timeout(DEADLINE, dropped.notified())
        .await
        .expect("second event rejected by full ingress");
    let admitted = ingress.recv().await.expect("first admitted event");
    assert_eq!(admitted.event.as_event().id, first.id);
    assert!(matches!(
        ingress.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    // A caller that refreshes from query results (such as Chat's updater) can
    // restore the missing body. Merely refetching and awaiting SDK Event cannot.
    let report = bus
        .query(
            vec![Filter::new().kind(Kind::TextNote)],
            QueryOptions::default(),
        )
        .await
        .expect("explicit reconciliation");
    assert!(
        report
            .events
            .iter()
            .any(|delivery| delivery.event.as_event().id == second.id)
    );
    subscription.close().await.expect("close subscription");
    bus.client().shutdown().await;
}

#[test]
fn repeated_gaps_coalesce_without_postponing_recovery_and_retries_are_rate_bounded() {
    let mut replay = LiveReplay::default();
    let mut now = Instant::now();
    replay.gap(now);
    let first_due = replay.due;
    for _ in 0..1000 {
        now += Duration::from_millis(1);
        replay.gap(now);
        assert_eq!(
            replay.due, first_due,
            "busy receivers cannot defer recovery forever"
        );
    }
    for _ in 0..100 {
        now = replay.due.expect("one pending attempt").max(now);
        let delay = replay.delay;
        replay.attempted(now, false);
        assert_eq!(replay.delay, (delay * 2).min(LiveReplay::MAX_DELAY));
        assert_eq!(replay.due, Some(now + replay.delay));
        replay.gap(now);
        assert_eq!(replay.due, Some(now + replay.delay));
    }
    assert_eq!(replay.delay, LiveReplay::MAX_DELAY);
    replay.attempted(now, true);
    assert!(
        replay.due.is_none(),
        "success without another gap stops replay"
    );
    replay.gap(now);
    assert_eq!(
        replay.due,
        Some(now + LiveReplay::MAX_DELAY),
        "successful setup alone does not reset pressure backoff"
    );
    replay.attempted(now, true);
    now += LiveReplay::MAX_DELAY * 2;
    replay.gap(now);
    assert_eq!(replay.due, Some(now + LiveReplay::MIN_DELAY));
}

async fn close_with_pending_recovery(wait_for_replay: bool) {
    let first = event("initial callback fence");
    let marker = event("callback fence after overflow");
    let first_id = first.id;
    let marker_id = marker.id;
    let mut relay = RelayFixture::start(vec![first.clone()], true).await;
    let bus = relay.bus(8).await;
    let mut observer = bus.client().notifications();
    let initial = Arc::new(HandlerGate::default());
    let closing = Arc::new(HandlerGate::default());
    let _release_initial = ReleaseOnDrop(Arc::clone(&initial));
    let _release_closing = ReleaseOnDrop(Arc::clone(&closing));
    let initial_gate = Arc::clone(&initial);
    let closing_gate = Arc::clone(&closing);
    let count = AtomicUsize::new(0);
    let subscription = bus
        .subscribe(
            vec![Filter::new().kind(Kind::TextNote)],
            Arc::new(move |delivery| {
                let id = delivery.event.as_event().id;
                if id == first_id {
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        initial_gate.wait();
                    } else if wait_for_replay {
                        closing_gate.wait();
                    }
                } else if id == marker_id && !wait_for_replay {
                    closing_gate.wait();
                }
            }),
        )
        .await
        .expect("subscription");
    let id = relay.request().await;
    relay
        .send
        .send(vec![RelayMessage::event(id.clone(), first)])
        .await
        .expect("first event");
    timeout(DEADLINE, initial.entered.notified())
        .await
        .expect("initial callback blocked");
    let mut burst = (0..32)
        .map(|n| RelayMessage::notice(format!("overflow {n}")))
        .collect::<Vec<_>>();
    burst.push(RelayMessage::event(id.clone(), marker));
    burst.push(RelayMessage::notice("overflow fence"));
    relay.send.send(burst).await.expect("overflow burst");
    notification_fence(&mut observer).await;
    initial.release();
    timeout(DEADLINE, closing.entered.notified())
        .await
        .expect("callback at cancellation boundary");
    if wait_for_replay {
        assert_eq!(relay.request().await, id);
        assert_eq!(
            timeout(DEADLINE, relay.closes.recv())
                .await
                .expect("replay CLOSE deadline")
                .expect("replay CLOSE"),
            id
        );
    }
    let started = Arc::new(Notify::new());
    let notify = Arc::clone(&started);
    let close = tokio::spawn(async move {
        notify.notify_one();
        subscription.close().await
    });
    timeout(DEADLINE, started.notified())
        .await
        .expect("close starts");
    closing.release();
    timeout(DEADLINE, close)
        .await
        .expect("close deadline")
        .expect("close task")
        .expect("close result");
    assert_eq!(
        timeout(DEADLINE, relay.closes.recv())
            .await
            .expect("final CLOSE deadline")
            .expect("final CLOSE"),
        id
    );
    assert!(
        timeout(LiveReplay::MIN_DELAY * 3, relay.requests.recv())
            .await
            .is_err(),
        "closed subscriptions must never be reinstalled"
    );
    assert!(bus.client().subscriptions().await.is_empty());
    bus.client().shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_during_replay_backoff_prevents_reinstall() {
    close_with_pending_recovery(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_during_replay_delivery_prevents_later_reinstall() {
    close_with_pending_recovery(true).await;
}

#[tokio::test]
async fn partial_relay_failure_keeps_every_intent_and_can_retry_without_changing_keys() {
    use nostr_sdk::RelayServiceFlags;
    use std::collections::HashMap;

    let mut healthy = RelayFixture::start(Vec::new(), false).await;
    let mut failing = RelayFixture::start(Vec::new(), false).await;
    let keys = Keys::generate();
    let client = Client::new(keys.clone());
    let bus =
        RelayEventBus::with_client(client, [healthy.url.clone(), failing.url.clone()], DEADLINE)
            .await
            .expect("two configured relays");
    super::tests::wait_until_connected(&bus, &healthy.url).await;
    super::tests::wait_until_connected(&bus, &failing.url).await;
    let filters = vec![
        Filter::new().kind(Kind::TextNote).limit(2),
        Filter::new().kind(Kind::Metadata).limit(1),
    ];
    let subscription = bus
        .subscribe(filters.clone(), Arc::new(|_| {}))
        .await
        .expect("two active intents");
    let mut watched = HashMap::new();
    for _ in 0..2 {
        let (id, filter) = healthy.request_with_filters().await;
        watched.insert(id, filter[0].clone());
        failing.request().await;
    }
    let failed_relay = bus
        .client()
        .relay(&failing.url)
        .await
        .expect("configured relay");
    failed_relay.flags().remove(RelayServiceFlags::READ);
    assert!(
        !super::replay_live_subscriptions(bus.client(), bus.relays(), &watched, DEADLINE).await,
        "partial acceptance must remain pending"
    );
    for _ in 0..2 {
        let (id, filters) = healthy.request_with_filters().await;
        assert_eq!(filters, vec![watched[&id].clone()]);
    }
    assert!(failing.requests.try_recv().is_err());
    failed_relay.flags().add(RelayServiceFlags::READ);
    assert!(super::replay_live_subscriptions(bus.client(), bus.relays(), &watched, DEADLINE).await);
    for _ in 0..2 {
        let (id, filters) = failing.request_with_filters().await;
        assert_eq!(filters, vec![watched[&id].clone()]);
        healthy.request().await;
    }
    assert_eq!(
        bus.client()
            .signer()
            .await
            .expect("same signer")
            .get_public_key()
            .await
            .expect("public key"),
        keys.public_key()
    );
    let active = bus.client().subscriptions().await;
    assert_eq!(active.len(), 2);
    assert!(active.values().all(|relays| relays.len() == 2));
    subscription.close().await.expect("close");
    bus.client().shutdown().await;
}

#[tokio::test]
async fn replay_preserves_duplicate_callbacks_and_rejects_expired_events() {
    use nostr_sdk::Tag;
    use std::collections::HashMap;

    let expired = EventBuilder::text_note("expired history")
        .tags([Tag::expiration(Timestamp::from(1))])
        .sign_with_keys(&Keys::generate())
        .expect("expired signed event");
    let retained = event("retained history");
    let mut relay = RelayFixture::start(vec![expired, retained.clone()], false).await;
    let bus = relay.bus(128).await;
    let (send, mut deliveries) = mpsc::channel(8);
    let filter = Filter::new().kind(Kind::TextNote).limit(2);
    let subscription = bus
        .subscribe(
            vec![filter.clone()],
            Arc::new(move |event| {
                send.try_send(event.event.as_event().id)
                    .expect("callback output");
            }),
        )
        .await
        .expect("subscription");
    let id = relay.request().await;
    assert_eq!(
        timeout(DEADLINE, deliveries.recv())
            .await
            .expect("first event deadline")
            .expect("first event"),
        retained.id
    );
    assert!(
        super::replay_live_subscriptions(
            bus.client(),
            bus.relays(),
            &HashMap::from([(id.clone(), filter)]),
            DEADLINE
        )
        .await
    );
    assert_eq!(relay.request().await, id);
    assert_eq!(
        timeout(DEADLINE, deliveries.recv())
            .await
            .expect("duplicate event deadline")
            .expect("duplicate event"),
        retained.id
    );
    assert!(deliveries.try_recv().is_err());
    subscription.close().await.expect("close");
    bus.client().shutdown().await;
}

#[tokio::test]
async fn admission_replay_recovers_full_ingress_after_pressure_is_drained() {
    use nostr_pubsub::{EventSource, SOURCE_PRIORITY_RELAY, VerifiedEvent};

    let retained = event("retained delivery rejected by full ingress");
    let pressure = event("previously queued work");
    let pressure_id = pressure.id;
    let mut relay = RelayFixture::start(vec![retained.clone()], false).await;
    let bus = relay.bus(128).await;
    let mut observer = bus.client().notifications();
    let (send, mut ingress) = mpsc::channel(1);
    send.try_send(QueryEvent {
        event: VerifiedEvent::try_from(pressure).expect("valid queued event"),
        source: EventSource::relay(relay.url.clone()),
        priority: SOURCE_PRIORITY_RELAY,
    })
    .expect("fill bounded ingress");
    let rejected = Arc::new(Notify::new());
    let notify = Arc::clone(&rejected);
    let subscription = bus
        .subscribe_with_admission(
            vec![Filter::new().id(retained.id).limit(1)],
            Arc::new(move |delivery| match send.try_send(delivery) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    notify.notify_one();
                    false
                }
            }),
        )
        .await
        .expect("admission-aware subscription");
    let id = relay.request().await;
    timeout(DEADLINE, rejected.notified())
        .await
        .expect("known Full rejection");
    assert_eq!(
        ingress
            .recv()
            .await
            .expect("drain pressure")
            .event
            .as_event()
            .id,
        pressure_id
    );
    let recovered = timeout(DEADLINE, ingress.recv())
        .await
        .expect("recovery deadline")
        .expect("recovered delivery");
    assert_eq!(recovered.event.as_event().id, retained.id);
    assert_eq!(relay.request().await, id);
    assert_eq!(
        drain_event_notifications(&mut observer),
        (1, 2),
        "recovery works despite ID dedup, with no SDK notification gap"
    );
    assert!(
        timeout(LiveReplay::MIN_DELAY * 3, relay.requests.recv())
            .await
            .is_err(),
        "accepted replay ends retry work"
    );
    subscription.close().await.expect("close");
    bus.client().shutdown().await;
}

#[tokio::test]
async fn handled_policy_events_and_closed_ingress_do_not_request_replay() {
    for closed_queue in [false, true] {
        let retained = event("deliberately handled delivery");
        let mut relay = RelayFixture::start(vec![retained], false).await;
        let bus = relay.bus(128).await;
        let (send, ingress) = mpsc::channel::<QueryEvent>(1);
        drop(ingress);
        let handled = Arc::new(Notify::new());
        let notify = Arc::clone(&handled);
        let subscription = bus
            .subscribe_with_admission(
                vec![Filter::new().kind(Kind::TextNote)],
                Arc::new(move |delivery| {
                    if closed_queue {
                        assert!(matches!(
                            send.try_send(delivery),
                            Err(mpsc::error::TrySendError::Closed(_))
                        ));
                    }
                    notify.notify_one();
                    true
                }),
            )
            .await
            .expect("subscription");
        relay.request().await;
        timeout(DEADLINE, handled.notified())
            .await
            .expect("delivery handled");
        assert!(
            timeout(LiveReplay::MIN_DELAY * 3, relay.requests.recv())
                .await
                .is_err(),
            "handled deliveries do not trigger replay"
        );
        subscription.close().await.expect("close");
        bus.client().shutdown().await;
    }
}
