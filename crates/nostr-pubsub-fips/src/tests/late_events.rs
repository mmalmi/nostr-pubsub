use super::routed::{signed_note, udp_endpoint};
use super::*;

#[tokio::test]
async fn full_local_subscription_recovers_requested_event_when_receiver_drains() {
    let endpoint = udp_endpoint([119; 32], Vec::new()).await;
    let options = FipsPubsubClientOptions {
        max_replay_events: 1,
        ..Default::default()
    };
    let client = FipsPubsubClient::start(endpoint.clone(), options)
        .await
        .unwrap();
    let peer =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[120; 32]).unwrap().npub()).unwrap();
    let first = signed_note("fills the local delivery queue");
    let second = signed_note("arrives before the application drains it");
    let mut subscription = client
        .subscribe(vec![Filter::new().kind(Kind::TextNote)])
        .await
        .unwrap();
    client
        .inner
        .lock_subscriptions()
        .unwrap()
        .get_mut(&subscription.key)
        .unwrap()
        .peers
        .insert(peer.npub());
    let id = subscription.id().clone();
    let mut frames = Vec::new();
    for event in [&first, &second] {
        assert!(
            client
                .inner
                .accept_inventory(
                    &peer.npub(),
                    crate::client_inner::InventoryAdvertisement {
                        subscription_ids: vec![id.clone()],
                        event_id: event.as_event().id,
                        event_kind: event.as_event().kind.as_u16(),
                        payload_bytes: event_payload_bytes(event).unwrap(),
                        hop_limit: 2,
                    },
                )
                .unwrap()
                .is_some()
        );
        let frame = client
            .inner
            .codec
            .encode_frame(&FipsPubsubWireMessage::deliver(id.clone(), event.clone()))
            .unwrap();
        // Exercise signed frame decoding and normal WANT completion at the
        // authenticated peer boundary, without draining the application queue.
        client.inner.handle_frame(peer, &frame).await;
        frames.push(frame);
    }
    assert!(
        client
            .inner
            .pending_wants
            .lock()
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(subscription.try_recv().unwrap().event, first);

    // Receiver progress alone must recover an accepted body. It cannot depend
    // on a duplicate from the network, another subscription, or reconnecting.
    let recovered = timeout(Duration::from_millis(500), subscription.recv()).await;

    // Positive control: the second signed body reached the client and remains
    // replayable even when delivery to the original subscription was full.
    let mut replay = client
        .subscribe(vec![Filter::new().id(second.as_event().id)])
        .await
        .unwrap();
    assert_eq!(replay.try_recv().unwrap().event, second);
    replay.close();

    client.inner.handle_frame(peer, &frames[1]).await;
    let after_duplicate = subscription.try_recv();
    client.inner.reset_peer_epoch(&peer.npub());
    assert_ne!(client.inner.replay_frames_for_peer(&peer.npub()).len(), 0);
    let inventory = client.inner.inventory_frame(vec![id], &second, 2).unwrap();
    client.inner.handle_frame(peer, &inventory).await;
    client.inner.handle_frame(peer, &frames[1]).await;
    let after_reconnect = subscription.try_recv();

    subscription.close();
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
    eprintln!(
        "local delivery recovery: cached_replay=true, receiver_drain={:?}, \
         same_epoch_retry={after_duplicate:?}, next_epoch_retry={after_reconnect:?}",
        recovered
            .as_ref()
            .map(|event| event.as_ref().map(|event| event.event.as_event().id))
    );
    assert!(
        recovered.is_ok(),
        "draining a full subscription must recover the requested event; \
         after duplicate and peer-epoch replay: {after_reconnect:?}"
    );
    assert_eq!(recovered.unwrap().unwrap().event, second);
    assert!(
        matches!(after_duplicate, Err(mpsc::error::TryRecvError::Empty)),
        "a duplicate answer must not deliver the recovered event twice"
    );
    assert!(
        matches!(after_reconnect, Err(mpsc::error::TryRecvError::Empty)),
        "retry and reconnect must not deliver the recovered event twice"
    );
}

#[tokio::test]
async fn delayed_requested_answer_keeps_propagation_after_provider_rotation() {
    check_delayed_requested_answer(false).await;
}

#[tokio::test]
async fn delayed_requested_answer_keeps_fresh_subscription_after_provider_rotation() {
    check_delayed_requested_answer(true).await;
}

async fn check_delayed_requested_answer(fresh: bool) {
    let endpoint = udp_endpoint([116; 32], Vec::new()).await;
    let client = FipsPubsubClient::start(endpoint.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let peers = [117, 118].map(|seed| {
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[seed; 32]).unwrap().npub()).unwrap()
    });
    let event = signed_note("delayed original provider response");
    let filters = vec![Filter::new().id(event.as_event().id)];
    let older = if fresh {
        Some(client.subscribe(filters.clone()).await.unwrap())
    } else {
        None
    };
    let mut subscription = client.inner.subscribe(filters, !fresh).await.unwrap();
    client
        .inner
        .lock_subscriptions()
        .unwrap()
        .get_mut(&subscription.key)
        .unwrap()
        .peers
        .extend(peers.iter().map(PeerIdentity::npub));
    if let Some(older) = &older {
        client
            .inner
            .lock_subscriptions()
            .unwrap()
            .get_mut(&older.key)
            .unwrap()
            .peers
            .extend(peers.iter().map(PeerIdentity::npub));
    }
    for (index, peer) in peers.iter().enumerate() {
        let request = client
            .inner
            .accept_inventory(
                &peer.npub(),
                crate::client_inner::InventoryAdvertisement {
                    subscription_ids: vec![SubscriptionId::new(subscription.key.clone())],
                    event_id: event.as_event().id,
                    event_kind: event.as_event().kind.as_u16(),
                    payload_bytes: event_payload_bytes(&event).unwrap(),
                    hop_limit: 3,
                },
            )
            .unwrap();
        assert_eq!(request.is_some(), index == 0);
    }
    let requested_at = client.inner.pending_wants.lock().unwrap().entries
        [&event.as_event().id.to_hex()]
        .requested_at_ms;
    let retries = client.inner.retry_pending_frames(requested_at + 500);
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].0, peers[1]);
    if fresh {
        assert_eq!(
            client
                .inner
                .fresh_response_subscription(&peers[0].npub(), &event),
            Some(SubscriptionId::new(subscription.key.clone()))
        );
    }
    let wire_key = older.as_ref().map_or(&subscription.key, |older| &older.key);
    let frame = client
        .inner
        .codec
        .encode_frame(&FipsPubsubWireMessage::deliver(
            SubscriptionId::new(wire_key.clone()),
            event.clone(),
        ))
        .unwrap();
    client.inner.handle_frame(peers[0], &frame).await;
    assert_eq!(subscription.try_recv().unwrap().event, event);
    let remaining_hops = client
        .inner
        .recent_events
        .lock()
        .unwrap()
        .entries
        .iter()
        .find(|cached| cached.event == event)
        .unwrap()
        .hop_limit;
    let pending_count = client.inner.pending_wants.lock().unwrap().entries.len();
    subscription.close();
    drop(older);
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
    assert_eq!(
        remaining_hops, 2,
        "the first requested response must retain its propagation budget"
    );
    assert_eq!(
        pending_count, 0,
        "a valid late answer fulfills the pending WANT"
    );
}

#[tokio::test]
async fn repeated_answers_after_subscription_close_do_not_penalize_an_honest_peer() {
    check_late_answers(false).await;
}

#[tokio::test]
async fn repeated_answers_keep_the_original_wire_subscription_after_fresh_remapping() {
    check_late_answers(true).await;
}

async fn check_late_answers(remap: bool) {
    let endpoint = udp_endpoint([109; 32], Vec::new()).await;
    let client = FipsPubsubClient::start(endpoint.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let peer =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[110; 32]).unwrap().npub()).unwrap();
    for i in 0..20 {
        let event = signed_note(&format!("answered before close {i}"));
        let filters = vec![Filter::new().id(event.as_event().id)];
        let older = if remap {
            Some(client.subscribe(filters.clone()).await.unwrap())
        } else {
            None
        };
        let mut subscription = client.inner.subscribe(filters, !remap).await.unwrap();
        client
            .inner
            .lock_subscriptions()
            .unwrap()
            .get_mut(&subscription.key)
            .unwrap()
            .peers
            .insert(peer.npub());
        if let Some(older) = &older {
            client
                .inner
                .lock_subscriptions()
                .unwrap()
                .get_mut(&older.key)
                .unwrap()
                .peers
                .insert(peer.npub());
            assert!(
                client
                    .inner
                    .accept_inventory(
                        &peer.npub(),
                        crate::client_inner::InventoryAdvertisement {
                            subscription_ids: vec![SubscriptionId::new(subscription.key.clone())],
                            event_id: event.as_event().id,
                            event_kind: event.as_event().kind.as_u16(),
                            payload_bytes: event_payload_bytes(&event).unwrap(),
                            hop_limit: 2,
                        }
                    )
                    .unwrap()
                    .is_some()
            );
        }
        let wire_key = older.as_ref().map_or(&subscription.key, |older| &older.key);
        let frame = client
            .inner
            .codec
            .encode_frame(&FipsPubsubWireMessage::Event {
                subscription_id: Some(SubscriptionId::new(wire_key.clone())),
                event: event.clone(),
            })
            .unwrap();
        client.inner.handle_frame(peer, &frame).await;
        assert_eq!(subscription.try_recv().unwrap().event, event);
        subscription.close();
        drop(older);
        // A retry was already in flight when the application finished reading.
        // Exercise the normal authenticated frame handler after local CLOSE.
        client.inner.handle_frame(peer, &frame).await;
    }
    let snapshot = client.delivery_snapshot();
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
    assert_eq!(snapshot.subscription_events_received, 20);
    assert_eq!(snapshot.provider_cooldowns, 0);
}

#[tokio::test]
async fn repeated_unsolicited_answers_still_penalize_the_peer() {
    let endpoint = udp_endpoint([111; 32], Vec::new()).await;
    let client = FipsPubsubClient::start(endpoint.clone(), FipsPubsubClientOptions::default())
        .await
        .unwrap();
    let peer =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[112; 32]).unwrap().npub()).unwrap();
    let frame = client
        .inner
        .codec
        .encode_frame(&FipsPubsubWireMessage::Event {
            subscription_id: Some(SubscriptionId::new("never-subscribed")),
            event: signed_note("unsolicited repeated answer"),
        })
        .unwrap();
    for _ in 0..20 {
        client.inner.handle_frame(peer, &frame).await;
    }
    let snapshot = client.delivery_snapshot();
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
    assert_eq!(snapshot.subscription_events_received, 0);
    assert_eq!(snapshot.provider_cooldowns, 1);
}

#[tokio::test]
async fn closed_answer_exemption_is_scoped_to_peer_subscription_and_event() {
    for mismatch in ["peer", "subscription", "event"] {
        let endpoint = udp_endpoint([113; 32], Vec::new()).await;
        let client = FipsPubsubClient::start(endpoint.clone(), FipsPubsubClientOptions::default())
            .await
            .unwrap();
        let peer =
            PeerIdentity::from_npub(&Identity::from_secret_bytes(&[114; 32]).unwrap().npub())
                .unwrap();
        let event = signed_note("one requested answer");
        let mut subscription = client
            .subscribe(vec![Filter::new().id(event.as_event().id)])
            .await
            .unwrap();
        let key = subscription.key.clone();
        client
            .inner
            .lock_subscriptions()
            .unwrap()
            .get_mut(&key)
            .unwrap()
            .peers
            .insert(peer.npub());
        let encode = |key, event| {
            client
                .inner
                .codec
                .encode_frame(&FipsPubsubWireMessage::Event {
                    subscription_id: Some(key),
                    event,
                })
                .unwrap()
        };
        client
            .inner
            .handle_frame(
                peer,
                &encode(SubscriptionId::new(key.clone()), event.clone()),
            )
            .await;
        assert_eq!(subscription.try_recv().unwrap().event, event);
        subscription.close();
        let offender = if mismatch == "peer" {
            PeerIdentity::from_npub(&Identity::from_secret_bytes(&[115; 32]).unwrap().npub())
                .unwrap()
        } else {
            peer
        };
        let key = SubscriptionId::new(if mismatch == "subscription" {
            "never-subscribed".into()
        } else {
            key
        });
        let event = if mismatch == "event" {
            signed_note("unsolicited replacement")
        } else {
            event
        };
        let frame = encode(key, event);
        for _ in 0..20 {
            client.inner.handle_frame(offender, &frame).await;
        }
        let snapshot = client.delivery_snapshot();
        client.shutdown().await;
        endpoint.shutdown().await.unwrap();
        assert_eq!(snapshot.subscription_events_received, 1, "{mismatch}");
        assert_eq!(snapshot.provider_cooldowns, 1, "{mismatch}");
    }
}
