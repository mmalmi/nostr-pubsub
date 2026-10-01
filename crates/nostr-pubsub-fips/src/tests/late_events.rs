use super::routed::{signed_note, udp_endpoint};
use super::*;

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
