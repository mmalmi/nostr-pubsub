use super::routed::{udp_endpoint, udp_endpoint_with_role};
use super::*;
use crate::privacy_roles::ClientRole;

fn envelope(recipient: &Keys, content: &str) -> VerifiedEvent {
    VerifiedEvent::try_from(
        EventBuilder::new(Kind::Custom(1060), content)
            .tags([
                Tag::public_key(recipient.public_key()),
                Tag::parse(["v", "2"]).unwrap(),
            ])
            .sign_with_keys(&Keys::generate())
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn privacy_roles_reject_cross_subject_filters_and_private_export() {
    let a = Keys::generate().public_key();
    let b = Keys::generate().public_key();
    let scope = FipsDiscoveryScope::Author(a);
    let role = ClientRole::Discovery(scope.clone());
    assert!(
        role.validate_filters(&[Filter::new().kind(Kind::Metadata).author(a)])
            .is_ok()
    );
    assert!(
        role.validate_filters(&[Filter::new().kind(Kind::Metadata).authors([a, b])])
            .is_err()
    );
    assert!(
        role.validate_filters(&[Filter::new().kind(Kind::Custom(1060)).author(a)])
            .is_err()
    );
    assert!(
        FipsDiscoveryScope::from_filter(&Filter::new().kind(Kind::Custom(37368)).pubkey(a)).is_ok()
    );
    assert!(
        FipsDiscoveryScope::from_filter(&Filter::new().kind(Kind::Metadata).pubkey(a)).is_err()
    );
    let receiver = ClientRole::Receiver(vec![]);
    assert!(
        receiver
            .validate_filters(&[Filter::new().kind(Kind::Custom(1060)).author(a)])
            .is_ok()
    );
    assert!(
        receiver
            .validate_filters(&[Filter::new().kind(Kind::Custom(1060))])
            .is_err()
    );
    assert!(
        receiver
            .validate_filters(&[Filter::new().kind(Kind::Metadata).author(a)])
            .is_err()
    );
    assert!(
        receiver
            .validate_filters(&[Filter::new().kind(Kind::TextNote).author(a)])
            .is_err()
    );
    assert!(
        receiver
            .validate_filters(&[Filter::new().kind(Kind::Custom(54321)).author(a)])
            .is_err()
    );
    assert!(
        ClientRole::Receiver(vec![a])
            .validate_filters(&[Filter::new().kind(Kind::TextNote).author(a)])
            .is_ok()
    );
    assert!(
        receiver
            .validate_publication(&envelope(&Keys::generate(), "private"))
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn anonymous_private_copies_and_retry_use_distinct_sources_with_real_provider_receipts() {
    let transit = udp_endpoint([131; 32], Vec::new()).await;
    let address = transit.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let provider_endpoint = udp_endpoint(
        [132; 32],
        vec![PeerConfig::new(transit.npub(), "udp", &address)],
    )
    .await;
    let provider = FipsPubsubClient::start(
        provider_endpoint.clone(),
        FipsPubsubClientOptions {
            accept_anonymous_publications: true,
            max_inbound_routed_peers: 8,
            max_connected_peers: 10,
            fanout: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let reader_endpoint = udp_endpoint(
        [133; 32],
        vec![PeerConfig::new(transit.npub(), "udp", &address)],
    )
    .await;
    let reader = FipsPubsubClient::start_receive_only(
        reader_endpoint.clone(),
        FipsPubsubClientOptions {
            routed_peers: vec![provider_endpoint.npub().into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let recipient = Keys::generate();
    let mut incoming = reader
        .subscribe(vec![
            Filter::new()
                .kind(Kind::Custom(1060))
                .pubkey(recipient.public_key()),
        ])
        .await
        .unwrap();
    timeout(Duration::from_secs(15), async {
        while provider.peer_subscription_count().unwrap() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut sources = HashSet::new();
    let event = envelope(&recipient, "peer copy and retry");
    for (secret, event) in [
        ([134; 32], envelope(&Keys::generate(), "own sibling copy")),
        ([135; 32], event.clone()),
        ([136; 32], event.clone()),
    ] {
        let endpoint = udp_endpoint_with_role(
            secret,
            vec![PeerConfig::new(transit.npub(), "udp", &address)],
            true,
        )
        .await;
        timeout(Duration::from_secs(15), async {
            loop {
                let status = endpoint.routing_status().await.unwrap();
                assert!(status.client_only_requested);
                if status.client_only
                    && status.connected_peers > 0
                    && status.peers_without_client_support == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("current transit must negotiate the requested client role before publication");
        assert!(sources.insert(endpoint.npub().to_string()));
        let publisher = FipsAnonymousPublisher::start(
            endpoint.clone(),
            FipsPubsubClientOptions {
                routed_peers: vec![provider_endpoint.npub().into()],
                max_connected_peers: 1,
                fanout: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            publisher.client.inner.subscriptions.lock().unwrap().len(),
            1
        );
        assert!(!publisher.client.inner.role.advertises());
        let receipt = publisher
            .publish_once(event.clone(), Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(receipt.event_id, event.as_event().id);
        assert_eq!(receipt.provider, provider_endpoint.npub());
        assert!(endpoint.routing_status().await.unwrap().client_only);
        assert!(
            provider
                .inner
                .connected_peer_links()
                .await
                .unwrap()
                .iter()
                .all(|peer| !sources.contains(&peer.npub)),
            "inbound senders must never be promoted into provider roster"
        );
        endpoint.shutdown().await.unwrap();
    }
    assert_eq!(
        timeout(Duration::from_secs(5), incoming.recv())
            .await
            .unwrap()
            .unwrap()
            .event
            .as_event()
            .id,
        event.as_event().id
    );
    assert!(
        timeout(Duration::from_millis(200), incoming.recv())
            .await
            .is_err(),
        "retry must deduplicate"
    );
    reader.shutdown().await;
    provider.shutdown().await;
    for endpoint in [reader_endpoint, provider_endpoint, transit] {
        endpoint.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn identified_receiver_cannot_export_private_cache_or_external_replay_after_restart() {
    let endpoint = udp_endpoint([137; 32], Vec::new()).await;
    let client =
        FipsPubsubClient::start_receive_only(endpoint.clone(), FipsPubsubClientOptions::default())
            .await
            .unwrap();
    let peer =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[138; 32]).unwrap().npub()).unwrap();
    let event = envelope(&Keys::generate(), "private retained envelope");
    client
        .inner
        .remember_event(event.clone(), EventSource::local_index("test"), 2)
        .unwrap();
    for message in [
        FipsPubsubWireMessage::publish(event.clone()),
        FipsPubsubWireMessage::deliver(SubscriptionId::new("stranger"), event.clone()),
        FipsPubsubWireMessage::inv(
            vec![SubscriptionId::new("stranger")],
            event.as_event().id,
            1060,
            500,
            2,
        ),
    ] {
        let frame = client.inner.codec.encode_frame(&message).unwrap();
        assert!(client.inner.send_frame(peer, frame).is_err());
    }
    let store = Arc::new(InMemoryEventBus::default());
    store
        .publish(event.clone(), EventSource::local_index("restart"))
        .await
        .unwrap();
    client.set_replay_source(Some(store)).unwrap();
    for message in [
        FipsPubsubWireMessage::req(
            SubscriptionId::new("stranger"),
            vec![Filter::new().id(event.as_event().id)],
        ),
        FipsPubsubWireMessage::want(event.as_event().id),
    ] {
        client
            .inner
            .handle_frame(peer, &client.inner.codec.encode_frame(&message).unwrap())
            .await;
    }
    // Exercise the same final send guard as both cached and external replay paths.
    assert!(
        client
            .inner
            .validate_outbound(
                &peer,
                &client
                    .inner
                    .codec
                    .encode_frame(&FipsPubsubWireMessage::deliver(
                        SubscriptionId::new("stranger"),
                        event
                    ))
                    .unwrap()
            )
            .is_err()
    );
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_provider_cache_acceptance_is_not_a_remote_receipt() {
    let provider_endpoint = udp_endpoint([139; 32], Vec::new()).await;
    let address = provider_endpoint.bound_udp_listen_addrs().await.unwrap()[0].to_string();
    let provider = FipsPubsubClient::start(
        provider_endpoint.clone(),
        FipsPubsubClientOptions {
            max_inbound_routed_peers: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let endpoint = udp_endpoint(
        [140; 32],
        vec![PeerConfig::new(provider_endpoint.npub(), "udp", &address)],
    )
    .await;
    let publisher = FipsAnonymousPublisher::start(
        endpoint.clone(),
        FipsPubsubClientOptions {
            routed_peers: vec![provider_endpoint.npub().into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let result = publisher
        .publish_once(
            envelope(&Keys::generate(), "must stay queued"),
            Duration::from_millis(400),
        )
        .await;
    assert!(
        result.is_err(),
        "cache-only or absent OK must not retire a durable send"
    );
    provider.shutdown().await;
    endpoint.shutdown().await.unwrap();
    provider_endpoint.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identified_receiver_never_serves_private_req_inventory_or_exact_id_across_restart() {
    let event = envelope(
        &Keys::generate(),
        "private body retained across receiver restart",
    );
    let store = Arc::new(InMemoryEventBus::default());
    store
        .publish(
            event.clone(),
            EventSource::local_index("durable-local-history"),
        )
        .await
        .unwrap();
    for restart in [false, true] {
        let endpoint = udp_endpoint([141; 32], Vec::new()).await;
        let address = endpoint.bound_udp_listen_addrs().await.unwrap()[0].to_string();
        let stranger_endpoint = udp_endpoint(
            [142; 32],
            vec![PeerConfig::new(endpoint.npub(), "udp", &address)],
        )
        .await;
        let receiver = FipsPubsubClient::start_receive_only(
            endpoint.clone(),
            FipsPubsubClientOptions {
                routed_peers: vec![stranger_endpoint.npub().into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        receiver.set_replay_source(Some(store.clone())).unwrap();
        if !restart {
            receiver
                .inner
                .remember_event(event.clone(), EventSource::local_index("cache"), 2)
                .unwrap();
        }
        let stranger = FipsPubsubClient::start(
            stranger_endpoint.clone(),
            FipsPubsubClientOptions {
                routed_peers: vec![endpoint.npub().into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut subscription = stranger
            .subscribe(vec![
                Filter::new().kind(Kind::Custom(1060)),
                Filter::new().id(event.as_event().id),
            ])
            .await
            .unwrap();
        timeout(Duration::from_secs(10), async {
            while receiver.delivery_snapshot().req_frames_received < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual stranger requests must reach the identified receiver");
        assert!(
            timeout(Duration::from_millis(150), subscription.recv())
                .await
                .is_err()
        );
        assert_eq!(
            stranger.delivery_snapshot().inv_frames_received,
            0,
            "private inventory must not escape"
        );
        assert_eq!(
            stranger.delivery_snapshot().event_frames_received,
            0,
            "private cache or restart replay must not escape"
        );
        receiver.shutdown().await;
        stranger.shutdown().await;
        endpoint.shutdown().await.unwrap();
        stranger_endpoint.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn provider_receipt_is_bound_to_event_and_authenticated_peer() {
    let endpoint = udp_endpoint([143; 32], Vec::new()).await;
    let provider = Identity::from_secret_bytes(&[144; 32]).unwrap().npub();
    let publisher = FipsAnonymousPublisher::start(
        endpoint.clone(),
        FipsPubsubClientOptions {
            routed_peers: vec![provider.clone()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let event_id = envelope(&Keys::generate(), "binding").as_event().id;
    let wrong_id = EventId::all_zeros();
    let (reply, mut response) = oneshot::channel();
    publisher.client.inner.publications.lock().unwrap().insert(
        event_id,
        crate::anonymous_publication::PendingPublication {
            providers: HashSet::from([provider.clone()]),
            reply: Some(reply),
        },
    );
    publisher
        .client
        .inner
        .handle_publication_receipt("unknown", event_id, true, "");
    publisher
        .client
        .inner
        .handle_publication_receipt(&provider, wrong_id, true, "");
    assert!(response.try_recv().is_err());
    publisher
        .client
        .inner
        .handle_publication_receipt(&provider, event_id, true, "");
    assert_eq!(response.await.unwrap().unwrap().provider, provider);
    assert!(
        publisher
            .client
            .inner
            .publications
            .lock()
            .unwrap()
            .is_empty()
    );
    publisher.client.shutdown().await;
    endpoint.shutdown().await.unwrap();
}

#[tokio::test]
async fn anonymous_publisher_cleans_owned_tasks_on_validation_timeout_and_cancellation() {
    for mode in ["validation", "timeout", "cancel"] {
        let endpoint = udp_endpoint([145; 32], Vec::new()).await;
        let provider = Identity::from_secret_bytes(&[146; 32]).unwrap().npub();
        let publisher = FipsAnonymousPublisher::start(
            endpoint.clone(),
            FipsPubsubClientOptions {
                routed_peers: vec![provider],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let inner = Arc::downgrade(&publisher.client.inner);
        let tasks = {
            let tasks = publisher.client.tasks.lock().await;
            [
                tasks.transport.as_ref().unwrap().abort_handle(),
                tasks.peerfinding.as_ref().unwrap().abort_handle(),
                tasks.replay.as_ref().unwrap().abort_handle(),
            ]
        };
        let event = if mode == "validation" {
            super::routed::signed_note("invalid outer")
        } else {
            envelope(&Keys::generate(), "queued private send")
        };
        if mode == "cancel" {
            let task = tokio::spawn(publisher.publish_once(event, Duration::from_secs(30)));
            timeout(Duration::from_secs(1), async {
                loop {
                    if inner
                        .upgrade()
                        .is_some_and(|inner| !inner.publications.lock().unwrap().is_empty())
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert!(
                publisher
                    .publish_once(event, Duration::from_millis(20))
                    .await
                    .is_err()
            );
        }
        timeout(Duration::from_secs(1), async {
            while inner.upgrade().is_some()
                || !tasks.iter().all(tokio::task::AbortHandle::is_finished)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("no owned inner/tasks may survive failure or cancellation");
        // FIPS removes closed service receivers on its maintenance turn.
        // Wait on actual rebind readiness rather than assuming that turn ran.
        let rebound = timeout(Duration::from_secs(5), async {
            loop {
                match FipsPubsubClient::start_receive_only(
                    endpoint.clone(),
                    FipsPubsubClientOptions::default(),
                )
                .await
                {
                    Ok(client) => break client,
                    Err(error) if error.to_string().contains("already registered") => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("unexpected rebind failure after {mode}: {error}"),
                }
            }
        })
        .await
        .expect("closed receiver must release its actual service registration");
        rebound.shutdown().await;
        endpoint.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn receiver_rejects_cached_public_profile_advert_and_unknown_author_exports() {
    let endpoint = udp_endpoint([147; 32], Vec::new()).await;
    let own = Keys::generate();
    let stranger =
        PeerIdentity::from_npub(&Identity::from_secret_bytes(&[148; 32]).unwrap().npub()).unwrap();
    let client = FipsPubsubClient::start_receive_only_with_public_authors(
        endpoint.clone(),
        FipsPubsubClientOptions::default(),
        vec![own.public_key()],
    )
    .await
    .unwrap();
    for kind in [0, 37195, 54321] {
        let event = VerifiedEvent::try_from(
            EventBuilder::new(Kind::Custom(kind), "privately looked-up public event")
                .sign_with_keys(&Keys::generate())
                .unwrap(),
        )
        .unwrap();
        client
            .inner
            .remember_event(
                event.clone(),
                EventSource::local_index("stale-private-lookup"),
                4,
            )
            .unwrap();
        for message in [
            FipsPubsubWireMessage::deliver(SubscriptionId::new("stranger"), event.clone()),
            FipsPubsubWireMessage::inv(
                vec![SubscriptionId::new("stranger")],
                event.as_event().id,
                kind,
                500,
                1,
            ),
        ] {
            let frame = client.inner.codec.encode_frame(&message).unwrap();
            assert!(
                client.inner.validate_outbound(&stranger, &frame).is_err(),
                "identified receiver exported unapproved public author kind {kind}"
            );
        }
    }
    client.shutdown().await;
    endpoint.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn receiver_public_author_allowlist_covers_broad_cache_and_restart_exact_id_replay() {
    let own = Keys::generate();
    let approved = VerifiedEvent::try_from(
        EventBuilder::new(Kind::TextNote, "approved public head")
            .sign_with_keys(&own)
            .unwrap(),
    )
    .unwrap();
    let foreign = [0, 37195, 54321].map(|kind| {
        VerifiedEvent::try_from(
            EventBuilder::new(Kind::Custom(kind), "public body from private peer lookup")
                .tags([Tag::identifier("fips-overlay-v1")])
                .sign_with_keys(&Keys::generate())
                .unwrap(),
        )
        .unwrap()
    });
    let store = Arc::new(InMemoryEventBus::default());
    for event in foreign.iter().chain(std::iter::once(&approved)) {
        store
            .publish(
                event.clone(),
                EventSource::local_index("shared-local-cache"),
            )
            .await
            .unwrap();
    }
    for restart in [false, true] {
        let endpoint = udp_endpoint([149; 32], Vec::new()).await;
        let address = endpoint.bound_udp_listen_addrs().await.unwrap()[0].to_string();
        let stranger_endpoint = udp_endpoint(
            [150; 32],
            vec![PeerConfig::new(endpoint.npub(), "udp", &address)],
        )
        .await;
        let receiver = FipsPubsubClient::start_receive_only_with_public_authors(
            endpoint.clone(),
            FipsPubsubClientOptions {
                routed_peers: vec![stranger_endpoint.npub().into()],
                ..Default::default()
            },
            vec![own.public_key()],
        )
        .await
        .unwrap();
        receiver.set_replay_source(Some(store.clone())).unwrap();
        if !restart {
            for event in foreign.iter().chain(std::iter::once(&approved)) {
                receiver
                    .inner
                    .remember_event(
                        event.clone(),
                        EventSource::local_index("stale-private-lookup"),
                        4,
                    )
                    .unwrap();
            }
        }
        let stranger = FipsPubsubClient::start(
            stranger_endpoint.clone(),
            FipsPubsubClientOptions {
                routed_peers: vec![endpoint.npub().into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut incoming = stranger
            .subscribe(vec![Filter::new().kinds([
                Kind::Metadata,
                Kind::TextNote,
                Kind::Custom(37195),
                Kind::Custom(54321),
            ])])
            .await
            .unwrap();
        let delivered = timeout(Duration::from_secs(10), incoming.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            delivered.event.as_event().id,
            approved.as_event().id,
            "identified receiver exposed privately cached public author"
        );
        assert!(
            timeout(Duration::from_millis(150), incoming.recv())
                .await
                .is_err()
        );
        let before = stranger.delivery_snapshot();
        let peer = PeerIdentity::from_npub(endpoint.npub()).unwrap();
        for event in &foreign {
            stranger
                .inner
                .send_frame(
                    peer,
                    stranger
                        .inner
                        .codec
                        .encode_frame(&FipsPubsubWireMessage::want(event.as_event().id))
                        .unwrap(),
                )
                .unwrap();
        }
        assert!(
            timeout(Duration::from_millis(150), incoming.recv())
                .await
                .is_err()
        );
        assert_eq!(
            stranger.delivery_snapshot().event_frames_received,
            before.event_frames_received,
            "exact-ID WANT leaked a private lookup author"
        );
        assert_eq!(
            stranger.delivery_snapshot().inv_frames_received,
            before.inv_frames_received,
            "private lookup inventory leaked after restart"
        );
        receiver.shutdown().await;
        stranger.shutdown().await;
        endpoint.shutdown().await.unwrap();
        stranger_endpoint.shutdown().await.unwrap();
    }
}
