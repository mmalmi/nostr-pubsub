use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use nostr_pubsub::{EventRetentionPolicy, Filter, QueryOptions, RetainedEvents, VerifiedEvent};

fn advert(keys: &Keys, time: u64, scope: &str, value: &str) -> Event {
    EventBuilder::new(Kind::Custom(37195), value)
        .tags([
            Tag::identifier(scope),
            Tag::expiration(Timestamp::from(time + 600)),
        ])
        .custom_created_at(Timestamp::from(time))
        .sign_with_keys(keys)
        .unwrap()
}
fn policy() -> EventRetentionPolicy {
    EventRetentionPolicy::new(
        3,
        vec![Filter::new().kind(Kind::Custom(37195)).identifier("mesh")],
    )
}
#[test]
fn restored_signed_announcements_keep_only_current_coordinate_winners() {
    let keys = Keys::generate();
    let old = advert(&keys, 1000, "mesh", "old");
    let latest = advert(&keys, 1001, "mesh", "new");
    let other_scope = advert(&keys, 1002, "private", "hidden");
    let mut corrupt = latest.clone();
    corrupt.content = "forged".into();
    let mut cache = RetainedEvents::new(policy()).unwrap();
    cache.restore(
        [old.clone(), latest.clone(), old, other_scope, corrupt],
        1002,
    );
    let events = cache.query(&[Filter::new()], QueryOptions::default(), 1002);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_event(), &latest);
    assert!(
        cache
            .query(&[Filter::new()], QueryOptions::default(), 1601)
            .is_empty()
    );
}
#[test]
fn bounded_retention_uses_nostr_ties_and_filter_limits() {
    let keys = Keys::generate();
    let a = advert(&keys, 1000, "mesh", "a");
    let b = advert(&keys, 1000, "mesh", "b");
    let winner = if a.id < b.id { a.clone() } else { b.clone() };
    for events in [[a.clone(), b.clone()], [b, a]] {
        let mut cache = RetainedEvents::new(policy()).unwrap();
        cache.restore(events, 1000);
        assert_eq!(cache.snapshot(1000), [winner.clone()]);
        for _ in 0..8 {
            cache.insert(
                VerifiedEvent::try_from(advert(&Keys::generate(), 1001, "mesh", "")).unwrap(),
                1001,
            );
        }
        assert_eq!(cache.snapshot(1001).len(), 3);
        assert_eq!(
            cache
                .query(&[Filter::new().limit(1)], QueryOptions::default(), 1001)
                .len(),
            1
        );
        assert!(
            cache
                .query(&[Filter::new().limit(0)], QueryOptions::default(), 1001)
                .is_empty()
        );
    }
}
#[cfg(feature = "file-store")]
#[tokio::test]
async fn restart_serves_signed_announcements_without_a_network_and_excludes_second_writer() {
    use nostr_pubsub::{EventBus, EventSource, FileEventBus};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.json");
    let keys = Keys::generate();
    let event = advert(
        &keys,
        Timestamp::now().as_secs(),
        "mesh",
        "udp://127.0.0.1:9000",
    );
    let bus = FileEventBus::open(&path, policy()).await.unwrap();
    assert!(FileEventBus::open(&path, policy()).await.is_err());
    assert!(
        bus.publish(
            VerifiedEvent::try_from(event.clone()).unwrap(),
            EventSource::peer("original")
        )
        .await
        .unwrap()
        .accepted
    );
    drop(bus);
    // A malformed record cannot discard other usable, correctly signed peers.
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["events"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"id":"broken"}));
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let restored = FileEventBus::open(&path, policy()).await.unwrap();
    let report = restored
        .query(
            vec![Filter::new().kind(Kind::Custom(37195))],
            QueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.events.len(), 1);
    assert_eq!(report.events[0].event.as_event(), &event);
    assert_eq!(
        report.events[0].source.kind,
        nostr_pubsub::EventSourceKind::LocalIndex
    );
    restored.close().await;
    assert!(
        restored
            .query(vec![Filter::new()], QueryOptions::default())
            .await
            .is_err()
    );
    // An application can reopen immediately, even if a stopped consumer still
    // retains a clone of its old bus.
    FileEventBus::open(&path, policy())
        .await
        .unwrap()
        .close()
        .await;
}

#[test]
fn malformed_expiration_cannot_turn_a_signed_event_into_a_permanent_record() {
    let keys = Keys::generate();
    for value in ["", "invalid", "+1600", "-1", "18446744073709551616"] {
        let event = EventBuilder::new(Kind::Custom(37195), "invalid expiration")
            .custom_created_at(Timestamp::from(1000))
            .tags([
                Tag::identifier("mesh"),
                Tag::custom(nostr::TagKind::custom("expiration"), [value]),
            ])
            .sign_with_keys(&keys)
            .unwrap();
        let mut cache = RetainedEvents::new(policy()).unwrap();
        cache.restore([event], 1000);
        assert!(
            cache.snapshot(1000).is_empty(),
            "accepted expiration {value:?}"
        );
    }
}
