use nostr::{EventBuilder, Keys, PublicKey, Tag, Timestamp};

use super::*;

fn event(keys: &Keys, kind: u16, content: &str, tags: Vec<Tag>) -> VerifiedEvent {
    let event = EventBuilder::new(Kind::Custom(kind), content)
        .tags(tags)
        .custom_created_at(Timestamp::from(1_700_000_000))
        .sign_with_keys(keys)
        .expect("sign event");
    VerifiedEvent::try_from(event).expect("verified event")
}

fn notes(count: usize) -> Vec<VerifiedEvent> {
    let keys = Keys::generate();
    (0..count)
        .map(|index| event(&keys, 30078, &format!("root {index}"), Vec::new()))
        .collect()
}

#[test]
fn sync_frames_match_the_typescript_wire() {
    let frame = FeedSyncFrame::new(FeedSyncAction::Open, "wf1-ts-a1", vec![0x61, 0x00, 0xff]);
    let bytes = frame.encode().unwrap();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        r#"["FEED-SYNC/1","open","wf1-ts-a1","6100ff"]"#
    );
    assert!(FeedSyncFrame::is_feed_sync(&bytes));
    assert!(FeedSyncFrame::is_feed_sync(
        br#" [ "FEED-SYNC/1","end","wf1-ts-1",""]"#
    ));
    assert!(!FeedSyncFrame::is_feed_sync(br#"["REQ","wf1-ts-1",{}]"#));
    assert_eq!(FeedSyncFrame::decode(&bytes).unwrap(), frame);
    let end = FeedSyncFrame::new(FeedSyncAction::End, "wf1-ts-1", Vec::new());
    assert_eq!(FeedSyncFrame::decode(&end.encode().unwrap()).unwrap(), end);
    for invalid in [
        r#"["FEED-SYNC/1","open","cf1-ts-1","61"]"#,
        r#"["FEED-SYNC/1","open","wf1-fips-1","61"]"#,
        r#"["FEED-SYNC/1","open","wf1-ts-ABC","61"]"#,
        r#"["FEED-SYNC/1","open","wf1-ts-1234567890123","61"]"#,
        r#"["FEED-SYNC/1","open","wf1-ts-1",""]"#,
        r#"["FEED-SYNC/1","open","wf1-ts-1","6"]"#,
        r#"["FEED-SYNC/1","open","wf1-ts-1","AB"]"#,
        r#"["FEED-SYNC/1","end","wf1-ts-1","61"]"#,
        r#"["FEED-SYNC/1","accept","wf1-ts-1",""]"#,
        r#"["FEED-SYNC/1","end","wf1-ts-1"]"#,
        r#"["FEED-SYNC/1","end","wf1-ts-1","",""]"#,
    ] {
        assert!(
            FeedSyncFrame::decode(invalid.as_bytes()).is_err(),
            "{invalid}"
        );
    }
    let oversized = format!(
        r#"["FEED-SYNC/1","step","wf1-ts-1","{}"]"#,
        "ab".repeat(8_193)
    );
    assert!(FeedSyncFrame::decode(oversized.as_bytes()).is_err());
    let largest = format!(
        r#"["FEED-SYNC/1","step","wf1-ts-1","{}"]"#,
        "ab".repeat(8_192)
    );
    assert!(FeedSyncFrame::decode(largest.as_bytes()).is_ok());
}

#[test]
fn feed_ids_name_the_local_subscription() {
    assert_eq!(feed_wire_id("ts-1z", true), "wf1-ts-1z");
    assert_eq!(feed_wire_id("ts-1z", false), "cf1-ts-1z");
    assert_eq!(feed_subscription_key("cf1-ts-1z"), Some("ts-1z"));
    assert!(is_warm_feed_id("wf1-ts-1z") && !is_warm_feed_id("cf1-ts-1z"));
    for id in ["fips-1", "xf1-ts-1", "wf1-ts-", "wf1-ts-a_b", "wf2-ts-1"] {
        assert!(!is_public_feed_id(id), "{id}");
    }
}

#[test]
fn feed_filters_follow_the_typescript_rules() {
    let author = PublicKey::from_slice(&[2; 32]).unwrap();
    let kinds = Filter::new().kinds([Kind::Custom(7368), Kind::Custom(30078)]);
    assert!(valid_public_feed_filter(&kinds));
    assert_eq!(
        serde_json::to_string(&kinds).unwrap(),
        r#"{"kinds":[7368,30078]}"#
    );
    assert!(valid_public_feed_filter(&kinds.clone().author(author)));
    assert!(valid_public_feed_filter(
        &Filter::new()
            .kinds([Kind::TextNote, Kind::Reaction])
            .pubkey(author)
    ));
    for invalid in [
        Filter::new(),
        kinds.clone().limit(8),
        kinds.clone().since(Timestamp::from(1)),
        kinds.clone().pubkey(author),
        kinds.clone().author(author).pubkey(author),
        Filter::new()
            .kinds([Kind::TextNote])
            .pubkeys([author, Keys::generate().public_key()]),
        Filter::new().kind(Kind::Custom(1059)),
        Filter::new().kind(Kind::Custom(4)),
        kinds.clone().id(nostr::EventId::all_zeros()),
    ] {
        assert!(!valid_public_feed_filter(&invalid), "{invalid:?}");
    }
}

#[test]
fn windows_keep_public_matching_events_in_order_and_bounded() {
    let keys = Keys::generate();
    let mut window = FeedWindow::new(Filter::new().kind(Kind::Custom(30078)), 2);
    let roster = event(
        &keys,
        37368,
        "",
        vec![Tag::parse(["type", "group_roster"]).unwrap()],
    );
    window.remember(&roster);
    window.remember(&event(&keys, 1, "note", Vec::new()));
    assert!(window.is_empty());
    let roots = notes(3);
    for root in &roots {
        window.remember(root);
        window.remember(root);
    }
    assert_eq!(window.snapshot(), roots[1..].to_vec());
    assert!(window.get(&roots[0].as_event().id).is_none());
}

fn exchange(
    provider: &mut FeedSync,
    subscriber: &mut FeedSync,
    mut output: FeedSyncOutput,
    window: &[VerifiedEvent],
) -> (Vec<FeedSyncAction>, FeedSyncFinish) {
    let mut actions = Vec::new();
    loop {
        if let Some(finish) = output.finish {
            actions.extend(output.frames.iter().map(|frame| frame.action));
            return (actions, finish);
        }
        let [frame] = output.frames.as_slice() else {
            panic!("expected one frame, got {output:?}");
        };
        actions.push(frame.action);
        let frame = FeedSyncFrame::decode(&frame.encode().unwrap()).unwrap();
        let answer = subscriber.receive(
            "provider",
            &frame,
            Some((None, window.to_vec())),
            || true,
            0,
        );
        let [reply] = answer.frames.as_slice() else {
            panic!("expected one reply, got {answer:?}");
        };
        actions.push(reply.action);
        output = provider.receive("subscriber", reply, None, || false, 0);
    }
}

#[test]
fn a_warm_session_announces_only_what_the_subscriber_lacks() {
    let events = notes(40);
    let mut provider = FeedSync::new(65_525, 4);
    let mut subscriber = FeedSync::new(65_525, 4);
    let window = events[..30]
        .iter()
        .chain(&events[35..])
        .cloned()
        .collect::<Vec<_>>();
    let opening = provider.offer("subscriber", "wf1-ts-1", None, events.clone(), 0);
    let (actions, finish) = exchange(&mut provider, &mut subscriber, opening, &window);
    assert_eq!(actions.first(), Some(&FeedSyncAction::Open));
    assert_eq!(actions.last(), Some(&FeedSyncAction::End));
    assert!(!finish.fallback);
    let mut missing = finish.events;
    missing.sort_by_key(|event| event.as_event().id);
    let mut expected = events[30..35].to_vec();
    expected.sort_by_key(|event| event.as_event().id);
    assert_eq!(missing, expected);
    assert_eq!(provider.session_count(), 0);

    let opening = provider.offer("subscriber", "wf1-ts-1", None, events.clone(), 0);
    subscriber.receive(
        "provider",
        &FeedSyncFrame::new(FeedSyncAction::End, "wf1-ts-1", Vec::new()),
        None,
        || false,
        0,
    );
    let (actions, finish) = exchange(&mut provider, &mut subscriber, opening, &events);
    assert_eq!(
        actions,
        [
            FeedSyncAction::Open,
            FeedSyncAction::Reply,
            FeedSyncAction::End
        ]
    );
    assert!(finish.events.is_empty() && !finish.fallback);
}

#[test]
fn a_subscriber_answers_only_its_own_admitted_session() {
    let mut subscriber = FeedSync::new(65_525, 4);
    let open = |id: &str| FeedSyncFrame::new(FeedSyncAction::Open, id, vec![0x61]);
    let none: Vec<FeedSyncFrame> = Vec::new();
    assert_eq!(
        subscriber
            .receive("p", &open("wf1-ts-1"), None, || true, 0)
            .frames,
        none
    );
    assert_eq!(
        subscriber
            .receive(
                "p",
                &open("wf1-ts-1"),
                Some((None, Vec::new())),
                || false,
                0
            )
            .frames,
        none
    );
}

#[test]
fn declined_late_and_refused_sessions_fall_back_once() {
    let events = notes(3);
    let mut provider = FeedSync::new(65_525, 4);
    let opened = provider.offer("s", "wf1-ts-2", None, events.clone(), 0);
    assert_eq!(opened.frames[0].action, FeedSyncAction::Open);
    let declined = provider.receive(
        "s",
        &FeedSyncFrame::new(FeedSyncAction::Decline, "wf1-ts-2", Vec::new()),
        None,
        || false,
        0,
    );
    assert_eq!(declined.frames[0].action, FeedSyncAction::End);
    assert_eq!(declined.finish.unwrap().events, events);
    assert!(
        provider
            .receive(
                "s",
                &FeedSyncFrame::new(FeedSyncAction::Decline, "wf1-ts-2", Vec::new()),
                None,
                || false,
                0
            )
            .finish
            .is_none()
    );

    provider.offer("s", "wf1-ts-3", None, events.clone(), 0);
    assert_eq!(provider.expire(2_499), Vec::new());
    let [expired] = provider.expire(2_500).try_into().unwrap();
    assert_eq!(expired.frames[0].action, FeedSyncAction::End);
    assert!(expired.finish.unwrap().fallback);

    let bad = provider.offer("s", "wf1-ts-4", None, events.clone(), 0);
    assert_eq!(bad.frames[0].action, FeedSyncAction::Open);
    let replied = provider.receive(
        "s",
        &FeedSyncFrame::new(FeedSyncAction::Reply, "wf1-ts-4", vec![0x62]),
        None,
        || false,
        0,
    );
    assert!(replied.finish.unwrap().fallback);

    let mut small = FeedSync::new(65_525, 1);
    small.offer("s", "wf1-ts-5", None, events.clone(), 0);
    let refused = small.offer("s", "wf1-ts-6", None, events.clone(), 0);
    assert!(refused.frames.is_empty() && refused.finish.unwrap().fallback);
    let mut tiny_frames = FeedSync::new(4_096, 4);
    assert!(
        tiny_frames
            .offer("s", "wf1-ts-7", None, events, 0)
            .finish
            .unwrap()
            .fallback
    );
}
