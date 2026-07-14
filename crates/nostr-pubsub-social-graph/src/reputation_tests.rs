use nostr_sdk::prelude::{EventBuilder, Keys, ToBech32};
use nostr_social_memory::RatingEventExt;

use super::*;

#[test]
fn default_policy_explores_unknown_prioritizes_good_and_drops_bad() {
    let root = Keys::generate();
    let good = Keys::generate();
    let unknown = Keys::generate();
    let bad = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let now = now_unix_secs();
    let (mut reputation, policy) = PeerReputation::new(
        &root.public_key().to_bech32().expect("root npub"),
        PeerReputationConfig::default(),
    )
    .expect("peer reputation");

    let unknown_peer = policy
        .select_mesh_peer(&unknown.public_key().to_bech32().expect("unknown npub"))
        .expect("unknown policy")
        .expect("unknown remains eligible");
    assert!(unknown_peer.is_unknown());

    assert!(
        reputation
            .ingest_event(&rating_event(
                &root,
                &root_hex,
                &good.public_key().to_hex(),
                100,
                now.saturating_sub(1),
            ))
            .expect("good rating")
    );
    let good_peer = policy
        .select_mesh_peer(&good.public_key().to_bech32().expect("good npub"))
        .expect("good policy")
        .expect("good remains eligible");
    assert!(good_peer.quality_score.is_some_and(|score| score > 0));
    assert!(
        reputation
            .ingest_event(&rating_event(
                &root,
                &root_hex,
                &bad.public_key().to_hex(),
                0,
                now,
            ))
            .expect("bad rating")
    );
    assert_eq!(
        policy
            .select_mesh_peer(&bad.public_key().to_bech32().expect("bad npub"))
            .expect("bad policy"),
        None
    );
}

#[test]
fn explicitly_trusted_remote_rater_changes_the_projection() {
    let root = Keys::generate();
    let rater = Keys::generate();
    let subject = Keys::generate();
    let rater_hex = rater.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let now = 2_000_000_000;
    let (mut reputation, policy) = PeerReputation::new(
        &root.public_key().to_hex(),
        PeerReputationConfig {
            trusted_raters: BTreeSet::from([rater.public_key().to_bech32().expect("rater npub")]),
            ..PeerReputationConfig::default()
        },
    )
    .expect("peer reputation");

    assert!(
        reputation
            .ingest_event_at(&rating_event(&rater, &rater_hex, &subject_hex, 0, now), now,)
            .expect("trusted remote rating")
    );
    assert_eq!(
        policy
            .select_mesh_peer(&subject_hex)
            .expect("trusted remote decision"),
        None
    );
}

#[test]
fn retained_untrusted_rating_activates_when_its_rater_becomes_reachable() {
    let root = Keys::generate();
    let rater = Keys::generate();
    let subject = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let rater_hex = rater.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let now = 2_000_000_000;
    let (mut reputation, policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default()).expect("reputation");

    assert!(
        reputation
            .ingest_event_at(&rating_event(&rater, &rater_hex, &subject_hex, 0, now), now,)
            .expect("retain untrusted rating")
    );
    assert!(
        policy
            .select_mesh_peer(&subject_hex)
            .expect("unknown subject")
            .expect("untrusted rating is initially inert")
            .is_unknown()
    );

    assert!(
        reputation
            .ingest_event_at(
                &rating_event(&root, &root_hex, &rater_hex, 100, now + 1),
                now + 1,
            )
            .expect("trust rater")
    );
    assert_eq!(
        policy
            .select_mesh_peer(&subject_hex)
            .expect("activated negative rating"),
        None
    );
}

#[test]
fn reputation_rejects_forgery_and_newest_rating_allows_recovery() {
    let root = Keys::generate();
    let peer = Keys::generate();
    let attacker = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let peer_hex = peer.public_key().to_hex();
    let peer_npub = peer.public_key().to_bech32().expect("peer npub");
    let now = now_unix_secs();
    let (mut reputation, policy) = PeerReputation::new(
        &root.public_key().to_bech32().expect("root npub"),
        PeerReputationConfig::default(),
    )
    .expect("peer reputation");

    let forged = rating_event(&attacker, &root_hex, &peer_hex, 0, now.saturating_sub(2));
    assert!(!reputation.ingest_event(&forged).expect("forged rating"));
    assert!(
        policy
            .select_mesh_peer(&peer_npub)
            .expect("unknown decision")
            .expect("unknown remains eligible")
            .is_unknown()
    );

    let negative = rating_event(&root, &root_hex, &peer_hex, 0, now.saturating_sub(1));
    assert!(reputation.ingest_event(&negative).expect("negative rating"));
    assert_eq!(policy.select_mesh_peer(&peer_npub).expect("negative"), None);

    let recovered = rating_event(&root, &root_hex, &peer_hex, 100, now);
    assert!(reputation.ingest_event(&recovered).expect("recovery"));
    assert!(
        policy
            .select_mesh_peer(&peer_npub)
            .expect("recovered decision")
            .expect("recovered remains eligible")
            .quality_score
            .is_some_and(|score| score > 0)
    );
    assert!(!reputation.ingest_event(&negative).expect("stale rating"));
}

#[tokio::test]
async fn author_admission_is_transport_neutral_and_allows_unknowns() {
    let root = Keys::generate();
    let good = Keys::generate();
    let unknown = Keys::generate();
    let bad = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let now = now_unix_secs();
    let (mut reputation, policies) = PeerReputation::new(
        &root.public_key().to_bech32().expect("root npub"),
        PeerReputationConfig::default(),
    )
    .expect("peer reputation");
    reputation
        .ingest_event(&rating_event(
            &root,
            &root_hex,
            &good.public_key().to_hex(),
            100,
            now.saturating_sub(1),
        ))
        .expect("good rating");
    reputation
        .ingest_event(&rating_event(
            &root,
            &root_hex,
            &bad.public_key().to_hex(),
            0,
            now,
        ))
        .expect("bad rating");

    let relay = EventSource::relay("wss://bootstrap.example");
    let good_event = EventBuilder::text_note("good")
        .sign_with_keys(&good)
        .expect("good event");
    let unknown_event = EventBuilder::text_note("unknown")
        .sign_with_keys(&unknown)
        .expect("unknown event");
    let bad_event = EventBuilder::text_note("bad")
        .sign_with_keys(&bad)
        .expect("bad event");

    assert!(matches!(
        policies.check_event(&good_event, &relay).await.unwrap(),
        PolicyDecision::Allow { priority } if priority > 0
    ));
    assert!(!matches!(
        policies.check_event(&unknown_event, &relay).await.unwrap(),
        PolicyDecision::Drop { .. }
    ));
    assert!(matches!(
        policies.check_event(&bad_event, &relay).await.unwrap(),
        PolicyDecision::Drop { .. }
    ));

    assert!(matches!(
        policies
            .check_event(&unknown_event, &EventSource::fips_endpoint("peer"))
            .await
            .unwrap(),
        PolicyDecision::Throttle { .. }
    ));
}

#[test]
fn publisher_coalesces_material_changes_and_refreshes() {
    let root = Keys::generate();
    let subject = Keys::generate().public_key().to_hex();
    let root_hex = root.public_key().to_hex();
    let config = PeerRatingPublisherConfig::default();
    let min_interval_ms = duration_ms(config.min_publish_interval);
    let refresh_interval_ms = duration_ms(config.refresh_interval);
    let mut publisher =
        PeerRatingPublisher::new(&root_hex, DEFAULT_PEER_RATING_SCOPE, config).expect("publisher");

    let first = rating_event_with_samples(&root, &root_hex, &subject, 80, 1, 3);
    assert!(publisher.should_publish_event(&first, 1_000));
    assert!(publisher.record_published_event(&first, 1_000));

    let small = rating_event_with_samples(&root, &root_hex, &subject, 85, 2, 3);
    assert!(!publisher.should_publish_event(&small, 2_000));
    let material = rating_event_with_samples(&root, &root_hex, &subject, 95, 3, 3);
    assert!(!publisher.should_publish_event(&material, 2_000));
    assert!(publisher.should_publish_event(&material, 1_000 + min_interval_ms));
    assert!(publisher.record_published_event(&material, 1_000 + min_interval_ms));

    let low_evidence_negative = rating_event_with_samples(&root, &root_hex, &subject, 0, 4, 1);
    assert!(!publisher.should_publish_event(&low_evidence_negative, 1_001 + min_interval_ms));
    let negative = rating_event_with_samples(&root, &root_hex, &subject, 0, 5, 3);
    assert!(publisher.should_publish_event(&negative, 1_001 + min_interval_ms));
    assert!(publisher.record_published_event(&negative, 1_001 + min_interval_ms));
    assert!(!publisher.should_publish_event(&negative, 2_000 + min_interval_ms));
    assert!(
        publisher.should_publish_event(&negative, 1_001 + min_interval_ms + refresh_interval_ms)
    );
}

#[test]
fn publisher_from_events_matches_explicit_wall_clock_replay() {
    let root = Keys::generate();
    let subject = Keys::generate().public_key().to_hex();
    let root_hex = root.public_key().to_hex();
    let now_ms = now_unix_secs().saturating_mul(1_000);
    let event = rating_event_with_samples(&root, &root_hex, &subject, 80, now_ms / 1_000, 3);
    let wall = PeerRatingPublisher::from_events(
        &root_hex,
        DEFAULT_PEER_RATING_SCOPE,
        PeerRatingPublisherConfig::default(),
        [&event],
    )
    .expect("wall-clock publisher replay");
    let explicit = PeerRatingPublisher::from_events_at(
        &root_hex,
        DEFAULT_PEER_RATING_SCOPE,
        PeerRatingPublisherConfig::default(),
        [&event],
        now_ms,
    )
    .expect("explicit-time publisher replay");

    assert_eq!(wall.published, explicit.published);
}

#[test]
fn publisher_from_events_at_prunes_against_supplied_time() {
    let root = Keys::generate();
    let subject = Keys::generate().public_key().to_hex();
    let root_hex = root.public_key().to_hex();
    let now_ms = 1_000_000;
    let event = rating_event_with_samples(&root, &root_hex, &subject, 80, now_ms / 1_000, 3);
    let retained = PeerRatingPublisher::from_events_at(
        &root_hex,
        DEFAULT_PEER_RATING_SCOPE,
        PeerRatingPublisherConfig::default(),
        [&event],
        now_ms,
    )
    .expect("explicit-time retained replay");
    let expired = PeerRatingPublisher::from_events_at(
        &root_hex,
        DEFAULT_PEER_RATING_SCOPE,
        PeerRatingPublisherConfig::default(),
        [&event],
        now_ms + duration_ms(PEER_RATING_MAX_AGE) + 1,
    )
    .expect("explicit-time expired replay");

    assert_eq!(
        retained
            .published
            .get(&subject)
            .expect("retained cadence")
            .published_at_ms,
        now_ms
    );
    assert!(expired.published.is_empty());
}

#[test]
fn reputation_rejects_expired_and_far_future_ratings() {
    let root = Keys::generate();
    let subject = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let now = now_unix_secs();
    let (mut reputation, policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default()).expect("peer reputation");

    let expired = rating_event(
        &root,
        &root_hex,
        &subject_hex,
        0,
        now.saturating_sub(PEER_RATING_MAX_AGE.as_secs() + 1),
    );
    assert!(!reputation.ingest_event(&expired).expect("expired rating"));

    let future = rating_event(
        &root,
        &root_hex,
        &subject_hex,
        0,
        now.saturating_add(PEER_RATING_MAX_FUTURE_SKEW.as_secs() + 1),
    );
    assert!(!reputation.ingest_event(&future).expect("future rating"));
    assert!(
        policy
            .select_mesh_peer(&subject_hex)
            .expect("unknown policy")
            .expect("rejected ratings leave subject unknown")
            .is_unknown()
    );
}

#[test]
fn explicit_time_ingest_rejects_future_ratings_and_expires_policy_state() {
    let root = Keys::generate();
    let subject = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let fixed_now = 2_000_000_000;
    let (mut reputation, policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default()).expect("peer reputation");

    let future = rating_event(
        &root,
        &root_hex,
        &subject_hex,
        0,
        fixed_now + PEER_RATING_MAX_FUTURE_SKEW.as_secs() + 1,
    );
    assert!(
        !reputation
            .ingest_event_at(&future, fixed_now)
            .expect("future rating")
    );

    let negative = rating_event(&root, &root_hex, &subject_hex, 0, fixed_now);
    assert!(
        reputation
            .ingest_event_at(&negative, fixed_now)
            .expect("negative rating")
    );
    assert_eq!(
        policy.select_mesh_peer(&subject_hex).expect("negative"),
        None
    );

    assert_eq!(
        reputation
            .replay_at(
                std::iter::empty::<&Event>(),
                fixed_now + PEER_RATING_MAX_AGE.as_secs() + 1,
            )
            .expect("expire rating"),
        0
    );
    assert!(
        policy
            .select_mesh_peer(&subject_hex)
            .expect("expired policy")
            .expect("expired peer becomes unknown")
            .is_unknown()
    );
}

#[test]
fn wall_clock_and_explicit_time_ingest_and_replay_are_equivalent() {
    let root = Keys::generate();
    let good = Keys::generate();
    let bad = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let now = now_unix_secs();
    let events = vec![
        rating_event(
            &root,
            &root_hex,
            &good.public_key().to_hex(),
            100,
            now.saturating_sub(2),
        ),
        rating_event(
            &root,
            &root_hex,
            &bad.public_key().to_hex(),
            0,
            now.saturating_sub(1),
        ),
    ];
    let (mut wall_ingest, wall_ingest_policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default())
            .expect("wall-clock ingest reputation");
    let (mut explicit_ingest, explicit_ingest_policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default())
            .expect("explicit ingest reputation");

    assert_eq!(
        wall_ingest
            .ingest_event(&events[0])
            .expect("wall-clock ingest"),
        explicit_ingest
            .ingest_event_at(&events[0], now)
            .expect("explicit ingest")
    );
    assert_eq!(
        wall_ingest_policy
            .select_mesh_peer(&good.public_key().to_hex())
            .expect("wall-clock ingest policy"),
        explicit_ingest_policy
            .select_mesh_peer(&good.public_key().to_hex())
            .expect("explicit ingest policy")
    );

    let (mut wall_replay, wall_replay_policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default())
            .expect("wall-clock replay reputation");
    let (mut explicit_replay, explicit_replay_policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default())
            .expect("explicit replay reputation");
    assert_eq!(
        wall_replay.replay(&events).expect("wall-clock replay"),
        explicit_replay
            .replay_at(&events, now)
            .expect("explicit replay")
    );
    for peer in [&good, &bad] {
        let peer_id = peer.public_key().to_hex();
        assert_eq!(
            wall_replay_policy
                .select_mesh_peer(&peer_id)
                .expect("wall-clock replay policy"),
            explicit_replay_policy
                .select_mesh_peer(&peer_id)
                .expect("explicit replay policy")
        );
    }
}

#[test]
fn reputation_prune_forgets_stale_policy_state() {
    let root = Keys::generate();
    let subject = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let subject_npub = subject.public_key().to_bech32().expect("subject npub");
    let created_at = now_unix_secs();
    let (mut reputation, policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default()).expect("peer reputation");

    assert!(
        reputation
            .ingest_event(&rating_event(&root, &root_hex, &subject_hex, 0, created_at,))
            .expect("negative rating")
    );
    assert_eq!(
        policy.select_mesh_peer(&subject_npub).expect("negative"),
        None
    );

    assert_eq!(
        reputation
            .prune(created_at + PEER_RATING_MAX_AGE.as_secs() + 1)
            .expect("prune reputation"),
        1
    );
    assert!(
        policy
            .select_mesh_peer(&subject_npub)
            .expect("forgotten policy")
            .expect("forgotten peer is eligible again")
            .is_unknown()
    );
}

#[test]
fn reputation_snapshot_tracks_retained_state_and_rebuild_work() {
    let root = Keys::generate();
    let trusted = Keys::generate();
    let subject = Keys::generate();
    let attacker = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let trusted_hex = trusted.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let now = 2_000_000_000;
    let (mut reputation, _) = PeerReputation::new(
        &root_hex,
        PeerReputationConfig {
            trusted_raters: BTreeSet::from([trusted_hex]),
            ..PeerReputationConfig::default()
        },
    )
    .expect("peer reputation");

    assert_eq!(
        reputation.snapshot(),
        PeerReputationSnapshot {
            trusted_roots: 2,
            ..PeerReputationSnapshot::default()
        }
    );

    let forged = rating_event(&attacker, &root_hex, &subject_hex, 0, now);
    let accepted = rating_event(&root, &root_hex, &subject_hex, 0, now);
    assert_eq!(
        reputation
            .replay_at([&forged, &accepted], now)
            .expect("adversarial replay"),
        1
    );
    assert_eq!(
        reputation.snapshot(),
        PeerReputationSnapshot {
            retained_ratings: 1,
            retained_raters: 1,
            trusted_roots: 2,
            rating_events_considered: 2,
            retained_rating_updates: 1,
            graph_rebuilds: 1,
            graph_rebuild_rating_entries: 1,
        }
    );

    assert!(
        !reputation
            .ingest_event_at(&accepted, now)
            .expect("stale duplicate")
    );
    let after_duplicate = reputation.snapshot();
    assert_eq!(after_duplicate.rating_events_considered, 3);
    assert_eq!(after_duplicate.retained_rating_updates, 1);
    assert_eq!(after_duplicate.graph_rebuilds, 1);

    assert_eq!(
        reputation
            .prune(now + PEER_RATING_MAX_AGE.as_secs() + 1)
            .expect("expire retained rating"),
        1
    );
    assert_eq!(
        reputation.snapshot(),
        PeerReputationSnapshot {
            trusted_roots: 2,
            rating_events_considered: 3,
            retained_rating_updates: 1,
            graph_rebuilds: 2,
            graph_rebuild_rating_entries: 1,
            ..PeerReputationSnapshot::default()
        }
    );
}

#[test]
fn publisher_prune_forgets_stale_subjects() {
    let root = Keys::generate();
    let subject = Keys::generate().public_key().to_hex();
    let root_hex = root.public_key().to_hex();
    let mut publisher = PeerRatingPublisher::new(
        &root_hex,
        DEFAULT_PEER_RATING_SCOPE,
        PeerRatingPublisherConfig::default(),
    )
    .expect("publisher");
    let event = rating_event_with_samples(&root, &root_hex, &subject, 80, 1, 3);

    assert!(publisher.record_published_event(&event, 1_000));
    assert_eq!(
        publisher.prune(1_000 + duration_ms(PEER_RATING_MAX_AGE) + 1),
        1
    );
    assert!(publisher.should_publish_event(&event, 1_000 + duration_ms(PEER_RATING_MAX_AGE) + 1));
}

#[test]
fn untrusted_rating_flood_snapshot_stays_bounded_and_accounts_rebuild_entries() {
    let root = deterministic_keys(40_000).public_key().to_hex();
    let rater = deterministic_keys(40_001);
    let rater_hex = rater.public_key().to_hex();
    let now = 2_000_000_000;
    let (mut reputation, _) =
        PeerReputation::new(&root, PeerReputationConfig::default()).expect("reputation");
    let events = (0..=PEER_RATING_MAX_ENTRIES_PER_RATER)
        .map(|index| {
            rating_event(
                &rater,
                &rater_hex,
                &deterministic_keys(41_000 + index).public_key().to_hex(),
                100,
                now - (PEER_RATING_MAX_ENTRIES_PER_RATER - index) as u64,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        reputation
            .replay_at(&events, now)
            .expect("bounded untrusted flood"),
        events.len()
    );
    assert_eq!(
        reputation.snapshot(),
        PeerReputationSnapshot {
            retained_ratings: PEER_RATING_MAX_ENTRIES_PER_RATER,
            retained_raters: 1,
            trusted_roots: 1,
            rating_events_considered: events.len() as u64,
            retained_rating_updates: events.len() as u64,
            graph_rebuilds: 1,
            graph_rebuild_rating_entries: PEER_RATING_MAX_ENTRIES_PER_RATER as u64,
        }
    );
}

#[test]
fn reputation_enforces_total_and_per_rater_bounds() {
    let root = Keys::generate().public_key().to_hex();
    let (mut reputation, _) =
        PeerReputation::new(&root, PeerReputationConfig::default()).expect("reputation");

    for index in 0..=PEER_RATING_MAX_ENTRIES_PER_RATER {
        insert_stored_rating(&mut reputation, "one-rater", index, index as u64);
    }
    reputation.enforce_entry_limits();
    assert_eq!(
        reputation
            .latest
            .keys()
            .filter(|key| key.rater == "one-rater")
            .count(),
        PEER_RATING_MAX_ENTRIES_PER_RATER
    );

    reputation.latest.clear();
    for index in 0..=PEER_RATING_MAX_ENTRIES {
        insert_stored_rating(
            &mut reputation,
            &format!("rater-{index}"),
            index,
            index as u64,
        );
    }
    reputation.enforce_entry_limits();
    assert_eq!(reputation.latest.len(), PEER_RATING_MAX_ENTRIES);
}

#[test]
fn untrusted_rating_flood_cannot_evict_trust_anchor_ratings() {
    const TEST_MAX_ENTRIES: usize = 8;
    const TEST_MAX_ENTRIES_PER_RATER: usize = 4;
    let root = Keys::generate();
    let trusted = Keys::generate();
    let root_subject = Keys::generate();
    let trusted_subject = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let trusted_hex = trusted.public_key().to_hex();
    let now = now_unix_secs();
    let (mut reputation, policy) = PeerReputation::new(
        &root_hex,
        PeerReputationConfig {
            trusted_raters: BTreeSet::from([trusted_hex.clone()]),
            ..PeerReputationConfig::default()
        },
    )
    .expect("reputation");
    let sybil_raters = (1..=2)
        .map(|index| deterministic_keys(10_000 + index))
        .collect::<Vec<_>>();
    let subjects = (0..TEST_MAX_ENTRIES_PER_RATER)
        .map(|index| deterministic_keys(20_000 + index).public_key().to_hex())
        .collect::<Vec<_>>();
    let sybil_count = TEST_MAX_ENTRIES - 1;
    let mut events = Vec::with_capacity(sybil_count + 2);
    events.push(rating_event(
        &root,
        &root_hex,
        &root_subject.public_key().to_hex(),
        0,
        now.saturating_sub(2),
    ));
    events.push(rating_event(
        &trusted,
        &trusted_hex,
        &trusted_subject.public_key().to_hex(),
        0,
        now.saturating_sub(2),
    ));
    for index in 0..sybil_count {
        let rater = &sybil_raters[index / TEST_MAX_ENTRIES_PER_RATER];
        events.push(rating_event(
            rater,
            &rater.public_key().to_hex(),
            &subjects[index % TEST_MAX_ENTRIES_PER_RATER],
            100,
            now.saturating_sub(1),
        ));
    }

    assert_eq!(
        reputation
            .replay_at(&events, now)
            .expect("replay signed Sybil flood"),
        events.len()
    );
    reputation.enforce_entry_limits_with(TEST_MAX_ENTRIES, TEST_MAX_ENTRIES_PER_RATER);
    reputation.rebuild().expect("rebuild bounded projection");

    assert_eq!(reputation.latest.len(), TEST_MAX_ENTRIES);
    for subject in [&root_subject, &trusted_subject] {
        assert_eq!(
            policy
                .select_mesh_peer(&subject.public_key().to_hex())
                .expect("anchor rating policy"),
            None
        );
    }
}

#[test]
fn global_capacity_evicts_oldest_when_only_anchor_ratings_remain() {
    let root = deterministic_keys(30_000).public_key().to_hex();
    let trusted = (1..=2)
        .map(|index| deterministic_keys(30_000 + index).public_key().to_hex())
        .collect::<BTreeSet<_>>();
    let (mut reputation, _) = PeerReputation::new(
        &root,
        PeerReputationConfig {
            trusted_raters: trusted.clone(),
            ..PeerReputationConfig::default()
        },
    )
    .expect("reputation");
    let oldest = PeerRatingKey {
        rater: root.clone(),
        subject: "subject-0".to_string(),
        scope: DEFAULT_PEER_RATING_SCOPE.to_string(),
    };
    for (rater_index, rater) in std::iter::once(&root).chain(trusted.iter()).enumerate() {
        let count = if rater_index < 2 { 2 } else { 1 };
        for index in 0..count {
            insert_stored_rating(
                &mut reputation,
                rater,
                index,
                (rater_index * 2 + index) as u64,
            );
        }
    }

    reputation.enforce_entry_limits_with(4, 2);

    assert_eq!(reputation.latest.len(), 4);
    assert!(!reputation.latest.contains_key(&oldest));
}

#[test]
fn publisher_enforces_subject_bound() {
    let root = Keys::generate().public_key().to_hex();
    let mut publisher = PeerRatingPublisher::new(
        &root,
        DEFAULT_PEER_RATING_SCOPE,
        PeerRatingPublisherConfig::default(),
    )
    .expect("publisher");
    let now = duration_ms(PEER_RATING_MAX_AGE);
    for index in 0..=PEER_RATING_MAX_ENTRIES_PER_RATER {
        publisher.published.insert(
            format!("subject-{index}"),
            PublishedPeerRating {
                score: 0,
                class: PeerRatingClass::Neutral,
                published_at_ms: now.saturating_sub(index as u64),
            },
        );
    }

    assert_eq!(publisher.prune(now), 1);
    assert_eq!(publisher.published.len(), PEER_RATING_MAX_ENTRIES_PER_RATER);
}

fn insert_stored_rating(
    reputation: &mut PeerReputation,
    rater: &str,
    index: usize,
    created_at: u64,
) {
    let subject = format!("subject-{index}");
    let mut rating = Rating::new(rater, &subject, 50, 0, 100);
    rating.scope = Some(DEFAULT_PEER_RATING_SCOPE.to_string());
    rating.created_at = created_at;
    reputation.latest.insert(
        PeerRatingKey {
            rater: rater.to_string(),
            subject,
            scope: DEFAULT_PEER_RATING_SCOPE.to_string(),
        },
        StoredPeerRating {
            event_id: format!("event-{index}"),
            rating,
        },
    );
}

fn deterministic_keys(value: usize) -> Keys {
    Keys::parse(&format!("{value:064x}")).expect("deterministic test key")
}

fn rating_event(signer: &Keys, rater: &str, subject: &str, value: i64, created_at: u64) -> Event {
    rating_event_with_samples(signer, rater, subject, value, created_at, 1)
}

fn rating_event_with_samples(
    signer: &Keys,
    rater: &str,
    subject: &str,
    value: i64,
    created_at: u64,
    samples: u64,
) -> Event {
    let mut rating = Rating::new(rater, subject, value, 0, 100);
    rating.scope = Some(DEFAULT_PEER_RATING_SCOPE.to_string());
    rating.created_at = created_at;
    rating.sample_count = Some(samples);
    rating.to_event(signer).expect("signed rating")
}
