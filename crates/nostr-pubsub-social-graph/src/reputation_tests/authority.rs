use super::*;

#[test]
fn rating_filter_matches_only_explicit_authorities_and_scope() {
    use nostr_pubsub::{PubsubPeerInterest, VerifiedEvent};

    let root = Keys::generate();
    let rater = Keys::generate();
    let service_peer = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let peer_hex = service_peer.public_key().to_hex();
    let now = 2_000_000_000;
    let (mut reputation, _) = PeerReputation::new(
        &root_hex,
        PeerReputationConfig {
            trusted_raters: BTreeSet::from([
                root.public_key().to_bech32().unwrap(),
                rater.public_key().to_bech32().unwrap(),
                rater.public_key().to_hex(),
            ]),
            ..PeerReputationConfig::default()
        },
    )
    .unwrap();
    reputation
        .ingest_event_at(&rating_event(&root, &root_hex, &peer_hex, 100, now), now)
        .unwrap();
    let filter = reputation.rating_filter().unwrap();
    assert_eq!(filter.authors.as_ref().unwrap().len(), 2);
    for (signer, expected) in [(&root, true), (&rater, true), (&service_peer, false)] {
        let event = rating_event(signer, &signer.public_key().to_hex(), &peer_hex, 0, now);
        let verified = VerifiedEvent::try_from(event).unwrap();
        assert_eq!(
            PubsubPeerInterest::from_filters(std::slice::from_ref(&filter), &verified)
                == PubsubPeerInterest::Subscribed,
            expected
        );
    }
    let (other_scope, _) = PeerReputation::new(
        &root_hex,
        PeerReputationConfig {
            scope: "different.scope".to_owned(),
            ..PeerReputationConfig::default()
        },
    )
    .unwrap();
    let default_filter = other_scope.rating_filter().unwrap();
    assert_eq!(default_filter.authors.as_ref().unwrap().len(), 1);
    let event = VerifiedEvent::try_from(rating_event(&root, &root_hex, &peer_hex, 0, now)).unwrap();
    assert_eq!(
        PubsubPeerInterest::from_filters(&[default_filter], &event),
        PubsubPeerInterest::Unsubscribed
    );
}

#[test]
fn positive_peer_rating_does_not_delegate_rating_authority() {
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
    assert!(
        policy
            .select_mesh_peer(&subject_hex)
            .expect("subject after positive peer rating")
            .expect("good service does not authorize third-party ratings")
            .is_unknown()
    );
    let rebuilds = reputation.snapshot().graph_rebuilds;
    let newer_negative = rating_event(&rater, &rater_hex, &subject_hex, 0, now + 2);
    assert!(
        reputation
            .ingest_event_at(&newer_negative, now + 2)
            .expect("retain newer untrusted rating")
    );
    assert_eq!(reputation.snapshot().graph_rebuilds, rebuilds);

    let (mut configured, configured_policy) = PeerReputation::new(
        &root_hex,
        PeerReputationConfig {
            trusted_raters: BTreeSet::from([rater_hex]),
            ..PeerReputationConfig::default()
        },
    )
    .expect("explicitly authorize rater");
    configured
        .replay_at([&newer_negative], now + 2)
        .expect("replay with explicit authority");
    assert_eq!(
        configured_policy.select_mesh_peer(&subject_hex).unwrap(),
        None
    );
}

#[test]
fn configured_rater_cannot_delegate_and_root_revocation_survives_replay() {
    let root = Keys::generate();
    let rater = Keys::generate();
    let peer = Keys::generate();
    let subject = Keys::generate();
    let root_hex = root.public_key().to_hex();
    let rater_hex = rater.public_key().to_hex();
    let peer_hex = peer.public_key().to_hex();
    let subject_hex = subject.public_key().to_hex();
    let now = 2_000_000_000;
    let config = PeerReputationConfig {
        trusted_raters: BTreeSet::from([rater_hex.clone()]),
        ..PeerReputationConfig::default()
    };
    let endorsement = rating_event(&rater, &rater_hex, &peer_hex, 100, now);
    let peer_negative = rating_event(&peer, &peer_hex, &subject_hex, 0, now);
    let rater_negative = rating_event(&rater, &rater_hex, &subject_hex, 0, now + 1);
    let revocation = rating_event(&root, &root_hex, &rater_hex, 0, now + 2);
    let (mut reputation, policy) =
        PeerReputation::new(&root_hex, config.clone()).expect("configured rater");

    reputation
        .replay_at([&endorsement, &peer_negative], now)
        .expect("replay peer endorsement and third-party rating");
    assert!(
        policy
            .select_mesh_peer(&peer_hex)
            .unwrap()
            .unwrap()
            .quality_score
            .is_some_and(|score| score > 0)
    );
    assert!(
        policy
            .select_mesh_peer(&subject_hex)
            .unwrap()
            .unwrap()
            .is_unknown()
    );
    reputation
        .ingest_event_at(&rater_negative, now + 1)
        .unwrap();
    assert_eq!(policy.select_mesh_peer(&subject_hex).unwrap(), None);
    reputation.ingest_event_at(&revocation, now + 2).unwrap();
    assert!(
        policy
            .select_mesh_peer(&subject_hex)
            .unwrap()
            .unwrap()
            .is_unknown()
    );

    let events = [&peer_negative, &rater_negative, &endorsement, &revocation];
    let (mut restarted, restarted_policy) =
        PeerReputation::new(&root_hex, config).expect("restart configured rater");
    restarted.replay_at(events, now + 2).unwrap();
    assert!(
        restarted_policy
            .select_mesh_peer(&subject_hex)
            .unwrap()
            .unwrap()
            .is_unknown()
    );

    let (mut removed, removed_policy) =
        PeerReputation::new(&root_hex, PeerReputationConfig::default())
            .expect("remove configured authority");
    removed
        .replay_at(events[..3].iter().copied(), now + 2)
        .unwrap();
    assert!(
        removed_policy
            .select_mesh_peer(&subject_hex)
            .unwrap()
            .unwrap()
            .is_unknown()
    );
}
