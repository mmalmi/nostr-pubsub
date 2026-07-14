use std::collections::BTreeSet;

use nostr::{Event, EventBuilder, Keys, Kind};
use nostr_pubsub::{
    InvWantAction, InvWantCodec, InvWantMesh, InvWantMeshOptions, InvWantWireMessage, MeshPeer,
};

const PROTOCOL: &str = "nvpn.control.pubsub";
const VERSION: u8 = 1;
const MAX_WIRE_BYTES: usize = 60 * 1024;

#[test]
fn codec_preserves_the_deployed_nvpn_v1_envelope() {
    let codec = InvWantCodec::new(PROTOCOL, VERSION, MAX_WIRE_BYTES);
    let event_id = "01".repeat(32);
    let encoded = codec
        .encode(&InvWantWireMessage::Want {
            event_id: event_id.clone(),
        })
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();

    assert_eq!(value["protocol"], PROTOCOL);
    assert_eq!(value["version"], VERSION);
    assert_eq!(value["message"]["type"], "want");
    assert_eq!(value["message"]["event_id"], event_id);
    assert_eq!(
        codec.decode(&encoded).unwrap(),
        InvWantWireMessage::Want { event_id }
    );
}

#[test]
fn production_mesh_delivers_once_across_three_hops() {
    let event = signed_event();
    let event_id = event.id.to_hex();
    let mut alice = mesh();
    let mut bob = mesh();
    let mut carol = mesh();

    let inventory_for_bob = only_message(alice.publish(event, &[MeshPeer::new("bob")], 1).unwrap());
    let bob_actions = bob
        .receive(
            "alice",
            inventory_for_bob,
            &[MeshPeer::new("alice"), MeshPeer::new("carol")],
            2,
        )
        .unwrap();
    let want_for_alice = message_for(&bob_actions, "alice");
    assert_eq!(
        bob_actions.len(),
        1,
        "inventory is not forwarded before proof"
    );

    let frame_for_bob = only_message(
        alice
            .receive("bob", want_for_alice, &[MeshPeer::new("bob")], 3)
            .unwrap(),
    );
    let bob_frame_actions = bob
        .receive(
            "alice",
            frame_for_bob,
            &[MeshPeer::new("alice"), MeshPeer::new("carol")],
            4,
        )
        .unwrap();
    assert_eq!(delivered_ids(&bob_frame_actions), vec![event_id.clone()]);
    let inventory_for_carol = message_for(&bob_frame_actions, "carol");

    let carol_actions = carol
        .receive("bob", inventory_for_carol, &[MeshPeer::new("bob")], 5)
        .unwrap();
    assert_eq!(carol_actions.len(), 1, "Carol wants before forwarding");
    let want_for_bob = message_for(&carol_actions, "bob");
    let frame_for_carol = only_message(
        bob.receive(
            "carol",
            want_for_bob,
            &[MeshPeer::new("alice"), MeshPeer::new("carol")],
            6,
        )
        .unwrap(),
    );

    let carol_frame_actions = carol
        .receive("bob", frame_for_carol.clone(), &[MeshPeer::new("bob")], 7)
        .unwrap();
    assert_eq!(delivered_ids(&carol_frame_actions), vec![event_id]);
    assert!(
        carol
            .receive("bob", frame_for_carol.clone(), &[MeshPeer::new("bob")], 8,)
            .unwrap()
            .is_empty(),
        "a duplicate answer to a recent WANT is harmless"
    );
    carol.maintain(120_008);
    assert!(
        carol
            .receive("bob", frame_for_carol, &[MeshPeer::new("bob")], 120_009)
            .is_err(),
        "fulfilled request provenance is bounded by the route TTL"
    );
}

#[test]
fn unproven_inventory_is_never_amplified() {
    let mut mesh = mesh();
    let event_id = "ab".repeat(32);
    let inventory = InvWantWireMessage::Inventory {
        event_id: event_id.clone(),
        event_kind: 37_195,
        payload_bytes: 512,
        hop_limit: 4,
    };
    let peers = [
        MeshPeer::new("attacker"),
        MeshPeer::new("honest-a"),
        MeshPeer::new("honest-b"),
    ];
    let actions = mesh
        .receive("attacker", inventory.clone(), &peers, 1)
        .unwrap();

    let want = vec![InvWantAction::Send {
        peer_id: "attacker".to_string(),
        message: InvWantWireMessage::Want { event_id },
    }];
    assert_eq!(actions, want);
    assert_eq!(
        mesh.receive("attacker", inventory, &peers, 2).unwrap(),
        want,
        "an inventory retry must regenerate a lost WANT without fanout"
    );
}

#[test]
fn alternate_providers_recover_blackholes_without_unbounded_wants() {
    let event = signed_event();
    let event_id = event.id.to_hex();
    let inventory = inventory_for(&event, 4);
    let mut consumer = mesh();

    assert!(
        consumer
            .receive(
                "unsolicited",
                InvWantWireMessage::Frame {
                    event_id: event_id.clone(),
                    event: Box::new(event.clone()),
                },
                &[],
                1,
            )
            .is_err()
    );
    for (now, provider, hop_limit) in [(2, "blackhole", 2), (3, "honest", 4), (4, "backup", 3)] {
        assert_eq!(
            consumer
                .receive(provider, inventory_for(&event, hop_limit), &[], now)
                .unwrap(),
            vec![InvWantAction::Send {
                peer_id: provider.to_string(),
                message: InvWantWireMessage::Want {
                    event_id: event_id.clone(),
                },
            }]
        );
    }
    assert!(
        consumer
            .receive("excess", inventory, &[], 5)
            .unwrap()
            .is_empty()
    );
    assert!(
        consumer
            .receive(
                "excess",
                InvWantWireMessage::Frame {
                    event_id: event_id.clone(),
                    event: Box::new(event.clone()),
                },
                &[],
                6,
            )
            .is_err(),
        "a provider that was not sent a WANT cannot serve a frame"
    );

    let actions = consumer
        .receive(
            "honest",
            InvWantWireMessage::Frame {
                event_id: event_id.clone(),
                event: Box::new(event.clone()),
            },
            &[],
            7,
        )
        .unwrap();
    assert_eq!(delivered_ids(&actions).len(), 1);
    assert!(
        consumer
            .receive(
                "backup",
                InvWantWireMessage::Frame {
                    event_id,
                    event: Box::new(event),
                },
                &[],
                8,
            )
            .unwrap()
            .is_empty(),
        "a late answer from another requested provider is not malicious"
    );

    assert_alternate_can_answer_before_route_expiry();
}

fn assert_alternate_can_answer_before_route_expiry() {
    let mut consumer = InvWantMesh::new(InvWantMeshOptions {
        route_ttl_ms: 10,
        event_ttl_ms: 20,
        max_hops: 4,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    });
    let event = signed_event();
    consumer
        .receive("blackhole", inventory_for(&event, 4), &[], 1)
        .unwrap();
    consumer
        .receive("honest", inventory_for(&event, 4), &[], 9)
        .unwrap();
    let actions = consumer
        .receive(
            "honest",
            InvWantWireMessage::Frame {
                event_id: event.id.to_hex(),
                event: Box::new(event),
            },
            &[],
            15,
        )
        .unwrap();
    assert_eq!(delivered_ids(&actions).len(), 1);
}

#[test]
fn fulfilled_routes_absorb_late_requested_frames_without_scoring() {
    let mut consumer = InvWantMesh::new(InvWantMeshOptions {
        max_hops: 4,
        route_ttl_ms: 10,
        event_ttl_ms: 20,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    });

    for sequence in 0..3 {
        let event = signed_event();
        let event_id = event.id.to_hex();
        let now = sequence * 4 + 1;
        consumer
            .receive("primary", inventory_for(&event, 2), &[], now)
            .unwrap();
        consumer
            .receive("alternate", inventory_for(&event, 4), &[], now + 1)
            .unwrap();
        let delivered = delivered_ids(
            &consumer
                .receive(
                    "primary",
                    InvWantWireMessage::Frame {
                        event_id: event_id.clone(),
                        event: Box::new(event.clone()),
                    },
                    &[],
                    now + 2,
                )
                .unwrap(),
        );
        assert_eq!(delivered.as_slice(), std::slice::from_ref(&event_id));
        assert!(
            consumer
                .receive(
                    "alternate",
                    InvWantWireMessage::Frame {
                        event_id,
                        event: Box::new(event),
                    },
                    &[],
                    now + 3,
                )
                .unwrap()
                .is_empty()
        );
    }
    consumer.maintain(30);
    assert_eq!(consumer.peer_behavior_observation("alternate"), None);
    assert_eq!(
        consumer
            .peer_behavior_observation("primary")
            .map(|observation| (
                observation.valid_frames,
                observation.invalid_messages,
                observation.unserved_inventories,
            )),
        Some((3, 0, 0))
    );

    let rejected = signed_event();
    let rejected_id = rejected.id.to_hex();
    consumer
        .receive("primary", inventory_for(&rejected, 2), &[], 31)
        .unwrap();
    consumer
        .receive("alternate", inventory_for(&rejected, 4), &[], 32)
        .unwrap();
    consumer.dismiss_frame("primary", &rejected_id);
    assert!(
        consumer
            .receive(
                "alternate",
                InvWantWireMessage::Frame {
                    event_id: rejected_id.clone(),
                    event: Box::new(rejected.clone()),
                },
                &[],
                33,
            )
            .unwrap()
            .is_empty()
    );
    assert!(
        consumer
            .receive(
                "unrequested",
                InvWantWireMessage::Frame {
                    event_id: rejected_id,
                    event: Box::new(rejected),
                },
                &[],
                34,
            )
            .is_err()
    );
    consumer.maintain(50);
    assert_eq!(consumer.peer_behavior_observation("alternate"), None);
}

#[test]
fn transient_routes_are_evicted_atomically_and_route_less_wants_are_forgotten() {
    let event_a = signed_event();
    let event_b = signed_event();
    let event_a_id = event_a.id.to_hex();
    let options = InvWantMeshOptions {
        max_seen_events: 1,
        max_hops: 4,
        route_ttl_ms: 10,
        event_ttl_ms: 20,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    };
    let mut consumer = InvWantMesh::new(options);

    assert!(
        consumer
            .receive(
                "ghost",
                InvWantWireMessage::Want {
                    event_id: event_a_id.clone(),
                },
                &[],
                1,
            )
            .unwrap()
            .is_empty()
    );
    consumer
        .receive("provider-a", inventory_for(&event_a, 4), &[], 2)
        .unwrap();
    consumer
        .receive(
            "waiting",
            InvWantWireMessage::Want {
                event_id: event_a_id.clone(),
            },
            &[],
            3,
        )
        .unwrap();
    consumer
        .receive("provider-b", inventory_for(&event_b, 4), &[], 4)
        .unwrap();
    assert!(
        consumer
            .receive(
                "provider-a",
                InvWantWireMessage::Frame {
                    event_id: event_a_id.clone(),
                    event: Box::new(event_a.clone()),
                },
                &[],
                5,
            )
            .is_err(),
        "evicting the seen ID must evict its route and WANT state"
    );

    consumer
        .receive("provider-a", inventory_for(&event_a, 4), &[], 6)
        .unwrap();
    let actions = consumer
        .receive(
            "provider-a",
            InvWantWireMessage::Frame {
                event_id: event_a_id,
                event: Box::new(event_a),
            },
            &[],
            7,
        )
        .unwrap();
    assert!(actions.iter().all(|action| !matches!(
        action,
        InvWantAction::Send {
            peer_id,
            message: InvWantWireMessage::Frame { .. }
        } if peer_id == "ghost" || peer_id == "waiting"
    )));
}

#[test]
fn expired_routes_forget_pending_wants() {
    let event = signed_event();
    let event_id = event.id.to_hex();
    let mut consumer = InvWantMesh::new(InvWantMeshOptions {
        max_seen_events: 1,
        max_hops: 4,
        route_ttl_ms: 10,
        event_ttl_ms: 20,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    });
    consumer
        .receive("provider", inventory_for(&event, 4), &[], 1)
        .unwrap();
    consumer
        .receive(
            "waiting-after-ttl",
            InvWantWireMessage::Want {
                event_id: event_id.clone(),
            },
            &[],
            9,
        )
        .unwrap();
    consumer.maintain(12);
    consumer
        .receive("provider", inventory_for(&event, 4), &[], 13)
        .unwrap();
    let actions = consumer
        .receive(
            "provider",
            InvWantWireMessage::Frame {
                event_id,
                event: Box::new(event),
            },
            &[],
            14,
        )
        .unwrap();
    assert!(actions.iter().all(|action| !matches!(
        action,
        InvWantAction::Send { peer_id, message: InvWantWireMessage::Frame { .. } }
            if peer_id == "waiting-after-ttl"
    )));
}

#[test]
fn inventories_require_canonical_ids_and_local_hop_bounds() {
    let mut consumer = mesh();
    assert!(
        consumer
            .receive(
                "peer",
                InvWantWireMessage::Inventory {
                    event_id: "AB".repeat(32),
                    event_kind: 37_195,
                    payload_bytes: 512,
                    hop_limit: 4,
                },
                &[],
                1,
            )
            .is_err()
    );
    assert!(
        consumer
            .receive(
                "peer",
                InvWantWireMessage::Inventory {
                    event_id: "ab".repeat(32),
                    event_kind: 37_195,
                    payload_bytes: 512,
                    hop_limit: 5,
                },
                &[],
                2,
            )
            .is_err()
    );
}

#[test]
fn cached_event_can_be_replayed_to_a_peer_that_connected_later() {
    let event = signed_event();
    let event_id = event.id.to_hex();
    let expected_payload_bytes = u32::try_from(serde_json::to_vec(&event).unwrap().len()).unwrap();
    let mut provider = mesh();

    assert!(provider.publish(event.clone(), &[], 1).unwrap().is_empty());
    let inventory = only_message(
        provider
            .replay_to_peer(event, "late-peer", 20 * 60 * 1_000)
            .unwrap(),
    );
    assert_eq!(
        inventory,
        InvWantWireMessage::Inventory {
            event_id: event_id.clone(),
            event_kind: 37_195,
            payload_bytes: expected_payload_bytes,
            hop_limit: 4,
        }
    );

    let frame = provider
        .receive(
            "late-peer",
            InvWantWireMessage::Want { event_id },
            &[MeshPeer::new("late-peer")],
            20 * 60 * 1_000 + 1,
        )
        .unwrap();
    assert!(matches!(
        frame.as_slice(),
        [InvWantAction::Send {
            peer_id,
            message: InvWantWireMessage::Frame { .. },
        }] if peer_id == "late-peer"
    ));
}

#[test]
fn behavioral_priority_reserves_fanout_for_an_unknown_peer() {
    let options = InvWantMeshOptions {
        fanout: 3,
        unknown_peer_reserve: 1,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    };
    let mut mesh = InvWantMesh::new(options);
    let peers = [
        MeshPeer::observed("good-a", 100),
        MeshPeer::observed("good-b", 90),
        MeshPeer::observed("good-c", 80),
        MeshPeer::observed("bad", -100),
        MeshPeer::new("newcomer"),
    ];

    let actions = mesh.publish(signed_event(), &peers, 1).unwrap();
    let selected = actions
        .iter()
        .filter_map(|action| match action {
            InvWantAction::Send { peer_id, .. } => Some(peer_id.as_str()),
            InvWantAction::Deliver { .. } => None,
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(selected, BTreeSet::from(["good-a", "good-b", "newcomer"]));
}

#[test]
fn local_pubsub_behavior_learns_good_and_bad_without_classifying_newcomers() {
    let options = InvWantMeshOptions {
        fanout: 2,
        unknown_peer_reserve: 1,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    };
    let mut mesh = InvWantMesh::new(options);
    let peers = [
        MeshPeer::new("useful"),
        MeshPeer::new("malformed"),
        MeshPeer::new("newcomer"),
    ];

    for now in 1..=3 {
        let event = signed_event();
        let event_id = event.id.to_hex();
        let payload_bytes = u32::try_from(serde_json::to_vec(&event).unwrap().len()).unwrap();
        let inventory = InvWantWireMessage::Inventory {
            event_id: event_id.clone(),
            event_kind: 37_195,
            payload_bytes,
            hop_limit: 4,
        };
        assert_eq!(
            mesh.receive("useful", inventory, &peers, now * 2)
                .unwrap()
                .len(),
            1
        );
        mesh.receive(
            "useful",
            InvWantWireMessage::Frame {
                event_id,
                event: Box::new(event),
            },
            &peers,
            now * 2 + 1,
        )
        .unwrap();
    }
    for _ in 0..3 {
        mesh.record_invalid_message("malformed");
    }

    assert!(
        mesh.peer_behavior_score("useful")
            .is_some_and(|score| score > 0)
    );
    assert!(
        mesh.peer_behavior_score("malformed")
            .is_some_and(|score| score < 0)
    );
    assert_eq!(
        mesh.peer_behavior_observation("malformed")
            .map(|observation| observation.samples),
        Some(3)
    );
    assert_eq!(
        mesh.peer_behavior_observation("malformed")
            .map(|observation| observation.invalid_messages),
        Some(3)
    );
    assert_eq!(mesh.peer_behavior_score("newcomer"), None);

    let selected = mesh
        .publish(signed_event(), &peers, 100)
        .unwrap()
        .into_iter()
        .filter_map(|action| match action {
            InvWantAction::Send { peer_id, .. } => Some(peer_id),
            InvWantAction::Deliver { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        selected,
        BTreeSet::from(["newcomer".to_string(), "useful".to_string()])
    );
}

#[test]
fn provider_scoring_penalizes_irrelevant_and_unserved_inventories_but_not_silence() {
    let options = InvWantMeshOptions {
        fanout: 1,
        unknown_peer_reserve: 1,
        route_ttl_ms: 10,
        event_ttl_ms: 20,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    };

    let mut silent = InvWantMesh::new(options.clone());
    silent
        .publish(signed_event(), &[MeshPeer::new("silent")], 1)
        .unwrap();
    silent
        .publish(signed_event(), &[MeshPeer::new("silent")], 20)
        .unwrap();
    assert_eq!(silent.peer_behavior_score("silent"), None);

    let mut irrelevant = InvWantMesh::new(options.clone());
    for now in 1..=3 {
        irrelevant
            .receive(
                "irrelevant",
                InvWantWireMessage::Inventory {
                    event_id: format!("{:064x}", now + 10),
                    event_kind: 1,
                    payload_bytes: 512,
                    hop_limit: 4,
                },
                &[MeshPeer::new("irrelevant")],
                now,
            )
            .unwrap_err();
    }
    assert!(
        irrelevant
            .peer_behavior_score("irrelevant")
            .is_some_and(|score| score < 0)
    );

    let mut blackhole = InvWantMesh::new(options);
    for now in 1..=3 {
        blackhole
            .receive(
                "blackhole",
                InvWantWireMessage::Inventory {
                    event_id: format!("{now:064x}"),
                    event_kind: 37_195,
                    payload_bytes: 512,
                    hop_limit: 4,
                },
                &[MeshPeer::new("blackhole")],
                now,
            )
            .unwrap();
    }
    blackhole.maintain(20);
    assert!(
        blackhole
            .peer_behavior_score("blackhole")
            .is_some_and(|score| score < 0)
    );
    assert_eq!(
        blackhole
            .peer_behavior_observation("blackhole")
            .map(|observation| observation.unserved_inventories),
        Some(3)
    );

    let mut locally_rejected = InvWantMesh::new(InvWantMeshOptions {
        fanout: 1,
        route_ttl_ms: 10,
        event_ttl_ms: 20,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    });
    let rejected_id = "ff".repeat(32);
    locally_rejected
        .receive(
            "provider",
            InvWantWireMessage::Inventory {
                event_id: rejected_id.clone(),
                event_kind: 37_195,
                payload_bytes: 512,
                hop_limit: 4,
            },
            &[MeshPeer::new("provider")],
            1,
        )
        .unwrap();
    locally_rejected.dismiss_frame("provider", &rejected_id);
    locally_rejected
        .publish(signed_event(), &[MeshPeer::new("provider")], 20)
        .unwrap();
    assert_eq!(locally_rejected.peer_behavior_score("provider"), None);
}

fn mesh() -> InvWantMesh {
    let options = InvWantMeshOptions {
        fanout: 8,
        max_hops: 4,
        allowed_kinds: Some(BTreeSet::from([37_195])),
        ..InvWantMeshOptions::default()
    };
    InvWantMesh::new(options)
}

fn signed_event() -> Event {
    EventBuilder::new(Kind::Custom(37_195), "peer advert")
        .sign_with_keys(&Keys::generate())
        .unwrap()
}

fn inventory_for(event: &Event, hop_limit: u8) -> InvWantWireMessage {
    InvWantWireMessage::Inventory {
        event_id: event.id.to_hex(),
        event_kind: u16::from(event.kind),
        payload_bytes: u32::try_from(serde_json::to_vec(event).unwrap().len()).unwrap(),
        hop_limit,
    }
}

fn only_message(actions: Vec<InvWantAction>) -> InvWantWireMessage {
    assert_eq!(actions.len(), 1);
    match actions.into_iter().next().unwrap() {
        InvWantAction::Send { message, .. } => message,
        InvWantAction::Deliver { .. } => panic!("expected outbound message"),
    }
}

fn message_for(actions: &[InvWantAction], expected_peer: &str) -> InvWantWireMessage {
    actions
        .iter()
        .find_map(|action| match action {
            InvWantAction::Send { peer_id, message } if peer_id == expected_peer => {
                Some(message.clone())
            }
            InvWantAction::Send { .. } | InvWantAction::Deliver { .. } => None,
        })
        .unwrap_or_else(|| panic!("missing message for {expected_peer}"))
}

fn delivered_ids(actions: &[InvWantAction]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            InvWantAction::Deliver { event, .. } => Some(event.id.to_hex()),
            InvWantAction::Send { .. } => None,
        })
        .collect()
}
