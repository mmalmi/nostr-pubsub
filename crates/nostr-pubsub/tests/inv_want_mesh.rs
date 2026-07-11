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
            .receive("bob", frame_for_carol, &[MeshPeer::new("bob")], 8,)
            .unwrap()
            .is_empty()
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
    blackhole
        .publish(signed_event(), &[MeshPeer::new("blackhole")], 20)
        .unwrap();
    assert!(
        blackhole
            .peer_behavior_score("blackhole")
            .is_some_and(|score| score < 0)
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
