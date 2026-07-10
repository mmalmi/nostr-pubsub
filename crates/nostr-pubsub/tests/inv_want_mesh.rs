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
    let inventory_for_carol = message_for(&bob_actions, "carol");

    let carol_actions = carol
        .receive("bob", inventory_for_carol, &[MeshPeer::new("bob")], 3)
        .unwrap();
    let want_for_bob = message_for(&carol_actions, "bob");
    assert!(
        bob.receive(
            "carol",
            want_for_bob,
            &[MeshPeer::new("alice"), MeshPeer::new("carol")],
            4,
        )
        .unwrap()
        .is_empty()
    );

    let frame_for_bob = only_message(
        alice
            .receive("bob", want_for_alice, &[MeshPeer::new("bob")], 5)
            .unwrap(),
    );
    let bob_frame_actions = bob
        .receive(
            "alice",
            frame_for_bob,
            &[MeshPeer::new("alice"), MeshPeer::new("carol")],
            6,
        )
        .unwrap();
    assert_eq!(delivered_ids(&bob_frame_actions), vec![event_id.clone()]);

    let frame_for_carol = message_for(&bob_frame_actions, "carol");
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
