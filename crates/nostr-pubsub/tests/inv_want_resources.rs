use std::collections::BTreeSet;

use nostr::{Event, EventBuilder, Keys, Kind};
use nostr_pubsub::{InvWantAction, InvWantMesh, InvWantMeshOptions, InvWantWireMessage, MeshPeer};

const EVENT_KIND: u16 = 37_195;

#[test]
fn retained_pending_peer_count_tracks_delivery_and_route_expiry() {
    let first = signed_event("pending delivery");
    let first_id = first.id.to_hex();
    let mut provider = mesh();
    let mut middle = mesh();

    let inventory = only_message(
        provider
            .publish(first, &[MeshPeer::new("middle")], 1)
            .unwrap(),
    );
    let upstream_want = only_message(
        middle
            .receive("provider", inventory, &[MeshPeer::new("provider")], 2)
            .unwrap(),
    );
    for downstream in ["downstream-a", "downstream-b", "downstream-a"] {
        assert!(
            middle
                .receive(
                    downstream,
                    InvWantWireMessage::Want {
                        event_id: first_id.clone(),
                    },
                    &[],
                    3,
                )
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(middle.retained_state().pending_events, 1);
    assert_eq!(middle.retained_state().pending_peers, 2);

    let frame = only_message(
        provider
            .receive("middle", upstream_want, &[MeshPeer::new("middle")], 4)
            .unwrap(),
    );
    let actions = middle.receive("provider", frame, &[], 5).unwrap();
    assert_eq!(delivered_ids(&actions), vec![first_id]);
    assert_eq!(middle.retained_state().pending_events, 0);
    assert_eq!(middle.retained_state().pending_peers, 0);

    let second = signed_event("pending expiry");
    let second_id = second.id.to_hex();
    let inventory = only_message(
        provider
            .publish(second, &[MeshPeer::new("middle")], 10)
            .unwrap(),
    );
    only_message(
        middle
            .receive("provider", inventory, &[MeshPeer::new("provider")], 11)
            .unwrap(),
    );
    assert!(
        middle
            .receive(
                "downstream-c",
                InvWantWireMessage::Want {
                    event_id: second_id,
                },
                &[],
                12,
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(middle.retained_state().pending_events, 1);
    assert_eq!(middle.retained_state().pending_peers, 1);

    middle.maintain(120_012);
    assert_eq!(middle.retained_state().pending_events, 0);
    assert_eq!(middle.retained_state().pending_peers, 0);
}

#[test]
fn delivered_dedup_state_expires_and_allows_later_redelivery() {
    let event = signed_event("bounded delivered dedup");
    let event_id = event.id.to_hex();
    let mut provider = mesh();
    let mut consumer = mesh();

    let first_frame = requested_frame(&mut provider, &mut consumer, event.clone(), 1);
    let first_actions = consumer
        .receive("provider", first_frame.clone(), &[], 4)
        .unwrap();
    assert_eq!(delivered_ids(&first_actions), vec![event_id.clone()]);
    assert_eq!(consumer.retained_state().delivered_events, 1);

    let duplicate = consumer.receive("provider", first_frame, &[], 5).unwrap();
    assert!(delivered_ids(&duplicate).is_empty());
    assert_eq!(consumer.retained_state().delivered_events, 1);

    consumer.maintain(720_020);
    assert_eq!(consumer.retained_state().delivered_events, 0);
    let redelivery = deliver(&mut provider, &mut consumer, event, 720_021);
    assert_eq!(delivered_ids(&redelivery), vec![event_id]);
}

#[test]
fn delivered_dedup_state_preserves_the_hard_count_bound() {
    let mut provider = mesh_with_seen_limit(2);
    let mut consumer = mesh_with_seen_limit(2);

    for sequence in 0..3 {
        let event = signed_event(&format!("bounded delivered {sequence}"));
        let event_id = event.id.to_hex();
        let actions = deliver(&mut provider, &mut consumer, event, sequence * 10 + 1);
        assert_eq!(delivered_ids(&actions), vec![event_id]);
    }

    assert_eq!(consumer.retained_state().delivered_events, 2);
}

fn deliver(
    provider: &mut InvWantMesh,
    consumer: &mut InvWantMesh,
    event: Event,
    now_ms: u64,
) -> Vec<InvWantAction> {
    let frame = requested_frame(provider, consumer, event, now_ms);
    consumer
        .receive("provider", frame, &[], now_ms + 3)
        .unwrap()
}

fn requested_frame(
    provider: &mut InvWantMesh,
    consumer: &mut InvWantMesh,
    event: Event,
    now_ms: u64,
) -> InvWantWireMessage {
    let inventory = only_message(
        provider
            .publish(event, &[MeshPeer::new("consumer")], now_ms)
            .unwrap(),
    );
    let want = only_message(
        consumer
            .receive(
                "provider",
                inventory,
                &[MeshPeer::new("provider")],
                now_ms + 1,
            )
            .unwrap(),
    );
    only_message(
        provider
            .receive("consumer", want, &[MeshPeer::new("consumer")], now_ms + 2)
            .unwrap(),
    )
}

fn mesh() -> InvWantMesh {
    mesh_with_seen_limit(4_096)
}

fn mesh_with_seen_limit(max_seen_events: usize) -> InvWantMesh {
    InvWantMesh::new(InvWantMeshOptions {
        fanout: 8,
        max_hops: 4,
        max_seen_events,
        allowed_kinds: Some(BTreeSet::from([EVENT_KIND])),
        ..InvWantMeshOptions::default()
    })
}

fn signed_event(content: &str) -> Event {
    EventBuilder::new(Kind::Custom(EVENT_KIND), content)
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

fn delivered_ids(actions: &[InvWantAction]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            InvWantAction::Deliver { event, .. } => Some(event.id.to_hex()),
            InvWantAction::Send { .. } => None,
        })
        .collect()
}
