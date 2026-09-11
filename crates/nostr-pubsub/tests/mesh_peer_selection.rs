use nostr_pubsub::{MeshPeer, select_mesh_peers};

#[test]
fn all_candidates_fit_preserves_order_exclusion_and_first_observed_duplicate() {
    let peers = [
        MeshPeer::new("z-unknown"),
        MeshPeer::new("duplicate"),
        MeshPeer::observed("excluded", 1_000),
        MeshPeer::observed("duplicate", 40),
        MeshPeer::observed("best", 100),
        MeshPeer::observed("duplicate", 999),
        MeshPeer::observed("known-zero", 0),
        MeshPeer::new("a-unknown"),
        MeshPeer::observed("negative", -8),
    ];
    let expected = vec![
        MeshPeer::observed("best", 100),
        MeshPeer::observed("duplicate", 40),
        MeshPeer::observed("known-zero", 0),
        MeshPeer::new("a-unknown"),
        MeshPeer::new("z-unknown"),
        MeshPeer::observed("negative", -8),
    ];
    for capacity in [expected.len(), expected.len() + 1] {
        assert_eq!(
            select_mesh_peers(&peers, Some("excluded"), capacity, usize::MAX),
            expected,
        );
    }
    assert_eq!(
        select_mesh_peers(&peers, Some("excluded"), 4, 2),
        vec![
            MeshPeer::observed("best", 100),
            MeshPeer::observed("duplicate", 40),
            MeshPeer::new("z-unknown"),
            MeshPeer::new("a-unknown"),
        ],
    );
}

#[test]
fn unknown_reserve_replaces_last_ranked_known_peers_in_exact_order() {
    let peers = [
        MeshPeer::observed("a", 100),
        MeshPeer::observed("b", 90),
        MeshPeer::observed("c", 80),
        MeshPeer::observed("d", 70),
        MeshPeer::new("u1"),
        MeshPeer::new("u2"),
        MeshPeer::new("u3"),
    ];
    assert_eq!(
        select_mesh_peers(&peers, None, 4, 3),
        vec![
            MeshPeer::observed("a", 100),
            MeshPeer::new("u3"),
            MeshPeer::new("u2"),
            MeshPeer::new("u1"),
        ],
    );
    assert_eq!(select_mesh_peers(&peers, None, 4, 0), peers[..4]);
}

#[test]
fn empty_or_zero_capacity_never_selects_a_peer() {
    assert!(select_mesh_peers(&[], None, 4, 1).is_empty());
    let peers = [MeshPeer::observed("known", 100), MeshPeer::new("unknown")];
    assert!(select_mesh_peers(&peers, None, 0, usize::MAX).is_empty());
    assert!(select_mesh_peers(&peers[..1], Some("known"), 4, 1).is_empty());
}
