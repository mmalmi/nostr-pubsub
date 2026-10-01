use super::ScopedSeenIds;

fn assert_bounded(seen: &ScopedSeenIds) {
    assert!(seen.total <= seen.max_total);
    assert_eq!(
        seen.total,
        seen.scopes
            .values()
            .map(|window| window.ids.len())
            .sum::<usize>()
    );
    assert!(
        seen.global_order.len() <= 2 * seen.max_total,
        "{} queued records for {} live IDs",
        seen.global_order.len(),
        seen.total
    );
    for window in seen.scopes.values() {
        assert!(window.ids.len() <= seen.max_per_scope);
        assert!(
            window.order.len() <= seen.max_per_scope,
            "{} scope records for {} live IDs",
            window.order.len(),
            window.ids.len()
        );
    }
}

#[test]
fn observations_are_bounded_and_scoped_by_authenticated_peer() {
    let mut seen = ScopedSeenIds::new(2, 3);
    assert!(seen.observe("peer-a", "sub", "one"));
    assert!(!seen.observe("peer-a", "sub", "one"));
    assert!(seen.observe("peer-b", "sub", "one"));
    assert!(seen.observe("peer-a", "sub", "two"));
    assert!(seen.observe("peer-a", "sub", "three"));
    assert!(seen.observe("peer-a", "sub", "one"));

    seen.clear_peer("peer-a");
    assert!(seen.observe("peer-a", "sub", "three"));
    assert!(!seen.observe("peer-b", "sub", "one"));
    seen.clear_subscription("sub");
    assert!(seen.observe("peer-b", "sub", "one"));
}

#[test]
fn single_scope_churn_bounds_bookkeeping_and_keeps_recent_ids() {
    let mut seen = ScopedSeenIds::new(2, 4);
    for i in 0..10_000 {
        assert!(seen.observe("peer", "sub", &i.to_string()));
    }
    assert_eq!(seen.total, 2);
    assert_bounded(&seen);
    assert!(!seen.observe("peer", "sub", "9998"));
    assert!(!seen.observe("peer", "sub", "9999"));
    assert!(seen.observe("peer", "sub", "9997"));
    assert!(seen.observe("peer", "sub", "9998"));
}

#[test]
fn global_eviction_bounds_each_scope_queue() {
    let mut seen = ScopedSeenIds::new(2, 3);
    for i in 0..10_000 {
        assert!(seen.observe("peer-a", "sub", &i.to_string()));
        assert!(seen.observe("peer-b", "sub", &i.to_string()));
    }
    assert_eq!(seen.total, 3);
    assert_bounded(&seen);
    assert!(!seen.observe("peer-a", "sub", "9999"));
    assert!(!seen.observe("peer-b", "sub", "9998"));
    assert!(!seen.observe("peer-b", "sub", "9999"));
    assert!(seen.observe("peer-a", "sub", "9998"));
    assert!(seen.observe("peer-b", "sub", "9998"));
}

#[test]
fn anchored_scope_does_not_pin_stale_records_from_churning_scope() {
    let mut seen = ScopedSeenIds::new(2, 3);
    assert!(seen.observe("anchor", "sub", "first"));
    for i in 0..10_000 {
        assert!(seen.observe("churn", "sub", &i.to_string()));
    }
    assert_bounded(&seen);
    assert!(!seen.observe("anchor", "sub", "first"));
    assert!(!seen.observe("churn", "sub", "9998"));
    assert!(!seen.observe("churn", "sub", "9999"));
    assert!(seen.observe("third", "sub", "new"));
    // Compaction must preserve FIFO: the anchored ID is still the oldest.
    assert!(!seen.observe("churn", "sub", "9998"));
    assert!(seen.observe("anchor", "sub", "first"));
}

#[test]
fn clearing_peer_releases_its_records_without_clearing_other_peers() {
    let mut seen = ScopedSeenIds::new(2, 6);
    assert!(seen.observe("keep", "sub-a", "id"));
    for _ in 0..10_000 {
        assert!(seen.observe("clear", "sub-a", "id"));
        assert!(seen.observe("clear", "sub-b", "id"));
        seen.clear_peer("clear");
    }
    assert_bounded(&seen);
    assert_eq!(seen.total, 1);
    assert!(!seen.observe("keep", "sub-a", "id"));
    assert!(seen.observe("clear", "sub-a", "id"));
    seen.clear_peer("clear");
    seen.clear_peer("keep");
    assert!(seen.scopes.is_empty());
    assert!(seen.global_order.is_empty());
    assert_eq!(seen.total, 0);
}

#[test]
fn clearing_subscription_releases_its_records_without_clearing_other_epochs() {
    let mut seen = ScopedSeenIds::new(2, 6);
    assert!(seen.observe("peer-a", "keep", "id"));
    for _ in 0..10_000 {
        assert!(seen.observe("peer-a", "clear", "id"));
        assert!(seen.observe("peer-b", "clear", "id"));
        seen.clear_subscription("clear");
    }
    assert_bounded(&seen);
    assert_eq!(seen.total, 1);
    assert!(!seen.observe("peer-a", "keep", "id"));
    assert!(seen.observe("peer-a", "clear", "id"));
    seen.clear_subscription("clear");
    seen.clear_subscription("keep");
    assert!(seen.scopes.is_empty());
    assert!(seen.global_order.is_empty());
    assert_eq!(seen.total, 0);
}

#[test]
fn reused_id_keeps_its_new_generation_and_fifo_position() {
    let mut seen = ScopedSeenIds::new(1, 2);
    assert!(seen.observe("peer-a", "sub", "reused"));
    assert!(seen.observe("peer-a", "sub", "replacement"));
    assert!(seen.observe("peer-b", "sub", "older"));
    assert!(seen.observe("peer-a", "sub", "reused"));
    assert!(seen.observe("peer-c", "sub", "newest"));
    assert_bounded(&seen);
    // The stale first generation must not evict the newer observation.
    assert!(!seen.observe("peer-a", "sub", "reused"));
    assert!(!seen.observe("peer-c", "sub", "newest"));
    assert!(seen.observe("peer-b", "sub", "older"));
    assert!(seen.observe("peer-a", "sub", "reused"));
}
