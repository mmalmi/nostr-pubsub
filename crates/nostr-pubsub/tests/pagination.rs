use nostr::{EventBuilder, Keys, Kind, Timestamp};
use nostr_pubsub::{
    EventSource, Filter, HistoryCursor, HistoryPageCompletion as Completion,
    HistoryPageStatus as Status, QueryEvent, SourceId, VerifiedEvent,
};

fn delivery(source: &str, id: &str, at: u64) -> QueryEvent {
    let event = EventBuilder::new(Kind::TextNote, id)
        .custom_created_at(Timestamp::from(at))
        .sign_with_keys(&Keys::generate())
        .unwrap();
    QueryEvent {
        event: VerifiedEvent::try_from(event).unwrap(),
        source: EventSource::relay(source),
        priority: 0,
    }
}
fn cursor(source: &str, limit: usize, max: usize) -> HistoryCursor {
    HistoryCursor::new(
        SourceId::new(source),
        Filter::new().kind(Kind::TextNote).limit(limit),
        Timestamp::from(1000),
        max,
    )
    .unwrap()
}
#[test]
fn partial_pages_never_claim_exhaustion_or_skip_unseen_history() {
    let mut cursor = cursor("a", 3, 12);
    let page = cursor.begin_page().unwrap();
    assert!(cursor.observe(page.token, &delivery("a", "partial", 800)));
    let result = cursor.finish_page(page.token, Completion::Partial);
    assert_eq!(result.status, Status::Retry);
    assert_eq!(result.oldest, Some(Timestamp::from(800)));
    assert_eq!(
        cursor.begin_page().unwrap().filter.until,
        Some(Timestamp::from(1000))
    );
}
#[test]
fn short_complete_page_advances_inclusively() {
    let mut cursor = cursor("a", 300, 1200);
    let page = cursor.begin_page().unwrap();
    cursor.observe(page.token, &delivery("a", "short", 873));
    assert_eq!(
        cursor.finish_page(page.token, Completion::Complete).status,
        Status::Advanced
    );
    let next = cursor.begin_page().unwrap();
    assert_eq!(next.filter.until, Some(Timestamp::from(873)));
    assert_eq!(next.filter.limit, Some(300));
}
#[test]
fn sources_and_duplicate_delivery_keep_independent_boundaries() {
    let mut a = cursor("a", 3, 12);
    let mut b = cursor("b", 3, 12);
    let pa = a.begin_page().unwrap();
    let pb = b.begin_page().unwrap();
    let recent = delivery("a", "same", 900);
    assert!(a.observe(pa.token, &recent));
    assert!(!a.observe(pa.token, &recent));
    assert!(!b.observe(pb.token, &recent));
    let mut other = recent.clone();
    other.source = EventSource::relay("b");
    assert!(b.observe(pb.token, &other));
    b.observe(pb.token, &delivery("b", "old-outlier", 100));
    a.finish_page(pa.token, Completion::Complete);
    b.finish_page(pb.token, Completion::Complete);
    assert_eq!(
        a.begin_page().unwrap().filter.until,
        Some(Timestamp::from(900))
    );
    assert_eq!(
        b.begin_page().unwrap().filter.until,
        Some(Timestamp::from(100))
    );
}
#[test]
fn dense_seconds_expand_then_stop_at_the_explicit_bound() {
    let mut cursor = cursor("a", 2, 4);
    let page = cursor.begin_page().unwrap();
    for id in ["a", "b"] {
        cursor.observe(page.token, &delivery("a", id, 1000));
    }
    assert_eq!(
        cursor.finish_page(page.token, Completion::Complete).status,
        Status::Advanced
    );
    let page = cursor.begin_page().unwrap();
    assert_eq!(page.filter.limit, Some(4));
    for id in ["a", "b", "c", "d"] {
        cursor.observe(page.token, &delivery("a", id, 1000));
    }
    assert_eq!(
        cursor.finish_page(page.token, Completion::Complete).status,
        Status::Limited
    );
    assert!(cursor.begin_page().is_none());
}
#[test]
fn only_complete_unsaturated_boundary_may_move_to_previous_second() {
    let mut cursor = cursor("a", 2, 4);
    let page = cursor.begin_page().unwrap();
    cursor.observe(page.token, &delivery("a", "a", 1000));
    cursor.finish_page(page.token, Completion::Complete);
    assert_eq!(
        cursor.begin_page().unwrap().filter.until,
        Some(Timestamp::from(999))
    );
}
#[test]
fn empty_complete_and_since_boundary_end_history() {
    let mut cursor = HistoryCursor::new(
        SourceId::new("a"),
        Filter::new().since(Timestamp::from(1000)).limit(2),
        Timestamp::from(1000),
        4,
    )
    .unwrap();
    let page = cursor.begin_page().unwrap();
    assert_eq!(page.filter.since, Some(Timestamp::from(1000)));
    assert!(!cursor.observe(page.token, &delivery("a", "too-old", 999)));
    assert!(!cursor.observe(page.token, &delivery("a", "future", 1001)));
    cursor.observe(page.token, &delivery("a", "boundary", 1000));
    assert_eq!(
        cursor.finish_page(page.token, Completion::Complete).status,
        Status::Exhausted
    );
    let mut empty = self::cursor("a", 2, 4);
    let page = empty.begin_page().unwrap();
    assert_eq!(
        empty.finish_page(page.token, Completion::Complete).status,
        Status::Exhausted
    );
}
#[test]
fn cancel_and_finished_tokens_reject_late_delivery() {
    let mut cursor = cursor("a", 2, 4);
    let page = cursor.begin_page().unwrap();
    assert!(cursor.begin_page().is_none());
    cursor.finish_page(page.token, Completion::Partial);
    let active = cursor.begin_page().unwrap();
    assert!(!cursor.observe(page.token, &delivery("a", "late", 800)));
    assert_eq!(
        cursor.finish_page(page.token, Completion::Complete).status,
        Status::Stale
    );
    cursor.cancel();
    assert!(!cursor.observe(active.token, &delivery("a", "cancelled", 800)));
    assert!(cursor.begin_page().is_none());
}
#[test]
fn memory_bounds_and_invalid_ranges_are_rejected() {
    assert!(
        HistoryCursor::new(
            SourceId::new("a"),
            Filter::new().limit(0),
            Timestamp::from(1000),
            4
        )
        .is_err()
    );
    assert!(
        HistoryCursor::new(
            SourceId::new("a"),
            Filter::new().limit(5),
            Timestamp::from(1000),
            4
        )
        .is_err()
    );
    assert!(
        HistoryCursor::new(
            SourceId::new("a"),
            Filter::new().since(Timestamp::from(1001)),
            Timestamp::from(1000),
            100
        )
        .is_err()
    );
    let mut cursor = cursor("a", 2, 2);
    let page = cursor.begin_page().unwrap();
    for id in ["a", "b", "c"] {
        cursor.observe(page.token, &delivery("a", id, 900));
    }
    let result = cursor.finish_page(page.token, Completion::Partial);
    assert_eq!(result.status, Status::Limited);
    assert_eq!(result.unique_events, 2);
}

#[test]
fn tokens_are_bound_to_cursor_instances() {
    let mut old = cursor("a", 2, 4);
    let mut new = cursor("a", 2, 4);
    let old_page = old.begin_page().unwrap();
    let new_page = new.begin_page().unwrap();
    assert_ne!(old_page.token, new_page.token);
    assert!(!new.observe(old_page.token, &delivery("a", "late query", 900)));
    assert_eq!(
        new.finish_page(old_page.token, Completion::Complete).status,
        Status::Stale
    );
    assert!(new.observe(new_page.token, &delivery("a", "current query", 900)));
}
