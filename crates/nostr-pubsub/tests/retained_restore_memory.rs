//! A lazy restore must not collect its whole input before applying retention.
//! This integration binary has one test so unrelated allocations cannot mask
//! the Linux process-memory measurement.
#![cfg(target_os = "linux")]

use nostr::{EventBuilder, Keys, Timestamp};
use nostr_pubsub::{EventRetentionPolicy, Filter, RetainedEvents};

fn resident_kib() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .map(|value| value.split_whitespace().next().unwrap().parse().unwrap())
        })
        .expect("Linux process status reports resident memory")
}

#[test]
fn restoring_a_lazy_duplicate_stream_bounds_transient_memory() {
    let event = EventBuilder::text_note("x".repeat(60_000))
        .custom_created_at(Timestamp::from(1000))
        .sign_with_keys(&Keys::generate())
        .unwrap();
    let mut retained =
        RetainedEvents::new(EventRetentionPolicy::new(1, vec![Filter::new()])).unwrap();
    let initial_kib = resident_kib();
    let mut peak_kib = initial_kib;
    // The producer owns only one event. A cache limited to one event must not
    // collect all sixty megabytes of this input before applying its bounds.
    retained.restore(
        (0..1024).map(|index| {
            if index % 64 == 0 {
                peak_kib = peak_kib.max(resident_kib());
            }
            event.clone()
        }),
        1000,
    );
    assert_eq!(retained.snapshot(1000), vec![event]);
    let added_kib = peak_kib.saturating_sub(initial_kib);
    assert!(
        added_kib < 16 * 1024,
        "restoring one event added {added_kib} KiB of resident memory"
    );
}
