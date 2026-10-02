use nostr_pubsub_reconcile::{Filter, Limits, Record, Session};
use sha2::{Digest, Sha256};

fn record(timestamp: u64, id: u64) -> Record {
    let bytes = Sha256::digest(id.to_be_bytes()).into();
    Record {
        timestamp,
        id: bytes,
    }
}

fn differences(
    a: Vec<Record>,
    b: Vec<Record>,
    filter: Filter,
) -> (Vec<[u8; 32]>, Vec<[u8; 32]>, usize) {
    let mut a = Session::new(
        a,
        filter,
        Limits {
            max_frame_bytes: 4096,
            ..Limits::default()
        },
    )
    .unwrap();
    let mut b = Session::new(
        b,
        filter,
        Limits {
            max_frame_bytes: 4096,
            ..Limits::default()
        },
    )
    .unwrap();
    let mut query = a.initiate().unwrap();
    let (mut have, mut need, mut bytes) = (Vec::new(), Vec::new(), 0);
    loop {
        let response = b.respond(&query).unwrap();
        bytes += query.len() + response.len();
        let step = a.reconcile(&response).unwrap();
        have.extend(step.have);
        need.extend(step.need);
        match step.next {
            Some(next) => query = next,
            None => break,
        }
    }
    (have, need, bytes)
}

#[test]
fn finds_only_sparse_gaps_in_agreed_history_including_equal_timestamps() {
    let all: Vec<_> = (0..10_000).map(|n| record(100 + n / 3, n)).collect();
    let a = all
        .iter()
        .copied()
        .filter(|r| *r != record(101, 4))
        .collect();
    let b = all
        .iter()
        .copied()
        .filter(|r| *r != record(102, 6))
        .collect();
    let (have, need, bytes) = differences(
        a,
        b,
        Filter {
            since: 101,
            until: 4000,
        },
    );
    assert_eq!(have, vec![record(102, 6).id]);
    assert_eq!(need, vec![record(101, 4).id]);
    assert!(
        bytes < 20_000,
        "sparse repair must not replay all IDs: {bytes}"
    );
}

#[test]
fn history_filter_excludes_old_and_future_records() {
    let (have, need, _) = differences(
        vec![record(99, 1), record(100, 2), record(201, 4)],
        vec![record(200, 3)],
        Filter {
            since: 100,
            until: 200,
        },
    );
    assert_eq!(have, vec![record(100, 2).id]);
    assert_eq!(need, vec![record(200, 3).id]);
}

#[test]
fn limits_fail_explicitly_instead_of_silently_claiming_complete() {
    let limits = Limits {
        max_records: 1,
        ..Limits::default()
    };
    assert!(
        Session::new(
            vec![record(100, 1), record(101, 2)],
            Filter {
                since: 0,
                until: 200
            },
            limits
        )
        .is_err()
    );
    assert!(
        Session::new(
            vec![record(100, 1), record(101, 1)],
            Filter {
                since: 0,
                until: 200
            },
            Limits::default()
        )
        .is_err()
    );
}

#[test]
fn supports_timestamps_beyond_32_bits_and_empty_sets() {
    let (have, need, _) = differences(
        vec![],
        vec![record(5_000_000_000, 1)],
        Filter {
            since: 4_000_000_000,
            until: 6_000_000_000,
        },
    );
    assert_eq!(have, [] as [[u8; 32]; 0]);
    assert_eq!(need, vec![record(5_000_000_000, 1).id]);
}

#[test]
fn rejects_malformed_oversized_and_overflowing_frames_without_panicking() {
    let mut session = Session::new(
        vec![],
        Filter {
            since: 0,
            until: 100,
        },
        Limits {
            max_frame_bytes: 4096,
            ..Limits::default()
        },
    )
    .unwrap();
    for frame in [
        vec![],
        vec![0x61, 1],
        vec![0x61, 0, 33],
        vec![0x61; 20_000],
        [vec![0x61], vec![0xff; 12]].concat(),
    ] {
        assert!(session.respond(&frame).is_err());
    }
}

#[test]
fn bounded_dense_difference_does_not_skip_ranges() {
    let all: Vec<_> = (0..10_000).map(|n| record(100 + n / 3, n)).collect();
    let a: Vec<_> = all
        .iter()
        .copied()
        .enumerate()
        .filter(|(n, _)| n % 7 != 0)
        .map(|(_, r)| r)
        .collect();
    let b: Vec<_> = all
        .iter()
        .copied()
        .enumerate()
        .filter(|(n, _)| n % 11 != 0)
        .map(|(_, r)| r)
        .collect();
    let expected_have = a.iter().filter(|r| !b.contains(r)).count();
    let expected_need = b.iter().filter(|r| !a.contains(r)).count();
    let (have, need, _) = differences(
        a,
        b,
        Filter {
            since: 100,
            until: 4000,
        },
    );
    assert_eq!(have.len(), expected_have);
    assert_eq!(need.len(), expected_need);
}

#[test]
fn validates_the_tail_even_when_an_earlier_range_fills_the_output() {
    let records = (0..1000).map(|n| record(100 + n / 3, n));
    let mut peer = Session::new(
        records,
        Filter {
            since: 0,
            until: 1000,
        },
        Limits {
            max_frame_bytes: 4096,
            ..Limits::default()
        },
    )
    .unwrap();
    assert!(peer.respond(&[0x61, 0, 0, 2, 0, 0xff]).is_err());
}
