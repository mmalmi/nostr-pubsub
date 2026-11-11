use crate::{Filter, Limits, Session, codec::encode_var_int, engine, types::Error};

fn id_list(count: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x61, 0, 0, 2];
    frame.extend(encode_var_int(count));
    frame.extend(payload);
    frame
}

fn empty_storage() -> crate::storage::SummedStorage {
    crate::storage::SummedStorage::new(Vec::new())
}

fn upstream_storage() -> negentropy::NegentropyStorageVector {
    let mut storage = negentropy::NegentropyStorageVector::new();
    storage.seal().unwrap();
    storage
}

// Unlike the advisory's large but representable allocation, u64::MAX cannot be
// a HashSet<Id> capacity. Even an unpatched parser fails with capacity overflow
// before allocation. This keeps the regression safe when run against old code.
fn malformed_lists() -> [Vec<u8>; 2] {
    [id_list(u64::MAX, &[]), id_list(2, &[7; 63])]
}

#[test]
fn copied_engine_rejects_impossible_id_lists_before_allocation() {
    for frame in malformed_lists() {
        let mut responder = engine::Negentropy::owned(empty_storage(), 4096).unwrap();
        assert!(matches!(
            responder.reconcile(&frame),
            Err(Error::ParseEndsPrematurely)
        ));

        let mut initiator = engine::Negentropy::owned(empty_storage(), 4096).unwrap();
        initiator.initiate().unwrap();
        assert!(matches!(
            initiator.reconcile_with_ids(&frame, &mut Vec::new(), &mut Vec::new()),
            Err(Error::ParseEndsPrematurely)
        ));
    }
}

#[test]
fn upstream_engine_rejects_impossible_id_lists_before_allocation() {
    for frame in malformed_lists() {
        let mut responder = negentropy::Negentropy::owned(upstream_storage(), 4096).unwrap();
        assert!(matches!(
            responder.reconcile(&frame),
            Err(negentropy::Error::ParseEndsPrematurely)
        ));

        let mut initiator = negentropy::Negentropy::owned(upstream_storage(), 4096).unwrap();
        initiator.initiate().unwrap();
        assert!(matches!(
            initiator.reconcile_with_ids(&frame, &mut Vec::new(), &mut Vec::new()),
            Err(negentropy::Error::ParseEndsPrematurely)
        ));
    }
}

#[test]
fn public_session_rejects_malformed_lists_in_both_roles() {
    let new_session = || {
        Session::new(
            [],
            Filter {
                since: 0,
                until: 100,
            },
            Limits::default(),
        )
        .unwrap()
    };
    for frame in malformed_lists() {
        let mut responder = new_session();
        assert!(responder.respond(&frame).is_err());

        let mut initiator = new_session();
        initiator.initiate().unwrap();
        assert!(initiator.reconcile(&frame).is_err());
    }
}

#[test]
fn snapshot_collapses_duplicates_and_rejects_conflicts_and_overflow() {
    let record = |n: u16, timestamp| {
        let mut id = [0_u8; 32];
        id[..2].copy_from_slice(&n.to_be_bytes());
        crate::Record { timestamp, id }
    };
    let filter = Filter {
        since: 0,
        until: 10,
    };
    let limits = Limits {
        max_records: 4,
        ..Limits::default()
    };
    // Many repeats of few records stay within the limit, however long the input.
    let repeated = (0..5_000).map(|n| record(n % 4, 1));
    assert!(Session::new(repeated, filter, limits).is_ok());
    // Records outside the window do not count.
    let outside = (0..5_000).map(|n| record(n, if n < 4 { 1 } else { 11 }));
    assert!(Session::new(outside, filter, limits).is_ok());
    let conflict = [record(1, 1), record(2, 1), record(1, 2)];
    assert_eq!(
        Session::new(conflict, filter, limits).err().unwrap(),
        "one record ID has conflicting timestamps"
    );
    let overflow = (0..5).map(|n| record(n, 1));
    assert_eq!(
        Session::new(overflow, filter, limits).err().unwrap(),
        "reconciliation window exceeds record limit"
    );
}
