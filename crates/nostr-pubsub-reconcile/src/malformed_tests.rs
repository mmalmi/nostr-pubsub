use negentropy::{Error, NegentropyStorageVector};

use crate::{Filter, Limits, Session, codec::encode_var_int, engine};

fn id_list(count: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x61, 0, 0, 2];
    frame.extend(encode_var_int(count));
    frame.extend(payload);
    frame
}

fn empty_storage() -> NegentropyStorageVector {
    let mut storage = NegentropyStorageVector::new();
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
        let mut responder = negentropy::Negentropy::owned(empty_storage(), 4096).unwrap();
        assert!(matches!(
            responder.reconcile(&frame),
            Err(Error::ParseEndsPrematurely)
        ));

        let mut initiator = negentropy::Negentropy::owned(empty_storage(), 4096).unwrap();
        initiator.initiate().unwrap();
        assert!(matches!(
            initiator.reconcile_with_ids(&frame, &mut Vec::new(), &mut Vec::new()),
            Err(Error::ParseEndsPrematurely)
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
