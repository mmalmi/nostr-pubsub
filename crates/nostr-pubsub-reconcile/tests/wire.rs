use nostr_pubsub_reconcile::{Filter, Limits, Record, Session};
use sha2::{Digest, Sha256};

fn varint(value: &mut &[u8]) -> u64 {
    let mut n = 0;
    loop {
        let byte = value[0];
        *value = &value[1..];
        n = n * 128 + u64::from(byte & 127);
        if byte < 128 {
            return n;
        }
    }
}
fn take<'a>(value: &mut &'a [u8], count: usize) -> &'a [u8] {
    let result = &value[..count];
    *value = &value[count..];
    result
}

// Independently check that every advertised fingerprint and ID list describes
// exactly the range preceding its wire boundary, including truncated frames.
fn check_frame(mut frame: &[u8], records: &[Record]) {
    assert!(frame.len() <= 4096);
    assert_eq!(take(&mut frame, 1), [0x61]);
    let (mut timestamp, mut lower) = (0_u64, 0);
    while !frame.is_empty() {
        let delta = varint(&mut frame);
        timestamp = if delta == 0 {
            u64::MAX
        } else {
            timestamp.checked_add(delta - 1).unwrap()
        };
        let count = usize::try_from(varint(&mut frame)).unwrap();
        let mut id = [0; 32];
        id[..count].copy_from_slice(take(&mut frame, count));
        let upper = records.partition_point(|r| (r.timestamp, r.id) < (timestamp, id));
        let range = &records[lower..upper];
        match varint(&mut frame) {
            0 => {}
            1 => {
                let actual = take(&mut frame, 16);
                let mut sum = [0_u8; 32];
                for record in range {
                    let mut carry = 0_u16;
                    for (word, value) in sum.iter_mut().zip(record.id) {
                        let next = u16::from(*word) + u16::from(value) + carry;
                        *word = next.to_le_bytes()[0];
                        carry = next >> 8;
                    }
                }
                let mut count = range.len();
                let mut encoded = vec![u8::try_from(count & 127).unwrap()];
                count >>= 7;
                while count != 0 {
                    encoded.push(u8::try_from(count & 127).unwrap() | 128);
                    count >>= 7;
                }
                let mut hash = Sha256::new();
                hash.update(sum);
                hash.update(encoded.into_iter().rev().collect::<Vec<_>>());
                assert_eq!(
                    actual,
                    &hash.finalize()[..16],
                    "wrong range at {lower}..{upper}"
                );
            }
            2 => {
                assert_eq!(usize::try_from(varint(&mut frame)).unwrap(), range.len());
                for record in range {
                    assert_eq!(take(&mut frame, 32), record.id);
                }
            }
            _ => panic!("unknown mode"),
        }
        lower = upper;
    }
}

#[test]
fn every_bounded_frame_describes_its_actual_range() {
    let records = |salt: u64| {
        let mut rows: Vec<_> = (0_u64..10_000)
            .map(|n| Record {
                timestamp: 100 + n / 4,
                id: Sha256::digest([n.to_be_bytes(), salt.to_be_bytes()].concat()).into(),
            })
            .collect();
        rows.sort_by_key(|r| (r.timestamp, r.id));
        rows
    };
    let (left, right) = (records(1), records(2));
    let filter = Filter {
        since: 0,
        until: 4000,
    };
    let limits = Limits {
        max_frame_bytes: 4096,
        max_rounds: 1024,
        ..Limits::default()
    };
    let mut a = Session::new(left.clone(), filter, limits).unwrap();
    let mut b = Session::new(right.clone(), filter, limits).unwrap();
    let mut query = a.initiate().unwrap();
    let (mut have, mut need) = (Vec::new(), Vec::new());
    loop {
        check_frame(&query, &left);
        let response = b.respond(&query).unwrap();
        check_frame(&response, &right);
        let step = a.reconcile(&response).unwrap();
        have.extend(step.have);
        need.extend(step.need);
        if let Some(next) = step.next {
            query = next;
        } else {
            break;
        }
    }
    have.sort_unstable();
    have.dedup();
    need.sort_unstable();
    need.dedup();
    assert_eq!(have.len(), left.len());
    assert_eq!(need.len(), right.len());
}
