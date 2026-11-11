use crate::types::{Bound, Error, Id, Item};
use sha2::{Digest, Sha256};

/// Sorted snapshot with running 256-bit ID sums: any range fingerprint is one
/// subtraction and one hash instead of a pass over the range.
pub struct SummedStorage {
    items: Vec<Item>,
    sums: Vec<[u64; 4]>,
}

impl SummedStorage {
    /// `records` must hold unique IDs.
    pub fn new(records: Vec<([u8; 32], u64)>) -> Self {
        let mut items: Vec<Item> = records
            .into_iter()
            .map(|(id, timestamp)| Item::with_timestamp_and_id(timestamp, Id::from_byte_array(id)))
            .collect();
        items.sort_unstable();
        let mut sums = Vec::with_capacity(items.len() + 1);
        let mut total = [0_u64; 4];
        sums.push(total);
        for item in &items {
            let mut carry = false;
            for (lane, bytes) in total.iter_mut().zip(item.id.as_bytes().as_chunks::<8>().0) {
                let word = u64::from_le_bytes(*bytes);
                let (sum, first) = lane.overflowing_add(word);
                let (sum, second) = sum.overflowing_add(u64::from(carry));
                *lane = sum;
                carry = first | second;
            }
            sums.push(total);
        }
        Self { items, sums }
    }
}

impl SummedStorage {
    pub fn size(&self) -> usize {
        self.items.len()
    }

    pub fn iterate(
        &self,
        begin: usize,
        end: usize,
        cb: &mut dyn FnMut(Item, usize) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        if begin > end || end > self.items.len() {
            return Err(Error::BadRange);
        }
        for i in begin..end {
            if !cb(self.items[i], i)? {
                break;
            }
        }
        Ok(())
    }
    pub fn find_lower_bound(&self, first: usize, last: usize, value: &Bound) -> usize {
        first + self.items[first..last].partition_point(|item| *item < value.item)
    }

    pub fn range_fingerprint(&self, begin: usize, end: usize) -> Result<[u8; 16], Error> {
        if begin > end || end > self.items.len() {
            return Err(Error::BadRange);
        }
        let mut bytes = [0_u8; 32];
        let mut borrow = false;
        for lane in 0..4 {
            let (difference, first) = self.sums[end][lane].overflowing_sub(self.sums[begin][lane]);
            let (difference, second) = difference.overflowing_sub(u64::from(borrow));
            borrow = first | second;
            bytes[lane * 8..lane * 8 + 8].copy_from_slice(&difference.to_le_bytes());
        }
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hasher.update(crate::codec::encode_var_int((end - begin) as u64));
        let hash: [u8; 32] = hasher.finalize().into();
        Ok(hash[..16].try_into().expect("sixteen-byte fingerprint"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use negentropy::{NegentropyStorageBase, NegentropyStorageVector};
    use sha2::{Digest, Sha256};

    #[test]
    fn range_fingerprints_match_the_reference_storage() {
        let records: Vec<([u8; 32], u64)> = (0_u64..300)
            .map(|n| (Sha256::digest(n.to_le_bytes()).into(), 1_000 + n / 7))
            .collect();
        let mut reference = NegentropyStorageVector::new();
        for (id, timestamp) in &records {
            reference
                .insert(*timestamp, negentropy::Id::from_byte_array(*id))
                .unwrap();
        }
        reference.seal().unwrap();
        let summed = SummedStorage::new(records);
        for (begin, end) in [(0, 0), (0, 1), (0, 300), (17, 130), (129, 300), (299, 300)] {
            assert_eq!(
                summed.range_fingerprint(begin, end).unwrap(),
                reference.fingerprint(begin, end).unwrap().to_bytes(),
            );
        }
        assert!(summed.range_fingerprint(2, 1).is_err());
        assert!(summed.range_fingerprint(0, 301).is_err());
    }
}
