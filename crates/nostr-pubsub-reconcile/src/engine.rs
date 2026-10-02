// Copyright (c) 2023 Doug Hoyte; 2023 Yuki Kishimoto. MIT licensed.
// Adapted from negentropy 0.5.0. Keep the upstream storage and public wire types,
// but fix frame truncation: a discarded range must remain in the next fingerprint.
// Preserve the reference algorithm layout for protocol audits.
#![allow(clippy::too_many_lines, clippy::if_not_else, clippy::unused_self)]
use super::codec::{decode_var_int, encode_var_int, get_byte_array, get_bytes};
use negentropy::{
    Bound, Error, FINGERPRINT_SIZE, ID_SIZE, Id, Item, NegentropyStorageBase, PROTOCOL_VERSION,
    Storage,
};
use std::collections::HashSet;

#[derive(Clone, Copy)]
enum Mode {
    Skip = 0,
    Fingerprint = 1,
    IdList = 2,
}
impl Mode {
    fn as_u64(self) -> u64 {
        self as u64
    }
}
impl TryFrom<u64> for Mode {
    type Error = Error;
    fn try_from(n: u64) -> Result<Self, Error> {
        match n {
            0 => Ok(Self::Skip),
            1 => Ok(Self::Fingerprint),
            2 => Ok(Self::IdList),
            _ => Err(Error::UnexpectedMode(n)),
        }
    }
}
const MAX_U64: u64 = u64::MAX;
const BUCKETS: usize = 16;
const DOUBLE_BUCKETS: usize = BUCKETS * 2;

/// Negentropy
#[derive(Debug)]
pub struct Negentropy<'a, T> {
    storage: Storage<'a, T>,
    frame_size_limit: usize,
    is_initiator: bool,
    last_timestamp_in: u64,
    last_timestamp_out: u64,
}

impl<'a, T> Negentropy<'a, T>
where
    T: NegentropyStorageBase,
{
    /// Create new [`Negentropy`] instance
    ///
    /// Frame size limit must be `equal to 0` or `greater than 4096`
    pub fn new(storage: Storage<'a, T>, frame_size_limit: usize) -> Result<Self, Error> {
        if frame_size_limit != 0 && frame_size_limit < 4096 {
            return Err(Error::FrameSizeLimitTooSmall);
        }

        Ok(Self {
            storage,
            frame_size_limit,
            is_initiator: false,
            last_timestamp_in: 0,
            last_timestamp_out: 0,
        })
    }

    /// Create new [`Negentropy`] instance from owned storage
    ///
    /// Frame size limit must be `equal to 0` or `greater than 4096`
    pub fn owned(storage: T, frame_size_limit: usize) -> Result<Self, Error> {
        Self::new(Storage::Owned(storage), frame_size_limit)
    }

    /// Initiate reconciliation set
    pub fn initiate(&mut self) -> Result<Vec<u8>, Error> {
        if self.is_initiator {
            return Err(Error::AlreadyBuiltInitialMessage);
        }
        self.is_initiator = true;

        let mut output: Vec<u8> = Vec::new();
        output.push(u8::try_from(PROTOCOL_VERSION).expect("protocol version fits u8"));

        output.extend(self.split_range(0, self.storage.size()?, Bound::with_timestamp(MAX_U64))?);

        Ok(output)
    }

    /// Reconcile (server method)
    pub fn reconcile(&mut self, query: &[u8]) -> Result<Vec<u8>, Error> {
        if self.is_initiator {
            return Err(Error::Initiator);
        }

        self.reconcile_aux(query, &mut Vec::new(), &mut Vec::new())
    }

    /// Reconcile (client method)
    pub fn reconcile_with_ids(
        &mut self,
        query: &[u8],
        have_ids: &mut Vec<Id>,
        need_ids: &mut Vec<Id>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if !self.is_initiator {
            return Err(Error::NonInitiator);
        }

        let output: Vec<u8> = self.reconcile_aux(query, have_ids, need_ids)?;
        if output.len() == 1 {
            return Ok(None);
        }

        Ok(Some(output))
    }

    fn reconcile_aux(
        &mut self,
        mut query: &[u8],
        have_ids: &mut Vec<Id>,
        need_ids: &mut Vec<Id>,
    ) -> Result<Vec<u8>, Error> {
        self.last_timestamp_in = 0;
        self.last_timestamp_out = 0;

        let mut full_output: Vec<u8> = Vec::with_capacity(1);
        full_output.push(u8::try_from(PROTOCOL_VERSION).expect("protocol version fits u8"));

        let protocol_version: u64 = get_byte_array::<1>(&mut query)?
            .first()
            .copied()
            .map(u64::from)
            .ok_or(Error::ProtocolVersionNotFound)?;

        if !(0x60..=0x6F).contains(&protocol_version) {
            return Err(Error::InvalidProtocolVersion);
        }

        if protocol_version != PROTOCOL_VERSION {
            if self.is_initiator {
                return Err(Error::UnsupportedProtocolVersion);
            }
            return Ok(full_output);
        }

        let storage_size = self.storage.size()?;
        let mut prev_bound: Bound = Bound::new();
        let mut prev_index: usize = 0;
        let mut skip: bool = false;

        while !query.is_empty() {
            let mut o: Vec<u8> = Vec::new();
            let timestamp_before_range = self.last_timestamp_out;
            let mut truncated = false;

            let curr_bound: Bound = self.decode_bound(&mut query)?;
            let mode: Mode = self.decode_mode(&mut query)?;

            let lower: usize = prev_index;
            let mut upper: usize =
                self.storage
                    .find_lower_bound(prev_index, storage_size, &curr_bound);

            match mode {
                Mode::Skip => {
                    skip = true;
                }
                Mode::Fingerprint => {
                    let their_fingerprint: [u8; FINGERPRINT_SIZE] = get_byte_array(&mut query)?;
                    let our_fingerprint: [u8; FINGERPRINT_SIZE] =
                        self.storage.fingerprint(lower, upper)?.to_bytes();

                    if their_fingerprint != our_fingerprint {
                        // do_skip
                        if skip {
                            skip = false;
                            o.extend(self.encode_bound(&prev_bound));
                            o.extend(self.encode_mode(Mode::Skip));
                        }

                        o.extend(self.split_range(lower, upper, curr_bound)?);
                    } else {
                        skip = true;
                    }
                }
                Mode::IdList => {
                    let num_ids: u64 = decode_var_int(&mut query)?;

                    let mut their_elems: HashSet<Id> = HashSet::with_capacity(
                        usize::try_from(num_ids).map_err(|_| Error::BadRange)?,
                    );

                    for _ in 0..num_ids {
                        let e: [u8; ID_SIZE] = get_byte_array(&mut query)?;
                        their_elems.insert(Id::from_byte_array(e));
                    }

                    self.storage.iterate(lower, upper, &mut |item: Item, _| {
                        let k: Id = item.id;
                        if !their_elems.contains(&k) {
                            if self.is_initiator {
                                have_ids.push(k);
                            }
                        } else {
                            their_elems.remove(&k);
                        }

                        Ok(true)
                    })?;

                    if self.is_initiator {
                        skip = true;

                        for k in their_elems {
                            need_ids.push(k);
                        }
                    } else {
                        // do_skip
                        if skip {
                            skip = false;
                            o.extend(self.encode_bound(&prev_bound));
                            o.extend(self.encode_mode(Mode::Skip));
                        }

                        let mut response_ids: Vec<u8> = Vec::new();
                        let mut num_response_ids: usize = 0;
                        let mut end_bound = curr_bound;

                        self.storage
                            .iterate(lower, upper, &mut |item: Item, index| {
                                if self.exceeded_frame_size_limit(
                                    full_output.len() + response_ids.len(),
                                ) {
                                    end_bound = Bound::from_item(&item);
                                    upper = index; // shrink upper so that remaining range gets correct fingerprint
                                    truncated = true;
                                    return Ok(false);
                                }

                                response_ids.extend(item.id.iter());
                                num_response_ids += 1;
                                Ok(true)
                            })?;

                        o.extend(self.encode_bound(&end_bound));
                        o.extend(self.encode_mode(Mode::IdList));
                        o.extend(encode_var_int(num_response_ids as u64));
                        o.extend(response_ids);

                        full_output.extend(&o);
                        o.clear();
                    }
                }
            }

            if truncated || self.exceeded_frame_size_limit(full_output.len() + o.len()) {
                // frameSizeLimit exceeded: Stop range processing and return a fingerprint for the remaining range
                // A nonempty `o` was never emitted. Do not skip its records.
                // ID-list responses were already committed and cleared above.
                let remaining = if o.is_empty() { upper } else { lower };
                if !o.is_empty() {
                    self.last_timestamp_out = timestamp_before_range;
                }
                // A pending Skip may precede the part we could not emit. Its
                // boundary must reach the wire before the tail fingerprint;
                // otherwise the peer hashes a different (larger) range.
                if !truncated {
                    let boundary = if o.is_empty() { curr_bound } else { prev_bound };
                    full_output.extend(self.encode_bound(&boundary));
                    full_output.extend(self.encode_mode(Mode::Skip));
                }
                let remaining_fingerprint = self.storage.fingerprint(remaining, storage_size)?;

                full_output.extend(self.encode_bound(&Bound::with_timestamp(MAX_U64)));
                full_output.extend(self.encode_mode(Mode::Fingerprint));
                full_output.extend(remaining_fingerprint.iter());
                break;
            }
            full_output.extend(o);

            prev_index = upper;
            prev_bound = curr_bound;
        }

        Ok(full_output)
    }

    fn split_range(
        &mut self,
        lower: usize,
        upper: usize,
        upper_bound: Bound,
    ) -> Result<Vec<u8>, Error> {
        let num_elems: usize = upper - lower;
        let mut o: Vec<u8> = Vec::with_capacity(10 + 10 + num_elems);

        if num_elems < DOUBLE_BUCKETS {
            o.extend(self.encode_bound(&upper_bound));
            o.extend(self.encode_mode(Mode::IdList));

            o.extend(encode_var_int(num_elems as u64));
            self.storage.iterate(lower, upper, &mut |item: Item, _| {
                o.extend(item.id.iter());
                Ok(true)
            })?;
        } else {
            let items_per_bucket: usize = num_elems / BUCKETS;
            let buckets_with_extra: usize = num_elems % BUCKETS;
            let mut curr: usize = lower;

            for i in 0..BUCKETS {
                let bucket_size: usize = items_per_bucket + usize::from(i < buckets_with_extra);
                let our_fingerprint = self.storage.fingerprint(curr, curr + bucket_size)?;
                curr += bucket_size;

                let next_bound = if curr == upper {
                    upper_bound
                } else {
                    let mut prev_item: Item = Item::with_timestamp(0);
                    let mut curr_item: Item = Item::with_timestamp(0);

                    self.storage
                        .iterate(curr - 1, curr + 1, &mut |item: Item, index| {
                            if index == curr - 1 {
                                prev_item = item;
                            } else {
                                curr_item = item;
                            }

                            Ok(true)
                        })?;

                    self.get_minimal_bound(&prev_item, &curr_item)?
                };

                o.extend(self.encode_bound(&next_bound));
                o.extend(self.encode_mode(Mode::Fingerprint));
                o.extend(our_fingerprint.iter());
            }
        }

        Ok(o)
    }

    fn exceeded_frame_size_limit(&self, n: usize) -> bool {
        self.frame_size_limit != 0 && n > self.frame_size_limit - 200
    }

    // Decoding

    fn decode_mode(&self, encoded: &mut &[u8]) -> Result<Mode, Error> {
        let mode = decode_var_int(encoded)?;
        Mode::try_from(mode)
    }

    fn decode_timestamp_in(&mut self, encoded: &mut &[u8]) -> Result<u64, Error> {
        let timestamp: u64 = decode_var_int(encoded)?;
        let mut timestamp = if timestamp == 0 {
            MAX_U64
        } else {
            timestamp - 1
        };
        timestamp = timestamp.saturating_add(self.last_timestamp_in);
        self.last_timestamp_in = timestamp;
        Ok(timestamp)
    }

    fn decode_bound(&mut self, encoded: &mut &[u8]) -> Result<Bound, Error> {
        let timestamp = self.decode_timestamp_in(encoded)?;
        let len: usize = usize::try_from(decode_var_int(encoded)?).map_err(|_| Error::BadRange)?;
        let id: &[u8] = get_bytes(encoded, len)?;
        Bound::with_timestamp_and_id(timestamp, id)
    }

    // Encoding
    fn encode_mode(&self, mode: Mode) -> Vec<u8> {
        encode_var_int(mode.as_u64())
    }

    fn encode_timestamp_out(&mut self, timestamp: u64) -> Vec<u8> {
        if timestamp == MAX_U64 {
            self.last_timestamp_out = MAX_U64;
            return encode_var_int(0);
        }

        let temp: u64 = timestamp;
        let timestamp: u64 = timestamp.saturating_sub(self.last_timestamp_out);
        self.last_timestamp_out = temp;
        encode_var_int(timestamp.saturating_add(1))
    }

    fn encode_bound(&mut self, bound: &Bound) -> Vec<u8> {
        let mut output: Vec<u8> = Vec::new();

        output.extend(self.encode_timestamp_out(bound.item.timestamp));
        output.extend(encode_var_int(bound.id_len as u64));

        let mut bound_slice = bound.item.id.to_vec();
        bound_slice.resize(bound.id_len, 0);
        output.extend(bound_slice);

        output
    }

    fn get_minimal_bound(&self, prev: &Item, curr: &Item) -> Result<Bound, Error> {
        if curr.timestamp != prev.timestamp {
            Ok(Bound::with_timestamp(curr.timestamp))
        } else {
            let mut shared_prefix_bytes: usize = 0;
            let curr_key = curr.id;
            let prev_key = prev.id;

            for i in 0..ID_SIZE {
                if curr_key[i] != prev_key[i] {
                    break;
                }
                shared_prefix_bytes += 1;
            }
            Ok(Bound::with_timestamp_and_id(
                curr.timestamp,
                &curr_key[..=shared_prefix_bytes],
            )?)
        }
    }
}
