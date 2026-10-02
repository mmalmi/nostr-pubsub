// Copyright (c) 2023 Doug Hoyte
// Copyright (c) 2023 Yuki Kishimoto
// Distributed under the MIT software license

use core::convert::TryInto;

use negentropy::Error;

#[inline]
pub fn get_byte_array<const N: usize>(encoded: &mut &[u8]) -> Result<[u8; N], Error> {
    Ok(get_bytes(encoded, N)?.try_into()?)
}

pub fn get_bytes<'a>(encoded: &'a mut &[u8], n: usize) -> Result<&'a [u8], Error> {
    if encoded.len() < n {
        return Err(Error::ParseEndsPrematurely);
    }
    let res: &[u8] = &encoded[..n];
    *encoded = encoded.get(n..).unwrap_or_default();
    Ok(res)
}

pub fn decode_var_int(encoded: &mut &[u8]) -> Result<u64, Error> {
    let mut value = 0_u64;
    for _ in 0..10 {
        let byte = get_byte_array::<1>(encoded)?[0];
        value = value
            .checked_mul(128)
            .and_then(|n| n.checked_add(u64::from(byte & 127)))
            .ok_or(Error::BadRange)?;
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    Err(Error::BadRange)
}

pub fn encode_var_int(mut n: u64) -> Vec<u8> {
    if n == 0 {
        return vec![0];
    }

    let mut o: Vec<u8> = Vec::with_capacity(10);

    while n > 0 {
        o.push((n & 0x7F) as u8);
        n >>= 7;
    }

    o.reverse();

    for i in 0..(o.len() - 1) {
        o[i] |= 0x80;
    }

    o
}
