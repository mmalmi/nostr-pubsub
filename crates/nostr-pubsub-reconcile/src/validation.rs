use crate::Result;

// Validate untrusted ranges before calling the upstream codec, including checked
// timestamp arithmetic. Work is bounded by the already-checked frame byte limit.
pub(crate) fn validate(mut bytes: &[u8]) -> Result<()> {
    if take(&mut bytes, 1)? != [0x61] {
        return Err("unsupported Negentropy version".into());
    }
    let (mut timestamp, mut previous) = (0_u64, (0_u64, [0_u8; 32]));
    while !bytes.is_empty() {
        let delta = varint(&mut bytes)?;
        timestamp = if delta == 0 || timestamp == u64::MAX {
            u64::MAX
        } else {
            timestamp
                .checked_add(delta - 1)
                .ok_or("timestamp overflow")?
        };
        let len = usize::try_from(varint(&mut bytes)?).map_err(|_| "invalid bound length")?;
        if len > 32 {
            return Err("invalid bound length".into());
        }
        let mut id = [0; 32];
        id[..len].copy_from_slice(take(&mut bytes, len)?);
        if (timestamp, id) < previous {
            return Err("unordered reconciliation ranges".into());
        }
        previous = (timestamp, id);
        match varint(&mut bytes)? {
            0 => {}
            1 => {
                take(&mut bytes, 16)?;
            }
            2 => {
                let count = usize::try_from(varint(&mut bytes)?).map_err(|_| "invalid ID count")?;
                take(
                    &mut bytes,
                    count.checked_mul(32).ok_or("ID count overflow")?,
                )?;
            }
            _ => return Err("invalid reconciliation mode".into()),
        }
    }
    Ok(())
}

fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if n > bytes.len() {
        return Err("truncated reconciliation frame".into());
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}

fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut result = 0_u64;
    for _ in 0..10 {
        let byte = take(bytes, 1)?[0];
        result = result
            .checked_mul(128)
            .and_then(|v| v.checked_add(u64::from(byte & 127)))
            .ok_or("varint overflow")?;
        if byte & 128 == 0 {
            return Ok(result);
        }
    }
    Err("varint too long".into())
}
