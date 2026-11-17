//! Redis/KeyDB compatible HyperLogLog implementation.
//!
//! HLL values are stored as ordinary strings so `GETRANGE`, `SETRANGE`,
//! `APPEND`, RDB, DUMP and AOF all work on them, exactly like Redis. The
//! byte-level layout (sparse / dense encodings, cached cardinality) follows
//! `hyperloglog.c` so values are interchangeable with a real KeyDB server.

pub const HDR_SIZE: usize = 16;
pub const REGISTERS: usize = 16384;
pub const DENSE: u8 = 0;
pub const SPARSE: u8 = 1;
pub const DENSE_SIZE: usize = HDR_SIZE + (REGISTERS * 6).div_ceil(8);

const REGISTER_MAX: u32 = 63;
const BITS: usize = 6;
const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;
const SPARSE_XZERO_MAX_LEN: usize = 16384;
const HLL_Q: usize = 50;
const ALPHA_INF: f64 = 0.7213475204444817;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllError {
    /// Magic, encoding or dense length are invalid: treated as WRONGTYPE.
    Invalid,
    /// The sparse opcode stream is structurally broken: INVALIDOBJ.
    Corrupted,
}

/// Create an empty sparse HLL: header followed by XZERO:16384.
pub fn create() -> Vec<u8> {
    let mut data = vec![0u8; HDR_SIZE];
    data[0..4].copy_from_slice(b"HYLL");
    data[4] = SPARSE;
    data.extend_from_slice(&xzero_set(SPARSE_XZERO_MAX_LEN));
    data
}

/// Check the parts of the representation that `isHLLObjectOrReply` checks.
pub fn validate(data: &[u8]) -> Result<(), HllError> {
    if data.len() < HDR_SIZE {
        return Err(HllError::Invalid);
    }
    if &data[0..4] != b"HYLL" {
        return Err(HllError::Invalid);
    }
    let encoding = data[4];
    if encoding > SPARSE {
        return Err(HllError::Invalid);
    }
    if encoding == DENSE && data.len() != DENSE_SIZE {
        return Err(HllError::Invalid);
    }
    Ok(())
}

pub fn encoding(data: &[u8]) -> u8 {
    data[4]
}

pub fn invalidate_cache(data: &mut [u8]) {
    data[15] |= 0x80;
}

/// Return the cached cardinality when the cache is still valid.
pub fn cache_card(data: &[u8]) -> Option<u64> {
    if data[15] & 0x80 != 0 {
        return None;
    }
    let mut card = 0u64;
    for (index, byte) in data[8..16].iter().enumerate() {
        card |= u64::from(*byte) << (index * 8);
    }
    Some(card)
}

pub fn set_cache(data: &mut [u8], card: u64) {
    for index in 0..8 {
        data[8 + index] = ((card >> (index * 8)) & 0xff) as u8;
    }
}

/// MurmurHash64A, endian neutral, as used by Redis.
fn murmur64a(data: &[u8], seed: u32) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let len = data.len();
    let mut h = u64::from(seed) ^ (len as u64).wrapping_mul(M);
    let chunks = len / 8;
    for chunk in 0..chunks {
        let start = chunk * 8;
        let mut k = u64::from_le_bytes(data[start..start + 8].try_into().expect("chunk"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    let tail = &data[chunks * 8..];
    let mut k = 0u64;
    for (index, byte) in tail.iter().enumerate() {
        k |= u64::from(*byte) << (index * 8);
    }
    h ^= k;
    if !tail.is_empty() {
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Compute the register index and the pattern length for an element.
pub fn pat_len(element: &[u8]) -> (usize, u8) {
    let hash = murmur64a(element, 0xadc8_3b19);
    let index = (hash & ((1 << 14) - 1)) as usize;
    let mut hash = hash >> 14;
    hash |= 1u64 << HLL_Q;
    let mut bit = 1u64;
    let mut count = 1u8;
    while hash & bit == 0 {
        count += 1;
        bit <<= 1;
    }
    (index, count)
}

fn dense_get(registers: &[u8], index: usize) -> u8 {
    let byte = index * BITS / 8;
    let fb = (index * BITS) & 7;
    let fb8 = 8 - fb;
    let b0 = u32::from(registers.get(byte).copied().unwrap_or(0));
    let b1 = u32::from(registers.get(byte + 1).copied().unwrap_or(0));
    (((b0 >> fb) | (b1 << fb8)) & REGISTER_MAX) as u8
}

fn dense_add(registers: &mut [u8], index: usize, value: u8) -> bool {
    if value > dense_get(registers, index) {
        dense_set(registers, index, value);
        true
    } else {
        false
    }
}

fn dense_set(registers: &mut [u8], index: usize, value: u8) -> bool {
    let byte = index * BITS / 8;
    let fb = (index * BITS) & 7;
    let fb8 = 8 - fb;
    let value = u32::from(value);
    if let Some(slot) = registers.get_mut(byte) {
        *slot &= !(((REGISTER_MAX << fb) & 0xff) as u8);
        *slot |= ((value << fb) & 0xff) as u8;
    }
    if let Some(slot) = registers.get_mut(byte + 1) {
        *slot &= !(((REGISTER_MAX >> fb8) & 0xff) as u8);
        *slot |= ((value >> fb8) & 0xff) as u8;
    }
    true
}

fn is_zero(byte: u8) -> bool {
    byte & 0xc0 == 0
}

fn is_xzero(byte: u8) -> bool {
    byte & 0xc0 == 0x40
}

fn is_val(byte: u8) -> bool {
    byte & 0x80 != 0
}

fn zero_len(byte: u8) -> usize {
    usize::from(byte & 0x3f) + 1
}

fn xzero_len(first: u8, second: u8) -> usize {
    ((usize::from(first & 0x3f) << 8) | usize::from(second)) + 1
}

fn val_value(byte: u8) -> u8 {
    ((byte >> 2) & 0x1f) + 1
}

fn val_len(byte: u8) -> usize {
    usize::from(byte & 0x3) + 1
}

fn val_set(value: u8, length: usize) -> u8 {
    (((value - 1) << 2) | (length as u8 - 1)) | 0x80
}

fn zero_set(length: usize) -> u8 {
    (length - 1) as u8
}

fn xzero_set(length: usize) -> [u8; 2] {
    let value = length - 1;
    [((value >> 8) as u8) | 0x40, (value & 0xff) as u8]
}

/// Add every element to the HLL, returning the number of register updates.
pub fn add_elements(
    data: &mut Vec<u8>,
    elements: &[Vec<u8>],
    sparse_max: usize,
) -> Result<usize, HllError> {
    validate(data)?;
    let mut updated = 0usize;
    for element in elements {
        let (index, count) = pat_len(element);
        let changed = if encoding(data) == DENSE {
            dense_add(&mut data[HDR_SIZE..], index, count)
        } else {
            sparse_set(data, index, count, sparse_max)?
        };
        if changed {
            updated += 1;
        }
    }
    if updated > 0 {
        invalidate_cache(data);
    }
    Ok(updated)
}

fn promote_and_set(data: &mut Vec<u8>, index: usize, count: u8) -> Result<(), HllError> {
    sparse_to_dense(data)?;
    dense_add(&mut data[HDR_SIZE..], index, count);
    Ok(())
}

/// Port of `hllSparseSet`: update one register in the sparse representation.
fn sparse_set(
    data: &mut Vec<u8>,
    index: usize,
    count: u8,
    sparse_max: usize,
) -> Result<bool, HllError> {
    if count > SPARSE_VAL_MAX_VALUE {
        promote_and_set(data, index, count)?;
        return Ok(true);
    }
    if index >= REGISTERS {
        return Err(HllError::Corrupted);
    }

    let mut position = 0usize;
    let mut first = 0usize;
    let mut previous: Option<usize> = None;
    let mut span = 0usize;
    let mut found = false;
    while position < data.len() - HDR_SIZE {
        let byte = data[HDR_SIZE + position];
        let op_length;
        if is_zero(byte) {
            span = zero_len(byte);
            op_length = 1;
        } else if is_val(byte) {
            span = val_len(byte);
            op_length = 1;
        } else {
            let second = *data.get(HDR_SIZE + position + 1).unwrap_or(&0);
            span = xzero_len(byte, second);
            op_length = 2;
        }
        if index < first + span {
            found = true;
            break;
        }
        previous = Some(position);
        position += op_length;
        first += span;
    }
    if !found || position >= data.len() - HDR_SIZE || span == 0 {
        return Err(HllError::Corrupted);
    }

    let byte = data[HDR_SIZE + position];
    let zero_op = is_zero(byte);
    let xzero_op = is_xzero(byte);
    let val_op = is_val(byte);
    let runlen = if zero_op {
        zero_len(byte)
    } else if xzero_op {
        xzero_len(byte, *data.get(HDR_SIZE + position + 1).unwrap_or(&0))
    } else {
        val_len(byte)
    };

    if val_op {
        let oldcount = val_value(byte);
        if oldcount >= count {
            return Ok(false);
        }
        if runlen == 1 {
            data[HDR_SIZE + position] = val_set(count, 1);
            return merge_sparse_values(data, previous);
        }
    }
    if zero_op && runlen == 1 {
        data[HDR_SIZE + position] = val_set(count, 1);
        return merge_sparse_values(data, previous);
    }

    let last = first + span - 1;
    let mut sequence: Vec<u8> = Vec::with_capacity(5);
    if zero_op || xzero_op {
        if index != first {
            let length = index - first;
            if length > SPARSE_ZERO_MAX_LEN {
                sequence.extend_from_slice(&xzero_set(length));
            } else {
                sequence.push(zero_set(length));
            }
        }
        sequence.push(val_set(count, 1));
        if index != last {
            let length = last - index;
            if length > SPARSE_ZERO_MAX_LEN {
                sequence.extend_from_slice(&xzero_set(length));
            } else {
                sequence.push(zero_set(length));
            }
        }
    } else {
        let current = val_value(byte);
        if index != first {
            sequence.push(val_set(current, index - first));
        }
        sequence.push(val_set(count, 1));
        if index != last {
            sequence.push(val_set(current, last - index));
        }
    }

    let old_length = if xzero_op { 2 } else { 1 };
    let delta = sequence.len() as isize - old_length as isize;
    if delta > 0 && data.len() + delta as usize > sparse_max {
        promote_and_set(data, index, count)?;
        return Ok(true);
    }
    data.splice(
        HDR_SIZE + position..HDR_SIZE + position + old_length,
        sequence,
    );
    merge_sparse_values(data, previous)
}

/// Merge adjacent VAL opcodes, then invalidate the cached cardinality.
fn merge_sparse_values(data: &mut Vec<u8>, previous: Option<usize>) -> Result<bool, HllError> {
    let mut position = previous.unwrap_or(0);
    let mut scan = 5;
    while position < data.len() - HDR_SIZE && scan > 0 {
        scan -= 1;
        let byte = data[HDR_SIZE + position];
        if is_xzero(byte) {
            position += 2;
            continue;
        }
        if is_zero(byte) {
            position += 1;
            continue;
        }
        if position + 1 < data.len() - HDR_SIZE && is_val(data[HDR_SIZE + position + 1]) {
            let first_value = val_value(byte);
            let second_value = val_value(data[HDR_SIZE + position + 1]);
            if first_value == second_value {
                let length = val_len(byte) + val_len(data[HDR_SIZE + position + 1]);
                if length <= SPARSE_VAL_MAX_LEN {
                    data[HDR_SIZE + position + 1] = val_set(first_value, length);
                    data.remove(HDR_SIZE + position);
                    continue;
                }
            }
        }
        position += 1;
    }
    invalidate_cache(data);
    Ok(true)
}

/// Convert a sparse HLL to the dense representation in place.
///
/// Returns `Ok(true)` when a conversion happened and `Ok(false)` when the
/// value was already dense. The cached cardinality is preserved.
pub fn sparse_to_dense(data: &mut Vec<u8>) -> Result<bool, HllError> {
    validate(data)?;
    if encoding(data) == DENSE {
        return Ok(false);
    }
    let mut registers = vec![0u8; REGISTERS];
    let mut index = 0usize;
    let mut position = 0usize;
    while position < data.len() - HDR_SIZE {
        let byte = data[HDR_SIZE + position];
        if is_zero(byte) {
            index += zero_len(byte);
            position += 1;
        } else if is_xzero(byte) {
            let second = *data.get(HDR_SIZE + position + 1).unwrap_or(&0);
            index += xzero_len(byte, second);
            position += 2;
        } else {
            let runlen = val_len(byte);
            let regval = val_value(byte);
            if index + runlen > REGISTERS {
                break;
            }
            for slot in &mut registers[index..index + runlen] {
                *slot = regval;
            }
            index += runlen;
            position += 1;
        }
    }
    if index != REGISTERS {
        return Err(HllError::Corrupted);
    }

    let mut packed = vec![0u8; (REGISTERS * BITS).div_ceil(8)];
    for (register, value) in registers.iter().enumerate() {
        dense_set(&mut packed, register, *value);
    }
    let mut dense = Vec::with_capacity(DENSE_SIZE);
    dense.extend_from_slice(&data[..HDR_SIZE]);
    dense[4] = DENSE;
    dense.extend_from_slice(&packed);
    *data = dense;
    Ok(true)
}

/// Return all 16384 registers, converting the value to dense if needed.
pub fn get_registers(data: &mut Vec<u8>) -> Result<Vec<u8>, HllError> {
    validate(data)?;
    sparse_to_dense(data)?;
    let registers = &data[HDR_SIZE..];
    let mut values = Vec::with_capacity(REGISTERS);
    for index in 0..REGISTERS {
        values.push(dense_get(registers, index));
    }
    Ok(values)
}

/// Set one register, choosing dense or sparse storage automatically.
pub fn set_register(
    data: &mut Vec<u8>,
    index: usize,
    count: u8,
    sparse_max: usize,
) -> Result<bool, HllError> {
    validate(data)?;
    if encoding(data) == DENSE {
        Ok(dense_add(&mut data[HDR_SIZE..], index, count))
    } else {
        sparse_set(data, index, count, sparse_max)
    }
}

/// Merge an HLL's registers into `max`.
pub fn merge(data: &[u8], max: &mut [u8; REGISTERS]) -> Result<(), HllError> {
    validate(data)?;
    if encoding(data) == DENSE {
        let registers = &data[HDR_SIZE..];
        for (index, slot) in max.iter_mut().enumerate() {
            let value = dense_get(registers, index);
            if value > *slot {
                *slot = value;
            }
        }
        return Ok(());
    }

    let mut index = 0usize;
    let mut position = 0usize;
    while position < data.len() - HDR_SIZE {
        let byte = data[HDR_SIZE + position];
        if is_zero(byte) {
            index += zero_len(byte);
            position += 1;
        } else if is_xzero(byte) {
            let second = *data.get(HDR_SIZE + position + 1).unwrap_or(&0);
            index += xzero_len(byte, second);
            position += 2;
        } else {
            let runlen = val_len(byte);
            let regval = val_value(byte);
            if index + runlen > REGISTERS {
                break;
            }
            for offset in 0..runlen {
                if regval > max[index + offset] {
                    max[index + offset] = regval;
                }
            }
            index += runlen;
            position += 1;
        }
    }
    if index != REGISTERS {
        return Err(HllError::Corrupted);
    }
    Ok(())
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y += y;
        if previous == z {
            break;
        }
    }
    z
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if previous == z {
            break;
        }
    }
    z / 3.0
}

fn estimate(histogram: &[u64; 64]) -> u64 {
    let m = REGISTERS as f64;
    let mut z = m * tau((m - histogram[HLL_Q + 1] as f64) / m);
    for index in (1..=HLL_Q).rev() {
        z += histogram[index] as f64;
        z *= 0.5;
    }
    z += m * sigma(histogram[0] as f64 / m);
    let estimate = (ALPHA_INF * m * m / z).round();
    if !estimate.is_finite() || estimate <= 0.0 {
        return 0;
    }
    estimate as u64
}

/// Approximate cardinality from a dense HLL.
pub fn count(data: &[u8]) -> Result<u64, HllError> {
    validate(data)?;
    let mut histogram = [0u64; 64];
    if encoding(data) == DENSE {
        let registers = &data[HDR_SIZE..];
        for index in 0..REGISTERS {
            histogram[dense_get(registers, index) as usize] += 1;
        }
        return Ok(estimate(&histogram));
    }

    let mut index = 0usize;
    let mut position = 0usize;
    while position < data.len() - HDR_SIZE {
        let byte = data[HDR_SIZE + position];
        if is_zero(byte) {
            let runlen = zero_len(byte);
            index += runlen;
            histogram[0] += runlen as u64;
            position += 1;
        } else if is_xzero(byte) {
            let second = *data.get(HDR_SIZE + position + 1).unwrap_or(&0);
            let runlen = xzero_len(byte, second);
            index += runlen;
            histogram[0] += runlen as u64;
            position += 2;
        } else {
            let runlen = val_len(byte);
            let value = val_value(byte) as usize;
            index += runlen;
            histogram[value] += runlen as u64;
            position += 1;
        }
    }
    if index != REGISTERS {
        return Err(HllError::Corrupted);
    }
    Ok(estimate(&histogram))
}

/// Approximate cardinality from a raw register array.
pub fn count_registers(registers: &[u8; REGISTERS]) -> u64 {
    let mut histogram = [0u64; 64];
    for value in registers {
        histogram[usize::from(*value)] += 1;
    }
    estimate(&histogram)
}

/// Render the sparse opcode stream for `PFDEBUG DECODE`.
pub fn decode_sparse(data: &[u8]) -> Option<String> {
    if validate(data).is_err() || encoding(data) != SPARSE {
        return None;
    }
    let mut parts = Vec::new();
    let mut position = 0usize;
    while position < data.len() - HDR_SIZE {
        let byte = data[HDR_SIZE + position];
        if is_zero(byte) {
            parts.push(format!("z:{}", zero_len(byte)));
            position += 1;
        } else if is_xzero(byte) {
            let second = *data.get(HDR_SIZE + position + 1).unwrap_or(&0);
            parts.push(format!("Z:{}", xzero_len(byte, second)));
            position += 2;
        } else {
            parts.push(format!("v:{},{}", val_value(byte), val_len(byte)));
            position += 1;
        }
    }
    Some(parts.join(" "))
}
