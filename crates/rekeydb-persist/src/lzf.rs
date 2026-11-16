//! Minimal LZF decompressor for RDB string payloads.

/// Decompress an LZF block into `expected_len` bytes.
pub fn decompress(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(expected_len);
    let mut index = 0;
    while index < input.len() {
        let ctrl = input[index] as usize;
        index += 1;
        if ctrl < 32 {
            let length = ctrl + 1;
            if index + length > input.len() {
                return None;
            }
            output.extend_from_slice(&input[index..index + length]);
            index += length;
        } else {
            let mut length = ctrl >> 5;
            if length == 7 {
                if index >= input.len() {
                    return None;
                }
                length += input[index] as usize;
                index += 1;
            }
            if index >= input.len() {
                return None;
            }
            let reference = output
                .len()
                .checked_sub(((ctrl & 0x1f) << 8) + input[index] as usize + 1)?;
            index += 1;
            if reference >= output.len() {
                return None;
            }
            for offset in 0..length + 2 {
                let byte = *output.get(reference + offset)?;
                output.push(byte);
            }
        }
    }
    if output.len() == expected_len {
        Some(output)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_run() {
        // ctrl=2 -> three literal bytes.
        assert_eq!(decompress(&[2, b'a', b'b', b'c'], 3).unwrap(), b"abc");
    }
}
