//! Redis/KeyDB CRC64 (Jones polynomial, reflected input/output).

const POLY: u64 = 0xad93d23594c935a9;

fn reflect(mut data: u64, bits: u32) -> u64 {
    let mut result = data & 1;
    for _ in 1..bits {
        data >>= 1;
        result = (result << 1) | (data & 1);
    }
    result
}

pub fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    for &byte in data {
        let mut mask = 0x01u8;
        while mask != 0 {
            let mut bit = crc & 0x8000_0000_0000_0000;
            if byte & mask != 0 {
                // C's `!bit` is logical negation, not a bitwise complement.
                bit = u64::from(bit == 0);
            }
            crc <<= 1;
            if bit != 0 {
                crc ^= POLY;
            }
            mask <<= 1;
        }
    }
    reflect(crc, 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector() {
        assert_eq!(crc64(0, b"123456789"), 0xe9c6d914c4b8d9ca);
    }
}
