//! Ziplist decoder used by RDB quicklist payloads.

/// Parse a ziplist into its string elements. Integers are converted to their
/// decimal representation, matching Redis' logical list values.
pub fn parse(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if data.len() < 11 {
        return Err("ziplist too short".to_string());
    }
    let mut index = 10usize; // zlbytes(4) + zltail(4) + zllen(2)
    let mut values = Vec::new();
    while index < data.len() {
        if data[index] == 0xFF {
            return Ok(values);
        }
        // prevlen
        let prev = data[index];
        if prev < 0xFE {
            index += 1;
        } else {
            if index + 5 > data.len() {
                return Err("truncated ziplist prevlen".to_string());
            }
            index += 5;
        }
        if index >= data.len() {
            return Err("truncated ziplist entry".to_string());
        }
        let encoding = data[index];
        index += 1;
        if encoding < 0xC0 {
            let (length, header) = match encoding {
                0x00..=0x3F => ((encoding & 0x3f) as usize, 1),
                0x40..=0x7F => {
                    if index >= data.len() {
                        return Err("truncated ziplist 14-bit length".to_string());
                    }
                    let length = (((encoding & 0x3f) as usize) << 8) | data[index] as usize;
                    (length, 2)
                }
                0x80 => {
                    if index + 4 > data.len() {
                        return Err("truncated ziplist 32-bit length".to_string());
                    }
                    let length = u32::from_be_bytes([
                        data[index],
                        data[index + 1],
                        data[index + 2],
                        data[index + 3],
                    ]) as usize;
                    (length, 5)
                }
                _ => return Err("invalid ziplist string encoding".to_string()),
            };
            // `index` already points one byte after the encoding byte; for
            // multi-byte headers move past the remaining header bytes.
            index += header - 1;
            if index + length > data.len() {
                return Err("truncated ziplist string".to_string());
            }
            values.push(data[index..index + length].to_vec());
            index += length;
        } else {
            let (length, value) = match encoding {
                0xC0 => {
                    if index + 2 > data.len() {
                        return Err("truncated ziplist int16".to_string());
                    }
                    (2, i16::from_le_bytes([data[index], data[index + 1]]) as i64)
                }
                0xD0 => {
                    if index + 4 > data.len() {
                        return Err("truncated ziplist int32".to_string());
                    }
                    (
                        4,
                        i32::from_le_bytes([
                            data[index],
                            data[index + 1],
                            data[index + 2],
                            data[index + 3],
                        ]) as i64,
                    )
                }
                0xE0 => {
                    if index + 8 > data.len() {
                        return Err("truncated ziplist int64".to_string());
                    }
                    (
                        8,
                        i64::from_le_bytes([
                            data[index],
                            data[index + 1],
                            data[index + 2],
                            data[index + 3],
                            data[index + 4],
                            data[index + 5],
                            data[index + 6],
                            data[index + 7],
                        ]),
                    )
                }
                0xF0 => {
                    if index + 3 > data.len() {
                        return Err("truncated ziplist int24".to_string());
                    }
                    let raw = (data[index] as i32)
                        | ((data[index + 1] as i32) << 8)
                        | ((data[index + 2] as i32) << 16);
                    (3, ((raw << 8) >> 8) as i64)
                }
                0xF1..=0xFD => (0, ((encoding & 0x0f) as i64) - 1),
                0xFE => {
                    if index >= data.len() {
                        return Err("truncated ziplist int8".to_string());
                    }
                    (1, data[index] as i8 as i64)
                }
                _ => return Err("invalid ziplist integer encoding".to_string()),
            };
            values.push(value.to_string().into_bytes());
            index += length;
        }
    }
    Err("ziplist missing terminator".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_minimal_ziplist() {
        // zlbytes, zltail, zllen, one 6-bit string "a", end.
        let mut data = Vec::new();
        data.extend_from_slice(&13u32.to_le_bytes());
        data.extend_from_slice(&12u32.to_le_bytes());
        data.extend_from_slice(&1u16.to_le_bytes());
        data.push(0); // prevlen
        data.push(1); // 6-bit string length
        data.push(b'a');
        data.push(0xFF);
        assert_eq!(parse(&data).unwrap(), vec![b"a".to_vec()]);
    }
}
