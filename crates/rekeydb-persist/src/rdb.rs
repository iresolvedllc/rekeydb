//! RDB v9 reader and writer for the value types implemented so far.
//!
//! The writer emits uncompressed strings and the legacy `RDB_TYPE_LIST`
//! representation for lists, which KeyDB's loader converts into quicklists.
//! The reader accepts KeyDB-produced quicklist/ziplist and intset encodings so
//! the corpus can grow before our writer emits those compact forms.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use rekeydb_core::{HashValue, SortedSet, Value};

use crate::crc64::crc64;
use crate::lzf;
use crate::ziplist;

const RDB_VERSION: u32 = 9;
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

const RDB_OPCODE_MODULE_AUX: u8 = 247;
const RDB_OPCODE_IDLE: u8 = 248;
const RDB_OPCODE_FREQ: u8 = 249;
const RDB_OPCODE_AUX: u8 = 250;
const RDB_OPCODE_RESIZEDB: u8 = 251;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
const RDB_OPCODE_EXPIRETIME: u8 = 253;
const RDB_OPCODE_SELECTDB: u8 = 254;
const RDB_OPCODE_EOF: u8 = 255;

const RDB_ENC_INT8: u64 = 0;
const RDB_ENC_INT16: u64 = 1;
const RDB_ENC_INT32: u64 = 2;
const RDB_ENC_LZF: u64 = 3;

#[derive(Debug, Clone)]
pub struct RdbEntry {
    pub db: usize,
    pub key: Vec<u8>,
    pub value: Value,
    pub expire_ms: Option<i64>,
}

#[derive(Debug)]
pub enum RdbError {
    UnexpectedEof,
    InvalidMagic,
    UnsupportedVersion(u32),
    UnsupportedType(u8),
    UnsupportedValue(&'static str),
    Corrupt(String),
    Checksum,
    UnsupportedEncoding(u64),
}

impl fmt::Display for RdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of RDB file"),
            Self::InvalidMagic => write!(f, "invalid RDB magic"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported RDB version {version}"),
            Self::UnsupportedType(kind) => write!(f, "unsupported RDB object type {kind}"),
            Self::UnsupportedValue(kind) => write!(f, "unsupported value type {kind}"),
            Self::Corrupt(message) => write!(f, "corrupt RDB: {message}"),
            Self::Checksum => write!(f, "RDB checksum mismatch"),
            Self::UnsupportedEncoding(encoding) => {
                write!(f, "unsupported RDB string encoding {encoding}")
            }
        }
    }
}

impl std::error::Error for RdbError {}

pub fn save(entries: &[RdbEntry]) -> Result<Vec<u8>, RdbError> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("REDIS{RDB_VERSION:04}").as_bytes());
    write_aux(&mut out, b"redis-ver", b"6.3.0");
    write_aux(&mut out, b"redis-bits", b"64");
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    write_aux(&mut out, b"ctime", ctime.to_string().as_bytes());
    write_aux(&mut out, b"used-mem", b"0");

    let max_db = entries.iter().map(|entry| entry.db).max().unwrap_or(0);
    for db in 0..=max_db {
        let rows: Vec<&RdbEntry> = entries.iter().filter(|entry| entry.db == db).collect();
        if rows.is_empty() {
            continue;
        }
        out.push(RDB_OPCODE_SELECTDB);
        write_len(&mut out, db as u64);
        out.push(RDB_OPCODE_RESIZEDB);
        write_len(&mut out, rows.len() as u64);
        write_len(
            &mut out,
            rows.iter()
                .filter(|entry| entry.expire_ms.is_some())
                .count() as u64,
        );
        for entry in rows {
            if let Some(expire) = entry.expire_ms {
                out.push(RDB_OPCODE_EXPIRETIME_MS);
                out.extend_from_slice(&(expire as u64).to_le_bytes());
            }
            out.push(object_type(&entry.value)?);
            write_string(&mut out, &entry.key);
            write_object_value(&mut out, &entry.value)?;
        }
    }

    out.push(RDB_OPCODE_EOF);
    let checksum = crc64(0, &out);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

fn object_type(value: &Value) -> Result<u8, RdbError> {
    match value {
        Value::String(_) => Ok(RDB_TYPE_STRING),
        Value::List(list) if !list.is_empty() => Ok(RDB_TYPE_LIST),
        Value::Set(set) if !set.is_empty() => Ok(RDB_TYPE_SET),
        Value::List(_) => Err(RdbError::UnsupportedValue("empty list")),
        Value::Set(_) => Err(RdbError::UnsupportedValue("empty set")),
        Value::Hash(_) => Ok(RDB_TYPE_HASH),
        Value::ZSet(_) => Ok(RDB_TYPE_ZSET_2),
        Value::Stream(_) => Err(RdbError::UnsupportedValue("stream")),
    }
}

fn write_object_value(out: &mut Vec<u8>, value: &Value) -> Result<(), RdbError> {
    match value {
        Value::String(bytes) => write_string(out, bytes),
        Value::List(list) => {
            write_len(out, list.len() as u64);
            for item in list {
                write_string(out, item);
            }
        }
        Value::Set(set) => {
            write_len(out, set.len() as u64);
            for member in set {
                write_string(out, member);
            }
        }
        Value::Hash(map) => {
            write_len(out, map.len() as u64);
            for (field, value) in map {
                write_string(out, field);
                write_string(out, value);
            }
        }
        Value::ZSet(zset) => {
            write_len(out, zset.scores.len() as u64);
            for (member, score) in &zset.scores {
                write_string(out, member);
                out.extend_from_slice(&score.to_bits().to_le_bytes());
            }
        }
        Value::Stream(_) => return Err(RdbError::UnsupportedValue("stream")),
    }
    Ok(())
}

fn write_object(out: &mut Vec<u8>, value: &Value) -> Result<(), RdbError> {
    out.push(object_type(value)?);
    write_object_value(out, value)
}

/// Create a `DUMP` payload: object type + object + KeyDB aux + version + CRC.
pub fn dump(value: &Value) -> Result<Vec<u8>, RdbError> {
    let mut out = Vec::new();
    write_object(&mut out, value)?;
    // KeyDB duplicates RESTORE payloads with its MVCC timestamp. Emit a
    // neutral value; the field is consumed by RESTORE and ignored by us.
    write_aux(&mut out, b"mvcc-tstamp", b"0");
    out.extend_from_slice(&(RDB_VERSION as u16).to_le_bytes());
    let checksum = crc64(0, &out);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

/// Parse and verify a `DUMP` payload.
pub fn restore(bytes: &[u8]) -> Result<Value, RdbError> {
    if bytes.len() < 10 {
        return Err(RdbError::UnexpectedEof);
    }
    let footer = bytes.len() - 10;
    let version = u16::from_le_bytes(bytes[footer..footer + 2].try_into().expect("2 bytes"));
    if u32::from(version) > RDB_VERSION {
        return Err(RdbError::UnsupportedVersion(u32::from(version)));
    }
    let expected = u64::from_le_bytes(bytes[bytes.len() - 8..].try_into().expect("8 bytes"));
    let actual = crc64(0, &bytes[..bytes.len() - 8]);
    if expected != actual {
        return Err(RdbError::Checksum);
    }

    let mut reader = Reader {
        data: &bytes[..footer],
        position: 0,
    };
    let kind = reader.u8()?;
    let value = read_object(kind, &mut reader)?;
    // KeyDB appends auxiliary fields after the object. Ignore them.
    while reader.position < footer {
        let opcode = reader.u8()?;
        match opcode {
            RDB_OPCODE_AUX => {
                let _ = reader.string()?;
                let _ = reader.string()?;
            }
            RDB_OPCODE_EOF => break,
            _ => return Err(RdbError::UnsupportedType(opcode)),
        }
    }
    Ok(value)
}

pub fn load(bytes: &[u8]) -> Result<Vec<RdbEntry>, RdbError> {
    if bytes.len() < 17 {
        return Err(RdbError::UnexpectedEof);
    }
    if &bytes[..5] != b"REDIS" {
        return Err(RdbError::InvalidMagic);
    }
    let version = std::str::from_utf8(&bytes[5..9])
        .ok()
        .and_then(|text| text.parse::<u32>().ok())
        .ok_or(RdbError::InvalidMagic)?;
    if version > RDB_VERSION {
        return Err(RdbError::UnsupportedVersion(version));
    }
    let body_end = bytes.len() - 8;
    let expected = u64::from_le_bytes(bytes[body_end..].try_into().expect("8 bytes"));
    if expected != 0 {
        let actual = crc64(0, &bytes[..body_end]);
        if actual != expected {
            return Err(RdbError::Checksum);
        }
    }

    let mut reader = Reader {
        data: bytes,
        position: 9,
    };
    let mut db = 0usize;
    let mut pending_expire = None;
    let mut entries = Vec::new();
    loop {
        let opcode = reader.u8()?;
        match opcode {
            RDB_OPCODE_EOF => break,
            RDB_OPCODE_AUX => {
                let _key = reader.string()?;
                let _value = reader.string()?;
            }
            RDB_OPCODE_SELECTDB => db = reader.len()? as usize,
            RDB_OPCODE_RESIZEDB => {
                let _db_size = reader.len()?;
                let _expires = reader.len()?;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                pending_expire = Some(reader.u64_le()? as i64);
            }
            RDB_OPCODE_EXPIRETIME => {
                pending_expire = Some((reader.u32_le()? as i64).saturating_mul(1000));
            }
            RDB_OPCODE_IDLE => {
                let _idle = reader.len()?;
            }
            RDB_OPCODE_FREQ => {
                let _freq = reader.u8()?;
            }
            RDB_OPCODE_MODULE_AUX => {
                return Err(RdbError::UnsupportedType(RDB_OPCODE_MODULE_AUX));
            }
            kind if is_object_type(kind) => {
                let key = reader.string()?;
                let value = read_object(kind, &mut reader)?;
                entries.push(RdbEntry {
                    db,
                    key,
                    value,
                    expire_ms: pending_expire.take(),
                });
            }
            other => return Err(RdbError::UnsupportedType(other)),
        }
    }
    Ok(entries)
}

fn is_object_type(kind: u8) -> bool {
    (kind <= 7) || (9..=15).contains(&kind)
}

fn read_object(kind: u8, reader: &mut Reader<'_>) -> Result<Value, RdbError> {
    match kind {
        RDB_TYPE_STRING => Ok(Value::String(reader.string()?)),
        RDB_TYPE_LIST => {
            let length = reader.len()? as usize;
            let mut list = std::collections::VecDeque::with_capacity(length);
            for _ in 0..length {
                list.push_back(reader.string()?);
            }
            Ok(Value::List(list))
        }
        RDB_TYPE_SET => {
            let length = reader.len()? as usize;
            let mut set = std::collections::HashSet::with_capacity(length);
            for _ in 0..length {
                set.insert(reader.string()?);
            }
            Ok(Value::Set(set))
        }
        RDB_TYPE_ZSET | RDB_TYPE_ZSET_2 => {
            let length = reader.len()? as usize;
            let mut zset = SortedSet::default();
            for _ in 0..length {
                let member = reader.string()?;
                let score = if kind == RDB_TYPE_ZSET_2 {
                    f64::from_bits(reader.u64_le()?)
                } else {
                    read_old_double(reader)?
                };
                zset.scores.insert(member, score);
            }
            Ok(Value::ZSet(zset))
        }
        RDB_TYPE_HASH => {
            let length = reader.len()? as usize;
            let mut hash = HashValue::with_capacity(length);
            for _ in 0..length {
                let field = reader.string()?;
                let value = reader.string()?;
                hash.insert(field, value);
            }
            Ok(Value::Hash(hash))
        }
        RDB_TYPE_SET_INTSET => {
            let blob = reader.string()?;
            Ok(Value::Set(parse_intset(&blob)?))
        }
        RDB_TYPE_LIST_QUICKLIST => {
            let nodes = reader.len()? as usize;
            let mut list = std::collections::VecDeque::new();
            for _ in 0..nodes {
                let blob = reader.string()?;
                let values = ziplist::parse(&blob).map_err(RdbError::Corrupt)?;
                list.extend(values);
            }
            Ok(Value::List(list))
        }
        RDB_TYPE_LIST_ZIPLIST => {
            let blob = reader.string()?;
            let values = ziplist::parse(&blob).map_err(RdbError::Corrupt)?;
            Ok(Value::List(values.into_iter().collect()))
        }
        RDB_TYPE_HASH_ZIPLIST => {
            let blob = reader.string()?;
            let values = ziplist::parse(&blob).map_err(RdbError::Corrupt)?;
            if !values.len().is_multiple_of(2) {
                return Err(RdbError::Corrupt(
                    "hash ziplist has an odd number of entries".to_string(),
                ));
            }
            let mut hash = HashValue::with_capacity(values.len() / 2);
            let mut iter = values.into_iter();
            while let (Some(field), Some(value)) = (iter.next(), iter.next()) {
                hash.insert(field, value);
            }
            Ok(Value::Hash(hash))
        }
        RDB_TYPE_ZSET_ZIPLIST => {
            let blob = reader.string()?;
            let values = ziplist::parse(&blob).map_err(RdbError::Corrupt)?;
            if !values.len().is_multiple_of(2) {
                return Err(RdbError::Corrupt(
                    "zset ziplist has an odd number of entries".to_string(),
                ));
            }
            let mut zset = SortedSet::default();
            let mut iter = values.into_iter();
            while let (Some(member), Some(score)) = (iter.next(), iter.next()) {
                let text = String::from_utf8_lossy(&score);
                let parsed = text
                    .parse::<f64>()
                    .map_err(|_| RdbError::Corrupt("invalid zset score".to_string()))?;
                zset.scores.insert(member, parsed);
            }
            Ok(Value::ZSet(zset))
        }
        RDB_TYPE_HASH_ZIPMAP | RDB_TYPE_STREAM_LISTPACKS => Err(RdbError::UnsupportedType(kind)),
        _ => Err(RdbError::UnsupportedType(kind)),
    }
}

fn read_old_double(reader: &mut Reader<'_>) -> Result<f64, RdbError> {
    let length = reader.u8()?;
    match length {
        255 => Ok(f64::NEG_INFINITY),
        254 => Ok(f64::INFINITY),
        253 => Ok(f64::NAN),
        length => {
            let bytes = reader.bytes(length as usize)?;
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.parse::<f64>().ok())
                .ok_or_else(|| RdbError::Corrupt("invalid double value".to_string()))
        }
    }
}

fn parse_intset(blob: &[u8]) -> Result<std::collections::HashSet<Vec<u8>>, RdbError> {
    if blob.len() < 8 {
        return Err(RdbError::Corrupt("truncated intset".to_string()));
    }
    let encoding = u32::from_le_bytes(blob[0..4].try_into().expect("4 bytes"));
    let length = u32::from_le_bytes(blob[4..8].try_into().expect("4 bytes")) as usize;
    let width = match encoding {
        2 => 2usize,
        4 => 4,
        8 => 8,
        other => {
            return Err(RdbError::Corrupt(format!(
                "invalid intset encoding {other}"
            )));
        }
    };
    if blob.len() < 8 + width * length {
        return Err(RdbError::Corrupt("truncated intset elements".to_string()));
    }
    let mut set = std::collections::HashSet::with_capacity(length);
    for index in 0..length {
        let offset = 8 + index * width;
        let value = match width {
            2 => i16::from_le_bytes(blob[offset..offset + 2].try_into().expect("2 bytes")) as i64,
            4 => i32::from_le_bytes(blob[offset..offset + 4].try_into().expect("4 bytes")) as i64,
            8 => i64::from_le_bytes(blob[offset..offset + 8].try_into().expect("8 bytes")),
            _ => unreachable!(),
        };
        set.insert(value.to_string().into_bytes());
    }
    Ok(set)
}

fn write_aux(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    out.push(RDB_OPCODE_AUX);
    write_string(out, key);
    write_string(out, value);
}

fn write_len(out: &mut Vec<u8>, length: u64) {
    if length < (1 << 6) {
        out.push(length as u8);
    } else if length < (1 << 14) {
        out.push(0x40 | ((length >> 8) as u8));
        out.push(length as u8);
    } else if length <= u32::MAX as u64 {
        out.push(0x80);
        out.extend_from_slice(&(length as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&length.to_be_bytes());
    }
}

fn write_string(out: &mut Vec<u8>, value: &[u8]) {
    write_len(out, value.len() as u64);
    out.extend_from_slice(value);
}

struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], RdbError> {
        if self.position + length > self.data.len() {
            return Err(RdbError::UnexpectedEof);
        }
        let slice = &self.data[self.position..self.position + length];
        self.position += length;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, RdbError> {
        Ok(self.bytes(1)?[0])
    }

    fn u32_le(&mut self) -> Result<u32, RdbError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64_le(&mut self) -> Result<u64, RdbError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn len(&mut self) -> Result<u64, RdbError> {
        let (length, encoded) = self.len_encoded()?;
        if encoded {
            return Err(RdbError::Corrupt(
                "encoded length where plain length expected".to_string(),
            ));
        }
        Ok(length)
    }

    fn len_encoded(&mut self) -> Result<(u64, bool), RdbError> {
        let first = self.u8()?;
        match first >> 6 {
            0 => Ok((u64::from(first & 0x3f), false)),
            1 => {
                let second = self.u8()?;
                Ok(((u64::from(first & 0x3f) << 8) | u64::from(second), false))
            }
            2 => match first {
                0x80 => Ok((
                    u64::from(u32::from_be_bytes(
                        self.bytes(4)?.try_into().expect("4 bytes"),
                    )),
                    false,
                )),
                0x81 => Ok((
                    u64::from_be_bytes(self.bytes(8)?.try_into().expect("8 bytes")),
                    false,
                )),
                other => Err(RdbError::Corrupt(format!(
                    "unknown length encoding {other}"
                ))),
            },
            3 => Ok((u64::from(first & 0x3f), true)),
            _ => unreachable!(),
        }
    }

    fn string(&mut self) -> Result<Vec<u8>, RdbError> {
        let (length, encoded) = self.len_encoded()?;
        if !encoded {
            return Ok(self.bytes(length as usize)?.to_vec());
        }
        match length {
            RDB_ENC_INT8 => {
                let value = self.bytes(1)?[0] as i8 as i64;
                Ok(value.to_string().into_bytes())
            }
            RDB_ENC_INT16 => {
                let value = i16::from_le_bytes(self.bytes(2)?.try_into().expect("2 bytes"));
                Ok((value as i64).to_string().into_bytes())
            }
            RDB_ENC_INT32 => {
                let value = i32::from_le_bytes(self.bytes(4)?.try_into().expect("4 bytes"));
                Ok((value as i64).to_string().into_bytes())
            }
            RDB_ENC_LZF => {
                let compressed_len = self.len()? as usize;
                let original_len = self.len()? as usize;
                let compressed = self.bytes(compressed_len)?;
                lzf::decompress(compressed, original_len)
                    .ok_or_else(|| RdbError::Corrupt("invalid LZF payload".to_string()))
            }
            other => Err(RdbError::UnsupportedEncoding(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashSet, VecDeque};

    fn entry(value: Value, expire_ms: Option<i64>) -> RdbEntry {
        RdbEntry {
            db: 0,
            key: b"key".to_vec(),
            value,
            expire_ms,
        }
    }

    #[test]
    fn roundtrips_supported_values() {
        let mut hash = HashValue::new();
        hash.insert(b"field".to_vec(), b"value".to_vec());
        let mut zset = SortedSet::default();
        zset.scores.insert(b"member".to_vec(), 1.5);
        let mut set = HashSet::new();
        set.insert(b"one".to_vec());
        set.insert(b"2".to_vec());
        let entries = vec![
            entry(Value::String(b"value".to_vec()), None),
            entry(
                Value::List(VecDeque::from(vec![b"a".to_vec(), b"b".to_vec()])),
                Some(1_700_000_000_000),
            ),
            entry(Value::Set(set), None),
            entry(Value::Hash(hash), None),
            entry(Value::ZSet(zset), None),
        ];
        let bytes = save(&entries).unwrap();
        let loaded = load(&bytes).unwrap();
        assert_eq!(loaded.len(), 5);
        assert_eq!(loaded[0].key, b"key");
        assert_eq!(loaded[1].expire_ms, Some(1_700_000_000_000));
        match &loaded[1].value {
            Value::List(list) => {
                assert_eq!(list, &VecDeque::from(vec![b"a".to_vec(), b"b".to_vec()]))
            }
            _ => panic!("expected list"),
        }
        match &loaded[3].value {
            Value::Hash(hash) => {
                assert_eq!(hash.get(b"field".as_slice()), Some(&b"value".to_vec()))
            }
            _ => panic!("expected hash"),
        }
        match &loaded[4].value {
            Value::ZSet(zset) => assert_eq!(zset.scores.get(b"member".as_slice()), Some(&1.5)),
            _ => panic!("expected zset"),
        }
    }

    #[test]
    fn dump_restore_roundtrip() {
        let payload = dump(&Value::String(b"hello".to_vec())).unwrap();
        assert_eq!(restore(&payload).unwrap(), Value::String(b"hello".to_vec()));
        let mut payload = dump(&Value::Set([b"a".to_vec()].into_iter().collect())).unwrap();
        let last = payload.len() - 1;
        payload[last] ^= 0xFF;
        assert!(matches!(restore(&payload), Err(RdbError::Checksum)));
    }

    #[test]
    fn rejects_corrupt_checksum() {
        let bytes = save(&[entry(Value::String(b"value".to_vec()), None)]).unwrap();
        let mut corrupt = bytes.clone();
        corrupt[10] ^= 0xFF;
        assert!(matches!(load(&corrupt), Err(RdbError::Checksum)));
    }

    #[test]
    fn parses_intset() {
        let mut blob = Vec::new();
        blob.extend_from_slice(&2u32.to_le_bytes());
        blob.extend_from_slice(&2u32.to_le_bytes());
        blob.extend_from_slice(&1i16.to_le_bytes());
        blob.extend_from_slice(&2i16.to_le_bytes());
        let set = parse_intset(&blob).unwrap();
        assert!(set.contains(b"1".as_slice()));
        assert!(set.contains(b"2".as_slice()));
    }
}
