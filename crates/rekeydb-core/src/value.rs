//! Redis/KeyDB value types and encoding rules.
//!
//! M1 starts with strings and sets. Lists, hashes, and sorted sets use the
//! same enum so command handlers can be added without another database
//! refactor. Compact encodings (intset, ziplist, quicklist, skiplist) will be
//! added behind these variants; the observable `OBJECT ENCODING` contract is
//! defined here from the start.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;

use indexmap::IndexMap;

/// Thresholds mirrored from Redis 6.2 / KeyDB configuration defaults.
#[derive(Debug, Clone, Copy)]
pub struct EncodingLimits {
    pub hash_max_ziplist_entries: usize,
    pub hash_max_ziplist_value: usize,
    pub set_max_intset_entries: usize,
    pub zset_max_ziplist_entries: usize,
    pub zset_max_ziplist_value: usize,
}

impl Default for EncodingLimits {
    fn default() -> Self {
        Self {
            hash_max_ziplist_entries: 512,
            hash_max_ziplist_value: 64,
            set_max_intset_entries: 512,
            zset_max_ziplist_entries: 128,
            zset_max_ziplist_value: 64,
        }
    }
}

/// Sorted set placeholder. A skiplist-backed implementation lands with the
/// ZSET command family; scores are authoritative here.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SortedSet {
    pub scores: HashMap<Vec<u8>, f64>,
}

pub type HashValue = IndexMap<Vec<u8>, Vec<u8>>;

/// A 128-bit stream entry ID (`<milliseconds>-<sequence>`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: Self = Self { ms: 0, seq: 0 };
    pub const MAX: Self = Self {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    pub fn next(self) -> Option<Self> {
        if self.seq == u64::MAX {
            self.ms.checked_add(1).map(|ms| Self { ms, seq: 0 })
        } else {
            Some(Self {
                ms: self.ms,
                seq: self.seq + 1,
            })
        }
    }

    pub fn previous(self) -> Option<Self> {
        if self.seq == 0 {
            self.ms.checked_sub(1).map(|ms| Self { ms, seq: u64::MAX })
        } else {
            Some(Self {
                ms: self.ms,
                seq: self.seq - 1,
            })
        }
    }

    /// Parse `ms`, `ms-seq` or the `-` / `+` special IDs.
    pub fn parse(text: &str, default_seq: u64) -> Option<Self> {
        if text == "-" {
            return Some(Self::MIN);
        }
        if text == "+" {
            return Some(Self::MAX);
        }
        let (ms, seq) = match text.split_once('-') {
            Some((ms, seq)) => (ms, Some(seq)),
            None => (text, None),
        };
        if ms.is_empty() || !ms.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let ms = ms.parse::<u64>().ok()?;
        let seq = match seq {
            Some(seq) => {
                if seq.is_empty() || !seq.bytes().all(|byte| byte.is_ascii_digit()) {
                    return None;
                }
                seq.parse::<u64>().ok()?
            }
            None => default_seq,
        };
        Some(Self { ms, seq })
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.ms, self.seq)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamNode {
    pub entries: Vec<StreamEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamNack {
    pub consumer: Vec<u8>,
    pub delivery_time: i64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamConsumer {
    pub name: Vec<u8>,
    pub seen_time: i64,
    pub pel: BTreeSet<StreamId>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamGroup {
    pub last_delivered_id: StreamId,
    pub pel: BTreeMap<StreamId, StreamNack>,
    pub consumers: BTreeMap<Vec<u8>, StreamConsumer>,
}

/// A stream, modeled as the ordered sequence of radix-tree listpack nodes
/// used by Redis. Keeping the node boundaries is required for approximate
/// `MAXLEN`/`MINID` trimming and for `stream-node-max-entries` semantics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stream {
    pub nodes: Vec<StreamNode>,
    pub last_id: StreamId,
    pub groups: BTreeMap<Vec<u8>, StreamGroup>,
}

impl Stream {
    pub fn len(&self) -> usize {
        self.nodes.iter().map(|node| node.entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &StreamEntry> {
        self.nodes.iter().flat_map(|node| node.entries.iter())
    }

    pub fn first_id(&self) -> Option<StreamId> {
        self.iter().next().map(|entry| entry.id)
    }

    pub fn last_valid_id(&self) -> Option<StreamId> {
        self.nodes
            .last()
            .and_then(|node| node.entries.last())
            .map(|entry| entry.id)
    }

    pub fn find(&self, id: StreamId) -> Option<&StreamEntry> {
        self.iter().find(|entry| entry.id == id)
    }

    pub fn append(
        &mut self,
        id: StreamId,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        node_max_entries: usize,
    ) {
        let max_entries = node_max_entries.max(1);
        let entry = StreamEntry { id, fields };
        if let Some(node) = self.nodes.last_mut()
            && node.entries.len() < max_entries
        {
            node.entries.push(entry);
        } else {
            self.nodes.push(StreamNode {
                entries: vec![entry],
            });
        }
        self.last_id = id;
    }

    pub fn delete(&mut self, id: StreamId) -> bool {
        for position in 0..self.nodes.len() {
            if let Some(index) = self.nodes[position]
                .entries
                .iter()
                .position(|entry| entry.id == id)
            {
                self.nodes[position].entries.remove(index);
                if self.nodes[position].entries.is_empty() {
                    self.nodes.remove(position);
                }
                return true;
            }
        }
        false
    }

    /// Delete entries from the front following Redis' `MAXLEN` trimming.
    pub fn trim_maxlen(&mut self, maxlen: usize, approx: bool, limit: usize) -> usize {
        let mut deleted = 0usize;
        while self.len() > maxlen {
            let Some(node) = self.nodes.first() else {
                break;
            };
            let entries = node.entries.len();
            if limit != 0 && deleted + entries > limit {
                break;
            }
            if self.len() - entries >= maxlen {
                self.nodes.remove(0);
                deleted += entries;
                continue;
            }
            if approx {
                break;
            }
            let remove = (self.len() - maxlen).min(entries);
            self.nodes[0].entries.drain(..remove);
            deleted += remove;
            if self.nodes[0].entries.is_empty() {
                self.nodes.remove(0);
            }
            break;
        }
        deleted
    }

    /// Delete entries from the front following Redis' `MINID` trimming.
    pub fn trim_minid(&mut self, minid: StreamId, approx: bool, limit: usize) -> usize {
        let mut deleted = 0usize;
        while !self.nodes.is_empty() {
            let node = self.nodes.first().expect("non-empty");
            let entries = node.entries.len();
            if limit != 0 && deleted + entries > limit {
                break;
            }
            let last_id = node.entries.last().map(|entry| entry.id);
            if last_id.is_some_and(|id| id < minid) {
                self.nodes.remove(0);
                deleted += entries;
                continue;
            }
            if approx {
                break;
            }
            let remove = self.nodes[0]
                .entries
                .iter()
                .take_while(|entry| entry.id < minid)
                .count();
            self.nodes[0].entries.drain(..remove);
            deleted += remove;
            if self.nodes[0].entries.is_empty() {
                self.nodes.remove(0);
            }
            break;
        }
        deleted
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashValue),
    Set(HashSet<Vec<u8>>),
    ZSet(SortedSet),
    Stream(Stream),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::List(_) => "list",
            Self::Hash(_) => "hash",
            Self::Set(_) => "set",
            Self::ZSet(_) => "zset",
            Self::Stream(_) => "stream",
        }
    }

    pub fn encoding(&self, limits: EncodingLimits) -> &'static str {
        match self {
            Self::String(bytes) => string_encoding(bytes),
            Self::List(_) => "quicklist",
            Self::Hash(map) => {
                if map.len() <= limits.hash_max_ziplist_entries
                    && map.iter().all(|(k, v)| {
                        k.len() <= limits.hash_max_ziplist_value
                            && v.len() <= limits.hash_max_ziplist_value
                    })
                {
                    "ziplist"
                } else {
                    "hashtable"
                }
            }
            Self::Set(set) => {
                if set.len() <= limits.set_max_intset_entries
                    && set.iter().all(|member| is_canonical_int(member))
                {
                    "intset"
                } else {
                    "hashtable"
                }
            }
            Self::Stream(_) => "stream",
            Self::ZSet(zset) => {
                if zset.scores.len() <= limits.zset_max_ziplist_entries
                    && zset
                        .scores
                        .keys()
                        .all(|member| member.len() <= limits.zset_max_ziplist_value)
                {
                    "ziplist"
                } else {
                    "skiplist"
                }
            }
        }
    }

    /// Number of logical elements: string bytes, collection members.
    pub fn len(&self) -> usize {
        match self {
            Self::String(bytes) => bytes.len(),
            Self::List(list) => list.len(),
            Self::Hash(map) => map.len(),
            Self::Set(set) => set.len(),
            Self::ZSet(zset) => zset.scores.len(),
            Self::Stream(stream) => stream.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rough allocation estimate used by `MEMORY USAGE` and `INFO memory`.
    pub fn memory_usage(&self) -> usize {
        match self {
            Self::String(bytes) => bytes.len() + 16,
            Self::List(list) => 32 + list.iter().map(|item| item.len() + 16).sum::<usize>(),
            Self::Hash(map) => {
                48 + map
                    .iter()
                    .map(|(key, value)| key.len() + value.len() + 32)
                    .sum::<usize>()
            }
            Self::Set(set) => 48 + set.iter().map(|member| member.len() + 24).sum::<usize>(),
            Self::ZSet(zset) => {
                48 + zset
                    .scores
                    .keys()
                    .map(|member| member.len() + 40)
                    .sum::<usize>()
            }
            Self::Stream(stream) => {
                64 + stream
                    .iter()
                    .map(|entry| {
                        32 + entry
                            .fields
                            .iter()
                            .map(|(field, value)| field.len() + value.len() + 16)
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            }
        }
    }

    pub fn as_string(&self) -> Option<&Vec<u8>> {
        match self {
            Self::String(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_string_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Self::String(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&HashSet<Vec<u8>>> {
        match self {
            Self::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<Vec<u8>>> {
        match self {
            Self::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&VecDeque<Vec<u8>>> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<Vec<u8>>> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn as_hash(&self) -> Option<&HashValue> {
        match self {
            Self::Hash(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashValue> {
        match self {
            Self::Hash(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_zset(&self) -> Option<&SortedSet> {
        match self {
            Self::ZSet(zset) => Some(zset),
            _ => None,
        }
    }

    pub fn as_zset_mut(&mut self) -> Option<&mut SortedSet> {
        match self {
            Self::ZSet(zset) => Some(zset),
            _ => None,
        }
    }

    pub fn as_stream(&self) -> Option<&Stream> {
        match self {
            Self::Stream(stream) => Some(stream),
            _ => None,
        }
    }

    pub fn as_stream_mut(&mut self) -> Option<&mut Stream> {
        match self {
            Self::Stream(stream) => Some(stream),
            _ => None,
        }
    }
}

fn string_encoding(bytes: &[u8]) -> &'static str {
    if is_canonical_int(bytes) {
        "int"
    } else if bytes.len() <= 44 {
        "embstr"
    } else {
        "raw"
    }
}

pub fn is_canonical_int(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let Ok(value) = text.parse::<i64>() else {
        return false;
    };
    value.to_string().as_bytes() == bytes
}

/// Format a double like C's `%.17g`, which KeyDB uses for sorted-set scores
/// in DUMP/digest output and RESP2 replies.
pub fn format_double_17g(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value == f64::INFINITY {
        return "inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }
    let exponent = value.abs().log10().floor() as i32;
    if !(-4..17).contains(&exponent) {
        let text = format!("{value:.16e}");
        if let Some(position) = text.find('e') {
            let exponent_text = text[position + 1..].to_string();
            let mut mantissa = text[..position].to_string();
            while mantissa.ends_with('0') {
                mantissa.pop();
            }
            if mantissa.ends_with('.') {
                mantissa.pop();
            }
            let exponent_value: i32 = exponent_text.parse().unwrap_or(0);
            return format!("{mantissa}e{exponent_value:+03}");
        }
        return text;
    }
    let decimals = (17 - 1 - exponent).max(0) as usize;
    let mut text = format!("{value:.decimals$}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text == "-0" {
        text = "0".to_string();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_encodings() {
        let limits = EncodingLimits::default();
        assert_eq!(Value::String(b"123".to_vec()).encoding(limits), "int");
        assert_eq!(Value::String(b"01".to_vec()).encoding(limits), "embstr");
        assert_eq!(Value::String(b"hello".to_vec()).encoding(limits), "embstr");
        assert_eq!(Value::String(vec![b'x'; 45]).encoding(limits), "raw");
    }

    #[test]
    fn set_encodings() {
        let limits = EncodingLimits::default();
        let ints = Value::Set([b"1".to_vec(), b"2".to_vec()].into_iter().collect());
        assert_eq!(ints.encoding(limits), "intset");
        let strings = Value::Set([b"one".to_vec(), b"two".to_vec()].into_iter().collect());
        assert_eq!(strings.encoding(limits), "hashtable");
    }

    #[test]
    fn format_double_17g_roundtrips() {
        let mut state = 0x1234_5678_9abc_def0u64;
        for _ in 0..200_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let value = f64::from_bits(state);
            if !value.is_finite() || value == 0.0 {
                continue;
            }
            let text = format_double_17g(value);
            let parsed = text.parse::<f64>().unwrap_or_else(|_| panic!("bad {text}"));
            assert_eq!(
                parsed.to_bits(),
                value.to_bits(),
                "{value:?} formatted as {text} parsed as {parsed:?}"
            );
        }
    }

    #[test]
    fn type_names_and_lengths() {
        assert_eq!(Value::String(b"abc".to_vec()).len(), 3);
        assert_eq!(Value::Set([b"a".to_vec()].into_iter().collect()).len(), 1);
        assert_eq!(
            Value::Set([b"a".to_vec()].into_iter().collect()).type_name(),
            "set"
        );
    }
}
