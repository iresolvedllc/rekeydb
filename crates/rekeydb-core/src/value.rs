//! Redis/KeyDB value types and encoding rules.
//!
//! M1 starts with strings and sets. Lists, hashes, and sorted sets use the
//! same enum so command handlers can be added without another database
//! refactor. Compact encodings (intset, ziplist, quicklist, skiplist) will be
//! added behind these variants; the observable `OBJECT ENCODING` contract is
//! defined here from the start.

use std::collections::{HashMap, HashSet, VecDeque};

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
#[derive(Debug, Clone, Default)]
pub struct SortedSet {
    pub scores: HashMap<Vec<u8>, f64>,
}

#[derive(Debug, Clone)]
pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    ZSet(SortedSet),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::List(_) => "list",
            Self::Hash(_) => "hash",
            Self::Set(_) => "set",
            Self::ZSet(_) => "zset",
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
    fn type_names_and_lengths() {
        assert_eq!(Value::String(b"abc".to_vec()).len(), 3);
        assert_eq!(Value::Set([b"a".to_vec()].into_iter().collect()).len(), 1);
        assert_eq!(
            Value::Set([b"a".to_vec()].into_iter().collect()).type_name(),
            "set"
        );
    }
}
