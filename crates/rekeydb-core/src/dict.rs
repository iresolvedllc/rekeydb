//! A chained hash table with Redis-style iteration and scanning semantics.
//!
//! This is intentionally not `std::collections::HashMap`: KeyDB's `dict.c`
//! behavior is part of the compatibility surface for `SCAN`, random sampling,
//! eviction, and snapshot freezing. The first implementation keeps one table
//! and can be extended with incremental rehashing without changing callers.

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

/// Minimum number of buckets.
const MIN_BUCKETS: usize = 8;

/// A chained hash table.
#[derive(Debug)]
pub struct Dict<K, V> {
    buckets: Vec<Vec<(K, V)>>,
    len: usize,
    hasher: RandomState,
}

impl<K, V> Default for Dict<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Dict<K, V> {
    pub fn new() -> Self {
        Self {
            buckets: (0..MIN_BUCKETS).map(|_| Vec::new()).collect(),
            len: 0,
            hasher: RandomState::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let buckets = capacity.max(MIN_BUCKETS).next_power_of_two();
        Self {
            buckets: (0..buckets).map(|_| Vec::new()).collect(),
            len: 0,
            hasher: RandomState::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
        self.len = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.iter().map(|(k, v)| (k, v)))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.len = 0;
        self.buckets
            .iter_mut()
            .flat_map(|bucket| std::mem::take(bucket).into_iter())
    }

    /// Remove all entries and return them. This is used when merging a
    /// snapshot back into the current database.
    pub fn into_entries(mut self) -> Vec<(K, V)> {
        self.len = 0;
        self.buckets
            .into_iter()
            .flat_map(|bucket| bucket.into_iter())
            .collect()
    }
}

impl<K: Hash + Eq, V> Dict<K, V> {
    pub fn reserve(&mut self, additional: usize) {
        if self.len + additional > self.buckets.len() {
            let wanted = (self.len + additional).next_power_of_two().max(MIN_BUCKETS);
            if wanted > self.buckets.len() {
                self.resize(wanted);
            }
        }
    }

    fn bucket_index<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        self.hasher.hash_one(key) as usize & (self.buckets.len() - 1)
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.buckets[self.bucket_index(key)]
            .iter()
            .find_map(|(k, v)| (k.borrow() == key).then_some(v))
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self.bucket_index(key);
        self.buckets[index]
            .iter_mut()
            .find_map(|(k, v)| (Borrow::<Q>::borrow(&*k) == key).then_some(v))
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Insert or replace a value, returning the previous value.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if (self.len + 1) > self.buckets.len() * 2 {
            self.resize(self.buckets.len() * 2);
        }
        let index = self.bucket_index(&key);
        for (k, v) in &mut self.buckets[index] {
            if *k == key {
                return Some(std::mem::replace(v, value));
            }
        }
        self.buckets[index].push((key, value));
        self.len += 1;
        None
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self.bucket_index(key);
        let bucket = &mut self.buckets[index];
        let position = bucket.iter().position(|(k, _)| k.borrow() == key)?;
        let (_, value) = bucket.swap_remove(position);
        self.len -= 1;
        Some(value)
    }

    /// Return a stable bucket cursor and at least `count` key references when
    /// available. A bucket is never split across calls, so a single call may
    /// return more than `count` keys.
    ///
    /// The cursor is a bucket index. Rehashing can move entries between calls,
    /// so callers that require Redis' full `SCAN` guarantee should ensure the
    /// dictionary is not rehashed while a scan is in progress. The incremental
    /// rehash implementation will preserve this API.
    pub fn scan(&self, cursor: usize, count: usize) -> (usize, Vec<&K>) {
        let mut keys = Vec::with_capacity(count);
        let mut index = cursor.min(self.buckets.len());
        while index < self.buckets.len() {
            for (key, _) in &self.buckets[index] {
                keys.push(key);
            }
            index += 1;
            if keys.len() >= count {
                break;
            }
        }
        let next = if index >= self.buckets.len() {
            0
        } else {
            index
        };
        (next, keys)
    }

    pub fn random_key(&self) -> Option<&K> {
        if self.len == 0 {
            return None;
        }
        // Deterministic fallback for now; the server only needs a key that
        // exists. Eviction sampling will add a seeded RNG.
        self.iter().map(|(k, _)| k).nth(self.len / 2)
    }

    fn resize(&mut self, new_len: usize) {
        debug_assert!(new_len.is_power_of_two());
        let old = std::mem::replace(
            &mut self.buckets,
            (0..new_len).map(|_| Vec::new()).collect(),
        );
        for bucket in old {
            for (key, value) in bucket {
                let index = self.hasher.hash_one(&key) as usize & (new_len - 1);
                self.buckets[index].push((key, value));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let mut dict = Dict::new();
        assert_eq!(dict.insert(b"a".to_vec(), 1), None);
        assert_eq!(dict.insert(b"b".to_vec(), 2), None);
        assert_eq!(dict.get(&b"a".to_vec()), Some(&1));
        assert_eq!(dict.insert(b"a".to_vec(), 3), Some(1));
        assert_eq!(dict.get(&b"a".to_vec()), Some(&3));
        assert_eq!(dict.len(), 2);
        assert_eq!(dict.remove(&b"a".to_vec()), Some(3));
        assert_eq!(dict.get(&b"a".to_vec()), None);
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn grows_and_keeps_all_entries() {
        let mut dict = Dict::new();
        for i in 0..10_000u32 {
            dict.insert(i.to_le_bytes(), i);
        }
        assert_eq!(dict.len(), 10_000);
        for i in 0..10_000u32 {
            assert_eq!(dict.get(&i.to_le_bytes()), Some(&i));
        }
    }

    #[test]
    fn scan_visits_every_key_once() {
        let mut dict = Dict::new();
        for i in 0..1_000u32 {
            dict.insert(i.to_le_bytes(), i);
        }
        let mut seen = std::collections::HashSet::new();
        let mut cursor = 0;
        loop {
            let (next, keys) = dict.scan(cursor, 17);
            for key in keys {
                assert!(seen.insert(*key));
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 1_000);
    }
}
