//! Snapshot database with KeyDB-style read views.
//!
//! Writes are serialized by the server's global lock. A snapshot freezes the
//! current dictionary in O(1) and installs a fresh current dictionary. Values
//! are immutable behind `Arc`, so snapshots share them until a write clones the
//! value (`Arc::make_mut`). This mirrors KeyDB's `redisDbPersistentData` and
//! `redisDbPersistentDataSnapshot` without requiring manual epoch GC.

use std::collections::HashSet;
use std::sync::Arc;

use crate::dict::Dict;
use crate::expire::ExpireEntry;

/// A value plus its expiration, LRU/LFU metadata, and MVCC timestamp.
#[derive(Debug)]
pub struct Entry<V> {
    pub value: Arc<V>,
    pub expire: ExpireEntry,
    pub lru: u32,
    pub mvcc: u64,
}

impl<V> Entry<V> {
    pub fn new(value: Arc<V>, mvcc: u64) -> Self {
        Self {
            value,
            expire: ExpireEntry::new(),
            lru: 0,
            mvcc,
        }
    }

    pub fn with_expire(value: Arc<V>, expire: ExpireEntry, mvcc: u64) -> Self {
        Self {
            value,
            expire,
            lru: 0,
            mvcc,
        }
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expire.is_expired(now_ms)
    }
}

impl<V> Clone for Entry<V> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            expire: self.expire.clone(),
            lru: self.lru,
            mvcc: self.mvcc,
        }
    }
}

/// An immutable read view over a database at a checkpoint.
#[derive(Debug)]
pub struct Snapshot<V> {
    map: Dict<Vec<u8>, Entry<V>>,
    tombstones: HashSet<Vec<u8>>,
    parent: Option<Arc<Snapshot<V>>>,
    checkpoint: u64,
}

impl<V> Snapshot<V> {
    pub fn checkpoint(&self) -> u64 {
        self.checkpoint
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry<V>> {
        if let Some(entry) = self.map.get(key) {
            return Some(entry);
        }
        if self.tombstones.contains(key) {
            return None;
        }
        self.parent.as_ref().and_then(|parent| parent.get(key))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub fn len(&self) -> usize {
        let mut keys = HashSet::new();
        self.collect_keys(&mut keys);
        keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Collect the visible contents of this snapshot. This is used by RDB
    /// serialization and tests; hot-path iteration will get a borrowed
    /// iterator once the dictionary exposes one.
    pub fn collect_visible(&self) -> Vec<(Vec<u8>, Entry<V>)> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        self.collect_visible_into(&mut out, &mut seen);
        out
    }

    fn collect_visible_into(
        &self,
        out: &mut Vec<(Vec<u8>, Entry<V>)>,
        seen: &mut HashSet<Vec<u8>>,
    ) {
        for (key, entry) in self.map.iter() {
            if seen.insert(key.clone()) {
                out.push((key.clone(), entry.clone()));
            }
        }
        if let Some(parent) = &self.parent {
            for (key, entry) in parent.collect_visible() {
                if !seen.contains(&key) && !self.tombstones.contains(&key) {
                    seen.insert(key.clone());
                    out.push((key, entry));
                }
            }
        }
    }

    fn collect_keys(&self, keys: &mut HashSet<Vec<u8>>) {
        for key in self.map.keys() {
            keys.insert(key.clone());
        }
        if let Some(parent) = &self.parent {
            let mut parent_keys = HashSet::new();
            parent.collect_keys(&mut parent_keys);
            for key in parent_keys {
                if !keys.contains(&key) && !self.tombstones.contains(&key) {
                    keys.insert(key);
                }
            }
        }
    }
}

/// A database with a current dictionary and an optional snapshot chain.
#[derive(Debug)]
pub struct Database<V> {
    current: Dict<Vec<u8>, Entry<V>>,
    tombstones: HashSet<Vec<u8>>,
    snapshot: Option<Arc<Snapshot<V>>>,
}

impl<V> Default for Database<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Database<V> {
    pub fn new() -> Self {
        Self {
            current: Dict::new(),
            tombstones: HashSet::new(),
            snapshot: None,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry<V>> {
        if let Some(entry) = self.current.get(key) {
            return Some(entry);
        }
        if self.tombstones.contains(key) {
            return None;
        }
        self.snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get(key))
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Collect all visible entries. This is primarily for RDB serialization,
    /// KEYS, and tests; a borrowed iterator will replace it once the dict
    /// exposes cursor iteration over snapshot chains.
    pub fn collect_visible(&self) -> Vec<(Vec<u8>, Entry<V>)> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for (key, entry) in self.current.iter() {
            seen.insert(key.clone());
            out.push((key.clone(), entry.clone()));
        }
        if let Some(snapshot) = &self.snapshot {
            for (key, entry) in snapshot.collect_visible() {
                if !seen.contains(&key) && !self.tombstones.contains(&key) {
                    seen.insert(key.clone());
                    out.push((key, entry));
                }
            }
        }
        out
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.collect_visible()
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    pub fn current_len(&self) -> usize {
        self.current.len()
    }

    pub fn snapshot(&self) -> Option<&Arc<Snapshot<V>>> {
        self.snapshot.as_ref()
    }

    pub fn snapshot_depth(&self) -> usize {
        let mut depth = 0;
        let mut cursor = self.snapshot.as_ref();
        while let Some(snapshot) = cursor {
            depth += 1;
            cursor = snapshot.parent.as_ref();
        }
        depth
    }

    /// Number of visible keys, accounting for snapshots and tombstones.
    pub fn len(&self) -> usize {
        let snapshot_len = self.snapshot.as_ref().map_or(0, |snapshot| snapshot.len());
        let mut total = snapshot_len;
        for key in self.current.keys() {
            let shadowed = self
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.contains_key(key));
            if !shadowed {
                total += 1;
            }
        }
        for key in &self.tombstones {
            let shadowed = self
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.contains_key(key));
            if shadowed {
                total = total.saturating_sub(1);
            }
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert or replace a value. Returns the previous value if one was
    /// visible, including one stored in the snapshot.
    pub fn insert(&mut self, key: &[u8], value: V, mvcc: u64) -> Option<Arc<V>> {
        self.insert_entry(key.to_vec(), Entry::new(Arc::new(value), mvcc))
    }

    pub fn insert_entry(&mut self, key: Vec<u8>, entry: Entry<V>) -> Option<Arc<V>> {
        let previous = self.get(&key).map(|entry| Arc::clone(&entry.value));
        self.tombstones.remove(&key);
        self.current.insert(key, entry);
        previous
    }

    /// Get a mutable entry, materializing it from the snapshot on first write.
    ///
    /// The caller must mutate the value with `Arc::make_mut`, which clones only
    /// when a snapshot still shares it.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry<V>> {
        if !self.current.contains_key(key) {
            if self.tombstones.contains(key) {
                return None;
            }
            let entry = self
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get(key))
                .cloned()?;
            self.current.insert(key.to_vec(), entry);
        }
        self.current.get_mut(key)
    }

    /// Remove a key. Returns the removed value if it was visible.
    pub fn remove(&mut self, key: &[u8]) -> Option<Arc<V>> {
        if let Some(entry) = self.current.remove(key) {
            // If the same key exists in the snapshot, deleting it from the
            // current dictionary must not resurrect the old snapshot value.
            if self
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.get(key).is_some())
            {
                self.tombstones.insert(key.to_vec());
            }
            return Some(entry.value);
        }
        if self.tombstones.contains(key) {
            return None;
        }
        let entry = self
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get(key))?;
        let value = Arc::clone(&entry.value);
        self.tombstones.insert(key.to_vec());
        Some(value)
    }

    /// Freeze the current state and install a new empty current dictionary.
    ///
    /// If an older-or-equal snapshot already satisfies `checkpoint`, its handle
    /// is reused, matching KeyDB's snapshot reuse optimization.
    pub fn create_snapshot(&mut self, checkpoint: u64) -> Arc<Snapshot<V>> {
        if let Some(existing) = &self.snapshot
            && checkpoint <= existing.checkpoint
        {
            return Arc::clone(existing);
        }
        let map = std::mem::take(&mut self.current);
        let tombstones = std::mem::take(&mut self.tombstones);
        let parent = self.snapshot.take();
        let snapshot = Arc::new(Snapshot {
            map,
            tombstones,
            parent,
            checkpoint,
        });
        self.snapshot = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Merge a snapshot back into the current database.
    ///
    /// The snapshot must be the head of the chain and must have no reader
    /// handles other than the database and the argument. A pinned snapshot is
    /// returned to the caller so it can be retried after readers release it.
    pub fn end_snapshot(&mut self, snapshot: Arc<Snapshot<V>>) -> Result<(), Arc<Snapshot<V>>> {
        let Some(head) = self.snapshot.as_ref() else {
            return Err(snapshot);
        };
        if !Arc::ptr_eq(head, &snapshot) {
            return Err(snapshot);
        }
        if Arc::strong_count(&snapshot) != 2 {
            return Err(snapshot);
        }

        let head = self.snapshot.take().expect("head exists");
        drop(snapshot);

        let mut inner = match Arc::try_unwrap(head) {
            Ok(inner) => inner,
            Err(head) => {
                self.snapshot = Some(Arc::clone(&head));
                return Err(head);
            }
        };

        // Deletes in the current map hide keys in the snapshot and, if the
        // snapshot itself shadows a parent, must propagate to that parent.
        for key in self.tombstones.drain() {
            inner.map.remove(&key);
            if inner
                .parent
                .as_ref()
                .is_some_and(|parent| parent.get(&key).is_some())
            {
                inner.tombstones.insert(key);
            }
        }

        // New or updated keys win over the snapshot and any tombstone.
        for (key, entry) in self.current.drain() {
            inner.tombstones.remove(&key);
            inner.map.insert(key, entry);
        }

        let Snapshot {
            map,
            tombstones,
            parent,
            ..
        } = inner;
        self.current = map;
        self.tombstones = tombstones;
        self.snapshot = parent;
        Ok(())
    }

    pub fn clear(&mut self) {
        self.current.clear();
        self.tombstones.clear();
        self.snapshot = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(v: &[u8]) -> Vec<u8> {
        v.to_vec()
    }

    #[test]
    fn insert_get_remove() {
        let mut db = Database::new();
        assert!(db.is_empty());
        db.insert(b"a", value(b"1"), 1);
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"1");
        assert_eq!(db.len(), 1);
        assert_eq!(db.remove(b"a").unwrap().as_slice(), b"1");
        assert!(db.get(b"a").is_none());
        assert!(db.is_empty());
    }

    #[test]
    fn snapshot_isolates_reads() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let snapshot = db.create_snapshot(10);
        db.insert(b"a", value(b"2"), 11);
        db.insert(b"b", value(b"3"), 12);

        assert_eq!(snapshot.get(b"a").unwrap().value.as_slice(), b"1");
        assert!(snapshot.get(b"b").is_none());
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"2");
        assert_eq!(db.get(b"b").unwrap().value.as_slice(), b"3");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(db.len(), 2);

        db.end_snapshot(snapshot).unwrap();
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"2");
        assert_eq!(db.get(b"b").unwrap().value.as_slice(), b"3");
        assert_eq!(db.len(), 2);
        assert_eq!(db.snapshot_depth(), 0);
    }

    #[test]
    fn delete_after_snapshot_does_not_resurrect() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let snapshot = db.create_snapshot(10);
        assert_eq!(db.remove(b"a").unwrap().as_slice(), b"1");
        assert!(db.get(b"a").is_none());
        assert_eq!(snapshot.get(b"a").unwrap().value.as_slice(), b"1");
        db.end_snapshot(snapshot).unwrap();
        assert!(db.get(b"a").is_none());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn overwrite_after_snapshot_wins() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let snapshot = db.create_snapshot(10);
        db.insert(b"a", value(b"2"), 11);
        db.end_snapshot(snapshot).unwrap();
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"2");
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn delete_then_reinsert_after_snapshot() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let snapshot = db.create_snapshot(10);
        db.remove(b"a");
        db.insert(b"a", value(b"2"), 11);
        db.end_snapshot(snapshot).unwrap();
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"2");
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn nested_snapshots() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let first = db.create_snapshot(10);
        db.insert(b"a", value(b"2"), 11);
        db.insert(b"b", value(b"b1"), 12);
        let second = db.create_snapshot(20);
        db.insert(b"a", value(b"3"), 21);
        db.remove(b"b");

        assert_eq!(first.get(b"a").unwrap().value.as_slice(), b"1");
        assert!(first.get(b"b").is_none());
        assert_eq!(second.get(b"a").unwrap().value.as_slice(), b"2");
        assert_eq!(second.get(b"b").unwrap().value.as_slice(), b"b1");
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"3");
        assert!(db.get(b"b").is_none());
        assert_eq!(db.snapshot_depth(), 2);

        assert!(db.end_snapshot(first).is_err());
        db.end_snapshot(second).unwrap();
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"3");
        assert!(db.get(b"b").is_none());
        assert_eq!(db.snapshot_depth(), 1);
    }

    #[test]
    fn pinned_snapshot_cannot_end() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let snapshot = db.create_snapshot(10);
        let pinned = Arc::clone(&snapshot);
        assert!(db.end_snapshot(snapshot).is_err());
        drop(pinned);
        // The failed end returned the handle, so the caller still owns it.
        let snapshot = db.snapshot().unwrap().clone();
        db.end_snapshot(snapshot).unwrap();
        assert_eq!(db.snapshot_depth(), 0);
    }

    #[test]
    fn visible_collection_matches_len() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        db.insert(b"b", value(b"2"), 2);
        let snapshot = db.create_snapshot(10);
        db.remove(b"a");
        db.insert(b"b", value(b"3"), 11);
        db.insert(b"c", value(b"4"), 12);
        let visible = snapshot.collect_visible();
        assert_eq!(visible.len(), 2);
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn write_through_snapshot_materializes() {
        let mut db = Database::new();
        db.insert(b"a", value(b"1"), 1);
        let snapshot = db.create_snapshot(10);
        let entry = db.get_mut(b"a").unwrap();
        let value = Arc::make_mut(&mut entry.value);
        value.push(b'!');
        assert_eq!(db.get(b"a").unwrap().value.as_slice(), b"1!");
        assert_eq!(snapshot.get(b"a").unwrap().value.as_slice(), b"1");
    }
}
