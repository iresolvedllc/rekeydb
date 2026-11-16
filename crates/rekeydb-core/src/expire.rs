//! KeyDB-style expiration metadata.
//!
//! KeyDB stores expiration on the value object, not in a separate expires
//! dictionary, and supports expiring individual subkeys of a nested hash.
//! M1 only needs primary expirations, but the representation and commands are
//! designed for subkey expirations from the start.

/// One expiration target: either the key itself (`subkey == None`) or a
/// subkey inside a nested hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubExpire {
    pub subkey: Option<Vec<u8>>,
    pub when_ms: i64,
}

/// Expiration metadata attached to an entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpireEntry {
    entries: Vec<SubExpire>,
}

impl ExpireEntry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SubExpire> {
        self.entries.iter()
    }

    pub fn primary(&self) -> Option<i64> {
        self.entries
            .iter()
            .find(|entry| entry.subkey.is_none())
            .map(|entry| entry.when_ms)
    }

    pub fn subkey(&self, subkey: &[u8]) -> Option<i64> {
        self.entries
            .iter()
            .find(|entry| entry.subkey.as_deref() == Some(subkey))
            .map(|entry| entry.when_ms)
    }

    pub fn set_primary(&mut self, when_ms: i64) {
        self.set(None, when_ms);
    }

    pub fn set_subkey(&mut self, subkey: &[u8], when_ms: i64) {
        self.set(Some(subkey.to_vec()), when_ms);
    }

    fn set(&mut self, subkey: Option<Vec<u8>>, when_ms: i64) {
        if let Some(existing) = self.entries.iter_mut().find(|entry| entry.subkey == subkey) {
            existing.when_ms = when_ms;
        } else {
            self.entries.push(SubExpire { subkey, when_ms });
        }
        self.entries.sort_by_key(|entry| entry.when_ms);
    }

    pub fn remove_primary(&mut self) -> bool {
        self.remove(None)
    }

    pub fn remove_subkey(&mut self, subkey: &[u8]) -> bool {
        self.remove(Some(subkey))
    }

    fn remove(&mut self, subkey: Option<&[u8]>) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.subkey.as_deref() != subkey);
        self.entries.len() != before
    }

    /// True when the key itself is expired. Subkey expirations do not make
    /// the whole key disappear.
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.primary().is_some_and(|when| when <= now_ms)
    }

    pub fn is_subkey_expired(&self, subkey: &[u8], now_ms: i64) -> bool {
        self.subkey(subkey).is_some_and(|when| when <= now_ms)
    }

    /// Earliest expiration in this entry, including subkeys.
    pub fn next_expire(&self) -> Option<i64> {
        self.entries.first().map(|entry| entry.when_ms)
    }

    /// Merge another entry into this one. Used when materializing a value from
    /// a snapshot or loading RDB data.
    pub fn merge(&mut self, other: &ExpireEntry) {
        for entry in &other.entries {
            self.set(entry.subkey.clone(), entry.when_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_expire_roundtrip() {
        let mut expire = ExpireEntry::new();
        assert!(!expire.is_expired(100));
        expire.set_primary(200);
        assert_eq!(expire.primary(), Some(200));
        assert!(!expire.is_expired(199));
        assert!(expire.is_expired(200));
        assert!(expire.remove_primary());
        assert_eq!(expire.primary(), None);
        assert!(!expire.is_expired(300));
    }

    #[test]
    fn subkey_expires_are_independent() {
        let mut expire = ExpireEntry::new();
        expire.set_primary(500);
        expire.set_subkey(b"field", 100);
        assert_eq!(expire.subkey(b"field"), Some(100));
        assert!(expire.is_subkey_expired(b"field", 100));
        assert!(!expire.is_expired(100));
        assert!(expire.is_expired(500));
        assert_eq!(expire.next_expire(), Some(100));
    }

    #[test]
    fn updates_do_not_duplicate() {
        let mut expire = ExpireEntry::new();
        expire.set_primary(10);
        expire.set_primary(20);
        assert_eq!(expire.len(), 1);
        assert_eq!(expire.primary(), Some(20));
    }
}
