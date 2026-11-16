//! Redis-compatible `DEBUG DIGEST` implementation.

use sha1::{Digest, Sha1};

use rekeydb_core::Value;

/// Compute the dataset digest over `(db, key, value, has_expire)` entries.
///
/// The order of keys and of set members is irrelevant; list order is
/// significant. Database ids are mixed in ascending order.
pub fn dataset_digest<'a, I>(entries: I) -> String
where
    I: IntoIterator<Item = (usize, &'a [u8], &'a Value, bool)>,
{
    let mut entries: Vec<_> = entries.into_iter().collect();
    entries.sort_by_key(|(db, _, _, _)| *db);

    let mut final_digest = [0u8; 20];
    let mut current_db = None;
    for (db, key, value, has_expire) in entries {
        if current_db != Some(db) {
            current_db = Some(db);
            mix_digest(&mut final_digest, &(db as u32).to_be_bytes());
        }
        let mut key_digest = [0u8; 20];
        mix_digest(&mut key_digest, key);
        xor_object_digest(&mut key_digest, value, has_expire);
        xor_digest(&mut final_digest, &key_digest);
    }
    final_digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn xor_object_digest(digest: &mut [u8; 20], value: &Value, has_expire: bool) {
    let type_id = match value {
        Value::String(_) => 0u32,
        Value::List(_) => 1,
        Value::Set(_) => 2,
        Value::ZSet(_) => 3,
        Value::Hash(_) => 4,
    };
    mix_digest(digest, &type_id.to_be_bytes());
    match value {
        Value::String(bytes) => mix_digest(digest, bytes),
        Value::List(list) => {
            for item in list {
                mix_digest(digest, item);
            }
        }
        Value::Set(set) => {
            for member in set {
                xor_digest(digest, member);
            }
        }
        Value::Hash(map) => {
            for (field, value) in map {
                let mut element = [0u8; 20];
                mix_digest(&mut element, field);
                mix_digest(&mut element, value);
                xor_digest(digest, &element);
            }
        }
        Value::ZSet(zset) => {
            for (member, score) in &zset.scores {
                let mut element = [0u8; 20];
                mix_digest(&mut element, member);
                mix_digest(&mut element, format!("{score:.17}").as_bytes());
                xor_digest(digest, &element);
            }
        }
    }
    if has_expire {
        xor_digest(digest, b"!!expire!!");
    }
}

fn xor_digest(digest: &mut [u8; 20], data: &[u8]) {
    let hash = Sha1::digest(data);
    for (target, byte) in digest.iter_mut().zip(hash.iter()) {
        *target ^= *byte;
    }
}

fn mix_digest(digest: &mut [u8; 20], data: &[u8]) {
    xor_digest(digest, data);
    let hash = Sha1::digest(*digest);
    digest.copy_from_slice(&hash);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekeydb_core::Value;

    #[test]
    fn empty_dataset_is_zero() {
        assert_eq!(dataset_digest(std::iter::empty()), "0".repeat(40));
    }

    #[test]
    fn deterministic_for_sets() {
        let mut a = std::collections::HashSet::new();
        a.insert(b"a".to_vec());
        a.insert(b"b".to_vec());
        let value = Value::Set(a);
        let one = dataset_digest([(0, b"k".as_slice(), &value, false)]);
        let two = dataset_digest([(0, b"k".as_slice(), &value, false)]);
        assert_eq!(one, two);
        assert_eq!(one.len(), 40);
    }
}
