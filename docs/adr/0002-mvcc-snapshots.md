# ADR 0002: Global-lock writes with immutable snapshot reads

## Status

Accepted.

## Context

KeyDB is not a general multi-writer MVCC database. It serializes command
execution with a global lock and gives long-running readers an immutable
snapshot of a database. A snapshot freezes the current dictionary, installs a
fresh current dictionary, and tracks deletes in a tombstone dictionary. Readers
walk the current dictionary, tombstone set, and snapshot chain. Ending the last
reference merges changes back.

Porting this literally is necessary because the behavior is observable through
`SAVE`/`BGSAVE`, `DEBUG RELOAD`, async commands, `KEYS`/`SCAN`, and active
replication timestamps.

## Decision

- `Database<V>` holds `current`, `tombstones`, and an optional `Arc<Snapshot<V>>`.
- Entries hold `Arc<V>`, expiration metadata, LRU/LFU metadata, and an MVCC
  timestamp.
- Snapshot creation freezes `current` in O(1) and installs a new dictionary.
- Writes materialize a snapshot entry into the current dictionary and use
  `Arc::make_mut`; immutable snapshots keep their own `Arc` references, so a
  write clones the value only when it is still shared.
- Deletes add a tombstone when the key exists in the snapshot chain.
- Ending a snapshot merges current entries into the snapshot, removes
  tombstones, restores the parent, and becomes the new current dictionary.
- A snapshot can only be ended when it is the chain head and no reader handles
  remain. The caller retries after readers release their handles.
- MVCC timestamps use KeyDB's `(unix_ms << 20) | counter` layout and are stored
  from M1 onward.

## Consequences

- Rust's `Arc` removes the need for KeyDB's manual epoch GC for memory safety.
  A deferred-drop queue will handle large frees to avoid latency spikes.
- The global lock remains the scalability limit, matching KeyDB. Batching and
  async snapshot reads are the optimization path.
- The snapshot chain depth is capped at six, as in KeyDB.
