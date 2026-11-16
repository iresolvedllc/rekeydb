# ADR 0005: Zig storage behind a narrow versioned C ABI

## Status

Proposed; implementation begins in P6.

## Context

KeyDB's FLASH storage interface (`IStorage`, `IStorageFactory`, `StorageCache`)
stores serialized objects and expiration metadata. The boundary is already
coarse: insert/get/erase, enumerate, count, expiration/eviction candidates, and
write batches.

## Decision

- Rust defines the storage trait, cache policy (`writethrough`/`writeback`),
  serialization, eviction, and expiration integration.
- Zig implements the durable engine behind an opaque C handle.
- ABI rules:
  - only C-compatible types and explicit lengths;
  - caller-owned or explicitly freed buffers, never a Zig allocator crossing
    the boundary;
  - batch and cursor operations instead of per-key callbacks;
  - version handshake and error codes on open;
  - no Redis object semantics in Zig; values are opaque versioned blobs.
- File-format compatibility with RocksDB is not required; behavior and
  configuration compatibility are.

## Consequences

- The storage engine can be tested with a Rust in-memory backend.
- Zig toolchain churn is isolated to one crate and one header.
- Crash consistency and compaction tests become a dedicated gate.
