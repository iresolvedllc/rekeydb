# ADR 0001: Rust server, Zig substrate and tools

## Status

Accepted.

## Context

rekeydb must reach full KeyDB parity: RESP2/RESP3, 236 commands, RDB/AOF,
replication, active replication, cluster, Sentinel, the Redis Modules C ABI,
and FLASH storage. Splitting the per-command hot path across a Rust/Zig FFI
boundary would add allocator, ownership, and latency problems for little gain.

## Decision

- Rust owns the server: protocol, command dispatch, data model, MVCC snapshots,
  persistence, replication, cluster, ACL, and TLS.
- Zig owns bounded subsystems:
  - the FLASH storage engine behind a versioned C ABI;
  - operational tools (`rekeydb-cli`, `rekeydb-bench`,
    `rekeydb-check-rdb`, `rekeydb-check-aof`).
- Zig does not participate in per-command execution or per-key storage calls.
  The storage ABI is batch/cursor oriented.

## Consequences

- Rust and Zig can be built and tested independently.
- The Zig toolchain is pinned (0.17.0) and its surface area is kept small.
- A future storage engine can be replaced without touching command semantics.
- Tools can lag the server without blocking conformance tests; the stock
  KeyDB CLI is used as the test client during bootstrap.
