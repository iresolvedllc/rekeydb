# Compatibility matrix

This matrix tracks the program, not a reduction of scope. "Full parity" means
every row eventually lands.

| Area | P0 | M1 | Later |
|---|---|---|---|
| RESP2 | yes | — | — |
| RESP3 | codec, attributes, `HELLO` | push/tracking | — |
| Commands | all data types + keyspace, GEO, HLL, streams, Pub/Sub | **210/236** | remaining 26 |
| Expiry | primary TTL | subkey expiry | active cycle parity |
| MVCC | snapshot core + `DEBUG MVCC` | save/reload readers | async end |
| Persistence | RDB v9, AOF, DUMP/RESTORE, DIGEST | AOF preamble | AOF edge cases |
| Replication | full sync + live command stream + ROLE/INFO + handshake timeouts + multi-replica digests | replica ACKs, blocking command propagation, diskless-sync delay | backlog/partial resync, diskless-load modes, blocking WAIT, RDB sync payload |
| Cluster | — | — | bus, slots, failover |
| Scripting | — | — | Lua 5.1 |
| ACL/TLS | — | — | full |
| Modules | — | — | C ABI |
| FLASH | — | — | Zig engine |
| Sentinel | — | — | port |
| Tools | Zig CLI skeleton | CLI breadth | bench/checkers |
| Performance | — | baseline | parity gates |

The generated `spec/coverage.md` is authoritative for command coverage.
`docs/status.md` tracks completed work, current conformance results, and the
remaining gaps.
