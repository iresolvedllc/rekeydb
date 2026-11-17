# Compatibility matrix

This matrix tracks the program, not a reduction of scope. "Full parity" means
every row eventually lands.

| Area | P0 | M1 | Later |
|---|---|---|---|
| RESP2 | yes | — | — |
| RESP3 | codec, attributes, `HELLO` | push/tracking | — |
| Commands | all five data types + keyspace | — | remaining 236 |
| Expiry | primary TTL | subkey expiry | active cycle parity |
| MVCC | snapshot core + `DEBUG MVCC` | save/reload readers | async end |
| Persistence | RDB v9 load/save, DUMP/RESTORE, DIGEST | AOF | AOF rewrite |
| Replication | — | — | PSYNC, active replica |
| Cluster | — | — | bus, slots, failover |
| Scripting | — | — | Lua 5.1 |
| ACL/TLS | — | — | full |
| Modules | — | — | C ABI |
| FLASH | — | — | Zig engine |
| Sentinel | — | — | port |
| Tools | Zig CLI skeleton | CLI breadth | bench/checkers |
| Performance | — | baseline | parity gates |

The generated `spec/coverage.md` is authoritative for command coverage.
