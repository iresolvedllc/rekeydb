# rekeydb engineering status

_Last updated: 2026-11-17_

rekeydb is a staged Rust + Zig port of KeyDB. Rust owns the server, data
model, command execution, persistence, replication, and the command hot path.
Zig is reserved for the bounded FLASH storage engine and operational tools.
The goal remains full KeyDB parity; this document records what is complete,
what is partial, and what is still missing.

## Architecture

- `crates/rekeydb-protocol`: RESP2/RESP3 codec and command framing.
- `crates/rekeydb-core`: MVCC database, snapshot chain, custom dictionary,
  value types (strings, lists, hashes, sets, sorted sets, streams), expiry.
- `crates/rekeydb-persist`: RDB v9, AOF load/rewrite, DUMP/RESTORE, CRC64,
  ziplist support, dataset digests.
- `crates/rekeydb-server`: command dispatch and handlers, event loop,
  blocking clients, AOF/replication feed, replica thread, config surface.
- `xtask`: extracts command/config/test manifests from KeyDB and regenerates
  `spec/coverage.md`.
- `zig/`: CLI and the future FLASH engine boundary.

Writes execute under the global state lock, which also orders MVCC clock
ticks, AOF emission, and replica propagation. Values are reference-counted
and copied on write for snapshots.

## Completed work

### Core data types and commands

| Area | Status |
|---|---|---|
| Strings | complete, including `SET` options, `GETEX`/`GETDEL`, ranges, `INCRBYFLOAT`, `STRALGO LCS` |
| Bitmaps | complete: `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP` (including KeyDB shift modes), `BITFIELD`, `BITFIELD_RO` |
| Lists | complete, including blocking pops/moves and `MULTI`/`EXEC` wakeups |
| Hashes | complete, including `KEYDB.HRENAME`, `HRANDFIELD`, `HSCAN` |
| Sets | complete, including stores and random members |
| Sorted sets | complete, including range/lex/score queries, set operations, blocking pops, RESP3 replies |
| GEO | complete (`GEOADD`, `GEOPOS`, `GEODIST`, `GEOHASH`, radius/box searches, stores) |
| HyperLogLog | complete byte-compatible sparse/dense implementation with `PFDEBUG` and corruption detection |
| Streams | complete command surface, consumer groups, blocking reads, AOF rewrite |
| Expiry | primary TTL plus subkey expiry primitives |
| Keyspace | `DEL`/`UNLINK`, `EXISTS`, `TYPE`, `KEYS`, `SCAN`, `RANDOMKEY`, `RENAME`, `MOVE`, `COPY`, `SWAPDB`, `SORT`, `OBJECT`, `DUMP`/`RESTORE` |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`/`UNWATCH` |
| Admin/connection | `HELLO`, `AUTH`, `CLIENT`, `CONFIG`, `INFO`, `COMMAND`, `TIME`, `LASTSAVE`, `DEBUG` subsets, `SAVE`/`BGSAVE`, `BGREWRITEAOF` |

### Persistence

- RDB v9 save/load and `DEBUG RELOAD` for strings, lists, hashes, sets,
  sorted sets, expirations, and compact encodings.
- DUMP/RESTORE cross-compatible with KeyDB for those five types.
- AOF startup replay, `appendfsync`, truncation handling, `DEBUG LOADAOF`,
  and command-based rewrite for all implemented types including streams and
  consumer groups.
- `DEBUG DIGEST`/`DIGEST-VALUE`; values match KeyDB for the five RDB-backed
  types and for streams.
- AOF/replication propagation now shares one path: command rewrites,
  `Outcome.aof_commands`, database `SELECT` prefixing, and blocked-command
  propagation are consistent.

### Replication (core)

Implemented:

- `SLAVEOF` / `REPLICAOF` and `SLAVEOF NO ONE` promotion.
- `PSYNC` full resynchronization (`+FULLRESYNC`) and legacy `SYNC`.
- Full-sync payload transfer, replica link state, reconnection loop, and
  timeout handling for handshakes.
- Live command propagation from master to all connected replicas, including
  database `SELECT`, command rewriting (`GETDEL`→`DEL`, `GETSET`→`SET`,
  blocking list commands, streams, and other AOF rewrites).
- Replica command application, read-only enforcement, replica offset
  tracking, periodic `REPLCONF ACK`, `REPLCONF GETACK`, `WAIT` reporting,
  `ROLE`, and `INFO replication` fields.
- Blocked list/zset/stream operations propagate their effective commands.
- AOF and replication can run simultaneously.

The full-sync payload currently uses an AOF-style command stream, not an RDB
payload. This lets rekeydb replicate streams and all implemented types to
other rekeydb instances, but a KeyDB replica cannot consume rekeydb's
full-sync payload. A rekeydb replica can consume a KeyDB RDB payload for the
five RDB-backed types.

Partial/incomplete:

- No replication backlog or partial resynchronization (`+CONTINUE`); every
  `PSYNC` performs a full resync.
- No diskless-sync delay/load modes, periodic master `PING`/`GETACK`, chained
  replication, or replication of command statistics.
- `WAIT` returns the current acknowledged replica count immediately; it does
  not block for the requested timeout.
- `integration/replication.tcl`: 23 tests pass, then the suite reaches an
  `EVAL` test and stops because scripting is not implemented. Before that
  point, two handshake tests and the four concurrent multi-replica digest
  tests still fail; the latter is under investigation after the
  `format_double_17g` fix (a controlled three-replica reproduction now
  matches, so the remaining issue is specific to the upstream test flow).

## Conformance status

All suites below were run with the KeyDB Tcl harness, `--clients 1`.

| Suite | Result |
|---|---:|
| `unit/protocol.tcl` | 22/22 |
| `unit/type/list.tcl` | 176/176 |
| `unit/type/hash.tcl` | 76/76 |
| `unit/type/zset.tcl` | 254/254 |
| `unit/bitops.tcl` | 53/53 |
| `unit/bitfield.tcl` | 16/16 |
| `unit/type/incr.tcl` | 24/24 |
| `unit/type/string.tcl` | 72/72 |
| `unit/geo.tcl` | 56/56 |
| `unit/hyperloglog.tcl` | 20/20 |
| `unit/aofrw.tcl` | 19/19 |
| `integration/aof.tcl` | 25/25 |
| `unit/keyspace.tcl` | 51/51 |
| `unit/type/stream.tcl` | 56/56 |
| `unit/type/stream-cgroups.tcl` | 35/35 |
| `integration/replication.tcl` | 23 pass, then blocked by missing scripting; 6 known failures |

Command coverage: **204 / 236**.

## Engineering fixes landed in this cycle

- `format_double_17g` no longer strips zeros from integer-formatted doubles.
  The previous behavior turned values such as `4.69e16` into `469...042`,
  corrupting RDB/AOF/digest output and causing replicas to diverge after
  full sync. A 200,000-value random round-trip test now guards this.
- Blocking `BRPOPLPUSH`/`BLMOVE` propagation drops the timeout argument and
  emits valid `RPOPLPUSH`/`LMOVE` commands.
- Served blocking list/zset operations now propagate to AOF and replicas.
- `SETRANGE` validates negative and oversized offsets, preserves existing
  keys on empty writes, and marks mutated integer strings as `raw`.
- `SET ... NX GET` is rejected as a syntax error.
- `STRALGO LCS` implemented with `LEN`, `IDX`, `WITHMATCHLEN`,
  `MINMATCHLEN`, `STRINGS`, and `KEYS`.
- `INFO commandstats` and `CONFIG RESETSTAT` track per-command calls;
  replica-applied commands are counted.
- Startup config directives are seeded into the runtime `CONFIG` view, so
  per-test overrides such as `stream-node-max-entries` apply.
- `LFENCE` implemented as an acknowledgement hook for the test harness.

## Needs work

### Highest priority

1. **Replication hardening**
   - Identify and fix the remaining concurrent multi-replica digest failures
     in `integration/replication.tcl`.
   - Implement a replication backlog and `PSYNC +CONTINUE` partial resync.
   - Implement diskless-sync delay/load modes and handshake timeout semantics
     required by the two failing handshake tests.
   - Make `WAIT` block up to its timeout and use replica acknowledgements.
   - Use an RDB full-sync payload, ideally with KeyDB-compatible stream
     listpacks, so KeyDB replicas can attach to rekeydb masters.

2. **Scripting** (`EVAL`, `EVALSHA`, `SCRIPT`): required to continue
   `integration/replication.tcl` and for KeyDB scripting parity.

3. **Stream RDB/DUMP payloads** using listpack encoding for byte-level
   cross-compatibility. `DEBUG RELOAD` currently cannot serialize streams.

### Remaining command surface

`module`, `expdel`, `keydb.mexists`, `replping`, `monitor`, pub/sub commands,
`cluster`, `restore-asking`, `migrate`, `asking`, `readonly`/`readwrite`,
`memory`, `eval`/`evalsha`, `script`, `slowlog`, `latency`, `acl`,
`rreplay`, `keydb.cron`, `keydb.nhget`, `keydb.nhset`,
`KEYDB.MVCCRESTORE`, and `failover` are not implemented.

### Later milestones

- Cluster bus, slot migration, failover, and replica orchestration.
- Modules and the Zig FLASH storage engine.
- Sentinel.
- ACL, TLS, and connection security parity.
- Compact RDB output (quicklist/ziplist/intset/hash-ziplist/zset-ziplist).
- AOF RDB preamble support and automatic rewrite thresholds.
- RDB stream listpacks and `DUMP` compatibility for streams.
- Performance work: benchmark gates, multiplexing/server threads, async MVCC
  lifecycle, and Zig storage integration.

## Reproducing

```sh
cargo build -p rekeydb-server --bin rekeydb
./tests/conformance/run.sh --single unit/type/string --clients 1
./tests/conformance/run.sh --single unit/keyspace --clients 1
./tests/conformance/run.sh --single integration/replication --clients 1
make check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The KeyDB tree in `KeyDB/` is the development oracle. `scripts/build-oracle.sh`
builds it with `MALLOC=libc NO_MOTD=yes`.
