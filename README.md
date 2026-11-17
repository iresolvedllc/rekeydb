# rekeydb

A from-scratch Rust and Zig implementation of [KeyDB](https://github.com/Snapchat/KeyDB),
targeting full protocol, persistence, replication, cluster, module, and
operational parity.

- **Rust** owns the server: protocol, command execution, data model, MVCC
  snapshots, persistence, replication, cluster, ACL, and TLS.
- **Zig** owns bounded substrates: the FLASH storage engine and operational
  tools (`rekeydb-cli`, `rekeydb-bench`, `rekeydb-check-rdb`,
  `rekeydb-check-aof`).

## Status

Bootstrap / P0. Working today:

- RESP2 and RESP3 codec with inline commands, protocol limits, and Redis-style
  error strings;
- a `mio` event loop with a global state lock and batched command execution;
- an MVCC snapshot database with tombstones, nested read views, COW values,
  and KeyDB-compatible timestamps;
- strings (including bitmaps/BITFIELD and HyperLogLog), lists, hashes, sets,
  sorted sets, GEO commands, expiry, keyspace commands,
  `OBJECT ENCODING`,
  `DEBUG MVCC`, `DEBUG PROTOCOL`, `HELLO`, `CLIENT`, `CONFIG`, `INFO`, basic
  `MULTI`/`EXEC`/`WATCH`, `SORT`, and a conformance harness passing all 22
  KeyDB `unit/protocol.tcl` tests, all 176 `unit/type/list.tcl` tests,
  all 76 `unit/type/hash.tcl` tests, all 254 `unit/type/zset.tcl` tests,
  all 56 `unit/geo.tcl` tests, and all 20 `unit/hyperloglog.tcl` tests;
- RDB v9 save/load, `SAVE`/`BGSAVE`, `DEBUG RELOAD`, `DEBUG DIGEST`, and
  `DUMP`/`RESTORE`, cross-compatible with KeyDB in both directions for strings, lists, hashes,
  sets, sorted sets, and expirations;
- append-only persistence with `appendonly`, `appendfsync`, `BGREWRITEAOF`,
  startup replay, truncation handling, and `DEBUG LOADAOF`;
- generated command/config/test manifests and a Zig `rekeydb-cli`.

The reference KeyDB tree lives in `KeyDB/` (development snapshot) and
`reference/keydb/` (upstream v6.3.4, fetched on demand). Both are read-only
inputs for the specification extractor and conformance oracle.

## Layout

```
crates/rekeydb-protocol   RESP2/RESP3 codec
crates/rekeydb-core       dict, values, MVCC snapshots, expiry
crates/rekeydb-persist    RDB v9, DUMP/RESTORE, dataset digests
crates/rekeydb-server     server binary
xtask                     spec extraction and developer tasks
spec/                     generated command/config/test manifests
zig/                      Zig storage engine and tools
tests/                    conformance harness and corpora
docs/adr/                 architecture decision records
```

## Build

```sh
make build
make test
make check
```

The pinned toolchain is in `rust-toolchain.toml`. Zig 0.17.1 is pinned in
`zig/zig-version`. Build the Zig tools with:

```sh
./zig/build.sh
```

## Specification extraction

The command and configuration surface is generated from the reference KeyDB
source, so the port cannot silently drift:

```sh
make spec
cargo run -p xtask -- check   # validate without the reference tree
```

This writes `spec/commands.json`, `spec/configs.json`, and
`spec/coverage.md`.

## License

BSD-3-Clause. See `COPYING` and `NOTICE`.
