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
- strings, sets, expiry, keyspace commands, `OBJECT ENCODING`, `DEBUG MVCC`,
  `DEBUG PROTOCOL`, `HELLO`, `CLIENT`, `CONFIG`, `INFO`, and a conformance
  harness passing 21 of 22 KeyDB `unit/protocol.tcl` tests;
- generated command/config/test manifests and a Zig `rekeydb-cli`.

The reference KeyDB tree lives in `KeyDB/` (development snapshot) and
`reference/keydb/` (upstream v6.3.4, fetched on demand). Both are read-only
inputs for the specification extractor and conformance oracle.

## Layout

```
crates/rekeydb-protocol   RESP2/RESP3 codec
crates/rekeydb-core       dict, values, MVCC snapshots, expiry
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

The pinned toolchain is in `rust-toolchain.toml`. Zig 0.17.0 is pinned in
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
