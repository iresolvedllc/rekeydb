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

Bootstrap / P0. The server can currently speak RESP2/RESP3, run a minimal
event loop, and exercise the snapshot database with `SET`/`GET`/`DEL`.

The reference KeyDB tree lives in `KeyDB/` during bootstrap. It is read-only
input for the specification extractor and conformance oracle.

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
cargo build
cargo test
```

The pinned toolchain is in `rust-toolchain.toml`. Zig 0.17.0 is pinned in
`zig/zig-version`. Build the Zig tools with:

```sh
cd zig && zig build
```

## Specification extraction

The command and configuration surface is generated from the reference KeyDB
source, so the port cannot silently drift:

```sh
cargo run -p xtask -- spec
```

This writes `spec/commands.json`, `spec/configs.json`, and
`spec/coverage.md`.

## License

BSD-3-Clause. See `COPYING` and `NOTICE`.
