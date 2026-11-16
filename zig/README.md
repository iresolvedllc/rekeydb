# Zig substrate and tools

Zig 0.17.0 is pinned in `zig-version`.

Build the CLI directly:

```sh
./build.sh
./zig-out/bin/rekeydb-cli --port 6379 PING
```

Run its tests:

```sh
./test.sh
```

`build.zig` describes the intended package graph. On this toolchain the
`zig build` frontend currently deadlocks while preparing the configuration
cache, so local and CI builds invoke `zig build-exe` through `build.sh`. Keep
`build.zig` in sync with the source layout; it should become the canonical
entry point once the toolchain issue is resolved.

Planned tools:

- `rekeydb-cli` (current)
- `rekeydb-bench`
- `rekeydb-check-rdb`
- `rekeydb-check-aof`
- FLASH storage engine behind a narrow C ABI
