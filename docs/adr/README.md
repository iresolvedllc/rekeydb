# Architecture decision records

| ADR | Decision |
|---|---|
| [0001](0001-language-split.md) | Rust server, Zig substrate and tools |
| [0002](0002-mvcc-snapshots.md) | Global-lock writes with immutable snapshot reads |
| [0003](0003-threading.md) | Thread-per-event-loop, batched global lock ownership |
| [0004](0004-compatibility-target.md) | Full KeyDB parity against a pinned source tree |
| [0005](0005-zig-storage-abi.md) | Zig storage behind a narrow versioned C ABI |
| [0006](0006-rdb.md) | RDB v9 and DUMP payload compatibility |
