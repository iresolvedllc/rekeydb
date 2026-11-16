# Conformance status

## Bootstrap results

The adapted harness runs KeyDB's `unit/protocol.tcl` (22 tests). rekeydb
passes all 22, including the blocking `BLPOP` pipelining regression.

The list suite (`unit/type/list.tcl`) reaches 80 passing tests before it needs
`DEBUG RELOAD` (RDB support). Lists, blocking pops/moves, `MULTI`/`EXEC`,
basic `WATCH`, `RENAME`, and `SORT ... STORE` are implemented.

The reference binary passes all 22 protocol tests.

## Running

```sh
# On NixOS, make Tcl available first:
nix-shell -p tcl --run './tests/conformance/run.sh --single unit/protocol --clients 1'

# With Tcl installed system-wide:
./tests/conformance/run.sh --single unit/protocol --clients 1

# Validate the harness against the reference binary:
REKEYDB_BINARY="$PWD/KeyDB/src/keydb-server" \
    ./tests/conformance/run.sh --single unit/protocol --clients 1
```

The script copies the upstream test tree to `tests/tmp/conformance`, points
`src/keydb-server` at the requested binary, and uses the stock KeyDB CLI when
it has been built.

## Differential checks

`tests/diff/diff.py` currently matches KeyDB on:

- 27 string/keyspace commands;
- 14 error/edge-case replies;
- 29 set commands including encodings, wrong-type errors, stores, and
  single-member random operations;
- 32 list/transaction commands including blocking wakeups, moves, `MULTI`,
  `EXEC`, `RENAME`, and `SORT ... STORE`.

## Oracle build

`scripts/build-oracle.sh` builds `KeyDB/` with `MALLOC=libc NO_MOTD=yes`. On
NixOS it supplies `libuuid`, OpenSSL, and zlib through `nix-shell`; on Debian
and Ubuntu install `uuid-dev`, `libssl-dev`, and `zlib1g-dev`.

## Next

- RDB v9 load/save and `DEBUG RELOAD`/`DEBUG DIGEST`.
- Hashes and sorted sets with Redis 6.2 encodings.
- Remaining list-suite gaps after reload support.
