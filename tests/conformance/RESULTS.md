# Conformance status

## Bootstrap results

The adapted harness runs KeyDB's `unit/protocol.tcl` (22 tests). The reference
binary passes all 22. rekeydb runs 16 before the suite stops and passes 13:

- passing: empty queries, negative multibulk lengths, out-of-range lengths,
  wrong payload headers, inline quote/escape handling, and all three
  protocol-desync tests;
- failing: three raw-response tests that need `SADD`/`SRANDMEMBER`;
- stopping: `DEBUG PROTOCOL` (RESP3 attributes) is not implemented, so the
  remaining six tests do not execute yet.

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

## Oracle build

`scripts/build-oracle.sh` builds `KeyDB/` with `MALLOC=libc NO_MOTD=yes`. On
NixOS it supplies `libuuid`, OpenSSL, and zlib through `nix-shell`; on Debian
and Ubuntu install `uuid-dev`, `libssl-dev`, and `zlib1g-dev`.

## Next

- Add sets to pass the raw-response tests; add `DEBUG PROTOCOL` for RESP3
  attributes.
- Add the five M1 data types, then enable `unit/type/string.tcl`,
  `incr.tcl`, `list.tcl`, `hash.tcl`, `set.tcl`, and `zset.tcl`.
- Add `DEBUG DIGEST` and object-address parity for state comparison.
