# Conformance status

## Bootstrap results

`unit/protocol.tcl` runs against rekeydb through the adapted harness. At P0:

- empty queries, negative multibulk lengths, invalid lengths, wrong payload
  headers, inline quote handling, and all three protocol-desync tests pass;
- the remaining failures require set commands (`SADD`, `SRANDMEMBER`) and
  `DEBUG PROTOCOL` raw-response helpers, which belong to M1/M2.

## Running

```sh
# On NixOS, make Tcl available first:
nix-shell -p tcl --run './tests/conformance/run.sh --single unit/protocol --clients 1'

# With Tcl installed system-wide:
./tests/conformance/run.sh --single unit/protocol --clients 1
```

The script copies the upstream test tree to `tests/tmp/conformance`, points
`src/keydb-server` at `target/debug/rekeydb`, and uses the stock KeyDB CLI when
it has been built.

## Next

- Add the five M1 data types, then enable `unit/type/string.tcl`,
  `incr.tcl`, `list.tcl`, `hash.tcl`, `set.tcl`, and `zset.tcl`.
- Add `DEBUG DIGEST` and `DEBUG OBJECT` parity for state comparison.
- Replace the bootstrap test-client shim with the stock `keydb-cli` once the
  oracle build is available.
