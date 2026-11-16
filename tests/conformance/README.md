# Conformance harness

The target is KeyDB's Tcl suite. It is a black-box RESP test suite and is the
primary compatibility oracle.

Bootstrap plan:

1. Install Tcl 8.6 (`tclsh`).
2. Build the reference server with `scripts/build-oracle.sh`.
3. Adapt `KeyDB/tests/support/server.tcl` to launch `target/debug/rekeydb`
   instead of `KeyDB/src/keydb-server`.
4. Use the stock `keydb-cli` as the test client until `rekeydb-cli` supports
   the required flags. This keeps server conformance independent of Zig tool
   progress.
5. Run test files in this order:
   - `unit/protocol.tcl`
   - `unit/type/string.tcl`, `incr.tcl`
   - `unit/keyspace.tcl`, `expire.tcl`, `scan.tcl`, `dump.tcl`
   - the remaining type files
   - integration, cluster, sentinel, and module suites

Every skipped test must have a reason and a tracking entry. The generated
coverage report is the source of truth for commands, not an ad-hoc allowlist.

Until Tcl is installed, `tests/diff` provides a small differential runner that
compares two live RESP servers.
