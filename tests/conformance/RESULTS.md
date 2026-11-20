# Conformance status

## Bootstrap results

The adapted harness runs KeyDB's `unit/protocol.tcl` (22 tests). rekeydb
passes all 22, including the blocking `BLPOP` pipelining regression.

The list suite (`unit/type/list.tcl`) passes all **176** tests, including
`DEBUG RELOAD`, `DUMP`/`RESTORE`, blocking pops/moves, `MULTI`/`EXEC`, basic
`WATCH`, `RENAME`, and `SORT ... STORE`.

The hash suite (`unit/type/hash.tcl`) passes all **76** tests, including
ziplist/hashtable promotion, `HRANDFIELD`, `HSCAN`, `HINCRBYFLOAT`, encoding
edge cases, `DUMP`/`RESTORE`, and `KEYDB.HRENAME`.

The sorted-set suite (`unit/type/zset.tcl`) passes all **254** tests, including
ziplist/skiplist encodings, range/lex/score queries, set-operation stores,
blocking `BZPOPMIN`/`BZPOPMAX`, `ZRANDMEMBER`, `ZSCAN`, RDB reload, and RESP3
reply shapes.

The stream suite (`unit/type/stream.tcl`) passes all **56** tests, including
ID generation edge cases, exact/approximate `MAXLEN`/`MINID` trimming with
node-aware `LIMIT`, blocking `XREAD`, `XINFO`, `XSETID`, and AOF round-trips
for empty streams and deleted last entries.

The consumer-group suite (`unit/type/stream-cgroups.tcl`) passes all **35**
tests, including `XGROUP`, `XREADGROUP` history and new-message reads,
`XPENDING`, `XACK`, `XCLAIM`, `XAUTOCLAIM`, `XINFO FULL`, blocking group
reads, AOF rewrite of groups/consumers, and consumer-group state propagation
to a replica.

The string suite (`unit/type/string.tcl`) passes all **72** tests, including
`STRALGO LCS`, extended `SET` options, `SETRANGE` edge cases, and command
propagation to attached replication streams.

The bitfield suite (`unit/bitfield.tcl`) passes all **16** tests, including
blocking replication of bitfield writes and bitfield-overflow fuzzing.

The keyspace suite (`unit/keyspace.tcl`) passes all **51** tests, including
stream `COPY` and `DEBUG DIGEST-VALUE`.

Replication core is exercised by the suites above and by a focused
`integration/replication.tcl` run: **29 tests pass**, covering full sync,
live propagation, blocking list replication, `GETSET`/`GETDEL` rewriting,
`ROLE`, `FLUSHALL`, both handshake timeout tests, and all four concurrent
multi-replica digest tests. The suite then reaches an `EVAL` test and stops
because scripting is not implemented.

The multi-replica digest failures were traced to the test harness, not the
server: the upstream helpers stop background load with `/bin/kill`, which
does not exist on NixOS. `tests/conformance/run.sh` now rewrites that call to
the PATH-resolved `kill` in its private test copy, so load stops before
digests are compared.

The pub/sub suite (`unit/pubsub.tcl`) passes all **27** tests, including
channel and pattern subscriptions, sharded channels, RESP2 push frames,
`PUBSUB` introspection, connection cleanup, and every keyspace-notification
case in the suite.

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
  `EXEC`, `RENAME`, and `SORT ... STORE`;
- RDB files and `DUMP` payloads both ways with KeyDB for strings, lists,
  hashes, sets, sorted sets, and expirations;
- `DEBUG DIGEST` values match KeyDB exactly on all five data types and
  expirations.

## Oracle build

`scripts/build-oracle.sh` builds `KeyDB/` with `MALLOC=libc NO_MOTD=yes`. On
NixOS it supplies `libuuid`, OpenSSL, and zlib through `nix-shell`; on Debian
and Ubuntu install `uuid-dev`, `libssl-dev`, and `zlib1g-dev`.

AOF coverage:

- `unit/aofrw.tcl`: **19/19 pass**, including rewrite under concurrent write
  load, all five data types, delayed/refused rewrite handling, and AOF-enabled
  restart behavior;
- `integration/aof.tcl`: **25/25 pass**, including truncated AOF recovery,
  incomplete MULTI handling, bad-format detection, `keydb-check-aof`, SPOP,
  EXPIREAT, and fsync timing.

Additional suites:

- `unit/geo.tcl`: **56/56 pass**, including GEOADD options, GEOPOS/GEOHASH
  output parity, GEODIST units, radius/box searches, STORE/STOREDIST,
  COUNT/ANY ordering, and the randomized fuzzy searches;
- `unit/hyperloglog.tcl`: **20/20 pass**, including sparse/dense encodings,
  sparse-to-dense promotion, corrupted-object detection, PFDEBUG, multi-key
  merges, and cached cardinality invalidation;

- `unit/bitops.tcl`: **53/53 pass**;
- `unit/bitfield.tcl`: **16/16 pass**;
- `unit/type/incr.tcl`: **24/24 pass**;
- `unit/type/string.tcl`: **72/72 pass**;
- `unit/keyspace.tcl`: **51/51 pass**.

## Next

- Replication hardening: partial resync/backlog, diskless modes, blocking
  `WAIT`, RDB-based full sync, and the remaining multi-replica digest tests.
- Scripting (`EVAL`/`EVALSHA`/`SCRIPT`).
- Stream RDB and `DUMP` payloads (listpack encoding).
- Cluster, modules, Sentinel, ACL/TLS, and FLASH.
- `KEYDB.NHGET`/`NHSET` nested hashes.
- AOF RDB preamble support and automatic rewrite thresholds.
- Compact RDB output (quicklist/ziplist/intset/hash-ziplist/zset-ziplist) for
  byte-level parity.
