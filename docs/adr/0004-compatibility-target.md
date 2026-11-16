# ADR 0004: Full KeyDB parity against a pinned source tree

## Status

Accepted.

## Context

The attached KeyDB tree reports `KEYDB_REAL_VERSION 255.255.255`, so it is a
development snapshot, not a tagged release. It differs from upstream v6.3.4
(for example, FLASH storage database-index handling), and the differences are
part of the intended behavior.

## Decision

- Treat the attached `KeyDB/` tree as the reference oracle and pin it by
  content hash in `spec/keydb.lock`.
- Generate the command, configuration, and test manifests from it with
  `cargo run -p xtask -- spec`; commit the generated manifests.
- Keep upstream v6.3.4 in `reference/keydb/` (fetched by
  `scripts/fetch-keydb.sh`) for triage and cross-version comparison.
- Target RDB version 9 and Redis 6.2.6-era command semantics.
- Track full parity explicitly: no command, config, persistence feature,
  replication mode, cluster behavior, module API, Sentinel behavior, FLASH
  behavior, or tool is silently descoped.
- Prototype the Redis Modules C ABI early; it is the critical path to
  drop-in compatibility.

## Consequences

- Reproducing the reference build requires the pinned tree. Generated
  manifests are committed so normal builds do not need it.
- Cross-checking against v6.3.4 can reveal whether a behavior is a release
  behavior or a development-snapshot change.
