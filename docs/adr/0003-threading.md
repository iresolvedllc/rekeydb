# ADR 0003: Thread-per-event-loop, batched global lock ownership

## Status

Accepted.

## Context

KeyDB runs multiple worker threads, each with its own event loop and client
set. Command execution takes a global lock; threads release it for socket I/O
and long-running snapshot reads. Handing commands to an async runtime would
obscure lock lifetimes and make it harder to match KeyDB's latency behavior.

## Decision

- Use `mio` and one OS thread per worker event loop.
- Each worker owns its clients, query buffers, and reply buffers.
- Decode commands without the state lock. Acquire the global lock once per
  ready batch, execute commands, release, then write replies.
- Long-running commands will run against a pinned snapshot without the lock
  and re-acquire only to commit side effects.
- A dedicated timer/main thread runs cron, active expiry, snapshots, and
  signals. Auxiliary work (RDB/AOF, lazy free, FLASH reads) uses a bounded
  thread pool.
- No Tokio in the server core.

## Consequences

- Lock contention is measurable and tunable through batch sizes.
- `server-threads`, `min-clients-per-thread`, and thread affinity map directly
  to KeyDB semantics.
- Blocking commands, pub/sub, and client tracking need cross-worker queues;
  they are scheduled for P2 rather than bolted onto the P0 loop.
