# Benchmarks

Baselines are recorded against the reference KeyDB binary on the same host.

Recommended load generators:

- `memtier_benchmark` for pipelined SET/GET and mixed workloads;
- `redis-benchmark` for quick smoke comparisons;
- `rekeydb-bench` once the Zig tool lands.

Record for every run:

- CPU model, core count, NUMA/affinity settings;
- `server-threads`, pipeline depth, key/value sizes, dataset size;
- throughput, p50/p99/p999 latency, memory RSS;
- command mix and duration.

Store results under `bench/results/<date>/` with the exact command lines.
Initial gates: SET/GET throughput at least 90% of KeyDB at four threads and
pipeline 16; p99 no worse than 1.25x; memory no worse than 1.3x for common
types.
