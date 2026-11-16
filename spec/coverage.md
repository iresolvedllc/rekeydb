# rekeydb command coverage

Implemented: 91 / 236

| Category | Implemented | Total |
|---|---:|---:|
| @admin | 1 | 1 |
| @bitmap | 0 | 7 |
| @connection | 7 | 8 |
| @dangerous | 2 | 2 |
| @geo | 0 | 10 |
| @hash | 0 | 19 |
| @hyperloglog | 0 | 5 |
| @keyspace | 23 | 39 |
| @list | 21 | 21 |
| @replication | 0 | 3 |
| @scripting | 0 | 3 |
| @set | 16 | 16 |
| @sortedset | 0 | 32 |
| @stream | 0 | 15 |
| @string | 12 | 22 |
| @transaction | 5 | 5 |
| uncategorized | 4 | 28 |

## Missing

- `module`
- `getex`
- `setnx`
- `setex`
- `psetex`
- `expdel`
- `keydb.mexists`
- `setbit`
- `getbit`
- `bitfield`
- `bitfield_ro`
- `setrange`
- `getrange`
- `substr`
- `zadd`
- `zincrby`
- `zrem`
- `zremrangebyscore`
- `zremrangebyrank`
- `zremrangebylex`
- `zunionstore`
- `zinterstore`
- `zdiffstore`
- `zunion`
- `zinter`
- `zdiff`
- `zrange`
- `zrangestore`
- `zrangebyscore`
- `zrevrangebyscore`
- `zrangebylex`
- `zrevrangebylex`
- `zcount`
- `zlexcount`
- `zrevrange`
- `zcard`
- `zscore`
- `zmscore`
- `zrank`
- `zrevrank`
- `zscan`
- `zpopmin`
- `zpopmax`
- `bzpopmin`
- `bzpopmax`
- `zrandmember`
- `hset`
- `hsetnx`
- `hget`
- `hmset`
- `hmget`
- `hincrby`
- `hincrbyfloat`
- `hdel`
- `hlen`
- `hstrlen`
- `hkeys`
- `hvals`
- `hgetall`
- `hexists`
- `hrandfield`
- `hscan`
- `incrbyfloat`
- `msetnx`
- `swapdb`
- `move`
- `copy`
- `expiremember`
- `expirememberat`
- `pexpirememberat`
- `replping`
- `save`
- `bgsave`
- `bgrewriteaof`
- `sync`
- `psync`
- `replconf`
- `monitor`
- `slaveof`
- `replicaof`
- `subscribe`
- `unsubscribe`
- `psubscribe`
- `punsubscribe`
- `publish`
- `pubsub`
- `cluster`
- `restore`
- `restore-asking`
- `migrate`
- `asking`
- `readonly`
- `readwrite`
- `dump`
- `memory`
- `eval`
- `evalsha`
- `slowlog`
- `script`
- `bitop`
- `bitcount`
- `bitpos`
- `geoadd`
- `georadius`
- `georadius_ro`
- `georadiusbymember`
- `georadiusbymember_ro`
- `geohash`
- `geopos`
- `geodist`
- `geosearch`
- `geosearchstore`
- `pfselftest`
- `pfadd`
- `pfcount`
- `pfmerge`
- `pfdebug`
- `xadd`
- `xrange`
- `xrevrange`
- `xlen`
- `xread`
- `xreadgroup`
- `xgroup`
- `xsetid`
- `xack`
- `xpending`
- `xclaim`
- `xautoclaim`
- `xinfo`
- `xdel`
- `xtrim`
- `post`
- `host:`
- `latency`
- `acl`
- `rreplay`
- `keydb.cron`
- `keydb.hrename`
- `stralgo`
- `keydb.nhget`
- `keydb.nhset`
- `KEYDB.MVCCRESTORE`
- `failover`
- `lfence`
