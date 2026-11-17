use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rekeydb_core::{
    Database, EncodingLimits, Entry, ExpireEntry, HashValue, MvccClock, Snapshot, Value,
};
use rekeydb_persist::{RdbEntry, dataset_digest, dump, load, restore, save};
use rekeydb_protocol::{Command, RespValue, RespVersion};

use crate::config::Config;
use crate::util::glob_match;

/// The per-connection state the command layer is allowed to touch.
#[derive(Debug, Clone)]
pub struct ClientContext {
    pub id: u64,
    pub db: usize,
    pub name: Vec<u8>,
    pub version: RespVersion,
    pub addr: String,
    pub multi: Option<Vec<Command>>,
    pub watched: Vec<(usize, Vec<u8>, u64)>,
}

impl ClientContext {
    pub fn new(id: u64, addr: String) -> Self {
        Self {
            id,
            db: 0,
            name: Vec::new(),
            version: RespVersion::V2,
            addr,
            multi: None,
            watched: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    Pop,
    Move,
    ZPop { max: bool },
}

/// A client waiting for data on one or more list keys.
#[derive(Debug, Clone)]
pub struct BlockRequest {
    pub db: usize,
    pub keys: Vec<Vec<u8>>,
    pub kind: BlockKind,
    pub source_side: ListSide,
    pub destination: Option<Vec<u8>>,
    pub destination_side: ListSide,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub reply: RespValue,
    pub close: bool,
    pub shutdown: bool,
    pub block: Option<Box<BlockRequest>>,
    pub wake: Vec<(usize, Vec<u8>)>,
    pub unblock: Option<(u64, bool)>,
}

impl Outcome {
    pub fn reply(reply: RespValue) -> Self {
        Self {
            reply,
            close: false,
            shutdown: false,
            block: None,
            wake: Vec::new(),
            unblock: None,
        }
    }

    pub fn ok() -> Self {
        Self::reply(RespValue::ok())
    }

    pub fn error(message: impl Into<Vec<u8>>) -> Self {
        Self::reply(RespValue::error(message))
    }

    pub fn blocked(request: BlockRequest) -> Self {
        Self {
            reply: RespValue::Null,
            close: false,
            shutdown: false,
            block: Some(Box::new(request)),
            wake: Vec::new(),
            unblock: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub total_connections_received: u64,
    pub total_commands_processed: u64,
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
}

pub struct State {
    pub config: Config,
    dbs: Vec<Database<Value>>,
    clock: MvccClock,
    stats: Stats,
    start: Instant,
    start_unix: i64,
    now_override: Option<i64>,
    config_values: BTreeMap<String, String>,
    debug_snapshots: Vec<Option<Arc<Snapshot<Value>>>>,
    encoding_limits: EncodingLimits,
    pub connected_clients: usize,
    pub blocked_clients: usize,
}

impl State {
    pub fn new(config: Config) -> Self {
        let databases = config.databases;
        let mut config_values = BTreeMap::new();
        for key in [
            "bind",
            "port",
            "databases",
            "server-threads",
            "io-threads",
            "requirepass",
            "dir",
            "logfile",
            "dbfilename",
            "daemonize",
            "protected-mode",
            "maxmemory",
            "maxmemory-policy",
            "appendonly",
            "appendfsync",
            "save",
            "tcp-keepalive",
            "timeout",
            "hz",
            "rdbchecksum",
            "sanitize-dump-payload",
            "skip-checksum-validation",
            "lazyfree-lazy-eviction",
            "lazyfree-lazy-expire",
            "lazyfree-lazy-server-del",
            "lazyfree-lazy-user-del",
            "list-max-ziplist-size",
            "hash-max-ziplist-entries",
            "hash-max-ziplist-value",
            "set-max-intset-entries",
            "zset-max-ziplist-entries",
            "zset-max-ziplist-value",
        ] {
            config_values.insert(key.to_string(), String::new());
        }
        let mut state = Self {
            config,
            dbs: (0..databases).map(|_| Database::new()).collect(),
            clock: MvccClock::new(0),
            stats: Stats::default(),
            start: Instant::now(),
            start_unix: now_unix_ms() / 1000,
            now_override: None,
            config_values,
            debug_snapshots: (0..databases).map(|_| None).collect(),
            encoding_limits: EncodingLimits::default(),
            connected_clients: 0,
            blocked_clients: 0,
        };
        state.refresh_config_values();
        state
    }

    fn refresh_config_values(&mut self) {
        self.config_values
            .insert("bind".into(), self.config.bind.clone());
        self.config_values
            .insert("port".into(), self.config.port.to_string());
        self.config_values
            .insert("databases".into(), self.config.databases.to_string());
        self.config_values.insert(
            "server-threads".into(),
            self.config.server_threads.to_string(),
        );
        self.config_values
            .insert("io-threads".into(), self.config.server_threads.to_string());
        self.config_values.insert(
            "requirepass".into(),
            self.config.requirepass.clone().unwrap_or_default(),
        );
        self.config_values
            .insert("dir".into(), self.config.dir.display().to_string());
        self.config_values.insert(
            "logfile".into(),
            self.config.logfile.clone().unwrap_or_default(),
        );
        self.config_values
            .insert("dbfilename".into(), self.config.dbfilename.clone());
        for (key, value) in [
            ("daemonize", "no"),
            ("protected-mode", "yes"),
            ("maxmemory", "0"),
            ("maxmemory-policy", "noeviction"),
            ("appendonly", "no"),
            ("appendfsync", "everysec"),
            ("save", "3600 1 300 100 60 10000"),
            ("tcp-keepalive", "300"),
            ("timeout", "0"),
            ("hz", "10"),
            ("rdbchecksum", "yes"),
            ("sanitize-dump-payload", "no"),
            ("skip-checksum-validation", "no"),
            ("lazyfree-lazy-eviction", "no"),
            ("lazyfree-lazy-expire", "no"),
            ("lazyfree-lazy-server-del", "no"),
            ("lazyfree-lazy-user-del", "no"),
            ("list-max-ziplist-size", "-2"),
            ("hash-max-ziplist-entries", "512"),
            ("hash-max-ziplist-value", "64"),
            ("set-max-intset-entries", "512"),
            ("zset-max-ziplist-entries", "128"),
            ("zset-max-ziplist-value", "64"),
        ] {
            self.config_values.insert(key.into(), value.into());
        }
    }

    pub fn now_ms(&self) -> i64 {
        self.now_override.unwrap_or_else(now_unix_ms)
    }

    /// Test hook for deterministic TTLs.
    pub fn set_now_ms(&mut self, now_ms: i64) {
        self.now_override = Some(now_ms);
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn stats_mut(&mut self) -> &mut Stats {
        &mut self.stats
    }

    pub fn db(&self, index: usize) -> &Database<Value> {
        &self.dbs[index]
    }

    pub fn db_mut(&mut self, index: usize) -> &mut Database<Value> {
        &mut self.dbs[index]
    }

    fn expire_if_needed(&mut self, db_index: usize, key: &[u8]) {
        let now = self.now_ms();
        let expired = self.dbs[db_index]
            .get(key)
            .is_some_and(|entry| entry.is_expired(now));
        if expired {
            self.dbs[db_index].remove(key);
        }
    }

    fn existing(&mut self, db_index: usize, key: &[u8]) -> Option<Entry<Value>> {
        self.expire_if_needed(db_index, key);
        self.dbs[db_index].get(key).cloned()
    }

    /// Execute one command. The caller holds the global lock.
    pub fn execute(&mut self, command: &Command, client: &mut ClientContext) -> Outcome {
        let args = &command.args;
        if args.is_empty() {
            return Outcome::ok();
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        self.stats.total_commands_processed += 1;

        if name == "MULTI" {
            if args.len() != 1 {
                return wrong_args(args);
            }
            if client.multi.is_some() {
                return Outcome::error("ERR MULTI calls can not be nested");
            }
            client.multi = Some(Vec::new());
            return Outcome::ok();
        }
        if name == "DISCARD" {
            if args.len() != 1 {
                return wrong_args(args);
            }
            if client.multi.take().is_none() {
                return Outcome::error("ERR DISCARD without MULTI");
            }
            client.watched.clear();
            return Outcome::ok();
        }
        if name == "WATCH" {
            if args.len() < 2 {
                return wrong_args(args);
            }
            if client.multi.is_some() {
                return Outcome::error("ERR WATCH inside MULTI is not allowed");
            }
            for key in &args[1..] {
                self.expire_if_needed(client.db, key);
                let version = self.dbs[client.db].get(key).map_or(0, |entry| entry.mvcc);
                client.watched.push((client.db, key.clone(), version));
            }
            return Outcome::ok();
        }
        if name == "UNWATCH" {
            if args.len() != 1 {
                return wrong_args(args);
            }
            client.watched.clear();
            return Outcome::ok();
        }
        if name == "EXEC" {
            if args.len() != 1 {
                return wrong_args(args);
            }
            let Some(commands) = client.multi.take() else {
                return Outcome::error("ERR EXEC without MULTI");
            };
            let dirty = client.watched.iter().any(|(db, key, version)| {
                self.dbs[*db].get(key).map_or(0, |entry| entry.mvcc) != *version
            });
            client.watched.clear();
            if dirty {
                return Outcome::reply(RespValue::NullArray);
            }
            let mut replies = Vec::with_capacity(commands.len());
            let mut wake = Vec::new();
            let mut close = false;
            let mut shutdown = false;
            for command in commands {
                let outcome = self.execute(&command, client);
                if let Some(block) = outcome.block {
                    replies.push(match block.kind {
                        BlockKind::Pop | BlockKind::ZPop { .. } => RespValue::NullArray,
                        BlockKind::Move => RespValue::null(),
                    });
                } else {
                    replies.push(outcome.reply);
                }
                wake.extend(outcome.wake);
                close |= outcome.close;
                shutdown |= outcome.shutdown;
            }
            return Outcome {
                reply: RespValue::Array(replies),
                close,
                shutdown,
                block: None,
                wake,
                unblock: None,
            };
        }
        if let Some(queue) = client.multi.as_mut() {
            queue.push(command.clone());
            return Outcome::reply(RespValue::simple("QUEUED"));
        }

        match name.as_str() {
            "PING" => {
                if args.len() == 1 {
                    Outcome::reply(RespValue::simple("PONG"))
                } else if args.len() == 2 {
                    Outcome::reply(RespValue::bulk(args[1].clone()))
                } else {
                    wrong_args(args)
                }
            }
            "ECHO" => {
                if args.len() == 2 {
                    Outcome::reply(RespValue::bulk(args[1].clone()))
                } else {
                    wrong_args(args)
                }
            }
            "QUIT" => Outcome {
                reply: RespValue::ok(),
                close: true,
                shutdown: false,
                block: None,
                wake: Vec::new(),
                unblock: None,
            },
            "SELECT" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                let Ok(index) = parse_usize(&args[1]) else {
                    return Outcome::error("ERR invalid DB index");
                };
                if index >= self.dbs.len() {
                    return Outcome::error("ERR DB index is out of range");
                }
                client.db = index;
                Outcome::ok()
            }
            "SET" => self.cmd_set(args, client),
            "GET" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.existing(client.db, &args[1]) {
                    Some(entry) => match entry.value.as_string() {
                        Some(bytes) => {
                            self.stats.keyspace_hits += 1;
                            Outcome::reply(RespValue::bulk(bytes.clone()))
                        }
                        None => wrong_type(),
                    },
                    None => {
                        self.stats.keyspace_misses += 1;
                        Outcome::reply(RespValue::null())
                    }
                }
            }
            "GETDEL" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                let Some(entry) = self.existing(client.db, &args[1]) else {
                    return Outcome::reply(RespValue::null());
                };
                let Some(bytes) = entry.value.as_string() else {
                    return wrong_type();
                };
                let bytes = bytes.clone();
                self.dbs[client.db].remove(&args[1]);
                Outcome::reply(RespValue::bulk(bytes))
            }
            "GETSET" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let previous = self.existing(client.db, &args[1]);
                if let Some(entry) = &previous
                    && entry.value.as_string().is_none()
                {
                    return wrong_type();
                }
                let mvcc = self.clock.next(self.now_ms());
                self.dbs[client.db].insert(&args[1], Value::String(args[2].clone()), mvcc);
                match previous {
                    Some(entry) => Outcome::reply(RespValue::bulk(
                        entry.value.as_string().expect("checked string").clone(),
                    )),
                    None => Outcome::reply(RespValue::null()),
                }
            }
            "DEL" | "UNLINK" => {
                if args.len() < 2 {
                    return wrong_args(args);
                }
                let mut removed = 0;
                for key in &args[1..] {
                    self.expire_if_needed(client.db, key);
                    if self.dbs[client.db].remove(key).is_some() {
                        removed += 1;
                    }
                }
                Outcome::reply(RespValue::Integer(removed))
            }
            "EXISTS" | "TOUCH" => {
                if args.len() < 2 {
                    return wrong_args(args);
                }
                let mut count = 0;
                for key in &args[1..] {
                    self.expire_if_needed(client.db, key);
                    if self.dbs[client.db].contains_key(key) {
                        count += 1;
                    }
                }
                Outcome::reply(RespValue::Integer(count))
            }
            "TYPE" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                self.expire_if_needed(client.db, &args[1]);
                match self.dbs[client.db].get(&args[1]) {
                    Some(entry) => Outcome::reply(RespValue::simple(entry.value.type_name())),
                    None => Outcome::reply(RespValue::simple("none")),
                }
            }
            "DBSIZE" => {
                if args.len() != 1 {
                    return wrong_args(args);
                }
                Outcome::reply(RespValue::Integer(self.dbs[client.db].len() as i64))
            }
            "FLUSHDB" => {
                if args.len() > 2 {
                    return wrong_args(args);
                }
                self.dbs[client.db].clear();
                Outcome::ok()
            }
            "FLUSHALL" => {
                if args.len() > 2 {
                    return wrong_args(args);
                }
                for db in &mut self.dbs {
                    db.clear();
                }
                Outcome::ok()
            }
            "KEYS" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                self.cmd_keys(&args[1], client.db)
            }
            "SCAN" => self.cmd_scan(args, client.db),
            "TTL" | "PTTL" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                self.cmd_ttl(args, client.db)
            }
            "PERSIST" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                self.expire_if_needed(client.db, &args[1]);
                let Some(entry) = self.dbs[client.db].get_mut(&args[1]) else {
                    return Outcome::reply(RespValue::Integer(0));
                };
                let removed = entry.expire.remove_primary();
                Outcome::reply(RespValue::Integer(i64::from(removed)))
            }
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
                self.cmd_expire(&name, args, client.db)
            }
            "APPEND" => self.cmd_append(args, client.db),
            "STRLEN" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.existing(client.db, &args[1]) {
                    Some(entry) => match entry.value.as_string() {
                        Some(bytes) => Outcome::reply(RespValue::Integer(bytes.len() as i64)),
                        None => wrong_type(),
                    },
                    None => Outcome::reply(RespValue::Integer(0)),
                }
            }
            "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" | "LLEN" | "LINDEX"
            | "LSET" | "LRANGE" | "LTRIM" | "LREM" | "LINSERT" | "LPOS" | "RPOPLPUSH" | "LMOVE" => {
                self.cmd_list_command(&name, args, client.db)
            }
            "BLPOP" | "BRPOP" => self.cmd_bpop(&name, args, client.db),
            "BRPOPLPUSH" => self.cmd_brpoplpush(args, client.db),
            "BLMOVE" => self.cmd_blmove(args, client.db),
            "ZADD" | "ZINCRBY" | "ZREM" | "ZREMRANGEBYSCORE" | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYLEX" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" | "ZUNION"
            | "ZINTER" | "ZDIFF" | "ZRANGE" | "ZRANGESTORE" | "ZRANGEBYSCORE"
            | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX" | "ZREVRANGEBYLEX" | "ZCOUNT" | "ZLEXCOUNT"
            | "ZREVRANGE" | "ZCARD" | "ZSCORE" | "ZMSCORE" | "ZRANK" | "ZREVRANK" | "ZSCAN"
            | "ZPOPMIN" | "ZPOPMAX" | "ZRANDMEMBER" => {
                self.cmd_zset(&name, args, client.db, client.version)
            }
            "BZPOPMIN" | "BZPOPMAX" => self.cmd_bzpop(&name, args, client.db),
            "KEYDB.HRENAME" => self.cmd_hrename(args, client.db),
            "HSET" | "HSETNX" | "HMSET" | "HGET" | "HMGET" | "HDEL" | "HLEN" | "HSTRLEN"
            | "HKEYS" | "HVALS" | "HGETALL" | "HEXISTS" | "HINCRBY" | "HINCRBYFLOAT"
            | "HRANDFIELD" | "HSCAN" => self.cmd_hash(&name, args, client.db, client.version),
            "SADD" | "SREM" | "SCARD" | "SISMEMBER" | "SMISMEMBER" | "SMEMBERS" | "SPOP"
            | "SRANDMEMBER" | "SMOVE" | "SINTER" | "SUNION" | "SDIFF" | "SINTERSTORE"
            | "SUNIONSTORE" | "SDIFFSTORE" | "SSCAN" => {
                self.cmd_set_command(&name, args, client.db)
            }
            "OBJECT" => self.cmd_object(args, client.db),
            "INCR" | "DECR" | "INCRBY" | "DECRBY" => self.cmd_incr(&name, args, client.db),
            "MSET" => {
                if args.len() < 3 || args.len().is_multiple_of(2) {
                    return wrong_args(args);
                }
                let mvcc = self.clock.next(self.now_ms());
                for pair in args[1..].chunks(2) {
                    self.dbs[client.db].insert(&pair[0], Value::String(pair[1].clone()), mvcc);
                }
                Outcome::ok()
            }
            "MGET" => {
                if args.len() < 2 {
                    return wrong_args(args);
                }
                let mut values = Vec::with_capacity(args.len() - 1);
                for key in &args[1..] {
                    match self.existing(client.db, key) {
                        Some(entry) => match entry.value.as_string() {
                            Some(bytes) => values.push(RespValue::bulk(bytes.clone())),
                            None => values.push(RespValue::null()),
                        },
                        None => values.push(RespValue::null()),
                    }
                }
                Outcome::reply(RespValue::Array(values))
            }
            "RANDOMKEY" => {
                if args.len() != 1 {
                    return wrong_args(args);
                }
                self.expire_all(client.db);
                let keys = self.dbs[client.db].keys();
                if keys.is_empty() {
                    Outcome::reply(RespValue::null())
                } else {
                    let index = (self.now_ms().unsigned_abs() as usize) % keys.len();
                    Outcome::reply(RespValue::bulk(keys[index].clone()))
                }
            }
            "RENAME" | "RENAMENX" => self.cmd_rename(&name, args, client.db),
            "DUMP" => self.cmd_dump(args, client.db),
            "RESTORE" => self.cmd_restore(args, client.db),
            "SORT" => self.cmd_sort(args, client.db),
            "CONFIG" => self.cmd_config(args),
            "DEBUG" => self.cmd_debug(args, client),
            "SAVE" => {
                if args.len() != 1 {
                    return wrong_args(args);
                }
                match self.save_rdb_to_disk() {
                    Ok(()) => Outcome::ok(),
                    Err(err) => Outcome::error(format!("ERR {err}")),
                }
            }
            "BGSAVE" => {
                if args.len() > 2 {
                    return wrong_args(args);
                }
                match self.save_rdb_to_disk() {
                    Ok(()) => Outcome::reply(RespValue::simple("Background saving started")),
                    Err(err) => Outcome::error(format!("ERR {err}")),
                }
            }
            "INFO" => self.cmd_info(args),
            "COMMAND" => self.cmd_command(args),
            "CLIENT" => self.cmd_client(args, client),
            "HELLO" => self.cmd_hello(args, client),
            "AUTH" => self.cmd_auth(args),
            "ROLE" => Outcome::reply(RespValue::Array(vec![
                RespValue::bulk(b"master".to_vec()),
                RespValue::Integer(0),
                RespValue::Array(Vec::new()),
            ])),
            "WAIT" => Outcome::reply(RespValue::Integer(0)),
            "LASTSAVE" => Outcome::reply(RespValue::Integer(self.start_unix)),
            "TIME" => {
                let now = self.now_ms();
                Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk((now / 1000).to_string().into_bytes()),
                    RespValue::bulk(((now % 1000) * 1000).to_string().into_bytes()),
                ]))
            }
            "RESET" => {
                client.db = 0;
                client.name.clear();
                client.version = RespVersion::V2;
                Outcome::reply(RespValue::simple("RESET"))
            }
            "SHUTDOWN" => Outcome {
                reply: RespValue::ok(),
                close: true,
                shutdown: true,
                block: None,
                wake: Vec::new(),
                unblock: None,
            },
            _ => {
                let mut message = format!(
                    "ERR unknown command '{}', with args beginning with: ",
                    String::from_utf8_lossy(&args[0])
                );
                for arg in args.iter().skip(1).take(10) {
                    message.push('\'');
                    message.push_str(&String::from_utf8_lossy(arg));
                    message.push_str("', ");
                }
                Outcome::error(message)
            }
        }
    }

    fn cmd_set(&mut self, args: &[Vec<u8>], client: &mut ClientContext) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let now = self.now_ms();
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut keepttl = false;
        let mut expire_at: Option<i64> = None;
        let mut index = 3;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "NX" => {
                    nx = true;
                    index += 1;
                }
                "XX" => {
                    xx = true;
                    index += 1;
                }
                "GET" => {
                    get = true;
                    index += 1;
                }
                "KEEPTTL" => {
                    keepttl = true;
                    index += 1;
                }
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Ok(number) = parse_i64(value) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    let when = match option.as_str() {
                        "EX" => now.saturating_add(number.saturating_mul(1000)),
                        "PX" => now.saturating_add(number),
                        "EXAT" => number.saturating_mul(1000),
                        "PXAT" => number,
                        _ => unreachable!(),
                    };
                    expire_at = Some(when);
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }

        if nx && xx {
            return Outcome::error("ERR syntax error");
        }
        if expire_at.is_some_and(|when| when <= now) {
            return Outcome::error("ERR invalid expire time in set");
        }

        let previous = self.existing(client.db, &args[1]);
        if nx && previous.is_some() {
            return Outcome::reply(RespValue::null());
        }
        if xx && previous.is_none() {
            return Outcome::reply(RespValue::null());
        }
        let old_reply = if get {
            match previous.as_ref() {
                Some(entry) => match entry.value.as_string() {
                    Some(bytes) => RespValue::bulk(bytes.clone()),
                    None => return wrong_type(),
                },
                None => RespValue::null(),
            }
        } else {
            RespValue::ok()
        };
        let expire = if keepttl {
            previous
                .as_ref()
                .map(|entry| entry.expire.clone())
                .unwrap_or_default()
        } else {
            let mut expire = ExpireEntry::new();
            if let Some(when) = expire_at {
                expire.set_primary(when);
            }
            expire
        };
        let mvcc = self.clock.next(now);
        self.dbs[client.db].insert_entry(
            args[1].clone(),
            Entry::with_expire(Arc::new(Value::String(args[2].clone())), expire, mvcc),
        );
        Outcome::reply(old_reply)
    }

    fn cmd_keys(&mut self, pattern: &[u8], db_index: usize) -> Outcome {
        let now = self.now_ms();
        let keys: Vec<RespValue> = self.dbs[db_index]
            .collect_visible()
            .into_iter()
            .filter(|(key, entry)| !entry.is_expired(now) && glob_match(pattern, key))
            .map(|(key, _)| RespValue::bulk(key))
            .collect();
        Outcome::reply(RespValue::Array(keys))
    }

    fn cmd_scan(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let Ok(cursor) = parse_usize(&args[1]) else {
            return Outcome::error("ERR invalid cursor");
        };
        let mut count = 10usize;
        let mut pattern: Option<&[u8]> = None;
        let mut type_filter: Option<Vec<u8>> = None;
        let mut index = 2;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "MATCH" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    pattern = Some(value);
                    index += 2;
                }
                "COUNT" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Ok(value) = parse_usize(value) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    count = value.max(1);
                    index += 2;
                }
                "TYPE" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    type_filter = Some(value.to_ascii_lowercase());
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let now = self.now_ms();
        let mut keys: Vec<Vec<u8>> = self.dbs[db_index]
            .collect_visible()
            .into_iter()
            .filter(|(key, entry)| {
                !entry.is_expired(now)
                    && pattern.is_none_or(|pattern| glob_match(pattern, key))
                    && type_filter.as_ref().is_none_or(|wanted| {
                        entry
                            .value
                            .type_name()
                            .as_bytes()
                            .eq_ignore_ascii_case(wanted)
                    })
            })
            .map(|(key, _)| key)
            .collect();
        keys.sort();
        let start = cursor.min(keys.len());
        let end = start.saturating_add(count).min(keys.len());
        let next = if end >= keys.len() { 0 } else { end };
        let batch = keys[start..end]
            .iter()
            .cloned()
            .map(RespValue::bulk)
            .collect();
        Outcome::reply(RespValue::Array(vec![
            RespValue::bulk(next.to_string().into_bytes()),
            RespValue::Array(batch),
        ]))
    }

    fn cmd_ttl(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        let milliseconds = args[0].eq_ignore_ascii_case(b"pttl");
        self.expire_if_needed(db_index, &args[1]);
        let now = self.now_ms();
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::Integer(-2));
        };
        let Some(when) = entry.expire.primary() else {
            return Outcome::reply(RespValue::Integer(-1));
        };
        let remaining = when.saturating_sub(now);
        if remaining <= 0 {
            return Outcome::reply(RespValue::Integer(-2));
        }
        let ttl = if milliseconds {
            remaining
        } else {
            (remaining + 500) / 1000
        };
        Outcome::reply(RespValue::Integer(ttl))
    }

    fn cmd_expire(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let Ok(number) = parse_i64(&args[2]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        let now = self.now_ms();
        let when = match command {
            "EXPIRE" => now.saturating_add(number.saturating_mul(1000)),
            "PEXPIRE" => now.saturating_add(number),
            "EXPIREAT" => number.saturating_mul(1000),
            "PEXPIREAT" => number,
            _ => unreachable!(),
        };
        self.expire_if_needed(db_index, &args[1]);
        if !self.dbs[db_index].contains_key(&args[1]) {
            return Outcome::reply(RespValue::Integer(0));
        }
        if when <= now {
            self.dbs[db_index].remove(&args[1]);
            return Outcome::reply(RespValue::Integer(1));
        }
        if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
            entry.expire.set_primary(when);
        }
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_append(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        self.expire_if_needed(db_index, &args[1]);
        let mvcc = self.clock.next(self.now_ms());
        if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
            let value = Arc::make_mut(&mut entry.value);
            let Some(bytes) = value.as_string_mut() else {
                return wrong_type();
            };
            bytes.extend_from_slice(&args[2]);
            return Outcome::reply(RespValue::Integer(bytes.len() as i64));
        }
        let value = Value::String(args[2].clone());
        let length = value.len() as i64;
        self.dbs[db_index].insert(&args[1], value, mvcc);
        Outcome::reply(RespValue::Integer(length))
    }

    fn cmd_incr(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        let (key, delta) = match command {
            "INCR" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                (&args[1], 1)
            }
            "DECR" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                (&args[1], -1)
            }
            "INCRBY" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let Ok(delta) = parse_i64(&args[2]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                (&args[1], delta)
            }
            "DECRBY" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let Ok(delta) = parse_i64(&args[2]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                (&args[1], delta.checked_neg().unwrap_or(i64::MIN))
            }
            _ => unreachable!(),
        };
        let existing = self.existing(db_index, key);
        let (current, expire) = match existing {
            Some(entry) => match entry.value.as_string() {
                Some(bytes) => match parse_i64(bytes) {
                    Ok(value) => (value, entry.expire),
                    Err(_) => {
                        return Outcome::error("ERR value is not an integer or out of range");
                    }
                },
                None => return wrong_type(),
            },
            None => (0, ExpireEntry::new()),
        };
        let Some(value) = current.checked_add(delta) else {
            return Outcome::error("ERR increment or decrement would overflow");
        };
        let mvcc = self.clock.next(self.now_ms());
        self.dbs[db_index].insert_entry(
            key.to_vec(),
            Entry::with_expire(
                Arc::new(Value::String(value.to_string().into_bytes())),
                expire,
                mvcc,
            ),
        );
        Outcome::reply(RespValue::Integer(value))
    }

    fn expire_all(&mut self, db_index: usize) {
        let now = self.now_ms();
        let expired: Vec<Vec<u8>> = self.dbs[db_index]
            .collect_visible()
            .into_iter()
            .filter(|(_, entry)| entry.is_expired(now))
            .map(|(key, _)| key)
            .collect();
        for key in expired {
            self.dbs[db_index].remove(&key);
        }
    }

    fn cmd_list_command(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        match command {
            "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                self.expire_if_needed(db_index, &args[1]);
                let only_existing = command.ends_with('X');
                if only_existing && !self.dbs[db_index].contains_key(&args[1]) {
                    return Outcome::reply(RespValue::Integer(0));
                }
                let mvcc = self.clock.next(self.now_ms());
                let list = match self.list_for_write(db_index, &args[1], mvcc) {
                    Ok(list) => list,
                    Err(outcome) => return outcome,
                };
                if command.starts_with('L') {
                    for value in &args[2..] {
                        list.push_front(value.clone());
                    }
                } else {
                    for value in &args[2..] {
                        list.push_back(value.clone());
                    }
                }
                let mut outcome = Outcome::reply(RespValue::Integer(list.len() as i64));
                outcome.wake.push((db_index, args[1].clone()));
                outcome
            }
            "LLEN" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.get_list(db_index, &args[1]) {
                    Ok(Some(list)) => Outcome::reply(RespValue::Integer(list.len() as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "LPOP" | "RPOP" => {
                if args.len() < 2 || args.len() > 3 {
                    return wrong_args(args);
                }
                let count = if args.len() == 3 {
                    let Ok(count) = parse_i64(&args[2]) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    if count < 0 {
                        return Outcome::error("ERR value is out of range, must be positive");
                    }
                    if count == 0 {
                        return Outcome::reply(RespValue::NullArray);
                    }
                    Some(count as usize)
                } else {
                    None
                };
                self.expire_if_needed(db_index, &args[1]);
                if !self.dbs[db_index].contains_key(&args[1]) {
                    return Outcome::reply(RespValue::null());
                }
                let side = if command == "LPOP" {
                    ListSide::Left
                } else {
                    ListSide::Right
                };
                let mut popped = Vec::new();
                let empty;
                {
                    let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
                        return Outcome::reply(RespValue::null());
                    };
                    let value = Arc::make_mut(&mut entry.value);
                    let Some(list) = value.as_list_mut() else {
                        return wrong_type();
                    };
                    let take = count.unwrap_or(1);
                    for _ in 0..take {
                        let item = match side {
                            ListSide::Left => list.pop_front(),
                            ListSide::Right => list.pop_back(),
                        };
                        match item {
                            Some(item) => popped.push(item),
                            None => break,
                        }
                    }
                    empty = list.is_empty();
                }
                if empty {
                    self.dbs[db_index].remove(&args[1]);
                }
                if count.is_none() {
                    match popped.into_iter().next() {
                        Some(value) => Outcome::reply(RespValue::bulk(value)),
                        None => Outcome::reply(RespValue::null()),
                    }
                } else {
                    Outcome::reply(RespValue::Array(
                        popped.into_iter().map(RespValue::bulk).collect(),
                    ))
                }
            }
            "LINDEX" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let Ok(requested) = parse_i64(&args[2]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                match self.get_list(db_index, &args[1]) {
                    Ok(Some(list)) => {
                        let index = normalize_index(list.len(), requested);
                        match index.and_then(|index| list.get(index)) {
                            Some(value) => Outcome::reply(RespValue::bulk(value.clone())),
                            None => Outcome::reply(RespValue::null()),
                        }
                    }
                    Ok(None) => Outcome::reply(RespValue::null()),
                    Err(outcome) => outcome,
                }
            }
            "LSET" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Ok(requested) = parse_i64(&args[2]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
                    return Outcome::error("ERR no such key");
                };
                let value = Arc::make_mut(&mut entry.value);
                let Some(list) = value.as_list_mut() else {
                    return wrong_type();
                };
                let index = normalize_index(list.len(), requested);
                let Some(index) = index else {
                    return Outcome::error("ERR index out of range");
                };
                list[index] = args[3].clone();
                Outcome::ok()
            }
            "LRANGE" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let (Ok(start), Ok(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                match self.get_list(db_index, &args[1]) {
                    Ok(Some(list)) => {
                        let Some((start, stop)) = normalize_range(list.len(), start, stop) else {
                            return Outcome::reply(RespValue::Array(Vec::new()));
                        };
                        Outcome::reply(RespValue::Array(
                            list.range(start..=stop)
                                .cloned()
                                .map(RespValue::bulk)
                                .collect(),
                        ))
                    }
                    Ok(None) => Outcome::reply(RespValue::Array(Vec::new())),
                    Err(outcome) => outcome,
                }
            }
            "LTRIM" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let (Ok(start), Ok(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
                    return Outcome::ok();
                };
                let value = Arc::make_mut(&mut entry.value);
                let Some(list) = value.as_list_mut() else {
                    return wrong_type();
                };
                let range = normalize_range(list.len(), start, stop);
                let trimmed: VecDeque<Vec<u8>> = match range {
                    Some((start, stop)) => list.range(start..=stop).cloned().collect(),
                    None => VecDeque::new(),
                };
                list.clear();
                list.extend(trimmed);
                let empty = list.is_empty();
                if empty {
                    self.dbs[db_index].remove(&args[1]);
                }
                Outcome::ok()
            }
            "LREM" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Ok(count) = parse_i64(&args[2]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
                    return Outcome::reply(RespValue::Integer(0));
                };
                let value = Arc::make_mut(&mut entry.value);
                let Some(list) = value.as_list_mut() else {
                    return wrong_type();
                };
                let mut removed = 0i64;
                let mut kept = VecDeque::with_capacity(list.len());
                if count >= 0 {
                    for item in std::mem::take(list) {
                        if item == args[3] && (count == 0 || removed < count) {
                            removed += 1;
                        } else {
                            kept.push_back(item);
                        }
                    }
                } else {
                    let limit = count.unsigned_abs();
                    let mut reversed = VecDeque::with_capacity(list.len());
                    while let Some(item) = list.pop_back() {
                        if item == args[3] && removed < limit as i64 {
                            removed += 1;
                        } else {
                            reversed.push_back(item);
                        }
                    }
                    while let Some(item) = reversed.pop_back() {
                        kept.push_back(item);
                    }
                }
                *list = kept;
                let empty = list.is_empty();
                if empty {
                    self.dbs[db_index].remove(&args[1]);
                }
                Outcome::reply(RespValue::Integer(removed))
            }
            "LINSERT" => {
                if args.len() != 5 {
                    return wrong_args(args);
                }
                let before = if args[2].eq_ignore_ascii_case(b"BEFORE") {
                    true
                } else if args[2].eq_ignore_ascii_case(b"AFTER") {
                    false
                } else {
                    return Outcome::error("ERR syntax error");
                };
                let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
                    return Outcome::reply(RespValue::Integer(0));
                };
                let value = Arc::make_mut(&mut entry.value);
                let Some(list) = value.as_list_mut() else {
                    return wrong_type();
                };
                let Some(position) = list.iter().position(|item| item == &args[3]) else {
                    return Outcome::reply(RespValue::Integer(-1));
                };
                let index = if before { position } else { position + 1 };
                list.insert(index, args[4].clone());
                Outcome::reply(RespValue::Integer(list.len() as i64))
            }
            "LPOS" => self.cmd_lpos(args, db_index),
            "RPOPLPUSH" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.list_move(
                    db_index,
                    &args[1],
                    &args[2],
                    ListSide::Right,
                    ListSide::Left,
                ) {
                    Ok(Some(value)) => {
                        let mut outcome = Outcome::reply(RespValue::bulk(value));
                        outcome.wake.push((db_index, args[2].clone()));
                        outcome
                    }
                    Ok(None) => Outcome::reply(RespValue::null()),
                    Err(outcome) => outcome,
                }
            }
            "LMOVE" => {
                if args.len() != 5 {
                    return wrong_args(args);
                }
                let Some(from) = parse_side(&args[3]) else {
                    return Outcome::error("ERR syntax error");
                };
                let Some(to) = parse_side(&args[4]) else {
                    return Outcome::error("ERR syntax error");
                };
                match self.list_move(db_index, &args[1], &args[2], from, to) {
                    Ok(Some(value)) => {
                        let mut outcome = Outcome::reply(RespValue::bulk(value));
                        outcome.wake.push((db_index, args[2].clone()));
                        outcome
                    }
                    Ok(None) => Outcome::reply(RespValue::null()),
                    Err(outcome) => outcome,
                }
            }
            _ => unreachable!(),
        }
    }

    fn cmd_lpos(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let mut rank = 1i64;
        let mut count: Option<i64> = None;
        let mut maxlen = 0i64;
        let mut index = 3;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            let Some(value) = args.get(index + 1) else {
                return Outcome::error("ERR syntax error");
            };
            let Ok(parsed) = parse_i64(value) else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            match option.as_str() {
                "RANK" => {
                    if parsed == 0 {
                        return Outcome::error(
                            "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second, ...",
                        );
                    }
                    rank = parsed;
                    index += 2;
                }
                "COUNT" => {
                    if parsed < 0 {
                        return Outcome::error("ERR COUNT can't be negative");
                    }
                    count = Some(parsed);
                    index += 2;
                }
                "MAXLEN" => {
                    if parsed < 0 {
                        return Outcome::error("ERR MAXLEN can't be negative");
                    }
                    maxlen = parsed;
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let list = match self.get_list(db_index, &args[1]) {
            Ok(Some(list)) => list,
            Ok(None) => {
                return if count.is_some() {
                    Outcome::reply(RespValue::Array(Vec::new()))
                } else {
                    Outcome::reply(RespValue::null())
                };
            }
            Err(outcome) => return outcome,
        };
        let needle = &args[2];
        let mut matches = Vec::new();
        let scan_limit = if maxlen == 0 {
            list.len()
        } else {
            (maxlen as usize).min(list.len())
        };
        if rank > 0 {
            let mut skipped = 0i64;
            for (position, item) in list.iter().take(scan_limit).enumerate() {
                if item != needle {
                    continue;
                }
                skipped += 1;
                if skipped < rank {
                    continue;
                }
                matches.push(position as i64);
                if count.is_none_or(|count| count > 0 && matches.len() >= count as usize) {
                    break;
                }
            }
        } else {
            let mut skipped = 0i64;
            for (position, item) in list.iter().enumerate().rev().take(scan_limit) {
                if item != needle {
                    continue;
                }
                skipped += 1;
                if skipped < rank.unsigned_abs() as i64 {
                    continue;
                }
                matches.push(position as i64);
                if count.is_none_or(|count| count > 0 && matches.len() >= count as usize) {
                    break;
                }
            }
        }
        if count.is_some() {
            Outcome::reply(RespValue::Array(
                matches.into_iter().map(RespValue::Integer).collect(),
            ))
        } else {
            match matches.into_iter().next() {
                Some(value) => Outcome::reply(RespValue::Integer(value)),
                None => Outcome::reply(RespValue::null()),
            }
        }
    }

    fn cmd_bpop(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let timeout_ms = match parse_timeout(args.last().expect("timeout")) {
            Ok(timeout) => timeout,
            Err(outcome) => return outcome,
        };
        let side = if command == "BLPOP" {
            ListSide::Left
        } else {
            ListSide::Right
        };
        let keys = &args[1..args.len() - 1];
        for key in keys {
            self.expire_if_needed(db_index, key);
            match self.dbs[db_index].get(key) {
                Some(entry) if entry.value.as_list().is_none() => return wrong_type(),
                Some(entry) if !entry.value.as_list().is_some_and(VecDeque::is_empty) => {
                    let value = self
                        .list_pop_value(db_index, key, side)
                        .expect("list is non-empty");
                    return Outcome::reply(RespValue::Array(vec![
                        RespValue::bulk(key.clone()),
                        RespValue::bulk(value),
                    ]));
                }
                _ => {}
            }
        }
        Outcome::blocked(BlockRequest {
            db: db_index,
            keys: keys.to_vec(),
            kind: BlockKind::Pop,
            source_side: side,
            destination: None,
            destination_side: ListSide::Left,
            timeout_ms,
        })
    }

    fn cmd_brpoplpush(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        let timeout_ms = match parse_timeout(&args[3]) {
            Ok(timeout) => timeout,
            Err(outcome) => return outcome,
        };
        match self.list_move(
            db_index,
            &args[1],
            &args[2],
            ListSide::Right,
            ListSide::Left,
        ) {
            Ok(Some(value)) => {
                let mut outcome = Outcome::reply(RespValue::bulk(value));
                outcome.wake.push((db_index, args[2].clone()));
                outcome
            }
            Ok(None) => Outcome::blocked(BlockRequest {
                db: db_index,
                keys: vec![args[1].clone()],
                kind: BlockKind::Move,
                source_side: ListSide::Right,
                destination: Some(args[2].clone()),
                destination_side: ListSide::Left,
                timeout_ms,
            }),
            Err(outcome) => outcome,
        }
    }

    fn cmd_blmove(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 6 {
            return wrong_args(args);
        }
        let Some(from) = parse_side(&args[3]) else {
            return Outcome::error("ERR syntax error");
        };
        let Some(to) = parse_side(&args[4]) else {
            return Outcome::error("ERR syntax error");
        };
        let timeout_ms = match parse_timeout(&args[5]) {
            Ok(timeout) => timeout,
            Err(outcome) => return outcome,
        };
        match self.list_move(db_index, &args[1], &args[2], from, to) {
            Ok(Some(value)) => {
                let mut outcome = Outcome::reply(RespValue::bulk(value));
                outcome.wake.push((db_index, args[2].clone()));
                outcome
            }
            Ok(None) => Outcome::blocked(BlockRequest {
                db: db_index,
                keys: vec![args[1].clone()],
                kind: BlockKind::Move,
                source_side: from,
                destination: Some(args[2].clone()),
                destination_side: to,
                timeout_ms,
            }),
            Err(outcome) => outcome,
        }
    }

    /// Serve a blocked client after data is available. Returns `None` if the
    /// source is still empty, in which case the client stays blocked.
    pub fn serve_blocked(&mut self, request: &BlockRequest) -> Option<RespValue> {
        match request.kind {
            BlockKind::Pop => {
                for key in &request.keys {
                    match self.dbs[request.db].get(key) {
                        Some(entry)
                            if entry.value.as_list().is_some_and(|list| !list.is_empty()) =>
                        {
                            let value = self
                                .list_pop_value(request.db, key, request.source_side)
                                .expect("list is non-empty");
                            return Some(RespValue::Array(vec![
                                RespValue::bulk(key.clone()),
                                RespValue::bulk(value),
                            ]));
                        }
                        Some(entry) if entry.value.as_list().is_none() => {}
                        _ => {}
                    }
                }
                None
            }
            BlockKind::ZPop { max } => {
                for key in &request.keys {
                    if let Ok(Some(zset)) = self.get_zset(request.db, key) {
                        if zset.scores.is_empty() {
                            continue;
                        }
                        let members = sorted_members(&zset, max);
                        let (member, score) = &members[0];
                        self.remove_zset_members(request.db, key, std::slice::from_ref(member));
                        return Some(RespValue::Array(vec![
                            RespValue::bulk(key.clone()),
                            RespValue::bulk(member.clone()),
                            score_value(*score, RespVersion::V2),
                        ]));
                    }
                }
                None
            }
            BlockKind::Move => {
                let destination = request.destination.as_ref()?;
                match self.list_move(
                    request.db,
                    &request.keys[0],
                    destination,
                    request.source_side,
                    request.destination_side,
                ) {
                    Ok(Some(value)) => Some(RespValue::bulk(value)),
                    Ok(None) => None,
                    Err(outcome) => Some(outcome.reply),
                }
            }
        }
    }

    fn get_list(
        &mut self,
        db_index: usize,
        key: &[u8],
    ) -> Result<Option<VecDeque<Vec<u8>>>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_list() {
                Some(list) => Ok(Some(list.clone())),
                None => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn list_for_write(
        &mut self,
        db_index: usize,
        key: &[u8],
        mvcc: u64,
    ) -> Result<&mut VecDeque<Vec<u8>>, Outcome> {
        if !self.dbs[db_index].contains_key(key) {
            self.dbs[db_index].insert(key, Value::List(VecDeque::new()), mvcc);
        }
        let Some(entry) = self.dbs[db_index].get_mut(key) else {
            return Err(wrong_type());
        };
        entry.mvcc = mvcc;
        let value = Arc::make_mut(&mut entry.value);
        match value {
            Value::List(list) => Ok(list),
            _ => Err(wrong_type()),
        }
    }

    fn list_pop_value(&mut self, db_index: usize, key: &[u8], side: ListSide) -> Option<Vec<u8>> {
        let mvcc = self.clock.next(self.now_ms());
        let empty;
        let value;
        {
            let entry = self.dbs[db_index].get_mut(key)?;
            entry.mvcc = mvcc;
            let value_ref = Arc::make_mut(&mut entry.value);
            let list = value_ref.as_list_mut()?;
            value = match side {
                ListSide::Left => list.pop_front(),
                ListSide::Right => list.pop_back(),
            };
            empty = list.is_empty();
        }
        if empty {
            self.dbs[db_index].remove(key);
        }
        value
    }

    fn list_move(
        &mut self,
        db_index: usize,
        source: &[u8],
        destination: &[u8],
        from: ListSide,
        to: ListSide,
    ) -> Result<Option<Vec<u8>>, Outcome> {
        self.expire_if_needed(db_index, source);
        self.expire_if_needed(db_index, destination);
        if source == destination {
            let Some(entry) = self.dbs[db_index].get_mut(source) else {
                return Ok(None);
            };
            let value_ref = Arc::make_mut(&mut entry.value);
            let Some(list) = value_ref.as_list_mut() else {
                return Err(wrong_type());
            };
            let value = match from {
                ListSide::Left => list.pop_front(),
                ListSide::Right => list.pop_back(),
            };
            if let Some(value) = &value {
                match to {
                    ListSide::Left => list.push_front(value.clone()),
                    ListSide::Right => list.push_back(value.clone()),
                }
            }
            return Ok(value);
        }

        match self.dbs[db_index].get(source) {
            Some(entry) if entry.value.as_list().is_none() => return Err(wrong_type()),
            None => return Ok(None),
            _ => {}
        }
        if let Some(entry) = self.dbs[db_index].get(destination)
            && entry.value.as_list().is_none()
        {
            return Err(wrong_type());
        }
        let Some(value) = self.list_pop_value(db_index, source, from) else {
            return Ok(None);
        };
        let mvcc = self.clock.next(self.now_ms());
        let list = self.list_for_write(db_index, destination, mvcc)?;
        match to {
            ListSide::Left => list.push_front(value.clone()),
            ListSide::Right => list.push_back(value.clone()),
        }
        Ok(Some(value))
    }

    fn cmd_hrename(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        self.expire_if_needed(db_index, &args[1]);
        if !self.dbs[db_index].contains_key(&args[1]) {
            return Outcome::reply(RespValue::null());
        }
        let mvcc = self.clock.next(self.now_ms());
        let hash = match self.hash_for_write(db_index, &args[1], mvcc) {
            Ok(hash) => hash,
            Err(outcome) => return outcome,
        };
        let Some(value) = hash.shift_remove(&args[2]) else {
            return Outcome::error("ERR hash key doesn't exist");
        };
        hash.insert(args[3].clone(), value);
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_zset(
        &mut self,
        command: &str,
        args: &[Vec<u8>],
        db_index: usize,
        version: RespVersion,
    ) -> Outcome {
        match command {
            "ZADD" => self.cmd_zadd(args, db_index, version),
            "ZINCRBY" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Some(increment) = parse_score(&args[2]) else {
                    return Outcome::error("ERR value is not a valid float");
                };
                let member = &args[3];
                let current = match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => zset.scores.get(member).copied().unwrap_or(0.0),
                    Ok(None) => 0.0,
                    Err(outcome) => return outcome,
                };
                let score = current + increment;
                if score.is_nan() {
                    return Outcome::error("ERR resulting score is not a number (NaN)");
                }
                let mvcc = self.clock.next(self.now_ms());
                let zset = match self.zset_for_write(db_index, &args[1], mvcc) {
                    Ok(zset) => zset,
                    Err(outcome) => return outcome,
                };
                zset.scores.insert(member.clone(), score);
                let mut outcome = Outcome::reply(score_value(score, version));
                outcome.wake.push((db_index, args[1].clone()));
                outcome
            }
            "ZREM" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(_)) => Outcome::reply(RespValue::Integer(self.remove_zset_members(
                        db_index,
                        &args[1],
                        &args[2..],
                    ) as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZCARD" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => Outcome::reply(RespValue::Integer(zset.scores.len() as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZSCORE" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => match zset.scores.get(&args[2]) {
                        Some(score) => Outcome::reply(score_value(*score, version)),
                        None => Outcome::reply(RespValue::null()),
                    },
                    Ok(None) => Outcome::reply(RespValue::null()),
                    Err(outcome) => outcome,
                }
            }
            "ZMSCORE" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => Outcome::reply(RespValue::Array(
                        args[2..]
                            .iter()
                            .map(|member| match zset.scores.get(member) {
                                Some(score) => score_value(*score, version),
                                None => RespValue::null(),
                            })
                            .collect(),
                    )),
                    Ok(None) => Outcome::reply(RespValue::Array(
                        args[2..].iter().map(|_| RespValue::null()).collect(),
                    )),
                    Err(outcome) => outcome,
                }
            }
            "ZRANK" | "ZREVRANK" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => {
                        let members = sorted_members(&zset, command == "ZREVRANK");
                        match members.iter().position(|(member, _)| member == &args[2]) {
                            Some(index) => Outcome::reply(RespValue::Integer(index as i64)),
                            None => Outcome::reply(RespValue::null()),
                        }
                    }
                    Ok(None) => Outcome::reply(RespValue::null()),
                    Err(outcome) => outcome,
                }
            }
            "ZCOUNT" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Some(min) = parse_score_bound(&args[2]) else {
                    return Outcome::error("ERR min or max is not a float");
                };
                let Some(max) = parse_score_bound(&args[3]) else {
                    return Outcome::error("ERR min or max is not a float");
                };
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => Outcome::reply(RespValue::Integer(
                        zset.scores
                            .values()
                            .filter(|score| score_in_range(**score, min, max))
                            .count() as i64,
                    )),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZLEXCOUNT" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Some(min) = parse_lex_bound(&args[2]) else {
                    return Outcome::error("ERR min or max not valid string range item");
                };
                let Some(max) = parse_lex_bound(&args[3]) else {
                    return Outcome::error("ERR min or max not valid string range item");
                };
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => Outcome::reply(RespValue::Integer(
                        zset.scores
                            .keys()
                            .filter(|member| lex_in_range(member, &min, &max))
                            .count() as i64,
                    )),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX"
            | "ZREVRANGEBYLEX" => self.cmd_zrange(command, args, db_index, version, None),
            "ZRANGESTORE" => {
                if args.len() < 5 {
                    return wrong_args(args);
                }
                self.cmd_zrange(
                    command,
                    &args[1..],
                    db_index,
                    version,
                    Some(args[1].clone()),
                )
            }
            "ZREMRANGEBYRANK" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let (Ok(start), Ok(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => {
                        let members = sorted_members(&zset, false);
                        let Some((start, stop)) = normalize_range(members.len(), start, stop)
                        else {
                            return Outcome::reply(RespValue::Integer(0));
                        };
                        let keys: Vec<Vec<u8>> = members[start..=stop]
                            .iter()
                            .map(|(member, _)| member.clone())
                            .collect();
                        Outcome::reply(RespValue::Integer(
                            self.remove_zset_members(db_index, &args[1], &keys) as i64,
                        ))
                    }
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZREMRANGEBYSCORE" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Some(min) = parse_score_bound(&args[2]) else {
                    return Outcome::error("ERR min or max is not a float");
                };
                let Some(max) = parse_score_bound(&args[3]) else {
                    return Outcome::error("ERR min or max is not a float");
                };
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => {
                        let keys: Vec<Vec<u8>> = zset
                            .scores
                            .iter()
                            .filter(|(_, score)| score_in_range(**score, min, max))
                            .map(|(member, _)| member.clone())
                            .collect();
                        Outcome::reply(RespValue::Integer(
                            self.remove_zset_members(db_index, &args[1], &keys) as i64,
                        ))
                    }
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZREMRANGEBYLEX" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Some(min) = parse_lex_bound(&args[2]) else {
                    return Outcome::error("ERR min or max not valid string range item");
                };
                let Some(max) = parse_lex_bound(&args[3]) else {
                    return Outcome::error("ERR min or max not valid string range item");
                };
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => {
                        let keys: Vec<Vec<u8>> = zset
                            .scores
                            .keys()
                            .filter(|member| lex_in_range(member, &min, &max))
                            .cloned()
                            .collect();
                        Outcome::reply(RespValue::Integer(
                            self.remove_zset_members(db_index, &args[1], &keys) as i64,
                        ))
                    }
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "ZPOPMIN" | "ZPOPMAX" => {
                if args.len() < 2 || args.len() > 3 {
                    return wrong_args(args);
                }
                let count = if args.len() == 3 {
                    match parse_i64(&args[2]) {
                        Ok(value) if value > 0 => Some(value as usize),
                        _ => return Outcome::reply(RespValue::Array(Vec::new())),
                    }
                } else {
                    None
                };
                let max = command == "ZPOPMAX";
                match self.get_zset(db_index, &args[1]) {
                    Ok(Some(zset)) => {
                        let members = sorted_members(&zset, max);
                        let take = count.unwrap_or(1).min(members.len());
                        let selected: Vec<(Vec<u8>, f64)> =
                            members.into_iter().take(take).collect();
                        if !selected.is_empty() {
                            let keys: Vec<Vec<u8>> =
                                selected.iter().map(|(member, _)| member.clone()).collect();
                            self.remove_zset_members(db_index, &args[1], &keys);
                        }
                        Outcome::reply(zset_pop_reply(&selected, count.is_some(), version))
                    }
                    Ok(None) => Outcome::reply(RespValue::Array(Vec::new())),
                    Err(outcome) => outcome,
                }
            }
            "ZRANDMEMBER" => self.cmd_zrandmember(args, db_index, version),
            "ZSCAN" => self.cmd_zscan(args, db_index, version),
            "ZUNION" | "ZINTER" | "ZDIFF" => self.cmd_zsetop(command, args, db_index, version),
            "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" => {
                self.cmd_zsetop(command, args, db_index, version)
            }
            _ => unreachable!(),
        }
    }

    fn cmd_zadd(&mut self, args: &[Vec<u8>], db_index: usize, version: RespVersion) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut incr = false;
        let mut index = 2;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GT" => gt = true,
                "LT" => lt = true,
                "CH" => ch = true,
                "INCR" => incr = true,
                _ => break,
            }
            index += 1;
        }
        if nx && xx {
            return Outcome::error("ERR XX and NX options at the same time are not compatible");
        }
        if gt && lt {
            return Outcome::error(
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            );
        }
        if nx && (gt || lt) {
            return Outcome::error(
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            );
        }
        let pairs = &args[index..];
        if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
            return Outcome::error("ERR syntax error");
        }
        if incr && pairs.len() != 2 {
            return Outcome::error("ERR INCR option supports a single increment-element pair");
        }
        let existing = match self.get_zset(db_index, &args[1]) {
            Ok(existing) => existing,
            Err(outcome) => return outcome,
        };
        if existing.is_none() && xx {
            return Outcome::reply(if incr {
                RespValue::null()
            } else {
                RespValue::Integer(0)
            });
        }

        // Parse and validate every pair before creating the key, so a single
        // bad score does not leave a partially modified sorted set behind.
        let mut requested = Vec::with_capacity(pairs.len() / 2);
        for pair in pairs.chunks(2) {
            let Some(score) = parse_score(&pair[0]) else {
                return Outcome::error("ERR value is not a valid float");
            };
            let old = existing
                .as_ref()
                .and_then(|zset| zset.scores.get(&pair[1]).copied());
            let new_score = if incr {
                match old {
                    Some(old) => old + score,
                    None => score,
                }
            } else {
                score
            };
            if new_score.is_nan() {
                return Outcome::error("ERR value is not a valid float");
            }
            requested.push((pair[1].clone(), old, new_score));
        }

        let mvcc = self.clock.next(self.now_ms());
        let zset = match self.zset_for_write(db_index, &args[1], mvcc) {
            Ok(zset) => zset,
            Err(outcome) => return outcome,
        };
        let mut added = 0i64;
        let mut changed = 0i64;
        let mut incr_result = None;
        for (member, old, new_score) in requested {
            match old {
                Some(old) => {
                    if nx {
                        continue;
                    }
                    if (gt && new_score <= old) || (lt && new_score >= old) {
                        continue;
                    }
                    if new_score != old {
                        zset.scores.insert(member, new_score);
                        changed += 1;
                    }
                    if incr {
                        incr_result = Some(new_score);
                    }
                }
                None => {
                    if xx {
                        continue;
                    }
                    zset.scores.insert(member, new_score);
                    added += 1;
                    changed += 1;
                    if incr {
                        incr_result = Some(new_score);
                    }
                }
            }
        }
        let mut outcome = if incr {
            match incr_result {
                Some(score) => Outcome::reply(score_value(score, version)),
                None => Outcome::reply(RespValue::null()),
            }
        } else {
            Outcome::reply(RespValue::Integer(if ch { changed } else { added }))
        };
        if added > 0 || changed > 0 {
            outcome.wake.push((db_index, args[1].clone()));
        }
        outcome
    }

    fn cmd_zrange(
        &mut self,
        command: &str,
        args: &[Vec<u8>],
        db_index: usize,
        version: RespVersion,
        store: Option<Vec<u8>>,
    ) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let key_index = 1;
        let start_index = key_index + 1;
        let stop_index = key_index + 2;
        let source = &args[key_index];
        let mut with_scores = false;
        let mut by_score = command.contains("BYSCORE");
        let mut by_lex = command.contains("BYLEX");
        let mut rev = command.starts_with("ZREV");
        let legacy_rev = command == "ZREVRANGE";
        let legacy_score = matches!(command, "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE");
        let legacy_lex = matches!(command, "ZRANGEBYLEX" | "ZREVRANGEBYLEX");
        let mut limit: Option<(usize, usize)> = None;
        let mut index = stop_index + 1;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "WITHSCORES" => {
                    if legacy_lex || store.is_some() {
                        return Outcome::error("ERR syntax error");
                    }
                    with_scores = true;
                    index += 1;
                }
                "BYSCORE" => {
                    if legacy_rev || legacy_score || legacy_lex {
                        return Outcome::error("ERR syntax error");
                    }
                    by_score = true;
                    index += 1;
                }
                "BYLEX" => {
                    if legacy_rev || legacy_score || legacy_lex {
                        return Outcome::error("ERR syntax error");
                    }
                    by_lex = true;
                    index += 1;
                }
                "REV" => {
                    if legacy_rev || legacy_score || legacy_lex {
                        return Outcome::error("ERR syntax error");
                    }
                    rev = true;
                    index += 1;
                }
                "LIMIT" => {
                    if legacy_rev {
                        return Outcome::error("ERR syntax error");
                    }
                    let Some(offset) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Some(count) = args.get(index + 2) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let (Ok(offset), Ok(count)) = (parse_i64(offset), parse_i64(count)) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    if offset < 0 {
                        return Outcome::error("ERR offset is out of range");
                    }
                    limit = Some((offset as usize, count.max(0) as usize));
                    index += 3;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        if !by_score && !by_lex && with_scores && store.is_some() {
            return Outcome::error("ERR syntax error");
        }
        if by_lex && with_scores {
            return Outcome::error("ERR syntax error");
        }
        if !by_score && !by_lex && limit.is_some() {
            return Outcome::error("ERR syntax error");
        }
        if legacy_rev && (by_score || by_lex) {
            return Outcome::error("ERR syntax error");
        }
        // Validate the range arguments even when the source key is missing.
        if by_lex {
            let (min_arg, max_arg) = if rev {
                (&args[stop_index], &args[start_index])
            } else {
                (&args[start_index], &args[stop_index])
            };
            if parse_lex_bound(min_arg).is_none() || parse_lex_bound(max_arg).is_none() {
                return Outcome::error("ERR min or max not valid string range item");
            }
        } else if by_score {
            let (min_arg, max_arg) = if rev {
                (&args[stop_index], &args[start_index])
            } else {
                (&args[start_index], &args[stop_index])
            };
            if parse_score_bound(min_arg).is_none() || parse_score_bound(max_arg).is_none() {
                return Outcome::error("ERR min or max is not a float");
            }
        } else if parse_i64(&args[start_index]).is_err() || parse_i64(&args[stop_index]).is_err() {
            return Outcome::error("ERR value is not an integer or out of range");
        }
        let zset = match self.get_zset(db_index, source) {
            Ok(Some(zset)) => zset,
            Ok(None) => {
                if let Some(destination) = store {
                    self.dbs[db_index].remove(&destination);
                    return Outcome::reply(RespValue::Integer(0));
                }
                return Outcome::reply(RespValue::Array(Vec::new()));
            }
            Err(outcome) => return outcome,
        };
        let mut selected: Vec<(Vec<u8>, f64)>;
        if by_lex {
            let (min_arg, max_arg) = if rev {
                (&args[stop_index], &args[start_index])
            } else {
                (&args[start_index], &args[stop_index])
            };
            let Some(min) = parse_lex_bound(min_arg) else {
                return Outcome::error("ERR min or max not valid string range item");
            };
            let Some(max) = parse_lex_bound(max_arg) else {
                return Outcome::error("ERR min or max not valid string range item");
            };
            selected = zset
                .scores
                .iter()
                .filter(|(member, _)| lex_in_range(member, &min, &max))
                .map(|(member, score)| (member.clone(), *score))
                .collect();
            selected.sort_by(|a, b| a.0.cmp(&b.0));
        } else if by_score {
            let (min_arg, max_arg) = if rev {
                (&args[stop_index], &args[start_index])
            } else {
                (&args[start_index], &args[stop_index])
            };
            let Some(min) = parse_score_bound(min_arg) else {
                return Outcome::error("ERR min or max is not a float");
            };
            let Some(max) = parse_score_bound(max_arg) else {
                return Outcome::error("ERR min or max is not a float");
            };
            selected = sorted_members(&zset, false)
                .into_iter()
                .filter(|(_, score)| score_in_range(*score, min, max))
                .collect();
        } else {
            let (Ok(start), Ok(stop)) =
                (parse_i64(&args[start_index]), parse_i64(&args[stop_index]))
            else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            let members = sorted_members(&zset, rev);
            selected = match normalize_range(members.len(), start, stop) {
                Some((start, stop)) => members[start..=stop].to_vec(),
                None => Vec::new(),
            };
        }
        if by_lex || by_score {
            if rev {
                selected.reverse();
            }
            if let Some((offset, count)) = limit {
                selected = selected.into_iter().skip(offset).take(count).collect();
            }
        }
        if let Some(destination) = store {
            let mvcc = self.clock.next(self.now_ms());
            let length = selected.len() as i64;
            if selected.is_empty() {
                self.dbs[db_index].remove(&destination);
            } else {
                let mut zset = rekeydb_core::SortedSet::default();
                for (member, score) in selected {
                    zset.scores.insert(member, score);
                }
                self.dbs[db_index].insert(&destination, Value::ZSet(zset), mvcc);
            }
            let mut outcome = Outcome::reply(RespValue::Integer(length));
            outcome.wake.push((db_index, destination));
            outcome
        } else {
            let mut values = Vec::new();
            for (member, score) in selected {
                if with_scores && version == RespVersion::V3 {
                    values.push(RespValue::Array(vec![
                        RespValue::bulk(member),
                        score_value(score, version),
                    ]));
                } else {
                    values.push(RespValue::bulk(member));
                    if with_scores {
                        values.push(score_value(score, version));
                    }
                }
            }
            Outcome::reply(RespValue::Array(values))
        }
    }

    fn cmd_zrandmember(
        &mut self,
        args: &[Vec<u8>],
        db_index: usize,
        version: RespVersion,
    ) -> Outcome {
        if args.len() < 2 || args.len() > 4 {
            return wrong_args(args);
        }
        let with_scores = args
            .get(3)
            .is_some_and(|option| option.eq_ignore_ascii_case(b"WITHSCORES"));
        if args.len() == 4 && !with_scores {
            return Outcome::error("ERR syntax error");
        }
        let count = if args.len() >= 3 {
            match parse_i64(&args[2]) {
                Ok(value) => Some(value),
                Err(_) => return Outcome::error("ERR value is not an integer or out of range"),
            }
        } else {
            None
        };
        if count.is_some_and(|count| count.unsigned_abs() > 1_000_000) {
            return Outcome::error("ERR value is out of range");
        }
        let zset = match self.get_zset(db_index, &args[1]) {
            Ok(Some(zset)) => zset,
            Ok(None) => {
                return if count.is_none() {
                    Outcome::reply(RespValue::null())
                } else {
                    Outcome::reply(RespValue::Array(Vec::new()))
                };
            }
            Err(outcome) => return outcome,
        };
        let members = sorted_members(&zset, false);
        let Some(count) = count else {
            let seed = self.now_ms().unsigned_abs() ^ fnv1a(&args[1]);
            let (member, score) = &members[(seed as usize) % members.len()];
            return Outcome::reply(if with_scores {
                RespValue::Array(vec![
                    RespValue::bulk(member.clone()),
                    score_value(*score, version),
                ])
            } else {
                RespValue::bulk(member.clone())
            });
        };
        if count == 0 {
            return Outcome::reply(RespValue::Array(Vec::new()));
        }
        let take = count.unsigned_abs() as usize;
        let seed = self.now_ms().unsigned_abs() ^ fnv1a(&args[1]);
        let mut selected = Vec::new();
        if count > 0 {
            let take = take.min(members.len());
            let start = (seed as usize) % members.len();
            for offset in 0..take {
                selected.push(members[(start + offset) % members.len()].clone());
            }
        } else {
            for offset in 0..take {
                selected.push(members[(seed as usize + offset) % members.len()].clone());
            }
        }
        let mut values = Vec::new();
        for (member, score) in selected {
            if with_scores && version == RespVersion::V3 {
                values.push(RespValue::Array(vec![
                    RespValue::bulk(member),
                    score_value(score, version),
                ]));
            } else {
                values.push(RespValue::bulk(member));
                if with_scores {
                    values.push(score_value(score, version));
                }
            }
        }
        Outcome::reply(RespValue::Array(values))
    }

    fn cmd_zscan(&mut self, args: &[Vec<u8>], db_index: usize, version: RespVersion) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let Ok(cursor) = parse_usize(&args[2]) else {
            return Outcome::error("ERR invalid cursor");
        };
        let mut count = 10usize;
        let mut pattern: Option<&[u8]> = None;
        let mut index = 3;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "MATCH" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    pattern = Some(value);
                    index += 2;
                }
                "COUNT" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Ok(value) = parse_usize(value) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    count = value.max(1);
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let zset = match self.get_zset(db_index, &args[1]) {
            Ok(Some(zset)) => zset,
            Ok(None) => {
                return Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk(b"0".to_vec()),
                    RespValue::Array(Vec::new()),
                ]));
            }
            Err(outcome) => return outcome,
        };
        let members: Vec<(Vec<u8>, f64)> = sorted_members(&zset, false)
            .into_iter()
            .filter(|(member, _)| pattern.is_none_or(|pattern| glob_match(pattern, member)))
            .collect();
        let start = cursor.min(members.len());
        let end = start.saturating_add(count).min(members.len());
        let next = if end >= members.len() { 0 } else { end };
        let mut values = Vec::new();
        for (member, score) in &members[start..end] {
            values.push(RespValue::bulk(member.clone()));
            values.push(score_value(*score, version));
        }
        Outcome::reply(RespValue::Array(vec![
            RespValue::bulk(next.to_string().into_bytes()),
            RespValue::Array(values),
        ]))
    }

    fn cmd_zsetop(
        &mut self,
        command: &str,
        args: &[Vec<u8>],
        db_index: usize,
        version: RespVersion,
    ) -> Outcome {
        let store = command.ends_with("STORE");
        let base = if store { 2 } else { 1 };
        if args.len() <= base {
            return wrong_args(args);
        }
        let Ok(numkeys) = parse_usize(&args[base]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        if numkeys == 0 || args.len() < base + 1 + numkeys {
            return Outcome::error("ERR at least 1 input key is needed for this command");
        }
        let mut weights = vec![1.0f64; numkeys];
        let mut aggregate = "SUM".to_string();
        let mut with_scores = false;
        let mut index = base + 1 + numkeys;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "WEIGHTS" => {
                    if index + numkeys >= args.len() {
                        return Outcome::error("ERR syntax error");
                    }
                    for offset in 0..numkeys {
                        let Some(weight) = parse_score(&args[index + 1 + offset]) else {
                            return Outcome::error("ERR weight value is not a float");
                        };
                        weights[offset] = weight;
                    }
                    index += 1 + numkeys;
                }
                "AGGREGATE" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let value = String::from_utf8_lossy(value).to_ascii_uppercase();
                    if !matches!(value.as_str(), "SUM" | "MIN" | "MAX") {
                        return Outcome::error("ERR syntax error");
                    }
                    aggregate = value;
                    index += 2;
                }
                "WITHSCORES" => {
                    if store {
                        return Outcome::error("ERR syntax error");
                    }
                    with_scores = true;
                    index += 1;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let keys: Vec<&Vec<u8>> = args[base + 1..base + 1 + numkeys].iter().collect();
        let mut sets = Vec::new();
        for key in &keys {
            match self.get_zset_input(db_index, key) {
                Ok(zset) => sets.push(zset.unwrap_or_default()),
                Err(outcome) => return outcome,
            }
        }
        let operation = command.trim_end_matches("STORE");
        let mut result = std::collections::HashMap::new();
        if operation == "ZDIFF" {
            if let Some(first) = sets.first() {
                result = first.scores.clone();
                for set in &sets[1..] {
                    result.retain(|member, _| !set.scores.contains_key(member));
                }
            }
        } else {
            for (index, set) in sets.iter().enumerate() {
                for (member, score) in &set.scores {
                    let mut weighted = score * weights[index];
                    if weighted.is_nan() {
                        weighted = 0.0;
                    }
                    if operation == "ZUNION" {
                        result
                            .entry(member.clone())
                            .and_modify(|existing: &mut f64| {
                                *existing = aggregate_score(*existing, weighted, &aggregate);
                            })
                            .or_insert(weighted);
                    } else if index == 0 || result.contains_key(member) {
                        result
                            .entry(member.clone())
                            .and_modify(|existing: &mut f64| {
                                *existing = aggregate_score(*existing, weighted, &aggregate);
                            })
                            .or_insert(weighted);
                    }
                }
            }
            if operation == "ZINTER" {
                for set in &sets[1..] {
                    result.retain(|member, _| set.scores.contains_key(member));
                }
            }
        }
        if store {
            let destination = &args[1];
            let mvcc = self.clock.next(self.now_ms());
            let length = result.len() as i64;
            if result.is_empty() {
                self.dbs[db_index].remove(destination);
            } else {
                let zset = rekeydb_core::SortedSet { scores: result };
                self.dbs[db_index].insert(destination, Value::ZSet(zset), mvcc);
            }
            let mut outcome = Outcome::reply(RespValue::Integer(length));
            outcome.wake.push((db_index, destination.clone()));
            outcome
        } else {
            let zset = rekeydb_core::SortedSet { scores: result };
            let mut values = Vec::new();
            for (member, score) in sorted_members(&zset, false) {
                if with_scores && version == RespVersion::V3 {
                    values.push(RespValue::Array(vec![
                        RespValue::bulk(member),
                        score_value(score, version),
                    ]));
                } else {
                    values.push(RespValue::bulk(member));
                    if with_scores {
                        values.push(score_value(score, version));
                    }
                }
            }
            Outcome::reply(RespValue::Array(values))
        }
    }

    fn get_zset_input(
        &mut self,
        db_index: usize,
        key: &[u8],
    ) -> Result<Option<rekeydb_core::SortedSet>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_ref() {
                Value::ZSet(zset) => Ok(Some(zset.clone())),
                Value::Set(set) => {
                    let mut zset = rekeydb_core::SortedSet::default();
                    for member in set {
                        zset.scores.insert(member.clone(), 1.0);
                    }
                    Ok(Some(zset))
                }
                _ => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn get_zset(
        &mut self,
        db_index: usize,
        key: &[u8],
    ) -> Result<Option<rekeydb_core::SortedSet>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_zset() {
                Some(zset) => Ok(Some(zset.clone())),
                None => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn zset_for_write(
        &mut self,
        db_index: usize,
        key: &[u8],
        mvcc: u64,
    ) -> Result<&mut rekeydb_core::SortedSet, Outcome> {
        if !self.dbs[db_index].contains_key(key) {
            self.dbs[db_index].insert(key, Value::ZSet(rekeydb_core::SortedSet::default()), mvcc);
        }
        let Some(entry) = self.dbs[db_index].get_mut(key) else {
            return Err(wrong_type());
        };
        entry.mvcc = mvcc;
        let value = Arc::make_mut(&mut entry.value);
        match value {
            Value::ZSet(zset) => Ok(zset),
            _ => Err(wrong_type()),
        }
    }

    fn remove_zset_members(&mut self, db_index: usize, key: &[u8], members: &[Vec<u8>]) -> usize {
        let mvcc = self.clock.next(self.now_ms());
        let mut removed = 0;
        let mut empty = false;
        if let Some(entry) = self.dbs[db_index].get_mut(key) {
            entry.mvcc = mvcc;
            let value = Arc::make_mut(&mut entry.value);
            if let Value::ZSet(zset) = value {
                for member in members {
                    if zset.scores.remove(member).is_some() {
                        removed += 1;
                    }
                }
                empty = zset.scores.is_empty();
            }
        }
        if empty {
            self.dbs[db_index].remove(key);
        }
        removed
    }

    fn cmd_bzpop(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let timeout_ms = match parse_timeout(args.last().expect("timeout")) {
            Ok(timeout) => timeout,
            Err(outcome) => return outcome,
        };
        let max = command == "BZPOPMAX";
        let keys = &args[1..args.len() - 1];
        for key in keys {
            match self.get_zset(db_index, key) {
                Ok(Some(zset)) if !zset.scores.is_empty() => {
                    let members = sorted_members(&zset, max);
                    let (member, score) = &members[0];
                    self.remove_zset_members(db_index, key, std::slice::from_ref(member));
                    return Outcome::reply(RespValue::Array(vec![
                        RespValue::bulk(key.clone()),
                        RespValue::bulk(member.clone()),
                        RespValue::bulk(format_float(*score).into_bytes()),
                    ]));
                }
                Ok(_) => {}
                Err(outcome) => return outcome,
            }
        }
        Outcome::blocked(BlockRequest {
            db: db_index,
            keys: keys.to_vec(),
            kind: BlockKind::ZPop { max },
            source_side: ListSide::Left,
            destination: None,
            destination_side: ListSide::Left,
            timeout_ms,
        })
    }

    fn cmd_hash(
        &mut self,
        command: &str,
        args: &[Vec<u8>],
        db_index: usize,
        version: RespVersion,
    ) -> Outcome {
        match command {
            "HSET" | "HMSET" => {
                if args.len() < 4 || !args.len().is_multiple_of(2) {
                    return wrong_args(args);
                }
                let mvcc = self.clock.next(self.now_ms());
                let hash = match self.hash_for_write(db_index, &args[1], mvcc) {
                    Ok(hash) => hash,
                    Err(outcome) => return outcome,
                };
                let mut created = 0;
                for pair in args[2..].chunks(2) {
                    if hash.insert(pair[0].clone(), pair[1].clone()).is_none() {
                        created += 1;
                    }
                }
                if command == "HMSET" {
                    Outcome::ok()
                } else {
                    Outcome::reply(RespValue::Integer(created))
                }
            }
            "HSETNX" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let mvcc = self.clock.next(self.now_ms());
                let hash = match self.hash_for_write(db_index, &args[1], mvcc) {
                    Ok(hash) => hash,
                    Err(outcome) => return outcome,
                };
                if hash.contains_key(&args[2]) {
                    Outcome::reply(RespValue::Integer(0))
                } else {
                    hash.insert(args[2].clone(), args[3].clone());
                    Outcome::reply(RespValue::Integer(1))
                }
            }
            "HGET" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => match hash.get(&args[2]) {
                        Some(value) => Outcome::reply(RespValue::bulk(value.clone())),
                        None => Outcome::reply(RespValue::null()),
                    },
                    Ok(None) => Outcome::reply(RespValue::null()),
                    Err(outcome) => outcome,
                }
            }
            "HMGET" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => Outcome::reply(RespValue::Array(
                        args[2..]
                            .iter()
                            .map(|field| match hash.get(field) {
                                Some(value) => RespValue::bulk(value.clone()),
                                None => RespValue::null(),
                            })
                            .collect(),
                    )),
                    Ok(None) => Outcome::reply(RespValue::Array(
                        args[2..].iter().map(|_| RespValue::null()).collect(),
                    )),
                    Err(outcome) => outcome,
                }
            }
            "HDEL" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                match self.get_hash(db_index, &args[1]) {
                    Ok(Some(_)) => Outcome::reply(RespValue::Integer(self.remove_hash_fields(
                        db_index,
                        &args[1],
                        &args[2..],
                    ) as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "HLEN" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => Outcome::reply(RespValue::Integer(hash.len() as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "HSTRLEN" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => Outcome::reply(RespValue::Integer(
                        hash.get(&args[2]).map_or(0, |value| value.len()) as i64,
                    )),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "HKEYS" | "HVALS" | "HGETALL" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                let hash = match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => hash,
                    Ok(None) => return Outcome::reply(RespValue::Array(Vec::new())),
                    Err(outcome) => return outcome,
                };
                let mut values = Vec::new();
                for field in hash.keys() {
                    let value = hash.get(field).expect("field exists");
                    if command == "HGETALL" {
                        values.push(RespValue::bulk(field.clone()));
                        values.push(RespValue::bulk(value.clone()));
                    } else if command == "HKEYS" {
                        values.push(RespValue::bulk(field.clone()));
                    } else {
                        values.push(RespValue::bulk(value.clone()));
                    }
                }
                Outcome::reply(RespValue::Array(values))
            }
            "HEXISTS" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => {
                        Outcome::reply(RespValue::Integer(i64::from(hash.contains_key(&args[2]))))
                    }
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "HINCRBY" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Ok(increment) = parse_i64(&args[3]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                let current = match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => match hash.get(&args[2]) {
                        Some(value) => match parse_i64(value) {
                            Ok(value) => value,
                            Err(_) => {
                                return Outcome::error("ERR hash value is not an integer");
                            }
                        },
                        None => 0,
                    },
                    Ok(None) => 0,
                    Err(outcome) => return outcome,
                };
                let Some(value) = current.checked_add(increment) else {
                    return Outcome::error("ERR increment or decrement would overflow");
                };
                let mvcc = self.clock.next(self.now_ms());
                match self.hash_for_write(db_index, &args[1], mvcc) {
                    Ok(hash) => {
                        hash.insert(args[2].clone(), value.to_string().into_bytes());
                        Outcome::reply(RespValue::Integer(value))
                    }
                    Err(outcome) => outcome,
                }
            }
            "HINCRBYFLOAT" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let Some(increment) = std::str::from_utf8(&args[3])
                    .ok()
                    .and_then(|text| text.parse::<f64>().ok())
                else {
                    return Outcome::error("ERR value is not a valid float");
                };
                if !increment.is_finite() {
                    return Outcome::error("ERR value is NaN or Infinity");
                }
                let current = match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => match hash.get(&args[2]) {
                        Some(value) => match std::str::from_utf8(value)
                            .ok()
                            .and_then(|text| text.parse::<f64>().ok())
                        {
                            Some(value) => value,
                            None => {
                                return Outcome::error("ERR hash value is not a float");
                            }
                        },
                        None => 0.0,
                    },
                    Ok(None) => 0.0,
                    Err(outcome) => return outcome,
                };
                let value = current + increment;
                if !value.is_finite() {
                    return Outcome::error("ERR increment would produce NaN or Infinity");
                }
                let text = format_float(value);
                let mvcc = self.clock.next(self.now_ms());
                match self.hash_for_write(db_index, &args[1], mvcc) {
                    Ok(hash) => {
                        hash.insert(args[2].clone(), text.clone().into_bytes());
                        Outcome::reply(RespValue::bulk(text.into_bytes()))
                    }
                    Err(outcome) => outcome,
                }
            }
            "HRANDFIELD" => {
                if args.len() < 2 || args.len() > 4 {
                    return wrong_args(args);
                }
                let with_values = args
                    .get(3)
                    .is_some_and(|option| option.eq_ignore_ascii_case(b"WITHVALUES"));
                if args.len() == 4 && !with_values {
                    return Outcome::error("ERR syntax error");
                }
                let count = if args.len() >= 3 {
                    match parse_i64(&args[2]) {
                        Ok(value) => Some(value),
                        Err(_) => {
                            return Outcome::error("ERR value is not an integer or out of range");
                        }
                    }
                } else {
                    None
                };
                if count.is_some_and(|count| count.unsigned_abs() > 1_000_000) {
                    return Outcome::error("ERR value is out of range");
                }
                let hash = match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => hash,
                    Ok(None) => {
                        return if count.is_none() {
                            Outcome::reply(RespValue::null())
                        } else {
                            Outcome::reply(RespValue::Array(Vec::new()))
                        };
                    }
                    Err(outcome) => return outcome,
                };
                let fields: Vec<Vec<u8>> = hash.keys().cloned().collect();
                let seed = self.now_ms().unsigned_abs() ^ fnv1a(&args[1]);
                let Some(count) = count else {
                    let field = &fields[(seed as usize) % fields.len()];
                    return Outcome::reply(RespValue::bulk(field.clone()));
                };
                if count == 0 {
                    return Outcome::reply(RespValue::Array(Vec::new()));
                }
                let take = count.unsigned_abs() as usize;
                let mut selected = Vec::with_capacity(take);
                let unique = count > 0;
                if unique {
                    let take = take.min(fields.len());
                    let start = (seed as usize) % fields.len();
                    for offset in 0..take {
                        selected.push(fields[(start + offset) % fields.len()].clone());
                    }
                } else {
                    for offset in 0..take {
                        selected.push(fields[(seed as usize + offset) % fields.len()].clone());
                    }
                }
                if with_values {
                    let mut values = Vec::new();
                    for field in selected {
                        let value = hash.get(&field).expect("field exists").clone();
                        if version == RespVersion::V3 {
                            values.push(RespValue::Array(vec![
                                RespValue::bulk(field),
                                RespValue::bulk(value),
                            ]));
                        } else {
                            values.push(RespValue::bulk(field));
                            values.push(RespValue::bulk(value));
                        }
                    }
                    Outcome::reply(RespValue::Array(values))
                } else {
                    Outcome::reply(RespValue::Array(
                        selected.into_iter().map(RespValue::bulk).collect(),
                    ))
                }
            }
            "HSCAN" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                let Ok(cursor) = parse_usize(&args[2]) else {
                    return Outcome::error("ERR invalid cursor");
                };
                let mut count = 10usize;
                let mut pattern: Option<&[u8]> = None;
                let mut index = 3;
                while index < args.len() {
                    let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
                    match option.as_str() {
                        "MATCH" => {
                            let Some(value) = args.get(index + 1) else {
                                return Outcome::error("ERR syntax error");
                            };
                            pattern = Some(value);
                            index += 2;
                        }
                        "COUNT" => {
                            let Some(value) = args.get(index + 1) else {
                                return Outcome::error("ERR syntax error");
                            };
                            let Ok(value) = parse_usize(value) else {
                                return Outcome::error(
                                    "ERR value is not an integer or out of range",
                                );
                            };
                            count = value.max(1);
                            index += 2;
                        }
                        _ => return Outcome::error("ERR syntax error"),
                    }
                }
                let hash = match self.get_hash(db_index, &args[1]) {
                    Ok(Some(hash)) => hash,
                    Ok(None) => {
                        return Outcome::reply(RespValue::Array(vec![
                            RespValue::bulk(b"0".to_vec()),
                            RespValue::Array(Vec::new()),
                        ]));
                    }
                    Err(outcome) => return outcome,
                };
                let fields: Vec<Vec<u8>> = hash
                    .keys()
                    .filter(|field| pattern.is_none_or(|pattern| glob_match(pattern, field)))
                    .cloned()
                    .collect();
                let start = cursor.min(fields.len());
                let end = start.saturating_add(count).min(fields.len());
                let next = if end >= fields.len() { 0 } else { end };
                let mut values = Vec::new();
                for field in &fields[start..end] {
                    values.push(RespValue::bulk(field.clone()));
                    values.push(RespValue::bulk(
                        hash.get(field).expect("field exists").clone(),
                    ));
                }
                Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk(next.to_string().into_bytes()),
                    RespValue::Array(values),
                ]))
            }
            _ => unreachable!(),
        }
    }

    fn get_hash(&mut self, db_index: usize, key: &[u8]) -> Result<Option<HashValue>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_hash() {
                Some(hash) => Ok(Some(hash.clone())),
                None => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn hash_for_write(
        &mut self,
        db_index: usize,
        key: &[u8],
        mvcc: u64,
    ) -> Result<&mut HashValue, Outcome> {
        if !self.dbs[db_index].contains_key(key) {
            self.dbs[db_index].insert(key, Value::Hash(HashValue::new()), mvcc);
        }
        let Some(entry) = self.dbs[db_index].get_mut(key) else {
            return Err(wrong_type());
        };
        entry.mvcc = mvcc;
        let value = Arc::make_mut(&mut entry.value);
        match value {
            Value::Hash(hash) => Ok(hash),
            _ => Err(wrong_type()),
        }
    }

    fn remove_hash_fields(&mut self, db_index: usize, key: &[u8], fields: &[Vec<u8>]) -> usize {
        let mvcc = self.clock.next(self.now_ms());
        let mut removed = 0;
        let mut empty = false;
        if let Some(entry) = self.dbs[db_index].get_mut(key) {
            entry.mvcc = mvcc;
            let value = Arc::make_mut(&mut entry.value);
            if let Value::Hash(hash) = value {
                for field in fields {
                    if hash.shift_remove(field).is_some() {
                        removed += 1;
                    }
                }
                empty = hash.is_empty();
            }
        }
        if empty {
            self.dbs[db_index].remove(key);
        }
        removed
    }

    fn cmd_set_command(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        match command {
            "SADD" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                let mvcc = self.clock.next(self.now_ms());
                let set = match self.set_for_write(db_index, &args[1], mvcc) {
                    Ok(set) => set,
                    Err(outcome) => return outcome,
                };
                let mut added = 0;
                for member in &args[2..] {
                    if set.insert(member.clone()) {
                        added += 1;
                    }
                }
                Outcome::reply(RespValue::Integer(added))
            }
            "SREM" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                match self.get_set(db_index, &args[1]) {
                    Ok(Some(_)) => Outcome::reply(RespValue::Integer(self.remove_set_members(
                        db_index,
                        &args[1],
                        &args[2..],
                    ) as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "SCARD" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => Outcome::reply(RespValue::Integer(set.len() as i64)),
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "SISMEMBER" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => {
                        Outcome::reply(RespValue::Integer(i64::from(set.contains(&args[2]))))
                    }
                    Ok(None) => Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => outcome,
                }
            }
            "SMISMEMBER" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => {
                        let values = args[2..]
                            .iter()
                            .map(|member| RespValue::Integer(i64::from(set.contains(member))))
                            .collect();
                        Outcome::reply(RespValue::Array(values))
                    }
                    Ok(None) => Outcome::reply(RespValue::Array(
                        args[2..].iter().map(|_| RespValue::Integer(0)).collect(),
                    )),
                    Err(outcome) => outcome,
                }
            }
            "SMEMBERS" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => {
                        let mut members: Vec<Vec<u8>> = set.into_iter().collect();
                        members.sort();
                        Outcome::reply(RespValue::Array(
                            members.into_iter().map(RespValue::bulk).collect(),
                        ))
                    }
                    Ok(None) => Outcome::reply(RespValue::Array(Vec::new())),
                    Err(outcome) => outcome,
                }
            }
            "SRANDMEMBER" => {
                if args.len() < 2 || args.len() > 3 {
                    return wrong_args(args);
                }
                let count = if args.len() == 3 {
                    match parse_i64(&args[2]) {
                        Ok(value) => Some(value),
                        Err(_) => {
                            return Outcome::error("ERR value is not an integer or out of range");
                        }
                    }
                } else {
                    None
                };
                let set = match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => set,
                    Ok(None) => {
                        return if count.is_none() {
                            Outcome::reply(RespValue::NullArray)
                        } else {
                            Outcome::reply(RespValue::Array(Vec::new()))
                        };
                    }
                    Err(outcome) => return outcome,
                };
                let members: Vec<Vec<u8>> = set.into_iter().collect();
                if members.is_empty() {
                    return if count.is_none() {
                        Outcome::reply(RespValue::NullArray)
                    } else {
                        Outcome::reply(RespValue::Array(Vec::new()))
                    };
                }
                let seed = self.now_ms().unsigned_abs() ^ fnv1a(&args[1]);
                let Some(count) = count else {
                    let index = (seed as usize) % members.len();
                    return Outcome::reply(RespValue::bulk(members[index].clone()));
                };
                if count == 0 {
                    return Outcome::reply(RespValue::Array(Vec::new()));
                }
                let take = count.unsigned_abs() as usize;
                let unique = count > 0;
                let mut selected = Vec::new();
                if unique {
                    let take = take.min(members.len());
                    // A deterministic walk is enough for the bootstrap; real
                    // random sampling lands with the randomized command work.
                    let start = (seed as usize) % members.len();
                    for offset in 0..take {
                        selected.push(members[(start + offset) % members.len()].clone());
                    }
                } else {
                    for offset in 0..take {
                        selected.push(members[(seed as usize + offset) % members.len()].clone());
                    }
                }
                Outcome::reply(RespValue::Array(
                    selected.into_iter().map(RespValue::bulk).collect(),
                ))
            }
            "SPOP" => {
                if args.len() < 2 || args.len() > 3 {
                    return wrong_args(args);
                }
                let count = if args.len() == 3 {
                    match parse_i64(&args[2]) {
                        Ok(value) if value >= 0 => Some(value as usize),
                        _ => {
                            return Outcome::error("ERR value is out of range, must be positive");
                        }
                    }
                } else {
                    None
                };
                let set = match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => set,
                    Ok(None) => {
                        return if count.is_none() {
                            Outcome::reply(RespValue::null())
                        } else {
                            Outcome::reply(RespValue::Array(Vec::new()))
                        };
                    }
                    Err(outcome) => return outcome,
                };
                let mut members: Vec<Vec<u8>> = set.into_iter().collect();
                members.sort();
                let take = count.unwrap_or(1).min(members.len());
                let selected: Vec<Vec<u8>> = members.drain(..take).collect();
                if !selected.is_empty() {
                    self.remove_set_members(db_index, &args[1], &selected);
                }
                if count.is_some() {
                    Outcome::reply(RespValue::Array(
                        selected.into_iter().map(RespValue::bulk).collect(),
                    ))
                } else {
                    match selected.into_iter().next() {
                        Some(member) => Outcome::reply(RespValue::bulk(member)),
                        None => Outcome::reply(RespValue::null()),
                    }
                }
            }
            "SMOVE" => {
                if args.len() != 4 {
                    return wrong_args(args);
                }
                let source = match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => set,
                    Ok(None) => return Outcome::reply(RespValue::Integer(0)),
                    Err(outcome) => return outcome,
                };
                if !source.contains(&args[3]) {
                    return Outcome::reply(RespValue::Integer(0));
                }
                if args[1] == args[2] {
                    return Outcome::reply(RespValue::Integer(1));
                }
                if let Some(entry) = self.existing(db_index, &args[2])
                    && entry.value.as_set().is_none()
                {
                    return wrong_type();
                }
                self.remove_set_members(db_index, &args[1], std::slice::from_ref(&args[3]));
                let mvcc = self.clock.next(self.now_ms());
                if let Ok(destination) = self.set_for_write(db_index, &args[2], mvcc) {
                    destination.insert(args[3].clone());
                }
                Outcome::reply(RespValue::Integer(1))
            }
            "SINTER" | "SUNION" | "SDIFF" | "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" => {
                self.cmd_setop(command, args, db_index)
            }
            "SSCAN" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                let Ok(cursor) = parse_usize(&args[2]) else {
                    return Outcome::error("ERR invalid cursor");
                };
                let mut count = 10usize;
                let mut pattern: Option<&[u8]> = None;
                let mut index = 3;
                while index < args.len() {
                    let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
                    match option.as_str() {
                        "MATCH" => {
                            let Some(value) = args.get(index + 1) else {
                                return Outcome::error("ERR syntax error");
                            };
                            pattern = Some(value);
                            index += 2;
                        }
                        "COUNT" => {
                            let Some(value) = args.get(index + 1) else {
                                return Outcome::error("ERR syntax error");
                            };
                            let Ok(value) = parse_usize(value) else {
                                return Outcome::error(
                                    "ERR value is not an integer or out of range",
                                );
                            };
                            count = value.max(1);
                            index += 2;
                        }
                        _ => return Outcome::error("ERR syntax error"),
                    }
                }
                let set = match self.get_set(db_index, &args[1]) {
                    Ok(Some(set)) => set,
                    Ok(None) => {
                        return Outcome::reply(RespValue::Array(vec![
                            RespValue::bulk(b"0".to_vec()),
                            RespValue::Array(Vec::new()),
                        ]));
                    }
                    Err(outcome) => return outcome,
                };
                let mut members: Vec<Vec<u8>> = set
                    .into_iter()
                    .filter(|member| pattern.is_none_or(|pattern| glob_match(pattern, member)))
                    .collect();
                members.sort();
                let start = cursor.min(members.len());
                let end = start.saturating_add(count).min(members.len());
                let next = if end >= members.len() { 0 } else { end };
                let batch = members[start..end]
                    .iter()
                    .cloned()
                    .map(RespValue::bulk)
                    .collect();
                Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk(next.to_string().into_bytes()),
                    RespValue::Array(batch),
                ]))
            }
            _ => unreachable!(),
        }
    }

    fn cmd_setop(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        let store = command.ends_with("STORE");
        let first_key = if store { 2 } else { 1 };
        if args.len() <= first_key {
            return wrong_args(args);
        }
        let operation = command.trim_end_matches("STORE");
        let mut sets = Vec::new();
        for key in &args[first_key..] {
            match self.get_set(db_index, key) {
                Ok(Some(set)) => sets.push(set),
                Ok(None) => sets.push(HashSet::new()),
                Err(outcome) => return outcome,
            }
        }
        let mut result = sets.first().cloned().unwrap_or_default();
        match operation {
            "SINTER" => {
                for set in &sets[1..] {
                    result.retain(|member| set.contains(member));
                }
            }
            "SUNION" => {
                for set in &sets[1..] {
                    result.extend(set.iter().cloned());
                }
            }
            "SDIFF" => {
                for set in &sets[1..] {
                    result.retain(|member| !set.contains(member));
                }
            }
            _ => unreachable!(),
        }
        if store {
            let mvcc = self.clock.next(self.now_ms());
            let cardinality = result.len() as i64;
            if result.is_empty() {
                self.dbs[db_index].remove(&args[1]);
            } else {
                self.dbs[db_index].insert(&args[1], Value::Set(result), mvcc);
            }
            Outcome::reply(RespValue::Integer(cardinality))
        } else {
            let mut members: Vec<Vec<u8>> = result.into_iter().collect();
            members.sort();
            Outcome::reply(RespValue::Array(
                members.into_iter().map(RespValue::bulk).collect(),
            ))
        }
    }

    fn get_set(
        &mut self,
        db_index: usize,
        key: &[u8],
    ) -> Result<Option<HashSet<Vec<u8>>>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_set() {
                Some(set) => Ok(Some(set.clone())),
                None => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn set_for_write(
        &mut self,
        db_index: usize,
        key: &[u8],
        mvcc: u64,
    ) -> Result<&mut HashSet<Vec<u8>>, Outcome> {
        if !self.dbs[db_index].contains_key(key) {
            self.dbs[db_index].insert(key, Value::Set(HashSet::new()), mvcc);
        }
        let Some(entry) = self.dbs[db_index].get_mut(key) else {
            return Err(wrong_type());
        };
        entry.mvcc = mvcc;
        let value = Arc::make_mut(&mut entry.value);
        match value {
            Value::Set(set) => Ok(set),
            _ => Err(wrong_type()),
        }
    }

    fn remove_set_members(&mut self, db_index: usize, key: &[u8], members: &[Vec<u8>]) -> usize {
        let mut removed = 0;
        let mut empty = false;
        let mvcc = self.clock.next(self.now_ms());
        if let Some(entry) = self.dbs[db_index].get_mut(key) {
            entry.mvcc = mvcc;
            let value = Arc::make_mut(&mut entry.value);
            if let Value::Set(set) = value {
                for member in members {
                    if set.remove(member) {
                        removed += 1;
                    }
                }
                empty = set.is_empty();
            }
        }
        if empty {
            self.dbs[db_index].remove(key);
        }
        removed
    }

    fn cmd_object(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        match sub.as_str() {
            "ENCODING" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                self.expire_if_needed(db_index, &args[2]);
                match self.dbs[db_index].get(&args[2]) {
                    Some(entry) => Outcome::reply(RespValue::bulk(
                        entry
                            .value
                            .encoding(self.encoding_limits)
                            .as_bytes()
                            .to_vec(),
                    )),
                    None => Outcome::error("ERR no such key"),
                }
            }
            "REFCOUNT" | "IDLETIME" | "FREQ" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                self.expire_if_needed(db_index, &args[2]);
                if self.dbs[db_index].contains_key(&args[2]) {
                    Outcome::reply(RespValue::Integer(if sub == "REFCOUNT" { 1 } else { 0 }))
                } else {
                    Outcome::error("ERR no such key")
                }
            }
            "HELP" => Outcome::reply(RespValue::Array(vec![
                RespValue::bulk(
                    b"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_vec(),
                ),
                RespValue::bulk(b"ENCODING <key>".to_vec()),
                RespValue::bulk(b"FREQ <key>".to_vec()),
                RespValue::bulk(b"IDLETIME <key>".to_vec()),
                RespValue::bulk(b"REFCOUNT <key>".to_vec()),
            ])),
            _ => Outcome::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'. Try OBJECT HELP.",
                String::from_utf8_lossy(&args[1])
            )),
        }
    }

    fn cmd_rename(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        self.expire_if_needed(db_index, &args[1]);
        self.expire_if_needed(db_index, &args[2]);
        let Some(entry) = self.dbs[db_index].get(&args[1]).cloned() else {
            return Outcome::error("ERR no such key");
        };
        if args[1] == args[2] {
            return if command == "RENAMENX" {
                Outcome::reply(RespValue::Integer(0))
            } else {
                Outcome::ok()
            };
        }
        let destination_exists = self.dbs[db_index].contains_key(&args[2]);
        if command == "RENAMENX" && destination_exists {
            return Outcome::reply(RespValue::Integer(0));
        }
        self.dbs[db_index].remove(&args[1]);
        let mvcc = self.clock.next(self.now_ms());
        let mut entry = entry;
        entry.mvcc = mvcc;
        self.dbs[db_index].insert_entry(args[2].clone(), entry);
        let mut outcome = if command == "RENAMENX" {
            Outcome::reply(RespValue::Integer(1))
        } else {
            Outcome::ok()
        };
        outcome.wake.push((db_index, args[2].clone()));
        outcome
    }

    fn cmd_dump(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 2 {
            return wrong_args(args);
        }
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::null());
        };
        match dump(&entry.value) {
            Ok(payload) => Outcome::reply(RespValue::bulk(payload)),
            Err(err) => Outcome::error(format!("ERR {err}")),
        }
    }

    fn cmd_restore(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let Ok(ttl) = parse_i64(&args[2]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        if ttl < 0 {
            return Outcome::error("ERR Invalid TTL value, must be >= 0");
        }
        let mut replace = false;
        let mut absttl = false;
        let mut index = 4;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "REPLACE" => {
                    replace = true;
                    index += 1;
                }
                "ABSTTL" => {
                    absttl = true;
                    index += 1;
                }
                "IDLETIME" | "FREQ" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    if parse_i64(value).is_err() {
                        return Outcome::error("ERR value is not an integer or out of range");
                    }
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        self.expire_if_needed(db_index, &args[1]);
        if self.dbs[db_index].contains_key(&args[1]) && !replace {
            return Outcome::error("BUSYKEY Target key name already exists.");
        }
        let value = match restore(&args[3]) {
            Ok(value) => value,
            Err(_) => {
                return Outcome::error("ERR DUMP payload version or checksum are wrong");
            }
        };
        let expire = if ttl == 0 {
            ExpireEntry::new()
        } else {
            let when = if absttl {
                ttl
            } else {
                self.now_ms().saturating_add(ttl)
            };
            let mut expire = ExpireEntry::new();
            expire.set_primary(when);
            expire
        };
        let mvcc = self.clock.next(self.now_ms());
        self.dbs[db_index].insert_entry(
            args[1].clone(),
            Entry::with_expire(Arc::new(value), expire, mvcc),
        );
        Outcome::ok()
    }

    fn cmd_sort(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let mut alpha = false;
        let mut desc = false;
        let mut store: Option<Vec<u8>> = None;
        let mut offset = 0usize;
        let mut count: Option<usize> = None;
        let mut index = 2;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "ALPHA" => {
                    alpha = true;
                    index += 1;
                }
                "ASC" => {
                    desc = false;
                    index += 1;
                }
                "DESC" => {
                    desc = true;
                    index += 1;
                }
                "LIMIT" => {
                    let Some(offset_arg) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Some(count_arg) = args.get(index + 2) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let (Ok(parsed_offset), Ok(parsed_count)) =
                        (parse_i64(offset_arg), parse_i64(count_arg))
                    else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    if parsed_offset < 0 {
                        return Outcome::error("ERR syntax error");
                    }
                    offset = parsed_offset as usize;
                    count = if parsed_count < 0 {
                        None
                    } else {
                        Some(parsed_count as usize)
                    };
                    index += 3;
                }
                "STORE" => {
                    let Some(destination) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    store = Some(destination.clone());
                    index += 2;
                }
                "BY" | "GET" => {
                    return Outcome::error("ERR syntax error");
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }

        let mut values: Vec<Vec<u8>> = match self.dbs[db_index].get(&args[1]) {
            Some(entry) => match entry.value.as_ref() {
                Value::List(list) => list.iter().cloned().collect(),
                Value::Set(set) => set.iter().cloned().collect(),
                Value::ZSet(zset) => zset.scores.keys().cloned().collect(),
                _ => return wrong_type(),
            },
            None => Vec::new(),
        };

        if alpha {
            values.sort();
        } else {
            let mut scored = Vec::with_capacity(values.len());
            for value in values {
                let Ok(text) = std::str::from_utf8(&value) else {
                    return Outcome::error("ERR One or more scores can't be converted into double");
                };
                let Ok(score) = text.parse::<f64>() else {
                    return Outcome::error("ERR One or more scores can't be converted into double");
                };
                scored.push((score, value));
            }
            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            values = scored.into_iter().map(|(_, value)| value).collect();
        }
        if desc {
            values.reverse();
        }
        let values: Vec<Vec<u8>> = values
            .into_iter()
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
            .collect();

        if let Some(destination) = store {
            let mvcc = self.clock.next(self.now_ms());
            let length = values.len() as i64;
            if values.is_empty() {
                self.dbs[db_index].remove(&destination);
            } else {
                self.dbs[db_index].insert(
                    &destination,
                    Value::List(values.into_iter().collect()),
                    mvcc,
                );
            }
            let mut outcome = Outcome::reply(RespValue::Integer(length));
            outcome.wake.push((db_index, destination));
            outcome
        } else {
            Outcome::reply(RespValue::Array(
                values.into_iter().map(RespValue::bulk).collect(),
            ))
        }
    }

    fn cmd_config(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let pattern = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
                let mut values = Vec::new();
                for (key, value) in &self.config_values {
                    if glob_match(pattern.as_bytes(), key.as_bytes()) {
                        values.push(RespValue::bulk(key.clone().into_bytes()));
                        values.push(RespValue::bulk(value.clone().into_bytes()));
                    }
                }
                Outcome::reply(RespValue::Array(values))
            }
            "SET" => {
                if args.len() < 4 || !args.len().is_multiple_of(2) {
                    return wrong_args(args);
                }
                for pair in args[2..].chunks(2) {
                    let key = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                    let value = String::from_utf8_lossy(&pair[1]).to_string();
                    if !is_modifiable_config(&key) {
                        return Outcome::error(format!("ERR Unsupported CONFIG parameter: {key}"));
                    }
                    if key == "requirepass" {
                        self.config.requirepass = if value.is_empty() {
                            None
                        } else {
                            Some(value.clone())
                        };
                    }
                    if (key == "maxmemory"
                        || key == "server-threads"
                        || key == "timeout"
                        || key == "hz")
                        && value.parse::<u64>().is_err()
                    {
                        return Outcome::error("ERR Invalid argument");
                    }
                    if let Ok(parsed) = value.parse::<usize>() {
                        match key.as_str() {
                            "set-max-intset-entries" => {
                                self.encoding_limits.set_max_intset_entries = parsed;
                            }
                            "hash-max-ziplist-entries" => {
                                self.encoding_limits.hash_max_ziplist_entries = parsed;
                            }
                            "hash-max-ziplist-value" => {
                                self.encoding_limits.hash_max_ziplist_value = parsed;
                            }
                            "zset-max-ziplist-entries" => {
                                self.encoding_limits.zset_max_ziplist_entries = parsed;
                            }
                            "zset-max-ziplist-value" => {
                                self.encoding_limits.zset_max_ziplist_value = parsed;
                            }
                            _ => {}
                        }
                    }
                    self.config_values.insert(key, value);
                }
                Outcome::ok()
            }
            "RESETSTAT" => {
                self.stats = Stats::default();
                Outcome::ok()
            }
            "REWRITE" => {
                if self.config.config_file.is_some() {
                    Outcome::ok()
                } else {
                    Outcome::error("ERR The server is running without a config file")
                }
            }
            _ => Outcome::error(format!(
                "ERR Unknown CONFIG subcommand or wrong number of arguments for '{}'",
                String::from_utf8_lossy(&args[1])
            )),
        }
    }

    fn cmd_info(&self, args: &[Vec<u8>]) -> Outcome {
        if args.len() > 2 {
            return wrong_args(args);
        }
        let section = args
            .get(1)
            .map(|value| String::from_utf8_lossy(value).to_ascii_uppercase())
            .unwrap_or_else(|| "DEFAULT".to_string());
        let all = matches!(section.as_str(), "DEFAULT" | "ALL");
        let mut out = String::new();
        if all || section == "SERVER" {
            let uptime = self.start.elapsed().as_secs();
            let _ = write!(
                out,
                "# Server\r\nredis_version:6.3.0\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:0\r\nkeydb_version:6.3.0\r\nkeydb_git_sha1:00000000\r\nkeydb_git_dirty:0\r\nkeydb_build_id:0\r\nredis_mode:standalone\r\nos:Linux\r\narch_bits:64\r\nmultiplexing_api:epoll\r\natomicvar_api:atomic-builtin\r\ngcc_version:0.0.0\r\nprocess_id:{}\r\nprocess_supervised:no\r\nrun_id:{}\r\ntcp_port:{}\r\nserver_time_usec:{}\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\nhz:10\r\nconfigured_hz:10\r\nlru_clock:{}\r\nexecutable:{}\r\nconfig_file:{}\r\nio_threads_active:0\r\n\r\n",
                std::process::id(),
                "0".repeat(40),
                self.config.port,
                self.now_ms() * 1000,
                uptime,
                uptime / 86400,
                (self.now_ms() / 1000) & ((1 << 24) - 1),
                std::env::current_exe()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                self.config
                    .config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
            );
        }
        if all || section == "CLIENTS" {
            let _ = write!(
                out,
                "# Clients\r\nconnected_clients:{}\r\ncluster_connections:0\r\nmaxclients:10000\r\nclient_recent_max_input_buffer:0\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:{}\r\ntracking_clients:0\r\npubsub_clients:0\r\nwatching_clients:0\r\nclients_in_timeout_table:0\r\ntotal_watched_keys:0\r\ntotal_blocking_keys:0\r\ntotal_blocking_keys_on_nokey:0\r\n\r\n",
                self.connected_clients, self.blocked_clients
            );
        }
        if all || section == "MEMORY" {
            let used = self.used_memory();
            let _ = write!(
                out,
                "# Memory\r\nused_memory:{}\r\nused_memory_human:{:.2}M\r\nused_memory_rss:{}\r\nused_memory_peak:{}\r\nused_memory_peak_human:{:.2}M\r\nused_memory_lua:0\r\nused_memory_scripts:0\r\nnumber_of_cached_scripts:0\r\nmaxmemory:0\r\nmaxmemory_human:0B\r\nmaxmemory_policy:noeviction\r\nmem_fragmentation_ratio:1.00\r\nmem_allocator:jemalloc-0.0.0\r\nactive_defrag_running:0\r\nlazyfree_pending_objects:0\r\nlazyfreed_objects:0\r\n\r\n",
                used,
                used as f64 / (1024.0 * 1024.0),
                used,
                used,
                used as f64 / (1024.0 * 1024.0),
            );
        }
        if all || section == "PERSISTENCE" {
            let _ = write!(
                out,
                "# Persistence\r\nloading:0\r\nasync_loading:0\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\nrdb_changes_since_last_save:0\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:-1\r\nrdb_current_bgsave_time_sec:-1\r\nrdb_saves:0\r\nrdb_last_cow_size:0\r\naof_enabled:0\r\naof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:ok\r\naof_rewrites:0\r\naof_rewrites_consecutive_failures:0\r\naof_last_write_status:ok\r\naof_last_cow_size:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\n\r\n",
                self.start_unix
            );
        }
        if all || section == "STATS" {
            let _ = write!(
                out,
                "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nexpired_stale_perc:0.00\r\nexpired_time_cap_reached_count:0\r\nexpire_cycle_cpu_milliseconds:0\r\nevicted_keys:0\r\nevicted_clients:0\r\ntotal_eviction_exceeded_time:0\r\ncurrent_eviction_exceeded_time:0\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\npubsub_channels:0\r\npubsub_patterns:0\r\npubsubshard_channels:0\r\nlatest_fork_usec:0\r\ntotal_forks:0\r\nmigrate_cached_sockets:0\r\nslave_expires_tracked_keys:0\r\nactive_defrag_hits:0\r\nactive_defrag_misses:0\r\nactive_defrag_key_hits:0\r\nactive_defrag_key_misses:0\r\ntotal_active_defrag_time:0\r\ncurrent_active_defrag_time:0\r\ntracking_total_keys:0\r\ntracking_total_items:0\r\ntracking_total_prefixes:0\r\nunexpected_error_replies:0\r\ntotal_error_replies:0\r\ndump_payload_sanitizations:0\r\ntotal_reads_processed:0\r\ntotal_writes_processed:0\r\nio_threaded_reads_processed:0\r\nio_threaded_writes_processed:0\r\n\r\n",
                self.stats.total_connections_received,
                self.stats.total_commands_processed,
                self.stats.total_net_input_bytes,
                self.stats.total_net_output_bytes,
                self.stats.keyspace_hits,
                self.stats.keyspace_misses,
            );
        }
        if all || section == "REPLICATION" {
            let _ = write!(
                out,
                "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n\r\n",
                "0".repeat(40),
                "0".repeat(40)
            );
        }
        if all || section == "CPU" {
            let _ = write!(
                out,
                "# CPU\r\nused_cpu_sys:0.000000\r\nused_cpu_user:0.000000\r\nused_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.000000\r\nused_cpu_sys_main_thread:0.000000\r\nused_cpu_user_main_thread:0.000000\r\n\r\n"
            );
        }
        if all || section == "KEYSPACE" {
            let _ = writeln!(out, "# Keyspace");
            let now = self.now_ms();
            for (index, db) in self.dbs.iter().enumerate() {
                let entries = db.collect_visible();
                if entries.is_empty() {
                    continue;
                }
                let expires = entries
                    .iter()
                    .filter(|(_, entry)| entry.expire.primary().is_some_and(|when| when > now))
                    .count();
                let _ = write!(
                    out,
                    "db{}:keys={},expires={},avg_ttl=0\r\n",
                    index,
                    entries.len(),
                    expires
                );
            }
            out.push_str("\r\n");
        }
        Outcome::reply(RespValue::bulk(out.into_bytes()))
    }

    fn used_memory(&self) -> usize {
        self.dbs
            .iter()
            .flat_map(|db| db.collect_visible())
            .map(|(key, entry)| key.len() + entry.value.memory_usage() + 64)
            .sum()
    }

    fn cmd_command(&self, args: &[Vec<u8>]) -> Outcome {
        if args.len() == 1 {
            return Outcome::reply(RespValue::Array(Vec::new()));
        }
        let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        match sub.as_str() {
            "COUNT" => Outcome::reply(RespValue::Integer(236)),
            "INFO" => {
                // Command metadata is generated from spec/commands.json and
                // will replace this placeholder in M1.
                let mut values = Vec::new();
                for _ in args.iter().skip(2) {
                    values.push(RespValue::Null);
                }
                Outcome::reply(RespValue::Array(values))
            }
            "GETKEYS" => Outcome::reply(RespValue::Array(Vec::new())),
            "DOCS" => Outcome::reply(RespValue::Array(Vec::new())),
            _ => wrong_args(args),
        }
    }

    fn cmd_client(&mut self, args: &[Vec<u8>], client: &mut ClientContext) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        match sub.as_str() {
            "ID" => {
                if args.len() != 2 {
                    return client_error(&sub);
                }
                Outcome::reply(RespValue::Integer(client.id as i64))
            }
            "SETNAME" => {
                if args.len() != 3 {
                    return client_error(&sub);
                }
                if args[2].iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
                    return Outcome::error(
                        "ERR Client names cannot contain spaces, newlines or special characters.",
                    );
                }
                client.name = args[2].clone();
                Outcome::ok()
            }
            "GETNAME" => {
                if args.len() != 2 {
                    return client_error(&sub);
                }
                if client.name.is_empty() {
                    Outcome::reply(RespValue::null())
                } else {
                    Outcome::reply(RespValue::bulk(client.name.clone()))
                }
            }
            "LIST" | "INFO" => {
                if args.len() > 2 {
                    return client_error(&sub);
                }
                let line = format!(
                    "id={} addr={} laddr=127.0.0.1:{} fd=-1 name={} age={} idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-net-in=0 tot-net-out=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client|{} user=default redir=-1 resp={}\n",
                    client.id,
                    client.addr,
                    self.config.port,
                    String::from_utf8_lossy(&client.name),
                    self.start.elapsed().as_secs(),
                    client.db,
                    sub.to_ascii_lowercase(),
                    if client.version == RespVersion::V3 {
                        3
                    } else {
                        2
                    },
                );
                Outcome::reply(RespValue::bulk(line.into_bytes()))
            }
            "UNBLOCK" => {
                if args.len() < 3 || args.len() > 4 {
                    return client_error(&sub);
                }
                let Ok(id) = parse_u64(&args[2]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                let as_error = args
                    .get(3)
                    .is_some_and(|mode| mode.eq_ignore_ascii_case(b"ERROR"));
                Outcome {
                    reply: RespValue::Integer(1),
                    close: false,
                    shutdown: false,
                    block: None,
                    wake: Vec::new(),
                    unblock: Some((id, as_error)),
                }
            }
            "SETINFO" => Outcome::ok(),
            _ => Outcome::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.",
                String::from_utf8_lossy(&args[1])
            )),
        }
    }

    fn rdb_path(&self) -> std::path::PathBuf {
        self.config.dir.join(&self.config.dbfilename)
    }

    fn save_rdb_to_disk(&mut self) -> Result<(), String> {
        let path = self.rdb_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
        let now = self.now_ms();
        let mut entries = Vec::new();
        for (db_index, db) in self.dbs.iter().enumerate() {
            for (key, entry) in db.collect_visible() {
                if entry.is_expired(now) {
                    continue;
                }
                let expire = entry.expire.primary();
                entries.push(RdbEntry {
                    db: db_index,
                    key,
                    value: (*entry.value).clone(),
                    expire_ms: expire,
                });
            }
        }
        let bytes = save(&entries).map_err(|err| err.to_string())?;
        std::fs::write(&path, bytes).map_err(|err| format!("{}: {err}", path.display()))
    }

    pub fn reload_rdb_from_disk(&mut self) -> Result<(), String> {
        let path = self.rdb_path();
        let bytes = std::fs::read(&path).map_err(|err| format!("{}: {err}", path.display()))?;
        let entries = load(&bytes).map_err(|err| err.to_string())?;
        for db in &mut self.dbs {
            db.clear();
        }
        let now = self.now_ms();
        for entry in entries {
            if entry.expire_ms.is_some_and(|expire| expire <= now) {
                continue;
            }
            let mut expire = ExpireEntry::new();
            if let Some(when) = entry.expire_ms {
                expire.set_primary(when);
            }
            let mvcc = self.clock.next(now);
            self.dbs[entry.db].insert_entry(
                entry.key,
                Entry::with_expire(Arc::new(entry.value), expire, mvcc),
            );
        }
        Ok(())
    }

    fn digest_entries(&self) -> Vec<(usize, Vec<u8>, Value, bool)> {
        let now = self.now_ms();
        let mut entries = Vec::new();
        for (db_index, db) in self.dbs.iter().enumerate() {
            for (key, entry) in db.collect_visible() {
                if entry.is_expired(now) {
                    continue;
                }
                entries.push((
                    db_index,
                    key,
                    (*entry.value).clone(),
                    entry.expire.primary().is_some(),
                ));
            }
        }
        entries
    }

    fn cmd_debug(&mut self, args: &[Vec<u8>], client: &mut ClientContext) -> Outcome {
        if args.len() < 2 {
            return Outcome::error("ERR wrong number of arguments for 'debug' command");
        }
        let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        match sub.as_str() {
            "MVCC" => {
                if args.len() < 3 {
                    return Outcome::error("ERR DEBUG MVCC requires a subcommand");
                }
                match String::from_utf8_lossy(&args[2])
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "SNAPSHOT" => {
                        let checkpoint = self.clock.current();
                        if self.debug_snapshots[client.db].is_none() {
                            let snapshot = self.dbs[client.db].create_snapshot(checkpoint);
                            self.debug_snapshots[client.db] = Some(snapshot);
                        }
                        let checkpoint = self.debug_snapshots[client.db]
                            .as_ref()
                            .map_or(0, |snapshot| snapshot.checkpoint());
                        Outcome::reply(RespValue::Integer(checkpoint as i64))
                    }
                    "END" => {
                        let Some(snapshot) = self.debug_snapshots[client.db].take() else {
                            return Outcome::ok();
                        };
                        match self.dbs[client.db].end_snapshot(snapshot) {
                            Ok(()) => Outcome::ok(),
                            Err(snapshot) => {
                                self.debug_snapshots[client.db] = Some(snapshot);
                                Outcome::error("ERR snapshot is pinned")
                            }
                        }
                    }
                    "DEPTH" => Outcome::reply(RespValue::Integer(
                        self.dbs[client.db].snapshot_depth() as i64,
                    )),
                    _ => Outcome::error("ERR unknown DEBUG MVCC subcommand"),
                }
            }
            "OBJECT" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                self.expire_if_needed(client.db, &args[2]);
                let Some(entry) = self.dbs[client.db].get(&args[2]) else {
                    return Outcome::error("ERR no such key");
                };
                let encoding = entry.value.encoding(self.encoding_limits);
                let address = fnv1a(&args[2]);
                let text = format!(
                    "Value at:0x{address:x} refcount:1 encoding:{encoding} serializedlength:{} lru:0 lru_seconds_idle:0",
                    entry.value.len()
                );
                Outcome::reply(RespValue::bulk(text.into_bytes()))
            }
            "SLEEP" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let Ok(seconds) = String::from_utf8_lossy(&args[2]).parse::<f64>() else {
                    return Outcome::error("ERR value is not a valid float");
                };
                if seconds > 0.0 {
                    std::thread::sleep(std::time::Duration::from_secs_f64(seconds.min(60.0)));
                }
                Outcome::ok()
            }
            "RELOAD" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                if let Err(err) = self.save_rdb_to_disk() {
                    return Outcome::error(format!("ERR {err}"));
                }
                match self.reload_rdb_from_disk() {
                    Ok(()) => Outcome::ok(),
                    Err(err) => Outcome::error(format!("ERR {err}")),
                }
            }
            "DIGEST" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                let entries = self.digest_entries();
                let digest = dataset_digest(
                    entries
                        .iter()
                        .map(|(db, key, value, expire)| (*db, key.as_slice(), value, *expire)),
                );
                Outcome::reply(RespValue::simple(digest))
            }
            "PROTOCOL" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let kind = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
                let reply = match kind.as_str() {
                    "attrib" => RespValue::Attribute {
                        pairs: vec![(
                            RespValue::bulk(b"key-popularity".to_vec()),
                            RespValue::Array(vec![
                                RespValue::bulk(b"key:123".to_vec()),
                                RespValue::Integer(90),
                            ]),
                        )],
                        value: Box::new(RespValue::bulk(
                            b"Some real reply following the attribute".to_vec(),
                        )),
                    },
                    "bignum" => {
                        RespValue::BigNumber(b"1234567999999999999999999999999999999".to_vec())
                    }
                    "true" => RespValue::Boolean(true),
                    "false" => RespValue::Boolean(false),
                    "double" => RespValue::Double(std::f64::consts::PI),
                    "null" => RespValue::Null,
                    "map" => RespValue::Map(vec![
                        (RespValue::bulk(b"a".to_vec()), RespValue::Integer(1)),
                        (RespValue::bulk(b"b".to_vec()), RespValue::Integer(2)),
                    ]),
                    "set" => RespValue::Set(vec![
                        RespValue::bulk(b"a".to_vec()),
                        RespValue::bulk(b"b".to_vec()),
                    ]),
                    "verbatim" => RespValue::VerbatimString {
                        format: *b"txt",
                        data: b"Some verbatim string".to_vec(),
                    },
                    "push" => RespValue::Push(vec![
                        RespValue::bulk(b"message".to_vec()),
                        RespValue::bulk(b"payload".to_vec()),
                    ]),
                    "array" => RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(2)]),
                    "integer" => RespValue::Integer(42),
                    "bulk" => RespValue::bulk(b"bulk string".to_vec()),
                    "simple" => RespValue::simple("simple string"),
                    "error" => RespValue::error("ERR protocol error reply"),
                    _ => {
                        return Outcome::error(format!(
                            "ERR unknown DEBUG PROTOCOL type '{}'",
                            String::from_utf8_lossy(&args[2])
                        ));
                    }
                };
                Outcome::reply(reply)
            }
            "SET-ACTIVE-EXPIRE" | "QUICKLIST-PACKED-THRESHOLD" | "STRINGMATCH-LEN" => Outcome::ok(),
            "JMAP" => Outcome::ok(),
            _ => Outcome::error(format!(
                "ERR DEBUG subcommand '{}' is not supported yet",
                String::from_utf8_lossy(&args[1])
            )),
        }
    }

    fn cmd_hello(&mut self, args: &[Vec<u8>], client: &mut ClientContext) -> Outcome {
        let mut index = 1;
        let mut version = client.version;
        if let Some(value) = args.get(index) {
            let Ok(proto) = parse_i64(value) else {
                return Outcome::error("NOPROTO unsupported protocol version");
            };
            match proto {
                2 => version = RespVersion::V2,
                3 => version = RespVersion::V3,
                _ => return Outcome::error("NOPROTO unsupported protocol version"),
            }
            index += 1;
        }
        let mut authenticated = self.config.requirepass.is_none();
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "AUTH" => {
                    if args.len() < index + 3 {
                        return Outcome::error("ERR syntax error");
                    }
                    let user = &args[index + 1];
                    let password = &args[index + 2];
                    if !user.eq_ignore_ascii_case(b"default") {
                        return Outcome::error(
                            "WRONGPASS invalid username-password pair or user is disabled.",
                        );
                    }
                    if self
                        .config
                        .requirepass
                        .as_ref()
                        .is_some_and(|expected| expected.as_bytes() != password.as_slice())
                    {
                        return Outcome::error(
                            "WRONGPASS invalid username-password pair or user is disabled.",
                        );
                    }
                    authenticated = true;
                    index += 3;
                }
                "SETNAME" => {
                    let Some(name) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    if name.iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
                        return Outcome::error(
                            "ERR Client names cannot contain spaces, newlines or special characters.",
                        );
                    }
                    client.name = name.clone();
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        if !authenticated {
            return Outcome::error(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
            );
        }
        client.version = version;
        let map = vec![
            (
                RespValue::bulk(b"server".to_vec()),
                RespValue::bulk(b"redis".to_vec()),
            ),
            (
                RespValue::bulk(b"version".to_vec()),
                RespValue::bulk(b"6.3.0".to_vec()),
            ),
            (
                RespValue::bulk(b"proto".to_vec()),
                RespValue::Integer(if version == RespVersion::V3 { 3 } else { 2 }),
            ),
            (
                RespValue::bulk(b"id".to_vec()),
                RespValue::Integer(client.id as i64),
            ),
            (
                RespValue::bulk(b"mode".to_vec()),
                RespValue::bulk(b"standalone".to_vec()),
            ),
            (
                RespValue::bulk(b"role".to_vec()),
                RespValue::bulk(b"master".to_vec()),
            ),
            (
                RespValue::bulk(b"modules".to_vec()),
                RespValue::Array(Vec::new()),
            ),
        ];
        Outcome::reply(RespValue::Map(map))
    }

    fn cmd_auth(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.len() != 2 && args.len() != 3 {
            return wrong_args(args);
        }
        let Some(expected) = self.config.requirepass.as_ref() else {
            return Outcome::error(
                "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
            );
        };
        let (user, password) = if args.len() == 2 {
            (b"default".as_slice(), args[1].as_slice())
        } else {
            (args[1].as_slice(), args[2].as_slice())
        };
        if user.eq_ignore_ascii_case(b"default") && expected.as_bytes() == password {
            Outcome::ok()
        } else {
            Outcome::error("WRONGPASS invalid username-password pair or user is disabled.")
        }
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[derive(Debug, Clone, Copy)]
struct ScoreBound {
    value: f64,
    inclusive: bool,
}

#[derive(Debug, Clone)]
enum LexBound {
    NegInf,
    PosInf,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}

fn parse_score(value: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(value).ok()?;
    let score = match text.to_ascii_lowercase().as_str() {
        "inf" | "+inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => text.parse::<f64>().ok()?,
    };
    (!score.is_nan()).then_some(score)
}

fn parse_score_bound(value: &[u8]) -> Option<ScoreBound> {
    if let Some(rest) = value.strip_prefix(b"(") {
        Some(ScoreBound {
            value: parse_score(rest)?,
            inclusive: false,
        })
    } else {
        let rest = value.strip_prefix(b"[").unwrap_or(value);
        Some(ScoreBound {
            value: parse_score(rest)?,
            inclusive: true,
        })
    }
}

fn score_in_range(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    let above = score > min.value || (min.inclusive && score == min.value);
    let below = score < max.value || (max.inclusive && score == max.value);
    above && below
}

fn parse_lex_bound(value: &[u8]) -> Option<LexBound> {
    match value.first().copied() {
        Some(b'-') if value.len() == 1 => Some(LexBound::NegInf),
        Some(b'+') if value.len() == 1 => Some(LexBound::PosInf),
        Some(b'[') => Some(LexBound::Included(value[1..].to_vec())),
        Some(b'(') => Some(LexBound::Excluded(value[1..].to_vec())),
        _ => None,
    }
}

fn lex_in_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
    let above = match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Included(value) => member >= value.as_slice(),
        LexBound::Excluded(value) => member > value.as_slice(),
    };
    let below = match max {
        LexBound::PosInf => true,
        LexBound::NegInf => false,
        LexBound::Included(value) => member <= value.as_slice(),
        LexBound::Excluded(value) => member < value.as_slice(),
    };
    above && below
}

fn sorted_members(zset: &rekeydb_core::SortedSet, reverse: bool) -> Vec<(Vec<u8>, f64)> {
    let mut members: Vec<(Vec<u8>, f64)> = zset
        .scores
        .iter()
        .map(|(member, score)| (member.clone(), *score))
        .collect();
    members.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    if reverse {
        members.reverse();
    }
    members
}

fn score_value(score: f64, version: RespVersion) -> RespValue {
    if version == RespVersion::V3 {
        RespValue::Double(score)
    } else {
        RespValue::bulk(format_float(score).into_bytes())
    }
}

fn zset_pop_reply(
    selected: &[(Vec<u8>, f64)],
    count_given: bool,
    version: RespVersion,
) -> RespValue {
    if selected.is_empty() {
        return RespValue::Array(Vec::new());
    }
    if count_given && version == RespVersion::V3 {
        return RespValue::Array(
            selected
                .iter()
                .map(|(member, score)| {
                    RespValue::Array(vec![
                        RespValue::bulk(member.clone()),
                        score_value(*score, version),
                    ])
                })
                .collect(),
        );
    }
    let mut values = Vec::new();
    for (member, score) in selected {
        values.push(RespValue::bulk(member.clone()));
        values.push(score_value(*score, version));
    }
    RespValue::Array(values)
}

fn aggregate_score(existing: f64, incoming: f64, aggregate: &str) -> f64 {
    let result = match aggregate {
        "MIN" => existing.min(incoming),
        "MAX" => existing.max(incoming),
        _ => existing + incoming,
    };
    if result.is_nan() { 0.0 } else { result }
}

fn format_float(value: f64) -> String {
    rekeydb_core::format_double_17g(value)
}

fn normalize_index(len: usize, requested: i64) -> Option<usize> {
    let len = len as i64;
    let index = if requested < 0 {
        len + requested
    } else {
        requested
    };
    if index < 0 || index >= len {
        None
    } else {
        Some(index as usize)
    }
}

fn normalize_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut start = if start < 0 { len + start } else { start };
    let mut stop = if stop < 0 { len + stop } else { stop };
    if start < 0 {
        start = 0;
    }
    if stop >= len {
        stop = len - 1;
    }
    if start >= len || stop < 0 || start > stop {
        return None;
    }
    Some((start as usize, stop as usize))
}

fn parse_side(value: &[u8]) -> Option<ListSide> {
    if value.eq_ignore_ascii_case(b"LEFT") {
        Some(ListSide::Left)
    } else if value.eq_ignore_ascii_case(b"RIGHT") {
        Some(ListSide::Right)
    } else {
        None
    }
}

fn parse_timeout(value: &[u8]) -> Result<Option<u64>, Outcome> {
    let Ok(text) = std::str::from_utf8(value) else {
        return Err(Outcome::error("ERR timeout is not a float or out of range"));
    };
    let Ok(seconds) = text.parse::<f64>() else {
        return Err(Outcome::error("ERR timeout is not a float or out of range"));
    };
    if !seconds.is_finite() {
        return Err(Outcome::error("ERR timeout is not a float or out of range"));
    }
    if seconds < 0.0 {
        return Err(Outcome::error("ERR timeout is negative"));
    }
    if seconds == 0.0 {
        return Ok(None);
    }
    let milliseconds = (seconds * 1000.0).ceil();
    if milliseconds > u64::MAX as f64 {
        return Err(Outcome::error("ERR timeout is out of range"));
    }
    Ok(Some(milliseconds as u64))
}

fn wrong_type() -> Outcome {
    Outcome::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn client_error(subcommand: &str) -> Outcome {
    Outcome::error(format!(
        "ERR Unknown subcommand or wrong number of arguments for '{subcommand}'. Try CLIENT HELP."
    ))
}

fn wrong_args(args: &[Vec<u8>]) -> Outcome {
    let name = args
        .first()
        .map(|arg| String::from_utf8_lossy(arg).to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string());
    Outcome::error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

fn parse_i64(value: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(value)
        .map_err(|_| ())?
        .parse::<i64>()
        .map_err(|_| ())
}

fn parse_u64(value: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(value)
        .map_err(|_| ())?
        .parse::<u64>()
        .map_err(|_| ())
}

fn parse_usize(value: &[u8]) -> Result<usize, ()> {
    std::str::from_utf8(value)
        .map_err(|_| ())?
        .parse::<usize>()
        .map_err(|_| ())
}

fn is_modifiable_config(key: &str) -> bool {
    matches!(
        key,
        "requirepass"
            | "maxmemory"
            | "maxmemory-policy"
            | "appendonly"
            | "appendfsync"
            | "save"
            | "tcp-keepalive"
            | "timeout"
            | "hz"
            | "sanitize-dump-payload"
            | "skip-checksum-validation"
            | "lazyfree-lazy-eviction"
            | "lazyfree-lazy-expire"
            | "lazyfree-lazy-server-del"
            | "lazyfree-lazy-user-del"
            | "hash-max-ziplist-entries"
            | "hash-max-ziplist-value"
            | "set-max-intset-entries"
            | "zset-max-ziplist-entries"
            | "zset-max-ziplist-value"
    )
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekeydb_protocol::Command;

    fn state() -> State {
        let config = Config {
            databases: 4,
            ..Config::default()
        };
        let mut state = State::new(config);
        state.set_now_ms(1_700_000_000_000);
        state
    }

    fn new_client() -> ClientContext {
        ClientContext::new(1, "127.0.0.1:1234".to_string())
    }

    fn run(state: &mut State, client: &mut ClientContext, args: &[&[u8]]) -> RespValue {
        let command = Command::new(args.iter().map(|arg| arg.to_vec()).collect());
        state.execute(&command, client).reply
    }

    #[test]
    fn set_get_del() {
        let mut state = state();
        let mut client = new_client();
        assert_eq!(
            run(&mut state, &mut client, &[b"SET", b"key", b"value"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"key"]),
            RespValue::bulk(b"value".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"DEL", b"key"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"key"]),
            RespValue::Null
        );
    }

    #[test]
    fn set_nx_xx_and_get() {
        let mut state = state();
        let mut client = new_client();
        assert_eq!(
            run(&mut state, &mut client, &[b"SET", b"k", b"one", b"NX"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SET", b"k", b"two", b"NX"]),
            RespValue::Null
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SET", b"k", b"two", b"GET"]),
            RespValue::bulk(b"one".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"k"]),
            RespValue::bulk(b"two".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SET", b"missing", b"x", b"XX"]),
            RespValue::Null
        );
    }

    #[test]
    fn expiry_is_lazy_and_reported() {
        let mut state = state();
        let mut client = new_client();
        run(
            &mut state,
            &mut client,
            &[b"SET", b"k", b"v", b"PX", b"1000"],
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"PTTL", b"k"]),
            RespValue::Integer(1000)
        );
        state.set_now_ms(1_700_000_001_001);
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"k"]),
            RespValue::Null
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"TTL", b"k"]),
            RespValue::Integer(-2)
        );
    }

    #[test]
    fn select_and_flush() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SET", b"a", b"1"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"SELECT", b"1"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"a"]),
            RespValue::Null
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SELECT", b"99"]),
            RespValue::error("ERR DB index is out of range")
        );
        run(&mut state, &mut client, &[b"FLUSHALL"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"DBSIZE"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn incr_and_append() {
        let mut state = state();
        let mut client = new_client();
        assert_eq!(
            run(&mut state, &mut client, &[b"INCR", b"n"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"INCRBY", b"n", b"4"]),
            RespValue::Integer(5)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"APPEND", b"s", b"ab"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"APPEND", b"s", b"cd"]),
            RespValue::Integer(4)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"s"]),
            RespValue::bulk(b"abcd".to_vec())
        );
    }

    #[test]
    fn config_get_and_set() {
        let mut state = state();
        let mut client = new_client();
        let reply = run(&mut state, &mut client, &[b"CONFIG", b"GET", b"maxmemory"]);
        assert_eq!(
            reply,
            RespValue::Array(vec![
                RespValue::bulk(b"maxmemory".to_vec()),
                RespValue::bulk(b"0".to_vec())
            ])
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"CONFIG", b"SET", b"maxmemory", b"1024"]
            ),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"CONFIG", b"GET", b"maxmemory"]),
            RespValue::Array(vec![
                RespValue::bulk(b"maxmemory".to_vec()),
                RespValue::bulk(b"1024".to_vec())
            ])
        );
    }

    #[test]
    fn hello_switches_protocol_and_auth() {
        let mut state = state();
        let mut client = new_client();
        let reply = run(&mut state, &mut client, &[b"HELLO", b"3"]);
        assert!(matches!(reply, RespValue::Map(_)));
        assert_eq!(client.version, RespVersion::V3);

        state.config.requirepass = Some("secret".to_string());
        let mut unauthenticated = new_client();
        let reply = run(&mut state, &mut unauthenticated, &[b"HELLO", b"3"]);
        assert!(matches!(reply, RespValue::Error(_)));
        let reply = run(
            &mut state,
            &mut unauthenticated,
            &[b"HELLO", b"3", b"AUTH", b"default", b"secret"],
        );
        assert!(matches!(reply, RespValue::Map(_)));
    }

    #[test]
    fn keys_and_scan() {
        let mut state = state();
        let mut client = new_client();
        run(
            &mut state,
            &mut client,
            &[b"MSET", b"a", b"1", b"b", b"2", b"c", b"3"],
        );
        let reply = run(&mut state, &mut client, &[b"KEYS", b"*"]);
        let RespValue::Array(values) = reply else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 3);

        let reply = run(&mut state, &mut client, &[b"SCAN", b"0", b"COUNT", b"2"]);
        let RespValue::Array(parts) = reply else {
            panic!("expected array");
        };
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], RespValue::bulk(b"2".to_vec()));
        assert_eq!(
            parts[1],
            RespValue::Array(vec![
                RespValue::bulk(b"a".to_vec()),
                RespValue::bulk(b"b".to_vec())
            ])
        );
    }

    #[test]
    fn debug_mvcc_snapshot_roundtrip() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SET", b"k", b"v1"]);
        let reply = run(&mut state, &mut client, &[b"DEBUG", b"MVCC", b"SNAPSHOT"]);
        assert!(matches!(reply, RespValue::Integer(_)));
        assert_eq!(
            run(&mut state, &mut client, &[b"DEBUG", b"MVCC", b"DEPTH"]),
            RespValue::Integer(1)
        );
        run(&mut state, &mut client, &[b"SET", b"k", b"v2"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"DEBUG", b"MVCC", b"END"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"k"]),
            RespValue::bulk(b"v2".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"DEBUG", b"MVCC", b"DEPTH"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn debug_object_reports_stable_encoding() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SET", b"n", b"123"]);
        let first = run(&mut state, &mut client, &[b"DEBUG", b"OBJECT", b"n"]);
        let RespValue::BulkString(first) = first else {
            panic!("expected bulk string");
        };
        let text = String::from_utf8(first).unwrap();
        assert!(text.contains("encoding:int"));
        run(&mut state, &mut client, &[b"INCR", b"n"]);
        let second = run(&mut state, &mut client, &[b"DEBUG", b"OBJECT", b"n"]);
        let RespValue::BulkString(second) = second else {
            panic!("expected bulk string");
        };
        let second = String::from_utf8(second).unwrap();
        assert_eq!(
            text.split_whitespace().nth(1),
            second.split_whitespace().nth(1)
        );
    }

    #[test]
    fn set_commands_and_encoding() {
        let mut state = state();
        let mut client = new_client();
        assert_eq!(
            run(&mut state, &mut client, &[b"SADD", b"s", b"a", b"b"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SADD", b"s", b"b", b"c"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SCARD", b"s"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SISMEMBER", b"s", b"a"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"SMISMEMBER", b"s", b"a", b"zzz"]
            ),
            RespValue::Array(vec![RespValue::Integer(1), RespValue::Integer(0)])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"TYPE", b"s"]),
            RespValue::simple("set")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"s"]),
            RespValue::bulk(b"hashtable".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SREM", b"s", b"a", b"missing"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SMEMBERS", b"s"]),
            RespValue::Array(vec![
                RespValue::bulk(b"b".to_vec()),
                RespValue::bulk(b"c".to_vec())
            ])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SREM", b"s", b"b", b"c"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"EXISTS", b"s"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn set_intset_encoding_and_wrongtype() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SADD", b"ints", b"1", b"2"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"ints"]),
            RespValue::bulk(b"intset".to_vec())
        );
        run(&mut state, &mut client, &[b"SADD", b"ints", b"three"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"ints"]),
            RespValue::bulk(b"hashtable".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"ints"]),
            RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SADD", b"str", b"x"]),
            RespValue::Integer(1)
        );
        run(&mut state, &mut client, &[b"SET", b"str", b"value"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"SADD", b"str", b"y"]),
            RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"str"]),
            RespValue::bulk(b"value".to_vec())
        );
    }

    #[test]
    fn set_operations_and_srandmember() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SADD", b"a", b"1", b"2", b"3"]);
        run(&mut state, &mut client, &[b"SADD", b"b", b"2", b"3", b"4"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"SINTER", b"a", b"b"]),
            RespValue::Array(vec![
                RespValue::bulk(b"2".to_vec()),
                RespValue::bulk(b"3".to_vec())
            ])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SUNION", b"a", b"b"]),
            RespValue::Array(vec![
                RespValue::bulk(b"1".to_vec()),
                RespValue::bulk(b"2".to_vec()),
                RespValue::bulk(b"3".to_vec()),
                RespValue::bulk(b"4".to_vec())
            ])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SDIFF", b"a", b"b"]),
            RespValue::Array(vec![RespValue::bulk(b"1".to_vec())])
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"SINTERSTORE", b"dst", b"a", b"b"]
            ),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SCARD", b"dst"]),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SRANDMEMBER", b"missing"]),
            RespValue::NullArray
        );
        let reply = run(&mut state, &mut client, &[b"SRANDMEMBER", b"a", b"100"]);
        let RespValue::Array(values) = reply else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 3);
        let reply = run(&mut state, &mut client, &[b"SPOP", b"a", b"2"]);
        let RespValue::Array(values) = reply else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 2);
        assert_eq!(
            run(&mut state, &mut client, &[b"SCARD", b"a"]),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn list_commands() {
        let mut state = state();
        let mut client = new_client();
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"LPUSH", b"l", b"a", b"b", b"c", b"d"]
            ),
            RespValue::Integer(4)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"RPUSH", b"l", b"0", b"1", b"2"]),
            RespValue::Integer(7)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LRANGE", b"l", b"0", b"-1"]),
            RespValue::Array(
                [b"d", b"c", b"b", b"a", b"0", b"1", b"2"]
                    .into_iter()
                    .map(|value| RespValue::bulk(value.to_vec()))
                    .collect()
            )
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LINDEX", b"l", b"-1"]),
            RespValue::bulk(b"2".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LSET", b"l", b"0", b"z"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LINDEX", b"l", b"0"]),
            RespValue::bulk(b"z".to_vec())
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"LINSERT", b"l", b"BEFORE", b"a", b"x"]
            ),
            RespValue::Integer(8)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LPOS", b"l", b"a"]),
            RespValue::Integer(4)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LREM", b"l", b"0", b"a"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LTRIM", b"l", b"0", b"2"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LLEN", b"l"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LPOP", b"l", b"2"]),
            RespValue::Array(vec![
                RespValue::bulk(b"z".to_vec()),
                RespValue::bulk(b"c".to_vec())
            ])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"RPOP", b"l"]),
            RespValue::bulk(b"b".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"EXISTS", b"l"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn list_move_and_blocking() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"RPUSH", b"src", b"a", b"b"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"RPOPLPUSH", b"src", b"dst"]),
            RespValue::bulk(b"b".to_vec())
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"LMOVE", b"src", b"dst", b"LEFT", b"RIGHT"]
            ),
            RespValue::bulk(b"a".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LRANGE", b"dst", b"0", b"-1"]),
            RespValue::Array(vec![
                RespValue::bulk(b"b".to_vec()),
                RespValue::bulk(b"a".to_vec())
            ])
        );

        let blocked = state.execute(
            &Command::new(vec![b"BLPOP".to_vec(), b"waiting".to_vec(), b"1".to_vec()]),
            &mut client,
        );
        let request = *blocked.block.expect("missing list should block");
        // Still blocked because the list is empty.
        assert!(state.serve_blocked(&request).is_none());
        run(&mut state, &mut client, &[b"RPUSH", b"waiting", b"value"]);
        assert_eq!(
            state.serve_blocked(&request),
            Some(RespValue::Array(vec![
                RespValue::bulk(b"waiting".to_vec()),
                RespValue::bulk(b"value".to_vec())
            ]))
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"EXISTS", b"waiting"]),
            RespValue::Integer(0)
        );
    }

    #[test]
    fn list_wrongtype() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SET", b"s", b"value"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"LPUSH", b"s", b"x"]),
            RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
        run(&mut state, &mut client, &[b"RPUSH", b"l", b"x"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"l"]),
            RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"l"]),
            RespValue::bulk(b"quicklist".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"TYPE", b"l"]),
            RespValue::simple("list")
        );
    }

    #[test]
    fn multi_exec_and_watch() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"MULTI"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"SET", b"a", b"1"]),
            RespValue::simple("QUEUED")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"INCR", b"a"]),
            RespValue::simple("QUEUED")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"EXEC"]),
            RespValue::Array(vec![RespValue::ok(), RespValue::Integer(2)])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"a"]),
            RespValue::bulk(b"2".to_vec())
        );

        run(&mut state, &mut client, &[b"WATCH", b"a"]);
        run(&mut state, &mut client, &[b"MULTI"]);
        run(&mut state, &mut client, &[b"GET", b"a"]);
        let mut other = new_client();
        run(&mut state, &mut other, &[b"SET", b"a", b"changed"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"EXEC"]),
            RespValue::NullArray
        );

        run(&mut state, &mut client, &[b"WATCH", b"a"]);
        run(&mut state, &mut client, &[b"MULTI"]);
        run(&mut state, &mut client, &[b"GET", b"a"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"EXEC"]),
            RespValue::Array(vec![RespValue::bulk(b"changed".to_vec())])
        );
    }

    #[test]
    fn rename_and_sort() {
        let mut state = state();
        let mut client = new_client();
        run(
            &mut state,
            &mut client,
            &[b"LPUSH", b"bob", b"abc", b"def", b"hij"],
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"RENAME", b"bob", b"foo"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"EXISTS", b"bob"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LRANGE", b"foo", b"0", b"-1"]),
            RespValue::Array(vec![
                RespValue::bulk(b"hij".to_vec()),
                RespValue::bulk(b"def".to_vec()),
                RespValue::bulk(b"abc".to_vec())
            ])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"RENAME", b"missing", b"foo"]),
            RespValue::error("ERR no such key")
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"RPUSH", b"notfoo", b"hello", b"hola", b"aguacate"]
            ),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"SORT", b"notfoo", b"ALPHA", b"STORE", b"foo"]
            ),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LRANGE", b"foo", b"0", b"-1"]),
            RespValue::Array(vec![
                RespValue::bulk(b"aguacate".to_vec()),
                RespValue::bulk(b"hello".to_vec()),
                RespValue::bulk(b"hola".to_vec())
            ])
        );
    }

    #[test]
    fn lpos_maxlen_from_tail() {
        let mut state = state();
        let mut client = new_client();
        run(
            &mut state,
            &mut client,
            &[
                b"RPUSH", b"mylist", b"a", b"b", b"c", b"d", b"e", b"c", b"c", b"f",
            ],
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[
                    b"LPOS", b"mylist", b"c", b"COUNT", b"0", b"MAXLEN", b"3", b"RANK", b"-1"
                ]
            ),
            RespValue::Array(vec![RespValue::Integer(6), RespValue::Integer(5)])
        );
    }

    #[test]
    fn debug_reload_preserves_values_and_digest() {
        let dir = std::env::temp_dir().join(format!("rekeydb-rdb-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config {
            databases: 4,
            dir: dir.clone(),
            ..Config::default()
        };
        let mut state = State::new(config);
        state.set_now_ms(1_700_000_000_000);
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SET", b"s", b"value"]);
        run(&mut state, &mut client, &[b"RPUSH", b"l", b"a", b"b", b"c"]);
        run(
            &mut state,
            &mut client,
            &[b"SADD", b"set", b"1", b"2", b"3"],
        );
        run(
            &mut state,
            &mut client,
            &[b"HSET", b"h", b"a", b"1", b"b", b"2"],
        );
        run(&mut state, &mut client, &[b"PEXPIRE", b"s", b"60000"]);
        let before = run(&mut state, &mut client, &[b"DEBUG", b"DIGEST"]);
        let RespValue::SimpleString(before) = before else {
            panic!("expected digest status");
        };
        assert_eq!(before.len(), 40);
        assert_eq!(
            run(&mut state, &mut client, &[b"DEBUG", b"RELOAD"]),
            RespValue::ok()
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"s"]),
            RespValue::bulk(b"value".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"LRANGE", b"l", b"0", b"-1"]),
            RespValue::Array(vec![
                RespValue::bulk(b"a".to_vec()),
                RespValue::bulk(b"b".to_vec()),
                RespValue::bulk(b"c".to_vec())
            ])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"SCARD", b"set"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HGET", b"h", b"a"]),
            RespValue::bulk(b"1".to_vec())
        );
        let after = run(&mut state, &mut client, &[b"DEBUG", b"DIGEST"]);
        let RespValue::SimpleString(after) = after else {
            panic!("expected digest status");
        };
        assert_eq!(before, after);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn hash_commands_and_encoding() {
        let mut state = state();
        let mut client = new_client();
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"HSET", b"h", b"a", b"1", b"b", b"2"]
            ),
            RespValue::Integer(2)
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"HSET", b"h", b"a", b"9", b"c", b"3"]
            ),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HLEN", b"h"]),
            RespValue::Integer(3)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HGET", b"h", b"a"]),
            RespValue::bulk(b"9".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HMGET", b"h", b"a", b"missing"]),
            RespValue::Array(vec![RespValue::bulk(b"9".to_vec()), RespValue::null()])
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HINCRBY", b"h", b"a", b"1"]),
            RespValue::Integer(10)
        );
        assert_eq!(
            run(
                &mut state,
                &mut client,
                &[b"HINCRBYFLOAT", b"h", b"a", b"0.5"]
            ),
            RespValue::bulk(b"10.5".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"h"]),
            RespValue::bulk(b"ziplist".to_vec())
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"TYPE", b"h"]),
            RespValue::simple("hash")
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HDEL", b"h", b"b", b"missing"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"HGETALL", b"h"]),
            RespValue::Array(vec![
                RespValue::bulk(b"a".to_vec()),
                RespValue::bulk(b"10.5".to_vec()),
                RespValue::bulk(b"c".to_vec()),
                RespValue::bulk(b"3".to_vec())
            ])
        );
    }

    #[test]
    fn hash_promotes_and_scans() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"HSET", b"h", b"a", b"1"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"h"]),
            RespValue::bulk(b"ziplist".to_vec())
        );
        run(
            &mut state,
            &mut client,
            &[b"HSET", b"h", b"long", &[b'x'; 80]],
        );
        assert_eq!(
            run(&mut state, &mut client, &[b"OBJECT", b"ENCODING", b"h"]),
            RespValue::bulk(b"hashtable".to_vec())
        );
        let scan = run(
            &mut state,
            &mut client,
            &[b"HSCAN", b"h", b"0", b"COUNT", b"100"],
        );
        let RespValue::Array(parts) = scan else {
            panic!("expected scan array");
        };
        assert_eq!(parts[0], RespValue::bulk(b"0".to_vec()));
        let RespValue::Array(values) = &parts[1] else {
            panic!("expected values");
        };
        assert_eq!(values.len(), 4);
        assert_eq!(
            run(&mut state, &mut client, &[b"HEXISTS", b"h", b"a"]),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn hash_wrongtype() {
        let mut state = state();
        let mut client = new_client();
        run(&mut state, &mut client, &[b"SET", b"s", b"value"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"HSET", b"s", b"a", b"1"]),
            RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
        run(&mut state, &mut client, &[b"HSET", b"h", b"a", b"1"]);
        assert_eq!(
            run(&mut state, &mut client, &[b"GET", b"h"]),
            RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }

    #[test]
    fn config_values_are_sorted() {
        let state = state();
        assert!(state.config_values.contains_key("maxmemory"));
        assert_eq!(state.config_values["server-threads"], "1");
    }
}
