use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rekeydb_core::{Database, EncodingLimits, Entry, ExpireEntry, MvccClock, Snapshot, Value};
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
}

impl ClientContext {
    pub fn new(id: u64, addr: String) -> Self {
        Self {
            id,
            db: 0,
            name: Vec::new(),
            version: RespVersion::V2,
            addr,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub reply: RespValue,
    pub close: bool,
    pub shutdown: bool,
}

impl Outcome {
    pub fn reply(reply: RespValue) -> Self {
        Self {
            reply,
            close: false,
            shutdown: false,
        }
    }

    pub fn ok() -> Self {
        Self::reply(RespValue::ok())
    }

    pub fn error(message: impl Into<Vec<u8>>) -> Self {
        Self::reply(RespValue::error(message))
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
            "CONFIG" => self.cmd_config(args),
            "DEBUG" => self.cmd_debug(args, client),
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
        let value = Arc::make_mut(&mut entry.value);
        match value {
            Value::Set(set) => Ok(set),
            _ => Err(wrong_type()),
        }
    }

    fn remove_set_members(&mut self, db_index: usize, key: &[u8], members: &[Vec<u8>]) -> usize {
        let mut removed = 0;
        let mut empty = false;
        if let Some(entry) = self.dbs[db_index].get_mut(key) {
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
                "# Clients\r\nconnected_clients:{}\r\ncluster_connections:0\r\nmaxclients:10000\r\nclient_recent_max_input_buffer:0\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:0\r\ntracking_clients:0\r\npubsub_clients:0\r\nwatching_clients:0\r\nclients_in_timeout_table:0\r\ntotal_watched_keys:0\r\ntotal_blocking_keys:0\r\ntotal_blocking_keys_on_nokey:0\r\n\r\n",
                self.connected_clients
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
            "SETINFO" => Outcome::ok(),
            _ => Outcome::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.",
                String::from_utf8_lossy(&args[1])
            )),
        }
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
    fn config_values_are_sorted() {
        let state = state();
        assert!(state.config_values.contains_key("maxmemory"));
        assert_eq!(state.config_values["server-threads"], "1");
    }
}
