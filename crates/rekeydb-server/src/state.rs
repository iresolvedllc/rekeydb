#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rekeydb_core::{
    Database, EncodingLimits, Entry, ExpireEntry, HashValue, MvccClock, Snapshot, Stream,
    StreamConsumer, StreamEntry, StreamGroup, StreamId, StreamNack, Value,
};
use rekeydb_persist::aof::{self, AofFsync, AofWriter};
use rekeydb_persist::{RdbEntry, dataset_digest, dump, load, restore, save, value_digest};
use rekeydb_protocol::{Command, RespValue, RespVersion, encode_command};

use crate::config::Config;
use crate::geo;
use crate::hll;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockKind {
    Pop,
    Move { rpoplpush: bool },
    ZPop { max: bool },
    StreamRead(StreamBlock),
}

/// State captured when an XREAD/XREADGROUP command blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBlock {
    /// `None` represents the special XREADGROUP `>` ID.
    pub ids: Vec<Option<StreamId>>,
    pub group: Option<Vec<u8>>,
    pub consumer: Option<Vec<u8>>,
    pub count: usize,
    pub noack: bool,
    pub version: RespVersion,
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
    /// When set, these commands replace the default AOF entry for this command.
    pub aof_commands: Option<Vec<Vec<Vec<u8>>>>,
    /// Additional reply frames sent after `reply` on the same connection.
    pub extra: Vec<RespValue>,
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
            aof_commands: None,
            extra: Vec::new(),
        }
    }

    pub fn with_frames(mut frames: Vec<RespValue>) -> Self {
        let reply = if frames.is_empty() {
            RespValue::Null
        } else {
            frames.remove(0)
        };
        Self {
            reply,
            close: false,
            shutdown: false,
            block: None,
            wake: Vec::new(),
            unblock: None,
            aof_commands: None,
            extra: frames,
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
            aof_commands: None,
            extra: Vec::new(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    Master,
    Replica,
}

#[derive(Debug, Clone)]
struct ReplicationState {
    role: ReplicationRole,
    replid: String,
    offset: i64,
    selected_db: Option<usize>,
    pending: Vec<u8>,
    clients: BTreeMap<u64, i64>,
    master_host: String,
    master_port: u16,
    master_replid: String,
    link_status: String,
    sync_in_progress: bool,
    last_io_ms: i64,
    pending_replicaof: Option<Option<(String, u16)>>,
    replaying: bool,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Master,
            replid: new_replid(),
            offset: 0,
            selected_db: None,
            pending: Vec::new(),
            clients: BTreeMap::new(),
            master_host: String::new(),
            master_port: 0,
            master_replid: String::new(),
            link_status: "down".to_string(),
            sync_in_progress: false,
            last_io_ms: 0,
            pending_replicaof: None,
            replaying: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ClientSubscriptions {
    channels: BTreeSet<Vec<u8>>,
    patterns: BTreeSet<Vec<u8>>,
    shards: BTreeSet<Vec<u8>>,
    version: RespVersion,
}

impl ClientSubscriptions {
    fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty() && self.shards.is_empty()
    }

    fn total(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
}

#[derive(Debug)]
struct PubSubDelivery {
    target: u64,
    message: RespValue,
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
    explicit_configs: BTreeSet<String>,
    debug_snapshots: Vec<Option<Arc<Snapshot<Value>>>>,
    encoding_limits: EncodingLimits,
    aof: Option<AofWriter>,
    aof_fsync: AofFsync,
    loading: bool,
    aof_last_write_status: String,
    aof_last_bgrewrite_status: String,
    aof_rewrite_in_progress: bool,
    aof_rewrite_scheduled: bool,
    rdb_bgsave_in_progress: bool,
    aof_last_rewrite_time_sec: i64,
    aof_rewrites: u64,
    client_names: BTreeMap<u64, Vec<u8>>,
    pub connected_clients: usize,
    pub blocked_clients: usize,
    replication: ReplicationState,
    raw_string_keys: BTreeSet<(usize, Vec<u8>)>,
    command_calls: BTreeMap<String, u64>,
    channel_subs: BTreeMap<Vec<u8>, BTreeSet<u64>>,
    pattern_subs: BTreeMap<Vec<u8>, BTreeSet<u64>>,
    shard_subs: BTreeMap<Vec<u8>, BTreeSet<u64>>,
    client_subs: BTreeMap<u64, ClientSubscriptions>,
    pending_pubsub: Vec<PubSubDelivery>,
    has_expiries: bool,
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
            "appendonly",
            "appendfilename",
            "appendfsync",
            "proto-max-bulk-len",
            "aof-use-rdb-preamble",
            "aof-load-truncated",
            "auto-aof-rewrite-percentage",
            "auto-aof-rewrite-min-size",
            "no-appendfsync-on-rewrite",
            "repl-ping-replica-period",
            "repl-timeout",
            "repl-diskless-sync",
            "repl-diskless-sync-delay",
            "repl-diskless-load",
            "repl-backlog-size",
            "repl-disable-tcp-nodelay",
            "replica-read-only",
            "replica-serve-stale-data",
            "daemonize",
            "loglevel",
            "lua-time-limit",
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
            "hll-sparse-max-bytes",
            "stream-node-max-entries",
            "notify-keyspace-events",
        ] {
            config_values.insert(key.to_string(), String::new());
        }
        config_values.extend(config.settings.clone());
        if let Some(value) = config_values.get("notify-keyspace-events").cloned()
            && let Some(canonical) = canonical_notify_flags(&value)
        {
            config_values.insert("notify-keyspace-events".into(), canonical);
        }
        let explicit_configs: BTreeSet<String> = config.settings.keys().cloned().collect();
        let mut state = Self {
            config,
            dbs: (0..databases).map(|_| Database::new()).collect(),
            clock: MvccClock::new(0),
            stats: Stats::default(),
            start: Instant::now(),
            start_unix: now_unix_ms() / 1000,
            now_override: None,
            config_values,
            explicit_configs,
            debug_snapshots: (0..databases).map(|_| None).collect(),
            encoding_limits: EncodingLimits::default(),
            aof: None,
            aof_fsync: AofFsync::EverySec,
            loading: false,
            aof_last_write_status: "ok".to_string(),
            aof_last_bgrewrite_status: "ok".to_string(),
            aof_rewrite_in_progress: false,
            aof_rewrite_scheduled: false,
            rdb_bgsave_in_progress: false,
            aof_last_rewrite_time_sec: -1,
            aof_rewrites: 0,
            client_names: BTreeMap::new(),
            connected_clients: 0,
            blocked_clients: 0,
            replication: ReplicationState::default(),
            raw_string_keys: BTreeSet::new(),
            command_calls: BTreeMap::new(),
            channel_subs: BTreeMap::new(),
            pattern_subs: BTreeMap::new(),
            shard_subs: BTreeMap::new(),
            client_subs: BTreeMap::new(),
            pending_pubsub: Vec::new(),
            has_expiries: false,
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
        self.config_values.insert(
            "appendonly".into(),
            if self.config.appendonly { "yes" } else { "no" }.into(),
        );
        self.config_values
            .insert("appendfilename".into(), self.config.appendfilename.clone());
        self.config_values
            .insert("appendfsync".into(), self.config.appendfsync.clone());
        self.config_values
            .entry("proto-max-bulk-len".into())
            .or_insert_with(|| "512mb".into());
        self.config_values
            .entry("hll-sparse-max-bytes".into())
            .or_insert_with(|| "3000".into());
        self.config_values
            .entry("stream-node-max-entries".into())
            .or_insert_with(|| "100".into());
        for (key, value) in [
            ("repl-ping-replica-period", "10"),
            ("repl-timeout", "60"),
            ("repl-diskless-sync", "yes"),
            ("repl-diskless-sync-delay", "5"),
            ("repl-diskless-load", "disabled"),
            ("repl-disable-tcp-nodelay", "no"),
            ("repl-backlog-size", "1048576"),
            ("replica-read-only", "yes"),
            ("replica-serve-stale-data", "yes"),
            ("notify-keyspace-events", ""),
        ] {
            self.config_values
                .entry(key.into())
                .or_insert_with(|| value.into());
        }
        self.config_values.insert(
            "aof-use-rdb-preamble".into(),
            if self.config.aof_use_rdb_preamble {
                "yes"
            } else {
                "no"
            }
            .into(),
        );
        self.config_values.insert(
            "aof-load-truncated".into(),
            if self.config.aof_load_truncated {
                "yes"
            } else {
                "no"
            }
            .into(),
        );
        for (key, value) in [
            ("daemonize", "no"),
            ("loglevel", "notice"),
            ("lua-time-limit", "5000"),
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

    pub fn replication_role(&self) -> ReplicationRole {
        self.replication.role
    }

    pub fn replication_offset(&self) -> i64 {
        self.replication.offset
    }

    /// Diskless-sync batching delay in seconds. The delay is only honored
    /// when it was explicitly configured (startup config or CONFIG SET), so
    /// the default value does not slow down ordinary single-replica syncs.
    pub fn replication_diskless_delay_seconds(&self) -> u64 {
        if !self.explicit_configs.contains("repl-diskless-sync-delay") {
            return 0;
        }
        if !self
            .config_values
            .get("repl-diskless-sync")
            .is_some_and(|value| value.eq_ignore_ascii_case("yes"))
        {
            return 0;
        }
        self.config_values
            .get("repl-diskless-sync-delay")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    pub fn replication_timeout_seconds(&self) -> Option<u64> {
        self.config_values
            .get("repl-timeout")
            .and_then(|value| value.parse::<u64>().ok())
    }

    pub fn replication_id(&self) -> &str {
        &self.replication.replid
    }

    pub fn master_link_status(&self) -> &str {
        &self.replication.link_status
    }

    pub fn master_address(&self) -> (String, u16) {
        (
            self.replication.master_host.clone(),
            self.replication.master_port,
        )
    }

    pub fn master_offset(&self) -> i64 {
        self.replication.offset
    }

    pub fn replica_acked(&self) -> usize {
        self.replication.clients.len()
    }

    pub fn request_replicaof(&mut self, host: String, port: u16) {
        self.replication.role = ReplicationRole::Replica;
        self.replication.master_host = host.clone();
        self.replication.master_port = port;
        self.replication.link_status = "down".to_string();
        self.replication.sync_in_progress = true;
        self.replication.master_replid.clear();
        self.replication.pending_replicaof = Some(Some((host, port)));
    }

    pub fn request_promotion(&mut self) {
        self.replication.role = ReplicationRole::Master;
        self.replication.replid = new_replid();
        self.replication.master_replid.clear();
        self.replication.master_host.clear();
        self.replication.master_port = 0;
        self.replication.link_status = "down".to_string();
        self.replication.sync_in_progress = false;
        self.replication.pending_replicaof = Some(None);
    }

    pub fn take_replicaof_request(&mut self) -> Option<Option<(String, u16)>> {
        self.replication.pending_replicaof.take()
    }

    pub fn replication_payload(&mut self) -> Result<(String, i64, Vec<u8>), String> {
        let entries = self.aof_entries();
        let payload = aof::encode_entries(&entries);
        self.replication.selected_db = None;
        Ok((
            self.replication.replid.clone(),
            self.replication.offset,
            payload,
        ))
    }

    pub fn register_replica(&mut self, client_id: u64) {
        self.replication.clients.insert(client_id, -1);
    }

    pub fn update_replica_ack(&mut self, client_id: u64, offset: i64) {
        self.replication.clients.insert(client_id, offset);
    }

    pub fn replication_pending_len(&self) -> usize {
        self.replication.pending.len()
    }

    pub fn take_replication_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.replication.pending)
    }

    pub fn take_pubsub_deliveries(&mut self) -> Vec<(u64, RespValue)> {
        std::mem::take(&mut self.pending_pubsub)
            .into_iter()
            .map(|delivery| (delivery.target, delivery.message))
            .collect()
    }

    pub fn set_master_link_up(&mut self, replid: String, offset: i64) {
        self.replication.master_replid = replid;
        self.replication.link_status = "up".to_string();
        self.replication.sync_in_progress = false;
        self.replication.offset = offset;
        self.replication.last_io_ms = now_unix_ms();
    }

    pub fn set_master_link_down(&mut self) {
        self.replication.link_status = "down".to_string();
        self.replication.sync_in_progress = true;
    }

    pub fn set_master_offset(&mut self, offset: i64) {
        self.replication.offset = offset;
        self.replication.last_io_ms = now_unix_ms();
    }

    pub fn apply_full_sync(
        &mut self,
        payload: &[u8],
        replid: String,
        offset: i64,
    ) -> Result<(), String> {
        for db in &mut self.dbs {
            db.clear();
        }
        if payload.starts_with(b"REDIS") {
            let entries = load(payload).map_err(|err| err.to_string())?;
            let now = self.now_ms();
            for entry in entries {
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
        } else {
            let mut decoder = rekeydb_protocol::RespDecoder::default();
            decoder.feed(payload);
            let mut client = ClientContext::new(0, "master".to_string());
            loop {
                match decoder.next_command() {
                    Ok(Some(command)) => {
                        let outcome = self.execute_from_master(&command, &mut client);
                        if let RespValue::Error(error) = outcome.reply {
                            return Err(format!(
                                "full sync command failed: {}",
                                String::from_utf8_lossy(&error)
                            ));
                        }
                    }
                    Ok(None) => break,
                    Err(err) => return Err(format!("full sync payload: {err}")),
                }
            }
        }
        self.set_master_link_up(replid, offset);
        Ok(())
    }

    pub fn execute_from_master(
        &mut self,
        command: &Command,
        client: &mut ClientContext,
    ) -> Outcome {
        self.replication.replaying = true;
        let outcome = self.execute(command, client);
        self.replication.replaying = false;
        outcome
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
        if expired && !self.loading {
            self.dbs[db_index].remove(key);
            self.notify_keyspace_event(db_index, key, 'x', "expired");
        }
    }

    /// Remove expired keys and emit the `expired` notification. This is a
    /// coarse active-expiry pass; it only runs once a TTL has been set.
    pub fn expire_cycle(&mut self) {
        if self.loading || !self.has_expiries {
            return;
        }
        let now = self.now_ms();
        let mut expired = Vec::new();
        for (db_index, db) in self.dbs.iter().enumerate() {
            for (key, entry) in db.collect_visible() {
                if entry.is_expired(now) {
                    expired.push((db_index, key));
                    if expired.len() >= 512 {
                        break;
                    }
                }
            }
            if expired.len() >= 512 {
                break;
            }
        }
        for (db, key) in expired {
            if self.dbs[db].remove(&key).is_some() {
                self.notify_keyspace_event(db, &key, 'x', "expired");
            }
        }
    }

    fn enforce_maxmemory(&mut self) {
        let limit = self
            .config_values
            .get("maxmemory")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        if limit == 0 || self.loading {
            return;
        }
        if self
            .config_values
            .get("maxmemory-policy")
            .is_some_and(|policy| policy.eq_ignore_ascii_case("noeviction"))
        {
            return;
        }
        loop {
            let used: usize = self
                .dbs
                .iter()
                .map(|db| {
                    db.collect_visible()
                        .iter()
                        .map(|(key, entry)| key.len() + entry.value.memory_usage())
                        .sum::<usize>()
                })
                .sum();
            if used <= limit {
                return;
            }
            let mut victim = None;
            for (db_index, db) in self.dbs.iter().enumerate() {
                if let Some((key, _)) = db.collect_visible().into_iter().next() {
                    victim = Some((db_index, key));
                    break;
                }
            }
            let Some((db, key)) = victim else {
                return;
            };
            if self.dbs[db].remove(&key).is_some() {
                self.notify_keyspace_event(db, &key, 'e', "evicted");
            } else {
                return;
            }
        }
    }

    fn existing(&mut self, db_index: usize, key: &[u8]) -> Option<Entry<Value>> {
        self.expire_if_needed(db_index, key);
        self.dbs[db_index].get(key).cloned()
    }

    /// Execute one command and append it to the AOF when it is a write.
    pub fn execute(&mut self, command: &Command, client: &mut ClientContext) -> Outcome {
        let db = client.db;
        let outcome = self.execute_inner(command, client);
        if !self.loading
            && (self.aof.is_some() || !self.replication.clients.is_empty())
            && outcome.block.is_none()
            && !matches!(outcome.reply, RespValue::Error(_))
            && !matches!(&outcome.reply, RespValue::SimpleString(value) if value == b"QUEUED")
        {
            self.aof_propagate(db, command, &outcome);
        }
        self.notify_command(command, &outcome, db);
        outcome
    }

    fn execute_inner(&mut self, command: &Command, client: &mut ClientContext) -> Outcome {
        let args = &command.args;
        if args.is_empty() {
            return Outcome::ok();
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        self.stats.total_commands_processed += 1;
        *self.command_calls.entry(name.clone()).or_insert(0) += 1;
        if self.replication.role == ReplicationRole::Replica
            && !self.replication.replaying
            && !matches!(name.as_str(), "SLAVEOF" | "REPLICAOF" | "REPLCONF")
            && self
                .config_values
                .get("replica-read-only")
                .map(|value| !value.eq_ignore_ascii_case("no"))
                .unwrap_or(true)
            && is_aof_write(&name)
        {
            return Outcome::error("READONLY You can't write against a read only replica.");
        }

        if self
            .client_subs
            .get(&client.id)
            .is_some_and(|subs| !subs.is_empty())
            && !matches!(
                name.as_str(),
                "SUBSCRIBE"
                    | "UNSUBSCRIBE"
                    | "PSUBSCRIBE"
                    | "PUNSUBSCRIBE"
                    | "SSUBSCRIBE"
                    | "SUNSUBSCRIBE"
                    | "PING"
                    | "QUIT"
                    | "RESET"
            )
        {
            return Outcome::error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                name.to_ascii_lowercase()
            ));
        }

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
                        BlockKind::Pop | BlockKind::ZPop { .. } | BlockKind::StreamRead(_) => {
                            RespValue::NullArray
                        }
                        BlockKind::Move { .. } => RespValue::null(),
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
                aof_commands: None,
                extra: Vec::new(),
            };
        }
        if let Some(queue) = client.multi.as_mut() {
            queue.push(command.clone());
            return Outcome::reply(RespValue::simple("QUEUED"));
        }

        match name.as_str() {
            "PING" => {
                let subscribed = self
                    .client_subs
                    .get(&client.id)
                    .is_some_and(|subs| !subs.is_empty());
                if subscribed {
                    if args.len() > 2 {
                        return wrong_args(args);
                    }
                    let payload = args.get(1).cloned().unwrap_or_default();
                    let frame = vec![RespValue::bulk(b"pong".to_vec()), RespValue::bulk(payload)];
                    Outcome::reply(push_frame(frame, client.version))
                } else if args.len() == 1 {
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
                aof_commands: None,
                extra: Vec::new(),
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
            "SETNX" => self.cmd_setnx(args, client.db),
            "SETEX" => self.cmd_setex(&name, args, client.db),
            "PSETEX" => self.cmd_setex(&name, args, client.db),
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
            "GETEX" => self.cmd_getex(args, client.db),
            "PEXPIREMEMBERAT" | "EXPIREMEMBERAT" | "EXPIREMEMBER" | "PEXPIREMEMBER" => {
                self.cmd_subexpire(&name, args, client.db)
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
            "SETRANGE" => self.cmd_setrange(args, client.db),
            "GETRANGE" => self.cmd_getrange(args, client.db),
            "SETBIT" => self.cmd_setbit(args, client.db),
            "GETBIT" => self.cmd_getbit(args, client.db),
            "BITCOUNT" => self.cmd_bitcount(args, client.db),
            "BITPOS" => self.cmd_bitpos(args, client.db),
            "BITOP" => self.cmd_bitop(args, client.db),
            "BITFIELD" | "BITFIELD_RO" => self.cmd_bitfield(&name, args, client.db),
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
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "SSUBSCRIBE"
            | "SUNSUBSCRIBE" | "PUBLISH" | "SPUBLISH" | "PUBSUB" => {
                self.cmd_pubsub(&name, args, client)
            }
            "SLAVEOF" | "REPLICAOF" => self.cmd_replicaof(args),
            "REPLCONF" => self.cmd_replconf(args),
            "ROLE" => self.cmd_role(),
            "LFENCE" => Outcome::ok(),
            "STRALGO" => self.cmd_stralgo(args, client.db),
            "WAIT" => self.cmd_wait(args),
            "PFADD" => self.cmd_pfadd(args, client.db),
            "PFCOUNT" => self.cmd_pfcount(args, client.db),
            "PFMERGE" => self.cmd_pfmerge(args, client.db),
            "PFDEBUG" => self.cmd_pfdebug(args, client.db),
            "PFSELFTEST" => {
                if args.len() != 1 {
                    wrong_args(args)
                } else {
                    self.cmd_pfselftest()
                }
            }
            "XADD" => self.cmd_xadd(args, client.db),
            "XLEN" => self.cmd_xlen(args, client.db),
            "XRANGE" => self.cmd_xrange(&name, args, client.db),
            "XREVRANGE" => self.cmd_xrange(&name, args, client.db),
            "XDEL" => self.cmd_xdel(args, client.db),
            "XTRIM" => self.cmd_xtrim(args, client.db),
            "XSETID" => self.cmd_xsetid(args, client.db),
            "XREAD" | "XREADGROUP" => self.cmd_xread(&name, args, client.db, client.version),
            "XGROUP" => self.cmd_xgroup(args, client.db),
            "XACK" => self.cmd_xack(args, client.db),
            "XPENDING" => self.cmd_xpending(args, client.db),
            "XCLAIM" => self.cmd_xclaim(args, client.db),
            "XAUTOCLAIM" => self.cmd_xautoclaim(args, client.db),
            "XINFO" => self.cmd_xinfo(args, client.db),
            "GEOADD" => self.cmd_geoadd(args, client.db, client.version),
            "GEOPOS" => self.cmd_geopos(args, client.db),
            "GEOHASH" => self.cmd_geohash(args, client.db),
            "GEODIST" => self.cmd_geodist(args, client.db),
            "GEORADIUS"
            | "GEORADIUS_RO"
            | "GEORADIUSBYMEMBER"
            | "GEORADIUSBYMEMBER_RO"
            | "GEOSEARCH"
            | "GEOSEARCHSTORE" => self.cmd_geo_search(&name, args, client.db),
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
            "INCRBYFLOAT" => self.cmd_incrbyfloat(args, client.db),
            "MSETNX" => self.cmd_msetnx(args, client.db),
            "SUBSTR" => self.cmd_getrange(args, client.db),
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
            "MOVE" => self.cmd_move(args, client.db),
            "COPY" => self.cmd_copy(args, client.db),
            "SWAPDB" => self.cmd_swapdb(args),
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
                    Ok(()) => {
                        self.rdb_bgsave_in_progress = true;
                        Outcome::reply(RespValue::simple("Background saving started"))
                    }
                    Err(err) => Outcome::error(format!("ERR {err}")),
                }
            }
            "BGREWRITEAOF" => {
                if args.len() != 1 {
                    return wrong_args(args);
                }
                if self.aof_rewrite_in_progress {
                    return Outcome::error(
                        "ERR Background append only file rewriting already in progress",
                    );
                }
                if self.rdb_bgsave_in_progress {
                    self.aof_rewrite_scheduled = true;
                    return Outcome::reply(RespValue::simple(
                        "Background append only file rewriting scheduled",
                    ));
                }
                match self.rewrite_aof() {
                    Ok(()) => {
                        self.aof_rewrite_in_progress = true;
                        Outcome::reply(RespValue::simple(
                            "Background append only file rewriting started",
                        ))
                    }
                    Err(err) => Outcome::error(format!("ERR {err}")),
                }
            }
            "INFO" => self.cmd_info(args),
            "COMMAND" => self.cmd_command(args),
            "CLIENT" => self.cmd_client(args, client),
            "HELLO" => self.cmd_hello(args, client),
            "AUTH" => self.cmd_auth(args),
            "LASTSAVE" => Outcome::reply(RespValue::Integer(self.start_unix)),
            "TIME" => {
                let now = self.now_ms();
                Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk((now / 1000).to_string().into_bytes()),
                    RespValue::bulk(((now % 1000) * 1000).to_string().into_bytes()),
                ]))
            }
            "RESET" => {
                let id = client.id;
                self.unregister_client(id);
                self.client_subs.remove(&id);
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
                aof_commands: None,
                extra: Vec::new(),
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

    fn cmd_getex(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::null());
        };
        let Some(value) = entry.value.as_string().cloned() else {
            return wrong_type();
        };
        if args.len() > 2 {
            let option = String::from_utf8_lossy(&args[2]).to_ascii_uppercase();
            let now = self.now_ms();
            let expire_at = match option.as_str() {
                "PERSIST" => {
                    if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
                        entry.expire.remove_primary();
                    }
                    None
                }
                "EX" | "PX" | "EXAT" | "PXAT" => {
                    let Some(amount) = args.get(3) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Ok(amount) = parse_i64(amount) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    let when = match option.as_str() {
                        "EX" => now + amount * 1000,
                        "PX" => now + amount,
                        "EXAT" => amount * 1000,
                        _ => amount,
                    };
                    if when <= now {
                        return Outcome::error("ERR invalid expire time in 'getex' command");
                    }
                    if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
                        entry.expire.set_primary(when);
                        self.has_expiries = true;
                    }
                    Some(when)
                }
                _ => return Outcome::error("ERR syntax error"),
            };
            let _ = expire_at;
        } else if args.len() > 2 {
            return Outcome::error("ERR syntax error");
        }
        Outcome::reply(RespValue::bulk(value))
    }

    fn cmd_setnx(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        self.expire_if_needed(db_index, &args[1]);
        if self.dbs[db_index].contains_key(&args[1]) {
            return Outcome::reply(RespValue::Integer(0));
        }
        let mvcc = self.clock.next(self.now_ms());
        self.dbs[db_index].insert(&args[1], Value::String(args[2].clone()), mvcc);
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_setex(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        let Ok(amount) = parse_i64(&args[2]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        if amount <= 0 {
            return Outcome::error(format!(
                "ERR invalid expire time in '{}' command",
                command.to_ascii_lowercase()
            ));
        }
        let now = self.now_ms();
        let when = if command == "SETEX" {
            now.saturating_add(amount.saturating_mul(1000))
        } else {
            now.saturating_add(amount)
        };
        let mut expire = ExpireEntry::new();
        expire.set_primary(when);
        self.has_expiries = true;
        let mvcc = self.clock.next(now);
        self.dbs[db_index].insert_entry(
            args[1].clone(),
            Entry::with_expire(Arc::new(Value::String(args[3].clone())), expire, mvcc),
        );
        Outcome::ok()
    }

    fn cmd_incrbyfloat(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let Some(increment) = std::str::from_utf8(&args[2])
            .ok()
            .and_then(|text| text.parse::<f64>().ok())
        else {
            return Outcome::error("ERR value is not a valid float");
        };
        if !increment.is_finite() {
            return Outcome::error("ERR increment would produce NaN or Infinity");
        }
        self.expire_if_needed(db_index, &args[1]);
        let (current, expire) = match self.dbs[db_index].get(&args[1]) {
            Some(entry) => match entry.value.as_string() {
                Some(bytes) => match std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|text| text.parse::<f64>().ok())
                {
                    Some(value) => (value, entry.expire.clone()),
                    None => return Outcome::error("ERR value is not a valid float"),
                },
                None => return wrong_type(),
            },
            None => (0.0, ExpireEntry::new()),
        };
        let value = current + increment;
        if !value.is_finite() {
            return Outcome::error("ERR increment would produce NaN or Infinity");
        }
        let text = format_float(value);
        let mvcc = self.clock.next(self.now_ms());
        self.dbs[db_index].insert_entry(
            args[1].clone(),
            Entry::with_expire(
                Arc::new(Value::String(text.clone().into_bytes())),
                expire,
                mvcc,
            ),
        );
        Outcome::reply(RespValue::bulk(text.into_bytes()))
    }

    fn cmd_msetnx(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return wrong_args(args);
        }
        for pair in args[1..].chunks(2) {
            self.expire_if_needed(db_index, &pair[0]);
            if self.dbs[db_index].contains_key(&pair[0]) {
                return Outcome::reply(RespValue::Integer(0));
            }
        }
        let mvcc = self.clock.next(self.now_ms());
        for pair in args[1..].chunks(2) {
            self.dbs[db_index].insert(&pair[0], Value::String(pair[1].clone()), mvcc);
        }
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_move(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let Ok(destination) = parse_usize(&args[2]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        if destination >= self.dbs.len() {
            return Outcome::error("ERR DB index is out of range");
        }
        if destination == db_index {
            return Outcome::error("ERR source and destination objects are the same");
        }
        self.expire_if_needed(db_index, &args[1]);
        self.expire_if_needed(destination, &args[1]);
        if self.dbs[destination].contains_key(&args[1]) {
            return Outcome::reply(RespValue::Integer(0));
        }
        let Some(entry) = self.dbs[db_index].get(&args[1]).cloned() else {
            return Outcome::reply(RespValue::Integer(0));
        };
        self.dbs[db_index].remove(&args[1]);
        self.dbs[destination].insert_entry(args[1].clone(), entry);
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_copy(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let mut destination_db = db_index;
        let mut replace = false;
        let mut index = 3;
        while index < args.len() {
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "REPLACE" => {
                    replace = true;
                    index += 1;
                }
                "DB" => {
                    let Some(value) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    let Ok(db) = parse_usize(value) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    if db >= self.dbs.len() {
                        return Outcome::error("ERR DB index is out of range");
                    }
                    destination_db = db;
                    index += 2;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        self.expire_if_needed(db_index, &args[1]);
        self.expire_if_needed(destination_db, &args[2]);
        let Some(entry) = self.dbs[db_index].get(&args[1]).cloned() else {
            return Outcome::reply(RespValue::Integer(0));
        };
        if self.dbs[destination_db].contains_key(&args[2]) && !replace {
            return Outcome::reply(RespValue::Integer(0));
        }
        let mvcc = self.clock.next(self.now_ms());
        let mut entry = entry;
        entry.mvcc = mvcc;
        self.dbs[destination_db].insert_entry(args[2].clone(), entry);
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_swapdb(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let (Ok(first), Ok(second)) = (parse_usize(&args[1]), parse_usize(&args[2])) else {
            return Outcome::error("ERR invalid DB index");
        };
        if first >= self.dbs.len() || second >= self.dbs.len() {
            return Outcome::error("ERR DB index is out of range");
        }
        self.dbs.swap(first, second);
        Outcome::ok()
    }

    fn cmd_stralgo(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        if !args[1].eq_ignore_ascii_case(b"lcs") {
            return Outcome::error("ERR syntax error");
        }
        let mut getlen = false;
        let mut getidx = false;
        let mut withmatchlen = false;
        let mut minmatchlen = 0usize;
        let mut strings: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut index = 2;
        while index < args.len() {
            let remaining = args.len() - 1 - index;
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "IDX" => {
                    getidx = true;
                    index += 1;
                }
                "LEN" => {
                    getlen = true;
                    index += 1;
                }
                "WITHMATCHLEN" => {
                    withmatchlen = true;
                    index += 1;
                }
                "MINMATCHLEN" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    minmatchlen = value.max(0) as usize;
                    index += 2;
                }
                "STRINGS" if remaining > 1 => {
                    if strings.is_some() {
                        return Outcome::error("ERR Either use STRINGS or KEYS");
                    }
                    strings = Some((args[index + 1].clone(), args[index + 2].clone()));
                    index += 3;
                }
                "KEYS" if remaining > 1 => {
                    if strings.is_some() {
                        return Outcome::error("ERR Either use STRINGS or KEYS");
                    }
                    let mut values = Vec::with_capacity(2);
                    for key in &args[index + 1..index + 3] {
                        self.expire_if_needed(db_index, key);
                        let value = match self.dbs[db_index].get(key) {
                            Some(entry) => match entry.value.as_string() {
                                Some(bytes) => bytes.clone(),
                                None => {
                                    return Outcome::error(
                                        "ERR The specified keys must contain string values",
                                    );
                                }
                            },
                            None => Vec::new(),
                        };
                        values.push(value);
                    }
                    strings = Some((values[0].clone(), values[1].clone()));
                    index += 3;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let Some((a, b)) = strings else {
            return Outcome::error(
                "ERR Please specify two strings: STRINGS or KEYS options are mandatory",
            );
        };
        if getlen && getidx {
            return Outcome::error(
                "ERR If you want both the length and indexes, please just use IDX.",
            );
        }
        if a.len() >= u32::MAX as usize - 1 || b.len() >= u32::MAX as usize - 1 {
            return Outcome::error("ERR String too long for LCS");
        }
        let alen = a.len();
        let blen = b.len();
        let width = blen + 1;
        let mut table = vec![0u32; (alen + 1) * width];
        for i in 1..=alen {
            for j in 1..=blen {
                table[i * width + j] = if a[i - 1] == b[j - 1] {
                    table[(i - 1) * width + j - 1] + 1
                } else {
                    table[(i - 1) * width + j].max(table[i * width + j - 1])
                };
            }
        }
        let lcs_len = table[alen * width + blen] as usize;
        let compute = getidx || !getlen;
        let mut result = vec![0u8; lcs_len];
        let mut matches: Vec<RespValue> = Vec::new();
        let mut a_start = alen;
        let mut a_end = 0usize;
        let mut b_start = 0usize;
        let mut b_end = 0usize;
        let mut i = alen;
        let mut j = blen;
        let mut idx = lcs_len;
        while compute && i > 0 && j > 0 {
            let mut emit = false;
            if a[i - 1] == b[j - 1] {
                idx -= 1;
                result[idx] = a[i - 1];
                if a_start == alen {
                    a_start = i - 1;
                    a_end = i - 1;
                    b_start = j - 1;
                    b_end = j - 1;
                } else if a_start == i && b_start == j {
                    a_start -= 1;
                    b_start -= 1;
                } else {
                    emit = true;
                }
                if a_start == 0 || b_start == 0 {
                    emit = true;
                }
                i -= 1;
                j -= 1;
            } else {
                let first = table[(i - 1) * width + j];
                let second = table[i * width + j - 1];
                if first > second {
                    i -= 1;
                } else {
                    j -= 1;
                }
                if a_start != alen {
                    emit = true;
                }
            }
            if emit {
                let match_len = a_end - a_start + 1;
                if minmatchlen == 0 || match_len >= minmatchlen {
                    let mut item = vec![
                        RespValue::Array(vec![
                            RespValue::Integer(a_start as i64),
                            RespValue::Integer(a_end as i64),
                        ]),
                        RespValue::Array(vec![
                            RespValue::Integer(b_start as i64),
                            RespValue::Integer(b_end as i64),
                        ]),
                    ];
                    if withmatchlen {
                        item.push(RespValue::Integer(match_len as i64));
                    }
                    matches.push(RespValue::Array(item));
                }
                a_start = alen;
            }
        }
        if getidx {
            return Outcome::reply(RespValue::Map(vec![
                (
                    RespValue::bulk(b"matches".to_vec()),
                    RespValue::Array(matches),
                ),
                (
                    RespValue::bulk(b"len".to_vec()),
                    RespValue::Integer(lcs_len as i64),
                ),
            ]));
        }
        if getlen {
            return Outcome::reply(RespValue::Integer(lcs_len as i64));
        }
        Outcome::reply(RespValue::bulk(result))
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
        if nx && get {
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
                self.has_expiries = true;
            }
            expire
        };
        let mvcc = self.clock.next(now);
        self.raw_string_keys.remove(&(client.db, args[1].clone()));
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
            if self.loading {
                if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
                    entry.expire.set_primary(when);
                }
                return Outcome::reply(RespValue::Integer(1));
            }
            self.dbs[db_index].remove(&args[1]);
            return Outcome::reply(RespValue::Integer(1));
        }
        if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
            entry.expire.set_primary(when);
            self.has_expiries = true;
        }
        Outcome::reply(RespValue::Integer(1))
    }

    fn cmd_subexpire(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        let Ok(amount) = parse_i64(&args[3]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        let now = self.now_ms();
        let when = match command {
            "EXPIREMEMBER" => now.saturating_add(amount.saturating_mul(1000)),
            "PEXPIREMEMBER" => now.saturating_add(amount),
            "EXPIREMEMBERAT" => amount.saturating_mul(1000),
            "PEXPIREMEMBERAT" => amount,
            _ => unreachable!(),
        };
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::Integer(0));
        };
        if entry.value.as_set().is_some() {
            if when <= now {
                return Outcome::reply(RespValue::Integer(i64::from(
                    self.remove_set_members(db_index, &args[1], &[args[2].clone()]) > 0,
                )));
            }
            return Outcome::reply(RespValue::Integer(1));
        }
        if entry.value.as_hash().is_some() {
            if when <= now {
                return Outcome::reply(RespValue::Integer(i64::from(
                    self.remove_hash_fields(db_index, &args[1], &[args[2].clone()]) > 0,
                )));
            }
            if let Some(entry) = self.dbs[db_index].get_mut(&args[1]) {
                entry.expire.set_subkey(&args[2], when);
            }
            return Outcome::reply(RespValue::Integer(1));
        }
        wrong_type()
    }

    fn cmd_setbit(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        let Ok(offset) = parse_u64(&args[2]) else {
            return Outcome::error("ERR bit offset is not an integer or out of range");
        };
        if offset >> 3 >= self.configured_max_bulk_len() {
            return Outcome::error("ERR bit offset is not an integer or out of range");
        }
        let Ok(bit) = parse_i64(&args[3]) else {
            return Outcome::error("ERR bit is not an integer or out of range");
        };
        if bit != 0 && bit != 1 {
            return Outcome::error("ERR bit is not an integer or out of range");
        }
        let mvcc = self.clock.next(self.now_ms());
        if !self.dbs[db_index].contains_key(&args[1]) {
            self.dbs[db_index].insert(&args[1], Value::String(Vec::new()), mvcc);
        }
        let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
            return wrong_type();
        };
        entry.mvcc = mvcc;
        let value = Arc::make_mut(&mut entry.value);
        let Some(bytes) = value.as_string_mut() else {
            return wrong_type();
        };
        let byte = (offset / 8) as usize;
        let mask = 1 << (7 - (offset % 8));
        if bytes.len() <= byte {
            bytes.resize(byte + 1, 0);
        }
        let old = i64::from(bytes[byte] & mask != 0);
        if bit == 1 {
            bytes[byte] |= mask;
        } else {
            bytes[byte] &= !mask;
        }
        Outcome::reply(RespValue::Integer(old))
    }

    fn cmd_getbit(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let Ok(offset) = parse_u64(&args[2]) else {
            return Outcome::error("ERR bit offset is not an integer or out of range");
        };
        if offset >> 3 >= self.configured_max_bulk_len() {
            return Outcome::error("ERR bit offset is not an integer or out of range");
        }
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::Integer(0));
        };
        let Some(bytes) = entry.value.as_string() else {
            return wrong_type();
        };
        let byte = (offset / 8) as usize;
        if byte >= bytes.len() {
            return Outcome::reply(RespValue::Integer(0));
        }
        let mask = 1 << (7 - (offset % 8));
        Outcome::reply(RespValue::Integer(i64::from(bytes[byte] & mask != 0)))
    }

    fn cmd_bitcount(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 2 && args.len() != 4 {
            return Outcome::error("ERR syntax error");
        }
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::Integer(0));
        };
        let Some(bytes) = entry.value.as_string() else {
            return wrong_type();
        };
        let (start, end) = if args.len() == 2 {
            (0i64, bytes.len() as i64 - 1)
        } else {
            let (Ok(start), Ok(end)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            if start < 0 && end < 0 && start > end {
                return Outcome::reply(RespValue::Integer(0));
            }
            let len = bytes.len() as i64;
            let mut start = if start < 0 { len + start } else { start };
            let mut end = if end < 0 { len + end } else { end };
            if start < 0 {
                start = 0;
            }
            if end < 0 {
                end = 0;
            }
            if end >= len {
                end = len - 1;
            }
            (start, end)
        };
        if start > end || bytes.is_empty() {
            return Outcome::reply(RespValue::Integer(0));
        }
        let count: u32 = bytes[start as usize..=end as usize]
            .iter()
            .map(|byte| byte.count_ones())
            .sum();
        Outcome::reply(RespValue::Integer(i64::from(count)))
    }

    fn cmd_bitpos(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 || args.len() > 5 {
            return wrong_args(args);
        }
        let Ok(bit) = parse_i64(&args[2]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        if bit != 0 && bit != 1 {
            return Outcome::error("ERR The bit argument must be 1 or 0.");
        }
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::Integer(if bit == 0 { 0 } else { -1 }));
        };
        let Some(bytes) = entry.value.as_string() else {
            return wrong_type();
        };
        let len = bytes.len() as i64;
        let mut start = 0i64;
        let mut end = len - 1;
        let end_given = args.len() == 5;
        if args.len() >= 4 {
            let Ok(value) = parse_i64(&args[3]) else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            start = if value < 0 { len + value } else { value };
            if start < 0 {
                start = 0;
            }
        }
        if end_given {
            let Ok(value) = parse_i64(&args[4]) else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            end = if value < 0 { len + value } else { value };
            if end >= len {
                end = len - 1;
            }
        }
        if start > end || start >= len {
            return Outcome::reply(RespValue::Integer(-1));
        }
        for byte_index in start..=end {
            let byte = bytes[byte_index as usize];
            if bit == 1 {
                if byte != 0 {
                    for bit_index in 0..8 {
                        if byte & (1 << (7 - bit_index)) != 0 {
                            return Outcome::reply(RespValue::Integer(byte_index * 8 + bit_index));
                        }
                    }
                }
            } else if byte != 0xFF {
                for bit_index in 0..8 {
                    if byte & (1 << (7 - bit_index)) == 0 {
                        return Outcome::reply(RespValue::Integer(byte_index * 8 + bit_index));
                    }
                }
            }
        }
        if bit == 0 && !end_given {
            return Outcome::reply(RespValue::Integer(len * 8));
        }
        Outcome::reply(RespValue::Integer(-1))
    }

    fn cmd_bitop(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let op = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        let shift_op = matches!(op.as_str(), "LSHIFT" | "RSHIFT");
        if op == "NOT" && args.len() != 4 {
            return Outcome::error("ERR BITOP NOT must be called with a single source key.");
        }
        if shift_op && args.len() != 5 {
            return Outcome::error(
                "ERR BITOP SHIFT must be called with a single source key and an integer shift.",
            );
        }
        if !matches!(
            op.as_str(),
            "AND" | "OR" | "XOR" | "NOT" | "LSHIFT" | "RSHIFT"
        ) {
            return Outcome::error("ERR syntax error");
        }
        let source_start = 3;
        let source_end = if shift_op { 4 } else { args.len() };
        let mut sources = Vec::new();
        for key in &args[source_start..source_end] {
            self.expire_if_needed(db_index, key);
            match self.dbs[db_index].get(key) {
                Some(entry) => match entry.value.as_string() {
                    Some(bytes) => sources.push(bytes.clone()),
                    None => return wrong_type(),
                },
                None => sources.push(Vec::new()),
            }
        }
        let result: Vec<u8> = if shift_op {
            let Some(shift_text) = args.get(4) else {
                return Outcome::error("ERR BITOP SHIFT's last parameter must be an integer");
            };
            let Ok(mut shift) = parse_i64(shift_text) else {
                return Outcome::error("ERR BITOP SHIFT's last parameter must be an integer");
            };
            if op == "RSHIFT" {
                shift = -shift;
            }
            if !(-(1 << 30)..=1 << 30).contains(&shift) {
                return Outcome::error("ERR BITOP SHIFT value is too large.");
            }
            shift_bits(&sources[0], shift)
        } else {
            let maxlen = sources.iter().map(Vec::len).max().unwrap_or(0);
            (0..maxlen)
                .map(|index| {
                    let mut value = if op == "NOT" {
                        !sources[0].get(index).copied().unwrap_or(0)
                    } else {
                        sources[0].get(index).copied().unwrap_or(0)
                    };
                    if op != "NOT" {
                        for source in &sources[1..] {
                            let byte = source.get(index).copied().unwrap_or(0);
                            value = match op.as_str() {
                                "AND" => value & byte,
                                "OR" => value | byte,
                                "XOR" => value ^ byte,
                                _ => unreachable!(),
                            };
                        }
                    }
                    value
                })
                .collect()
        };
        let destination = &args[2];
        let mvcc = self.clock.next(self.now_ms());
        if result.is_empty() {
            self.dbs[db_index].remove(destination);
        } else {
            self.dbs[db_index].insert(destination, Value::String(result.clone()), mvcc);
        }
        Outcome::reply(RespValue::Integer(result.len() as i64))
    }

    fn configured_max_bulk_len(&self) -> u64 {
        let text = self
            .config_values
            .get("proto-max-bulk-len")
            .map(String::as_str)
            .unwrap_or("512mb");
        parse_memory(text).unwrap_or(512 * 1024 * 1024)
    }

    fn cmd_bitfield(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let read_only = command == "BITFIELD_RO";
        let mut overflow = BitOverflow::Wrap;
        let mut results = Vec::new();
        let mut index = 2;
        while index < args.len() {
            let operation = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match operation.as_str() {
                "OVERFLOW" => {
                    let Some(mode) = args.get(index + 1) else {
                        return Outcome::error("ERR syntax error");
                    };
                    overflow = match String::from_utf8_lossy(mode).to_ascii_uppercase().as_str() {
                        "WRAP" => BitOverflow::Wrap,
                        "SAT" => BitOverflow::Sat,
                        "FAIL" => BitOverflow::Fail,
                        _ => return Outcome::error("ERR syntax error"),
                    };
                    index += 2;
                }
                "GET" => {
                    if index + 2 >= args.len() {
                        return Outcome::error("ERR syntax error");
                    }
                    let Some((signed, bits)) = parse_bitfield_type(&args[index + 1]) else {
                        return Outcome::error(
                            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
                        );
                    };
                    let Some(offset) = self.parse_bitfield_offset(&args[index + 2], bits) else {
                        return Outcome::error("ERR bit offset is not an integer or out of range");
                    };
                    self.expire_if_needed(db_index, &args[1]);
                    let value = match self.dbs[db_index].get(&args[1]) {
                        Some(entry) => match entry.value.as_string() {
                            Some(bytes) => read_bitfield(bytes, offset, bits),
                            None => return wrong_type(),
                        },
                        None => 0,
                    };
                    results.push(RespValue::Integer(bitfield_value(value, signed, bits)));
                    index += 3;
                }
                "SET" | "INCRBY" => {
                    if read_only {
                        return Outcome::error("ERR BITFIELD_RO only supports the GET subcommand");
                    }
                    if index + 3 >= args.len() {
                        return Outcome::error("ERR syntax error");
                    }
                    let Some((signed, bits)) = parse_bitfield_type(&args[index + 1]) else {
                        return Outcome::error(
                            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
                        );
                    };
                    let Some(offset) = self.parse_bitfield_offset(&args[index + 2], bits) else {
                        return Outcome::error("ERR bit offset is not an integer or out of range");
                    };
                    let Ok(argument) = parse_i64(&args[index + 3]) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    let mvcc = self.clock.next(self.now_ms());
                    if !self.dbs[db_index].contains_key(&args[1]) {
                        self.dbs[db_index].insert(&args[1], Value::String(Vec::new()), mvcc);
                    }
                    let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
                        return wrong_type();
                    };
                    entry.mvcc = mvcc;
                    let value = Arc::make_mut(&mut entry.value);
                    let Some(bytes) = value.as_string_mut() else {
                        return wrong_type();
                    };
                    let old = read_bitfield(bytes, offset, bits);
                    let old_value = bitfield_value(old, signed, bits);
                    if operation == "SET" {
                        let (minimum, maximum) = bitfield_limits(signed, bits);
                        let requested = i128::from(argument);
                        let stored = if requested < minimum || requested > maximum {
                            match overflow {
                                BitOverflow::Fail => {
                                    results.push(RespValue::Null);
                                    index += 4;
                                    continue;
                                }
                                BitOverflow::Sat => {
                                    if requested < minimum {
                                        minimum
                                    } else {
                                        maximum
                                    }
                                }
                                BitOverflow::Wrap => wrap_bitfield(requested, signed, bits),
                            }
                        } else {
                            requested
                        };
                        ensure_bitfield_len(bytes, offset, bits);
                        write_bitfield(bytes, offset, bits, stored, signed);
                        results.push(RespValue::Integer(old_value));
                    } else {
                        let (minimum, maximum) = bitfield_limits(signed, bits);
                        let new_value = i128::from(old_value) + i128::from(argument);
                        let stored = if new_value < minimum || new_value > maximum {
                            match overflow {
                                BitOverflow::Fail => {
                                    results.push(RespValue::Null);
                                    index += 4;
                                    continue;
                                }
                                BitOverflow::Sat => {
                                    if new_value < minimum {
                                        minimum
                                    } else {
                                        maximum
                                    }
                                }
                                BitOverflow::Wrap => {
                                    let masked = wrap_bitfield(new_value, signed, bits);
                                    ensure_bitfield_len(bytes, offset, bits);
                                    write_bitfield(bytes, offset, bits, masked, signed);
                                    results.push(RespValue::Integer(bitfield_value(
                                        masked as u64,
                                        signed,
                                        bits,
                                    )));
                                    index += 4;
                                    continue;
                                }
                            }
                        } else {
                            new_value
                        };
                        ensure_bitfield_len(bytes, offset, bits);
                        write_bitfield(bytes, offset, bits, stored, signed);
                        results.push(RespValue::Integer(bitfield_value(
                            stored as u64,
                            signed,
                            bits,
                        )));
                    }
                    index += 4;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        Outcome::reply(RespValue::Array(results))
    }

    fn parse_bitfield_offset(&self, value: &[u8], bits: u8) -> Option<u64> {
        let text = std::str::from_utf8(value).ok()?;
        let (text, hash) = if let Some(rest) = text.strip_prefix('#') {
            (rest, true)
        } else {
            (text, false)
        };
        let offset = text.parse::<i64>().ok()?;
        if offset < 0 {
            return None;
        }
        let offset = if hash {
            offset.checked_mul(i64::from(bits))?
        } else {
            offset
        };
        let offset = u64::try_from(offset).ok()?;
        if offset >> 3 >= self.configured_max_bulk_len() {
            return None;
        }
        Some(offset)
    }

    fn cmd_setrange(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        let offset = match parse_i64(&args[2]) {
            Ok(offset) if offset >= 0 => offset as usize,
            _ => return Outcome::error("ERR offset is out of range"),
        };
        let max_len = self.configured_max_bulk_len() as usize;
        let end = match offset.checked_add(args[3].len()) {
            Some(end) if end <= max_len => end,
            _ => {
                return Outcome::error(
                    "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                );
            }
        };
        self.expire_if_needed(db_index, &args[1]);
        let existing = self.dbs[db_index].get(&args[1]);
        if args[3].is_empty() {
            return match existing {
                Some(entry) => match entry.value.as_string() {
                    Some(bytes) => Outcome::reply(RespValue::Integer(bytes.len() as i64)),
                    None => wrong_type(),
                },
                None => Outcome::reply(RespValue::Integer(0)),
            };
        }
        if existing.is_some_and(|entry| entry.value.as_string().is_none()) {
            return wrong_type();
        }
        let mvcc = self.clock.next(self.now_ms());
        if !self.dbs[db_index].contains_key(&args[1]) {
            self.dbs[db_index].insert(&args[1], Value::String(Vec::new()), mvcc);
        }
        let Some(entry) = self.dbs[db_index].get_mut(&args[1]) else {
            return wrong_type();
        };
        entry.mvcc = mvcc;
        let value = Arc::make_mut(&mut entry.value);
        let Some(bytes) = value.as_string_mut() else {
            return wrong_type();
        };
        if bytes.len() < end {
            bytes.resize(end, 0);
        }
        bytes[offset..end].copy_from_slice(&args[3]);
        let len = bytes.len() as i64;
        self.raw_string_keys.insert((db_index, args[1].clone()));
        Outcome::reply(RespValue::Integer(len))
    }

    fn cmd_getrange(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 4 {
            return wrong_args(args);
        }
        let (Ok(start), Ok(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        self.expire_if_needed(db_index, &args[1]);
        let Some(entry) = self.dbs[db_index].get(&args[1]) else {
            return Outcome::reply(RespValue::bulk(Vec::new()));
        };
        let Some(bytes) = entry.value.as_string() else {
            return wrong_type();
        };
        let len = bytes.len() as i64;
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop || start >= len || stop < 0 {
            return Outcome::reply(RespValue::bulk(Vec::new()));
        }
        Outcome::reply(RespValue::bulk(
            bytes[start as usize..=stop as usize].to_vec(),
        ))
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
                kind: BlockKind::Move { rpoplpush: true },
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
                kind: BlockKind::Move { rpoplpush: false },
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
        match &request.kind {
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
                        let members = sorted_members(&zset, *max);
                        let (member, score) = &members[0];
                        self.remove_zset_members(request.db, key, std::slice::from_ref(member));
                        let command = if *max {
                            b"ZPOPMAX".to_vec()
                        } else {
                            b"ZPOPMIN".to_vec()
                        };
                        self.propagate(request.db, &[command, key.clone()]);
                        return Some(RespValue::Array(vec![
                            RespValue::bulk(key.clone()),
                            RespValue::bulk(member.clone()),
                            score_value(*score, RespVersion::V2),
                        ]));
                    }
                }
                None
            }
            BlockKind::StreamRead(block) => {
                for (index, key) in request.keys.iter().enumerate() {
                    self.expire_if_needed(request.db, key);
                    let mut missing_group = false;
                    let mut missing_key = false;
                    let (group_last, have_data) = match self.dbs[request.db].get(key) {
                        Some(entry) => match entry.value.as_stream() {
                            Some(stream) => {
                                if let Some(group_name) = &block.group {
                                    match stream.groups.get(group_name) {
                                        Some(group) => (
                                            Some(group.last_delivered_id),
                                            stream.last_valid_id().is_some_and(|maxid| {
                                                maxid > group.last_delivered_id
                                            }),
                                        ),
                                        None => {
                                            missing_group = true;
                                            (None, false)
                                        }
                                    }
                                } else {
                                    let gt = block
                                        .ids
                                        .get(index)
                                        .and_then(|id| *id)
                                        .unwrap_or(StreamId::MIN);
                                    (None, stream.last_valid_id().is_some_and(|maxid| maxid > gt))
                                }
                            }
                            None => (None, false),
                        },
                        None => {
                            missing_key = true;
                            (None, false)
                        }
                    };
                    if missing_group || (missing_key && block.group.is_some()) {
                        return Some(
                            Outcome::error(
                                "NOGROUP the consumer group this client was blocked on no longer exists",
                            )
                            .reply,
                        );
                    }
                    if !have_data {
                        continue;
                    }
                    let start = match group_last {
                        Some(last) => last.next().unwrap_or(StreamId::MAX),
                        None => block
                            .ids
                            .get(index)
                            .and_then(|id| *id)
                            .unwrap_or(StreamId::MIN)
                            .next()
                            .unwrap_or(StreamId::MAX),
                    };
                    let now = self.now_ms();
                    let group_info = match (&block.group, &block.consumer) {
                        (Some(group), Some(consumer)) => {
                            Some((group.as_slice(), consumer.as_slice(), block.noack))
                        }
                        _ => None,
                    };
                    let (entries, commands) = match self.stream_serve_read(
                        request.db,
                        key,
                        start,
                        block.count,
                        group_info,
                        false,
                        now,
                    ) {
                        Ok(result) => result,
                        Err(outcome) => return Some(outcome.reply),
                    };
                    self.append_aof_commands(request.db, &commands);
                    let served = vec![(key.clone(), entries)];
                    return Some(stream_read_reply(&served, block.version));
                }
                None
            }
            BlockKind::Move { rpoplpush } => {
                let destination = request.destination.as_ref()?;
                match self.list_move(
                    request.db,
                    &request.keys[0],
                    destination,
                    request.source_side,
                    request.destination_side,
                ) {
                    Ok(Some(value)) => {
                        if *rpoplpush {
                            self.propagate(
                                request.db,
                                &[
                                    b"RPOPLPUSH".to_vec(),
                                    request.keys[0].clone(),
                                    destination.clone(),
                                ],
                            );
                        } else {
                            let side = |side: ListSide| match side {
                                ListSide::Left => b"LEFT".to_vec(),
                                ListSide::Right => b"RIGHT".to_vec(),
                            };
                            self.propagate(
                                request.db,
                                &[
                                    b"LMOVE".to_vec(),
                                    request.keys[0].clone(),
                                    destination.clone(),
                                    side(request.source_side),
                                    side(request.destination_side),
                                ],
                            );
                        }
                        Some(RespValue::bulk(value))
                    }
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

    fn publish_to_subscribers(
        &mut self,
        channel: &[u8],
        payload: &[u8],
        kind: &[u8],
        patterns: bool,
        sharded: bool,
    ) -> usize {
        let source = if sharded {
            &self.shard_subs
        } else {
            &self.channel_subs
        };
        let direct: Vec<u64> = source
            .get(channel)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
        let mut deliveries = 0usize;
        for id in direct {
            deliveries += 1;
            self.pending_pubsub.push(PubSubDelivery {
                target: id,
                message: push_frame(
                    vec![
                        RespValue::bulk(kind.to_vec()),
                        RespValue::bulk(channel.to_vec()),
                        RespValue::bulk(payload.to_vec()),
                    ],
                    self.client_subs
                        .get(&id)
                        .map_or(RespVersion::V2, |subs| subs.version),
                ),
            });
        }
        if patterns {
            for (pattern, ids) in self.pattern_subs.clone() {
                if !glob_match(&pattern, channel) {
                    continue;
                }
                for id in ids {
                    deliveries += 1;
                    self.pending_pubsub.push(PubSubDelivery {
                        target: id,
                        message: push_frame(
                            vec![
                                RespValue::bulk(b"pmessage".to_vec()),
                                RespValue::bulk(pattern.clone()),
                                RespValue::bulk(channel.to_vec()),
                                RespValue::bulk(payload.to_vec()),
                            ],
                            self.client_subs
                                .get(&id)
                                .map_or(RespVersion::V2, |subs| subs.version),
                        ),
                    });
                }
            }
        }
        deliveries
    }

    fn notify_keyspace_event(&mut self, db: usize, key: &[u8], class: char, event: &str) {
        if self.loading {
            return;
        }
        let flags = self
            .config_values
            .get("notify-keyspace-events")
            .cloned()
            .unwrap_or_default();
        if flags.is_empty() || !(flags.contains('A') || flags.contains(class)) {
            return;
        }
        if flags.contains('K') {
            let mut channel = format!("__keyspace@{db}__:").into_bytes();
            channel.extend_from_slice(key);
            self.publish_to_subscribers(&channel, event.as_bytes(), b"message", true, false);
        }
        if flags.contains('E') {
            let channel = format!("__keyevent@{db}__:{event}").into_bytes();
            self.publish_to_subscribers(&channel, key, b"message", true, false);
        }
    }

    fn notify_command(&mut self, command: &Command, outcome: &Outcome, db: usize) {
        if self.loading || matches!(outcome.reply, RespValue::Error(_)) {
            return;
        }
        let args = &command.args;
        if args.is_empty() {
            return;
        }
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        let integer = match &outcome.reply {
            RespValue::Integer(value) => Some(*value),
            _ => None,
        };
        let not_null = !matches!(outcome.reply, RespValue::Null | RespValue::NullArray);
        match name.as_str() {
            "SET" => {
                if not_null {
                    self.notify_keyspace_event(db, &args[1], '$', "set");
                }
            }
            "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" | "GETSET" | "GETDEL" => {
                if not_null {
                    let keys: Vec<&Vec<u8>> = if name == "MSET" || name == "MSETNX" {
                        args[1..].iter().step_by(2).collect()
                    } else {
                        vec![&args[1]]
                    };
                    for key in keys {
                        self.notify_keyspace_event(db, key, '$', "set");
                    }
                }
            }
            "APPEND" | "SETRANGE" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" => {
                if not_null {
                    self.notify_keyspace_event(db, &args[1], '$', &name.to_ascii_lowercase());
                }
            }
            "DEL" | "UNLINK" => {
                if integer.unwrap_or(0) > 0 {
                    for key in &args[1..] {
                        self.notify_keyspace_event(db, key, 'g', &name.to_ascii_lowercase());
                    }
                }
            }
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
                if integer == Some(1) {
                    self.notify_keyspace_event(db, &args[1], 'g', &name.to_ascii_lowercase());
                }
            }
            "PERSIST" => {
                if integer == Some(1) {
                    self.notify_keyspace_event(db, &args[1], 'g', "persist");
                }
            }
            "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LINSERT" | "LSET" | "LTRIM" | "LREM" => {
                if not_null {
                    self.notify_keyspace_event(db, &args[1], 'l', &name.to_ascii_lowercase());
                }
            }
            "LPOP" | "RPOP" => {
                if not_null {
                    self.notify_keyspace_event(db, &args[1], 'l', &name.to_ascii_lowercase());
                }
            }
            "SADD" | "SREM" => {
                if integer.unwrap_or(0) > 0 {
                    self.notify_keyspace_event(db, &args[1], 's', &name.to_ascii_lowercase());
                }
            }
            "ZADD" | "ZREM" | "ZINCRBY" => {
                if integer.unwrap_or(0) > 0 || name == "ZINCRBY" {
                    self.notify_keyspace_event(db, &args[1], 'z', &name.to_ascii_lowercase());
                }
            }
            "HSET" | "HMSET" => {
                if name == "HMSET" || integer.unwrap_or(0) > 0 {
                    self.notify_keyspace_event(db, &args[1], 'h', "hset");
                }
            }
            "HINCRBY" | "HINCRBYFLOAT" => {
                self.notify_keyspace_event(db, &args[1], 'h', &name.to_ascii_lowercase());
            }
            "HDEL" => {
                if integer.unwrap_or(0) > 0 {
                    self.notify_keyspace_event(db, &args[1], 'h', "hdel");
                }
            }
            "XADD" => {
                if not_null {
                    self.notify_keyspace_event(db, &args[1], 't', "xadd");
                }
            }
            "XDEL" | "XTRIM" if integer.unwrap_or(0) > 0 => {
                self.notify_keyspace_event(db, &args[1], 't', &name.to_ascii_lowercase());
            }
            _ => {}
        }
    }

    fn cmd_pubsub(
        &mut self,
        command: &str,
        args: &[Vec<u8>],
        client: &mut ClientContext,
    ) -> Outcome {
        match command {
            "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                if args.len() < 2 {
                    return wrong_args(args);
                }
                let mut frames = Vec::new();
                let mut subs = self.client_subs.remove(&client.id).unwrap_or_default();
                subs.version = client.version;
                for channel in &args[1..] {
                    match command {
                        "SUBSCRIBE" => {
                            subs.channels.insert(channel.clone());
                            self.channel_subs
                                .entry(channel.clone())
                                .or_default()
                                .insert(client.id);
                        }
                        "PSUBSCRIBE" => {
                            subs.patterns.insert(channel.clone());
                            self.pattern_subs
                                .entry(channel.clone())
                                .or_default()
                                .insert(client.id);
                        }
                        _ => {
                            subs.shards.insert(channel.clone());
                            self.shard_subs
                                .entry(channel.clone())
                                .or_default()
                                .insert(client.id);
                        }
                    }
                    let (kind, count) = match command {
                        "SUBSCRIBE" => (b"subscribe".to_vec(), subs.total()),
                        "PSUBSCRIBE" => (b"psubscribe".to_vec(), subs.total()),
                        _ => (b"ssubscribe".to_vec(), subs.shards.len()),
                    };
                    frames.push(subscription_frame(
                        &kind,
                        Some(channel),
                        count,
                        client.version,
                    ));
                }
                self.client_subs.insert(client.id, subs);
                Outcome::with_frames(frames)
            }
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" | "SUNSUBSCRIBE" => {
                let mut subs = self.client_subs.remove(&client.id).unwrap_or_default();
                subs.version = client.version;
                let (kind, requested): (&[u8], Vec<Vec<u8>>) = match command {
                    "UNSUBSCRIBE" => (
                        b"unsubscribe",
                        if args.len() > 1 {
                            args[1..].to_vec()
                        } else {
                            subs.channels.iter().cloned().collect()
                        },
                    ),
                    "PUNSUBSCRIBE" => (
                        b"punsubscribe",
                        if args.len() > 1 {
                            args[1..].to_vec()
                        } else {
                            subs.patterns.iter().cloned().collect()
                        },
                    ),
                    _ => (
                        b"sunsubscribe",
                        if args.len() > 1 {
                            args[1..].to_vec()
                        } else {
                            subs.shards.iter().cloned().collect()
                        },
                    ),
                };
                if requested.is_empty() {
                    let count = if command == "SUNSUBSCRIBE" {
                        subs.shards.len()
                    } else {
                        subs.total()
                    };
                    let frame = subscription_frame(kind, None, count, client.version);
                    if !subs.is_empty() {
                        self.client_subs.insert(client.id, subs);
                    }
                    return Outcome::reply(frame);
                }
                let mut frames = Vec::new();
                for channel in &requested {
                    match command {
                        "UNSUBSCRIBE" => {
                            subs.channels.remove(channel);
                            if let Some(ids) = self.channel_subs.get_mut(channel) {
                                ids.remove(&client.id);
                                if ids.is_empty() {
                                    self.channel_subs.remove(channel);
                                }
                            }
                        }
                        "PUNSUBSCRIBE" => {
                            subs.patterns.remove(channel);
                            if let Some(ids) = self.pattern_subs.get_mut(channel) {
                                ids.remove(&client.id);
                                if ids.is_empty() {
                                    self.pattern_subs.remove(channel);
                                }
                            }
                        }
                        _ => {
                            subs.shards.remove(channel);
                            if let Some(ids) = self.shard_subs.get_mut(channel) {
                                ids.remove(&client.id);
                                if ids.is_empty() {
                                    self.shard_subs.remove(channel);
                                }
                            }
                        }
                    }
                    let count = if command == "SUNSUBSCRIBE" {
                        subs.shards.len()
                    } else {
                        subs.total()
                    };
                    frames.push(subscription_frame(
                        kind,
                        Some(channel),
                        count,
                        client.version,
                    ));
                }
                if !subs.is_empty() {
                    self.client_subs.insert(client.id, subs);
                }
                Outcome::with_frames(frames)
            }
            "PUBLISH" | "SPUBLISH" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                let (kind, patterns, sharded) = if command == "PUBLISH" {
                    (b"message".to_vec(), true, false)
                } else {
                    (b"smessage".to_vec(), false, true)
                };
                let deliveries =
                    self.publish_to_subscribers(&args[1], &args[2], &kind, patterns, sharded);
                Outcome::reply(RespValue::Integer(deliveries as i64))
            }
            "PUBSUB" => {
                if args.len() < 2 {
                    return wrong_args(args);
                }
                let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
                match sub.as_str() {
                    "CHANNELS" | "SHARDCHANNELS" => {
                        if args.len() > 3 {
                            return Outcome::error("ERR syntax error");
                        }
                        let pattern = args.get(2).map(Vec::as_slice);
                        let source = if sub == "CHANNELS" {
                            &self.channel_subs
                        } else {
                            &self.shard_subs
                        };
                        let channels: Vec<RespValue> = source
                            .iter()
                            .filter(|(channel, ids)| {
                                !ids.is_empty()
                                    && pattern.is_none_or(|pattern| glob_match(pattern, channel))
                            })
                            .map(|(channel, _)| RespValue::bulk(channel.clone()))
                            .collect();
                        Outcome::reply(RespValue::Array(channels))
                    }
                    "NUMSUB" | "SHARDNUMSUB" => {
                        let source = if sub == "NUMSUB" {
                            &self.channel_subs
                        } else {
                            &self.shard_subs
                        };
                        let mut values = Vec::new();
                        for channel in &args[2..] {
                            values.push(RespValue::bulk(channel.clone()));
                            values.push(RespValue::Integer(
                                source.get(channel).map_or(0, |ids| ids.len()) as i64,
                            ));
                        }
                        Outcome::reply(RespValue::Array(values))
                    }
                    "NUMPAT" => {
                        if args.len() != 2 {
                            return Outcome::error("ERR syntax error");
                        }
                        Outcome::reply(RespValue::Integer(self.pattern_subs.len() as i64))
                    }
                    _ => {
                        Outcome::error("ERR Unknown PUBSUB subcommand or wrong number of arguments")
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn cmd_replicaof(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        if args[1].eq_ignore_ascii_case(b"no") && args[2].eq_ignore_ascii_case(b"one") {
            self.request_promotion();
            return Outcome::ok();
        }
        let Ok(port) = parse_u64(&args[2]) else {
            return Outcome::error("ERR Invalid master port");
        };
        if port == 0 || port > u64::from(u16::MAX) {
            return Outcome::error("ERR Invalid master port");
        }
        let host = String::from_utf8_lossy(&args[1]).to_string();
        self.request_replicaof(host, port as u16);
        Outcome::ok()
    }

    fn cmd_replconf(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let subcommand = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        match subcommand.as_str() {
            "GETACK" => Outcome::reply(RespValue::Array(vec![
                RespValue::bulk(b"REPLCONF".to_vec()),
                RespValue::bulk(b"ACK".to_vec()),
                RespValue::bulk(self.replication.offset.to_string().into_bytes()),
            ])),
            _ => Outcome::ok(),
        }
    }

    fn cmd_role(&self) -> Outcome {
        match self.replication.role {
            ReplicationRole::Master => {
                let replicas = self
                    .replication
                    .clients
                    .values()
                    .map(|offset| {
                        RespValue::Array(vec![
                            RespValue::bulk(b"127.0.0.1".to_vec()),
                            RespValue::Integer(0),
                            RespValue::Integer((*offset).max(0)),
                        ])
                    })
                    .collect();
                Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk(b"master".to_vec()),
                    RespValue::Integer(self.replication.offset),
                    RespValue::Array(replicas),
                ]))
            }
            ReplicationRole::Replica => {
                let state = if self.replication.link_status == "up" {
                    "connected"
                } else if self.replication.sync_in_progress {
                    "handshake"
                } else {
                    "connect"
                };
                Outcome::reply(RespValue::Array(vec![
                    RespValue::bulk(b"slave".to_vec()),
                    RespValue::bulk(self.replication.master_host.clone().into_bytes()),
                    RespValue::Integer(i64::from(self.replication.master_port)),
                    RespValue::bulk(state.as_bytes().to_vec()),
                    RespValue::Integer(self.replication.offset),
                ]))
            }
        }
    }

    fn cmd_wait(&mut self, args: &[Vec<u8>]) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let Ok(numreplicas) = parse_i64(&args[1]) else {
            return Outcome::error("ERR value is not an integer or out of range");
        };
        if parse_i64(&args[2]).is_err() {
            return Outcome::error("ERR timeout is not an integer or out of range");
        }
        let acked = self
            .replication
            .clients
            .values()
            .filter(|offset| **offset >= self.replication.offset)
            .count() as i64;
        let _ = numreplicas;
        Outcome::reply(RespValue::Integer(acked))
    }

    fn hll_sparse_max_bytes(&self) -> usize {
        self.config_values
            .get("hll-sparse-max-bytes")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(3000)
    }

    fn hll_lookup(&mut self, db_index: usize, key: &[u8]) -> Result<Option<Vec<u8>>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_string() {
                Some(bytes) => Ok(Some(bytes.clone())),
                None => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn cmd_pfadd(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let key = &args[1];
        let mut data = match self.hll_lookup(db_index, key) {
            Ok(Some(data)) => {
                if let Err(error) = hll::validate(&data) {
                    return hll_error(error);
                }
                data
            }
            Ok(None) => hll::create(),
            Err(outcome) => return outcome,
        };
        let created = !self.dbs[db_index].contains_key(key);
        let mut updated = usize::from(created);
        match hll::add_elements(&mut data, &args[2..], self.hll_sparse_max_bytes()) {
            Ok(count) => updated += count,
            Err(error) => return hll_error(error),
        }
        if updated > 0 {
            let mvcc = self.clock.next(self.now_ms());
            self.dbs[db_index].insert(key, Value::String(data), mvcc);
            return Outcome::reply(RespValue::Integer(1));
        }
        Outcome::reply(RespValue::Integer(0))
    }

    fn cmd_pfcount(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        if args.len() == 2 {
            let key = &args[1];
            let mut data = match self.hll_lookup(db_index, key) {
                Ok(Some(data)) => data,
                Ok(None) => return Outcome::reply(RespValue::Integer(0)),
                Err(outcome) => return outcome,
            };
            if let Err(error) = hll::validate(&data) {
                return hll_error(error);
            }
            if let Some(card) = hll::cache_card(&data) {
                return Outcome::reply(RespValue::Integer(card as i64));
            }
            let card = match hll::count(&data) {
                Ok(card) => card,
                Err(error) => return hll_error(error),
            };
            hll::set_cache(&mut data, card);
            let mvcc = self.clock.next(self.now_ms());
            self.dbs[db_index].insert(key, Value::String(data), mvcc);
            return Outcome::reply(RespValue::Integer(card as i64));
        }

        let mut registers = [0u8; hll::REGISTERS];
        for key in &args[1..] {
            let data = match self.hll_lookup(db_index, key) {
                Ok(Some(data)) => data,
                Ok(None) => continue,
                Err(outcome) => return outcome,
            };
            if let Err(error) = hll::validate(&data) {
                return hll_error(error);
            }
            if let Err(error) = hll::merge(&data, &mut registers) {
                return hll_error(error);
            }
        }
        Outcome::reply(RespValue::Integer(hll::count_registers(&registers) as i64))
    }

    fn cmd_pfmerge(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let mut registers = [0u8; hll::REGISTERS];
        let mut use_dense = false;
        for key in &args[1..] {
            let data = match self.hll_lookup(db_index, key) {
                Ok(Some(data)) => data,
                Ok(None) => continue,
                Err(outcome) => return outcome,
            };
            if let Err(error) = hll::validate(&data) {
                return hll_error(error);
            }
            if hll::encoding(&data) == hll::DENSE {
                use_dense = true;
            }
            if let Err(error) = hll::merge(&data, &mut registers) {
                return hll_error(error);
            }
        }

        let destination = &args[1];
        let mut data = match self.hll_lookup(db_index, destination) {
            Ok(Some(data)) => data,
            Ok(None) => hll::create(),
            Err(outcome) => return outcome,
        };
        if use_dense && let Err(error) = hll::sparse_to_dense(&mut data) {
            return hll_error(error);
        }
        let sparse_max = self.hll_sparse_max_bytes();
        for (index, value) in registers.iter().enumerate() {
            if *value == 0 {
                continue;
            }
            if let Err(error) = hll::set_register(&mut data, index, *value, sparse_max) {
                return hll_error(error);
            }
        }
        hll::invalidate_cache(&mut data);
        let mvcc = self.clock.next(self.now_ms());
        self.dbs[db_index].insert(destination, Value::String(data), mvcc);
        Outcome::ok()
    }

    fn cmd_pfdebug(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let subcommand = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        if args.len() != 3 {
            return Outcome::error(format!(
                "ERR Wrong number of arguments for the '{}' subcommand",
                String::from_utf8_lossy(&args[1])
            ));
        }
        let key = &args[2];
        let mut data = match self.hll_lookup(db_index, key) {
            Ok(Some(data)) => data,
            Ok(None) => return Outcome::error("ERR The specified key does not exist"),
            Err(outcome) => return outcome,
        };
        if let Err(error) = hll::validate(&data) {
            return hll_error(error);
        }
        match subcommand.as_str() {
            "GETREG" => {
                let registers = match hll::get_registers(&mut data) {
                    Ok(registers) => registers,
                    Err(error) => return hll_error(error),
                };
                let mvcc = self.clock.next(self.now_ms());
                self.dbs[db_index].insert(key, Value::String(data), mvcc);
                Outcome::reply(RespValue::Array(
                    registers
                        .into_iter()
                        .map(|value| RespValue::Integer(i64::from(value)))
                        .collect(),
                ))
            }
            "DECODE" => match hll::decode_sparse(&data) {
                Some(decoded) => Outcome::reply(RespValue::bulk(decoded.into_bytes())),
                None => Outcome::error("ERR HLL encoding is not sparse"),
            },
            "ENCODING" => {
                Outcome::reply(RespValue::simple(if hll::encoding(&data) == hll::DENSE {
                    "dense"
                } else {
                    "sparse"
                }))
            }
            "TODENSE" => {
                let converted = match hll::sparse_to_dense(&mut data) {
                    Ok(converted) => converted,
                    Err(error) => return hll_error(error),
                };
                if converted {
                    let mvcc = self.clock.next(self.now_ms());
                    self.dbs[db_index].insert(key, Value::String(data), mvcc);
                }
                Outcome::reply(RespValue::Integer(i64::from(converted)))
            }
            _ => Outcome::error(format!(
                "ERR Unknown PFDEBUG subcommand '{}'",
                String::from_utf8_lossy(&args[1])
            )),
        }
    }

    fn cmd_pfselftest(&mut self) -> Outcome {
        let mut dense = hll::create();
        if hll::sparse_to_dense(&mut dense).is_err() {
            return Outcome::error("ERR TESTFAILED Could not create dense HLL");
        }
        let mut sparse = hll::create();
        for value in 0..2000u32 {
            let element = value.to_le_bytes().to_vec();
            let elements = std::slice::from_ref(&element);
            if hll::add_elements(&mut dense, elements, usize::MAX).is_err()
                || hll::add_elements(&mut sparse, elements, usize::MAX).is_err()
            {
                return Outcome::error("ERR TESTFAILED HLL add failed");
            }
        }
        let dense_count = hll::count(&dense).unwrap_or(0);
        let sparse_count = hll::count(&sparse).unwrap_or(0);
        if dense_count != sparse_count {
            return Outcome::error("ERR TESTFAILED Dense and sparse representations disagree");
        }
        Outcome::ok()
    }

    fn stream_node_max_entries(&self) -> usize {
        self.config_values
            .get("stream-node-max-entries")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
            .max(1)
    }

    fn stream_ref(&mut self, db_index: usize, key: &[u8]) -> Result<Option<&Stream>, Outcome> {
        self.expire_if_needed(db_index, key);
        match self.dbs[db_index].get(key) {
            Some(entry) => match entry.value.as_stream() {
                Some(stream) => Ok(Some(stream)),
                None => Err(wrong_type()),
            },
            None => Ok(None),
        }
    }

    fn stream_for_write(
        &mut self,
        db_index: usize,
        key: &[u8],
        mvcc: u64,
    ) -> Result<Option<&mut Stream>, Outcome> {
        self.expire_if_needed(db_index, key);
        if !self.dbs[db_index].contains_key(key) {
            return Ok(None);
        }
        let Some(entry) = self.dbs[db_index].get_mut(key) else {
            return Ok(None);
        };
        entry.mvcc = mvcc;
        match Arc::make_mut(&mut entry.value) {
            Value::Stream(stream) => Ok(Some(stream)),
            _ => Err(wrong_type()),
        }
    }

    fn cmd_xadd(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 5 {
            return wrong_args(args);
        }
        let key = &args[1];
        let parsed = match self.parse_stream_trim_args(args, true) {
            Ok(parsed) => parsed,
            Err(outcome) => return outcome,
        };
        let idpos = parsed.id_position;
        let field_pos = idpos + 1;
        if args.len() < field_pos + 2 || !(args.len() - field_pos).is_multiple_of(2) {
            return Outcome::error("ERR wrong number of arguments for XADD");
        }
        if parsed.id_given && parsed.id.is_some_and(|id| id == StreamId::MIN) {
            return Outcome::error("ERR The ID specified in XADD must be greater than 0-0");
        }
        self.expire_if_needed(db_index, key);
        let exists = self.dbs[db_index].contains_key(key);
        if exists {
            match self.dbs[db_index].get(key) {
                Some(entry) if entry.value.as_stream().is_none() => return wrong_type(),
                _ => {}
            }
        }
        if !exists && parsed.nomkstream {
            let mut outcome = Outcome::reply(RespValue::null());
            outcome.aof_commands = Some(Vec::new());
            return outcome;
        }
        let now = self.now_ms();
        let node_max = self.stream_node_max_entries();
        let existing_last = self.dbs[db_index]
            .get(key)
            .and_then(|entry| entry.value.as_stream())
            .map(|stream| stream.last_id)
            .unwrap_or(StreamId::MIN);
        let id = match parsed.id {
            Some(id) if parsed.id_given => {
                if id <= existing_last {
                    return Outcome::error(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                id
            }
            _ => {
                if existing_last == StreamId::MAX {
                    return Outcome::error(
                        "ERR The stream has exhausted the last possible ID, unable to add more items",
                    );
                }
                let fresh = StreamId {
                    ms: now.max(0) as u64,
                    seq: 0,
                };
                if fresh.ms > existing_last.ms {
                    fresh
                } else {
                    match existing_last.next() {
                        Some(id) => id,
                        None => {
                            return Outcome::error(
                                "ERR The stream has exhausted the last possible ID, unable to add more items",
                            );
                        }
                    }
                }
            }
        };
        let mut fields = Vec::with_capacity((args.len() - field_pos) / 2);
        for pair in args[field_pos..].chunks(2) {
            fields.push((pair[0].clone(), pair[1].clone()));
        }
        let mvcc = self.clock.next(now);
        if !exists {
            self.dbs[db_index].insert(key, Value::Stream(Stream::default()), mvcc);
        }
        let trim_strategy = parsed.strategy.clone();
        let approx = parsed.approx;
        let limit = parsed.limit;
        let mut aof_args = args.to_vec();
        aof_args[idpos] = id.to_string().into_bytes();
        {
            let Some(stream) = (match self.stream_for_write(db_index, key, mvcc) {
                Ok(stream) => stream,
                Err(outcome) => return outcome,
            }) else {
                return wrong_type();
            };
            stream.append(id, fields, node_max);
            match &trim_strategy {
                StreamTrimStrategy::MaxLen(maxlen) => {
                    stream.trim_maxlen(*maxlen, approx, limit);
                }
                StreamTrimStrategy::MinId(minid) => {
                    stream.trim_minid(*minid, approx, limit);
                }
                StreamTrimStrategy::None => {}
            }
            if approx && !matches!(trim_strategy, StreamTrimStrategy::None) {
                // Approximate trimming must be propagated as an exact trim
                // reflecting the result computed here.
                let value_index = parsed.value_index;
                aof_args[value_index - 1] = b"=".to_vec();
                let value = match &trim_strategy {
                    StreamTrimStrategy::MaxLen(_) => stream.len().to_string(),
                    StreamTrimStrategy::MinId(minid) => {
                        stream.first_id().unwrap_or(*minid).to_string()
                    }
                    StreamTrimStrategy::None => unreachable!(),
                };
                aof_args[value_index] = value.into_bytes();
                let mut index = value_index + 1;
                while index + 1 < idpos {
                    if aof_args[index].eq_ignore_ascii_case(b"limit") {
                        aof_args.drain(index..index + 2);
                    } else {
                        index += 1;
                    }
                }
            }
        }
        let mut outcome = Outcome::reply(RespValue::bulk(id.to_string().into_bytes()));
        outcome.wake.push((db_index, key.clone()));
        outcome.aof_commands = Some(vec![aof_args]);
        outcome
    }

    fn cmd_xlen(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 2 {
            return wrong_args(args);
        }
        match self.stream_ref(db_index, &args[1]) {
            Ok(Some(stream)) => Outcome::reply(RespValue::Integer(stream.len() as i64)),
            Ok(None) => Outcome::reply(RespValue::Integer(0)),
            Err(outcome) => outcome,
        }
    }

    fn cmd_xrange(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let reverse = command == "XREVRANGE";
        let start_arg = if reverse { &args[3] } else { &args[2] };
        let end_arg = if reverse { &args[2] } else { &args[3] };
        let (mut start, start_exclusive) = match parse_stream_range(start_arg, 0) {
            Ok(value) => value,
            Err(outcome) => return outcome,
        };
        if start_exclusive {
            match start.next() {
                Some(id) => start = id,
                None => return Outcome::error("ERR invalid start ID for the interval"),
            }
        }
        let (mut end, end_exclusive) = match parse_stream_range(end_arg, u64::MAX) {
            Ok(value) => value,
            Err(outcome) => return outcome,
        };
        if end_exclusive {
            match end.previous() {
                Some(id) => end = id,
                None => return Outcome::error("ERR invalid end ID for the interval"),
            }
        }
        let mut count: Option<usize> = None;
        let mut index = 4;
        while index < args.len() {
            if index + 1 >= args.len() || !args[index].eq_ignore_ascii_case(b"count") {
                return Outcome::error("ERR syntax error");
            }
            let Ok(value) = parse_i64(&args[index + 1]) else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            count = Some(value.max(0) as usize);
            index += 2;
        }
        let stream = match self.stream_ref(db_index, &args[1]) {
            Ok(Some(stream)) => stream,
            Ok(None) => return Outcome::reply(RespValue::Array(Vec::new())),
            Err(outcome) => return outcome,
        };
        if count == Some(0) {
            return Outcome::reply(RespValue::NullArray);
        }
        let mut entries: Vec<&StreamEntry> = stream
            .iter()
            .filter(|entry| entry.id >= start && entry.id <= end)
            .collect();
        if let Some(count) = count {
            entries.truncate(count);
        }
        if reverse {
            entries.reverse();
        }
        let values = entries
            .into_iter()
            .map(stream_entry_reply)
            .collect::<Vec<_>>();
        Outcome::reply(RespValue::Array(values))
    }

    fn cmd_xdel(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let mut ids = Vec::with_capacity(args.len() - 2);
        for value in &args[2..] {
            match parse_stream_id(value, 0, true) {
                Some(id) => ids.push(id),
                None => return invalid_stream_id(),
            }
        }
        let mvcc = self.clock.next(self.now_ms());
        let stream = match self.stream_for_write(db_index, &args[1], mvcc) {
            Ok(Some(stream)) => stream,
            Ok(None) => return Outcome::reply(RespValue::Integer(0)),
            Err(outcome) => return outcome,
        };
        let mut deleted = 0i64;
        for id in ids {
            if stream.delete(id) {
                deleted += 1;
            }
        }
        Outcome::reply(RespValue::Integer(deleted))
    }

    fn cmd_xtrim(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let parsed = match self.parse_stream_trim_args(args, false) {
            Ok(parsed) => parsed,
            Err(outcome) => return outcome,
        };
        let mvcc = self.clock.next(self.now_ms());
        let stream = match self.stream_for_write(db_index, &args[1], mvcc) {
            Ok(Some(stream)) => stream,
            Ok(None) => return Outcome::reply(RespValue::Integer(0)),
            Err(outcome) => return outcome,
        };
        let deleted = match &parsed.strategy {
            StreamTrimStrategy::MaxLen(maxlen) => {
                stream.trim_maxlen(*maxlen, parsed.approx, parsed.limit)
            }
            StreamTrimStrategy::MinId(minid) => {
                stream.trim_minid(*minid, parsed.approx, parsed.limit)
            }
            StreamTrimStrategy::None => 0,
        };
        let mut outcome = Outcome::reply(RespValue::Integer(deleted as i64));
        if parsed.approx && deleted > 0 {
            let value = match &parsed.strategy {
                StreamTrimStrategy::MaxLen(_) => stream.len().to_string(),
                StreamTrimStrategy::MinId(minid) => stream.first_id().unwrap_or(*minid).to_string(),
                StreamTrimStrategy::None => unreachable!(),
            };
            let name = match &parsed.strategy {
                StreamTrimStrategy::MaxLen(_) => b"MAXLEN".to_vec(),
                StreamTrimStrategy::MinId(_) => b"MINID".to_vec(),
                StreamTrimStrategy::None => unreachable!(),
            };
            outcome.aof_commands = Some(vec![vec![
                b"XTRIM".to_vec(),
                args[1].clone(),
                name,
                b"=".to_vec(),
                value.into_bytes(),
            ]]);
        }
        outcome
    }

    fn cmd_xsetid(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() != 3 {
            return wrong_args(args);
        }
        let Some(id) = parse_stream_id(&args[2], 0, true) else {
            return invalid_stream_id();
        };
        let mvcc = self.clock.next(self.now_ms());
        let Some(stream) = (match self.stream_for_write(db_index, &args[1], mvcc) {
            Ok(stream) => stream,
            Err(outcome) => return outcome,
        }) else {
            return Outcome::error("ERR no such key");
        };
        if let Some(maxid) = stream.last_valid_id()
            && id < maxid
        {
            return Outcome::error(
                "ERR The ID specified in XSETID is smaller than the target stream top item",
            );
        }
        stream.last_id = id;
        Outcome::ok()
    }

    fn cmd_xread(
        &mut self,
        command: &str,
        args: &[Vec<u8>],
        db_index: usize,
        version: RespVersion,
    ) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let xreadgroup = command == "XREADGROUP";
        let mut timeout: Option<i64> = None;
        let mut count: usize = 0;
        let mut noack = false;
        let mut groupname: Option<Vec<u8>> = None;
        let mut consumername: Option<Vec<u8>> = None;
        let mut streams_arg: Option<usize> = None;
        let mut streams_count = 0usize;
        let mut index = 1;
        while index < args.len() {
            let remaining = args.len() - 1 - index;
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "BLOCK" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR timeout is not an integer or out of range");
                    };
                    if value < 0 {
                        return Outcome::error("ERR timeout is negative");
                    }
                    timeout = Some(value);
                    index += 2;
                }
                "COUNT" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    count = value.max(0) as usize;
                    index += 2;
                }
                "STREAMS" if remaining > 0 => {
                    let start = index + 1;
                    let total = args.len() - start;
                    if !total.is_multiple_of(2) {
                        return Outcome::error(
                            "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.",
                        );
                    }
                    streams_arg = Some(start);
                    streams_count = total / 2;
                    break;
                }
                "GROUP" if remaining >= 2 => {
                    if !xreadgroup {
                        return Outcome::error(
                            "ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.",
                        );
                    }
                    groupname = Some(args[index + 1].clone());
                    consumername = Some(args[index + 2].clone());
                    index += 3;
                }
                "NOACK" => {
                    if !xreadgroup {
                        return Outcome::error(
                            "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
                        );
                    }
                    noack = true;
                    index += 1;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let Some(streams_arg) = streams_arg else {
            return Outcome::error("ERR syntax error");
        };
        if xreadgroup && groupname.is_none() {
            return Outcome::error("ERR Missing GROUP option for XREADGROUP");
        }

        let keys: Vec<Vec<u8>> = args[streams_arg..streams_arg + streams_count].to_vec();
        let id_args = &args[streams_arg + streams_count..];
        let mut ids: Vec<Option<StreamId>> = Vec::with_capacity(streams_count);
        for (position, id_arg) in id_args.iter().enumerate() {
            let key = &keys[position];
            let stream = match self.stream_ref(db_index, key) {
                Ok(stream) => stream,
                Err(outcome) => return outcome,
            };
            if let Some(groupname) = &groupname
                && !stream.is_some_and(|stream| stream.groups.contains_key(groupname))
            {
                return Outcome::error(no_group_message(key, groupname, true));
            }
            if id_arg == b"$" {
                if xreadgroup {
                    return Outcome::error(
                        "ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.",
                    );
                }
                ids.push(Some(stream.map_or(StreamId::MIN, |stream| stream.last_id)));
            } else if id_arg == b">" {
                if !xreadgroup {
                    return Outcome::error(
                        "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
                    );
                }
                ids.push(None);
            } else {
                match parse_stream_id(id_arg, 0, true) {
                    Some(id) => ids.push(Some(id)),
                    None => return invalid_stream_id(),
                }
            }
        }

        let now = self.now_ms();
        let mut served: Vec<(Vec<u8>, Vec<RespValue>)> = Vec::new();
        let mut aof_commands: Vec<Vec<Vec<u8>>> = Vec::new();
        for position in 0..streams_count {
            let key = &keys[position];
            let stream = match self.stream_ref(db_index, key) {
                Ok(Some(stream)) => stream,
                Ok(None) => continue,
                Err(outcome) => return outcome,
            };
            let (serve, history, start) = if let Some(groupname) = &groupname {
                let Some(group) = stream.groups.get(groupname) else {
                    return Outcome::error(no_group_message(key, groupname, true));
                };
                match ids[position] {
                    Some(gt) => (true, true, gt.next().unwrap_or(StreamId::MAX)),
                    None => {
                        let serve = stream
                            .last_valid_id()
                            .is_some_and(|maxid| maxid > group.last_delivered_id);
                        (
                            serve,
                            false,
                            group.last_delivered_id.next().unwrap_or(StreamId::MAX),
                        )
                    }
                }
            } else {
                let gt = ids[position].unwrap_or(StreamId::MIN);
                let serve = stream.last_valid_id().is_some_and(|maxid| maxid > gt);
                (serve, false, gt.next().unwrap_or(StreamId::MAX))
            };
            if !serve {
                continue;
            }
            let group_info = match (&groupname, &consumername) {
                (Some(group), Some(consumer)) => {
                    Some((group.as_slice(), consumer.as_slice(), noack))
                }
                _ => None,
            };
            let (entries, commands) = match self
                .stream_serve_read(db_index, key, start, count, group_info, history, now)
            {
                Ok(result) => result,
                Err(outcome) => return outcome,
            };
            aof_commands.extend(commands);
            served.push((key.clone(), entries));
        }

        if served.is_empty() {
            let Some(timeout) = timeout else {
                let mut outcome = Outcome::reply(RespValue::NullArray);
                if xreadgroup {
                    outcome.aof_commands = Some(Vec::new());
                }
                return outcome;
            };
            return Outcome::blocked(BlockRequest {
                db: db_index,
                keys,
                kind: BlockKind::StreamRead(StreamBlock {
                    ids,
                    group: groupname,
                    consumer: consumername,
                    count: if count == 0 { 1000 } else { count },
                    noack,
                    version,
                }),
                source_side: ListSide::Left,
                destination: None,
                destination_side: ListSide::Left,
                timeout_ms: if timeout == 0 {
                    None
                } else {
                    Some(timeout as u64)
                },
            });
        }

        let reply = stream_read_reply(&served, version);
        let mut outcome = Outcome::reply(reply);
        outcome.aof_commands = Some(aof_commands);
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn stream_serve_read(
        &mut self,
        db_index: usize,
        key: &[u8],
        start: StreamId,
        count: usize,
        group_info: Option<(&[u8], &[u8], bool)>,
        history: bool,
        now: i64,
    ) -> Result<StreamReadResult, Outcome> {
        let Some(entry) = self.dbs[db_index].get_mut(key) else {
            return Ok((Vec::new(), Vec::new()));
        };
        let Some(stream) = Arc::make_mut(&mut entry.value).as_stream_mut() else {
            return Err(wrong_type());
        };
        let mut commands = Vec::new();
        let Some((group_name, consumer_name, noack)) = group_info else {
            let limit = if count == 0 { usize::MAX } else { count };
            let replies = stream
                .iter()
                .filter(|entry| entry.id >= start)
                .take(limit)
                .map(stream_entry_reply)
                .collect();
            return Ok((replies, commands));
        };
        if !stream.groups.contains_key(group_name) {
            return Err(Outcome::error(no_group_message(key, group_name, false)));
        }
        let mut created_consumer = false;
        {
            let group = stream.groups.get_mut(group_name).expect("checked group");
            match group.consumers.entry(consumer_name.to_vec()) {
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    vacant.insert(StreamConsumer {
                        name: consumer_name.to_vec(),
                        seen_time: now,
                        pel: std::collections::BTreeSet::new(),
                    });
                    created_consumer = true;
                }
                std::collections::btree_map::Entry::Occupied(mut occupied) => {
                    occupied.get_mut().seen_time = now;
                }
            }
        }
        if history {
            let ids: Vec<StreamId> = {
                let group = stream.groups.get(group_name).expect("checked group");
                group
                    .consumers
                    .get(consumer_name)
                    .map(|consumer| {
                        let limit = if count == 0 { usize::MAX } else { count };
                        consumer.pel.range(start..).take(limit).copied().collect()
                    })
                    .unwrap_or_default()
            };
            let mut replies = Vec::with_capacity(ids.len());
            for id in ids {
                let fields = stream.find(id).map(|entry| entry.fields.clone());
                match fields {
                    Some(fields) => {
                        let group = stream.groups.get_mut(group_name).expect("checked group");
                        if let Some(nack) = group.pel.get_mut(&id) {
                            nack.delivery_time = now;
                            nack.delivery_count += 1;
                        }
                        replies.push(stream_entry_reply_fields(id, &fields));
                    }
                    None => replies.push(RespValue::Array(vec![
                        RespValue::bulk(id.to_string().into_bytes()),
                        RespValue::NullArray,
                    ])),
                }
            }
            if created_consumer && noack {
                commands.push(vec![
                    b"XGROUP".to_vec(),
                    b"CREATECONSUMER".to_vec(),
                    key.to_vec(),
                    group_name.to_vec(),
                    consumer_name.to_vec(),
                ]);
            }
            return Ok((replies, commands));
        }

        let limit = if count == 0 { usize::MAX } else { count };
        let entries: Vec<(StreamId, StreamFields)> = stream
            .iter()
            .filter(|entry| entry.id >= start)
            .take(limit)
            .map(|entry| (entry.id, entry.fields.clone()))
            .collect();
        let mut replies = Vec::with_capacity(entries.len());
        if let Some(last_id) = entries.last().map(|(id, _)| *id) {
            {
                let group = stream.groups.get_mut(group_name).expect("checked group");
                group.last_delivered_id = last_id;
                if !noack {
                    for (id, _) in &entries {
                        let previous_owner = group
                            .pel
                            .get(id)
                            .map(|nack| nack.consumer.clone())
                            .filter(|owner| !owner.is_empty());
                        if let Some(previous) = previous_owner
                            && previous != *consumer_name
                            && let Some(consumer) = group.consumers.get_mut(&previous)
                        {
                            consumer.pel.remove(id);
                        }
                        match group.pel.get_mut(id) {
                            Some(nack) => {
                                nack.consumer = consumer_name.to_vec();
                                nack.delivery_time = now;
                                nack.delivery_count = 1;
                            }
                            None => {
                                group.pel.insert(
                                    *id,
                                    StreamNack {
                                        consumer: consumer_name.to_vec(),
                                        delivery_time: now,
                                        delivery_count: 1,
                                    },
                                );
                            }
                        }
                        if let Some(consumer) = group.consumers.get_mut(consumer_name) {
                            consumer.pel.insert(*id);
                        }
                    }
                }
            }
            for (id, fields) in &entries {
                replies.push(stream_entry_reply_fields(*id, fields));
            }
            let group = stream.groups.get(group_name).expect("checked group");
            if noack {
                if created_consumer {
                    commands.push(vec![
                        b"XGROUP".to_vec(),
                        b"CREATECONSUMER".to_vec(),
                        key.to_vec(),
                        group_name.to_vec(),
                        consumer_name.to_vec(),
                    ]);
                }
                commands.push(vec![
                    b"XGROUP".to_vec(),
                    b"SETID".to_vec(),
                    key.to_vec(),
                    group_name.to_vec(),
                    group.last_delivered_id.to_string().into_bytes(),
                ]);
            } else {
                let last_delivered = group.last_delivered_id;
                for (id, _) in &entries {
                    let Some(nack) = group.pel.get(id) else {
                        continue;
                    };
                    commands.push(stream_xclaim_command(
                        key,
                        group_name,
                        consumer_name,
                        *id,
                        nack.delivery_time,
                        nack.delivery_count,
                        last_delivered,
                    ));
                }
            }
        } else if created_consumer && noack {
            commands.push(vec![
                b"XGROUP".to_vec(),
                b"CREATECONSUMER".to_vec(),
                key.to_vec(),
                group_name.to_vec(),
                consumer_name.to_vec(),
            ]);
        }
        Ok((replies, commands))
    }
    fn cmd_xgroup(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let subcommand = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        let now = self.now_ms();
        let mvcc = self.clock.next(now);
        match subcommand.as_str() {
            "CREATE" => {
                if args.len() != 5 && args.len() != 6 {
                    return xgroup_syntax_error(&args[1]);
                }
                let mut mkstream = false;
                if args.len() == 6 {
                    if !args[5].eq_ignore_ascii_case(b"mkstream") {
                        return xgroup_syntax_error(&args[1]);
                    }
                    mkstream = true;
                }
                let key = &args[2];
                let group_name = &args[3];
                let existing = self.stream_ref(db_index, key);
                let mut stream_existed = true;
                match existing {
                    Ok(Some(_)) => {}
                    Ok(None) => stream_existed = false,
                    Err(outcome) => return outcome,
                }
                let last_id = if args[4] == b"$" {
                    if stream_existed {
                        match self.stream_ref(db_index, key) {
                            Ok(Some(stream)) => stream.last_id,
                            _ => StreamId::MIN,
                        }
                    } else {
                        StreamId::MIN
                    }
                } else {
                    match parse_stream_id(&args[4], 0, true) {
                        Some(id) => id,
                        None => return invalid_stream_id(),
                    }
                };
                if !stream_existed {
                    if !mkstream {
                        return Outcome::error(
                            "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                        );
                    }
                    self.dbs[db_index].insert(key, Value::Stream(Stream::default()), mvcc);
                }
                let Some(stream) = (match self.stream_for_write(db_index, key, mvcc) {
                    Ok(stream) => stream,
                    Err(outcome) => return outcome,
                }) else {
                    return Outcome::error("ERR no such key");
                };
                if stream.groups.contains_key(group_name) {
                    return Outcome::error("BUSYGROUP Consumer Group name already exists");
                }
                stream.groups.insert(
                    group_name.clone(),
                    StreamGroup {
                        last_delivered_id: last_id,
                        ..StreamGroup::default()
                    },
                );
                Outcome::ok()
            }
            "SETID" => {
                if args.len() != 5 {
                    return xgroup_syntax_error(&args[1]);
                }
                let Some(stream) = (match self.stream_for_write(db_index, &args[2], mvcc) {
                    Ok(stream) => stream,
                    Err(outcome) => return outcome,
                }) else {
                    return Outcome::error(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    );
                };
                let id = if args[4] == b"$" {
                    stream.last_id
                } else {
                    match parse_stream_id(&args[4], 0, false) {
                        Some(id) => id,
                        None => return invalid_stream_id(),
                    }
                };
                let Some(group) = stream.groups.get_mut(&args[3]) else {
                    return Outcome::error(no_group_message(&args[2], &args[3], false));
                };
                group.last_delivered_id = id;
                Outcome::ok()
            }
            "DESTROY" => {
                if args.len() != 4 {
                    return xgroup_syntax_error(&args[1]);
                }
                let Some(stream) = (match self.stream_for_write(db_index, &args[2], mvcc) {
                    Ok(stream) => stream,
                    Err(outcome) => return outcome,
                }) else {
                    return Outcome::error(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    );
                };
                let removed = stream.groups.remove(&args[3]).is_some();
                let mut outcome = Outcome::reply(RespValue::Integer(i64::from(removed)));
                outcome.wake.push((db_index, args[2].clone()));
                outcome
            }
            "CREATECONSUMER" => {
                if args.len() != 5 {
                    return xgroup_syntax_error(&args[1]);
                }
                let Some(stream) = (match self.stream_for_write(db_index, &args[2], mvcc) {
                    Ok(stream) => stream,
                    Err(outcome) => return outcome,
                }) else {
                    return Outcome::error(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    );
                };
                let key_name = args[2].clone();
                let group_name = args[3].clone();
                let Some(group) = stream.groups.get_mut(&group_name) else {
                    return Outcome::error(no_group_message(&key_name, &group_name, false));
                };
                if group.consumers.contains_key(&args[4]) {
                    Outcome::reply(RespValue::Integer(0))
                } else {
                    group.consumers.insert(
                        args[4].clone(),
                        StreamConsumer {
                            name: args[4].clone(),
                            seen_time: now,
                            pel: std::collections::BTreeSet::new(),
                        },
                    );
                    Outcome::reply(RespValue::Integer(1))
                }
            }
            "DELCONSUMER" => {
                if args.len() != 5 {
                    return xgroup_syntax_error(&args[1]);
                }
                let Some(stream) = (match self.stream_for_write(db_index, &args[2], mvcc) {
                    Ok(stream) => stream,
                    Err(outcome) => return outcome,
                }) else {
                    return Outcome::error(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    );
                };
                let key_name = args[2].clone();
                let group_name = args[3].clone();
                let Some(group) = stream.groups.get_mut(&group_name) else {
                    return Outcome::error(no_group_message(&key_name, &group_name, false));
                };
                let Some(consumer) = group.consumers.remove(&args[4]) else {
                    return Outcome::reply(RespValue::Integer(0));
                };
                let pending = consumer.pel.len() as i64;
                for id in consumer.pel {
                    group.pel.remove(&id);
                }
                Outcome::reply(RespValue::Integer(pending))
            }
            "HELP" if args.len() == 2 => Outcome::reply(RespValue::Array(vec![
                RespValue::bulk(b"CREATE <key> <groupname> <id|$> [MKSTREAM]".to_vec()),
                RespValue::bulk(b"CREATECONSUMER <key> <groupname> <consumer>".to_vec()),
                RespValue::bulk(b"DELCONSUMER <key> <groupname> <consumer>".to_vec()),
                RespValue::bulk(b"DESTROY <key> <groupname>".to_vec()),
                RespValue::bulk(b"SETID <key> <groupname> <id|$>".to_vec()),
            ])),
            _ => xgroup_syntax_error(&args[1]),
        }
    }

    fn cmd_xack(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        let mut ids = Vec::with_capacity(args.len() - 3);
        for value in &args[3..] {
            match parse_stream_id(value, 0, true) {
                Some(id) => ids.push(id),
                None => return invalid_stream_id(),
            }
        }
        let mvcc = self.clock.next(self.now_ms());
        let Some(stream) = (match self.stream_for_write(db_index, &args[1], mvcc) {
            Ok(stream) => stream,
            Err(outcome) => return outcome,
        }) else {
            return Outcome::reply(RespValue::Integer(0));
        };
        let Some(group) = stream.groups.get_mut(&args[2]) else {
            return Outcome::reply(RespValue::Integer(0));
        };
        let mut acknowledged = 0i64;
        for id in ids {
            let Some(nack) = group.pel.remove(&id) else {
                continue;
            };
            if let Some(consumer) = group.consumers.get_mut(&nack.consumer) {
                consumer.pel.remove(&id);
            }
            acknowledged += 1;
        }
        Outcome::reply(RespValue::Integer(acknowledged))
    }

    fn cmd_xpending(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        let just_info = args.len() == 3;
        if !just_info && !(6..=9).contains(&args.len()) {
            return Outcome::error("ERR syntax error");
        }
        let mut min_idle = 0i64;
        let mut start = StreamId::MIN;
        let mut end = StreamId::MAX;
        let mut count = 0usize;
        let mut consumer_filter: Option<&Vec<u8>> = None;
        if !just_info {
            let mut start_index = 3;
            if args[3].eq_ignore_ascii_case(b"idle") {
                if args.len() < 8 {
                    return Outcome::error("ERR syntax error");
                }
                let Ok(value) = parse_i64(&args[4]) else {
                    return Outcome::error("ERR value is not an integer or out of range");
                };
                min_idle = value.max(0);
                start_index += 2;
            }
            let Ok(value) = parse_i64(&args[start_index + 2]) else {
                return Outcome::error("ERR value is not an integer or out of range");
            };
            count = value.max(0) as usize;
            let (mut range_start, start_exclusive) = match parse_stream_range(&args[start_index], 0)
            {
                Ok(value) => value,
                Err(outcome) => return outcome,
            };
            if start_exclusive {
                match range_start.next() {
                    Some(id) => range_start = id,
                    None => return Outcome::error("ERR invalid start ID for the interval"),
                }
            }
            let (mut range_end, end_exclusive) =
                match parse_stream_range(&args[start_index + 1], u64::MAX) {
                    Ok(value) => value,
                    Err(outcome) => return outcome,
                };
            if end_exclusive {
                match range_end.previous() {
                    Some(id) => range_end = id,
                    None => return Outcome::error("ERR invalid end ID for the interval"),
                }
            }
            start = range_start;
            end = range_end;
            if start_index + 3 < args.len() {
                consumer_filter = Some(&args[start_index + 3]);
            }
        }
        let now = self.now_ms();
        let stream = match self.stream_ref(db_index, &args[1]) {
            Ok(Some(stream)) => stream,
            Ok(None) => return Outcome::error(no_group_message(&args[1], &args[2], false)),
            Err(outcome) => return outcome,
        };
        let Some(group) = stream.groups.get(&args[2]) else {
            return Outcome::error(no_group_message(&args[1], &args[2], false));
        };
        if just_info {
            if group.pel.is_empty() {
                return Outcome::reply(RespValue::Array(vec![
                    RespValue::Integer(0),
                    RespValue::null(),
                    RespValue::null(),
                    RespValue::NullArray,
                ]));
            }
            let first = *group.pel.keys().next().expect("non-empty");
            let last = *group.pel.keys().next_back().expect("non-empty");
            let consumers = group
                .consumers
                .iter()
                .filter(|(_, consumer)| !consumer.pel.is_empty())
                .map(|(name, consumer)| {
                    RespValue::Array(vec![
                        RespValue::bulk(name.clone()),
                        RespValue::bulk(consumer.pel.len().to_string().into_bytes()),
                    ])
                })
                .collect();
            return Outcome::reply(RespValue::Array(vec![
                RespValue::Integer(group.pel.len() as i64),
                RespValue::bulk(first.to_string().into_bytes()),
                RespValue::bulk(last.to_string().into_bytes()),
                RespValue::Array(consumers),
            ]));
        }
        let ids: Vec<StreamId> = match consumer_filter {
            Some(name) => match group.consumers.get(name) {
                Some(consumer) => consumer.pel.range(start..=end).copied().collect(),
                None => return Outcome::reply(RespValue::Array(Vec::new())),
            },
            None => group.pel.range(start..=end).map(|(id, _)| *id).collect(),
        };
        let mut values = Vec::new();
        for id in ids {
            if values.len() >= count {
                break;
            }
            let Some(nack) = group.pel.get(&id) else {
                continue;
            };
            let elapsed = (now - nack.delivery_time).max(0);
            if elapsed < min_idle {
                continue;
            }
            values.push(RespValue::Array(vec![
                RespValue::bulk(id.to_string().into_bytes()),
                RespValue::bulk(nack.consumer.clone()),
                RespValue::Integer(elapsed),
                RespValue::Integer(nack.delivery_count as i64),
            ]));
        }
        Outcome::reply(RespValue::Array(values))
    }

    fn cmd_xclaim(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 6 {
            return wrong_args(args);
        }
        let now = self.now_ms();
        let mvcc = self.clock.next(now);
        let mut justid = false;
        let mut force = false;
        let mut retrycount: Option<u64> = None;
        let mut deliverytime: Option<i64> = None;
        let mut last_id = StreamId::MIN;
        let mut ids = Vec::new();
        let mut index = 5;
        while index < args.len() {
            let Some(id) = parse_stream_id(&args[index], 0, true) else {
                break;
            };
            ids.push(id);
            index += 1;
        }
        while index < args.len() {
            let remaining = args.len() - 1 - index;
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "FORCE" => {
                    force = true;
                    index += 1;
                }
                "JUSTID" => {
                    justid = true;
                    index += 1;
                }
                "IDLE" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR Invalid IDLE option argument for XCLAIM");
                    };
                    deliverytime = Some(now - value);
                    index += 2;
                }
                "TIME" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR Invalid TIME option argument for XCLAIM");
                    };
                    deliverytime = Some(value);
                    index += 2;
                }
                "RETRYCOUNT" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR Invalid RETRYCOUNT option argument for XCLAIM");
                    };
                    retrycount = Some(value.max(0) as u64);
                    index += 2;
                }
                "LASTID" if remaining > 0 => {
                    match parse_stream_id(&args[index + 1], 0, true) {
                        Some(id) => last_id = id,
                        None => return invalid_stream_id(),
                    }
                    index += 2;
                }
                _ => {
                    return Outcome::error(format!(
                        "ERR Unrecognized XCLAIM option '{}'",
                        String::from_utf8_lossy(&args[index])
                    ));
                }
            }
        }
        let min_idle = match parse_i64(&args[4]) {
            Ok(value) => value.max(0),
            Err(_) => {
                return Outcome::error("ERR Invalid min-idle-time argument for XCLAIM");
            }
        };
        let deliverytime = deliverytime
            .map(|value| value.clamp(0, now.max(0)))
            .unwrap_or(now);
        let Some(stream) = (match self.stream_for_write(db_index, &args[1], mvcc) {
            Ok(stream) => stream,
            Err(outcome) => return outcome,
        }) else {
            return Outcome::error(no_group_message(&args[1], &args[2], false));
        };
        if !stream.groups.contains_key(&args[2]) {
            return Outcome::error(no_group_message(&args[1], &args[2], false));
        }
        let key = args[1].clone();
        let group_name = args[2].clone();
        let consumer_name = args[3].clone();
        let mut propagate_last_id = false;
        {
            let group = stream.groups.get_mut(&group_name).expect("checked group");
            if last_id > group.last_delivered_id {
                group.last_delivered_id = last_id;
                propagate_last_id = true;
            }
        }
        let mut replies = Vec::new();
        let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();
        for id in ids {
            let entry_fields = stream.find(id).map(|entry| entry.fields.clone());
            let nack_state: Option<(i64, u64)>;
            {
                let group = stream.groups.get_mut(&group_name).expect("checked group");
                let mut nack = group.pel.get(&id).cloned();
                if nack.is_none() && force {
                    if entry_fields.is_none() {
                        continue;
                    }
                    nack = Some(StreamNack {
                        consumer: Vec::new(),
                        delivery_time: now,
                        delivery_count: 1,
                    });
                }
                let Some(mut nack) = nack else {
                    continue;
                };
                if !nack.consumer.is_empty() && min_idle > 0 && now - nack.delivery_time < min_idle
                {
                    continue;
                }
                if !group.consumers.contains_key(&consumer_name) {
                    group.consumers.insert(
                        consumer_name.clone(),
                        StreamConsumer {
                            name: consumer_name.clone(),
                            seen_time: now,
                            pel: std::collections::BTreeSet::new(),
                        },
                    );
                }
                if !nack.consumer.is_empty()
                    && nack.consumer != consumer_name
                    && let Some(old) = group.consumers.get_mut(&nack.consumer)
                {
                    old.pel.remove(&id);
                }
                nack.delivery_time = deliverytime;
                if let Some(retrycount) = retrycount {
                    nack.delivery_count = retrycount;
                } else if !justid {
                    nack.delivery_count += 1;
                }
                nack.consumer = consumer_name.clone();
                if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                    consumer.pel.insert(id);
                }
                group.pel.insert(id, nack.clone());
                nack_state = Some((nack.delivery_time, nack.delivery_count));
                propagate_last_id = false;
            }
            if let Some((time, count)) = nack_state {
                if justid {
                    replies.push(RespValue::bulk(id.to_string().into_bytes()));
                } else {
                    match &entry_fields {
                        Some(fields) => replies.push(stream_entry_reply_fields(id, fields)),
                        None => replies.push(RespValue::null()),
                    }
                }
                let last = stream
                    .groups
                    .get(&group_name)
                    .map(|group| group.last_delivered_id)
                    .unwrap_or(StreamId::MIN);
                commands.push(stream_xclaim_command(
                    &key,
                    &group_name,
                    &consumer_name,
                    id,
                    time,
                    count,
                    last,
                ));
            }
        }
        if propagate_last_id {
            let last = stream
                .groups
                .get(&group_name)
                .map(|group| group.last_delivered_id)
                .unwrap_or(StreamId::MIN);
            commands.push(vec![
                b"XGROUP".to_vec(),
                b"SETID".to_vec(),
                key,
                group_name,
                last.to_string().into_bytes(),
            ]);
        }
        let mut outcome = Outcome::reply(RespValue::Array(replies));
        outcome.aof_commands = Some(commands);
        outcome
    }

    fn cmd_xautoclaim(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 6 {
            return wrong_args(args);
        }
        let now = self.now_ms();
        let mvcc = self.clock.next(now);
        let min_idle = match parse_i64(&args[4]) {
            Ok(value) => value.max(0),
            Err(_) => {
                return Outcome::error("ERR Invalid min-idle-time argument for XAUTOCLAIM");
            }
        };
        let (mut start, start_exclusive) = match parse_stream_range(&args[5], 0) {
            Ok(value) => value,
            Err(outcome) => return outcome,
        };
        if start_exclusive {
            match start.next() {
                Some(id) => start = id,
                None => return Outcome::error("ERR invalid start ID for the interval"),
            }
        }
        let mut count = 100usize;
        let mut justid = false;
        let mut index = 6;
        while index < args.len() {
            let remaining = args.len() - 1 - index;
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "COUNT" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR COUNT must be > 0");
                    };
                    if value <= 0 {
                        return Outcome::error("ERR COUNT must be > 0");
                    }
                    count = value as usize;
                    index += 2;
                }
                "JUSTID" => {
                    justid = true;
                    index += 1;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }
        let Some(stream) = (match self.stream_for_write(db_index, &args[1], mvcc) {
            Ok(stream) => stream,
            Err(outcome) => return outcome,
        }) else {
            return Outcome::error(no_group_message(&args[1], &args[2], false));
        };
        if !stream.groups.contains_key(&args[2]) {
            return Outcome::error(no_group_message(&args[1], &args[2], false));
        }
        let key = args[1].clone();
        let group_name = args[2].clone();
        let consumer_name = args[3].clone();
        let ids: Vec<StreamId> = {
            let group = stream.groups.get(&group_name).expect("checked group");
            group.pel.range(start..).map(|(id, _)| *id).collect()
        };
        let mut replies = Vec::new();
        let mut commands: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut attempts = count.saturating_mul(10);
        let mut remaining = count;
        let mut processed = 0usize;
        while processed < ids.len() && attempts > 0 && remaining > 0 {
            let id = ids[processed];
            processed += 1;
            attempts -= 1;
            let entry_fields = stream.find(id).map(|entry| entry.fields.clone());
            let nack_state: Option<(i64, u64)>;
            {
                let group = stream.groups.get_mut(&group_name).expect("checked group");
                let Some(mut nack) = group.pel.get(&id).cloned() else {
                    continue;
                };
                if min_idle > 0 && now - nack.delivery_time < min_idle {
                    continue;
                }
                if !group.consumers.contains_key(&consumer_name) {
                    group.consumers.insert(
                        consumer_name.clone(),
                        StreamConsumer {
                            name: consumer_name.clone(),
                            seen_time: now,
                            pel: std::collections::BTreeSet::new(),
                        },
                    );
                }
                if nack.consumer != consumer_name
                    && let Some(old) = group.consumers.get_mut(&nack.consumer)
                {
                    old.pel.remove(&id);
                }
                nack.delivery_time = now;
                if !justid {
                    nack.delivery_count += 1;
                }
                nack.consumer = consumer_name.clone();
                if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                    consumer.pel.insert(id);
                }
                group.pel.insert(id, nack.clone());
                nack_state = Some((nack.delivery_time, nack.delivery_count));
            }
            if let Some((time, delivery_count)) = nack_state {
                if justid {
                    replies.push(RespValue::bulk(id.to_string().into_bytes()));
                } else {
                    match &entry_fields {
                        Some(fields) => replies.push(stream_entry_reply_fields(id, fields)),
                        None => replies.push(RespValue::null()),
                    }
                }
                let last = stream
                    .groups
                    .get(&group_name)
                    .map(|group| group.last_delivered_id)
                    .unwrap_or(StreamId::MIN);
                commands.push(stream_xclaim_command(
                    &key,
                    &group_name,
                    &consumer_name,
                    id,
                    time,
                    delivery_count,
                    last,
                ));
                remaining -= 1;
            }
        }
        let cursor = ids.get(processed).copied().unwrap_or(StreamId::MIN);
        let mut outcome = Outcome::reply(RespValue::Array(vec![
            RespValue::bulk(cursor.to_string().into_bytes()),
            RespValue::Array(replies),
        ]));
        outcome.aof_commands = Some(commands);
        outcome
    }

    fn cmd_xinfo(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 2 {
            return wrong_args(args);
        }
        let subcommand = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
        if subcommand == "HELP" {
            return Outcome::reply(RespValue::Array(vec![
                RespValue::bulk(b"CONSUMERS <key> <groupname>".to_vec()),
                RespValue::bulk(b"GROUPS <key>".to_vec()),
                RespValue::bulk(b"STREAM <key> [FULL [COUNT <count>]]".to_vec()),
            ]));
        }
        if args.len() < 3 {
            return xinfo_syntax_error();
        }
        let key = &args[2];
        match subcommand.as_str() {
            "CONSUMERS" => {
                if args.len() != 4 {
                    return xinfo_syntax_error();
                }
                let now = self.now_ms();
                let stream = match self.stream_ref(db_index, key) {
                    Ok(Some(stream)) => stream,
                    Ok(None) => return Outcome::error("ERR no such key"),
                    Err(outcome) => return outcome,
                };
                let Some(group) = stream.groups.get(&args[3]) else {
                    return Outcome::error(no_group_message(key, &args[3], false));
                };
                let values = group
                    .consumers
                    .iter()
                    .map(|(name, consumer)| {
                        RespValue::Map(vec![
                            (
                                RespValue::bulk(b"name".to_vec()),
                                RespValue::bulk(name.clone()),
                            ),
                            (
                                RespValue::bulk(b"pending".to_vec()),
                                RespValue::Integer(consumer.pel.len() as i64),
                            ),
                            (
                                RespValue::bulk(b"idle".to_vec()),
                                RespValue::Integer((now - consumer.seen_time).max(0)),
                            ),
                        ])
                    })
                    .collect();
                Outcome::reply(RespValue::Array(values))
            }
            "GROUPS" => {
                if args.len() != 3 {
                    return xinfo_syntax_error();
                }
                let stream = match self.stream_ref(db_index, key) {
                    Ok(Some(stream)) => stream,
                    Ok(None) => return Outcome::error("ERR no such key"),
                    Err(outcome) => return outcome,
                };
                let values = stream
                    .groups
                    .iter()
                    .map(|(name, group)| {
                        RespValue::Map(vec![
                            (
                                RespValue::bulk(b"name".to_vec()),
                                RespValue::bulk(name.clone()),
                            ),
                            (
                                RespValue::bulk(b"consumers".to_vec()),
                                RespValue::Integer(group.consumers.len() as i64),
                            ),
                            (
                                RespValue::bulk(b"pending".to_vec()),
                                RespValue::Integer(group.pel.len() as i64),
                            ),
                            (
                                RespValue::bulk(b"last-delivered-id".to_vec()),
                                RespValue::bulk(group.last_delivered_id.to_string().into_bytes()),
                            ),
                        ])
                    })
                    .collect();
                Outcome::reply(RespValue::Array(values))
            }
            "STREAM" => {
                let mut full = false;
                let mut count = 10usize;
                if args.len() > 3 {
                    if !args[3].eq_ignore_ascii_case(b"full") {
                        return xinfo_syntax_error();
                    }
                    match args.len() {
                        4 => full = true,
                        6 if args[4].eq_ignore_ascii_case(b"count") => {
                            let Ok(value) = parse_i64(&args[5]) else {
                                return Outcome::error(
                                    "ERR value is not an integer or out of range",
                                );
                            };
                            count = if value < 0 { 10 } else { value as usize };
                            full = true;
                        }
                        _ => return xinfo_syntax_error(),
                    }
                }
                let stream = match self.stream_ref(db_index, key) {
                    Ok(Some(stream)) => stream,
                    Ok(None) => return Outcome::error("ERR no such key"),
                    Err(outcome) => return outcome,
                };
                let limit = if count == 0 { usize::MAX } else { count };
                if !full {
                    let first = stream.iter().next().map(stream_entry_reply);
                    let last = stream.iter().last().map(stream_entry_reply);
                    return Outcome::reply(RespValue::Map(vec![
                        (
                            RespValue::bulk(b"length".to_vec()),
                            RespValue::Integer(stream.len() as i64),
                        ),
                        (
                            RespValue::bulk(b"radix-tree-keys".to_vec()),
                            RespValue::Integer(stream.nodes.len() as i64),
                        ),
                        (
                            RespValue::bulk(b"radix-tree-nodes".to_vec()),
                            RespValue::Integer(stream.nodes.len().max(1) as i64 + 1),
                        ),
                        (
                            RespValue::bulk(b"last-generated-id".to_vec()),
                            RespValue::bulk(stream.last_id.to_string().into_bytes()),
                        ),
                        (
                            RespValue::bulk(b"groups".to_vec()),
                            RespValue::Integer(stream.groups.len() as i64),
                        ),
                        (
                            RespValue::bulk(b"first-entry".to_vec()),
                            first.unwrap_or(RespValue::null()),
                        ),
                        (
                            RespValue::bulk(b"last-entry".to_vec()),
                            last.unwrap_or(RespValue::null()),
                        ),
                    ]));
                }
                let entries = stream.iter().take(limit).map(stream_entry_reply).collect();
                let groups = stream
                    .groups
                    .iter()
                    .map(|(name, group)| {
                        let pending = group
                            .pel
                            .iter()
                            .take(limit)
                            .map(|(id, nack)| {
                                RespValue::Array(vec![
                                    RespValue::bulk(id.to_string().into_bytes()),
                                    RespValue::bulk(nack.consumer.clone()),
                                    RespValue::Integer(nack.delivery_time),
                                    RespValue::Integer(nack.delivery_count as i64),
                                ])
                            })
                            .collect();
                        let consumers = group
                            .consumers
                            .iter()
                            .map(|(consumer_name, consumer)| {
                                let pending = consumer
                                    .pel
                                    .iter()
                                    .take(limit)
                                    .map(|id| {
                                        let (time, count) = group
                                            .pel
                                            .get(id)
                                            .map(|nack| (nack.delivery_time, nack.delivery_count))
                                            .unwrap_or((0, 0));
                                        RespValue::Array(vec![
                                            RespValue::bulk(id.to_string().into_bytes()),
                                            RespValue::Integer(time),
                                            RespValue::Integer(count as i64),
                                        ])
                                    })
                                    .collect();
                                RespValue::Map(vec![
                                    (
                                        RespValue::bulk(b"name".to_vec()),
                                        RespValue::bulk(consumer_name.clone()),
                                    ),
                                    (
                                        RespValue::bulk(b"seen-time".to_vec()),
                                        RespValue::Integer(consumer.seen_time),
                                    ),
                                    (
                                        RespValue::bulk(b"pel-count".to_vec()),
                                        RespValue::Integer(consumer.pel.len() as i64),
                                    ),
                                    (
                                        RespValue::bulk(b"pending".to_vec()),
                                        RespValue::Array(pending),
                                    ),
                                ])
                            })
                            .collect();
                        RespValue::Map(vec![
                            (
                                RespValue::bulk(b"name".to_vec()),
                                RespValue::bulk(name.clone()),
                            ),
                            (
                                RespValue::bulk(b"last-delivered-id".to_vec()),
                                RespValue::bulk(group.last_delivered_id.to_string().into_bytes()),
                            ),
                            (
                                RespValue::bulk(b"pel-count".to_vec()),
                                RespValue::Integer(group.pel.len() as i64),
                            ),
                            (
                                RespValue::bulk(b"pending".to_vec()),
                                RespValue::Array(pending),
                            ),
                            (
                                RespValue::bulk(b"consumers".to_vec()),
                                RespValue::Array(consumers),
                            ),
                        ])
                    })
                    .collect();
                Outcome::reply(RespValue::Map(vec![
                    (
                        RespValue::bulk(b"length".to_vec()),
                        RespValue::Integer(stream.len() as i64),
                    ),
                    (
                        RespValue::bulk(b"radix-tree-keys".to_vec()),
                        RespValue::Integer(stream.nodes.len() as i64),
                    ),
                    (
                        RespValue::bulk(b"radix-tree-nodes".to_vec()),
                        RespValue::Integer(stream.nodes.len().max(1) as i64 + 1),
                    ),
                    (
                        RespValue::bulk(b"last-generated-id".to_vec()),
                        RespValue::bulk(stream.last_id.to_string().into_bytes()),
                    ),
                    (
                        RespValue::bulk(b"entries".to_vec()),
                        RespValue::Array(entries),
                    ),
                    (
                        RespValue::bulk(b"groups".to_vec()),
                        RespValue::Array(groups),
                    ),
                ]))
            }
            _ => xinfo_syntax_error(),
        }
    }
    fn parse_stream_trim_args(
        &self,
        args: &[Vec<u8>],
        xadd: bool,
    ) -> Result<ParsedStreamTrim, Outcome> {
        let mut parsed = ParsedStreamTrim {
            strategy: StreamTrimStrategy::None,
            approx: false,
            limit: 0,
            limit_given: false,
            value_index: 0,
            id: None,
            id_given: false,
            nomkstream: false,
            id_position: 2,
        };
        let mut index = 2usize;
        while index < args.len() {
            let remaining = args.len() - 1 - index;
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            if xadd && args[index] == b"*" {
                parsed.id_position = index;
                break;
            }
            match option.as_str() {
                "MAXLEN" | "MINID" if remaining > 0 => {
                    if !matches!(parsed.strategy, StreamTrimStrategy::None) {
                        return Err(Outcome::error(
                            "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                        ));
                    }
                    parsed.approx = false;
                    if remaining >= 2 && args[index + 1] == b"~" {
                        parsed.approx = true;
                        index += 1;
                    } else if remaining >= 2 && args[index + 1] == b"=" {
                        index += 1;
                    }
                    if option == "MAXLEN" {
                        let Ok(value) = parse_i64(&args[index + 1]) else {
                            return Err(Outcome::error(
                                "ERR value is not an integer or out of range",
                            ));
                        };
                        if value < 0 {
                            return Err(Outcome::error("ERR The MAXLEN argument must be >= 0."));
                        }
                        parsed.strategy = StreamTrimStrategy::MaxLen(value as usize);
                    } else {
                        let Some(value) = parse_stream_id(&args[index + 1], 0, true) else {
                            return Err(invalid_stream_id());
                        };
                        parsed.strategy = StreamTrimStrategy::MinId(value);
                    }
                    parsed.value_index = index + 1;
                    index += 2;
                }
                "LIMIT" if remaining > 0 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Err(Outcome::error(
                            "ERR value is not an integer or out of range",
                        ));
                    };
                    if value < 0 {
                        return Err(Outcome::error("ERR The LIMIT argument must be >= 0."));
                    }
                    parsed.limit = value as usize;
                    parsed.limit_given = true;
                    index += 2;
                }
                "NOMKSTREAM" if xadd => {
                    parsed.nomkstream = true;
                    index += 1;
                }
                _ if xadd => {
                    let Some(id) = parse_stream_id(&args[index], 0, true) else {
                        return Err(invalid_stream_id());
                    };
                    parsed.id = Some(id);
                    parsed.id_given = true;
                    parsed.id_position = index;
                    break;
                }
                _ => return Err(Outcome::error("ERR syntax error")),
            }
        }
        if parsed.limit != 0 && matches!(parsed.strategy, StreamTrimStrategy::None) {
            return Err(Outcome::error(
                "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
            ));
        }
        if !xadd && matches!(parsed.strategy, StreamTrimStrategy::None) {
            return Err(Outcome::error(
                "ERR syntax error, XTRIM must be called with a trimming strategy",
            ));
        }
        if parsed.limit_given && !parsed.approx {
            return Err(Outcome::error(
                "ERR syntax error, LIMIT cannot be used without the special ~ option",
            ));
        }
        if parsed.approx && !parsed.limit_given {
            parsed.limit = 100 * self.stream_node_max_entries();
        }
        if self.loading {
            parsed.limit = 0;
        }
        Ok(parsed)
    }

    fn cmd_geoadd(&mut self, args: &[Vec<u8>], db_index: usize, version: RespVersion) -> Outcome {
        if args.len() < 5 {
            return wrong_args(args);
        }
        let mut nx = false;
        let mut xx = false;
        let mut longidx = 2;
        while longidx < args.len() {
            let option = String::from_utf8_lossy(&args[longidx]).to_ascii_uppercase();
            match option.as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "CH" => {}
                _ => break,
            }
            longidx += 1;
        }
        if !(args.len() - longidx).is_multiple_of(3) || (xx && nx) {
            return Outcome::error("ERR syntax error");
        }
        if longidx == args.len() {
            return wrong_args(args);
        }

        // GEOADD is a thin wrapper over ZADD: coordinates are encoded as
        // 52-bit geohash scores and handed to the regular sorted-set path.
        let mut zadd_args = vec![b"ZADD".to_vec(), args[1].clone()];
        zadd_args.extend_from_slice(&args[2..longidx]);
        for triple in args[longidx..].chunks(3) {
            let (longitude, latitude) = match parse_geo_coordinates(&triple[..2]) {
                Ok(pair) => pair,
                Err(outcome) => return outcome,
            };
            let Some(hash) = geo::encode(longitude, latitude, geo::GEO_STEP_MAX) else {
                return invalid_geo_pair(longitude, latitude);
            };
            let bits = geo::align52_bits(hash);
            zadd_args.push(bits.to_string().into_bytes());
            zadd_args.push(triple[2].clone());
        }
        self.cmd_zadd(&zadd_args, db_index, version)
    }

    fn cmd_geopos(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let zset = match self.get_zset(db_index, &args[1]) {
            Ok(zset) => zset,
            Err(outcome) => return outcome,
        };
        let mut values = Vec::with_capacity(args.len() - 2);
        for member in &args[2..] {
            let point = zset
                .as_ref()
                .and_then(|zset| zset.scores.get(member))
                .and_then(|score| geo::decode_score(*score));
            match point {
                Some((longitude, latitude)) => values.push(RespValue::Array(vec![
                    RespValue::bulk(format_geo_coordinate(longitude).into_bytes()),
                    RespValue::bulk(format_geo_coordinate(latitude).into_bytes()),
                ])),
                None => values.push(RespValue::NullArray),
            }
        }
        Outcome::reply(RespValue::Array(values))
    }

    fn cmd_geohash(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 3 {
            return wrong_args(args);
        }
        let zset = match self.get_zset(db_index, &args[1]) {
            Ok(zset) => zset,
            Err(outcome) => return outcome,
        };
        const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
        let mut values = Vec::with_capacity(args.len() - 2);
        for member in &args[2..] {
            let point = zset
                .as_ref()
                .and_then(|zset| zset.scores.get(member))
                .and_then(|score| geo::decode_score(*score));
            match point {
                Some((longitude, latitude)) => {
                    let hash = geo::encode_standard(longitude, latitude, geo::GEO_STEP_MAX)
                        .unwrap_or_default();
                    let mut text = Vec::with_capacity(11);
                    for index in 0..11 {
                        let character = if index == 10 {
                            0
                        } else {
                            ((hash.bits >> (52 - ((index + 1) * 5))) & 0x1f) as usize
                        };
                        text.push(ALPHABET[character]);
                    }
                    values.push(RespValue::bulk(text));
                }
                None => values.push(RespValue::null()),
            }
        }
        Outcome::reply(RespValue::Array(values))
    }

    fn cmd_geodist(&mut self, args: &[Vec<u8>], db_index: usize) -> Outcome {
        if args.len() < 4 {
            return wrong_args(args);
        }
        if args.len() > 5 {
            return Outcome::error("ERR syntax error");
        }
        let conversion = if args.len() == 5 {
            match parse_geo_unit(&args[4]) {
                Ok(conversion) => conversion,
                Err(outcome) => return outcome,
            }
        } else {
            1.0
        };
        let zset = match self.get_zset(db_index, &args[1]) {
            Ok(zset) => zset,
            Err(outcome) => return outcome,
        };
        let Some(zset) = zset else {
            return Outcome::reply(RespValue::null());
        };
        let Some(first) = zset.scores.get(&args[2]).copied() else {
            return Outcome::reply(RespValue::null());
        };
        let Some(second) = zset.scores.get(&args[3]).copied() else {
            return Outcome::reply(RespValue::null());
        };
        let (Some((lon1, lat1)), Some((lon2, lat2))) =
            (geo::decode_score(first), geo::decode_score(second))
        else {
            return Outcome::reply(RespValue::null());
        };
        let distance = geo::distance(lon1, lat1, lon2, lat2) / conversion;
        Outcome::reply(RespValue::bulk(format!("{distance:.4}").into_bytes()))
    }

    fn cmd_geo_search(&mut self, command: &str, args: &[Vec<u8>], db_index: usize) -> Outcome {
        let search = matches!(command, "GEOSEARCH" | "GEOSEARCHSTORE");
        let search_store = command == "GEOSEARCHSTORE";
        let nostore = matches!(command, "GEORADIUS_RO" | "GEORADIUSBYMEMBER_RO");
        let by_member = matches!(command, "GEORADIUSBYMEMBER" | "GEORADIUSBYMEMBER_RO");
        let by_coords = matches!(command, "GEORADIUS" | "GEORADIUS_RO");
        let source_index = if search_store { 2 } else { 1 };
        if args.len() <= source_index {
            return wrong_args(args);
        }

        let mut source = match self.get_zset(db_index, &args[source_index]) {
            Ok(zset) => zset,
            Err(outcome) => return outcome,
        };

        let mut shape = geo::GeoShape::default();
        let mut storekey: Option<Vec<u8>> = None;
        let mut storedist = false;
        let base_args;

        if by_coords {
            if args.len() < 6 {
                return wrong_args(args);
            }
            base_args = 6;
            shape.circular = true;
            let (longitude, latitude) = match parse_geo_coordinates(&args[2..4]) {
                Ok(pair) => pair,
                Err(outcome) => return outcome,
            };
            shape.xy = (longitude, latitude);
            shape.radius = match parse_geo_radius(&args[4]) {
                Ok(radius) => radius,
                Err(outcome) => return outcome,
            };
            shape.conversion = match parse_geo_unit(&args[5]) {
                Ok(conversion) => conversion,
                Err(outcome) => return outcome,
            };
        } else if by_member {
            if args.len() < 5 {
                return wrong_args(args);
            }
            base_args = 5;
            shape.circular = true;
            if let Some(zset) = &source {
                let Some(score) = zset.scores.get(&args[2]).copied() else {
                    return Outcome::error("ERR could not decode requested zset member");
                };
                let Some(point) = geo::decode_score(score) else {
                    return Outcome::error("ERR could not decode requested zset member");
                };
                shape.xy = point;
            }
            shape.radius = match parse_geo_radius(&args[3]) {
                Ok(radius) => radius,
                Err(outcome) => return outcome,
            };
            shape.conversion = match parse_geo_unit(&args[4]) {
                Ok(conversion) => conversion,
                Err(outcome) => return outcome,
            };
        } else {
            base_args = if search_store { 3 } else { 2 };
            if args.len() < base_args {
                return wrong_args(args);
            }
            if search_store {
                storekey = Some(args[1].clone());
            }
        }

        let mut withdist = false;
        let mut withhash = false;
        let mut withcoords = false;
        let mut frommember = false;
        let mut fromloc = false;
        let mut byradius = false;
        let mut bybox = false;
        let mut any = false;
        let mut sort: Option<bool> = None; // Some(true) = ascending.
        let mut count: i64 = 0;
        let mut index = base_args;
        while index < args.len() {
            let remaining = args.len() - index;
            let option = String::from_utf8_lossy(&args[index]).to_ascii_uppercase();
            match option.as_str() {
                "WITHDIST" => {
                    withdist = true;
                    index += 1;
                }
                "WITHHASH" => {
                    withhash = true;
                    index += 1;
                }
                "WITHCOORD" => {
                    withcoords = true;
                    index += 1;
                }
                "ANY" => {
                    any = true;
                    index += 1;
                }
                "ASC" => {
                    sort = Some(true);
                    index += 1;
                }
                "DESC" => {
                    sort = Some(false);
                    index += 1;
                }
                "COUNT" if remaining > 1 => {
                    let Ok(value) = parse_i64(&args[index + 1]) else {
                        return Outcome::error("ERR value is not an integer or out of range");
                    };
                    if value <= 0 {
                        return Outcome::error("ERR COUNT must be > 0");
                    }
                    count = value;
                    index += 2;
                }
                "STORE" if remaining > 1 && !nostore && !search => {
                    storekey = Some(args[index + 1].clone());
                    storedist = false;
                    index += 2;
                }
                "STOREDIST" if remaining > 1 && !nostore && !search => {
                    storekey = Some(args[index + 1].clone());
                    storedist = true;
                    index += 2;
                }
                "STOREDIST" if search && search_store => {
                    storedist = true;
                    index += 1;
                }
                "FROMMEMBER" if remaining > 1 && search && !fromloc => {
                    if let Some(zset) = &source {
                        let Some(score) = zset.scores.get(&args[index + 1]).copied() else {
                            return Outcome::error("ERR could not decode requested zset member");
                        };
                        let Some(point) = geo::decode_score(score) else {
                            return Outcome::error("ERR could not decode requested zset member");
                        };
                        shape.xy = point;
                    }
                    frommember = true;
                    index += 2;
                }
                "FROMLONLAT" if remaining > 2 && search && !frommember => {
                    let (longitude, latitude) =
                        match parse_geo_coordinates(&args[index + 1..index + 3]) {
                            Ok(pair) => pair,
                            Err(outcome) => return outcome,
                        };
                    shape.xy = (longitude, latitude);
                    fromloc = true;
                    index += 3;
                }
                "BYRADIUS" if remaining > 2 && search && !bybox => {
                    shape.radius = match parse_geo_radius(&args[index + 1]) {
                        Ok(radius) => radius,
                        Err(outcome) => return outcome,
                    };
                    shape.conversion = match parse_geo_unit(&args[index + 2]) {
                        Ok(conversion) => conversion,
                        Err(outcome) => return outcome,
                    };
                    shape.circular = true;
                    byradius = true;
                    index += 3;
                }
                "BYBOX" if remaining > 3 && search && !byradius => {
                    shape.width = match parse_geo_coordinate_value(&args[index + 1], "width") {
                        Ok(value) => value,
                        Err(outcome) => return outcome,
                    };
                    shape.height = match parse_geo_coordinate_value(&args[index + 2], "height") {
                        Ok(value) => value,
                        Err(outcome) => return outcome,
                    };
                    if shape.width < 0.0 || shape.height < 0.0 {
                        return Outcome::error("ERR height or width cannot be negative");
                    }
                    shape.conversion = match parse_geo_unit(&args[index + 3]) {
                        Ok(conversion) => conversion,
                        Err(outcome) => return outcome,
                    };
                    shape.circular = false;
                    bybox = true;
                    index += 4;
                }
                _ => return Outcome::error("ERR syntax error"),
            }
        }

        if storekey.is_some() && (withdist || withhash || withcoords) {
            return Outcome::error(
                "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options",
            );
        }
        if search && !(frommember || fromloc) {
            return Outcome::error(format!(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {command}"
            ));
        }
        if search && !(byradius || bybox) {
            return Outcome::error(format!(
                "ERR exactly one of BYRADIUS and BYBOX can be specified for {command}"
            ));
        }
        if any && count == 0 {
            return Outcome::error("ERR the ANY argument requires COUNT argument");
        }

        let Some(zset) = source.take() else {
            if let Some(storekey) = &storekey {
                self.dbs[db_index].remove(storekey);
                return Outcome::reply(RespValue::Integer(0));
            }
            return Outcome::reply(RespValue::Array(Vec::new()));
        };

        if count != 0 && sort.is_none() && !any {
            sort = Some(true);
        }

        let radius = geo::calculate_areas(&mut shape);
        let limit = if any { count as usize } else { 0 };
        let members = sorted_members(&zset, false);
        let boxes = [
            radius.hash,
            radius.neighbors.north,
            radius.neighbors.south,
            radius.neighbors.east,
            radius.neighbors.west,
            radius.neighbors.north_east,
            radius.neighbors.north_west,
            radius.neighbors.south_east,
            radius.neighbors.south_west,
        ];
        let mut points: Vec<GeoPoint> = Vec::new();
        let mut last_processed: Option<geo::GeoHashBits> = None;
        for hash in boxes {
            if hash.is_zero() {
                continue;
            }
            if let Some(previous) = last_processed
                && previous.bits == hash.bits
                && previous.step == hash.step
            {
                continue;
            }
            if !points.is_empty() && limit > 0 && points.len() >= limit {
                break;
            }
            let (min, max) = geo::scores_of_box(hash);
            let lower = members.partition_point(|(_, score)| *score < min as f64);
            let upper = members.partition_point(|(_, score)| *score < max as f64);
            for (member, score) in &members[lower..upper] {
                let Some((longitude, latitude)) = geo::decode_score(*score) else {
                    continue;
                };
                let distance = if shape.circular {
                    geo::distance_if_in_circle(
                        shape.xy.0,
                        shape.xy.1,
                        longitude,
                        latitude,
                        shape.radius * shape.conversion,
                    )
                } else {
                    geo::distance_if_in_rectangle(
                        shape.width * shape.conversion,
                        shape.height * shape.conversion,
                        shape.xy.0,
                        shape.xy.1,
                        longitude,
                        latitude,
                    )
                };
                let Some(distance) = distance else {
                    continue;
                };
                points.push(GeoPoint {
                    member: member.clone(),
                    score: *score,
                    longitude,
                    latitude,
                    distance,
                });
                if limit > 0 && points.len() >= limit {
                    break;
                }
            }
            last_processed = Some(hash);
        }

        if points.is_empty() && storekey.is_none() {
            return Outcome::reply(RespValue::Array(Vec::new()));
        }

        if let Some(ascending) = sort {
            points.sort_by(|left, right| {
                let ordering = left
                    .distance
                    .partial_cmp(&right.distance)
                    .unwrap_or(std::cmp::Ordering::Equal);
                if ascending {
                    ordering
                } else {
                    ordering.reverse()
                }
            });
        }

        let returned_items = if count == 0 || (points.len() as i64) < count {
            points.len()
        } else {
            count as usize
        };

        if let Some(storekey) = storekey {
            if returned_items == 0 {
                self.dbs[db_index].remove(&storekey);
            } else {
                let mut result = rekeydb_core::SortedSet::default();
                for point in &points[..returned_items] {
                    let distance = point.distance / shape.conversion;
                    let score = if storedist { distance } else { point.score };
                    result.scores.insert(point.member.clone(), score);
                }
                let mvcc = self.clock.next(self.now_ms());
                self.dbs[db_index].insert(&storekey, Value::ZSet(result), mvcc);
            }
            return Outcome::reply(RespValue::Integer(returned_items as i64));
        }

        let option_length = usize::from(withdist) + usize::from(withhash) + usize::from(withcoords);
        let mut values = Vec::with_capacity(returned_items);
        for point in &points[..returned_items] {
            if option_length == 0 {
                values.push(RespValue::bulk(point.member.clone()));
                continue;
            }
            let mut item = Vec::with_capacity(option_length + 1);
            item.push(RespValue::bulk(point.member.clone()));
            if withdist {
                let distance = point.distance / shape.conversion;
                item.push(RespValue::bulk(format!("{distance:.4}").into_bytes()));
            }
            if withhash {
                item.push(RespValue::Integer(point.score as i64));
            }
            if withcoords {
                item.push(RespValue::Array(vec![
                    RespValue::bulk(format_geo_coordinate(point.longitude).into_bytes()),
                    RespValue::bulk(format_geo_coordinate(point.latitude).into_bytes()),
                ]));
            }
            values.push(RespValue::Array(item));
        }
        Outcome::reply(RespValue::Array(values))
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
                        RespValue::bulk(rekeydb_core::format_double_17g(*score).into_bytes()),
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
                    Some(entry) => {
                        let mut encoding = entry.value.encoding(self.encoding_limits);
                        if entry.value.as_string().is_some()
                            && self.raw_string_keys.contains(&(db_index, args[2].clone()))
                        {
                            encoding = "raw";
                        }
                        Outcome::reply(RespValue::bulk(encoding.as_bytes().to_vec()))
                    }
                    None => Outcome::error("ERR no such key"),
                }
            }
            "REFCOUNT" | "IDLETIME" | "FREQ" => {
                if args.len() != 3 {
                    return wrong_args(args);
                }
                self.expire_if_needed(db_index, &args[2]);
                if let Some(entry) = self.dbs[db_index].get(&args[2]) {
                    let refcount =
                        if sub == "REFCOUNT" {
                            match entry.value.as_string().and_then(|bytes| {
                                std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
                            }) {
                                Some(value) if (0..10_000).contains(&value) => 2_147_483_647,
                                _ => 1,
                            }
                        } else {
                            0
                        };
                    Outcome::reply(RespValue::Integer(refcount))
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
                    self.explicit_configs.insert(key.clone());
                    if key == "notify-keyspace-events" {
                        let Some(canonical) = canonical_notify_flags(&value) else {
                            return Outcome::error("ERR Invalid event flag");
                        };
                        self.config_values.insert(key, canonical);
                        continue;
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
                    match key.as_str() {
                        "appendonly" => {
                            let enabled =
                                matches!(value.to_ascii_lowercase().as_str(), "yes" | "true" | "1");
                            if enabled && self.aof.is_none() {
                                if let Err(err) = self.enable_aof() {
                                    return Outcome::error(format!("ERR {err}"));
                                }
                            } else if !enabled {
                                self.disable_aof();
                            }
                            self.config_values.insert(key, value);
                            continue;
                        }
                        "appendfsync" => {
                            let Some(fsync) = AofFsync::parse(&value) else {
                                return Outcome::error("ERR Invalid argument");
                            };
                            if let Some(writer) = self.aof.as_mut() {
                                writer.set_fsync(fsync);
                            }
                            self.aof_fsync = fsync;
                            self.config.appendfsync = value.clone();
                        }
                        "aof-use-rdb-preamble" => {
                            self.config.aof_use_rdb_preamble =
                                matches!(value.to_ascii_lowercase().as_str(), "yes" | "true" | "1");
                        }
                        "aof-load-truncated" => {
                            self.config.aof_load_truncated =
                                matches!(value.to_ascii_lowercase().as_str(), "yes" | "true" | "1");
                        }
                        _ => {}
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
                self.enforce_maxmemory();
                Outcome::ok()
            }
            "RESETSTAT" => {
                self.stats = Stats::default();
                self.command_calls.clear();
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

    fn cmd_info(&mut self, args: &[Vec<u8>]) -> Outcome {
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
            let aof_enabled = if self.aof.is_some() { 1 } else { 0 };
            let aof_size = std::fs::metadata(self.aof_path())
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            let _ = write!(
                out,
                "# Persistence\r\nloading:{}\r\nasync_loading:0\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\nrdb_changes_since_last_save:0\r\nrdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:-1\r\nrdb_current_bgsave_time_sec:-1\r\nrdb_saves:0\r\nrdb_last_cow_size:0\r\naof_enabled:{}\r\naof_rewrite_in_progress:{}\r\naof_rewrite_scheduled:{}\r\naof_last_rewrite_time_sec:{}\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:{}\r\naof_rewrites:{}\r\naof_rewrites_consecutive_failures:0\r\naof_last_write_status:{}\r\naof_last_cow_size:0\r\naof_current_size:{}\r\naof_base_size:{}\r\naof_pending_rewrite:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\n\r\n",
                u8::from(self.loading),
                u8::from(self.rdb_bgsave_in_progress),
                self.start_unix,
                aof_enabled,
                u8::from(self.aof_rewrite_in_progress),
                u8::from(self.aof_rewrite_scheduled),
                self.aof_last_rewrite_time_sec,
                self.aof_last_bgrewrite_status,
                self.aof_rewrites,
                self.aof_last_write_status,
                aof_size,
                aof_size,
            );
            self.aof_rewrite_in_progress = false;
            self.aof_rewrite_scheduled = false;
            self.rdb_bgsave_in_progress = false;
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
            let _ = write!(out, "# Replication\r\n");
            match self.replication.role {
                ReplicationRole::Master => {
                    let _ = write!(out, "role:master\r\n");
                    let _ = write!(
                        out,
                        "connected_slaves:{}\r\n",
                        self.replication.clients.len()
                    );
                    for (index, offset) in self.replication.clients.values().enumerate() {
                        let _ = writeln!(
                            out,
                            "slave{}:ip=127.0.0.1,port=0,state=online,offset={},lag=0",
                            index,
                            (*offset).max(0)
                        );
                    }
                    let _ = write!(out, "master_failover_state:no-failover\r\n");
                    let _ = write!(out, "master_replid:{}\r\n", self.replication.replid);
                    let _ = write!(out, "master_replid2:{}\r\n", "0".repeat(40));
                    let _ = write!(out, "master_repl_offset:{}\r\n", self.replication.offset);
                    let _ = write!(out, "second_repl_offset:-1\r\n");
                    let _ = write!(out, "repl_backlog_active:1\r\n");
                    let _ = write!(out, "repl_backlog_size:1048576\r\n");
                    let _ = write!(out, "repl_backlog_first_byte_offset:0\r\n");
                    let _ = write!(out, "repl_backlog_histlen:{}\r\n", self.replication.offset);
                }
                ReplicationRole::Replica => {
                    let idle = ((now_unix_ms() - self.replication.last_io_ms).max(0)) / 1000;
                    let _ = write!(out, "role:slave\r\n");
                    let _ = write!(out, "master_host:{}\r\n", self.replication.master_host);
                    let _ = write!(out, "master_port:{}\r\n", self.replication.master_port);
                    let _ = write!(
                        out,
                        "master_link_status:{}\r\n",
                        self.replication.link_status
                    );
                    let _ = write!(out, "master_last_io_seconds_ago:{idle}\r\n");
                    let _ = writeln!(
                        out,
                        "master_sync_in_progress:{}",
                        i64::from(self.replication.sync_in_progress)
                    );
                    let _ = write!(out, "slave_repl_offset:{}\r\n", self.replication.offset);
                    let _ = write!(out, "slave_read_only:1\r\n");
                    let _ = write!(out, "connected_slaves:0\r\n");
                    let _ = write!(out, "master_replid:{}\r\n", self.replication.master_replid);
                    let _ = write!(out, "master_repl_offset:{}\r\n", self.replication.offset);
                }
            }
            let _ = write!(out, "\r\n");
        }
        if section == "COMMANDSTATS" {
            let _ = writeln!(out, "# Commandstats");
            for (name, calls) in &self.command_calls {
                let _ = write!(
                    out,
                    "cmdstat_{}:calls={},usec=0,usec_per_call=0.00\r\n",
                    name.to_ascii_lowercase(),
                    calls
                );
            }
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
                self.client_names.insert(client.id, client.name.clone());
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
                let mut lines = String::new();
                for (id, name) in &self.client_names {
                    let is_current = *id == client.id;
                    let _ = writeln!(
                        lines,
                        "id={} addr={} laddr=127.0.0.1:{} fd=-1 name={} age={} idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-net-in=0 tot-net-out=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client|{} user=default redir=-1 resp={}",
                        id,
                        if is_current { client.addr.as_str() } else { "" },
                        self.config.port,
                        String::from_utf8_lossy(name),
                        self.start.elapsed().as_secs(),
                        client.db,
                        sub.to_ascii_lowercase(),
                        if client.version == RespVersion::V3 {
                            3
                        } else {
                            2
                        },
                    );
                }
                Outcome::reply(RespValue::bulk(lines.into_bytes()))
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
                    aof_commands: None,
                    extra: Vec::new(),
                }
            }
            "SETINFO" => Outcome::ok(),
            _ => Outcome::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.",
                String::from_utf8_lossy(&args[1])
            )),
        }
    }

    pub fn register_client(&mut self, id: u64) {
        self.client_names.entry(id).or_default();
    }

    pub fn unregister_client(&mut self, id: u64) {
        self.client_names.remove(&id);
        self.replication.clients.remove(&id);
        if let Some(subs) = self.client_subs.remove(&id) {
            for channel in subs.channels {
                if let Some(ids) = self.channel_subs.get_mut(&channel) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.channel_subs.remove(&channel);
                    }
                }
            }
            for pattern in subs.patterns {
                if let Some(ids) = self.pattern_subs.get_mut(&pattern) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.pattern_subs.remove(&pattern);
                    }
                }
            }
            for channel in subs.shards {
                if let Some(ids) = self.shard_subs.get_mut(&channel) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.shard_subs.remove(&channel);
                    }
                }
            }
        }
    }

    pub fn aof_path(&self) -> std::path::PathBuf {
        self.config.dir.join(&self.config.appendfilename)
    }

    pub fn enable_aof(&mut self) -> Result<(), String> {
        let path = self.aof_path();
        let fsync = AofFsync::parse(&self.config.appendfsync).unwrap_or(AofFsync::EverySec);
        self.aof_fsync = fsync;
        let writer = AofWriter::open(&path, fsync, false).map_err(|err| err.to_string())?;
        self.aof = Some(writer);
        self.rewrite_aof()?;
        self.config.appendonly = true;
        self.refresh_config_values();
        Ok(())
    }

    pub fn disable_aof(&mut self) {
        if self.aof_rewrite_in_progress || self.aof_rewrite_scheduled {
            println!("Killing AOF child process");
        }
        if let Some(mut writer) = self.aof.take() {
            let _ = writer.flush();
        }
        self.config.appendonly = false;
        self.aof_rewrite_in_progress = false;
        self.aof_rewrite_scheduled = false;
        self.refresh_config_values();
    }

    pub fn rewrite_aof(&mut self) -> Result<(), String> {
        let path = self.aof_path();
        let entries = self.aof_entries();
        let fsync = AofFsync::parse(&self.config.appendfsync).unwrap_or(self.aof_fsync);
        self.aof_rewrite_in_progress = true;
        let result = aof::rewrite(&path, &entries, fsync);
        self.aof_rewrite_in_progress = false;
        match result {
            Ok(()) => {
                let writer = AofWriter::open(&path, fsync, true).map_err(|err| err.to_string())?;
                self.aof = Some(writer);
                self.aof_fsync = fsync;
                self.aof_last_bgrewrite_status = "ok".to_string();
                self.aof_last_rewrite_time_sec = 0;
                self.aof_rewrites += 1;
                Ok(())
            }
            Err(err) => {
                self.aof_last_bgrewrite_status = format!("err: {err}");
                Err(err.to_string())
            }
        }
    }

    pub fn load_aof_from_disk(&mut self) -> Result<(), String> {
        let path = self.aof_path();
        if let Some(writer) = self.aof.as_mut() {
            writer.flush().map_err(|err| err.to_string())?;
        }
        let load = aof::load_with_options(&path)?;
        let file_len = std::fs::metadata(&path)
            .map(|metadata| metadata.len() as usize)
            .unwrap_or(0);
        let truncated = load.valid_bytes < file_len || load.open_multi_offset.is_some();
        if truncated {
            if !self.config.aof_load_truncated {
                return Err("Unexpected end of file reading the append only file".to_string());
            }
            let cut = load.open_multi_offset.unwrap_or(load.valid_bytes);
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|err| err.to_string())?;
            file.set_len(cut as u64).map_err(|err| err.to_string())?;
        }
        for db in &mut self.dbs {
            db.clear();
        }
        self.loading = true;
        let mut client = ClientContext::new(0, "aof-loader".to_string());
        for (db, args) in load.commands {
            client.db = db;
            let command = Command::new(args);
            let _ = self.execute(&command, &mut client);
        }
        client.multi = None;
        client.watched.clear();
        let now = self.now_ms();
        for db in &mut self.dbs {
            let expired: Vec<Vec<u8>> = db
                .collect_visible()
                .into_iter()
                .filter(|(_, entry)| entry.is_expired(now))
                .map(|(key, _)| key)
                .collect();
            for key in expired {
                db.remove(&key);
            }
        }
        self.loading = false;
        let fsync = AofFsync::parse(&self.config.appendfsync).unwrap_or(self.aof_fsync);
        self.aof = Some(AofWriter::open(&path, fsync, true).map_err(|err| err.to_string())?);
        Ok(())
    }

    fn aof_entries(&self) -> Vec<RdbEntry> {
        let now = self.now_ms();
        let mut entries = Vec::new();
        for (db_index, db) in self.dbs.iter().enumerate() {
            for (key, entry) in db.collect_visible() {
                if entry.is_expired(now) {
                    continue;
                }
                entries.push(RdbEntry {
                    db: db_index,
                    key,
                    value: (*entry.value).clone(),
                    expire_ms: entry.expire.primary(),
                });
            }
        }
        entries
    }

    fn append_aof_commands(&mut self, db: usize, commands: &[Vec<Vec<u8>>]) {
        if self.loading {
            return;
        }
        for args in commands {
            self.propagate(db, args);
        }
    }

    fn aof_propagate(&mut self, db: usize, command: &Command, outcome: &Outcome) {
        let Some(name) = command.name() else {
            return;
        };
        let name = String::from_utf8_lossy(name).to_ascii_uppercase();
        if let Some(commands) = &outcome.aof_commands {
            let commands = commands.clone();
            for args in &commands {
                self.propagate(db, args);
            }
            return;
        }
        if !is_aof_write(&name) {
            return;
        }
        let mut args = command.args.clone();
        if matches!(name.as_str(), "BLPOP" | "BRPOP") {
            let Some(RespValue::Array(values)) = Some(&outcome.reply) else {
                return;
            };
            let Some(RespValue::BulkString(key)) = values.first() else {
                return;
            };
            args = vec![
                if name == "BLPOP" {
                    b"LPOP".to_vec()
                } else {
                    b"RPOP".to_vec()
                },
                key.clone(),
            ];
        } else if name == "BRPOPLPUSH" {
            args = vec![b"RPOPLPUSH".to_vec(), args[1].clone(), args[2].clone()];
        } else if name == "BLMOVE" {
            args = vec![
                b"LMOVE".to_vec(),
                args[1].clone(),
                args[2].clone(),
                args[3].clone(),
                args[4].clone(),
            ];
        } else if name == "BZPOPMIN" || name == "BZPOPMAX" {
            let Some(RespValue::Array(values)) = Some(&outcome.reply) else {
                return;
            };
            let Some(RespValue::BulkString(key)) = values.first() else {
                return;
            };
            args = vec![
                if name == "BZPOPMIN" {
                    b"ZPOPMIN".to_vec()
                } else {
                    b"ZPOPMAX".to_vec()
                },
                key.clone(),
            ];
        } else if name == "GETDEL" {
            match &outcome.reply {
                RespValue::BulkString(_) => {
                    args = vec![b"DEL".to_vec(), args[1].clone()];
                }
                _ => return,
            }
        } else if name == "GETSET" {
            args = vec![b"SET".to_vec(), args[1].clone(), args[2].clone()];
        } else if name == "SPOP" {
            let mut members = Vec::new();
            match &outcome.reply {
                RespValue::BulkString(member) => members.push(member.clone()),
                RespValue::Array(values) => {
                    for value in values {
                        if let RespValue::BulkString(member) = value {
                            members.push(member.clone());
                        }
                    }
                }
                _ => {}
            }
            if members.is_empty() {
                return;
            }
            args = vec![b"SREM".to_vec(), args[1].clone()];
            args.extend(members);
        }
        self.propagate(db, &args);
    }

    fn propagate(&mut self, db: usize, args: &[Vec<u8>]) {
        if !self.loading
            && let Some(writer) = self.aof.as_mut()
            && let Err(err) = writer.append(db, args)
        {
            self.aof_last_write_status = format!("err: {err}");
        }
        if self.replication.role != ReplicationRole::Master || self.replication.clients.is_empty() {
            return;
        }
        let mut bytes = Vec::new();
        if self.replication.selected_db != Some(db) {
            let select = [b"SELECT".to_vec(), db.to_string().into_bytes()];
            let refs: Vec<&[u8]> = select.iter().map(Vec::as_slice).collect();
            bytes.extend_from_slice(&encode_command(&refs));
            self.replication.selected_db = Some(db);
        }
        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        bytes.extend_from_slice(&encode_command(&refs));
        self.replication.offset += bytes.len() as i64;
        self.replication.pending.extend_from_slice(&bytes);
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
                let mut encoding = entry.value.encoding(self.encoding_limits);
                if entry.value.as_string().is_some()
                    && self.raw_string_keys.contains(&(client.db, args[2].clone()))
                {
                    encoding = "raw";
                }
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
            "LOADAOF" => {
                if args.len() != 2 {
                    return wrong_args(args);
                }
                match self.load_aof_from_disk() {
                    Ok(()) => Outcome::ok(),
                    Err(err) => Outcome::error(format!("ERR {err}")),
                }
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
            "DIGEST-VALUE" => {
                if args.len() < 3 {
                    return wrong_args(args);
                }
                let mut values = Vec::new();
                for key in &args[2..] {
                    self.expire_if_needed(client.db, key);
                    let digest = match self.dbs[client.db].get(key) {
                        Some(entry) => value_digest(&entry.value, entry.expire.primary().is_some()),
                        None => "0".repeat(40),
                    };
                    values.push(RespValue::simple(digest));
                }
                Outcome::reply(RespValue::Array(values))
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
            "SET-ACTIVE-EXPIRE"
            | "QUICKLIST-PACKED-THRESHOLD"
            | "STRINGMATCH-LEN"
            | "AOF-FLUSH-SLEEP" => Outcome::ok(),
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

fn hll_error(error: hll::HllError) -> Outcome {
    match error {
        hll::HllError::Invalid => {
            Outcome::error("WRONGTYPE Key is not a valid HyperLogLog string value.")
        }
        hll::HllError::Corrupted => Outcome::error("INVALIDOBJ Corrupted HLL object detected"),
    }
}

#[derive(Debug, Clone)]
enum StreamTrimStrategy {
    None,
    MaxLen(usize),
    MinId(StreamId),
}

struct ParsedStreamTrim {
    strategy: StreamTrimStrategy,
    approx: bool,
    limit: usize,
    limit_given: bool,
    value_index: usize,
    id: Option<StreamId>,
    id_given: bool,
    nomkstream: bool,
    id_position: usize,
}

fn parse_stream_id(value: &[u8], default_seq: u64, strict: bool) -> Option<StreamId> {
    let text = std::str::from_utf8(value).ok()?;
    if strict && (text == "-" || text == "+") {
        return None;
    }
    StreamId::parse(text, default_seq)
}

fn parse_stream_range(value: &[u8], missing_seq: u64) -> Result<(StreamId, bool), Outcome> {
    if value.len() > 1 && value[0] == b'(' {
        match parse_stream_id(&value[1..], missing_seq, true) {
            Some(id) => Ok((id, true)),
            None => Err(invalid_stream_id()),
        }
    } else {
        match parse_stream_id(value, missing_seq, false) {
            Some(id) => Ok((id, false)),
            None => Err(invalid_stream_id()),
        }
    }
}

fn invalid_stream_id() -> Outcome {
    Outcome::error("ERR Invalid stream ID specified as stream command argument")
}

fn stream_entry_reply(entry: &StreamEntry) -> RespValue {
    stream_entry_reply_fields(entry.id, &entry.fields)
}

fn stream_entry_reply_fields(id: StreamId, fields: &[(Vec<u8>, Vec<u8>)]) -> RespValue {
    let mut pairs = Vec::with_capacity(fields.len() * 2);
    for (field, value) in fields {
        pairs.push(RespValue::bulk(field.clone()));
        pairs.push(RespValue::bulk(value.clone()));
    }
    RespValue::Array(vec![
        RespValue::bulk(id.to_string().into_bytes()),
        RespValue::Array(pairs),
    ])
}

fn no_group_message(key: &[u8], group: &[u8], xreadgroup: bool) -> Vec<u8> {
    if xreadgroup {
        format!(
            "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(group)
        )
        .into_bytes()
    } else {
        format!(
            "NOGROUP No such key '{}' or consumer group '{}'",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(group)
        )
        .into_bytes()
    }
}

fn xgroup_syntax_error(subcommand: &[u8]) -> Outcome {
    Outcome::error(format!(
        "ERR Unknown subcommand or wrong number of arguments for '{}'. Try XGROUP HELP.",
        String::from_utf8_lossy(subcommand)
    ))
}

fn xinfo_syntax_error() -> Outcome {
    Outcome::error(
        "ERR Unknown subcommand or wrong number of arguments for 'XINFO'. Try XINFO HELP.",
    )
}

fn stream_read_reply(served: &[(Vec<u8>, Vec<RespValue>)], version: RespVersion) -> RespValue {
    if matches!(version, RespVersion::V3) {
        RespValue::Map(
            served
                .iter()
                .map(|(key, entries)| {
                    (
                        RespValue::bulk(key.clone()),
                        RespValue::Array(entries.clone()),
                    )
                })
                .collect(),
        )
    } else {
        RespValue::Array(
            served
                .iter()
                .map(|(key, entries)| {
                    RespValue::Array(vec![
                        RespValue::bulk(key.clone()),
                        RespValue::Array(entries.clone()),
                    ])
                })
                .collect(),
        )
    }
}

fn stream_xclaim_command(
    key: &[u8],
    group: &[u8],
    consumer: &[u8],
    id: StreamId,
    delivery_time: i64,
    delivery_count: u64,
    last_id: StreamId,
) -> Vec<Vec<u8>> {
    vec![
        b"XCLAIM".to_vec(),
        key.to_vec(),
        group.to_vec(),
        consumer.to_vec(),
        b"0".to_vec(),
        id.to_string().into_bytes(),
        b"TIME".to_vec(),
        delivery_time.to_string().into_bytes(),
        b"RETRYCOUNT".to_vec(),
        delivery_count.to_string().into_bytes(),
        b"FORCE".to_vec(),
        b"JUSTID".to_vec(),
        b"LASTID".to_vec(),
        last_id.to_string().into_bytes(),
    ]
}

type StreamFields = Vec<(Vec<u8>, Vec<u8>)>;
type StreamReadResult = (Vec<RespValue>, Vec<Vec<Vec<u8>>>);

struct GeoPoint {
    member: Vec<u8>,
    score: f64,
    longitude: f64,
    latitude: f64,
    distance: f64,
}

fn parse_geo_coordinates(args: &[Vec<u8>]) -> Result<(f64, f64), Outcome> {
    let Some(longitude) = parse_score(&args[0]) else {
        return Err(Outcome::error("ERR value is not a valid float"));
    };
    let Some(latitude) = parse_score(&args[1]) else {
        return Err(Outcome::error("ERR value is not a valid float"));
    };
    if !(geo::GEO_LONG_MIN..=geo::GEO_LONG_MAX).contains(&longitude)
        || !(geo::GEO_LAT_MIN..=geo::GEO_LAT_MAX).contains(&latitude)
    {
        return Err(invalid_geo_pair(longitude, latitude));
    }
    Ok((longitude, latitude))
}

fn invalid_geo_pair(longitude: f64, latitude: f64) -> Outcome {
    Outcome::error(format!(
        "ERR invalid longitude,latitude pair {longitude:.6},{latitude:.6}"
    ))
}

fn parse_geo_radius(value: &[u8]) -> Result<f64, Outcome> {
    let Some(radius) = parse_score(value) else {
        return Err(Outcome::error("ERR need numeric radius"));
    };
    if radius < 0.0 {
        return Err(Outcome::error("ERR radius cannot be negative"));
    }
    Ok(radius)
}

fn parse_geo_coordinate_value(value: &[u8], name: &str) -> Result<f64, Outcome> {
    match parse_score(value) {
        Some(value) => Ok(value),
        None => Err(Outcome::error(format!("ERR need numeric {name}"))),
    }
}

fn parse_geo_unit(value: &[u8]) -> Result<f64, Outcome> {
    match value {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => Err(Outcome::error(
            "ERR unsupported unit provided. please use m, km, ft, mi",
        )),
    }
}

/// Format a decoded coordinate the way `addReplyHumanLongDouble` does:
/// seventeen fractional digits with trailing zeroes removed.
fn format_geo_coordinate(value: f64) -> String {
    let mut text = format!("{value:.17}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    text
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
        RespValue::bulk(rekeydb_core::format_double_17g(score).into_bytes())
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

fn is_aof_write(name: &str) -> bool {
    matches!(
        name,
        "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "APPEND"
            | "SETRANGE"
            | "SETBIT"
            | "BITOP"
            | "BITFIELD"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "INCRBYFLOAT"
            | "MSETNX"
            | "GETSET"
            | "GETDEL"
            | "MSET"
            | "DEL"
            | "UNLINK"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "RENAME"
            | "RENAMENX"
            | "MOVE"
            | "COPY"
            | "SWAPDB"
            | "RESTORE"
            | "GEOADD"
            | "XADD"
            | "XDEL"
            | "XTRIM"
            | "XSETID"
            | "XGROUP"
            | "XACK"
            | "XCLAIM"
            | "XAUTOCLAIM"
            | "XREADGROUP"
            | "PFADD"
            | "PFMERGE"
            | "GEORADIUS"
            | "GEORADIUSBYMEMBER"
            | "GEOSEARCHSTORE"
            | "FLUSHDB"
            | "FLUSHALL"
            | "LPUSH"
            | "RPUSH"
            | "LPUSHX"
            | "RPUSHX"
            | "LPOP"
            | "RPOP"
            | "LSET"
            | "LTRIM"
            | "LREM"
            | "LINSERT"
            | "RPOPLPUSH"
            | "LMOVE"
            | "BLPOP"
            | "BRPOP"
            | "BRPOPLPUSH"
            | "BLMOVE"
            | "SADD"
            | "SREM"
            | "SPOP"
            | "SMOVE"
            | "SINTERSTORE"
            | "SUNIONSTORE"
            | "SDIFFSTORE"
            | "HSET"
            | "HSETNX"
            | "HMSET"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "KEYDB.HRENAME"
            | "ZADD"
            | "ZINCRBY"
            | "ZREM"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYLEX"
            | "ZUNIONSTORE"
            | "ZINTERSTORE"
            | "ZDIFFSTORE"
            | "ZRANGESTORE"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "SORT"
    )
}

#[derive(Debug, Clone, Copy)]
enum BitOverflow {
    Wrap,
    Sat,
    Fail,
}

fn parse_memory(text: &str) -> Option<u64> {
    let text = text.trim().to_ascii_lowercase();
    let (value, multiplier) = if let Some(value) = text.strip_suffix("kb") {
        (value, 1024u64)
    } else if let Some(value) = text.strip_suffix("mb") {
        (value, 1024 * 1024)
    } else if let Some(value) = text.strip_suffix("gb") {
        (value, 1024 * 1024 * 1024)
    } else {
        (text.as_str(), 1)
    };
    value.parse::<u64>().ok()?.checked_mul(multiplier)
}

fn parse_bitfield_type(value: &[u8]) -> Option<(bool, u8)> {
    let text = std::str::from_utf8(value).ok()?;
    let (signed, number) = match text.as_bytes().first()? {
        b'i' => (true, &text[1..]),
        b'u' => (false, &text[1..]),
        _ => return None,
    };
    let bits = number.parse::<u8>().ok()?;
    if bits == 0 || bits > 64 || (!signed && bits == 64) {
        return None;
    }
    Some((signed, bits))
}

fn read_bitfield(bytes: &[u8], offset: u64, bits: u8) -> u64 {
    let mut value = 0u64;
    for index in 0..u64::from(bits) {
        value <<= 1;
        let bit_position = offset + index;
        let byte = bytes.get((bit_position / 8) as usize).copied().unwrap_or(0);
        if byte & (1 << (7 - bit_position % 8)) != 0 {
            value |= 1;
        }
    }
    value
}

fn ensure_bitfield_len(bytes: &mut Vec<u8>, offset: u64, bits: u8) {
    let needed = (offset + u64::from(bits)).div_ceil(8) as usize;
    if bytes.len() < needed {
        bytes.resize(needed, 0);
    }
}

fn write_bitfield(bytes: &mut [u8], offset: u64, bits: u8, value: i128, _signed: bool) {
    let raw = (value as u64) & bitfield_mask(bits);
    for index in 0..u64::from(bits) {
        let target = bits as u64 - 1 - index;
        let bit_position = offset + index;
        let byte_index = (bit_position / 8) as usize;
        let mask = 1 << (7 - bit_position % 8);
        if raw & (1 << target) != 0 {
            bytes[byte_index] |= mask;
        } else {
            bytes[byte_index] &= !mask;
        }
    }
}

fn bitfield_mask(bits: u8) -> u64 {
    if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

fn bitfield_limits(signed: bool, bits: u8) -> (i128, i128) {
    if signed {
        let half = 1i128 << (bits - 1);
        (-half, half - 1)
    } else {
        (0, (1i128 << bits) - 1)
    }
}

fn bitfield_value(raw: u64, signed: bool, bits: u8) -> i64 {
    let raw = raw & bitfield_mask(bits);
    if signed {
        let sign = 1u64 << (bits - 1);
        if raw & sign != 0 {
            (raw as i128 - (1i128 << bits)) as i64
        } else {
            raw as i64
        }
    } else {
        raw as i64
    }
}

fn wrap_bitfield(value: i128, signed: bool, bits: u8) -> i128 {
    let mask = (1i128 << bits) - 1;
    let raw = value & mask;
    if signed {
        let sign = 1i128 << (bits - 1);
        if raw & sign != 0 {
            raw - (1i128 << bits)
        } else {
            raw
        }
    } else {
        raw
    }
}

fn shift_bits(source: &[u8], shift: i64) -> Vec<u8> {
    let maxlen = source.len() as i64;
    if shift >= 0 {
        let byteoffset = (shift / 8) as usize;
        let bitshift = (shift % 8) as u32;
        let mut length = maxlen + (shift / 8);
        if bitshift != 0 {
            length += 1;
        }
        let mut result = vec![0u8; length.max(0) as usize];
        let src_len = (length - byteoffset as i64 - i64::from(bitshift != 0)).max(0) as usize;
        let mut carry = 0u8;
        for index in 0..src_len {
            let byte = *source.get(index).unwrap_or(&0);
            result[byteoffset + index] = (byte << bitshift) | carry;
            carry = if bitshift == 0 {
                0
            } else {
                byte >> (8 - bitshift)
            };
        }
        if bitshift != 0
            && let Some(last) = result.last_mut()
        {
            *last = carry;
        }
        result
    } else {
        let shift = shift.unsigned_abs() as i64;
        let byteoffset_base = (shift / 8) as usize;
        let bitshift = (shift % 8) as u32;
        let mut byteoffset = byteoffset_base;
        if bitshift != 0 {
            byteoffset += 1;
        }
        let length = (maxlen - (shift / 8)).max(0) as usize;
        let mut result = vec![0u8; length];
        if length == 0 {
            return result;
        }
        let first = *source.get(byteoffset).unwrap_or(&0);
        let previous = *source.get(byteoffset.saturating_sub(1)).unwrap_or(&0);
        result[0] = if bitshift == 0 {
            previous
        } else {
            (first << (8 - bitshift)) | (previous >> bitshift)
        };
        for (index, slot) in result.iter_mut().enumerate().skip(1) {
            let current = *source.get(byteoffset + index).unwrap_or(&0);
            let previous = *source.get(byteoffset + index - 1).unwrap_or(&0);
            *slot = if bitshift == 0 {
                previous
            } else {
                (current << (8 - bitshift)) | (previous >> bitshift)
            };
        }
        result
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value == f64::INFINITY {
        return "inf".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }
    let mut text = format!("{value}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text == "-0" {
        text = "0".to_string();
    }
    text
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
            | "save"
            | "tcp-keepalive"
            | "timeout"
            | "hz"
            | "loglevel"
            | "lua-time-limit"
            | "appendonly"
            | "appendfsync"
            | "proto-max-bulk-len"
            | "aof-use-rdb-preamble"
            | "aof-load-truncated"
            | "repl-ping-replica-period"
            | "repl-timeout"
            | "repl-diskless-sync"
            | "repl-diskless-sync-delay"
            | "repl-diskless-load"
            | "repl-disable-tcp-nodelay"
            | "repl-backlog-size"
            | "notify-keyspace-events"
            | "replica-read-only"
            | "replica-serve-stale-data"
            | "auto-aof-rewrite-percentage"
            | "auto-aof-rewrite-min-size"
            | "no-appendfsync-on-rewrite"
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
            | "hll-sparse-max-bytes"
            | "stream-node-max-entries"
    )
}

fn canonical_notify_flags(value: &str) -> Option<String> {
    const ALLOWED: &str = "KEAg$lshzxetmn";
    if !value.chars().all(|ch| ALLOWED.contains(ch)) {
        return None;
    }
    let mut seen = String::new();
    for ch in value.chars() {
        if !seen.contains(ch) {
            seen.push(ch);
        }
    }
    if seen.contains('A') {
        let mut out = String::from("A");
        if seen.contains('K') {
            out.push('K');
        }
        if seen.contains('E') {
            out.push('E');
        }
        return Some(out);
    }
    Some(seen)
}

fn push_frame(parts: Vec<RespValue>, version: RespVersion) -> RespValue {
    if version == RespVersion::V3 {
        RespValue::Push(parts)
    } else {
        RespValue::Array(parts)
    }
}

fn subscription_frame(
    kind: &[u8],
    channel: Option<&[u8]>,
    count: usize,
    version: RespVersion,
) -> RespValue {
    push_frame(
        vec![
            RespValue::bulk(kind.to_vec()),
            channel.map_or(RespValue::Null, |channel| RespValue::bulk(channel.to_vec())),
            RespValue::Integer(count as i64),
        ],
        version,
    )
}

fn new_replid() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let mut state = nanos ^ (u64::from(std::process::id()) << 32);
    let mut out = String::with_capacity(40);
    for _ in 0..20 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        out.push_str(&format!("{:02x}", (state >> 56) as u8));
    }
    out
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
        run_full(state, client, args).reply
    }

    fn run_full(
        state: &mut State,
        client: &mut ClientContext,
        args: &[&[u8]],
    ) -> crate::state::Outcome {
        let command = Command::new(args.iter().map(|arg| arg.to_vec()).collect());
        state.execute(&command, client)
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
    #[test]
    fn pubsub_subscribe_publish() {
        let mut state = state();
        let mut sub = ClientContext::new(2, "sub".to_string());
        let outcome = run_full(&mut state, &mut sub, &[b"SUBSCRIBE", b"chan1", b"chan2"]);
        assert_eq!(
            outcome.reply,
            RespValue::Array(vec![
                RespValue::bulk(b"subscribe".to_vec()),
                RespValue::bulk(b"chan1".to_vec()),
                RespValue::Integer(1),
            ])
        );
        assert_eq!(
            outcome.extra,
            vec![RespValue::Array(vec![
                RespValue::bulk(b"subscribe".to_vec()),
                RespValue::bulk(b"chan2".to_vec()),
                RespValue::Integer(2),
            ])]
        );
        let mut publisher = new_client();
        assert_eq!(
            run(
                &mut state,
                &mut publisher,
                &[b"PUBLISH", b"chan1", b"hello"]
            ),
            RespValue::Integer(1)
        );
        let deliveries = state.take_pubsub_deliveries();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].0, 2);
        assert_eq!(
            deliveries[0].1,
            RespValue::Array(vec![
                RespValue::bulk(b"message".to_vec()),
                RespValue::bulk(b"chan1".to_vec()),
                RespValue::bulk(b"hello".to_vec()),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                &mut publisher,
                &[b"PUBLISH", b"chan2", b"world"]
            ),
            RespValue::Integer(1)
        );
        let mut other = ClientContext::new(3, "other".to_string());
        assert_eq!(
            run(&mut state, &mut other, &[b"PUBLISH", b"chan1", b"x"]),
            RespValue::Integer(1)
        );
    }

    #[test]
    fn pubsub_patterns_and_pubsub_queries() {
        let mut state = state();
        let mut sub = ClientContext::new(2, "sub".to_string());
        assert_eq!(
            run(&mut state, &mut sub, &[b"PSUBSCRIBE", b"foo.*"]),
            RespValue::Array(vec![
                RespValue::bulk(b"psubscribe".to_vec()),
                RespValue::bulk(b"foo.*".to_vec()),
                RespValue::Integer(1),
            ])
        );
        let mut publisher = new_client();
        assert_eq!(
            run(
                &mut state,
                &mut publisher,
                &[b"PUBLISH", b"foo.bar", b"hello"]
            ),
            RespValue::Integer(1)
        );
        let deliveries = state.take_pubsub_deliveries();
        assert_eq!(
            deliveries[0].1,
            RespValue::Array(vec![
                RespValue::bulk(b"pmessage".to_vec()),
                RespValue::bulk(b"foo.*".to_vec()),
                RespValue::bulk(b"foo.bar".to_vec()),
                RespValue::bulk(b"hello".to_vec()),
            ])
        );
        assert_eq!(
            run(&mut state, &mut publisher, &[b"PUBSUB", b"NUMPAT"]),
            RespValue::Integer(1)
        );
        assert_eq!(
            run(
                &mut state,
                &mut publisher,
                &[b"PUBSUB", b"NUMSUB", b"foo.bar"]
            ),
            RespValue::Array(vec![
                RespValue::bulk(b"foo.bar".to_vec()),
                RespValue::Integer(0),
            ])
        );
        assert_eq!(
            run(&mut state, &mut publisher, &[b"PUBSUB", b"CHANNELS"]),
            RespValue::Array(vec![])
        );
    }

    #[test]
    fn pubsub_unsubscribe_all_and_restriction() {
        let mut state = state();
        let mut sub = ClientContext::new(2, "sub".to_string());
        run_full(&mut state, &mut sub, &[b"SUBSCRIBE", b"a", b"b"]);
        assert!(matches!(
            run(&mut state, &mut sub, &[b"GET", b"x"]),
            RespValue::Error(_)
        ));
        let outcome = run_full(&mut state, &mut sub, &[b"UNSUBSCRIBE"]);
        assert_eq!(
            outcome.reply,
            RespValue::Array(vec![
                RespValue::bulk(b"unsubscribe".to_vec()),
                RespValue::bulk(b"a".to_vec()),
                RespValue::Integer(1),
            ])
        );
        assert_eq!(outcome.extra.len(), 1);
        let mut publisher = new_client();
        assert_eq!(
            run(&mut state, &mut publisher, &[b"PUBLISH", b"a", b"x"]),
            RespValue::Integer(0)
        );
        let outcome = run_full(&mut state, &mut sub, &[b"UNSUBSCRIBE"]);
        assert_eq!(
            outcome.reply,
            RespValue::Array(vec![
                RespValue::bulk(b"unsubscribe".to_vec()),
                RespValue::Null,
                RespValue::Integer(0),
            ])
        );
        assert_eq!(run(&mut state, &mut sub, &[b"GET", b"x"]), RespValue::Null);
    }

    #[test]
    fn pubsub_sharded() {
        let mut state = state();
        let mut sub = ClientContext::new(2, "sub".to_string());
        run_full(&mut state, &mut sub, &[b"SSUBSCRIBE", b"schan"]);
        let mut publisher = new_client();
        assert_eq!(
            run(&mut state, &mut publisher, &[b"PUBLISH", b"schan", b"x"]),
            RespValue::Integer(0)
        );
        assert_eq!(
            run(&mut state, &mut publisher, &[b"SPUBLISH", b"schan", b"y"]),
            RespValue::Integer(1)
        );
        let deliveries = state.take_pubsub_deliveries();
        assert_eq!(
            deliveries[0].1,
            RespValue::Array(vec![
                RespValue::bulk(b"smessage".to_vec()),
                RespValue::bulk(b"schan".to_vec()),
                RespValue::bulk(b"y".to_vec()),
            ])
        );
        assert_eq!(
            run(
                &mut state,
                &mut publisher,
                &[b"PUBSUB", b"SHARDNUMSUB", b"schan"]
            ),
            RespValue::Array(vec![
                RespValue::bulk(b"schan".to_vec()),
                RespValue::Integer(1),
            ])
        );
    }
}
