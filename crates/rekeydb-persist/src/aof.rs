//! Append-only file writing, loading, and command-stream rewriting.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rekeydb_core::{Stream, StreamGroup, Value, format_double_17g};
use rekeydb_protocol::{Command, RespDecoder, encode_command};

use crate::rdb::RdbEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofFsync {
    No,
    EverySec,
    Always,
}

impl AofFsync {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "no" => Some(Self::No),
            "everysec" => Some(Self::EverySec),
            "always" => Some(Self::Always),
            _ => None,
        }
    }
}

pub struct AofWriter {
    file: BufWriter<File>,
    path: PathBuf,
    selected_db: Option<usize>,
    fsync: AofFsync,
    flush_each: bool,
    last_sync: Instant,
}

impl AofWriter {
    pub fn open(path: &Path, fsync: AofFsync, append: bool) -> std::io::Result<Self> {
        Self::open_with_mode(path, fsync, append, true)
    }

    /// Buffered variant used by rewrite: data is flushed once at the end.
    pub fn open_buffered(path: &Path, fsync: AofFsync, append: bool) -> std::io::Result<Self> {
        Self::open_with_mode(path, fsync, append, false)
    }

    fn open_with_mode(
        path: &Path,
        fsync: AofFsync,
        append: bool,
        flush_each: bool,
    ) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(path)?;
        Ok(Self {
            file: BufWriter::with_capacity(1024 * 1024, file),
            path: path.to_path_buf(),
            selected_db: None,
            fsync,
            flush_each,
            last_sync: Instant::now(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn set_fsync(&mut self, fsync: AofFsync) {
        self.fsync = fsync;
    }

    pub fn append(&mut self, db: usize, args: &[Vec<u8>]) -> std::io::Result<()> {
        if self.selected_db != Some(db) {
            self.file
                .write_all(&encode_command(&[b"SELECT", db.to_string().as_bytes()]))?;
            self.selected_db = Some(db);
        }
        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        self.file.write_all(&encode_command(&refs))?;
        if self.flush_each {
            self.file.flush()?;
        }
        self.sync_if_needed()
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.sync_if_needed()
    }

    pub fn sync_all(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }

    fn sync_if_needed(&mut self) -> std::io::Result<()> {
        match self.fsync {
            AofFsync::No => Ok(()),
            AofFsync::Always => {
                self.file.flush()?;
                self.file.get_ref().sync_data()?;
                self.last_sync = Instant::now();
                Ok(())
            }
            AofFsync::EverySec => {
                if self.last_sync.elapsed() >= Duration::from_secs(1) {
                    self.file.flush()?;
                    self.file.get_ref().sync_data()?;
                    self.last_sync = Instant::now();
                }
                Ok(())
            }
        }
    }
}

impl Drop for AofWriter {
    fn drop(&mut self) {
        let _ = self.file.flush();
    }
}

pub type AofCommand = (usize, Vec<Vec<u8>>);

#[derive(Debug)]
pub struct AofLoad {
    pub commands: Vec<AofCommand>,
    pub valid_bytes: usize,
    pub open_multi_offset: Option<usize>,
}

pub fn load(path: &Path) -> Result<Vec<AofCommand>, String> {
    Ok(load_with_options(path)?.commands)
}

pub fn load_with_options(path: &Path) -> Result<AofLoad, String> {
    let mut file = File::open(path).map_err(|err| format!("{}: {err}", path.display()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|err| format!("{}: {err}", path.display()))?;
    if data.starts_with(b"REDIS") {
        return Err("RDB-preamble AOF is not supported yet".to_string());
    }
    let mut decoder = RespDecoder::default();
    decoder.feed(&data);
    let mut db = 0usize;
    let mut commands = Vec::new();
    let mut open_multi_offset = None;
    loop {
        if let Some(byte) = decoder.peek_byte()
            && byte != b'*'
        {
            return Err(
                "Bad file format reading the append only file: expected a RESP array".to_string(),
            );
        }
        let before = decoder.position();
        match decoder.next_command() {
            Ok(Some(command)) => {
                if command.args.is_empty() {
                    continue;
                }
                let name = String::from_utf8_lossy(&command.args[0]).to_ascii_uppercase();
                if name == "MULTI" {
                    open_multi_offset.get_or_insert(before);
                } else if name == "EXEC" || name == "DISCARD" {
                    open_multi_offset = None;
                }
                if name == "SELECT" && command.args.len() == 2 {
                    db = String::from_utf8_lossy(&command.args[1])
                        .parse::<usize>()
                        .map_err(|_| "invalid SELECT in AOF".to_string())?;
                } else {
                    commands.push((db, command.args));
                }
            }
            Ok(None) => break,
            Err(err) => {
                return Err(format!(
                    "Bad file format reading the append only file: {err}"
                ));
            }
        }
    }
    Ok(AofLoad {
        commands,
        valid_bytes: decoder.position(),
        open_multi_offset,
    })
}

/// Serialize a dataset as a RESP command stream, used as the full-sync
/// payload for replicas.
pub fn encode_entries(entries: &[RdbEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    let max_db = entries.iter().map(|entry| entry.db).max().unwrap_or(0);
    for db in 0..=max_db {
        let rows: Vec<&RdbEntry> = entries.iter().filter(|entry| entry.db == db).collect();
        if rows.is_empty() {
            continue;
        }
        let select = [b"SELECT".to_vec(), db.to_string().into_bytes()];
        let refs: Vec<&[u8]> = select.iter().map(Vec::as_slice).collect();
        out.extend_from_slice(&encode_command(&refs));
        let mut emit = |_db: usize, args: &[Vec<u8>]| -> std::io::Result<()> {
            let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            out.extend_from_slice(&encode_command(&refs));
            Ok(())
        };
        for entry in rows {
            write_entry(&mut emit, db, entry).expect("in-memory emit");
        }
    }
    out
}

/// Rewrite the current dataset as a command-only AOF.
pub fn rewrite(path: &Path, entries: &[RdbEntry], fsync: AofFsync) -> std::io::Result<()> {
    let temp = path.with_extension("aof.tmp");
    let mut writer = AofWriter::open_buffered(&temp, fsync, false)?;
    let max_db = entries.iter().map(|entry| entry.db).max().unwrap_or(0);
    for db in 0..=max_db {
        let rows: Vec<&RdbEntry> = entries.iter().filter(|entry| entry.db == db).collect();
        if rows.is_empty() {
            continue;
        }
        writer.append(db, &[b"SELECT".to_vec(), db.to_string().into_bytes()])?;
        for entry in rows {
            write_entry(&mut |db, args| writer.append(db, args), db, entry)?;
        }
    }
    writer.sync_all()?;
    drop(writer);
    std::fs::rename(&temp, path)?;
    Ok(())
}

fn write_entry<F>(emit: &mut F, db: usize, entry: &RdbEntry) -> std::io::Result<()>
where
    F: FnMut(usize, &[Vec<u8>]) -> std::io::Result<()>,
{
    match &entry.value {
        Value::String(bytes) => {
            emit(db, &[b"SET".to_vec(), entry.key.clone(), bytes.clone()])?;
        }
        Value::List(list) => {
            let mut items: Vec<&Vec<u8>> = list.iter().collect();
            while !items.is_empty() {
                let take = items.len().min(64);
                let chunk: Vec<&Vec<u8>> = items.drain(..take).collect();
                let mut args = vec![b"RPUSH".to_vec(), entry.key.clone()];
                args.extend(chunk.into_iter().cloned());
                emit(db, &args)?;
            }
        }
        Value::Set(set) => {
            let mut items: Vec<&Vec<u8>> = set.iter().collect();
            items.sort();
            while !items.is_empty() {
                let take = items.len().min(64);
                let chunk: Vec<&Vec<u8>> = items.drain(..take).collect();
                let mut args = vec![b"SADD".to_vec(), entry.key.clone()];
                args.extend(chunk.into_iter().cloned());
                emit(db, &args)?;
            }
        }
        Value::Hash(map) => {
            let mut items: Vec<(&Vec<u8>, &Vec<u8>)> = map.iter().collect();
            while !items.is_empty() {
                let take = items.len().min(64);
                let chunk: Vec<(&Vec<u8>, &Vec<u8>)> = items.drain(..take).collect();
                let mut args = vec![b"HSET".to_vec(), entry.key.clone()];
                for (field, value) in chunk {
                    args.push(field.clone());
                    args.push(value.clone());
                }
                emit(db, &args)?;
            }
        }
        Value::Stream(stream) => {
            write_stream(emit, db, &entry.key, stream)?;
        }
        Value::ZSet(zset) => {
            let mut items: Vec<(&Vec<u8>, f64)> = zset
                .scores
                .iter()
                .map(|(member, score)| (member, *score))
                .collect();
            items.sort_by(|left, right| {
                left.1
                    .partial_cmp(&right.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.0.cmp(right.0))
            });
            while !items.is_empty() {
                let take = items.len().min(64);
                let chunk: Vec<(&Vec<u8>, f64)> = items.drain(..take).collect();
                let mut args = vec![b"ZADD".to_vec(), entry.key.clone()];
                for (member, score) in chunk {
                    args.push(format_double_17g(score).into_bytes());
                    args.push(member.clone());
                }
                emit(db, &args)?;
            }
        }
    }
    if let Some(expire) = entry.expire_ms {
        emit(
            db,
            &[
                b"PEXPIREAT".to_vec(),
                entry.key.clone(),
                expire.to_string().into_bytes(),
            ],
        )?;
    }
    Ok(())
}

fn write_stream<F>(emit: &mut F, db: usize, key: &[u8], stream: &Stream) -> std::io::Result<()>
where
    F: FnMut(usize, &[Vec<u8>]) -> std::io::Result<()>,
{
    if stream.is_empty() {
        // Redis creates empty streams with a trimmed-away XADD.
        emit(
            db,
            &[
                b"XADD".to_vec(),
                key.to_vec(),
                b"MAXLEN".to_vec(),
                b"0".to_vec(),
                b"0-1".to_vec(),
                b"x".to_vec(),
                b"y".to_vec(),
            ],
        )?;
    } else {
        for entry in stream.iter() {
            let mut args = vec![
                b"XADD".to_vec(),
                key.to_vec(),
                entry.id.to_string().into_bytes(),
            ];
            for (field, value) in &entry.fields {
                args.push(field.clone());
                args.push(value.clone());
            }
            emit(db, &args)?;
        }
    }
    emit(
        db,
        &[
            b"XSETID".to_vec(),
            key.to_vec(),
            stream.last_id.to_string().into_bytes(),
        ],
    )?;
    for (name, group) in &stream.groups {
        emit(
            db,
            &[
                b"XGROUP".to_vec(),
                b"CREATE".to_vec(),
                key.to_vec(),
                name.clone(),
                group.last_delivered_id.to_string().into_bytes(),
            ],
        )?;
        write_group(emit, db, key, name, group)?;
    }
    Ok(())
}

fn write_group<F>(
    emit: &mut F,
    db: usize,
    key: &[u8],
    name: &[u8],
    group: &StreamGroup,
) -> std::io::Result<()>
where
    F: FnMut(usize, &[Vec<u8>]) -> std::io::Result<()>,
{
    for (consumer_name, consumer) in &group.consumers {
        if consumer.pel.is_empty() {
            emit(
                db,
                &[
                    b"XGROUP".to_vec(),
                    b"CREATECONSUMER".to_vec(),
                    key.to_vec(),
                    name.to_vec(),
                    consumer_name.clone(),
                ],
            )?;
            continue;
        }
        for id in &consumer.pel {
            let Some(nack) = group.pel.get(id) else {
                continue;
            };
            emit(
                db,
                &[
                    b"XCLAIM".to_vec(),
                    key.to_vec(),
                    name.to_vec(),
                    consumer_name.clone(),
                    b"0".to_vec(),
                    id.to_string().into_bytes(),
                    b"TIME".to_vec(),
                    nack.delivery_time.to_string().into_bytes(),
                    b"RETRYCOUNT".to_vec(),
                    nack.delivery_count.to_string().into_bytes(),
                    b"JUSTID".to_vec(),
                    b"FORCE".to_vec(),
                ],
            )?;
        }
    }
    Ok(())
}

/// Load a command stream for tests and debugging.
pub fn decode_commands(bytes: &[u8]) -> Result<Vec<Command>, String> {
    let mut decoder = RespDecoder::default();
    decoder.feed(bytes);
    let mut commands = Vec::new();
    loop {
        match decoder.next_command() {
            Ok(Some(command)) => commands.push(command),
            Ok(None) => break,
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(commands)
}
