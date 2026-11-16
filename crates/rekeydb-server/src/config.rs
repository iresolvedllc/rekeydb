//! Server startup configuration.
//!
//! This is deliberately a small subset of `keydb.conf` for P0. The spec
//! extractor records the full KeyDB configuration surface so the remaining
//! options can be implemented without drifting.

use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub databases: usize,
    pub server_threads: usize,
    pub requirepass: Option<String>,
    pub dir: PathBuf,
    pub logfile: Option<String>,
    pub config_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 6379,
            databases: 16,
            server_threads: 1,
            requirepass: None,
            dir: PathBuf::from("."),
            logfile: None,
            config_file: None,
        }
    }
}

#[derive(Debug)]
pub enum Startup {
    Run(Config),
    Version,
    Help,
}

pub const HELP: &str = "\
rekeydb - a Rust and Zig implementation of KeyDB

Usage: rekeydb [OPTIONS]

Options:
  --config <path>       Read a KeyDB-style configuration file
  --port <port>         TCP port (default: 6379)
  --bind <address>      Bind address (default: 127.0.0.1)
  --databases <count>   Number of logical databases (default: 16)
  --requirepass <pass>  Set the default user password
  --version             Print version and exit
  --help                Print this help and exit
";

impl Config {
    pub fn from_args<I, S>(args: I) -> Result<Startup, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args: Vec<String> = args.into_iter().map(Into::into).collect();
        let mut config = Config::default();
        let mut config_path: Option<PathBuf> = None;
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--help" | "-h" => return Ok(Startup::Help),
                "--version" | "-v" => return Ok(Startup::Version),
                "--config" => {
                    let path = args
                        .get(i + 1)
                        .ok_or_else(|| "--config requires a path".to_string())?;
                    config_path = Some(PathBuf::from(path));
                    i += 2;
                }
                "--port" => {
                    let value = args
                        .get(i + 1)
                        .ok_or_else(|| "--port requires a value".to_string())?;
                    config.port = parse_port(value)?;
                    i += 2;
                }
                "--bind" => {
                    config.bind = args
                        .get(i + 1)
                        .ok_or_else(|| "--bind requires a value".to_string())?
                        .clone();
                    i += 2;
                }
                "--databases" => {
                    let value = args
                        .get(i + 1)
                        .ok_or_else(|| "--databases requires a value".to_string())?;
                    config.databases = parse_count(value, "databases")?;
                    i += 2;
                }
                "--requirepass" => {
                    config.requirepass = args.get(i + 1).cloned();
                    i += 2;
                }
                other if other.starts_with("--") => {
                    return Err(format!("unknown option: {other}"));
                }
                other => {
                    // KeyDB accepts `--port 6380` style options only. A bare
                    // argument is an error rather than silently ignored.
                    return Err(format!("unexpected argument: {other}"));
                }
            }
        }

        if let Some(path) = config_path {
            let loaded = load_file(&path)?;
            config.apply(loaded);
            config.config_file = Some(path);
        }
        config.validate()?;
        Ok(Startup::Run(config))
    }

    fn apply(&mut self, other: Config) {
        // Only apply values explicitly present in the file. `other` starts
        // from defaults, so this is a straightforward merge for P0.
        if other.bind != Config::default().bind {
            self.bind = other.bind;
        }
        if other.port != Config::default().port {
            self.port = other.port;
        }
        if other.databases != Config::default().databases {
            self.databases = other.databases;
        }
        if other.server_threads != Config::default().server_threads {
            self.server_threads = other.server_threads;
        }
        if other.requirepass.is_some() {
            self.requirepass = other.requirepass;
        }
        if other.dir != Config::default().dir {
            self.dir = other.dir;
        }
        if other.logfile.is_some() {
            self.logfile = other.logfile;
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.databases == 0 {
            return Err("databases must be greater than zero".to_string());
        }
        if self.server_threads == 0 {
            return Err("server-threads must be greater than zero".to_string());
        }
        Ok(())
    }
}

fn load_file(path: &Path) -> Result<Config, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("cannot read config {}: {err}", path.display()))?;
    parse_str(&text)
}

/// Parse a KeyDB-style configuration string. Unknown directives are ignored
/// for now, because the reference config contains options that are not yet
/// implemented; the spec extractor tracks them.
pub fn parse_str(text: &str) -> Result<Config, String> {
    let mut config = Config::default();
    for (line_no, raw) in text.lines().enumerate() {
        let line = strip_comment(raw).trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = parts.next().unwrap_or_default().trim();
        let value = parts.next().unwrap_or_default().trim();
        let value = unquote(value);
        match key.to_ascii_lowercase().as_str() {
            "bind" => {
                if let Some(first) = value.split_whitespace().find(|part| !part.starts_with('-')) {
                    config.bind = first.to_string();
                }
            }
            "port" => {
                config.port =
                    parse_port(value).map_err(|err| format!("line {}: {err}", line_no + 1))?;
            }
            "databases" => {
                config.databases = parse_count(value, "databases")
                    .map_err(|err| format!("line {}: {err}", line_no + 1))?;
            }
            "server-threads" | "io-threads" => {
                config.server_threads = parse_count(value, "server-threads")
                    .map_err(|err| format!("line {}: {err}", line_no + 1))?;
            }
            "requirepass" => {
                config.requirepass = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                };
            }
            "dir" => config.dir = PathBuf::from(value),
            "logfile" => {
                config.logfile = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                };
            }
            _ => {}
        }
    }
    config.validate()?;
    Ok(config)
}

fn strip_comment(line: &str) -> &str {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (index, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
            continue;
        }
        if ch == '#' {
            return &line[..index];
        }
    }
    line
}

fn unquote(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn parse_port(value: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|_| format!("invalid port: {value}"))
}

fn parse_count(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid {name}: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config_subset_and_ignores_unknown() {
        let config = parse_str(
            r#"
# comment
port 6380
bind 127.0.0.1 -::1
server-threads 4
databases 32
requirepass "hunter 2"
dir /tmp/rekeydb
appendonly yes
save 900 1
"#,
        )
        .unwrap();
        assert_eq!(config.port, 6380);
        assert_eq!(config.bind, "127.0.0.1");
        assert_eq!(config.server_threads, 4);
        assert_eq!(config.databases, 32);
        assert_eq!(config.requirepass.as_deref(), Some("hunter 2"));
        assert_eq!(config.dir, PathBuf::from("/tmp/rekeydb"));
    }

    #[test]
    fn value_comment_is_stripped_but_quoted_hash_is_kept() {
        let config = parse_str("requirepass \"a#b\" # trailing\n").unwrap();
        assert_eq!(config.requirepass.as_deref(), Some("a#b"));
    }

    #[test]
    fn io_threads_is_an_alias() {
        let config = parse_str("io-threads 8\n").unwrap();
        assert_eq!(config.server_threads, 8);
    }

    #[test]
    fn rejects_zero_databases() {
        let err = parse_str("databases 0\n").unwrap_err();
        assert!(err.contains("databases"));
    }
}
