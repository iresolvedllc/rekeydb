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
    pub dbfilename: String,
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
            dbfilename: "dump.rdb".to_string(),
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

        // First pass: locate the config file and handle informational flags.
        // KeyDB config files are loaded before command-line overrides.
        let mut config_path: Option<PathBuf> = None;
        let mut config_arg: Option<usize> = None;
        let mut index = 1;
        while index < args.len() {
            match args[index].as_str() {
                "--help" | "-h" => return Ok(Startup::Help),
                "--version" | "-v" => return Ok(Startup::Version),
                "--config" => {
                    let path = args
                        .get(index + 1)
                        .ok_or_else(|| "--config requires a path".to_string())?;
                    if config_path.is_some() {
                        return Err("--config specified more than once".to_string());
                    }
                    config_path = Some(PathBuf::from(path));
                    config_arg = Some(index);
                    index += 2;
                }
                "--port" | "--bind" | "--databases" | "--requirepass" => {
                    if args.get(index + 1).is_none() {
                        return Err(format!("{} requires a value", args[index]));
                    }
                    index += 2;
                }
                other if other.starts_with("--") => {
                    return Err(format!("unknown option: {other}"));
                }
                other => {
                    if config_path.is_some() {
                        return Err(format!("unexpected argument: {other}"));
                    }
                    config_path = Some(PathBuf::from(other));
                    config_arg = Some(index);
                    index += 1;
                }
            }
        }

        let mut config = if let Some(path) = &config_path {
            load_file(path)?
        } else {
            Config::default()
        };
        config.config_file = config_path;

        // Second pass: command-line values override the file.
        let mut index = 1;
        while index < args.len() {
            match args[index].as_str() {
                "--config" => index += 2,
                "--port" => {
                    config.port = parse_port(
                        args.get(index + 1)
                            .ok_or_else(|| "--port requires a value".to_string())?,
                    )?;
                    index += 2;
                }
                "--bind" => {
                    config.bind = args
                        .get(index + 1)
                        .ok_or_else(|| "--bind requires a value".to_string())?
                        .clone();
                    index += 2;
                }
                "--databases" => {
                    config.databases = parse_count(
                        args.get(index + 1)
                            .ok_or_else(|| "--databases requires a value".to_string())?,
                        "databases",
                    )?;
                    index += 2;
                }
                "--requirepass" => {
                    config.requirepass = Some(
                        args.get(index + 1)
                            .ok_or_else(|| "--requirepass requires a value".to_string())?
                            .clone(),
                    );
                    index += 2;
                }
                _ if config_arg == Some(index) => index += 1,
                _ => index += 1,
            }
        }
        config.validate()?;
        Ok(Startup::Run(config))
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
            "dbfilename" => config.dbfilename = value.to_string(),
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

    #[test]
    fn cli_overrides_config_file_and_positional_path() {
        let path = std::env::temp_dir().join(format!(
            "rekeydb-config-test-{}-{}.conf",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "port 6380\nserver-threads 3\n").unwrap();

        let config = match Config::from_args([
            "rekeydb".to_string(),
            "--port".to_string(),
            "6381".to_string(),
            path.display().to_string(),
        ])
        .unwrap()
        {
            Startup::Run(config) => config,
            other => panic!("unexpected startup: {other:?}"),
        };
        assert_eq!(config.port, 6381);
        assert_eq!(config.server_threads, 3);
        assert_eq!(config.config_file.as_deref(), Some(path.as_path()));

        let config = match Config::from_args([
            "rekeydb".to_string(),
            "--config".to_string(),
            path.display().to_string(),
            "--port".to_string(),
            "6382".to_string(),
        ])
        .unwrap()
        {
            Startup::Run(config) => config,
            other => panic!("unexpected startup: {other:?}"),
        };
        assert_eq!(config.port, 6382);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn missing_option_value_is_an_error() {
        let err = Config::from_args(["rekeydb", "--port"]).unwrap_err();
        assert!(err.contains("--port"));
    }
}
