//! Developer tasks.
//!
//! `cargo run -p xtask -- spec` extracts the command and configuration
//! surface from the pinned KeyDB source and writes the manifests under
//! `spec/`. The port uses those manifests for coverage tracking.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct CommandSpec {
    name: String,
    handler: String,
    arity: i32,
    flags: Vec<String>,
    categories: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConfigSpec {
    name: String,
    kind: String,
    alias: Option<String>,
    flags: String,
    default: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TestFile {
    path: String,
    lines: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct TestSpec {
    files: Vec<TestFile>,
    total_files: usize,
    total_lines: usize,
}

#[derive(Debug, Serialize)]
struct LockEntry {
    path: String,
    bytes: u64,
    fnv64: String,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let task = args.next().unwrap_or_else(|| "spec".to_string());
    let root = workspace_root();
    let result = match task.as_str() {
        "spec" => extract_spec(&root),
        "coverage" => write_coverage(&root),
        "check" => check_spec(&root),
        "help" | "--help" | "-h" => {
            println!("usage: cargo run -p xtask -- [spec|coverage|check]");
            return;
        }
        other => Err(format!("unknown task: {other}")),
    };
    if let Err(err) = result {
        eprintln!("xtask: {err}");
        std::process::exit(1);
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the workspace")
        .to_path_buf()
}

fn extract_spec(root: &Path) -> Result<(), String> {
    let keydb = locate_keydb(root)?;
    let spec_dir = root.join("spec");
    fs::create_dir_all(&spec_dir).map_err(|err| err.to_string())?;

    let commands = extract_commands(&keydb.join("src/server.cpp"))?;
    let configs = extract_configs(&keydb.join("src/config.cpp"))?;
    let tests = extract_tests(&keydb.join("tests"))?;

    write_json(&spec_dir.join("commands.json"), &commands)?;
    write_json(&spec_dir.join("configs.json"), &configs)?;
    write_json(&spec_dir.join("tests.json"), &tests)?;
    write_lock(root, &keydb, &spec_dir.join("keydb.lock"))?;
    write_coverage(root)?;

    println!("keydb source: {}", keydb.display());
    println!("commands: {}", commands.len());
    println!("configs:  {}", configs.len());
    println!(
        "tests:    {} files, {} lines",
        tests.total_files, tests.total_lines
    );
    Ok(())
}

fn locate_keydb(root: &Path) -> Result<PathBuf, String> {
    for candidate in [root.join("KeyDB"), root.join("reference/keydb")] {
        if candidate.join("src/server.cpp").is_file() {
            return Ok(candidate);
        }
    }
    Err("cannot find KeyDB source (expected KeyDB/ or reference/keydb/)".to_string())
}

fn extract_commands(path: &Path) -> Result<Vec<CommandSpec>, String> {
    let source =
        fs::read_to_string(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    let marker = "struct redisCommand redisCommandTable[] = {";
    let start = source
        .find(marker)
        .ok_or_else(|| "command table marker not found".to_string())?
        + marker.len();
    let rest = &source[start..];
    let end = rest
        .find("\n};")
        .ok_or_else(|| "command table terminator not found".to_string())?;
    let body = &rest[..end];

    let pattern = Regex::new(
        r#"(?s)\{\s*"([^"]+)"\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*(-?\d+)\s*,\s*"([^"]*)""#,
    )
    .expect("valid regex");

    let mut commands = Vec::new();
    for captures in pattern.captures_iter(body) {
        let flags: Vec<String> = captures[4]
            .split_whitespace()
            .map(ToString::to_string)
            .collect();
        let categories = flags
            .iter()
            .filter(|flag| flag.starts_with('@'))
            .cloned()
            .collect();
        commands.push(CommandSpec {
            name: captures[1].to_string(),
            handler: captures[2].to_string(),
            arity: captures[3].parse().unwrap_or(0),
            flags,
            categories,
        });
    }
    if commands.is_empty() {
        return Err("no commands extracted".to_string());
    }
    Ok(commands)
}

fn extract_configs(path: &Path) -> Result<Vec<ConfigSpec>, String> {
    let source =
        fs::read_to_string(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    let pattern = Regex::new(
        r#"(?ms)^\s*create(\w+)Config\(\s*"([^"]+)"\s*,\s*([^,]*),\s*([^,]*),\s*([^,]*),\s*([^,]*),"#,
    )
    .expect("valid regex");
    let mut configs = Vec::new();
    for captures in pattern.captures_iter(&source) {
        let alias = captures[3].trim();
        configs.push(ConfigSpec {
            name: captures[2].to_string(),
            kind: captures[1].to_string(),
            alias: if alias == "NULL" || alias.is_empty() {
                None
            } else {
                Some(alias.trim_matches('"').to_string())
            },
            flags: captures[4].trim().to_string(),
            default: captures[6].trim().to_string(),
        });
    }
    if configs.is_empty() {
        return Err("no configs extracted".to_string());
    }
    Ok(configs)
}

fn extract_tests(path: &Path) -> Result<TestSpec, String> {
    let mut files = Vec::new();
    visit_tcl(path, path, &mut files)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let total_lines = files.iter().map(|file| file.lines).sum();
    Ok(TestSpec {
        total_files: files.len(),
        total_lines,
        files,
    })
}

fn visit_tcl(root: &Path, path: &Path, out: &mut Vec<TestFile>) -> Result<(), String> {
    let entries =
        fs::read_dir(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| err.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            visit_tcl(root, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "tcl") {
            let text = fs::read_to_string(&path).map_err(|err| err.to_string())?;
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            out.push(TestFile {
                path: relative,
                lines: text.lines().count(),
            });
        }
    }
    Ok(())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let json = serde_json::to_string_pretty(value).map_err(|err| err.to_string())?;
    fs::write(path, json + "\n").map_err(|err| format!("cannot write {}: {err}", path.display()))
}

fn write_lock(root: &Path, keydb: &Path, path: &Path) -> Result<(), String> {
    let mut entries = Vec::new();
    for relative in ["src", "tests", "Makefile", "keydb.conf", "COPYING"] {
        let target = keydb.join(relative);
        if target.is_file() {
            entries.push(lock_entry(keydb, &target)?);
        } else if target.is_dir() {
            visit_files(keydb, &target, &mut entries)?;
        }
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    write_json(path, &entries)?;
    let _ = root;
    Ok(())
}

fn visit_files(root: &Path, path: &Path, out: &mut Vec<LockEntry>) -> Result<(), String> {
    for entry in fs::read_dir(path).map_err(|err| err.to_string())? {
        let entry = entry.map_err(|err| err.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tmp") {
                continue;
            }
            visit_files(root, &path, out)?;
        } else if path.is_file() && should_lock(&path) {
            out.push(lock_entry(root, &path)?);
        }
    }
    Ok(())
}

/// Keep the lock stable across local oracle builds. Object files, generated
/// release headers, and module binaries are build products, not specification
/// inputs.
fn should_lock(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    if matches!(name, "Makefile" | "COPYING" | "keydb.conf" | "README.md") {
        return true;
    }
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    matches!(
        extension,
        "c" | "cpp"
            | "h"
            | "hpp"
            | "asm"
            | "sh"
            | "tcl"
            | "conf"
            | "txt"
            | "md"
            | "py"
            | "rdb"
            | "aof"
            | "json"
    )
}

fn lock_entry(root: &Path, path: &Path) -> Result<LockEntry, String> {
    let bytes = fs::read(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    let mut hasher = Fnv64::default();
    bytes.hash(&mut hasher);
    Ok(LockEntry {
        path: path
            .strip_prefix(root)
            .unwrap_or(path)
            .display()
            .to_string(),
        bytes: bytes.len() as u64,
        fnv64: format!("{:016x}", hasher.finish()),
    })
}

fn check_spec(root: &Path) -> Result<(), String> {
    let spec_dir = root.join("spec");
    let commands: Vec<CommandSpec> = read_json(&spec_dir.join("commands.json"))?;
    let configs: Vec<ConfigSpec> = read_json(&spec_dir.join("configs.json"))?;
    let tests: TestSpec = read_json(&spec_dir.join("tests.json"))?;
    if commands.len() != 236 {
        return Err(format!("expected 236 commands, found {}", commands.len()));
    }
    if configs.is_empty() || tests.total_files == 0 {
        return Err("spec manifests are incomplete".to_string());
    }
    println!(
        "spec ok: {} commands, {} configs, {} tests / {} lines",
        commands.len(),
        configs.len(),
        tests.total_files,
        tests.total_lines
    );
    Ok(())
}

fn write_coverage(root: &Path) -> Result<(), String> {
    let spec_dir = root.join("spec");
    let commands: Vec<CommandSpec> = read_json(&spec_dir.join("commands.json"))?;
    let implemented_path = spec_dir.join("implemented.txt");
    let implemented: BTreeSet<String> = if implemented_path.is_file() {
        fs::read_to_string(&implemented_path)
            .map_err(|err| err.to_string())?
            .lines()
            .map(|line| line.trim().to_ascii_lowercase())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect()
    } else {
        BTreeSet::new()
    };

    let mut categories: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut missing = Vec::new();
    let mut done_total = 0usize;
    for command in &commands {
        let category = command
            .categories
            .first()
            .cloned()
            .unwrap_or_else(|| "uncategorized".to_string());
        let entry = categories.entry(category).or_insert((0, 0));
        entry.0 += 1;
        if implemented.contains(&command.name.to_ascii_lowercase()) {
            entry.1 += 1;
            done_total += 1;
        } else {
            missing.push(command.name.clone());
        }
    }

    let mut out = String::new();
    out.push_str("# rekeydb command coverage\n\n");
    out.push_str(&format!(
        "Implemented: {} / {}\n\n",
        done_total,
        commands.len()
    ));
    out.push_str("| Category | Implemented | Total |\n|---|---:|---:|\n");
    for (category, (total, done)) in &categories {
        out.push_str(&format!("| {category} | {done} | {total} |\n"));
    }
    out.push_str("\n## Missing\n\n");
    for name in missing {
        out.push_str(&format!("- `{name}`\n"));
    }
    fs::write(spec_dir.join("coverage.md"), out).map_err(|err| err.to_string())?;
    Ok(())
}

fn read_json<T: for<'de> serde::Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let text =
        fs::read_to_string(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    serde_json::from_str(&text).map_err(|err| format!("cannot parse {}: {err}", path.display()))
}

#[derive(Default)]
struct Fnv64(u64);

impl Hasher for Fnv64 {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf29ce484222325
        } else {
            self.0
        };
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        self.0 = hash;
    }
}
