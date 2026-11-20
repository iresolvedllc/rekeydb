//! Replica-side replication client.
//!
//! Each replica runs a dedicated thread that performs the PSYNC handshake,
//! loads the full-sync payload, then applies the command stream under the
//! global state lock.

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rekeydb_protocol::{RespDecoder, encode_command};

use crate::state::State;

pub struct ReplicaHandle {
    pub shutdown: Arc<AtomicBool>,
    pub join: Option<std::thread::JoinHandle<()>>,
}

pub fn spawn(
    state: Arc<Mutex<State>>,
    host: String,
    port: u16,
    our_port: u16,
    timeout: Duration,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !shutdown.load(Ordering::SeqCst) {
            if let Err(err) = sync_once(&state, &host, port, our_port, timeout, &shutdown) {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                println!("{err}");
                {
                    let mut state = state.lock().expect("state poisoned");
                    state.set_master_link_down();
                }
                for _ in 0..10 {
                    if shutdown.load(Ordering::SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    })
}

fn send(stream: &mut TcpStream, args: &[&[u8]]) -> Result<(), String> {
    stream
        .write_all(&encode_command(args))
        .map_err(|err| err.to_string())
}

fn read_line(
    stream: &mut TcpStream,
    shutdown: &AtomicBool,
    deadline: std::time::Instant,
) -> Result<String, String> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            Ok(0) => return Err("master closed the connection".to_string()),
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                if byte[0] != b'\r' {
                    line.push(byte[0]);
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                if shutdown.load(Ordering::SeqCst) {
                    return Err("replication stopped".to_string());
                }
                if std::time::Instant::now() >= deadline {
                    return Err("Timeout connecting to the MASTER".to_string());
                }
                continue;
            }
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(String::from_utf8_lossy(&line).to_string())
}

fn read_exact(
    stream: &mut TcpStream,
    length: usize,
    shutdown: &AtomicBool,
) -> Result<Vec<u8>, String> {
    let mut buffer = vec![0u8; length];
    let mut filled = 0usize;
    while filled < length {
        match stream.read(&mut buffer[filled..]) {
            Ok(0) => return Err("master closed the connection during full sync".to_string()),
            Ok(count) => filled += count,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                if shutdown.load(Ordering::SeqCst) {
                    return Err("replication stopped".to_string());
                }
                continue;
            }
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(buffer)
}

fn send_ack(stream: &mut TcpStream, offset: i64) -> Result<(), String> {
    send(
        stream,
        &[b"REPLCONF", b"ACK", offset.to_string().as_bytes()],
    )
}

fn sync_once(
    state: &Arc<Mutex<State>>,
    host: &str,
    port: u16,
    our_port: u16,
    timeout: Duration,
    shutdown: &AtomicBool,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    let address = format!("{host}:{port}");
    let socket_address = address
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve {address}: {err}"))?
        .next()
        .ok_or_else(|| format!("cannot resolve {address}"))?;
    let mut stream = TcpStream::connect_timeout(&socket_address, timeout)
        .map_err(|err| format!("Timeout connecting to the MASTER: {err}"))?;
    stream.set_nodelay(true).map_err(|err| err.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| err.to_string())?;

    send(&mut stream, &[b"PING"])?;
    let _ = read_line(&mut stream, shutdown, deadline)?;

    send(
        &mut stream,
        &[
            b"REPLCONF",
            b"listening-port",
            our_port.to_string().as_bytes(),
        ],
    )?;
    let _ = read_line(&mut stream, shutdown, deadline)?;
    send(
        &mut stream,
        &[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"],
    )?;
    let _ = read_line(&mut stream, shutdown, deadline)?;

    send(&mut stream, &[b"PSYNC", b"?", b"-1"])?;
    let response = read_line(&mut stream, shutdown, deadline)?;
    if let Some(error) = response.strip_prefix("-") {
        return Err(error.to_string());
    }
    let (replid, mut offset) = if let Some(rest) = response.strip_prefix("+FULLRESYNC ") {
        let mut parts = rest.split_whitespace();
        let replid = parts.next().unwrap_or_default().to_string();
        let offset = parts
            .next()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        (replid, offset)
    } else if response.starts_with("+CONTINUE") {
        let state = state.lock().expect("state poisoned");
        (
            state.replication_id().to_string(),
            state.replication_offset(),
        )
    } else {
        return Err(format!("unexpected PSYNC response: {response}"));
    };

    let length_line = read_line(&mut stream, shutdown, deadline)?;
    let Some(length_text) = length_line.strip_prefix('$') else {
        return Err(format!("expected bulk payload, got: {length_line}"));
    };
    let length = length_text
        .parse::<usize>()
        .map_err(|_| format!("invalid payload length: {length_text}"))?;
    let payload = read_exact(&mut stream, length, shutdown)?;
    if std::env::var_os("REKEYDB_TRACE_REPL").is_some() {
        eprintln!("repl: full sync payload bytes={length} offset={offset}");
    }

    {
        let mut state = state.lock().expect("state poisoned");
        state.apply_full_sync(&payload, replid, offset)?;
    }
    offset = {
        let state = state.lock().expect("state poisoned");
        state.replication_offset()
    };
    send_ack(&mut stream, offset)?;

    let mut decoder = RespDecoder::default();
    let mut client = crate::state::ClientContext::new(0, format!("{host}:{port}"));
    let mut buffer = [0u8; 64 * 1024];
    let mut last_ack = std::time::Instant::now();
    while !shutdown.load(Ordering::SeqCst) {
        match stream.read(&mut buffer) {
            Ok(0) => return Err("master closed the connection".to_string()),
            Ok(count) => {
                decoder.feed(&buffer[..count]);
                loop {
                    let before = decoder.position();
                    let command = match decoder.next_command() {
                        Ok(Some(command)) => command,
                        Ok(None) => break,
                        Err(err) => return Err(format!("bad replication stream: {err}")),
                    };
                    let consumed = (decoder.position() - before) as i64;
                    if command.args.is_empty() {
                        offset += consumed;
                        continue;
                    }
                    let name = String::from_utf8_lossy(&command.args[0]).to_ascii_uppercase();
                    if name == "REPLCONF"
                        && command
                            .args
                            .get(1)
                            .is_some_and(|value| value.eq_ignore_ascii_case(b"GETACK"))
                    {
                        offset += consumed;
                        send_ack(&mut stream, offset)?;
                        let mut state = state.lock().expect("state poisoned");
                        state.set_master_offset(offset);
                        continue;
                    }
                    {
                        let mut state = state.lock().expect("state poisoned");
                        let outcome = state.execute_from_master(&command, &mut client);
                        if let rekeydb_protocol::RespValue::Error(error) = outcome.reply {
                            return Err(format!(
                                "replication command failed: {}",
                                String::from_utf8_lossy(&error)
                            ));
                        }
                    }
                    offset += consumed;
                    {
                        let mut state = state.lock().expect("state poisoned");
                        state.set_master_offset(offset);
                    }
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                if last_ack.elapsed() >= Duration::from_millis(500) {
                    let current = {
                        let state = state.lock().expect("state poisoned");
                        state.replication_offset()
                    };
                    send_ack(&mut stream, current)?;
                    last_ack = std::time::Instant::now();
                }
            }
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(())
}
