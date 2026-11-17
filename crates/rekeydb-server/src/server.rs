//! Minimal P0 event loop.
//!
//! Each connection is a non-blocking socket registered with `mio`. Commands
//! are decoded outside the global state lock, then executed together while the
//! lock is held, mirroring KeyDB's "batch commands, then release for I/O"
//! model. Worker-thread sharding will be added on top of this loop.

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use rekeydb_protocol::{ProtocolError, RespDecoder, RespValue, RespVersion};

use crate::config::Config;
use crate::state::{BlockKind, BlockRequest, ClientContext, State};

const LISTENER: Token = Token(0);
const MAX_OUTPUT_BUFFER: usize = 64 * 1024 * 1024;
const MAX_QUERY_BUFFER: usize = 64 * 1024 * 1024;

struct BlockedClient {
    request: BlockRequest,
    deadline: Option<Instant>,
}

struct Client {
    stream: TcpStream,
    decoder: RespDecoder,
    out: Vec<u8>,
    ctx: ClientContext,
    close_after_flush: bool,
    shutdown_after_flush: bool,
    blocked: Option<BlockedClient>,
}

impl Client {
    fn new(stream: TcpStream, ctx: ClientContext) -> Self {
        Self {
            stream,
            decoder: RespDecoder::default(),
            out: Vec::new(),
            ctx,
            close_after_flush: false,
            shutdown_after_flush: false,
            blocked: None,
        }
    }
}

pub fn run(config: Config) -> Result<(), String> {
    println!("rekeydb PID: {}", std::process::id());
    let address = resolve_address(&config)?;
    let mut listener = match TcpListener::bind(address) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("Failed listening on port {}: {err}", config.port);
            return Err(format!("cannot bind {address}: {err}"));
        }
    };
    let local = listener
        .local_addr()
        .map_err(|err| format!("cannot read listener address: {err}"))?;

    let poll = Poll::new().map_err(|err| format!("cannot create poller: {err}"))?;
    let registry = poll
        .registry()
        .try_clone()
        .map_err(|err| format!("cannot clone poll registry: {err}"))?;
    registry
        .register(&mut listener, LISTENER, Interest::READABLE)
        .map_err(|err| format!("cannot register listener: {err}"))?;

    let mut state = State::new(config.clone());
    let rdb_path = config.dir.join(&config.dbfilename);
    let aof_path = config.dir.join(&config.appendfilename);
    if config.appendonly {
        if aof_path.is_file() {
            state
                .load_aof_from_disk()
                .map_err(|err| format!("cannot load {}: {err}", aof_path.display()))?;
        } else {
            if rdb_path.is_file() {
                state
                    .reload_rdb_from_disk()
                    .map_err(|err| format!("cannot load {}: {err}", rdb_path.display()))?;
            }
            state
                .enable_aof()
                .map_err(|err| format!("cannot enable AOF: {err}"))?;
        }
    } else if rdb_path.is_file() {
        state
            .reload_rdb_from_disk()
            .map_err(|err| format!("cannot load {}: {err}", rdb_path.display()))?;
    }
    let mut server = Server {
        poll,
        registry,
        listener,
        clients: HashMap::new(),
        blocked: Vec::new(),
        state: Mutex::new(state),
        next_token: 1,
        next_client_id: 1,
        running: true,
        trace_commands: std::env::var_os("REKEYDB_TRACE_COMMANDS").is_some(),
        trace_names: std::env::var_os("REKEYDB_TRACE_NAMES").is_some(),
    };
    println!(
        "rekeydb {} listening on {local} (server-threads={})",
        env!("CARGO_PKG_VERSION"),
        config.server_threads
    );
    println!("Thread 0 alive");
    server.run()
}

struct Server {
    poll: Poll,
    registry: mio::Registry,
    listener: TcpListener,
    clients: HashMap<Token, Client>,
    blocked: Vec<Token>,
    state: Mutex<State>,
    next_token: usize,
    next_client_id: u64,
    running: bool,
    trace_commands: bool,
    trace_names: bool,
}

impl Server {
    fn run(&mut self) -> Result<(), String> {
        let mut events = Events::with_capacity(1024);
        while self.running {
            self.poll
                .poll(&mut events, Some(Duration::from_millis(25)))
                .map_err(|err| format!("poll failed: {err}"))?;
            self.expire_blocked();
            for event in events.iter() {
                let token = event.token();
                if token == LISTENER {
                    self.accept();
                    continue;
                }
                if !self.clients.contains_key(&token) {
                    continue;
                }
                if event.is_readable() {
                    self.read_client(token);
                }
                if self.clients.contains_key(&token)
                    && (event.is_writable() || event.is_read_closed() || event.is_error())
                {
                    if event.is_error() || event.is_read_closed() {
                        if let Some(mut client) = self.clients.remove(&token) {
                            let _ = self.registry.deregister(&mut client.stream);
                            self.unblock(token);
                            self.client_gone(client.ctx.id);
                        }
                    } else {
                        self.flush_client(token);
                    }
                }
            }
        }
        Ok(())
    }

    fn accept(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, address)) => {
                    let token = Token(self.next_token);
                    self.next_token += 1;
                    if let Err(err) = self
                        .registry
                        .register(&mut stream, token, Interest::READABLE)
                    {
                        eprintln!("cannot register client {address}: {err}");
                        continue;
                    }
                    let id = self.next_client_id;
                    self.next_client_id += 1;
                    let ctx = ClientContext::new(id, address.to_string());
                    self.clients.insert(token, Client::new(stream, ctx));
                    let mut state = self.state.lock().expect("state poisoned");
                    state.connected_clients += 1;
                    state.register_client(id);
                    state.stats_mut().total_connections_received += 1;
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(err) => {
                    eprintln!("accept failed: {err}");
                    break;
                }
            }
        }
    }

    fn read_client(&mut self, token: Token) {
        let Some(mut client) = self.clients.remove(&token) else {
            return;
        };
        let mut buffer = [0u8; 16 * 1024];
        let mut close = false;
        let mut input_bytes = 0usize;
        let mut reads = 0usize;
        // Do not let one fast producer monopolize the event loop: drain a
        // bounded amount, process it, then return to polling other clients.
        while reads < 16 {
            reads += 1;
            match client.stream.read(&mut buffer) {
                Ok(0) => {
                    close = true;
                    break;
                }
                Ok(n) => {
                    input_bytes += n;
                    client.decoder.feed(&buffer[..n]);
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => {
                    eprintln!("client {} read failed: {err}", client.ctx.id);
                    close = true;
                    break;
                }
            }
        }

        if client.decoder.buffered() > MAX_QUERY_BUFFER {
            client
                .out
                .extend_from_slice(b"-ERR Protocol error: query buffer exceeds maximum\r\n");
            close = true;
        }

        if client.blocked.is_some() {
            self.state
                .lock()
                .expect("state poisoned")
                .stats_mut()
                .total_net_input_bytes += input_bytes as u64;
            if close {
                self.unblock(token);
                client.close_after_flush = true;
            }
            self.flush_client_inner(token, client);
            return;
        }

        self.process_client(token, client, input_bytes);
    }

    fn process_client(&mut self, token: Token, mut client: Client, input_bytes: usize) {
        let mut close = client.close_after_flush;
        let mut wakes: Vec<(usize, Vec<u8>)> = Vec::new();

        loop {
            let command = match client.decoder.next_command() {
                Ok(Some(command)) => command,
                Ok(None) => break,
                Err(err) => {
                    append_protocol_error(&mut client.out, &err);
                    close = true;
                    break;
                }
            };
            if command.args.is_empty() {
                continue;
            }
            if self.trace_commands {
                eprintln!("cmd[{}]: {:?}", client.ctx.id, command.args);
            } else if self.trace_names {
                eprintln!(
                    "cmd[{}]: {}",
                    client.ctx.id,
                    String::from_utf8_lossy(&command.args[0])
                );
            }
            let mut state = self.state.lock().expect("state poisoned");
            let outcome = state.execute(&command, &mut client.ctx);
            drop(state);
            let mut reply = outcome.reply;
            if let Some((id, as_error)) = outcome.unblock
                && !self.unblock_by_id(id, as_error)
            {
                reply = RespValue::Integer(0);
            }
            if let Some(block) = outcome.block {
                let deadline = block
                    .timeout_ms
                    .map(|milliseconds| Instant::now() + Duration::from_millis(milliseconds));
                client.blocked = Some(BlockedClient {
                    request: *block,
                    deadline,
                });
                self.add_blocked(token);
                break;
            }
            reply.encode(client.ctx.version, &mut client.out);
            if outcome.close {
                close = true;
            }
            if outcome.shutdown {
                client.shutdown_after_flush = true;
                close = true;
            }
            if close {
                break;
            }
            wakes.extend(outcome.wake);
        }

        self.state
            .lock()
            .expect("state poisoned")
            .stats_mut()
            .total_net_input_bytes += input_bytes as u64;

        if client.out.len() > MAX_OUTPUT_BUFFER {
            close = true;
        }
        if close {
            client.close_after_flush = true;
        }
        self.flush_client_inner(token, client);

        for (db, key) in wakes {
            self.wake_blocked(db, &key);
        }
    }

    fn wake_blocked(&mut self, db: usize, key: &[u8]) {
        loop {
            let position = self.blocked.iter().position(|token| {
                self.clients
                    .get(token)
                    .and_then(|client| client.blocked.as_ref())
                    .is_some_and(|blocked| {
                        blocked.request.db == db
                            && blocked
                                .request
                                .keys
                                .iter()
                                .any(|candidate| candidate == key)
                    })
            });
            let Some(position) = position else {
                return;
            };
            let token = self.remove_blocked_at(position);
            let Some(mut client) = self.clients.remove(&token) else {
                continue;
            };
            let Some(blocked) = client.blocked.take() else {
                self.clients.insert(token, client);
                continue;
            };
            let moved_destination = if blocked.request.kind == BlockKind::Move {
                blocked.request.destination.clone()
            } else {
                None
            };
            let reply = {
                let mut state = self.state.lock().expect("state poisoned");
                state.serve_blocked(&blocked.request)
            };
            let Some(reply) = reply else {
                client.blocked = Some(blocked);
                self.restore_blocked(position, token);
                self.clients.insert(token, client);
                return;
            };
            reply.encode(client.ctx.version, &mut client.out);
            self.process_client(token, client, 0);
            if let Some(destination) = moved_destination {
                self.wake_blocked(db, &destination);
            }
        }
    }

    fn expire_blocked(&mut self) {
        let now = Instant::now();
        let expired: Vec<Token> = self
            .blocked
            .iter()
            .copied()
            .filter(|token| {
                self.clients
                    .get(token)
                    .and_then(|client| client.blocked.as_ref())
                    .and_then(|blocked| blocked.deadline)
                    .is_some_and(|deadline| deadline <= now)
            })
            .collect();
        for token in expired {
            self.unblock(token);
            let Some(mut client) = self.clients.remove(&token) else {
                continue;
            };
            let Some(blocked) = client.blocked.take() else {
                self.clients.insert(token, client);
                continue;
            };
            let reply = match blocked.request.kind {
                BlockKind::Pop | BlockKind::ZPop { .. } => RespValue::NullArray,
                BlockKind::Move => RespValue::null(),
            };
            reply.encode(client.ctx.version, &mut client.out);
            self.process_client(token, client, 0);
        }
    }

    fn add_blocked(&mut self, token: Token) {
        if !self.blocked.contains(&token) {
            self.blocked.push(token);
            self.state.lock().expect("state poisoned").blocked_clients += 1;
        }
    }

    fn remove_blocked_at(&mut self, position: usize) -> Token {
        let token = self.blocked.remove(position);
        let mut state = self.state.lock().expect("state poisoned");
        state.blocked_clients = state.blocked_clients.saturating_sub(1);
        token
    }

    fn restore_blocked(&mut self, position: usize, token: Token) {
        self.blocked.insert(position, token);
        self.state.lock().expect("state poisoned").blocked_clients += 1;
    }

    fn unblock_by_id(&mut self, id: u64, as_error: bool) -> bool {
        let Some(token) = self
            .clients
            .iter()
            .find(|(_, client)| client.ctx.id == id && client.blocked.is_some())
            .map(|(token, _)| *token)
        else {
            return false;
        };
        self.unblock(token);
        let Some(mut client) = self.clients.remove(&token) else {
            return false;
        };
        let Some(blocked) = client.blocked.take() else {
            self.clients.insert(token, client);
            return false;
        };
        let reply = if as_error {
            RespValue::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
        } else {
            match blocked.request.kind {
                BlockKind::Pop | BlockKind::ZPop { .. } => RespValue::NullArray,
                BlockKind::Move => RespValue::null(),
            }
        };
        reply.encode(client.ctx.version, &mut client.out);
        self.process_client(token, client, 0);
        true
    }

    fn unblock(&mut self, token: Token) {
        if let Some(position) = self
            .blocked
            .iter()
            .position(|candidate| *candidate == token)
        {
            self.remove_blocked_at(position);
        }
    }

    fn flush_client(&mut self, token: Token) {
        let Some(client) = self.clients.remove(&token) else {
            return;
        };
        self.flush_client_inner(token, client);
    }

    fn flush_client_inner(&mut self, token: Token, mut client: Client) {
        while !client.out.is_empty() {
            match client.stream.write(&client.out) {
                Ok(0) => {
                    client.close_after_flush = true;
                    break;
                }
                Ok(n) => {
                    client.out.drain(..n);
                    self.state
                        .lock()
                        .expect("state poisoned")
                        .stats_mut()
                        .total_net_output_bytes += n as u64;
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => {
                    eprintln!("client {} write failed: {err}", client.ctx.id);
                    client.close_after_flush = true;
                    break;
                }
            }
        }

        if client.close_after_flush && client.out.is_empty() {
            let _ = self.registry.deregister(&mut client.stream);
            self.unblock(token);
            self.client_gone(client.ctx.id);
            if client.shutdown_after_flush {
                self.running = false;
            }
            return;
        }

        let interest = if client.out.is_empty() {
            Interest::READABLE
        } else {
            Interest::READABLE | Interest::WRITABLE
        };
        if let Err(err) = self
            .registry
            .reregister(&mut client.stream, token, interest)
        {
            eprintln!("cannot reregister client {}: {err}", client.ctx.id);
            let _ = self.registry.deregister(&mut client.stream);
            self.client_gone(client.ctx.id);
            return;
        }
        self.clients.insert(token, client);
    }

    fn client_gone(&mut self, id: u64) {
        let mut state = self.state.lock().expect("state poisoned");
        state.unregister_client(id);
        state.connected_clients = state.connected_clients.saturating_sub(1);
    }
}

fn append_protocol_error(out: &mut Vec<u8>, error: &ProtocolError) {
    out.push(b'-');
    out.extend_from_slice(b"ERR Protocol error: ");
    out.extend_from_slice(error.message.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn resolve_address(config: &Config) -> Result<SocketAddr, String> {
    let bind = format!("{}:{}", config.bind, config.port);
    let mut addresses = bind
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve {bind}: {err}"))?;
    addresses
        .next()
        .ok_or_else(|| format!("no addresses resolved for {bind}"))
}

/// Encode a reply using a specific protocol version. Exposed for integration
/// tests and future tooling.
pub fn encode_reply(reply: &RespValue, version: RespVersion) -> Vec<u8> {
    reply.encoded(version)
}
