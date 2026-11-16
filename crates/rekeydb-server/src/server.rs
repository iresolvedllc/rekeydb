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

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use rekeydb_protocol::{ProtocolError, RespDecoder, RespValue, RespVersion};

use crate::config::Config;
use crate::state::{ClientContext, State};

const LISTENER: Token = Token(0);
const MAX_OUTPUT_BUFFER: usize = 64 * 1024 * 1024;
const MAX_QUERY_BUFFER: usize = 64 * 1024 * 1024;

struct Client {
    stream: TcpStream,
    decoder: RespDecoder,
    out: Vec<u8>,
    ctx: ClientContext,
    close_after_flush: bool,
    shutdown_after_flush: bool,
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
        }
    }
}

pub fn run(config: Config) -> Result<(), String> {
    let address = resolve_address(&config)?;
    let mut listener =
        TcpListener::bind(address).map_err(|err| format!("cannot bind {address}: {err}"))?;
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

    let mut server = Server {
        poll,
        registry,
        listener,
        clients: HashMap::new(),
        state: Mutex::new(State::new(config.clone())),
        next_token: 1,
        next_client_id: 1,
        running: true,
    };
    eprintln!(
        "rekeydb {} listening on {local} (server-threads={})",
        env!("CARGO_PKG_VERSION"),
        config.server_threads
    );
    server.run()
}

struct Server {
    poll: Poll,
    registry: mio::Registry,
    listener: TcpListener,
    clients: HashMap<Token, Client>,
    state: Mutex<State>,
    next_token: usize,
    next_client_id: u64,
    running: bool,
}

impl Server {
    fn run(&mut self) -> Result<(), String> {
        let mut events = Events::with_capacity(1024);
        while self.running {
            self.poll
                .poll(&mut events, None)
                .map_err(|err| format!("poll failed: {err}"))?;
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
                            self.client_gone();
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
        loop {
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

        if !close {
            let mut commands = Vec::new();
            loop {
                match client.decoder.next_command() {
                    Ok(Some(command)) => {
                        if !command.args.is_empty() {
                            commands.push(command);
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        append_protocol_error(&mut client.out, &err);
                        close = true;
                        break;
                    }
                }
            }

            if !commands.is_empty() {
                let mut state = self.state.lock().expect("state poisoned");
                state.stats_mut().total_net_input_bytes += input_bytes as u64;
                for command in commands {
                    let outcome = state.execute(&command, &mut client.ctx);
                    outcome.reply.encode(client.ctx.version, &mut client.out);
                    if outcome.close {
                        close = true;
                    }
                    if outcome.shutdown {
                        client.shutdown_after_flush = true;
                        close = true;
                    }
                }
                drop(state);
            } else {
                self.state
                    .lock()
                    .expect("state poisoned")
                    .stats_mut()
                    .total_net_input_bytes += input_bytes as u64;
            }
        }

        if client.out.len() > MAX_OUTPUT_BUFFER {
            close = true;
        }
        if close {
            client.close_after_flush = true;
        }
        self.flush_client_inner(token, client);
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
            self.client_gone();
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
            self.client_gone();
            return;
        }
        self.clients.insert(token, client);
    }

    fn client_gone(&mut self) {
        let mut state = self.state.lock().expect("state poisoned");
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
