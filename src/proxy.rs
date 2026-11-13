//! Bidirectional UDP relay. Each client gets a session with its own upstream
//! socket, so replies from upstream can be routed back to the client that
//! caused them.
//!
//! A `Proxy` is single-threaded state driven by its owner's event loop: it
//! registers the session sockets with that loop's [`Registry`] and is told
//! when one becomes readable. Nothing here blocks or locks.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use mio::{Interest, Registry, Token};

use crate::batch::{RecvBatch, Socket};
use crate::udp::{self, canonical};

/// How long a session may go without traffic in either direction before it
/// is closed, unless the caller picks otherwise.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Idle sessions are swept this many times per idle timeout, so a session
/// outlives its timeout by at most that fraction.
const SWEEPS_PER_TIMEOUT: u32 = 4;

/// Keeps the sweep from spinning when the idle timeout is tiny.
const MIN_SWEEP_INTERVAL: Duration = Duration::from_millis(1);

/// Relays datagrams to one upstream address and replies back to their senders.
pub struct Proxy {
    upstream: SocketAddr,
    idle_timeout: Duration,
    /// Session slot for each client.
    clients: HashMap<SocketAddr, usize>,
    /// Sessions by slot, which is also what their [`Token`] is derived from.
    sessions: Vec<Option<Session>>,
    /// Empty slots in `sessions`, reused before it grows.
    free: Vec<usize>,
    first_token: usize,
    next_sweep: Instant,
}

struct Session {
    client: SocketAddr,
    sock: Socket,
    last_active: Instant,
}

impl Proxy {
    /// Creates a proxy for `upstream` that closes sessions after
    /// `idle_timeout` without traffic. Its sessions use tokens from
    /// `first_token` upwards; the owner keeps the ones below.
    #[must_use]
    pub fn new(upstream: SocketAddr, idle_timeout: Duration, first_token: Token) -> Self {
        Self {
            upstream,
            idle_timeout,
            clients: HashMap::new(),
            sessions: Vec::new(),
            free: Vec::new(),
            first_token: first_token.0,
            next_sweep: Instant::now(),
        }
    }

    /// Relays `data` from `client` upstream as datagrams of `stride` bytes,
    /// opening a session if needed.
    ///
    /// # Errors
    ///
    /// Fails if a new session cannot be set up or the send fails.
    pub fn forward(
        &mut self,
        registry: &Registry,
        client: SocketAddr,
        data: &[u8],
        stride: usize,
        now: Instant,
    ) -> io::Result<()> {
        let slot = match self.clients.get(&client) {
            Some(&slot) => slot,
            None => self.open(registry, client, now)?,
        };
        let Some(session) = &mut self.sessions[slot] else {
            return Err(io::Error::other("session table out of sync"));
        };
        session.last_active = now;
        session.sock.send(self.upstream, data, stride)
    }

    /// Relays everything queued on the session behind `token` back to its
    /// client through `front`, so the client sees replies coming from the
    /// address it sent to. Tokens of closed sessions are ignored.
    pub fn relay_replies(
        &mut self,
        token: Token,
        front: &Socket,
        batch: &mut RecvBatch,
        now: Instant,
    ) {
        let Some(session) = token
            .0
            .checked_sub(self.first_token)
            .and_then(|slot| self.sessions.get_mut(slot))
            .and_then(Option::as_mut)
        else {
            return;
        };
        let client = session.client;
        loop {
            match session.sock.recv(batch) {
                Ok(true) => {}
                Ok(false) => break,
                // An ICMP error for an earlier send; the queue may hold more.
                Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => {
                    warn!("session {}: upstream unreachable", canonical(client));
                    continue;
                }
                Err(err) => {
                    warn!("session {}: receive failed: {err}", canonical(client));
                    break;
                }
            }
            for reply in batch.iter() {
                session.last_active = now;
                match front.send(client, reply.data, reply.stride) {
                    Ok(()) => {}
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        debug!("session {}: reply dropped", canonical(client));
                    }
                    Err(err) => warn!("session {}: reply failed: {err}", canonical(client)),
                }
            }
        }
    }

    /// Closes the sessions that have been idle for too long. Cheap to call
    /// on every turn of the loop: it only scans once per sweep interval.
    pub fn expire(&mut self, registry: &Registry, now: Instant) {
        if self.clients.is_empty() || now < self.next_sweep {
            return;
        }
        self.next_sweep = now + self.sweep_interval();
        for slot in 0..self.sessions.len() {
            let idle = self.sessions[slot]
                .as_ref()
                .is_some_and(|s| now.duration_since(s.last_active) >= self.idle_timeout);
            if idle && let Some(mut session) = self.sessions[slot].take() {
                self.clients.remove(&session.client);
                self.free.push(slot);
                // Closing the socket deregisters it too; this only makes it explicit.
                let _ = registry.deregister(session.sock.source());
                info!("session {} closed", canonical(session.client));
            }
        }
    }

    /// Returns how long the owner may wait for events before it must call
    /// [`Proxy::expire`] again, or `None` while there are no sessions.
    #[must_use]
    pub fn timeout(&self, now: Instant) -> Option<Duration> {
        (!self.clients.is_empty()).then(|| self.next_sweep.saturating_duration_since(now))
    }

    /// Returns the number of open sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.clients.len()
    }

    fn sweep_interval(&self) -> Duration {
        (self.idle_timeout / SWEEPS_PER_TIMEOUT).max(MIN_SWEEP_INTERVAL)
    }

    /// Opens a session for `client` and returns its slot.
    fn open(&mut self, registry: &Registry, client: SocketAddr, now: Instant) -> io::Result<usize> {
        let mut sock = Socket::new(udp::socket_for(self.upstream)?)?;
        // Connecting lets the kernel drop datagrams from anyone but upstream.
        sock.connect(self.upstream)?;
        let local = sock.local_addr()?;

        let slot = self.free.last().copied().unwrap_or(self.sessions.len());
        let token = Token(self.first_token + slot);
        registry.register(sock.source(), token, Interest::READABLE)?;

        let session = Some(Session {
            client,
            sock,
            last_active: now,
        });
        if self.free.pop().is_some() {
            self.sessions[slot] = session;
        } else {
            self.sessions.push(session);
        }
        if self.clients.is_empty() {
            self.next_sweep = now + self.sweep_interval();
        }
        self.clients.insert(client, slot);
        info!("session {} opened via {local}", canonical(client));
        Ok(slot)
    }
}
