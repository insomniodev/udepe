//! Bidirectional UDP relay. Each client gets a session with its own upstream
//! socket and reply thread, so replies from upstream can be routed back to the
//! client that caused them.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use log::{info, warn};

use crate::udp::{self, canonical};

/// How long a session may go without traffic in either direction before it
/// is closed, unless the caller picks otherwise.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Keep socket timeouts nonzero, even near the idle deadline.
const MIN_READ_TIMEOUT: Duration = Duration::from_millis(1);

/// Relays datagrams to one upstream address and replies back to their senders.
/// Dropping it closes all sessions and joins their threads.
pub struct Proxy {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

/// State shared with the reply threads.
struct Shared {
    /// Socket clients send to. Replies go out through it as well, so clients
    /// see them coming from the address they sent to.
    downstream: UdpSocket,
    upstream: SocketAddr,
    idle_timeout: Duration,
    sessions: Mutex<HashMap<SocketAddr, Session>>,
    shutdown: AtomicBool,
}

struct Session {
    sock: Arc<UdpSocket>,
    last_active: Instant,
}

impl Proxy {
    /// Creates a proxy that replies through `downstream` and closes sessions
    /// after `idle_timeout` without traffic.
    #[must_use]
    pub fn new(downstream: UdpSocket, upstream: SocketAddr, idle_timeout: Duration) -> Self {
        let shared = Shared {
            downstream,
            upstream,
            idle_timeout,
            sessions: Mutex::default(),
            shutdown: AtomicBool::new(false),
        };
        Self {
            shared: Arc::new(shared),
            workers: Vec::new(),
        }
    }

    /// Relays `data` from `client` upstream, opening a session if needed.
    ///
    /// # Errors
    ///
    /// Fails if a new session cannot be set up or the send fails.
    pub fn forward(&mut self, client: SocketAddr, data: &[u8]) -> io::Result<()> {
        self.workers.retain(|w| !w.is_finished());
        let sock = {
            let mut sessions = self.shared.sessions();
            let session = match sessions.entry(client) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    let sock = Arc::new(udp::socket_for(self.shared.upstream)?);
                    let local = sock.local_addr()?;
                    let worker = Shared::spawn_relay(&self.shared, client, Arc::clone(&sock))?;
                    self.workers.push(worker);
                    info!("session {} opened via {local}", canonical(client));
                    e.insert(Session {
                        sock,
                        last_active: Instant::now(),
                    })
                }
            };
            session.last_active = Instant::now();
            Arc::clone(&session.sock)
        };
        sock.send_to(data, self.shared.upstream).map(drop)
    }

    /// Returns the number of open sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.shared.sessions().len()
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Relaxed);
        for session in self.shared.sessions().values() {
            if let Ok(mut local) = session.sock.local_addr() {
                local.set_ip(match local {
                    SocketAddr::V4(_) => Ipv4Addr::LOCALHOST.into(),
                    SocketAddr::V6(_) => Ipv6Addr::LOCALHOST.into(),
                });
                // A separate socket covers mismatched downstream/upstream families.
                if let Err(err) = self
                    .shared
                    .downstream
                    .send_to(&[], local)
                    .or_else(|_| udp::send(local, &[]))
                {
                    warn!("session {local}: wake failed: {err}");
                }
            }
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Shared {
    fn spawn_relay(
        self: &Arc<Self>,
        client: SocketAddr,
        sock: Arc<UdpSocket>,
    ) -> io::Result<JoinHandle<()>> {
        let shared = Arc::clone(self);
        thread::Builder::new()
            .name(format!("session {}", canonical(client)))
            .spawn(move || shared.relay_replies(client, &sock))
    }

    /// Locks the session table. Every critical section leaves it consistent,
    /// so a panic elsewhere does not invalidate it.
    fn sessions(&self) -> MutexGuard<'_, HashMap<SocketAddr, Session>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Relays datagrams from upstream back to `client` until the session goes
    /// idle, receiving fails or the proxy shuts down.
    fn relay_replies(&self, client: SocketAddr, sock: &UdpSocket) {
        let mut buf = vec![0; udp::MAX_DATAGRAM];
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let left = self.expire_if_idle(client);
            if left.is_zero() {
                break;
            }
            if let Err(err) = sock.set_read_timeout(Some(left.max(MIN_READ_TIMEOUT))) {
                warn!("session {}: timeout setup failed: {err}", canonical(client));
                break;
            }
            let received = sock.recv_from(&mut buf);
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            match received {
                Ok((n, from)) if from == self.upstream => {
                    self.touch(client);
                    if let Err(err) = self.downstream.send_to(&buf[..n], client) {
                        warn!("session {}: reply failed: {err}", canonical(client));
                    }
                }
                Ok(_) => {}
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) => {}
                Err(err) => {
                    warn!("session {}: receive failed: {err}", canonical(client));
                    break;
                }
            }
        }
        let mut sessions = self.sessions();
        if sessions
            .get(&client)
            .is_some_and(|session| std::ptr::eq(session.sock.as_ref(), sock))
        {
            sessions.remove(&client);
        }
        drop(sessions);
        info!("session {} closed", canonical(client));
    }

    fn touch(&self, client: SocketAddr) {
        if let Some(s) = self.sessions().get_mut(&client) {
            s.last_active = Instant::now();
        }
    }

    /// Returns how long the session may still go without traffic, removing it
    /// once that is zero. Checking and removing under one lock keeps `forward`
    /// from reusing a session that is about to close.
    fn expire_if_idle(&self, client: SocketAddr) -> Duration {
        let mut sessions = self.sessions();
        let left = sessions.get(&client).map_or(Duration::ZERO, |s| {
            self.idle_timeout.saturating_sub(s.last_active.elapsed())
        });
        if left.is_zero() {
            sessions.remove(&client);
        }
        left
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn localhost() -> io::Result<UdpSocket> {
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
    }

    #[test]
    fn drop_wakes_sessions_with_long_idle_timeouts() -> io::Result<()> {
        for upstream in [localhost()?, UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))?] {
            let mut proxy = Proxy::new(
                localhost()?,
                upstream.local_addr()?,
                Duration::from_secs(60),
            );
            let clients = [localhost()?, localhost()?];
            for client in &clients {
                proxy.forward(client.local_addr()?, b"hello")?;
            }
            assert_eq!(proxy.session_count(), clients.len());
            thread::sleep(Duration::from_millis(20));

            let start = Instant::now();
            drop(proxy);
            assert!(start.elapsed() < Duration::from_secs(1));
        }
        Ok(())
    }

    #[test]
    fn replies_reach_the_original_client() -> io::Result<()> {
        let (front, upstream) = (localhost()?, localhost()?);
        let front_addr = front.local_addr()?;
        let mut proxy = Proxy::new(front, upstream.local_addr()?, Duration::from_millis(50));

        let mut buf = [0; 16];
        for data in [b"one", b"two"] {
            let client = localhost()?;
            proxy.forward(client.local_addr()?, data)?;

            let (n, from) = upstream.recv_from(&mut buf)?;
            assert_eq!(&buf[..n], data);
            upstream.send_to(&buf[..n], from)?;

            let (n, from) = client.recv_from(&mut buf)?;
            assert_eq!(&buf[..n], data);
            assert_eq!(from, front_addr);
        }
        assert_eq!(proxy.session_count(), 2);

        thread::sleep(Duration::from_millis(200));
        assert_eq!(proxy.session_count(), 0);
        Ok(())
    }
}
