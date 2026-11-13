//! The listen command: an event loop that receives datagrams in batches,
//! writes each to an output as one line and optionally relays it through a
//! `Proxy`. One `Listener` is one thread's worth of state and shares nothing.

use std::convert::Infallible;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use log::info;
use mio::{Events, Interest, Poll, Registry, Token};

use crate::batch::{RecvBatch, Socket};
#[cfg(feature = "forward")]
use crate::proxy::Proxy;
use crate::udp;

/// Token of the listening socket; the proxy's sessions use the ones above.
const FRONT: Token = Token(0);

/// Readiness events handled per turn of the loop.
const EVENT_CAPACITY: usize = 1024;

/// A bound socket that prints, and optionally relays, what it receives.
pub struct Listener {
    poll: Poll,
    events: Events,
    state: State,
}

/// Everything the loop acts on, kept apart from the event queue so events
/// can be handled while it is being read.
struct State {
    sock: Socket,
    batch: RecvBatch,
    #[cfg(feature = "forward")]
    proxy: Option<Proxy>,
}

impl Listener {
    /// Binds on `addr`. An IPv6 address also accepts IPv4 traffic.
    ///
    /// # Errors
    ///
    /// Fails if the socket cannot be bound or the event loop cannot be set up.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let mut sock = Socket::new(udp::bind(addr)?)?;
        let poll = Poll::new()?;
        poll.registry()
            .register(sock.source(), FRONT, Interest::READABLE)?;
        info!("listening on {}", sock.local_addr()?);
        Ok(Self {
            poll,
            events: Events::with_capacity(EVENT_CAPACITY),
            state: State {
                sock,
                batch: RecvBatch::new(),
                #[cfg(feature = "forward")]
                proxy: None,
            },
        })
    }

    /// Returns the bound address, useful when binding port 0.
    ///
    /// # Errors
    ///
    /// Fails if the OS cannot report the address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.state.sock.local_addr()
    }

    /// Also relays each datagram to `upstream`, routing replies back to the
    /// sender through this listener's socket. Sessions close after
    /// `idle_timeout` without traffic.
    #[cfg(feature = "forward")]
    #[must_use]
    pub fn forward_to(mut self, upstream: SocketAddr, idle_timeout: Duration) -> Self {
        self.state.proxy = Some(Proxy::new(upstream, idle_timeout, Token(FRONT.0 + 1)));
        info!("forwarding to {upstream}");
        self
    }

    /// Receives forever, writing each datagram to `out`. A failed relay is
    /// logged, not fatal, so this only returns on a receive or write error.
    ///
    /// # Errors
    ///
    /// Fails if polling, receiving or writing fails.
    pub fn run(&mut self, out: &mut impl Write) -> io::Result<Infallible> {
        loop {
            self.turn(out, None)?;
        }
    }

    /// Runs the loop once: waits for events for at most `timeout`, handles
    /// them and flushes `out`. It may return early without having done
    /// anything, so call it in a loop.
    ///
    /// # Errors
    ///
    /// Fails if polling, receiving or writing fails.
    pub fn turn(&mut self, out: &mut impl Write, timeout: Option<Duration>) -> io::Result<()> {
        #[cfg(feature = "forward")]
        let timeout = {
            let sweep = self.state.proxy.as_ref();
            let sweep = sweep.and_then(|proxy| proxy.timeout(Instant::now()));
            match (timeout, sweep) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            }
        };
        match self.poll.poll(&mut self.events, timeout) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::Interrupted => return Ok(()),
            Err(err) => return Err(err),
        }
        // One clock read per turn, not per datagram.
        let now = Instant::now();
        let (state, registry) = (&mut self.state, self.poll.registry());
        for event in &self.events {
            if event.token() == FRONT {
                state.receive(registry, out, now)?;
            } else {
                #[cfg(feature = "forward")]
                if let Some(proxy) = &mut state.proxy {
                    proxy.relay_replies(event.token(), &state.sock, &mut state.batch, now);
                }
            }
        }
        #[cfg(feature = "forward")]
        if let Some(proxy) = &mut state.proxy {
            proxy.expire(registry, now);
        }
        out.flush()
    }
}

impl State {
    /// Drains the listening socket: readiness is only reported once per
    /// burst, so everything queued has to be read before polling again.
    #[cfg_attr(not(feature = "forward"), expect(unused_variables))]
    fn receive(
        &mut self,
        registry: &Registry,
        out: &mut impl Write,
        now: Instant,
    ) -> io::Result<()> {
        while self.sock.recv(&mut self.batch)? {
            for received in self.batch.iter() {
                for datagram in received.datagrams() {
                    write_line(out, received.from, datagram)?;
                }
                #[cfg(feature = "forward")]
                if let Some(proxy) = &mut self.proxy
                    && let Err(err) =
                        proxy.forward(registry, received.from, received.data, received.stride, now)
                {
                    log::warn!(
                        "forwarding from {} failed: {err}",
                        udp::canonical(received.from)
                    );
                }
            }
        }
        Ok(())
    }
}

/// Writes one datagram as `<from>: <data>\n`. IPv4-mapped senders are shown
/// as IPv4 and trailing line breaks are dropped.
fn write_line(out: &mut impl Write, from: SocketAddr, data: &[u8]) -> io::Result<()> {
    let end = data
        .iter()
        .rposition(|b| !matches!(b, b'\r' | b'\n'))
        .map_or(0, |i| i + 1);
    write!(out, "{}: ", udp::canonical(from))?;
    out.write_all(&data[..end])?;
    out.write_all(b"\n")
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "forward")]
    use std::net::UdpSocket;
    use std::net::{Ipv4Addr, SocketAddrV6};

    use super::*;

    #[cfg(feature = "forward")]
    const DEADLINE: Duration = Duration::from_secs(5);

    #[cfg(feature = "forward")]
    fn localhost() -> io::Result<UdpSocket> {
        let sock = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        sock.set_nonblocking(true)?;
        Ok(sock)
    }

    /// Turns the loop until `sock` has a datagram, then returns it.
    #[cfg(feature = "forward")]
    fn recv_via(
        listener: &mut Listener,
        out: &mut Vec<u8>,
        sock: &UdpSocket,
    ) -> io::Result<(Vec<u8>, SocketAddr)> {
        let start = Instant::now();
        let mut buf = [0; 16];
        loop {
            match sock.recv_from(&mut buf) {
                Ok((n, from)) => return Ok((buf[..n].to_vec(), from)),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Err(err),
            }
            assert!(start.elapsed() < DEADLINE, "no datagram arrived");
            listener.turn(out, Some(Duration::from_millis(10)))?;
        }
    }

    #[test]
    fn writes_one_line_per_datagram() -> io::Result<()> {
        let v4: SocketAddr = (Ipv4Addr::LOCALHOST, 1234).into();
        let mapped = SocketAddrV6::new(Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 1234, 0, 0).into();

        let mut out = Vec::new();
        write_line(&mut out, v4, b"hello\r\n")?;
        write_line(&mut out, mapped, b"hello")?;
        assert_eq!(out, b"127.0.0.1:1234: hello\n127.0.0.1:1234: hello\n");
        Ok(())
    }

    #[cfg(feature = "forward")]
    #[test]
    fn replies_reach_the_original_client() -> io::Result<()> {
        let upstream = localhost()?;
        let mut listener = Listener::bind((Ipv4Addr::LOCALHOST, 0).into())?
            .forward_to(upstream.local_addr()?, Duration::from_millis(50));
        let front = listener.local_addr()?;
        let mut out = Vec::new();

        let clients = [localhost()?, localhost()?];
        let mut sessions = Vec::new();
        for (client, data) in clients.iter().zip([b"one", b"two"]) {
            client.send_to(data, front)?;
            let (got, session) = recv_via(&mut listener, &mut out, &upstream)?;
            assert_eq!(got, data);
            sessions.push(session);
        }
        assert_ne!(sessions[0], sessions[1], "each client gets its own socket");
        assert_eq!(
            listener.state.proxy.as_ref().map(Proxy::session_count),
            Some(2)
        );

        // Answer in reverse order: routing must follow the session, not arrival.
        for (client, session) in clients.iter().zip(&sessions).rev() {
            upstream.send_to(b"reply", session)?;
            let (got, from) = recv_via(&mut listener, &mut out, client)?;
            assert_eq!(got, b"reply");
            assert_eq!(from, front);
        }
        for client in &clients {
            assert!(client.recv_from(&mut [0; 16]).is_err(), "stray reply");
        }
        assert_eq!(String::from_utf8_lossy(&out).lines().count(), 2);

        let start = Instant::now();
        while listener.state.proxy.as_ref().map(Proxy::session_count) != Some(0) {
            assert!(start.elapsed() < DEADLINE, "sessions never expired");
            listener.turn(&mut out, Some(Duration::from_millis(10)))?;
        }
        Ok(())
    }

    #[cfg(feature = "forward")]
    #[test]
    fn expired_sessions_are_reopened_on_new_traffic() -> io::Result<()> {
        let upstream = localhost()?;
        let mut listener = Listener::bind((Ipv4Addr::LOCALHOST, 0).into())?
            .forward_to(upstream.local_addr()?, Duration::from_millis(20));
        let front = listener.local_addr()?;
        let mut out = Vec::new();
        let client = localhost()?;

        client.send_to(b"one", front)?;
        let (_, first) = recv_via(&mut listener, &mut out, &upstream)?;
        let start = Instant::now();
        while listener.state.proxy.as_ref().map(Proxy::session_count) != Some(0) {
            assert!(start.elapsed() < DEADLINE, "session never expired");
            listener.turn(&mut out, Some(Duration::from_millis(10)))?;
        }

        // A reply to the closed session must go nowhere.
        upstream.send_to(b"late", first)?;
        client.send_to(b"two", front)?;
        let (got, second) = recv_via(&mut listener, &mut out, &upstream)?;
        assert_eq!(got, b"two");
        upstream.send_to(b"reply", second)?;
        let (got, _) = recv_via(&mut listener, &mut out, &client)?;
        assert_eq!(got, b"reply");
        Ok(())
    }
}
