//! The listen command: receives datagrams, writes each to an output as one
//! line and optionally relays it through a `Proxy`.

use std::convert::Infallible;
use std::io::{self, Write};
use std::net::{SocketAddr, UdpSocket};

use log::info;

#[cfg(feature = "forward")]
use crate::proxy::{self, Proxy};
use crate::udp;

/// A bound socket that prints, and optionally relays, what it receives.
pub struct Listener {
    sock: UdpSocket,
    #[cfg(feature = "forward")]
    proxy: Option<Proxy>,
}

impl Listener {
    /// Binds on `addr`. An IPv6 address also accepts IPv4 traffic.
    ///
    /// # Errors
    ///
    /// Fails if the socket cannot be bound.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let sock = udp::bind(addr)?;
        info!("listening on {}", sock.local_addr()?);
        Ok(Self {
            sock,
            #[cfg(feature = "forward")]
            proxy: None,
        })
    }

    /// Returns the bound address, useful when binding port 0.
    ///
    /// # Errors
    ///
    /// Fails if the OS cannot report the address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Also relays each datagram to `upstream`, routing replies back to the
    /// sender through this listener's socket.
    ///
    /// # Errors
    ///
    /// Fails if the socket cannot be shared with the proxy.
    #[cfg(feature = "forward")]
    pub fn forward_to(mut self, upstream: SocketAddr) -> io::Result<Self> {
        let downstream = self.sock.try_clone()?;
        self.proxy = Some(Proxy::new(
            downstream,
            upstream,
            proxy::DEFAULT_IDLE_TIMEOUT,
        ));
        info!("forwarding to {upstream}");
        Ok(self)
    }

    /// Receives forever, writing each datagram to `out`. A failed relay is
    /// logged, not fatal, so this only returns on a receive or write error.
    ///
    /// # Errors
    ///
    /// Fails if receiving or writing fails.
    pub fn run(&mut self, out: &mut impl Write) -> io::Result<Infallible> {
        let mut buf = vec![0; udp::MAX_DATAGRAM];
        loop {
            let (n, from) = match self.sock.recv_from(&mut buf) {
                Ok(received) => received,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            };
            let data = &buf[..n];
            write_line(out, from, data)?;

            #[cfg(feature = "forward")]
            if let Some(proxy) = &mut self.proxy
                && let Err(err) = proxy.forward(from, data)
            {
                log::warn!("forwarding from {} failed: {err}", udp::canonical(from));
            }
        }
    }
}

/// Writes one datagram as `<from>: <data>\n` and flushes. IPv4-mapped senders
/// are shown as IPv4 and trailing line breaks are dropped.
fn write_line(out: &mut impl Write, from: SocketAddr, data: &[u8]) -> io::Result<()> {
    let end = data
        .iter()
        .rposition(|b| !matches!(b, b'\r' | b'\n'))
        .map_or(0, |i| i + 1);
    write!(out, "{}: ", udp::canonical(from))?;
    out.write_all(&data[..end])?;
    out.write_all(b"\n")?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV6};

    use super::*;

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
}
