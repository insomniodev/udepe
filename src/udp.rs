//! UDP socket primitives shared by the commands and the proxy. Dual-stack
//! policy lives here: [`bind`] on an IPv6 address also accepts IPv4 traffic,
//! and [`canonical`] turns the resulting IPv4-mapped addresses back into IPv4.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};

use socket2::{Domain, Protocol, Socket, Type};

/// Largest possible UDP payload; receive buffers use this size.
pub const MAX_DATAGRAM: usize = u16::MAX as usize;

/// Resolves a host name or IP literal to its first address.
///
/// # Errors
///
/// Fails if resolution fails or yields no addresses.
pub fn resolve(host: &str, port: u16) -> io::Result<SocketAddr> {
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no addresses found"))
}

/// Binds a datagram socket on `addr`. An IPv6 address also accepts IPv4
/// traffic; senders then show up as IPv4-mapped addresses, see [`canonical`].
///
/// # Errors
///
/// Fails if the socket cannot be created or bound.
pub fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
    let sock = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    if addr.is_ipv6() {
        sock.set_only_v6(false)?;
    }
    sock.bind(&addr.into())?;
    Ok(sock.into())
}

/// Opens a socket on an ephemeral local port that can send to `dest`.
///
/// # Errors
///
/// Fails if the socket cannot be created or bound.
pub fn socket_for(dest: SocketAddr) -> io::Result<UdpSocket> {
    let local: SocketAddr = match dest {
        SocketAddr::V4(_) => (Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => (Ipv6Addr::UNSPECIFIED, 0).into(),
    };
    UdpSocket::bind(local)
}

/// Sends `data` to `dest` as a single datagram from an ephemeral local port,
/// returning the number of bytes sent.
///
/// # Errors
///
/// Fails if no socket can be opened or the send fails.
pub fn send(dest: SocketAddr, data: &[u8]) -> io::Result<usize> {
    socket_for(dest)?.send_to(data, dest)
}

/// Returns IPv4-mapped IPv6 addresses (as seen by dual-stack sockets) as plain
/// IPv4, for display.
#[must_use]
pub fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_formats_addresses() {
        let ipv4 = Ipv4Addr::new(10, 0, 0, 1);
        let mapped = SocketAddr::from((ipv4.to_ipv6_mapped(), 80));
        assert_eq!(canonical(mapped).to_string(), "10.0.0.1:80");
        let ipv6 = SocketAddr::from((Ipv6Addr::LOCALHOST, 9));
        assert_eq!(canonical(ipv6).to_string(), "[::1]:9");
        let plain = SocketAddr::from((ipv4, 80));
        assert_eq!(canonical(plain), plain);
        assert_eq!(canonical(plain).to_string(), "10.0.0.1:80");
    }

    #[test]
    fn send_reaches_receiver() -> io::Result<()> {
        let recv = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        send(recv.local_addr()?, b"hello")?;

        let mut buf = [0; 16];
        let (n, _) = recv.recv_from(&mut buf)?;
        assert_eq!(&buf[..n], b"hello");
        Ok(())
    }

    #[test]
    fn dual_stack_bind_receives_ipv4_as_mapped() -> io::Result<()> {
        let recv = bind((Ipv6Addr::UNSPECIFIED, 0).into())?;
        let dest: SocketAddr = (Ipv4Addr::LOCALHOST, recv.local_addr()?.port()).into();
        let sender = socket_for(dest)?;
        sender.send_to(b"hello", dest)?;

        let mut buf = [0; 16];
        let (_, from) = recv.recv_from(&mut buf)?;
        assert!(from.is_ipv6());
        let from = canonical(from);
        assert!(from.is_ipv4());
        assert_eq!(from.port(), sender.local_addr()?.port());
        Ok(())
    }
}
