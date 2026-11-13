//! Batched, non-blocking datagram I/O: the seam between the event loop and
//! the syscall layer. The interface is shaped as batches in and batches out,
//! so the backend (`recvmmsg` and GSO/GRO through `quinn-udp` today) can be
//! replaced without touching the loop.

use std::io::{self, IoSliceMut};
use std::net::SocketAddr;

use mio::net::UdpSocket;
use quinn_udp::{BATCH_SIZE, RecvMeta, Transmit, UdpSocketState};

use crate::udp::MAX_DATAGRAM;

/// A non-blocking UDP socket that receives and sends in batches.
pub struct Socket {
    sock: UdpSocket,
    state: UdpSocketState,
}

impl Socket {
    /// Takes over `sock`, making it non-blocking and enabling receive
    /// coalescing where the platform supports it.
    ///
    /// Datagrams sent through it are never fragmented: one that exceeds the
    /// path MTU fails to send instead.
    ///
    /// # Errors
    ///
    /// Fails if the socket options cannot be set.
    pub fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        let sock = UdpSocket::from_std(sock);
        let state = UdpSocketState::new((&sock).into())?;
        Ok(Self { sock, state })
    }

    /// Returns the bound address.
    ///
    /// # Errors
    ///
    /// Fails if the OS cannot report the address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Restricts the socket to exchanging datagrams with `peer`.
    ///
    /// # Errors
    ///
    /// Fails if the OS rejects the association.
    pub fn connect(&self, peer: SocketAddr) -> io::Result<()> {
        self.sock.connect(peer)
    }

    /// The event source to register with a [`mio::Registry`].
    pub fn source(&mut self) -> &mut UdpSocket {
        &mut self.sock
    }

    /// Fills `batch` with whatever is queued, replacing its contents.
    /// Returns `false` once the socket has nothing left to read, which is
    /// when the caller should go back to polling.
    ///
    /// # Errors
    ///
    /// Fails if receiving fails for any reason other than an empty queue.
    pub fn recv(&self, batch: &mut RecvBatch) -> io::Result<bool> {
        batch.len = 0;
        let mut chunks = batch.buf.as_chunks_mut::<MAX_DATAGRAM>().0.iter_mut();
        let mut bufs: [IoSliceMut<'_>; BATCH_SIZE] = std::array::from_fn(|_| {
            IoSliceMut::new(
                chunks
                    .next()
                    .map(<[u8; MAX_DATAGRAM]>::as_mut_slice)
                    .unwrap_or_default(),
            )
        });
        let received = self.sock.try_io(|| {
            self.state
                .recv((&self.sock).into(), &mut bufs, &mut batch.meta)
        });
        match received {
            Ok(n) => {
                batch.len = n;
                Ok(true)
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Sends `data` to `dest` as datagrams of `stride` bytes each, the last
    /// one possibly shorter. Several datagrams go out in one system call
    /// where segmentation offload allows it.
    ///
    /// # Errors
    ///
    /// Fails if the send fails; [`io::ErrorKind::WouldBlock`] means the send
    /// buffer is full and the datagrams were dropped.
    pub fn send(&self, dest: SocketAddr, data: &[u8], stride: usize) -> io::Result<()> {
        let segments = data.len().div_ceil(stride.max(1));
        if segments <= self.state.max_gso_segments() {
            return self.transmit(dest, data, (segments > 1).then_some(stride));
        }
        segments_of(data, stride).try_for_each(|datagram| self.transmit(dest, datagram, None))
    }

    fn transmit(&self, dest: SocketAddr, data: &[u8], segment: Option<usize>) -> io::Result<()> {
        let transmit = Transmit {
            destination: dest,
            ecn: None,
            contents: data,
            segment_size: segment,
            src_ip: None,
        };
        self.sock
            .try_io(|| self.state.try_send((&self.sock).into(), &transmit))
    }
}

/// Reusable receive buffers for one [`Socket::recv`] call. Allocated once
/// per event loop, so the receive path does not allocate.
pub struct RecvBatch {
    buf: Vec<u8>,
    meta: [RecvMeta; BATCH_SIZE],
    len: usize,
}

impl RecvBatch {
    /// Allocates buffers for a full batch of maximum-size datagrams.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: vec![0; BATCH_SIZE * MAX_DATAGRAM],
            meta: [RecvMeta::default(); BATCH_SIZE],
            len: 0,
        }
    }

    /// Iterates over what the last [`Socket::recv`] call received.
    pub fn iter(&self) -> impl Iterator<Item = Received<'_>> {
        self.buf
            .as_chunks::<MAX_DATAGRAM>()
            .0
            .iter()
            .zip(&self.meta[..self.len])
            .map(|(buf, meta)| Received {
                from: meta.addr,
                data: &buf[..meta.len],
                stride: meta.stride,
            })
    }
}

impl Default for RecvBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// One or more datagrams received from the same sender. The kernel may
/// coalesce a burst into a single buffer; `stride` marks the boundaries.
#[derive(Debug, Clone, Copy)]
pub struct Received<'a> {
    /// Who sent the datagrams.
    pub from: SocketAddr,
    /// The datagrams, back to back.
    pub data: &'a [u8],
    /// Size of each datagram in `data`; the last one may be shorter.
    pub stride: usize,
}

impl<'a> Received<'a> {
    /// Iterates over the individual datagrams.
    pub fn datagrams(&self) -> impl Iterator<Item = &'a [u8]> + use<'a> {
        segments_of(self.data, self.stride)
    }
}

/// Splits `data` into datagrams of `stride` bytes. Empty data is one empty
/// datagram, which is valid UDP.
fn segments_of(data: &[u8], stride: usize) -> impl Iterator<Item = &[u8]> {
    std::iter::once(data)
        .filter(|data| data.is_empty())
        .chain(data.chunks(stride.max(1)))
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use mio::{Events, Interest, Poll, Token};

    use super::*;

    #[test]
    fn splits_coalesced_data_at_the_stride() {
        let split = |data: &'static [u8], stride| segments_of(data, stride).collect::<Vec<_>>();
        assert_eq!(split(b"", 0), [b""]);
        assert_eq!(split(b"abc", 3), [b"abc"]);
        assert_eq!(split(b"abcdefg", 3), [&b"abc"[..], b"def", b"g"]);
    }

    #[test]
    fn round_trips_a_burst() -> io::Result<()> {
        let bind = || std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).and_then(Socket::new);
        let (sender, mut receiver) = (bind()?, bind()?);
        let dest = receiver.local_addr()?;

        let mut poll = Poll::new()?;
        poll.registry()
            .register(receiver.source(), Token(0), Interest::READABLE)?;

        // Four datagrams in one call: three full segments and a short one.
        sender.send(dest, b"aaabbbcccd", 3)?;
        sender.send(dest, b"", 0)?;

        let mut events = Events::with_capacity(4);
        let mut batch = RecvBatch::new();
        let mut got = Vec::new();
        while got.len() < 5 {
            poll.poll(&mut events, Some(Duration::from_secs(5)))?;
            assert!(!events.is_empty(), "timed out with {got:?}");
            while receiver.recv(&mut batch)? {
                for received in batch.iter() {
                    assert_eq!(received.from, sender.local_addr()?);
                    got.extend(received.datagrams().map(<[u8]>::to_vec));
                }
            }
        }
        assert_eq!(got, [&b"aaa"[..], b"bbb", b"ccc", b"d", b""]);
        Ok(())
    }
}
