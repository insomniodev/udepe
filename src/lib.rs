//! Minimal UDP toolkit: send one datagram, listen and print datagrams, and
//! optionally relay them upstream with per-client sessions.
//!
//! Module boundaries:
//! - [`cli`] is pure: it turns arguments into a [`cli::Command`] and does no I/O.
//! - [`udp`] owns socket policy (dual-stack binding, address display).
//! - [`batch`] is the I/O seam: non-blocking sockets that receive and send
//!   datagrams in batches. Only it knows which system calls are used.
//! - [`listen`] is the only module that writes data output; status goes
//!   through the [`log`] facade, and the binary decides where it ends up.
//! - `proxy` (feature `forward`) relays datagrams and tracks sessions.
//!
//! There is no async runtime. [`listen::Listener`] is a single-threaded
//! event loop over [`mio`], and everything it drives is plain state that is
//! never shared between threads.

pub mod batch;
pub mod cli;
pub mod listen;
#[cfg(feature = "forward")]
pub mod proxy;
pub mod udp;
