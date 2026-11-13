//! Minimal UDP toolkit: send one datagram, listen and print datagrams, and
//! optionally relay them upstream with per-client sessions.
//!
//! Module boundaries:
//! - [`cli`] is pure: it turns arguments into a [`cli::Command`] and does no I/O.
//! - [`udp`] owns socket policy (dual-stack binding, address display).
//! - [`listen`] is the only module that writes data output; status goes
//!   through the [`log`] facade, and the binary decides where it ends up.
//! - `proxy` (feature `forward`) relays datagrams and tracks sessions.

pub mod cli;
pub mod listen;
#[cfg(feature = "forward")]
pub mod proxy;
pub mod udp;
