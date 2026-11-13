//! End-to-end tests driving the built binary through its public interface:
//! arguments in, datagrams and stdout/stderr out.

use std::io::{BufRead, BufReader, Lines};
use std::net::SocketAddr;
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_udepe");

/// A running `udepe listen`, killed on drop.
struct Listen {
    child: Child,
    addr: SocketAddr,
    stdout: Lines<BufReader<ChildStdout>>,
    /// Held open so status logging does not hit a closed pipe.
    _stderr: Lines<BufReader<ChildStderr>>,
}

impl Listen {
    fn spawn(extra: &[&str]) -> Self {
        let mut child = Command::new(BIN)
            .args(["listen", "0", "127.0.0.1"])
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn udepe");
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut stderr = BufReader::new(child.stderr.take().unwrap()).lines();
        let addr = bound_addr(&mut stderr);
        Self {
            child,
            addr,
            stdout,
            _stderr: stderr,
        }
    }

    fn next_line(&mut self) -> String {
        self.stdout
            .next()
            .expect("stdout closed")
            .expect("read stdout")
    }
}

impl Drop for Listen {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reads the `listening on <addr>` status line.
fn bound_addr(stderr: &mut Lines<BufReader<ChildStderr>>) -> SocketAddr {
    let line = stderr.next().expect("stderr closed").expect("read stderr");
    line.rsplit_once("listening on ")
        .and_then(|(_, addr)| addr.parse().ok())
        .unwrap_or_else(|| panic!("unexpected status line: {line}"))
}

#[test]
fn send_reaches_listener() {
    let mut listen = Listen::spawn(&[]);

    let status = Command::new(BIN)
        .args([
            "send",
            "127.0.0.1",
            &listen.addr.port().to_string(),
            "hello",
        ])
        .status()
        .expect("run udepe send");
    assert!(status.success());

    assert!(listen.next_line().ends_with(": hello"));
}

#[test]
fn bad_arguments_exit_with_usage() {
    let out = Command::new(BIN)
        .args(["send", "::1"])
        .output()
        .expect("run udepe");
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage:"));
}

#[cfg(feature = "forward")]
#[test]
fn forward_routes_replies_back() {
    use std::net::{Ipv4Addr, UdpSocket};
    use std::time::Duration;

    let upstream = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let up = upstream.local_addr().unwrap();
    let mut proxy = Listen::spawn(&["--forward", "127.0.0.1", &up.port().to_string()]);

    let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    upstream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client.send_to(b"ping", proxy.addr).unwrap();

    let mut buf = [0; 16];
    let (n, from) = upstream.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"ping");
    upstream.send_to(b"pong", from).unwrap();

    let (n, from) = client.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"pong");
    assert_eq!(from, proxy.addr);
    assert!(proxy.next_line().ends_with(": ping"));
}
