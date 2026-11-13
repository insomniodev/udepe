# udepe

Minimal UDP sender, listener and session-tracking relay. Rust 1.99, edition 2024.

```bash
cargo run -- listen 9998
cargo run -- send 127.0.0.1 9998 "hello"

# relay to another listener; replies are routed back to each sender
cargo run -- listen 9999 --forward 127.0.0.1 9998

cargo test
```

Received datagrams go to stdout as `<from>: <data>`; status goes to stderr.

## Layout

| Module          | Role                                                         |
| --------------- | ------------------------------------------------------------ |
| `src/main.rs`   | Binary boundary: args, logger, address resolution, exit codes |
| `src/cli.rs`    | Pure, zero-copy argument parsing                             |
| `src/udp.rs`    | Socket policy: dual-stack bind, ephemeral senders            |
| `src/batch.rs`  | I/O seam: non-blocking sockets, batched receive and send     |
| `src/listen.rs` | `Listener`: the event loop; receive, print, optionally relay |
| `src/proxy.rs`  | `Proxy`: session table, one upstream socket per client       |
| `tests/cli.rs`  | End-to-end tests against the built binary                    |

## I/O model

No async runtime. `Listener` is a single-threaded event loop on `mio`
(epoll on Linux); `quinn-udp` batches the system calls underneath
(`recvmmsg`, GRO on receive, GSO on send). Sessions are rows in a table owned
by the loop, so nothing is locked or shared between threads.

Datagrams are never fragmented: one larger than the path MTU is dropped.
When a send buffer is full, datagrams are dropped rather than queued.

## Features

`forward` (default) enables `listen --forward` and the `proxy` module. Build
without it using `cargo build --no-default-features`.
