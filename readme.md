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
| `src/listen.rs` | `Listener`: receive, print, optionally relay                 |
| `src/proxy.rs`  | `Proxy`: one upstream socket and reply thread per client     |
| `tests/cli.rs`  | End-to-end tests against the built binary                    |

## Features

`forward` (default) enables `listen --forward` and the `proxy` module. Build
without it using `cargo build --no-default-features`.
