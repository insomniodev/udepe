//! Binary boundary: reads arguments, installs the logger, resolves addresses
//! and maps failures to exit codes. All behavior lives in the library.

use std::io::{self, Write};
use std::net::SocketAddr;
use std::process::ExitCode;

use log::{Level, LevelFilter, Log, Metadata, Record, info};
use udepe::cli::{self, Command, Endpoint};
use udepe::listen::Listener;
use udepe::udp;

/// Writes `info` and above to stderr, keeping stdout for data.
struct StderrLogger;

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            // Logging must never take the program down, e.g. on a closed pipe.
            let level = record.level().as_str().to_ascii_lowercase();
            let _ = writeln!(io::stderr(), "{level}: {}", record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;

fn main() -> ExitCode {
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(LevelFilter::Info);
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    match cli::parse(&args) {
        Ok(cmd) => run(cmd).unwrap_or_else(|err| {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }),
        Err(err) => {
            if err != cli::Error::Usage {
                eprintln!("{err}");
            }
            eprint!("{}", cli::USAGE);
            ExitCode::from(2)
        }
    }
}

fn run(cmd: Command<'_>) -> io::Result<ExitCode> {
    match cmd {
        Command::Help => {
            io::stdout().write_all(cli::USAGE.as_bytes())?;
        }
        Command::Send { dest, message } => {
            let dest = resolve(dest)?;
            let n = udp::send(dest, message.as_bytes())?;
            info!("sent {n} bytes to {dest}");
        }
        Command::Listen {
            local,
            #[cfg(feature = "forward")]
            forward,
        } => {
            let mut listener = Listener::bind(resolve(local)?)?;
            #[cfg(feature = "forward")]
            if let Some(upstream) = forward {
                listener =
                    listener.forward_to(resolve(upstream)?, udepe::proxy::DEFAULT_IDLE_TIMEOUT);
            }
            // `run` only returns on failure: its success type is uninhabited.
            let Err(err) = listener.run(&mut io::stdout().lock());
            return Err(err);
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Resolves `ep`, naming the host in the error.
fn resolve(ep: Endpoint<'_>) -> io::Result<SocketAddr> {
    udp::resolve(ep.host, ep.port)
        .map_err(|err| io::Error::new(err.kind(), format!("invalid address {}: {err}", ep.host)))
}
