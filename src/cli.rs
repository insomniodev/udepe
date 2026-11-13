//! Command-line parsing. Parsing is pure and borrows from the arguments;
//! host names are resolved later by the caller, since that needs I/O.

use std::fmt;

/// Usage text printed for `help` and on invalid arguments.
#[cfg(feature = "forward")]
pub const USAGE: &str = "\
usage:
  udepe send <address> <port> <message>
      Send one datagram.
  udepe listen <port> [address] [--forward <address> <port>]
      Print received datagrams. Address defaults to :: (all interfaces).
      With --forward, also relay each datagram to another listener and
      route its replies back to the sender.
";

/// Usage text printed for `help` and on invalid arguments.
#[cfg(not(feature = "forward"))]
pub const USAGE: &str = "\
usage:
  udepe send <address> <port> <message>
      Send one datagram.
  udepe listen <port> [address]
      Print received datagrams. Address defaults to :: (all interfaces).
";

/// A parsed command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command<'a> {
    /// Print usage.
    Help,
    /// Send `message` as one datagram to `dest`.
    Send {
        /// Where to send.
        dest: Endpoint<'a>,
        /// Datagram payload.
        message: &'a str,
    },
    /// Receive datagrams on `local`.
    Listen {
        /// Address to bind.
        local: Endpoint<'a>,
        /// Where to relay received datagrams, if anywhere.
        #[cfg(feature = "forward")]
        forward: Option<Endpoint<'a>>,
    },
}

/// An unresolved host and a validated port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint<'a> {
    /// Host name or IP literal.
    pub host: &'a str,
    /// Port number.
    pub port: u16,
}

impl<'a> Endpoint<'a> {
    fn parse(host: &'a str, port: &str) -> Result<Self, Error> {
        if port.starts_with('+') {
            return Err(Error::InvalidPort(port.to_owned()));
        }
        let port = port
            .parse()
            .map_err(|_| Error::InvalidPort(port.to_owned()))?;
        Ok(Self { host, port })
    }
}

/// Why arguments could not be parsed.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Wrong number or shape of arguments.
    Usage,
    /// The first argument is not a known command.
    UnknownCommand(String),
    /// A port is not a number in `0..=65535`.
    InvalidPort(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => f.write_str("invalid arguments"),
            Self::UnknownCommand(cmd) => write!(f, "unknown command: {cmd}"),
            Self::InvalidPort(port) => write!(f, "invalid port: {port}"),
        }
    }
}

impl std::error::Error for Error {}

/// Parses `args`, excluding the program name, into a [`Command`].
///
/// # Errors
///
/// Returns an [`Error`] describing the first problem found.
pub fn parse<'a>(args: &[&'a str]) -> Result<Command<'a>, Error> {
    match args {
        ["-h" | "--help" | "help", ..] => Ok(Command::Help),
        ["send", host, port, message] => Ok(Command::Send {
            dest: Endpoint::parse(host, port)?,
            message,
        }),
        ["listen", rest @ ..] => parse_listen(rest),
        ["send", ..] | [] => Err(Error::Usage),
        [cmd, ..] => Err(Error::UnknownCommand((*cmd).to_owned())),
    }
}

/// Parses `<port> [address] [--forward <address> <port>]`.
fn parse_listen<'a>(args: &[&'a str]) -> Result<Command<'a>, Error> {
    #[cfg(feature = "forward")]
    let (args, forward) = match args {
        [rest @ .., "--forward", host, port] => (rest, Some(Endpoint::parse(host, port)?)),
        _ => (args, None),
    };
    let local = match args {
        [port] => Endpoint::parse("::", port)?,
        [port, host] => Endpoint::parse(host, port)?,
        _ => return Err(Error::Usage),
    };
    Ok(Command::Listen {
        local,
        #[cfg(feature = "forward")]
        forward,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(host: &str, port: u16) -> Endpoint<'_> {
        Endpoint { host, port }
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(
            parse(&["send", "::1", "9", "hi"]),
            Ok(Command::Send {
                dest: ep("::1", 9),
                message: "hi"
            })
        );
        let Ok(Command::Listen { local, .. }) = parse(&["listen", "9998"]) else {
            panic!("expected listen");
        };
        assert_eq!(local, ep("::", 9998));
    }

    #[cfg(feature = "forward")]
    #[test]
    fn parses_forward() {
        assert_eq!(
            parse(&["listen", "9998", "127.0.0.1", "--forward", "::1", "9997"]),
            Ok(Command::Listen {
                local: ep("127.0.0.1", 9998),
                forward: Some(ep("::1", 9997))
            })
        );
        assert_eq!(
            parse(&["listen", "9998", "--forward", "::1"]),
            Err(Error::Usage)
        );
    }

    #[test]
    fn rejects_bad_arguments() {
        assert_eq!(parse(&[]), Err(Error::Usage));
        assert_eq!(parse(&["send", "::1", "9"]), Err(Error::Usage));
        assert_eq!(parse(&["listen"]), Err(Error::Usage));
        assert_eq!(parse(&["listen", "1", "::", "extra"]), Err(Error::Usage));
        for port in ["70000", "-1", "", "+9"] {
            assert_eq!(
                parse(&["listen", port]),
                Err(Error::InvalidPort(port.into()))
            );
        }
        assert_eq!(parse(&["recv"]), Err(Error::UnknownCommand("recv".into())));
    }
}
