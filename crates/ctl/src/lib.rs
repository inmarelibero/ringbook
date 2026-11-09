//! Operator control over a local Unix socket.
//!
//! Each connection sends one line and reads one response. Commands reach the
//! hot thread through a ring and apply at a batch boundary. A global halt
//! bypasses the ring and sets an atomic flag, so it can't be delayed.

#![forbid(unsafe_code)]

use std::fs;
use std::io::{self, BufRead, BufReader, ErrorKind, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use ringbook_core::{ControlCommand, InstrumentId, ParamChange, ParamKey, Scale};
use ringbook_ring::Producer;

const OWNER_ONLY: u32 = 0o600;
const BPS_DECIMALS: u8 = 2;
const IDLE_SLEEP: Duration = Duration::from_millis(20);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// The socket used when none is configured.
pub const DEFAULT_SOCKET: &str = "/tmp/ringbook.sock";

/// A parsed operator request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Request {
    /// Report the engine's state.
    Status,
    /// Stop all quoting until restart.
    GlobalHalt,
    /// Send a command to the hot thread.
    Command(ControlCommand),
}

fn parse_bps(text: &str) -> Result<i64, String> {
    Scale::new(BPS_DECIMALS)
        .expect("constant scale is valid")
        .parse(text.as_bytes())
        .map_err(|error| format!("{text:?}: {error}"))
}

/// Parses a command line, using the same rules as the config file.
///
/// # Errors
///
/// Returns a message describing the problem.
pub fn parse_request(
    line: &str,
    instruments: &[(String, InstrumentId)],
) -> Result<Request, String> {
    let find = |name: &str| {
        instruments
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, id)| *id)
            .ok_or_else(|| format!("unknown instrument {name}"))
    };
    let words: Vec<&str> = line.split_whitespace().collect();
    match words.as_slice() {
        ["status"] => Ok(Request::Status),
        ["halt"] => Ok(Request::GlobalHalt),
        ["halt-symbol", name] => Ok(Request::Command(ControlCommand::HaltInstrument(find(
            name,
        )?))),
        ["resume-symbol", name] => Ok(Request::Command(ControlCommand::ResumeInstrument(find(
            name,
        )?))),
        ["set", key, value] => {
            let value = parse_bps(value)?;
            let key = match *key {
                "spread_bps" if value == 0 => {
                    return Err("spread_bps must be greater than zero".to_owned());
                }
                "spread_bps" => ParamKey::SpreadCentiBps,
                "inventory_skew_bps" => ParamKey::InventorySkewCentiBps,
                "requote_threshold_bps" => ParamKey::RequoteThresholdCentiBps,
                other => return Err(format!("{other} cannot be changed while running")),
            };
            Ok(Request::Command(ControlCommand::SetParam(ParamChange {
                key,
                value,
            })))
        }
        _ => Err(
            "usage: status | halt | halt-symbol NAME | resume-symbol NAME | set KEY VALUE"
                .to_owned(),
        ),
    }
}

struct Server {
    commands: Producer<ControlCommand>,
    halt_flag: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    instruments: Vec<(String, InstrumentId)>,
}

impl Server {
    fn respond(&mut self, line: &str) -> String {
        match parse_request(line, &self.instruments) {
            Ok(Request::Status) => self
                .status
                .lock()
                .map_or_else(|_| "status unavailable".to_owned(), |status| status.clone()),
            Ok(Request::GlobalHalt) => {
                self.halt_flag.store(true, Ordering::Release);
                "halting".to_owned()
            }
            Ok(Request::Command(command)) => {
                if self.commands.push(command).is_ok() {
                    "ok".to_owned()
                } else {
                    "error: control ring full, try again".to_owned()
                }
            }
            Err(message) => format!("error: {message}"),
        }
    }

    fn handle(&mut self, stream: UnixStream) -> io::Result<()> {
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
        let mut line = String::new();
        BufReader::new(&stream).read_line(&mut line)?;
        let response = self.respond(line.trim());
        (&stream).write_all(response.as_bytes())?;
        (&stream).write_all(b"\n")
    }
}

/// A running control server; removes its socket when stopped.
pub struct ControlServer {
    thread: JoinHandle<()>,
    stop: Arc<AtomicBool>,
    path: PathBuf,
}

impl ControlServer {
    /// Stops the server.
    ///
    /// # Panics
    ///
    /// Panics if the server thread panicked.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Release);
        self.thread.join().expect("the control thread panicked");
        let _ = fs::remove_file(self.path);
    }
}

/// Starts the control server on a socket only its owner can use.
///
/// # Errors
///
/// Returns an error if the socket can't be created.
pub fn start_control_server(
    path: &Path,
    commands: Producer<ControlCommand>,
    halt_flag: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    instruments: Vec<(String, InstrumentId)>,
) -> io::Result<ControlServer> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(OWNER_ONLY))?;
    listener.set_nonblocking(true)?;

    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let mut server = Server {
        commands,
        halt_flag,
        status,
        instruments,
    };
    let thread = std::thread::Builder::new()
        .name("control".into())
        .spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Err(error) = server.handle(stream) {
                            eprintln!("control: request failed: {error}");
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(IDLE_SLEEP);
                    }
                    Err(error) => eprintln!("control: accept failed: {error}"),
                }
            }
        })?;
    Ok(ControlServer {
        thread,
        stop,
        path: path.to_owned(),
    })
}

/// Sends one command and returns the response.
///
/// # Errors
///
/// Returns an error if the socket can't be reached.
pub fn send_command(path: &Path, line: &str) -> io::Result<String> {
    let stream = UnixStream::connect(path)?;
    stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
    (&stream).write_all(line.as_bytes())?;
    (&stream).write_all(b"\n")?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = String::new();
    io::Read::read_to_string(&mut &stream, &mut response)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbook_ring::channel;

    fn instruments() -> Vec<(String, InstrumentId)> {
        vec![("BTC-PERP".to_owned(), InstrumentId(3))]
    }

    fn parse(line: &str) -> Result<Request, String> {
        parse_request(line, &instruments())
    }

    #[test]
    fn parses_every_command() {
        assert_eq!(parse("status"), Ok(Request::Status));
        assert_eq!(parse("halt"), Ok(Request::GlobalHalt));
        assert_eq!(
            parse("halt-symbol BTC-PERP"),
            Ok(Request::Command(ControlCommand::HaltInstrument(
                InstrumentId(3)
            )))
        );
        assert_eq!(
            parse("resume-symbol BTC-PERP"),
            Ok(Request::Command(ControlCommand::ResumeInstrument(
                InstrumentId(3)
            )))
        );
        assert_eq!(
            parse("set spread_bps 5.5"),
            Ok(Request::Command(ControlCommand::SetParam(ParamChange {
                key: ParamKey::SpreadCentiBps,
                value: 550,
            })))
        );
    }

    #[test]
    fn rejects_invalid_commands() {
        assert!(parse("").is_err());
        assert!(parse("halt-symbol DOGE").unwrap_err().contains("unknown"));
        assert!(parse("set spread_bps 0").unwrap_err().contains("greater"));
        assert!(parse("set spread_bps x").is_err());
    }

    #[test]
    fn risk_limits_cannot_change_while_running() {
        let error = parse("set max_drawdown_usd 1").unwrap_err();
        assert!(error.contains("cannot be changed"));
    }

    fn socket(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rb-{}-{name}.sock", std::process::id()))
    }

    #[test]
    fn serves_commands_on_an_owner_only_socket() {
        let path = socket("serve");
        let (producer, mut consumer) = channel(4).unwrap();
        let halt = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new("running".to_owned()));
        let server =
            start_control_server(&path, producer, Arc::clone(&halt), status, instruments())
                .unwrap();

        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, OWNER_ONLY);

        assert_eq!(send_command(&path, "status").unwrap().trim(), "running");
        assert_eq!(
            send_command(&path, "halt-symbol BTC-PERP").unwrap().trim(),
            "ok"
        );
        assert_eq!(
            consumer.pop(),
            Some(ControlCommand::HaltInstrument(InstrumentId(3)))
        );
        assert!(
            send_command(&path, "nonsense")
                .unwrap()
                .starts_with("error")
        );

        assert_eq!(send_command(&path, "halt").unwrap().trim(), "halting");
        assert!(halt.load(Ordering::Acquire));

        server.stop();
        assert!(!path.exists());
    }

    #[test]
    fn a_full_control_ring_is_reported_and_halt_still_works() {
        let path = socket("full");
        let (producer, _consumer) = channel(1).unwrap();
        let halt = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new(String::new()));
        let server =
            start_control_server(&path, producer, Arc::clone(&halt), status, instruments())
                .unwrap();

        assert_eq!(
            send_command(&path, "halt-symbol BTC-PERP").unwrap().trim(),
            "ok"
        );
        assert!(
            send_command(&path, "halt-symbol BTC-PERP")
                .unwrap()
                .contains("full")
        );
        send_command(&path, "halt").unwrap();
        assert!(halt.load(Ordering::Acquire));
        server.stop();
    }
}
