//! Sends one command to a running engine.

use std::path::PathBuf;
use std::process::ExitCode;

use ringbook_ctl::{DEFAULT_SOCKET, send_command};

fn main() -> ExitCode {
    let line = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    let socket = std::env::var_os("RINGBOOK_CTL")
        .map_or_else(|| PathBuf::from(DEFAULT_SOCKET), PathBuf::from);
    match send_command(&socket, &line) {
        Ok(response) => {
            print!("{response}");
            if response.starts_with("error") {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("ringbookctl: {}: {error}", socket.display());
            ExitCode::FAILURE
        }
    }
}
