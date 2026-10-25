//! The entry points that `tetral` runs as inside a task's sandbox, selected
//! by `argv[1]` [`ENTRY`] before any other argument is parsed. Each writes
//! [`STARTED`] on the launch-status descriptor that the App Server passes it
//! right before it executes its command, and a failure to start as one JSON
//! object `{kind, message, path}`, before it or, when the exec itself fails,
//! after it. The end of that descriptor tells nothing: the sandbox's own
//! stages hold it open until the sandbox exits.

pub mod exec;

use std::ffi::OsString;
use std::process::ExitCode;

/// `argv[1]` of a task's launcher entry.
pub const ENTRY: &str = "__tetral-task";

/// What an entry writes on the launch-status descriptor when its command
/// starts.
pub const STARTED: &[u8] = b"\n";

/// Runs the entry that `arguments`, the arguments after [`ENTRY`], name.
/// It returns only when its command could not start.
pub fn run(arguments: &[OsString]) -> ExitCode {
    match arguments.split_first() {
        Some((kind, rest)) if kind == exec::KIND => exec::run(rest),
        _ => ExitCode::FAILURE,
    }
}
