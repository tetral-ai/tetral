//! The command's result: exactly one compact JSON object and a newline on
//! stdout, `{}` or the command's object on success, or
//! `{"error": {"code", "message"}}` with exit status 1.

use std::io::Write;
use std::process::ExitCode;

use serde_json::{Value, json};

/// The failure codes the commands write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    InvalidArguments,
    AppMismatch,
    UnsafeControlPath,
    InvalidBootstrap,
    InvalidConfiguration,
    IoFailed,
    ServerRunning,
    ServerNotRunning,
}

impl Code {
    pub fn as_str(self) -> &'static str {
        match self {
            Code::InvalidArguments => "invalid_arguments",
            Code::AppMismatch => "app_mismatch",
            Code::UnsafeControlPath => "unsafe_control_path",
            Code::InvalidBootstrap => "invalid_bootstrap",
            Code::InvalidConfiguration => "invalid_configuration",
            Code::IoFailed => "io_failed",
            Code::ServerRunning => "server_running",
            Code::ServerNotRunning => "server_not_running",
        }
    }
}

/// A failed command. Its message is fixed text, a code, a path or an
/// operating-system error, never a value from the bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandError {
    pub code: Code,
    pub message: String,
}

impl CommandError {
    pub fn new(code: Code, message: impl Into<String>) -> CommandError {
        CommandError {
            code,
            message: message.into(),
        }
    }
}

/// `tetral --version`'s object.
pub fn version() -> Value {
    json!({ "cli_version": env!("CARGO_PKG_VERSION") })
}

pub fn write(result: Result<Value, CommandError>) -> ExitCode {
    let (object, status) = match result {
        Ok(object) => (object, ExitCode::SUCCESS),
        Err(error) => (
            json!({ "error": { "code": error.code.as_str(), "message": error.message } }),
            ExitCode::FAILURE,
        ),
    };
    let mut line = serde_json::to_vec(&object).expect("a result always serializes");
    line.push(b'\n');
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(&line).and_then(|()| stdout.flush());
    status
}
