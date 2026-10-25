//! The command line and the checks every command runs, in this order; the
//! first that fails ends the command, so a refused command changes nothing in
//! the root:
//!
//! 1. the arguments (`invalid_arguments`): a known command and options,
//!    `TETRAL_HOME` an absolute path, `--app` nonempty and
//!    `TETRAL_LOG_LEVEL` one that `observe` accepts;
//! 2. the root checks of `state::private` (`unsafe_control_path`);
//! 3. the recorded app: an existing `config.toml` must parse
//!    (`invalid_configuration`) and name the same app (`app_mismatch`);
//! 4. the command itself.

use std::ffi::OsString;
use std::io;
use std::time::Duration;

use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use serde_json::Value;
use state::config::{self, ConfigError};
use state::files::IoFailure;
use state::paths::{ROOT_VARIABLE, StateRoot};
use state::private::{self, CheckError};

use crate::output::{Code, CommandError};
use crate::{configure, server};

#[derive(Parser)]
#[command(
    name = "tetral",
    disable_help_flag = true,
    disable_version_flag = true,
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Installs the bootstrap read from stdin.
    Configure {
        #[arg(long)]
        app: Option<String>,
        #[arg(long)]
        from_stdin: bool,
    },
    /// Runs, queries or stops the App Server.
    Server {
        #[command(subcommand)]
        command: ServerCommand,
    },
}

#[derive(Subcommand)]
enum ServerCommand {
    Run {
        #[arg(long)]
        app: Option<String>,
        #[arg(long)]
        idle_exit: Option<String>,
    },
    Status {
        #[arg(long)]
        app: Option<String>,
    },
    Stop {
        #[arg(long)]
        app: Option<String>,
    },
}

impl Command {
    fn app(&self) -> Option<&str> {
        match self {
            Command::Configure { app, .. }
            | Command::Server {
                command:
                    ServerCommand::Run { app, .. }
                    | ServerCommand::Status { app }
                    | ServerCommand::Stop { app },
            } => app.as_deref(),
        }
    }
}

/// `<duration>`: a positive integer followed by `s`, `m` or `h`.
fn duration(text: &str) -> Option<Duration> {
    let (count, unit) = text.split_at_checked(text.len().checked_sub(1)?)?;
    let seconds: u64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        _ => return None,
    };
    if count.is_empty() || !count.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let count: u64 = count.parse().ok().filter(|count| *count > 0)?;
    count.checked_mul(seconds).map(Duration::from_secs)
}

/// `tetral --version` alone, which runs before any check and reads no
/// environment variable and no file.
pub fn is_version(arguments: &[OsString]) -> bool {
    arguments.len() == 2 && arguments[1] == "--version"
}

pub fn run(arguments: Vec<OsString>) -> Result<Value, CommandError> {
    let cli = Cli::try_parse_from(arguments).map_err(|error| {
        invalid_arguments(match error.kind() {
            ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand => {
                "unknown command or option"
            }
            ErrorKind::MissingSubcommand | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => {
                "no command given"
            }
            _ => "the options do not fit the command",
        })
    })?;
    let root = std::env::var_os(ROOT_VARIABLE)
        .and_then(|root| StateRoot::new(&root))
        .ok_or_else(|| invalid_arguments("TETRAL_HOME must name an absolute path"))?;
    let app = cli
        .command
        .app()
        .filter(|app| !app.is_empty())
        .ok_or_else(|| invalid_arguments("--app must name the app"))?
        .to_owned();
    let level = observe::Level::from_env().map_err(|error| invalid_arguments(error.to_string()))?;
    match &cli.command {
        Command::Configure { from_stdin, .. } if !from_stdin => {
            return Err(invalid_arguments(
                "configure reads its bootstrap only with --from-stdin",
            ));
        }
        Command::Server {
            command:
                ServerCommand::Run {
                    idle_exit: Some(idle_exit),
                    ..
                },
        } if duration(idle_exit).is_none() => {
            return Err(invalid_arguments(
                "--idle-exit takes a positive integer followed by s, m or h",
            ));
        }
        _ => {}
    }
    let observer = observe::install(level);

    private::check_root(&root).map_err(check_failed)?;
    if let Some(recorded) = config::read(&root).map_err(config_failed)?
        && recorded.app != app
    {
        return Err(app_mismatch());
    }

    match cli.command {
        Command::Configure { .. } => configure::run(&root, &app, io::stdin().lock()),
        Command::Server { command } => match command {
            ServerCommand::Run { .. } => server::run(&root, &observer),
            ServerCommand::Status { .. } => server::status(&root),
            ServerCommand::Stop { .. } => server::stop(&root),
        },
    }
}

fn invalid_arguments(message: impl Into<String>) -> CommandError {
    CommandError::new(Code::InvalidArguments, message)
}

pub(crate) fn app_mismatch() -> CommandError {
    CommandError::new(Code::AppMismatch, "the state root belongs to another app")
}

pub(crate) fn io_failed(failure: IoFailure) -> CommandError {
    CommandError::new(Code::IoFailed, failure.to_string())
}

pub(crate) fn check_failed(error: CheckError) -> CommandError {
    match error {
        CheckError::Unsafe { .. } => CommandError::new(Code::UnsafeControlPath, error.to_string()),
        CheckError::Io(failure) => io_failed(failure),
    }
}

pub(crate) fn config_failed(error: ConfigError) -> CommandError {
    match error {
        ConfigError::Io(failure) => io_failed(failure),
        ConfigError::Format { .. } => {
            CommandError::new(Code::InvalidConfiguration, error.to_string())
        }
    }
}
