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

use clap::error::ErrorKind;
use clap::{Parser, Subcommand};
use serde_json::Value;
use state::atomic::IoFailure;
use state::config::{self, ConfigError};
use state::paths::{ROOT_VARIABLE, StateRoot};
use state::private::{self, CheckError};

use crate::configure;
use crate::output::{Code, CommandError};

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
}

impl Command {
    fn app(&self) -> Option<&str> {
        match self {
            Command::Configure { app, .. } => app.as_deref(),
        }
    }
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
    let Command::Configure { from_stdin, .. } = cli.command;
    if !from_stdin {
        return Err(invalid_arguments(
            "configure reads its bootstrap only with --from-stdin",
        ));
    }
    observe::install(level);

    private::check_root(&root).map_err(check_failed)?;
    if let Some(recorded) = config::read(&root).map_err(config_failed)?
        && recorded.app != app
    {
        return Err(app_mismatch());
    }

    configure::run(&root, &app, io::stdin().lock())
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

fn check_failed(error: CheckError) -> CommandError {
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
