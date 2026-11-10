//! `server run`, `server status` and `server stop`, after the checks of
//! `commands`. `server run` holds the root until it exits and then writes the
//! status as of its exit; `server status` and `server stop` reach the App
//! Server only through the local API.

use std::time::Duration;

use appserver::lifecycle::{self, Times};
use appserver::local_api::FixedStatus;
use appserver::startup::{self, StartError};
use serde_json::{Value, json};
use state::paths::{self, StateRoot};

use crate::commands::{check_failed, io_failed};
use crate::output::{Code, CommandError};

/// `idle_exit` is the `--idle-exit` duration, already checked.
pub fn run(
    root: &StateRoot,
    observer: &observe::Observer,
    idle_exit: Option<Duration>,
) -> Result<Value, CommandError> {
    if let Err(error) = startup::raise_descriptor_limit() {
        observe::warn!(
            "server.descriptor_limit_not_raised",
            error.os_code = error.raw_os_error().unwrap_or(0),
            error.code = observe::Code("descriptor_limit_not_raised")
        );
    }
    let runtime = runtime()?;
    let lock = startup::lock(root).map_err(start_failed)?;
    let ended = runtime.block_on(async {
        let started = startup::serve(root, lock).map_err(start_failed)?;
        // Opened once the start can no longer fail, so that a refused start
        // creates no log file under the root.
        if observer.open_log_file(&root.join(paths::LOGS)).is_err() {
            observe::warn!(
                "log.file_open_failed",
                error.code = observe::Code("log_file_open_failed")
            );
        }
        let fixed = FixedStatus {
            instance_id: started.configuration.credentials.instance_id.clone(),
            cli_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let times = Times::default();
        #[cfg(feature = "test-seams")]
        let times = {
            let mut times = times;
            if let Ok(seconds) = std::env::var("TETRAL_TEST_GIT_LOW_SPEED_SECONDS") {
                let seconds = seconds
                    .parse::<std::num::NonZeroU64>()
                    .expect("test Git low-speed window is a positive whole number of seconds");
                times.session.git_low_speed_window = Duration::from_secs(seconds.get());
            }
            times
        };
        lifecycle::run(started, fixed, times, idle_exit)
            .await
            .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))
    })?;
    // Handlers still blocked past the shutdown's deadline are left behind.
    runtime.shutdown_background();
    // The exit's wait for the log sinks ends by the shutdown's deadline too,
    // so that the process exits within it.
    observer.end_flush_within(ended.left);
    Ok(serde_json::to_value(ended.status).expect("a status always serializes"))
}

pub fn status(root: &StateRoot) -> Result<Value, CommandError> {
    let client = client(root)?;
    let status = runtime()?.block_on(client.status()).map_err(sdk_failed)?;
    Ok(serde_json::to_value(status).expect("a status always serializes"))
}

pub fn stop(root: &StateRoot) -> Result<Value, CommandError> {
    let client = client(root)?;
    runtime()?.block_on(client.shutdown()).map_err(sdk_failed)?;
    Ok(json!({}))
}

fn runtime() -> Result<tokio::runtime::Runtime, CommandError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))
}

fn client(root: &StateRoot) -> Result<sdk::Client, CommandError> {
    sdk::Client::new(root).map_err(sdk_failed)
}

fn sdk_failed(error: sdk::Error) -> CommandError {
    match error {
        sdk::Error::NotRunning => {
            CommandError::new(Code::ServerNotRunning, "no App Server owns the state root")
        }
        sdk::Error::NotResponding => {
            CommandError::new(Code::ServerNotResponding, error.to_string())
        }
        other => CommandError::new(Code::IoFailed, other.to_string()),
    }
}

fn start_failed(error: StartError) -> CommandError {
    match error {
        StartError::Running => CommandError::new(Code::ServerRunning, error.to_string()),
        StartError::Configuration(configuration) => match configuration {
            appserver::configuration::ConfigurationError::Io(failure) => io_failed(failure),
            other => CommandError::new(Code::InvalidConfiguration, other.to_string()),
        },
        StartError::Unsafe(check) => check_failed(check),
        StartError::Io(failure) => io_failed(failure),
        StartError::Executable(_) => CommandError::new(Code::IoFailed, error.to_string()),
        #[cfg(feature = "test-seams")]
        StartError::TestToolExecution(_) => {
            CommandError::new(Code::InvalidConfiguration, error.to_string())
        }
    }
}
