//! `tetral computer-use enable` and `disable`, after the checks of
//! `commands`. The command first finds whether an App Server owns the root:
//! it connects to the App Server's socket and, when that fails, tries the
//! root's lock. When an App Server answers, the command sends it
//! `POST /v1/computer-use` and writes nothing itself; when it takes the lock,
//! it writes `[computer_use]` of `config.toml` by atomic replacement. While
//! the lock is held and no App Server answers, as while one starts or stops,
//! it tries the pair again every 50 milliseconds, and fails with
//! `server_running` when neither succeeded 10 seconds after its first try,
//! on the seam clock; each reading of the clock while it waits to try again
//! is the test seam counter `computer_use.search_checked`.

use std::path::PathBuf;
use std::time::Duration;

use appserver::lock::{self, LockError};
use serde_json::{Value, json};
use state::config::{self, EncodeError};
use state::paths::{self, StateRoot};

use crate::commands::{app_mismatch, config_failed, io_failed};
use crate::output::{Code, CommandError};

/// How long after one try of the socket and the lock the next starts.
pub const SEARCH_INTERVAL: Duration = Duration::from_millis(50);
/// How long after its first try the command stops searching.
pub const SEARCH_LIMIT: Duration = Duration::from_secs(10);

/// `driver` is the checked `--driver`, given exactly when `enabled`.
pub fn run(
    root: &StateRoot,
    app: &str,
    enabled: bool,
    driver: Option<&str>,
) -> Result<Value, CommandError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
    runtime.block_on(apply(root, app, enabled, driver))
}

async fn apply(
    root: &StateRoot,
    app: &str,
    enabled: bool,
    driver: Option<&str>,
) -> Result<Value, CommandError> {
    let first = state::seams::now();
    loop {
        let tried = state::seams::now();
        match send(root, enabled, driver).await {
            Err(sdk::Error::NotRunning) => {}
            answered => return answered.map(|()| json!({})).map_err(answer_failed),
        }
        if let Some(recorded) = record_unowned(root, app, enabled, driver) {
            return recorded;
        }
        if state::seams::now() >= first + SEARCH_LIMIT {
            return Err(CommandError::new(
                Code::ServerRunning,
                "the state root's lock stayed held for 10 seconds with no App Server answering",
            ));
        }
        state::seams::sleep_until_counted(tried + SEARCH_INTERVAL, "computer_use.search_checked")
            .await;
    }
}

/// `POST /v1/computer-use` to the App Server that owns the root, or
/// `NotRunning` when none answers on its socket.
async fn send(root: &StateRoot, enabled: bool, driver: Option<&str>) -> Result<(), sdk::Error> {
    sdk::Client::new(root)?
        .set_computer_use(enabled, driver)
        .await
}

/// The command's result once it took the root's lock, or `None` while
/// another process holds it. A root without `run/` has no App Server and no
/// lock to take.
fn record_unowned(
    root: &StateRoot,
    app: &str,
    enabled: bool,
    driver: Option<&str>,
) -> Option<Result<Value, CommandError>> {
    let _lock = if root.join(paths::RUN).is_dir() {
        match lock::acquire(root) {
            Ok(lock) => Some(lock),
            Err(LockError::Held) => return None,
            Err(LockError::Io(failure)) => return Some(Err(io_failed(failure))),
        }
    } else {
        None
    };
    Some(record(root, app, enabled, driver))
}

/// Writes `[computer_use]` under the lock, checking the app again on what
/// the lock now protects.
fn record(
    root: &StateRoot,
    app: &str,
    enabled: bool,
    driver: Option<&str>,
) -> Result<Value, CommandError> {
    let mut config = config::read(root).map_err(config_failed)?.ok_or_else(|| {
        CommandError::new(
            Code::InvalidConfiguration,
            format!("{} is missing", root.join(paths::CONFIG).display()),
        )
    })?;
    if config.app != app {
        return Err(app_mismatch());
    }
    config.set_computer_use(enabled, driver.map(PathBuf::from));
    let encoded = config.encode().map_err(|error| match error {
        EncodeError::TooLarge => CommandError::new(
            Code::InvalidArguments,
            "--driver cannot fit in the configuration's 65,536-byte limit",
        ),
        EncodeError::Format => CommandError::new(Code::IoFailed, error.to_string()),
    })?;
    encoded.replace(root).map_err(io_failed)?;
    Ok(json!({}))
}

/// The App Server's failure answer, `{code, message}`, as the command's.
fn answer_failed(error: sdk::Error) -> CommandError {
    if let sdk::Error::Http { body, .. } = &error
        && let Ok(failure) = serde_json::from_slice::<Value>(body)
        && let (Some(code), Some(message)) = (failure["code"].as_str(), failure["message"].as_str())
    {
        let code = match code {
            "driver_failed" => Some(Code::DriverFailed),
            "invalid_arguments" => Some(Code::InvalidArguments),
            "io_failed" => Some(Code::IoFailed),
            _ => None,
        };
        if let Some(code) = code {
            return CommandError::new(code, message);
        }
    }
    CommandError::new(Code::IoFailed, error.to_string())
}
