//! `tetral drives enable`: it runs only while no App Server owns the root.
//! On Linux, where drives need no consent, it then succeeds with `{}` and
//! changes nothing. On a Mac it mounts a test drive from its own NFS server
//! and reads its one file, so macOS asks for network-volume access naming
//! the app that launched the command, and records the user's consent as
//! `drives_enabled = true`; every failure records nothing.

use appserver::lock::{self, LockError};
use serde_json::{Value, json};
use state::paths::{self, StateRoot};

use crate::commands::io_failed;
use crate::output::{Code, CommandError};

pub fn enable(root: &StateRoot) -> Result<Value, CommandError> {
    #[cfg(target_os = "macos")]
    macos::recorded(root)?;
    #[cfg(target_os = "macos")]
    state::files::create_private_dirs(&root.join(paths::RUN)).map_err(io_failed)?;
    // A root without `run/` has no App Server and no lock to take.
    let _lock = if root.join(paths::RUN).is_dir() {
        Some(lock::acquire(root).map_err(|error| match error {
            LockError::Held => CommandError::new(
                Code::ServerRunning,
                "an App Server or another command owns the state root",
            ),
            LockError::Io(failure) => io_failed(failure),
        })?)
    } else {
        None
    };
    #[cfg(target_os = "macos")]
    macos::enable(root)?;
    Ok(json!({}))
}

#[cfg(target_os = "macos")]
mod macos {
    use std::io;
    use std::path::Path;

    use drive::mount_macos::{force_unmount, listed, mount_nfs};
    use drive::nfs::TestDrive;
    use state::config::{self, Config};
    use state::paths::{self, StateRoot};
    use tokio::signal::unix::{SignalKind, signal};

    use crate::commands::{config_failed, io_failed};
    use crate::output::{Code, CommandError};

    /// The configuration the consent is recorded into; a root without one
    /// fails before anything is mounted.
    pub(super) fn recorded(root: &StateRoot) -> Result<Config, CommandError> {
        config::read(root).map_err(config_failed)?.ok_or_else(|| {
            CommandError::new(
                Code::InvalidConfiguration,
                format!("{} is missing", root.join(paths::CONFIG).display()),
            )
        })
    }

    fn mount_failed(message: String) -> CommandError {
        CommandError::new(Code::DrivesMountFailed, message)
    }

    /// The test mount, read from this process with no time limit, until
    /// `SIGINT` or `SIGTERM`.
    pub(super) fn enable(root: &StateRoot) -> Result<(), CommandError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
        runtime.block_on(async {
            // Taken first, so a signal at any later moment ends the
            // command through `cancelled` with the test mount removed.
            let signals = signal(SignalKind::interrupt())
                .and_then(|interrupt| Ok((interrupt, signal(SignalKind::terminate())?)));
            let (mut interrupt, mut terminate) =
                signals.map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
            let canonical = std::fs::canonicalize(root.path())
                .map_err(|error| io_failed(state::files::IoFailure::new(root.path(), error)))?;
            let point = canonical.join(paths::RUN).join("drives-enable");
            let server =
                TestDrive::start().map_err(|error| mount_failed(format!("NFS server: {error}")))?;
            prepare(&point)?;
            mount_nfs(server.port(), &server.source(), &point, false).map_err(mount_failed)?;
            let file = point.join("check.txt");
            let (read, reading) = tokio::sync::oneshot::channel();
            {
                let file = file.clone();
                std::thread::spawn(move || {
                    let _ = read.send(std::fs::read(file));
                });
            }
            let read = tokio::select! {
                biased;
                _ = interrupt.recv() => None,
                _ = terminate.recv() => None,
                read = reading => Some(read.expect("the reading thread answers")),
            };
            let recorded = match read {
                None => Err(CommandError::new(
                    Code::Cancelled,
                    "drives enable was cancelled while macOS asked for network-volume access",
                )),
                Some(Ok(_)) => record(root),
                // `EPERM` and `EACCES` alike.
                Some(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied => {
                    let failure = state::files::IoFailure::new(&file, error);
                    Err(CommandError::new(
                        Code::DrivesAccessDenied,
                        failure.to_string(),
                    ))
                }
                Some(Err(error)) => Err(io_failed(state::files::IoFailure::new(&file, error))),
            };
            let _ = force_unmount(&point);
            recorded
        })
    }

    /// The mount point `R/run/drives-enable` with mode `0700`, after a
    /// stale mount there is force-unmounted without touching the path.
    fn prepare(point: &Path) -> Result<(), CommandError> {
        if listed(point)
            .map_err(|error| mount_failed(format!("mount: {error}")))?
            .is_some()
        {
            force_unmount(point)
                .map_err(|error| mount_failed(format!("force unmount: {error}")))?;
        }
        state::files::create_private_dirs(point)
            .map_err(|failure| mount_failed(format!("mount point: {}", failure.source)))
    }

    /// Writes `drives_enabled = true` into the configuration as it stands
    /// under the lock.
    fn record(root: &StateRoot) -> Result<(), CommandError> {
        let mut config = recorded(root)?;
        config.drives_enabled = true;
        let encoded = config
            .encode()
            .map_err(|error| CommandError::new(Code::IoFailed, error.to_string()))?;
        encoded.replace(root).map_err(io_failed)
    }
}
