//! The start of `server run` after the command's own checks, in this order:
//! the state-root lock, the removal of interrupted replacements, an empty
//! `outbox/`, the configuration read under the lock, the socket directory
//! `S`, and the socket. A failure at any step leaves the steps before it and
//! touches nothing after it; without the lock, nothing in `S`
//! is touched, since the lock is what proves that no other App Server serves
//! the root's socket.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use rustix::fs::{AtFlags, CWD, Timespec, Timestamps, UTIME_NOW, UTIME_OMIT};

use state::files::{self, IoFailure};
use state::paths::{self, StateRoot};
use state::private::{self, CheckError};
use tokio::net::UnixListener;

use crate::configuration::{self, Configuration, ConfigurationError};
use crate::lock::{self, LockError, RootLock};

/// What a started App Server holds until it exits.
#[derive(Debug)]
pub struct Started {
    pub lock: RootLock,
    pub configuration: Configuration,
    pub socket: PathBuf,
    pub listener: UnixListener,
    pub outbox: PathBuf,
    /// The state root as `TETRAL_HOME` gives it.
    pub root: PathBuf,
    /// The state root's canonical path.
    pub canonical_root: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("an App Server or another command owns the state root")]
    Running,
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Unsafe(CheckError),
    #[error(transparent)]
    Io(#[from] IoFailure),
}

impl From<CheckError> for StartError {
    fn from(error: CheckError) -> StartError {
        match error {
            CheckError::Io(failure) => StartError::Io(failure),
            unsafe_path => StartError::Unsafe(unsafe_path),
        }
    }
}

/// Creates a missing `run/`, takes the lock without waiting and removes the
/// temporary files of interrupted replacements. A root that does not exist
/// holds no configuration, so the start fails without creating it.
pub fn lock(root: &StateRoot) -> Result<RootLock, StartError> {
    if let Err(error) = std::fs::symlink_metadata(root.path()) {
        return Err(if error.kind() == std::io::ErrorKind::NotFound {
            StartError::Configuration(ConfigurationError::Missing {
                path: root.join(paths::CONFIG),
            })
        } else {
            StartError::Io(IoFailure::new(root.path(), error))
        });
    }
    files::create_private_dirs(&root.join(paths::RUN))?;
    let lock = lock::acquire(root).map_err(|error| match error {
        LockError::Held => StartError::Running,
        LockError::Io(failure) => StartError::Io(failure),
    })?;
    files::remove_interrupted_replacements(root.path())?;
    Ok(lock)
}

/// The steps after the lock: the configuration; `outbox/` emptied, whose
/// reports belong to connections of an earlier process, only once the
/// configuration is valid, so that a start that fails on it leaves the root
/// as it was; then `S` and the socket, bound with mode `0600` before it
/// accepts a connection.
pub fn serve(root: &StateRoot, lock: RootLock) -> Result<Started, StartError> {
    let configuration = configuration::read(root)?;
    let canonical_root =
        std::fs::canonicalize(root.path()).map_err(|error| IoFailure::new(root.path(), error))?;
    observe::STARTUP.config_loaded(state::seams::now());
    state::outbox::reset(&root.join(paths::OUTBOX))?;

    private::create_socket_dir()?;
    let socket = paths::socket_path(root)
        .map_err(|error| IoFailure::new(root.path(), error))?
        .ok_or_else(|| IoFailure::new(root.path(), std::io::ErrorKind::NotFound))?;
    match std::fs::remove_file(&socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(IoFailure::new(&socket, error).into()),
    }
    let listener = UnixListener::bind(&socket).map_err(|error| IoFailure::new(&socket, error))?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| IoFailure::new(&socket, error))?;
    Ok(Started {
        lock,
        configuration,
        socket,
        listener,
        outbox: root.join(paths::OUTBOX),
        root: root.path().to_path_buf(),
        canonical_root,
    })
}

/// How often the socket's modification time is set to the current time, so
/// that age-based cleanup of `/tmp` never removes the socket of a running App
/// Server.
pub const SOCKET_TOUCH_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Sets the socket's modification time to now at `first` and then one
/// interval of the seam clock after each, counting each as the test seam
/// counter `socket.touched` only once the next one is scheduled, so that a
/// harness that advances the clock on seeing the count reaches the next.
pub(crate) async fn keep_socket_fresh(socket: PathBuf, first: Duration, interval: Duration) {
    let mut next = first;
    loop {
        state::seams::sleep_until(next).await;
        let now = Timestamps {
            last_access: Timespec {
                tv_sec: 0,
                tv_nsec: UTIME_OMIT,
            },
            last_modification: Timespec {
                tv_sec: 0,
                tv_nsec: UTIME_NOW,
            },
        };
        let _ = rustix::fs::utimensat(CWD, &socket, &now, AtFlags::SYMLINK_NOFOLLOW);
        next = state::seams::now() + interval;
        state::seams::count("socket.touched");
    }
}
