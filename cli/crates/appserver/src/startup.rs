//! The start of `server run` after the command's own checks, in this order:
//! the state-root lock, the removal of interrupted replacements, the
//! configuration read under the lock, the entries of `T`
//! deleted, the records an earlier App Server left in `execution/` read, the
//! socket directory `S`, and the socket. A failure at any step leaves the
//! steps before it and touches nothing after it; without the lock, nothing
//! in `S` is touched, since the lock is what proves that no other App
//! Server serves the root's socket.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use execution::cleanup::{Check, Leftovers};
use protocol::connection::Capabilities;
use rustix::fs::{AtFlags, CWD, Timespec, Timestamps, UTIME_NOW, UTIME_OMIT};

use state::files::{self, IoFailure};
use state::paths::{self, StateRoot};
use state::private::{self, CheckError};
use tokio::net::UnixListener;

use crate::capabilities;
use crate::configuration::{self, Configuration, ConfigurationError};
use crate::lock::{self, LockError, RootLock};

/// What a started App Server holds until it exits.
#[derive(Debug)]
pub struct Started {
    pub configuration: Configuration,
    pub socket: PathBuf,
    pub listener: UnixListener,
    pub ownership: SocketOwnership,
    pub capabilities: Capabilities,
    /// The running executable resolved once into its retained version directory.
    /// Capability checks, directory probes and tool launches share this path.
    pub tetral: PathBuf,
    /// The state root as `TETRAL_HOME` gives it.
    pub root: PathBuf,
    /// The state root's canonical path.
    pub canonical_root: PathBuf,
    /// The records an earlier App Server left, read before the first
    /// connection, and which Sessions they keep `cleanup_pending`.
    pub leftovers: (Check, Leftovers),
}

/// Owns the socket name for exactly as long as its root lock is held.
/// The listener must be closed before this value is dropped.
#[derive(Debug)]
pub struct SocketOwnership {
    socket: Option<PathBuf>,
    _lock: RootLock,
}

impl SocketOwnership {
    pub(crate) fn new(lock: RootLock) -> Self {
        Self {
            socket: None,
            _lock: lock,
        }
    }
}

impl Drop for SocketOwnership {
    fn drop(&mut self) {
        if let Some(socket) = self.socket.take() {
            if let Err(error) = std::fs::remove_file(&socket) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    observe::warn!(
                        "server.socket_cleanup_failed",
                        error.os_code = error.raw_os_error().unwrap_or(0),
                        error.code = observe::Code("socket_cleanup_failed")
                    );
                }
            }
        }
        // The lock field is dropped only after this cleanup returns.
        state::seams::barrier_blocking("shutdown.socket_removed");
    }
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
    #[error("cannot resolve the running executable: {0}")]
    Executable(std::io::Error),
    #[cfg(feature = "test-seams")]
    #[error("{0}")]
    TestToolExecution(String),
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

/// The steps after the lock: the configuration and the capabilities;
/// `T` emptied of its tasks' temporary directories and the records
/// of `execution/` read, only once both are valid, so that a start that
/// fails on them leaves the root as it was; then `S` and the socket, bound
/// with mode `0600` before it accepts a connection.
pub fn serve(root: &StateRoot, lock: RootLock) -> Result<Started, StartError> {
    let configuration = configuration::read(root)?;
    let tetral = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(StartError::Executable)?;
    let capabilities = capabilities::assemble(&tetral)?;
    let canonical_root =
        std::fs::canonicalize(root.path()).map_err(|error| IoFailure::new(root.path(), error))?;
    observe::STARTUP.config_loaded(state::seams::now());
    private::check_canonical_root_utf8(&canonical_root)?;
    execution::store::clear_task_parent(&execution::store::task_parent(&canonical_root));
    let leftovers = execution::cleanup::read(&canonical_root)?;

    private::create_socket_dir()?;
    let socket = paths::socket_path(root)
        .map_err(|error| IoFailure::new(root.path(), error))?
        .ok_or_else(|| IoFailure::new(root.path(), std::io::ErrorKind::NotFound))?;
    match std::fs::remove_file(&socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(IoFailure::new(&socket, error).into()),
    }
    let mut ownership = SocketOwnership::new(lock);
    let listener = UnixListener::bind(&socket).map_err(|error| IoFailure::new(&socket, error))?;
    ownership.socket = Some(socket.clone());
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| IoFailure::new(&socket, error))?;
    Ok(Started {
        ownership,
        configuration,
        socket,
        listener,
        capabilities,
        tetral,
        root: root.path().to_path_buf(),
        canonical_root,
        leftovers,
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
