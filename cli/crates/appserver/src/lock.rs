//! The state-root lock: `run/app-server.lock`, held with `flock`, through
//! which one process at a time owns the root. `configure` and `server run`
//! take it without waiting, so the one that finds it held fails at once.

use std::fs::File;

use rustix::fs::FlockOperation;
use state::files::{self, IoFailure};
use state::paths::{self, StateRoot};

/// The held lock; dropping it releases the root.
#[derive(Debug)]
pub struct RootLock {
    _file: File,
}

#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("another process owns the state root")]
    Held,
    #[error(transparent)]
    Io(#[from] IoFailure),
}

/// Opens the lock file, creating it with mode `0600` when it is missing, and
/// takes it without waiting. `run/` must exist.
pub fn acquire(root: &StateRoot) -> Result<RootLock, LockError> {
    let path = root.join(paths::LOCK);
    let file = files::open_private(&path)?;
    match rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(RootLock { _file: file }),
        Err(rustix::io::Errno::WOULDBLOCK) => Err(LockError::Held),
        Err(error) => Err(IoFailure::new(&path, error).into()),
    }
}
