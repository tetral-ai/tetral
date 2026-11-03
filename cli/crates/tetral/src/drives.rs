//! `tetral drives enable`: it runs only while no App Server owns the root.
//! On Linux, where drives need no consent, it then succeeds with `{}` and
//! changes nothing.

use appserver::lock::{self, LockError};
use serde_json::{Value, json};
use state::paths::{self, StateRoot};

use crate::commands::io_failed;
use crate::output::{Code, CommandError};

pub fn enable(root: &StateRoot) -> Result<Value, CommandError> {
    // A root without `run/` has no App Server and no lock to take.
    if root.join(paths::RUN).is_dir() {
        let _lock = lock::acquire(root).map_err(|error| match error {
            LockError::Held => CommandError::new(
                Code::ServerRunning,
                "an App Server or another command owns the state root",
            ),
            LockError::Io(failure) => io_failed(failure),
        })?;
    }
    Ok(json!({}))
}
