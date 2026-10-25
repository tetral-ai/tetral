//! The shutdown sequence, after the lifecycle task has moved to stopping:
//! the connection step, then the listener stops, the socket is removed and
//! the lock is released, in that order, so that no client reaches a socket
//! whose root another App Server may already own. The sequence waits for the
//! local API's handlers only until its deadline; past it, it still removes
//! the socket and releases the lock.

use std::path::PathBuf;
use std::time::Duration;

use crate::lifecycle::{Lifecycle, Listener};
use crate::lock::RootLock;

/// The time after the trigger at which the shutdown stops waiting.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

pub(crate) struct Sequence {
    /// The seam clock's time at which waiting stops.
    pub deadline: Duration,
    pub socket: PathBuf,
    pub lock: RootLock,
    pub listener: Option<Listener>,
}

pub(crate) async fn run(sequence: Sequence, lifecycle: Lifecycle) {
    lifecycle.connection_stopped();

    if let Some(listener) = sequence.listener {
        let _ = listener.stop.send(());
        tokio::select! {
            _ = listener.served => {}
            () = state::seams::sleep_until(sequence.deadline) => {}
        }
    }
    let _ = std::fs::remove_file(&sequence.socket);
    drop(sequence.lock);
    lifecycle.finished();
}
