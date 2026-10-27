//! The shutdown sequence, after the lifecycle task has moved to stopping:
//! the Connection owner stops, which ends every task through the
//! termination procedure and answers once each task's processes are
//! confirmed gone, then the listener stops, the socket is removed and the
//! lock is released, in that order, so that no client reaches a socket
//! whose root another App Server may already own. The sequence waits for
//! the Connection owner and the local API's handlers only until its
//! deadline; past it, it still removes the socket and releases the lock,
//! and the records of unconfirmed tasks stay for the next start. Each
//! reading of the clock for the deadline is the test seam counter
//! `shutdown.checked`, and the end of the wait for the Connection owner is
//! `shutdown.waiting`.

use std::time::Duration;

use connection::Connection;

use crate::lifecycle::{Lifecycle, Listener};
use crate::startup::SocketOwnership;

/// The time after the trigger at which the shutdown stops waiting.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

pub(crate) struct Sequence {
    /// The seam clock's time at which waiting stops.
    pub deadline: Duration,
    pub ownership: SocketOwnership,
    pub connection: Connection,
    pub listener: Option<Listener>,
}

pub(crate) async fn run(sequence: Sequence, lifecycle: Lifecycle) {
    tokio::select! {
        () = sequence.connection.stop() => {}
        () = state::seams::sleep_until_counted(sequence.deadline, "shutdown.checked") => {}
    }

    if let Some(mut listener) = sequence.listener {
        let _ = listener.stop.send(());
        state::seams::count("shutdown.waiting");
        tokio::select! {
            _ = &mut listener.served => {}
            () = state::seams::sleep_until_counted(sequence.deadline, "shutdown.checked") => {
                listener.served.abort();
                let _ = listener.served.await;
            }
        }
    }
    drop(sequence.ownership);
    lifecycle.finished(sequence.deadline);
}
