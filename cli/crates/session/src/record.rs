//! A Session record: what this machine keeps about one Session, from the
//! first `session.prepare` handled for it until a close deletes its local
//! data. It holds at most one binding of the current connection, which a new
//! binding replaces only once it has `failed`, and the binding the Session
//! had on an ended connection, `unavailable` until a new binding replaces it.

use protocol::connection::{PreparationError, Repository, RepositoryFailure};
use protocol::field::{AbsolutePath, Nullable};
use protocol::id::{Id, SafeId};
use protocol::local::{SessionEntry, SessionState};
use protocol::ports::ReportId;

use crate::tools::Tasks;

#[derive(Debug, Default)]
pub struct Record {
    pub current: Option<Binding>,
    pub unavailable: Option<Binding>,
}

#[derive(Debug)]
pub struct Binding {
    pub binding_id: Id,
    pub connection_id: Id,
    pub requested_cwd: AbsolutePath,
    pub create_cwd: bool,
    /// The canonical working directory, once a probe found it.
    pub cwd: Option<AbsolutePath>,
    pub state: SessionState,
    pub error: Option<PreparationError>,
    /// The preparation payload's content fingerprint.
    pub fingerprint: String,
    /// The preparation operation; a result of another one is discarded.
    pub operation: u64,
    /// The instructions that wait for the running preparation's result: the
    /// first and every repetition that joined it.
    pub waiting: Vec<Requester>,
    /// The reports of this binding's result that wait for their ACK.
    pub reports: Vec<ReportId>,
    /// The repositories the preparation clones, and those that failed.
    pub repositories: Vec<Repository>,
    pub repository_failures: Vec<RepositoryFailure>,
    pub tasks: Tasks,
}

/// A `session.prepare` that a report answers.
#[derive(Debug, Clone)]
pub struct Requester {
    pub message_id: Id,
    pub traceparent: Option<String>,
}

impl Record {
    /// The record's bindings as `GET /v1/sessions` lists them.
    pub fn entries(&self, session_id: &SafeId) -> Vec<SessionEntry> {
        [&self.current, &self.unavailable]
            .into_iter()
            .flatten()
            .map(|binding| SessionEntry {
                session_id: session_id.to_id(),
                binding_id: binding.binding_id.clone(),
                connection_id: binding.connection_id.clone(),
                cwd: Nullable(binding.cwd.clone()),
                state: binding.state,
                error: Nullable(binding.error.clone()),
            })
            .collect()
    }
}
