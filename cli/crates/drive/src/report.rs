//! Drive sync reports: the rejections that no sync point carries, grouped by
//! the task recorded on each change, one report per group for the Session's
//! current binding, fitted so that `drive_sync` stays within 65,536 bytes
//! and the report within 131,072. Each goes to the report outbox once, from
//! the ordinary budget; a refused report is released like one whose
//! connection ended.

use std::collections::BTreeMap;

use protocol::field::ConstInt;
use protocol::id::{Id, SafeId};
use protocol::ports::{LONGEST_REPORT_MESSAGE_ID, Submission};
use protocol::tool::{DriveRejection, DriveSyncOutcome, DriveSyncReport};

use crate::outcome::{self, Facts};
use crate::sync::Owner;
use crate::transfer::Binding;

/// The most bytes of a drive sync report's compact JSON.
pub const REPORT_MAX_BYTES: usize = protocol::tool::limits::DRIVE_SYNC_REPORT_JSON_BYTES;

/// The report body of `rejected` for `binding`, as the outbox names it.
pub fn body(
    binding: &Binding,
    session_id: &SafeId,
    task_id: Option<Id>,
    rejected: Vec<DriveRejection>,
) -> impl FnOnce(Id, Id) -> DriveSyncReport + Send + 'static {
    let (connection_id, binding_id) = (binding.connection_id.clone(), binding.binding_id.clone());
    let session_id = Id::new(session_id.as_str()).expect("a session_id is an Id");
    let report = move |drive_sync: DriveSyncOutcome, message_id: Id| DriveSyncReport {
        protocol_version: ConstInt,
        connection_id: connection_id.clone(),
        message_id,
        session_id: session_id.clone(),
        binding_id: binding_id.clone(),
        task_id: task_id.clone(),
        drive_sync,
    };
    // The room the envelope leaves, with the longest name the outbox gives.
    let longest = Id::new(LONGEST_REPORT_MESSAGE_ID).expect("an Id");
    let empty = empty_outcome();
    let envelope = size(&report(empty.clone(), longest)) - size(&empty);
    let room = outcome::MAX_BYTES.min(REPORT_MAX_BYTES.saturating_sub(envelope));
    let drive_sync = outcome::fit_within(
        Facts {
            rejected,
            ..Facts::default()
        },
        room,
    )
    .drive_sync
    .unwrap_or_else(empty_outcome);
    move |_connection, message_id| report(drive_sync, message_id)
}

fn empty_outcome() -> DriveSyncOutcome {
    outcome::fit(Facts {
        committed: 1,
        ..Facts::default()
    })
    .drive_sync
    .expect("a committed count")
}

fn size(value: &impl SizedJson) -> usize {
    value.json_len()
}

/// The compact JSON size of a report or of its `drive_sync`.
trait SizedJson {
    fn json_len(&self) -> usize;
}

impl SizedJson for DriveSyncReport {
    fn json_len(&self) -> usize {
        serde_json::to_vec(self).map_or(usize::MAX, |json| json.len())
    }
}

impl SizedJson for DriveSyncOutcome {
    fn json_len(&self) -> usize {
        serde_json::to_vec(self).map_or(usize::MAX, |json| json.len())
    }
}

impl Owner {
    /// Hands each group of the rejections no sync point carries to the
    /// outbox, once the Session has a current binding.
    pub(crate) fn send_reports(&mut self) {
        let Some(binding) = self.binding.borrow().clone() else {
            return;
        };
        if self.unreported.is_empty() {
            return;
        }
        let mut groups: BTreeMap<Option<String>, Vec<DriveRejection>> = BTreeMap::new();
        for rejection in std::mem::take(&mut self.unreported) {
            groups
                .entry(rejection.task)
                .or_default()
                .push(rejection.entry);
        }
        for (task, rejected) in groups {
            let task_id = task.and_then(|task| Id::new(task).ok());
            let build = body(&binding, &self.session_id, task_id, rejected);
            let submission = Submission::new(binding.connection_id.clone(), build);
            if self.reports.try_submit(submission).is_err() {
                observe::error!(
                    "report.dropped",
                    connection.id = observe::Id(binding.connection_id.as_str()),
                    error.code = observe::Code("resource_exhausted")
                );
            }
        }
    }
}
