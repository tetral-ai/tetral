//! Durable asynchronous Drive failure notices. Settlement records them in the
//! Session database; current-binding attempts use the ordinary report budget.
//! Only a matching ACK removes notices. Refusal or Dropped retries after one
//! second, while computer removal deletes the database and stops the owner.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use protocol::field::ConstInt;
use protocol::id::{Id, SafeId};
use protocol::ports::{
    LONGEST_REPORT_MESSAGE_ID, ReportId, ReportListener, ReportOutcome, Submission,
};
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

/// Part of the same transaction that settles a rejected pending change.
pub(crate) fn retain(tables: &mut crate::state::Tables, rejection: &crate::commit::Rejected) {
    let id = tables
        .failure_notices
        .last_key_value()
        .map_or(1, |(id, _)| id + 1);
    tables.failure_notices.insert(
        id,
        crate::state::FailureNotice {
            id,
            task_id: rejection.task.clone(),
            rejection: rejection.entry.clone(),
        },
    );
}

#[derive(Default)]
pub(crate) struct Delivery {
    attempts: HashMap<ReportId, Vec<i64>>,
    retry_at: Option<Duration>,
    refused: bool,
}

struct Listener(tokio::sync::mpsc::UnboundedSender<crate::sync::Message>);
impl ReportListener for Listener {
    fn outcome(&self, id: ReportId, outcome: ReportOutcome) {
        let _ = self
            .0
            .send(crate::sync::Message::ReportOutcome(id, outcome));
    }
}

impl Owner {
    pub(crate) fn report_wake(&self) -> Option<Duration> {
        self.binding.borrow().as_ref()?;
        self.report_delivery.retry_at
    }

    /// Changing connections retires local associations; admitted bytes still
    /// belong to their existing outbox lifetime. New attempts use this binding.
    pub(crate) fn reports_rebound(&mut self) {
        self.report_delivery.attempts.clear();
        self.report_delivery.retry_at = None;
    }

    pub(crate) async fn report_outcome(&mut self, id: ReportId, outcome: ReportOutcome) {
        let Some(notices) = self.report_delivery.attempts.remove(&id) else {
            return;
        };
        if outcome == ReportOutcome::Acked {
            // A failed state write keeps the records for another attempt.
            let _ = self
                .write(crate::state::Write {
                    notices_removed: notices,
                    ..Default::default()
                })
                .await;
        }
        self.report_delivery.retry_at = Some(self.clock.now() + Duration::from_secs(1));
    }

    pub(crate) fn send_reports(&mut self) {
        let Some(binding) = self.binding.borrow().clone() else {
            return;
        };
        if self
            .report_delivery
            .retry_at
            .is_some_and(|at| self.clock.now() < at)
        {
            return;
        }
        self.report_delivery.retry_at = None;
        let sending: HashSet<i64> = self
            .report_delivery
            .attempts
            .values()
            .flatten()
            .copied()
            .collect();
        let mut groups: BTreeMap<Option<String>, Vec<&crate::state::FailureNotice>> =
            BTreeMap::new();
        for (id, notice) in &self.state.tables().failure_notices {
            if !sending.contains(id) {
                groups
                    .entry(notice.task_id.clone())
                    .or_default()
                    .push(notice);
            }
        }
        for (task, notices) in groups {
            let ids = notices.iter().map(|notice| notice.id).collect();
            let rejected = notices
                .iter()
                .map(|notice| notice.rejection.clone())
                .collect();
            let build = body(
                &binding,
                &self.session_id,
                task.and_then(|task| Id::new(task).ok()),
                rejected,
            );
            let mut submission = Submission::new(binding.connection_id.clone(), build);
            submission.listener = Some(Arc::new(Listener(self.mailbox.clone())));
            match self.reports.try_submit(submission) {
                Ok(id) => {
                    self.report_delivery.attempts.insert(id, ids);
                    self.report_delivery.refused = false;
                }
                Err(_) => {
                    if !self.report_delivery.refused {
                        observe::warn!(
                            "drive.report_deferred",
                            session.id = observe::Id(self.session_id.as_str()),
                            connection.id = observe::Id(binding.connection_id.as_str()),
                            error.code = observe::Code("resource_exhausted")
                        );
                    }
                    self.report_delivery.refused = true;
                    self.report_delivery.retry_at = Some(self.clock.now() + Duration::from_secs(1));
                    break;
                }
            }
        }
    }
}
