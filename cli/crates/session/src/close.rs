//! A `session.close` and the removal of the Session's local data.
//!
//! An ordinary close names the current binding on its connection: the
//! binding admits nothing more and its tasks end (step 1 and 2); once every
//! managed descendant of the binding is gone and a `session.drives` being
//! handled has sent its report (step 3), the final upload of every bound
//! drive runs until 30 seconds after the close was admitted (step 4), and
//! without an open drive sync every pending change of `sync-state.db` is
//! abandoned instead; then the drives are unmounted and the clones, the
//! tasks' temporary
//! directories and `sessions/<session_id>/` are deleted (step 5), and the
//! `closed` report goes out (step 6). A late close, for a Session with no
//! binding of the close's connection, waits for every descendant of the
//! Session, abandons every pending change without uploading, and deletes
//! the same data. A close of a binding that a new binding of the
//! connection replaced is answered at once.
//!
//! A repeated close joins the close in progress; one after the close
//! completed on the same connection gets the saved report again. The
//! connection's end before the report is built stops an ordinary close:
//! its final upload ends in keep mode, nothing is deleted and no report is
//! sent, and the late close that follows joins its wait. A saved report
//! is dropped when its connection ends.

use std::path::{Path, PathBuf};
use std::time::Duration;

use drive::final_upload::{FinalKeeper, FinalMode, FinalUploadRequest};
use drive::lost_changes;
use protocol::connection::{LostChange, SessionOutcome, SessionStatus};
use protocol::id::Id;
use protocol::ports::{Close, ReportId, Submission};

use crate::drives::DriveSyncSlot;
use crate::owner::{ReportEvents, SessionMessage, SessionOwner, TaskStarter};

/// How long the final upload of a close may run, counted from its admission.
pub const FINAL_UPLOAD_WAIT: Duration = Duration::from_secs(30);

/// A close in progress.
pub(crate) struct Closing {
    kind: Kind,
    /// The closes it answers, the first and those that joined it.
    requesters: Vec<Close>,
    /// The end of its final upload on the drive sync's clock, set at the
    /// admission of an ordinary close when the Session has a drive sync.
    end: Option<Duration>,
    step: Step,
    lost_changes: Vec<LostChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Ordinary {
        binding_id: Id,
        connection_id: Id,
    },
    Late,
    /// An ordinary close the connection's end stopped: it waits for the
    /// binding's descendants and reports nothing.
    Stopped,
}

enum Step {
    /// Waits for the descendants and a `session.drives` being handled.
    Waiting,
    Uploading(FinalKeeper),
    /// Its keep-mode end was asked for; the upload's end changes nothing.
    Keeping,
    Deleting,
}

/// The report of a completed close, kept for the closes repeated on its
/// connection.
pub(crate) struct Saved {
    connection_id: Id,
    binding_id: Id,
    lost_changes: Vec<LostChange>,
}

impl<S: TaskStarter> SessionOwner<S> {
    /// A `session.close`: joins the close in progress, answers from the
    /// saved report, or starts the close its binding calls for.
    pub(crate) fn close_admitted(&mut self, close: Close) {
        if let Some(closing) = self.closing.as_mut() {
            match closing.kind.clone() {
                Kind::Stopped => {
                    closing.kind = Kind::Late;
                    closing.requesters.push(close);
                }
                _ => closing.requesters.push(close),
            }
            return self.close_progress();
        }
        if let Some(saved) = self.saved.as_ref().filter(|saved| {
            saved.connection_id == close.connection_id && saved.binding_id == close.binding_id
        }) {
            let lost_changes = saved.lost_changes.clone();
            return self.report_closed(&close, &lost_changes);
        }
        let current = self
            .record
            .as_ref()
            .and_then(|record| record.current.as_ref())
            .filter(|binding| binding.connection_id == close.connection_id);
        let kind = match current {
            Some(binding) if binding.binding_id == close.binding_id => Kind::Ordinary {
                binding_id: binding.binding_id.clone(),
                connection_id: binding.connection_id.clone(),
            },
            // A new binding of this connection replaced the one it names.
            Some(_) => return self.report_closed(&close, &[]),
            None => Kind::Late,
        };
        if self.record.is_none() && self.last_operation == 0 {
            self.recordless = true;
        }
        let end = match (&kind, &self.drive_sync) {
            (Kind::Ordinary { .. }, DriveSyncSlot::Open(sync)) => {
                Some(sync.now() + FINAL_UPLOAD_WAIT)
            }
            _ => None,
        };
        if matches!(kind, Kind::Ordinary { .. }) {
            self.close_binding(&close);
        }
        self.closing = Some(Closing {
            kind,
            requesters: vec![close],
            end,
            step: Step::Waiting,
            lost_changes: Vec::new(),
        });
        self.close_progress();
    }

    /// Moves the close on as far as what it waits for allows.
    pub(crate) fn close_progress(&mut self) {
        let Some(closing) = self.closing.as_ref() else {
            return;
        };
        if !matches!(closing.step, Step::Waiting) || matches!(closing.kind, Kind::Stopped) {
            return;
        }
        if !self.descendants_gone(&closing.kind) {
            return;
        }
        match closing.kind.clone() {
            Kind::Ordinary { binding_id, .. } => self.final_upload(&binding_id),
            Kind::Late => self.delete(),
            Kind::Stopped => {}
        }
    }

    /// Whether every managed descendant the close waits for is gone: the
    /// tasks, probes and clones of the binding, or of the whole Session for
    /// a late close, and no `session.drives` of the binding is being
    /// handled.
    fn descendants_gone(&self, kind: &Kind) -> bool {
        let Some(record) = &self.record else {
            return self.probes.is_empty() && self.clones.is_empty() && self.unconfirmed.is_empty();
        };
        let bindings = [record.current.as_ref(), record.unavailable.as_ref()];
        let mut waited = bindings.into_iter().flatten().filter(|binding| match kind {
            Kind::Ordinary {
                binding_id,
                connection_id,
            } => binding.binding_id == *binding_id && binding.connection_id == *connection_id,
            _ => true,
        });
        let busy = waited.any(|binding| {
            !binding.tasks.all_gone()
                || binding.applying.is_some()
                || self.probes.contains_key(&binding.operation)
                || self.clones.contains_key(&binding.operation)
        });
        let session_wide = !matches!(kind, Kind::Ordinary { .. })
            && (!self.unconfirmed.is_empty() || !self.probes.is_empty() || !self.clones.is_empty());
        !busy && !session_wide
    }

    /// Step 4: the final upload of every drive the binding binds.
    fn final_upload(&mut self, binding_id: &Id) {
        let drives: Vec<Id> = self
            .record
            .as_ref()
            .and_then(|record| record.current.as_ref())
            .filter(|binding| binding.binding_id == *binding_id)
            .map(|binding| {
                binding
                    .drives
                    .iter()
                    .map(|drive| drive.drive_id.to_id())
                    .collect()
            })
            .unwrap_or_default();
        let (DriveSyncSlot::Open(sync), Some(preparation), Some(closing)) =
            (&self.drive_sync, &self.preparation, self.closing.as_mut())
        else {
            return self.delete();
        };
        if drives.is_empty() {
            return self.delete();
        }
        let Some(request_message_id) = closing
            .requesters
            .first()
            .map(|close| close.message_id.clone())
        else {
            return;
        };
        let mut upload = sync.final_upload(FinalUploadRequest {
            drives,
            end: closing
                .end
                .unwrap_or_else(|| sync.now() + FINAL_UPLOAD_WAIT),
            mode: FinalMode::Abandon {
                request_message_id,
                connection: preparation.connection.clone(),
            },
        });
        let mailbox = self.mailbox.clone();
        closing.step = Step::Uploading(upload.keeper());
        tokio::spawn(async move {
            let lost_changes = upload.ended().await;
            let _ = mailbox.send(SessionMessage::CloseUploaded(lost_changes));
        });
    }

    /// The close's final upload ended.
    pub(crate) fn close_uploaded(&mut self, lost: Vec<LostChange>) {
        let Some(closing) = self.closing.as_mut() else {
            return;
        };
        match closing.step {
            Step::Uploading(_) => {
                closing.lost_changes = lost;
                self.delete();
            }
            // The connection ended first: the close waits for a late close.
            Step::Keeping => closing.step = Step::Waiting,
            Step::Waiting | Step::Deleting => {}
        }
    }

    /// Step 5, and first the abandoning of every pending change for a late
    /// close, or for any close whose drive sync is not open.
    fn delete(&mut self) {
        let Some(closing) = self.closing.as_mut() else {
            return;
        };
        closing.step = Step::Deleting;
        let late = closing.kind == Kind::Late;
        let request_message_id = closing
            .requesters
            .first()
            .map(|close| close.message_id.clone());
        let Some(preparation) = &self.preparation else {
            return;
        };
        let connection = preparation.connection.clone();
        let session_id = preparation.session_id.clone();
        let root = preparation.shared.paths.canonical.clone();
        let registry = preparation
            .shared
            .drives
            .borrow()
            .as_ref()
            .map(|context| context.registry.clone());
        let sync = match std::mem::take(&mut self.drive_sync) {
            DriveSyncSlot::Open(sync) => Some(sync),
            _ => None,
        };
        let mailbox = self.mailbox.clone();
        tokio::spawn(async move {
            let directory = state::paths::session_directory(&root, session_id.as_str());
            let mut lost = Vec::new();
            if let Some(sync) = &sync {
                sync.detach_all();
                if late && let Some(request_message_id) = &request_message_id {
                    let pending = sync.tables().await.pending_changes;
                    lost = lost_changes::entries(request_message_id, pending.into_keys());
                }
                sync.close().await;
            } else if let Some(request_message_id) = &request_message_id
                && let Ok(state) =
                    drive::state::SyncState::open(&directory.join(drive::state::FILE_NAME)).await
            {
                let pending = state.tables().pending_changes.clone();
                lost = lost_changes::entries(request_message_id, pending.into_keys());
                state.close().await;
            }
            if !lost.is_empty() {
                let (kept, written) = tokio::sync::oneshot::channel();
                connection.keep_lost_changes(
                    lost.clone(),
                    Box::new(move || {
                        let _ = kept.send(());
                    }),
                );
                let _ = written.await;
            }
            if let Some(registry) = registry {
                registry.remove(session_id.clone());
            }
            let removal = directory.clone();
            let _ = tokio::task::spawn_blocking(move || remove_local_data(&root, &removal)).await;
            let _ = mailbox.send(SessionMessage::CloseDeleted(lost));
        });
    }

    /// Step 6: the `closed` report of every close it answers; the record
    /// goes.
    pub(crate) fn close_deleted(&mut self, late_lost: Vec<LostChange>) {
        let Some(closing) = self.closing.take() else {
            return;
        };
        let mut lost_changes = closing.lost_changes;
        lost_changes.extend(late_lost);
        for close in &closing.requesters {
            self.report_closed(close, &lost_changes);
        }
        if let Some(first) = closing.requesters.first() {
            self.saved = Some(Saved {
                connection_id: first.connection_id.clone(),
                binding_id: first.binding_id.clone(),
                lost_changes,
            });
        }
        self.record = None;
        if let Some(preparation) = &self.preparation {
            preparation.connection.record_gone(&preparation.session_id);
        }
    }

    /// The connection `connection` ended: an ordinary close of one of its
    /// bindings stops before its report, ending its final upload in keep
    /// mode.
    pub(crate) fn close_connection_ended(&mut self, connection: &Id) {
        // A saved report answers repeated closes of its connection only.
        if self
            .saved
            .as_ref()
            .is_some_and(|saved| saved.connection_id == *connection)
        {
            self.saved = None;
        }
        let Some(closing) = self.closing.as_mut() else {
            return;
        };
        if matches!(closing.step, Step::Deleting) {
            return;
        }
        closing
            .requesters
            .retain(|close| close.connection_id != *connection);
        let ours = matches!(&closing.kind,
            Kind::Ordinary { connection_id, .. } if connection_id == connection);
        if !ours {
            return;
        }
        closing.kind = Kind::Stopped;
        if let Step::Uploading(upload) = &closing.step {
            upload.keep_now();
            closing.step = Step::Keeping;
        }
    }

    /// The outcome of a record-less owner's `closed` report: once every
    /// one has its outcome and no close runs, the owner asks to end. False
    /// for another report.
    pub(crate) fn closed_report_outcome(&mut self, report: ReportId) -> bool {
        let Some(index) = self.closed_reports.iter().position(|each| *each == report) else {
            return false;
        };
        self.closed_reports.remove(index);
        if self.closed_reports.is_empty()
            && self.closing.is_none()
            && let Some(preparation) = &self.preparation
        {
            preparation
                .connection
                .owner_idle(&preparation.session_id, self.received);
        }
        true
    }

    fn report_closed(&mut self, close: &Close, lost: &[LostChange]) {
        let Some(preparation) = &self.preparation else {
            return;
        };
        let session_id = preparation.session_id.to_id();
        let (request_message_id, binding_id) = (close.message_id.clone(), close.binding_id.clone());
        let lost = lost.to_vec();
        let mut submission = Submission::new(
            close.connection_id.clone(),
            move |connection_id, message_id| {
                let status = |lost_changes| SessionStatus {
                    connection_id: connection_id.clone(),
                    message_id: message_id.clone(),
                    request_message_id: request_message_id.clone(),
                    session_id: session_id.clone(),
                    binding_id: binding_id.clone(),
                    outcome: SessionOutcome::Closed { lost_changes },
                };
                let fitted = lost_changes::fit(&lost, |candidate| {
                    protocol::encode(&status(candidate.cloned())).len()
                });
                status(fitted)
            },
        );
        submission.final_for = vec![close.message_id.clone()];
        if self.recordless {
            submission.listener = Some(std::sync::Arc::new(ReportEvents {
                mailbox: self.mailbox.clone(),
            }));
        }
        let report = preparation.reports.submit(submission);
        if self.recordless {
            self.closed_reports.push(report);
        }
    }
}

/// Deletes each path in `repositories.json`, then the temporary directory
/// of each task directory under the Session's `tasks/`, then the Session's
/// directory.
fn remove_local_data(root: &Path, directory: &Path) {
    for path in crate::repositories::listed(directory) {
        let _ = std::fs::remove_dir_all(path);
    }
    let parent = execution::store::task_parent(root);
    let tasks: Vec<PathBuf> = std::fs::read_dir(directory.join(execution::store::TASKS))
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default();
    for task in tasks {
        if let Some(name) = task.file_name().and_then(|name| name.to_str()) {
            let _ =
                std::fs::remove_dir_all(execution::store::task_temporary_directory(&parent, name));
        }
    }
    let _ = std::fs::remove_dir_all(directory);
}
