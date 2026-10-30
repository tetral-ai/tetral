//! Commits of a Session's drive mounts: which changes are due, how a commit
//! is built and split, its answers, `409 content_missing`, faults, and the
//! drive winning over a rejected change. Each mount has at most one commit
//! in flight; its changes are recorded in `inflight_commits` before the
//! first sending, and an App Server start sends a stored commit again,
//! unchanged, before any other commit of its mount.

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use protocol::drive_transfer::limits;
use protocol::drive_transfer::{CommitChange, CommitResponse, CommitResult, CurrentVersion};
use protocol::field::{AbsolutePath, NonEmptyString, Nullable, Sha256};
use protocol::id::Id;
use protocol::tool::{ChangeKind, DriveRejection, RejectionReason};

use crate::state::{self, InflightChange, Key, PendingChange, Tables};
use crate::sync::{Message, Owner, now_ns};
use crate::transfer::{Commit, Ended, Request};

/// The most changes of one commit.
pub const MAX_CHANGES: usize = limits::COMMIT_MAX_CHANGES;

/// The most bytes of one commit's compact JSON body.
pub const MAX_BYTES: usize = limits::COMMIT_REQUEST_MAX_JSON_BYTES;

/// A drive mount's commit in flight.
pub(crate) struct Flight {
    pub id: u64,
    pub changes: Vec<CommitChange>,
    /// The sync point whose commit step sent it.
    pub sync_point: Option<u64>,
    /// When the request's deadline passes, for a sync point waiting on it.
    pub deadline: Duration,
}

/// A rejected change for a result or a drive sync report.
pub(crate) struct Rejected {
    pub key: Key,
    pub entry: DriveRejection,
}

/// The commit change of a pending change, when it can be sent.
pub(crate) fn change_of(change: &PendingChange) -> Option<CommitChange> {
    match change.change.as_str() {
        "write" => Some(CommitChange::Write {
            path: change.path.clone(),
            base_sha256: Nullable(match &change.base_sha256 {
                Some(base) => Some(Sha256::new(base.clone()).ok()?),
                None => None,
            }),
            content_sha256: Sha256::new(change.content_sha256.clone()?).ok()?,
            executable: change.executable.unwrap_or(false),
        }),
        _ => Some(CommitChange::Delete {
            path: change.path.clone(),
            base_sha256: Sha256::new(change.base_sha256.clone()?).ok()?,
        }),
    }
}

fn change_of_inflight(row: &InflightChange) -> Option<CommitChange> {
    change_of(&PendingChange {
        drive_id: row.drive_id.clone(),
        path: row.path.clone(),
        change: row.change.clone(),
        base_sha256: row.base_sha256.clone(),
        content_sha256: row.content_sha256.clone(),
        executable: row.executable,
        uploaded: true,
        last_write_at: 0,
        task_id: None,
        fault_code: None,
    })
}

/// Deletions first, then writes, each in UTF-8 byte order of path.
pub fn ordered(mut changes: Vec<CommitChange>) -> Vec<CommitChange> {
    changes.sort_by(|a, b| {
        let rank = |change: &CommitChange| matches!(change, CommitChange::Write { .. });
        (rank(a), a.path().as_bytes()).cmp(&(rank(b), b.path().as_bytes()))
    });
    changes
}

/// The compact JSON bytes of a commit body with no changes, for the binding
/// `connection_id`, `binding_id`.
pub fn body_base_bytes(connection_id: &str, binding_id: &str) -> usize {
    serde_json::json!({
        "protocol_version": 1,
        "connection_id": connection_id,
        "binding_id": binding_id,
        "changes": [],
    })
    .to_string()
    .len()
}

/// The first commit of `changes`, in order: the next changes while it holds
/// at most [`MAX_CHANGES`] and its body at most [`MAX_BYTES`].
pub fn first_commit(changes: &[CommitChange], base_bytes: usize) -> usize {
    let mut bytes = base_bytes;
    let mut count = 0;
    for change in changes {
        let size = serde_json::to_vec(change).map_or(usize::MAX, |json| json.len());
        let added = size + usize::from(count > 0);
        if count == MAX_CHANGES || bytes + added > MAX_BYTES {
            break;
        }
        bytes += added;
        count += 1;
    }
    count.max(1)
}

impl Owner {
    /// Whether the pending change `change` goes into the next commit of its
    /// mount; `covered` holds the changes a sync point commits.
    fn due(&self, change: &PendingChange, covered: &HashSet<Key>) -> bool {
        let key = Owner::key(&change.drive_id, &change.path);
        let ready = match change.change.as_str() {
            "write" => change.uploaded && change.content_sha256.is_some(),
            _ => change.base_sha256.is_some(),
        };
        if !ready || change.fault_code.is_some() {
            return false;
        }
        change
            .task_id
            .as_ref()
            .is_some_and(|task| self.reported_tasks.contains(task))
            || self.listed.contains(&key)
            || self.startup_due.contains(&key)
            || covered.contains(&key)
    }

    /// Starts the next commit of every mount with none in flight and a due
    /// change.
    pub(crate) async fn changes_ready(&mut self) {
        let drives: BTreeSet<String> = self
            .state
            .tables()
            .pending_changes
            .keys()
            .map(|(drive, _)| drive.clone())
            .collect();
        for drive in drives {
            // A sync point in its wait commits the mount's changes after it.
            let held = self
                .sync_points
                .values()
                .any(|point| point.waits_for(&drive));
            if !self.flights.contains_key(&drive) && !held {
                self.send_due(&drive, None, None, &HashSet::new()).await;
            }
        }
    }

    /// Sends the next commit of `drive` from its due changes; whether one
    /// was sent.
    pub(crate) async fn send_due(
        &mut self,
        drive: &str,
        sync_point: Option<u64>,
        traceparent: Option<String>,
        covered: &HashSet<Key>,
    ) -> bool {
        if self.flights.contains_key(drive) {
            return false;
        }
        let due: Vec<CommitChange> = self
            .state
            .tables()
            .pending_changes
            .iter()
            .filter(|((each, _), change)| each == drive && self.due(change, covered))
            .filter_map(|(_, change)| change_of(change))
            .collect();
        if due.is_empty() {
            return false;
        }
        let due = ordered(due);
        // The body's size depends on the binding it carries.
        let Some(binding) = self.binding.borrow().clone() else {
            return false;
        };
        let base = body_base_bytes(binding.connection_id.as_str(), binding.binding_id.as_str());
        let count = first_commit(&due, base);
        let changes: Vec<CommitChange> = due.into_iter().take(count).collect();
        let tables = self.state.tables();
        let inflight: Vec<InflightChange> = changes
            .iter()
            .filter_map(|change| {
                let pending = tables
                    .pending_changes
                    .get(&Owner::key(drive, change.path()))?;
                Some(InflightChange {
                    drive_id: drive.to_owned(),
                    path: pending.path.clone(),
                    change: pending.change.clone(),
                    base_sha256: pending.base_sha256.clone(),
                    content_sha256: pending.content_sha256.clone(),
                    executable: pending.executable,
                    changed_during_flight: false,
                })
            })
            .collect();
        let recorded = self
            .write(state::Write {
                inflight_removed_drives: vec![drive.to_owned()],
                inflight,
                ..state::Write::default()
            })
            .await;
        if recorded.is_err() {
            return false;
        }
        for change in &changes {
            let key = Owner::key(drive, change.path());
            self.startup_due.remove(&key);
        }
        self.send_commit(drive, changes, sync_point, traceparent);
        true
    }

    /// Sends `changes` as the commit of `drive`, which has none in flight.
    pub(crate) fn send_commit(
        &mut self,
        drive: &str,
        changes: Vec<CommitChange>,
        sync_point: Option<u64>,
        traceparent: Option<String>,
    ) {
        let Ok(drive_id) = Id::new(drive) else {
            return;
        };
        self.flight_ids += 1;
        let id = self.flight_ids;
        let bytes = serde_json::to_vec(&changes).map_or(0, |json| json.len());
        let request = Request {
            drive_id,
            binding: self.binding.subscribe(),
            traceparent,
            body: Commit {
                changes: changes.clone(),
            },
        };
        let (transfer, mailbox) = (self.transfer.clone(), self.mailbox.clone());
        let drive_key = drive.to_owned();
        tokio::spawn(async move {
            let answer = transfer.commit(request).await;
            let _ = mailbox.send(Message::CommitAnswered {
                drive: drive_key,
                id,
                answer,
            });
        });
        self.flights.insert(
            drive.to_owned(),
            Flight {
                id,
                changes,
                sync_point,
                deadline: self.clock.now() + crate::transfer::total(bytes as u64),
            },
        );
    }

    /// At start: each mount's stored commit is sent again, unchanged,
    /// before any other.
    pub(crate) fn resend_stored(&mut self) {
        let drives: BTreeSet<String> = self
            .state
            .tables()
            .inflight_commits
            .keys()
            .map(|(drive, _)| drive.clone())
            .collect();
        for drive in drives {
            let changes: Vec<CommitChange> = self
                .state
                .tables()
                .inflight_commits
                .iter()
                .filter(|((each, _), _)| *each == drive)
                .filter_map(|(_, row)| change_of_inflight(row))
                .collect();
            if !changes.is_empty() {
                self.send_commit(&drive, ordered(changes), None, None);
            }
        }
    }

    /// The answer of `drive`'s commit `id`.
    pub(crate) async fn commit_answered(
        &mut self,
        drive: String,
        id: u64,
        answer: Result<CommitResponse, Ended>,
    ) {
        let Some(flight) = self.flights.remove(&drive).filter(|flight| flight.id == id) else {
            return;
        };
        let mut resend = None;
        match answer {
            Ok(response) => self.apply_results(&drive, &flight, &response).await,
            Err(Ended::Decision(body)) if body.missing_sha256.is_some() => {
                let missing: HashSet<String> = body
                    .missing_sha256
                    .iter()
                    .flat_map(|hashes| hashes.iter().map(|hash| hash.as_str().to_owned()))
                    .collect();
                self.commit_lost(flight.sync_point, &drive, &flight.changes);
                resend = self.content_missing(&drive, &flight, &missing).await;
            }
            Err(ended) => {
                let (status, code) = match ended {
                    Ended::Permanent { status, code } => {
                        (status, code.unwrap_or("invalid_response"))
                    }
                    Ended::Decision(body) => (
                        body.code.status(),
                        crate::upload::declared(body.code.as_str()),
                    ),
                    _ => (0, "invalid_response"),
                };
                let _ = self
                    .write(state::Write {
                        inflight_removed_drives: vec![drive.clone()],
                        ..state::Write::default()
                    })
                    .await;
                for change in &flight.changes {
                    let key = Owner::key(&drive, change.path());
                    self.fault(&key, "commit", status, code).await;
                }
            }
        }
        self.recorded_during_flight
            .retain(|(each, _)| *each != drive);
        self.commit_ended(&drive, flight.sync_point, resend).await;
    }

    /// `409 content_missing`: the writes naming a missing hash upload
    /// again; the other changes are the commit to send again.
    async fn content_missing(
        &mut self,
        drive: &str,
        flight: &Flight,
        missing: &HashSet<String>,
    ) -> Option<Vec<CommitChange>> {
        let mut after = self.state.tables().clone();
        let mut reupload = Vec::new();
        for change in &flight.changes {
            if let CommitChange::Write { content_sha256, .. } = change
                && missing.contains(content_sha256.as_str())
            {
                let key = Owner::key(drive, change.path());
                after.inflight_commits.remove(&key);
                if let Some(pending) = after.pending_changes.get_mut(&key) {
                    pending.uploaded = false;
                    pending.content_sha256 = None;
                }
                self.uploads
                    .put
                    .remove(&(drive.to_owned(), content_sha256.as_str().to_owned()));
                reupload.push(key);
            }
        }
        let remaining: Vec<CommitChange> = flight
            .changes
            .iter()
            .filter(|change| {
                !matches!(change, CommitChange::Write { content_sha256, .. }
                    if missing.contains(content_sha256.as_str()))
            })
            .cloned()
            .collect();
        if remaining.is_empty() {
            after.inflight_commits.retain(|(each, _), _| each != drive);
        }
        let write = state::diff(self.state.tables(), &after);
        let _ = self.write(write).await;
        for key in &reupload {
            self.start_upload(key, None);
        }
        (!remaining.is_empty()).then_some(remaining)
    }

    /// Applies a commit's results in one transaction.
    async fn apply_results(&mut self, drive: &str, flight: &Flight, response: &CommitResponse) {
        let before = self.state.tables().clone();
        let mut after = before.clone();
        after.inflight_commits.retain(|(each, _), _| each != drive);
        let mut adopt = Vec::new();
        let mut restore = Vec::new();
        let mut rejected = Vec::new();
        let mut accepted = Vec::new();
        for (change, result) in flight.changes.iter().zip(response.results.iter()) {
            let key = Owner::key(drive, change.path());
            match result {
                CommitResult::Accepted { version_id } => {
                    accepted.push((key.clone(), change.clone()));
                    self.accepted(
                        &before,
                        &mut after,
                        &key,
                        change,
                        version_id.0.as_ref(),
                        &mut adopt,
                    );
                }
                CommitResult::Rejected {
                    reason,
                    rule,
                    current,
                } => {
                    let recorded = self.recorded_during_flight.contains(&key);
                    if let Some(row) = after.files.get_mut(&key)
                        && !recorded
                    {
                        set_current(row, current.as_ref());
                    }
                    after.pending_changes.remove(&key);
                    restore.push(key.1.clone());
                    rejected.push(Rejected {
                        key: key.clone(),
                        entry: self.rejection(
                            &key,
                            change,
                            *reason,
                            rule.clone(),
                            current.as_ref(),
                        ),
                    });
                }
            }
        }
        if !restore.is_empty() {
            let restored = crate::catchup::restore(&after, drive, &restore, now_ns());
            after.apply(restored);
        }
        let removed = removed_content(drive, &before, &after);
        let write = state::diff(&before, &after);
        if self.write(write).await.is_err() {
            return;
        }
        for file_id in removed {
            let _ = std::fs::remove_file(self.content_path(drive, file_id));
        }
        for (path, sha256) in adopt {
            if let Ok(sha256) = Sha256::new(sha256) {
                let _ = self.cache.adopt(path, sha256).await;
            }
        }
        if let Some(mounted) = self.mounted.get(drive) {
            for path in &restore {
                if let Some(row) = self.state.tables().files.get(&Owner::key(drive, path)) {
                    mounted.invalidate(row.file_id as u64);
                }
            }
        }
        self.commit_accepted(flight.sync_point, &accepted);
        for rejection in rejected {
            self.rejected(flight.sync_point, rejection);
        }
    }

    /// An accepted change: the drive-version columns take the committed
    /// version; the pending row goes unless the path changed during the
    /// flight, and then takes the base [`crate::local::accepted_in_flight`]
    /// gives it from `before`.
    fn accepted(
        &self,
        before: &Tables,
        after: &mut Tables,
        key: &Key,
        change: &CommitChange,
        version_id: Option<&Id>,
        adopt: &mut Vec<(std::path::PathBuf, String)>,
    ) {
        let changed = before
            .inflight_commits
            .get(key)
            .is_some_and(|row| row.changed_during_flight);
        match change {
            CommitChange::Write {
                content_sha256,
                executable,
                ..
            } => {
                let sha256 = content_sha256.as_str().to_owned();
                if let Some(row) = after.files.get_mut(key) {
                    row.version_id = version_id.map(|id| id.as_str().to_owned());
                    row.synced_sha256 = Some(sha256.clone());
                    row.synced_size = self.uploads.sizes.get(key).copied().or(row.size);
                    row.synced_executable = Some(*executable);
                    if !changed && row.present && row.content.as_deref() == Some("session") {
                        adopt.push((self.content_path(&key.0, row.file_id), sha256.clone()));
                        row.content = Some("cache".to_owned());
                        row.local_sha256 = Some(sha256.clone());
                    }
                }
            }
            CommitChange::Delete { .. } => {
                if let Some(row) = after.files.get_mut(key) {
                    row.version_id = None;
                    row.synced_sha256 = None;
                    row.synced_size = None;
                    row.synced_executable = None;
                    if !row.present {
                        after.files.remove(key);
                    }
                }
            }
        }
        if changed {
            after.apply(crate::local::accepted_in_flight(before, &key.0, &key.1));
        } else {
            after.pending_changes.remove(key);
        }
    }

    pub(crate) fn rejection(
        &self,
        key: &Key,
        change: &CommitChange,
        reason: RejectionReason,
        rule: Option<NonEmptyString>,
        current: Option<&CurrentVersion>,
    ) -> DriveRejection {
        let conflict = reason == RejectionReason::Conflict;
        DriveRejection {
            path: AbsolutePath::new(
                self.machine_path(&key.0, &key.1)
                    .to_string_lossy()
                    .into_owned(),
            )
            .expect("a machine path is absolute"),
            change: match change {
                CommitChange::Write { .. } => ChangeKind::Write,
                CommitChange::Delete { .. } => ChangeKind::Delete,
            },
            reason,
            rule,
            current_version_id: current
                .filter(|_| conflict)
                .map(|current| current.version_id.clone()),
            current_writer: current
                .filter(|_| conflict)
                .map(|current| current.writer.clone()),
        }
    }

    /// `413 file_too_large` for an upload: a `limit` rejection; the
    /// drive-version columns stay and the local entry takes them.
    pub(crate) async fn reject_limit(
        &mut self,
        key: &Key,
        change: &PendingChange,
        message: String,
    ) {
        let before = self.state.tables().clone();
        let mut after = before.clone();
        after.pending_changes.remove(key);
        let restored =
            crate::catchup::restore(&after, &key.0, std::slice::from_ref(&key.1), now_ns());
        after.apply(restored);
        let removed = removed_content(&key.0, &before, &after);
        if self.write(state::diff(&before, &after)).await.is_err() {
            return;
        }
        for file_id in removed {
            let _ = std::fs::remove_file(self.content_path(&key.0, file_id));
        }
        let Some(commit_change) = change_of(&PendingChange {
            content_sha256: Some("0".repeat(64)),
            ..change.clone()
        }) else {
            return;
        };
        let entry = self.rejection(
            key,
            &commit_change,
            RejectionReason::Limit,
            NonEmptyString::new(message).ok(),
            None,
        );
        self.rejected(
            None,
            Rejected {
                key: key.clone(),
                entry,
            },
        );
    }
}

/// The drive-version columns of a rejected change's path: `current`, or
/// none when it is absent or a deletion.
fn set_current(row: &mut crate::state::FileRow, current: Option<&CurrentVersion>) {
    let held = current.filter(|current| current.content_sha256.0.is_some());
    row.version_id = held.map(|current| current.version_id.as_str().to_owned());
    row.synced_sha256 = held.and_then(|current| {
        current
            .content_sha256
            .0
            .as_ref()
            .map(|hash| hash.as_str().to_owned())
    });
    row.synced_size = held.map(|current| current.size_bytes.get());
    row.synced_executable = held.map(|current| current.executable);
}

/// The `file_id`s of the content files of `drive` that `after` no longer
/// names.
fn removed_content(drive: &str, before: &Tables, after: &Tables) -> Vec<i64> {
    before
        .files
        .iter()
        .filter(|((each, _), row)| each == drive && row.content.as_deref() == Some("session"))
        .filter(|(key, row)| {
            !after.files.get(*key).is_some_and(|now| {
                now.file_id == row.file_id && now.content.as_deref() == Some("session")
            })
        })
        .map(|(_, row)| row.file_id)
        .collect()
}
