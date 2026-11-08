//! The sync point of a call whose result is being built: it covers the
//! Session's pending changes, starts their uploads, waits for them within
//! its wait, commits per drive mount within each request's deadline and
//! gives the outcome. Its task's later changes become background writes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use protocol::drive_transfer::CommitChange;
use protocol::field::AbsolutePath;
use protocol::tool::{DriveNotSynced, DriveRejection, NotSyncedReason};
use tokio::sync::oneshot;

use crate::commit::Rejected;
use crate::state::Key;
use crate::sync::Owner;

/// What a sync point is asked for.
#[derive(Debug, Clone)]
pub struct SyncPointRequest {
    /// The task of the call.
    pub task_id: Option<String>,
    /// The caller's upload wait; zero for a running result.
    pub wait: Duration,
    /// The instruction's `traceparent`, which its uploads and commits carry.
    pub traceparent: Option<String>,
}

/// A sync point's outcome, before it is fitted into a result's
/// `drive_sync`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncOutcome {
    pub committed: u64,
    pub deleted: u64,
    pub rejected: Vec<DriveRejection>,
    /// `uploading` and `upload_pending` in path order, then the call's
    /// task's local-only notices and the Session notices captured at entry.
    pub not_synced: Vec<DriveNotSynced>,
}

/// A sync point in progress.
pub(crate) struct SyncPoint {
    task: Option<String>,
    taskless_notices: Vec<crate::ops::Notice>,
    traceparent: Option<String>,
    pub covered: HashSet<Key>,
    /// Ready generations frozen when the upload wait ends. A sent batch
    /// leaves this map, so rewrites and new background work cannot extend it.
    pub remaining: HashMap<Key, u64>,
    wait_end: Duration,
    /// Whether the wait ended and the commit step runs.
    committing: bool,
    /// Drives whose upload wait ended early for Session close.
    released: HashSet<String>,
    /// The drive mounts whose commit answers it waits for: the flight and
    /// the time it stops waiting.
    waiting: BTreeMap<String, (u64, Duration)>,
    committed: u64,
    deleted: u64,
    rejected: Vec<DriveRejection>,
    uploading: HashSet<Key>,
    pub faulted: HashSet<Key>,
    /// The commits sent again without their missing writes once the
    /// outcome is given.
    resend: Vec<(String, Vec<CommitChange>)>,
    reply: Option<oneshot::Sender<SyncOutcome>>,
}

impl SyncPoint {
    /// Whether it is in its wait and covers a change of `drive`.
    pub(crate) fn waits_for(&self, drive: &str) -> bool {
        !self.committing
            && !self.released.contains(drive)
            && self.covered.iter().any(|(each, _)| each == drive)
    }
}

impl Owner {
    /// Starts a sync point: covers the Session's pending changes, starts
    /// their uploads and begins the wait; the rest follows its progress.
    pub(crate) async fn sync_point(
        &mut self,
        request: SyncPointRequest,
        began: Duration,
        reply: oneshot::Sender<SyncOutcome>,
    ) {
        let taskless_notices = std::mem::take(&mut self.taskless_notices);
        let covered: HashSet<Key> = self
            .state
            .tables()
            .pending_changes
            .iter()
            .filter(|(key, change)| {
                let other_task = change.task_id.as_ref().is_some_and(|task| {
                    Some(task) != request.task_id.as_ref() && !self.reported_tasks.contains(task)
                });
                !self.uncovered.contains(*key) && !other_task
            })
            .map(|(key, _)| key.clone())
            .collect();
        self.retry_faults(Some(&covered)).await;
        let mut keys: Vec<&Key> = covered.iter().collect();
        keys.sort();
        for key in keys {
            self.start_upload(key, request.traceparent.clone());
        }
        self.sync_point_ids += 1;
        self.sync_points.insert(
            self.sync_point_ids,
            SyncPoint {
                task: request.task_id,
                taskless_notices,
                traceparent: request.traceparent,
                covered,
                remaining: HashMap::new(),
                wait_end: began + request.wait,
                committing: false,
                released: HashSet::new(),
                waiting: BTreeMap::new(),
                committed: 0,
                deleted: 0,
                rejected: Vec::new(),
                uploading: HashSet::new(),
                faulted: HashSet::new(),
                resend: Vec::new(),
                reply: Some(reply),
            },
        );
        // One can arrive during a Session removal's final upload, on a Mac
        // when its flush outlasts that upload's; the waits that upload's
        // start ended end now.
        match self.closing_drives().as_slice() {
            [] => self.sync_points_progress().await,
            closing => self.release_sync_waits(closing).await,
        }
    }

    /// Whether the covered change at `key` still waits for its content.
    fn uploading(&self, key: &Key) -> bool {
        self.state
            .tables()
            .pending_changes
            .get(key)
            .is_some_and(|change| {
                change.change == "write" && !change.uploaded && change.fault_code.is_none()
            })
    }

    /// Moves every sync point on whose wait or commits ended.
    pub(crate) async fn sync_points_progress(&mut self) {
        let now = self.clock.now();
        let ids: Vec<u64> = self.sync_points.keys().copied().collect();
        for id in ids {
            let Some(point) = self.sync_points.get(&id) else {
                continue;
            };
            if !point.committing {
                let waiting = point
                    .covered
                    .iter()
                    .any(|key| !point.released.contains(&key.0) && self.uploading(key));
                if !waiting || now >= point.wait_end {
                    self.commit_step(id).await;
                }
            }
            let Some(point) = self.sync_points.get_mut(&id) else {
                continue;
            };
            let lost: Vec<String> = point
                .waiting
                .iter()
                .filter(|(_, (_, until))| *until <= now)
                .map(|(drive, _)| drive.clone())
                .collect();
            for drive in lost {
                point.waiting.remove(&drive);
                if let Some(flight) = self.flights.get(&drive) {
                    point.uploading.extend(
                        flight
                            .changes
                            .iter()
                            .map(|change| Owner::key(&drive, change.path())),
                    );
                }
            }
            if point.committing && point.waiting.is_empty() {
                self.outcome(id).await;
            }
        }
    }

    /// Ends the wait: the covered changes still uploading are listed; each
    /// mount with a commit in flight lists its covered changes, and the
    /// others send their due changes.
    async fn commit_step(&mut self, id: u64) {
        let Some(point) = self.sync_points.get_mut(&id) else {
            return;
        };
        point.committing = true;
        let covered = point.covered.clone();
        let released = point.released.clone();
        let drives = covered
            .iter()
            .map(|(drive, _)| drive.clone())
            .chain(
                self.state
                    .tables()
                    .pending_changes
                    .iter()
                    .filter(|(_, change)| self.due(change, &covered))
                    .map(|(key, _)| key.0.clone()),
            )
            .filter(|drive| !released.contains(drive))
            .collect();
        self.commit_drives(id, drives).await;
    }

    /// Close ends only the named drives' upload wait. Other drives retain
    /// the point's original wait, counts and report lifetime.
    pub(crate) async fn release_sync_waits(&mut self, drives: &[protocol::id::Id]) {
        let ids: Vec<_> = self.sync_points.keys().copied().collect();
        for id in ids {
            let point = &self.sync_points[&id];
            let released = drives
                .iter()
                .filter(|drive| point.waits_for(drive.as_str()))
                .map(|drive| drive.as_str().to_owned())
                .collect();
            self.commit_drives(id, released).await;
        }
        self.sync_points_progress().await;
    }

    async fn commit_drives(&mut self, id: u64, drives: HashSet<String>) {
        let Some(point) = self.sync_points.get(&id) else {
            return;
        };
        let covered = point.covered.clone();
        let traceparent = point.traceparent.clone();
        let remaining: HashMap<_, _> = self
            .state
            .tables()
            .pending_changes
            .iter()
            .filter(|(key, change)| drives.contains(&key.0) && self.due(change, &covered))
            .map(|(key, _)| {
                (
                    key.clone(),
                    self.uploads.generations.get(key).copied().unwrap_or(0),
                )
            })
            .collect();
        let point = self.sync_points.get_mut(&id).unwrap();
        point.remaining.extend(remaining);
        point.released.extend(drives.iter().cloned());
        let still: Vec<Key> = covered
            .iter()
            .filter(|key| drives.contains(&key.0) && self.uploading(key))
            .cloned()
            .collect();
        let mut in_flight = Vec::new();
        for drive in &drives {
            if self.flights.contains_key(drive) {
                in_flight.extend(
                    covered
                        .iter()
                        .filter(|key| {
                            key.0 == *drive
                                && self.state.tables().pending_changes.contains_key(*key)
                        })
                        .cloned(),
                );
            } else if self
                .send_due(drive, Some(id), traceparent.clone(), &covered)
                .await
                && let Some(flight) = self.flights.get(drive)
                && let Some(point) = self.sync_points.get_mut(&id)
            {
                point
                    .waiting
                    .insert(drive.clone(), (flight.id, flight.deadline));
            }
        }
        if let Some(point) = self.sync_points.get_mut(&id) {
            point.uploading.extend(still);
            point.uploading.extend(in_flight);
        }
    }

    /// The commit of `drive` that the sync point `sync_point` sent was
    /// answered: its next due changes go, or it stops waiting for the mount.
    /// `resend` is a `409 content_missing` commit without its missing
    /// writes.
    pub(crate) async fn commit_ended(
        &mut self,
        drive: &str,
        sync_point: Option<u64>,
        resend: Option<Vec<CommitChange>>,
    ) {
        let point =
            sync_point.and_then(|id| self.sync_points.get_mut(&id).map(|point| (id, point)));
        match (resend, point) {
            (Some(changes), Some((_, point))) => {
                point.waiting.remove(drive);
                if !changes.is_empty() {
                    point.resend.push((drive.to_owned(), changes));
                }
            }
            (Some(changes), None) if !changes.is_empty() => {
                self.send_commit(drive, changes, None, None)
            }
            (None, Some((id, point))) if point.waiting.contains_key(drive) => {
                let (covered, traceparent) = (point.covered.clone(), point.traceparent.clone());
                let sent = self.send_due(drive, Some(id), traceparent, &covered).await;
                let next = self
                    .flights
                    .get(drive)
                    .map(|flight| (flight.id, flight.deadline));
                if let Some(point) = self.sync_points.get_mut(&id) {
                    match next.filter(|_| sent) {
                        Some(next) => {
                            point.waiting.insert(drive.to_owned(), next);
                        }
                        None => {
                            point.waiting.remove(drive);
                        }
                    }
                }
            }
            _ => {}
        }
        self.forget_settled();
        self.changes_ready().await;
        self.sync_points_progress().await;
    }

    /// The listings and due marks of paths with no pending change.
    pub(crate) fn forget_settled(&mut self) {
        let pending = &self.state.tables().pending_changes;
        self.listed.retain(|key| pending.contains_key(key));
        self.uncovered.retain(|key| pending.contains_key(key));
        self.startup_due.retain(|key| pending.contains_key(key));
    }

    /// A final upload cancelled `drive`'s commit in flight: a sync point
    /// waiting for its answer lists its changes `uploading`.
    pub(crate) fn commit_cancelled(&mut self, drive: &str, changes: &[CommitChange]) {
        for point in self.sync_points.values_mut() {
            if point.waiting.remove(drive).is_some() {
                point.uploading.extend(
                    changes
                        .iter()
                        .map(|change| Owner::key(drive, change.path())),
                );
            }
        }
    }

    /// Every change of a commit of `sync_point` whose answer was
    /// `409 content_missing` is listed `uploading`.
    pub(crate) fn commit_content_missing(
        &mut self,
        sync_point: Option<u64>,
        drive: &str,
        changes: &[CommitChange],
    ) {
        if let Some(point) = sync_point.and_then(|id| self.sync_points.get_mut(&id)) {
            point.uploading.extend(
                changes
                    .iter()
                    .map(|change| Owner::key(drive, change.path())),
            );
        }
    }

    /// The accepted changes of a commit: the covered ones count in the
    /// outcome of the sync point that sent it.
    pub(crate) fn commit_accepted(
        &mut self,
        sync_point: Option<u64>,
        accepted: &[(Key, CommitChange)],
    ) {
        let Some(point) = sync_point.and_then(|id| self.sync_points.get_mut(&id)) else {
            return;
        };
        for (key, change) in accepted {
            if point.covered.contains(key) {
                match change {
                    CommitChange::Write { .. } => point.committed += 1,
                    CommitChange::Delete { .. } => point.deleted += 1,
                }
            }
        }
    }

    /// Select the result owner before settlement so asynchronous notices can
    /// be persisted in the same transaction as their rejected changes.
    pub(crate) fn rejection_owner(&self, sync_point: Option<u64>, key: &Key) -> Option<u64> {
        sync_point
            .filter(|id| {
                self.sync_points
                    .get(id)
                    .is_some_and(|point| point.covered.contains(key))
            })
            .or_else(|| {
                self.sync_points
                    .iter()
                    .find(|(_, point)| point.covered.contains(key))
                    .map(|(id, _)| *id)
            })
    }

    pub(crate) fn rejected(&mut self, owner: Option<u64>, rejection: Rejected) {
        if let Some(point) = owner.and_then(|id| self.sync_points.get_mut(&id)) {
            point.rejected.push(rejection.entry);
        }
    }

    /// A covered change of every sync point in progress faulted.
    pub(crate) fn faulted(&mut self, key: &Key) {
        for point in self.sync_points.values_mut() {
            if point.covered.contains(key) {
                point.faulted.insert(key.clone());
            }
        }
    }

    /// Reports the outcome to the caller, then sends again the commits it
    /// held back.
    async fn outcome(&mut self, id: u64) {
        let Some(mut point) = self.sync_points.remove(&id) else {
            return;
        };
        // Unsent batches also remain undecided at a deadline or content_missing
        // result. Keep the listing bounded to the point's original coverage.
        point.uploading.extend(
            point
                .remaining
                .keys()
                .filter(|key| point.covered.contains(*key))
                .cloned(),
        );
        let mut uploading: Vec<Key> = point
            .uploading
            .iter()
            .filter(|key| !point.faulted.contains(*key))
            .filter(|key| self.state.tables().pending_changes.contains_key(*key))
            .cloned()
            .collect();
        uploading.sort();
        let mut pending: Vec<Key> = point
            .faulted
            .iter()
            .filter(|key| self.state.tables().pending_changes.contains_key(*key))
            .cloned()
            .collect();
        pending.sort();
        let mut not_synced = Vec::new();
        for (keys, reason) in [
            (&uploading, NotSyncedReason::Uploading),
            (&pending, NotSyncedReason::UploadPending),
        ] {
            for key in keys {
                not_synced.push(DriveNotSynced {
                    path: self.absolute(key),
                    reason,
                });
                self.listed.insert(key.clone());
            }
        }
        self.uncovered.extend(uploading);
        let mut notices = std::mem::take(&mut point.taskless_notices);
        if let Some(task) = &point.task {
            notices.extend(self.notices.remove(task).unwrap_or_default());
            self.reported_tasks.insert(task.clone());
        }
        for notice in notices {
            let reason = match notice.reason {
                "symlink" => NotSyncedReason::Symlink,
                "special_file" => NotSyncedReason::SpecialFile,
                "git_repository" => NotSyncedReason::GitRepository,
                _ => continue,
            };
            if let Ok(path) = AbsolutePath::new(notice.path.to_string_lossy().into_owned()) {
                let notice = DriveNotSynced { path, reason };
                if !not_synced.contains(&notice) {
                    not_synced.push(notice);
                }
            }
        }
        if let Some(reply) = point.reply.take() {
            let _ = reply.send(SyncOutcome {
                committed: point.committed,
                deleted: point.deleted,
                rejected: std::mem::take(&mut point.rejected),
                not_synced,
            });
        }
        for (drive, changes) in point.resend {
            if self.flights.contains_key(&drive) {
                continue;
            }
            self.send_commit(&drive, changes, None, None);
        }
        self.changes_ready().await;
    }

    pub(crate) fn absolute(&self, key: &Key) -> AbsolutePath {
        AbsolutePath::new(
            self.machine_path(&key.0, &key.1)
                .to_string_lossy()
                .into_owned(),
        )
        .expect("a machine path is absolute")
    }

    /// The next time a sync point's wait ends or it stops waiting for a
    /// commit answer.
    pub(crate) fn sync_point_wake(&self) -> Option<Duration> {
        self.sync_points
            .values()
            .flat_map(|point| {
                let wait = (!point.committing).then_some(point.wait_end);
                wait.into_iter()
                    .chain(point.waiting.values().map(|(_, until)| *until))
            })
            .min()
    }
}
