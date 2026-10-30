//! The final upload of some of a Session's drives, at a close, a
//! `session.drives` or before an idle exit. It covers every pending change
//! of those drives, tries their faults again, starts every upload not
//! started and makes every covered change due. It ends once each covered
//! change is accepted, rejected or faulted, or at its end time, and then
//! cancels every upload and commit of those drives.
//!
//! In abandon mode every covered change still pending is then abandoned:
//! its `lost_changes` entries go to the Connection owner, and once they are
//! written its pending row, the drive's stored commit and its content file
//! go in one transaction, its path taking the drive's version; nothing of
//! those drives is uploaded or committed until they are mounted again. In
//! keep mode the changes stay pending; a stored commit is sent again when
//! the binding becomes current on a new connection, or at the next start.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use protocol::connection::LostChange;
use protocol::id::Id;
use protocol::ports::ConnectionPort;
use tokio::sync::{mpsc, oneshot};

use crate::state::Key;
use crate::sync::{DriveSync, Message, Owner, now_ns};

/// What a final upload is asked for.
pub struct FinalUploadRequest {
    pub drives: Vec<Id>,
    /// Its end time, on the drive sync's clock.
    pub end: Duration,
    pub mode: FinalMode,
}

/// What happens to the changes no commit answer decided by the end.
pub enum FinalMode {
    /// They are abandoned and named in entries for the `session.close` or
    /// `session.drives` of `request_message_id`, which `connection` keeps.
    Abandon {
        request_message_id: Id,
        connection: Arc<dyn ConnectionPort>,
    },
    /// They stay pending.
    Keep,
}

/// A final upload in progress.
pub struct FinalUpload {
    id: u64,
    mailbox: mpsc::UnboundedSender<Message>,
    lost_changes: oneshot::Receiver<Vec<LostChange>>,
}

/// What ends a final upload at once in keep mode, apart from its end.
pub struct FinalKeeper {
    id: u64,
    mailbox: mpsc::UnboundedSender<Message>,
}

impl FinalKeeper {
    /// Ends it at once in keep mode, as the connection's end does.
    pub fn keep_now(&self) {
        let _ = self.mailbox.send(Message::FinalKeep(self.id));
    }
}

impl FinalUpload {
    /// Ends it at once in keep mode, as the connection's end does.
    pub fn keep_now(&self) {
        self.keeper().keep_now();
    }

    pub fn keeper(&self) -> FinalKeeper {
        FinalKeeper {
            id: self.id,
            mailbox: self.mailbox.clone(),
        }
    }

    /// Waits for its end: the `lost_changes` entries it kept, unfitted, in
    /// `drive_id` order; none in keep mode.
    pub async fn ended(&mut self) -> Vec<LostChange> {
        (&mut self.lost_changes).await.unwrap_or_default()
    }
}

impl DriveSync {
    /// Starts a final upload of `request.drives`.
    pub fn final_upload(&self, request: FinalUploadRequest) -> FinalUpload {
        let id = self.final_ids.fetch_add(1, Ordering::Relaxed);
        let (reply, lost_changes) = oneshot::channel();
        self.send(Message::FinalUpload { id, request, reply });
        FinalUpload {
            id,
            mailbox: self.mailbox.clone(),
            lost_changes,
        }
    }
}

/// A final upload as its owner holds it.
pub(crate) struct Final {
    drives: BTreeSet<String>,
    covered: HashSet<Key>,
    end: Duration,
    mode: FinalMode,
    reply: oneshot::Sender<Vec<LostChange>>,
}

impl Owner {
    pub(crate) async fn final_upload(
        &mut self,
        id: u64,
        request: FinalUploadRequest,
        reply: oneshot::Sender<Vec<LostChange>>,
    ) {
        let drives: BTreeSet<String> = request
            .drives
            .iter()
            .map(|drive| drive.as_str().to_owned())
            .collect();
        let covered: HashSet<Key> = self
            .state
            .tables()
            .pending_changes
            .keys()
            .filter(|(drive, _)| drives.contains(drive))
            .cloned()
            .collect();
        self.retry_faults(Some(&covered)).await;
        let mut keys: Vec<&Key> = covered.iter().collect();
        keys.sort();
        for key in keys {
            self.start_upload(key, None);
        }
        self.startup_due.extend(covered.iter().cloned());
        self.final_uploads.insert(
            id,
            Final {
                drives,
                covered,
                end: request.end,
                mode: request.mode,
                reply,
            },
        );
        self.changes_ready().await;
    }

    /// The final upload `id` ends now in keep mode.
    pub(crate) async fn final_keep(&mut self, id: u64) {
        if let Some(upload) = self.final_uploads.get_mut(&id) {
            upload.mode = FinalMode::Keep;
            self.end_final(id).await;
        }
    }

    /// Ends every final upload whose covered changes are decided or whose
    /// end time passed.
    pub(crate) async fn final_uploads_progress(&mut self) {
        let now = self.clock.now();
        let pending = &self.state.tables().pending_changes;
        let ended: Vec<u64> = self
            .final_uploads
            .iter()
            .filter(|(_, upload)| {
                now >= upload.end
                    || upload.covered.iter().all(|key| {
                        pending
                            .get(key)
                            .is_none_or(|change| change.fault_code.is_some())
                    })
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ended {
            self.end_final(id).await;
        }
    }

    /// The earliest end time of a final upload in progress.
    pub(crate) fn final_wake(&self) -> Option<Duration> {
        self.final_uploads.values().map(|upload| upload.end).min()
    }

    /// Cancels every upload and commit of the drives, then abandons or keeps
    /// what no commit answer decided.
    async fn end_final(&mut self, id: u64) {
        let Some(upload) = self.final_uploads.remove(&id) else {
            return;
        };
        let uploading: Vec<Key> = self
            .uploads
            .running
            .keys()
            .filter(|(drive, _)| upload.drives.contains(drive))
            .cloned()
            .collect();
        for key in &uploading {
            self.cancel_upload(key);
        }
        for drive in &upload.drives {
            if let Some(flight) = self.flights.remove(drive) {
                flight.task.abort();
                self.commit_cancelled(drive, &flight.changes);
            }
        }
        self.recorded_during_flight
            .retain(|(drive, _)| !upload.drives.contains(drive));
        let lost_changes = match &upload.mode {
            FinalMode::Keep => Vec::new(),
            FinalMode::Abandon {
                request_message_id,
                connection,
            } => {
                self.abandon(&upload, request_message_id, connection.as_ref())
                    .await
            }
        };
        let _ = upload.reply.send(lost_changes);
        self.sync_points_progress().await;
    }

    /// Abandons the covered changes still pending once the Connection owner
    /// has kept their entries.
    async fn abandon(
        &mut self,
        upload: &Final,
        request_message_id: &Id,
        connection: &dyn ConnectionPort,
    ) -> Vec<LostChange> {
        self.closed.extend(upload.drives.iter().cloned());
        let mut abandoned: Vec<Key> = upload
            .covered
            .iter()
            .filter(|key| self.state.tables().pending_changes.contains_key(*key))
            .cloned()
            .collect();
        abandoned.sort();
        if abandoned.is_empty() {
            return Vec::new();
        }
        let entries = crate::lost_changes::entries(request_message_id, abandoned.clone());
        let (kept, written) = oneshot::channel();
        connection.keep_lost_changes(
            entries.clone(),
            Box::new(move || {
                let _ = kept.send(());
            }),
        );
        let _ = written.await;

        let before = self.state.tables().clone();
        let mut after = before.clone();
        for key in &abandoned {
            after.pending_changes.remove(key);
        }
        after
            .inflight_commits
            .retain(|(drive, _), _| !upload.drives.contains(drive));
        let mut removed = Vec::new();
        for drive in &upload.drives {
            let paths: Vec<String> = abandoned
                .iter()
                .filter(|(each, _)| each == drive)
                .map(|(_, path)| path.clone())
                .collect();
            if !paths.is_empty() {
                let restored = crate::catchup::restore(&after, drive, &paths, now_ns());
                after.apply(restored);
            }
        }
        for drive in &upload.drives {
            for file_id in crate::commit::removed_content(drive, &before, &after) {
                removed.push((drive.clone(), file_id));
            }
        }
        if self
            .write(crate::state::diff(&before, &after))
            .await
            .is_err()
        {
            return entries;
        }
        for (drive, file_id) in removed {
            let _ = std::fs::remove_file(self.content_path(&drive, file_id));
        }
        for (drive, path) in &abandoned {
            if let Some(mounted) = self.mounted.get(drive)
                && let Some(row) = self.state.tables().files.get(&Owner::key(drive, path))
            {
                mounted.invalidate(row.file_id as u64);
            }
            self.uploads.last_write.remove(&Owner::key(drive, path));
        }
        self.forget_settled();
        entries
    }
}
