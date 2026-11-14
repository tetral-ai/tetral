//! The final upload of some of a Session's drives, at a close, a
//! `session.drives` or before an idle exit. It covers every pending change
//! of those drives, tries their faults again, starts every upload not
//! started and makes every covered change due. It ends once each covered
//! change is accepted, rejected or faulted, or at its end time, and then
//! cancels every upload and commit of those drives.
//!
//! In abandon mode every change of those drives still pending, one
//! recorded after the start included, is then abandoned: its `lost_changes`
//! entries go to the Connection owner, and once they are written its
//! pending row and the drive's stored commit go in one transaction, its
//! path taking the drive's version, and its content file is removed after
//! it; nothing of those drives is uploaded or committed until they are
//! mounted again. On a Mac a WRITE to one of those drives that the NFS
//! server still holds is named with them, and answers `NFS3ERR_IO` and
//! changes nothing when its download ends. In keep mode the changes stay
//! pending; a stored commit is sent again when the binding becomes current
//! on a new connection, or at the next start.
//! A released final upload ends at once and cancels nothing: its uploads
//! and commits go on as ordinary ones.
//!
//! On a Mac it starts once the client's cache of the drives' mounts is
//! flushed and the NFS server has answered the WRITEs to them it received
//! before the flush began, at its end time when those have not ended by
//! then, or at once when its caller ends it during them. Ends its caller
//! gave during the flush reach the owner with the start and apply, in their
//! order, right after it, before the final upload can end by itself.

use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use protocol::connection::LostChange;
use protocol::id::Id;
use protocol::ports::ConnectionPort;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::state::Key;
use crate::sync::{DriveSync, Message, Owner, now_ns};

/// What a final upload is asked for.
pub struct FinalUploadRequest {
    /// Session removal ends result upload waits for these drives. Binding
    /// changes and idle exit preserve their ordinary result waits.
    pub session_close: bool,
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
    flushing: Flushing,
    lost_changes: oneshot::Receiver<Vec<LostChange>>,
}

/// What ends a final upload at once, in keep mode or released, apart from
/// its end.
pub struct FinalKeeper {
    id: u64,
    mailbox: mpsc::UnboundedSender<Message>,
    flushing: Flushing,
}

/// An end a keeper gives.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum FinalEnd {
    Keep,
    Release,
}

/// The flush of the drives' mounts a final upload waits for.
pub(crate) type Flush = Pin<Box<dyn Future<Output = ()> + Send>>;

/// While a final upload waits for its flush, the keepers' ends, which
/// stop the wait and reach the owner with the start.
#[derive(Clone, Default)]
struct Flushing {
    ends: Arc<std::sync::Mutex<Option<Vec<FinalEnd>>>>,
    ended: Arc<Notify>,
}

impl Flushing {
    fn ends(&self) -> std::sync::MutexGuard<'_, Option<Vec<FinalEnd>>> {
        self.ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl FinalKeeper {
    /// Ends it at once in keep mode, as the connection's end does.
    pub fn keep_now(&self) {
        self.end(FinalEnd::Keep);
    }

    /// Ends it at once without cancelling anything; its uploads and commits
    /// go on as ordinary ones, as when a tool call ends an idle exit.
    pub fn release(&self) {
        self.end(FinalEnd::Release);
    }

    fn end(&self, end: FinalEnd) {
        let mut ends = self.flushing.ends();
        match ends.as_mut() {
            Some(held) => {
                held.push(end);
                self.flushing.ended.notify_one();
            }
            None => {
                let _ = self.mailbox.send(match end {
                    FinalEnd::Keep => Message::FinalKeep(self.id),
                    FinalEnd::Release => Message::FinalRelease(self.id),
                });
            }
        }
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
            flushing: self.flushing.clone(),
        }
    }

    /// Waits for its end: the `lost_changes` entries it kept, unfitted, in
    /// `drive_id` order; none in keep mode.
    pub async fn ended(&mut self) -> Vec<LostChange> {
        (&mut self.lost_changes).await.unwrap_or_default()
    }
}

impl DriveSync {
    /// Starts a final upload of `request.drives`, which reaches the owner
    /// after the flush of their mounts, at its end, or at a keeper's end.
    pub fn final_upload(&self, request: FinalUploadRequest) -> FinalUpload {
        let drives = request
            .drives
            .iter()
            .map(|drive| drive.as_str().to_owned())
            .collect();
        let flush = self.flush(Some(drives), request.end);
        self.final_upload_after(request, flush)
    }

    /// [`DriveSync::final_upload`] once `flush`, when there is one, ends.
    pub(crate) fn final_upload_after(
        &self,
        request: FinalUploadRequest,
        flush: Option<Flush>,
    ) -> FinalUpload {
        let id = self.final_ids.fetch_add(1, Ordering::Relaxed);
        let (reply, lost_changes) = oneshot::channel();
        let flushing = Flushing::default();
        match flush {
            None => self.send(Message::FinalUpload {
                id,
                request,
                ends: Vec::new(),
                reply,
            }),
            Some(flush) => {
                *flushing.ends() = Some(Vec::new());
                let (mailbox, flushing) = (self.mailbox.clone(), flushing.clone());
                tokio::spawn(async move {
                    tokio::select! {
                        () = flush => {}
                        () = flushing.ended.notified() => {}
                    }
                    // Under the lock, an end goes either with the start or
                    // to the mailbox after it.
                    let mut ends = flushing.ends();
                    let _ = mailbox.send(Message::FinalUpload {
                        id,
                        request,
                        ends: ends.take().unwrap_or_default(),
                        reply,
                    });
                });
            }
        }
        FinalUpload {
            id,
            mailbox: self.mailbox.clone(),
            flushing,
            lost_changes,
        }
    }
}

/// A final upload as its owner holds it.
pub(crate) struct Final {
    session_close: bool,
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
        ends: Vec<FinalEnd>,
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
                session_close: request.session_close,
                drives,
                covered,
                end: request.end,
                mode: request.mode,
                reply,
            },
        );
        state::seams::count("drive.final_upload");
        if request.session_close {
            self.release_sync_waits(&request.drives).await;
        }
        self.changes_ready().await;
        // The first end takes effect; the ones after it find nothing.
        for end in ends {
            match end {
                FinalEnd::Keep => self.final_keep(id).await,
                FinalEnd::Release => self.final_release(id),
            }
        }
    }

    /// The drives of the Session removal's final uploads in progress.
    pub(crate) fn closing_drives(&self) -> Vec<Id> {
        self.final_uploads
            .values()
            .filter(|upload| upload.session_close)
            .flat_map(|upload| &upload.drives)
            .filter_map(|drive| Id::new(drive).ok())
            .collect()
    }

    /// The final upload `id` ends now in keep mode.
    pub(crate) async fn final_keep(&mut self, id: u64) {
        if let Some(upload) = self.final_uploads.get_mut(&id) {
            upload.mode = FinalMode::Keep;
            self.end_final(id).await;
        }
    }

    /// The final upload `id` ends now and leaves its uploads and commits
    /// running.
    pub(crate) fn final_release(&mut self, id: u64) {
        if let Some(upload) = self.final_uploads.remove(&id) {
            let _ = upload.reply.send(Vec::new());
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
    /// what no commit answer decided. A commit whose `200` arrived counts as
    /// answered: only the verification of its copies is cut short.
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
                match &flight.answer {
                    Some(answer) => {
                        self.apply_results(drive, &flight, answer, None).await;
                        self.commit_cancelled(drive, &[]);
                        self.send_reports();
                    }
                    None => self.commit_cancelled(drive, &flight.changes),
                }
            }
        }
        self.recorded_during_flight
            .retain(|(drive, _), _| !upload.drives.contains(drive));
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

    /// On a Mac, refuses the WRITEs to `drives` that the NFS server holds
    /// unanswered ([`crate::nfs::Server::refuse_unanswered`]); the paths
    /// they write.
    fn refuse_held_writes(&self, drives: &BTreeSet<String>) -> Vec<Key> {
        #[cfg(target_os = "macos")]
        if let crate::mount::Mounting::Nfs(server) = &self.mounting {
            return server
                .refuse_unanswered(self.session_id.as_str(), drives)
                .into_iter()
                .filter_map(|(drive, file_id)| {
                    let path = self
                        .inodes
                        .get(&(drive.clone(), i64::try_from(file_id).ok()?))?
                        .clone();
                    Some((drive, path))
                })
                .collect();
        }
        let _ = drives;
        Vec::new()
    }

    /// Abandons every pending change of the covered drives, one recorded
    /// after the start included, and on a Mac every WRITE to them the NFS
    /// server holds, once the Connection owner has kept their entries.
    async fn abandon(
        &mut self,
        upload: &Final,
        request_message_id: &Id,
        connection: &dyn ConnectionPort,
    ) -> Vec<LostChange> {
        self.closed.extend(upload.drives.iter().cloned());
        let mut abandoned: Vec<Key> = self
            .state
            .tables()
            .pending_changes
            .keys()
            .filter(|(drive, _)| upload.drives.contains(drive))
            .cloned()
            .collect();
        abandoned.sort();
        let held = self.refuse_held_writes(&upload.drives);
        if abandoned.is_empty() && held.is_empty() {
            return Vec::new();
        }
        let entries =
            crate::lost_changes::entries(request_message_id, abandoned.iter().cloned().chain(held));
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
            self.remove_content(&drive, file_id);
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
