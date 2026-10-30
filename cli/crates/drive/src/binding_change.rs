//! A `session.drives` handled on the Session's drive sync, against the
//! binding's current drive set:
//!
//! 1. the final upload of every removed drive and every drive whose access
//!    changed, under the old access, ending 30 seconds after this step
//!    began;
//! 2. every removed drive detached, its rows deleted from every table in
//!    one transaction and its content folder deleted;
//! 3. and 4. every drive whose access changed detached, then the mount
//!    check of the new set, which mounts each added drive and each of those
//!    again with the new access;
//! 5. the new set becomes the one the checks before commands mount.
//!
//! The connection's end during step 1 ends the final upload at once in
//! keep mode and stops the handling: nothing changes.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use protocol::connection::LostChange;
use protocol::id::Id;
use protocol::ports::ConnectionPort;
use tokio::sync::oneshot;

use crate::final_upload::{FinalMode, FinalUploadRequest};
use crate::mount::{Bound, Failure};
use crate::state::Write;
use crate::sync::{DriveSync, Message, Owner};

/// How long the final upload of a `session.drives` may run.
pub const FINAL_UPLOAD_WAIT: Duration = Duration::from_secs(30);

/// What a `session.drives` changes.
pub struct BindingChange {
    /// The binding's drive set before it.
    pub old: Vec<Bound>,
    /// Its `drives`, with this computer's excluded paths of each.
    pub new: Vec<Bound>,
    /// The `session.drives` message, which its entries name.
    pub request_message_id: Id,
    /// The Connection owner, which keeps the entries.
    pub connection: Arc<dyn ConnectionPort>,
}

/// What the `applied` report of a handled `session.drives` lists.
#[derive(Debug, Default)]
pub struct Applied {
    /// The new mount failures of this handling.
    pub failures: Vec<Failure>,
    /// The entries of the changes it abandoned, unfitted.
    pub lost_changes: Vec<LostChange>,
}

impl DriveSync {
    /// Handles `change`; `None` when `stop` ended step 1, which then left
    /// everything as it was.
    pub async fn change_binding(
        &self,
        change: BindingChange,
        stop: impl Future<Output = ()>,
    ) -> Option<Applied> {
        let old: BTreeMap<&str, &Bound> = change
            .old
            .iter()
            .map(|bound| (bound.drive_id.as_str(), bound))
            .collect();
        let new: BTreeMap<&str, &Bound> = change
            .new
            .iter()
            .map(|bound| (bound.drive_id.as_str(), bound))
            .collect();
        let removed: BTreeSet<&str> = old
            .keys()
            .filter(|drive| !new.contains_key(*drive))
            .copied()
            .collect();
        let changed: BTreeSet<&str> = old
            .iter()
            .filter(|(drive, bound)| {
                new.get(*drive)
                    .is_some_and(|now| now.read_only != bound.read_only)
            })
            .map(|(drive, _)| *drive)
            .collect();

        let mut lost_changes = Vec::new();
        let ending: Vec<Id> = removed
            .union(&changed)
            .filter_map(|drive| Id::new(*drive).ok())
            .collect();
        if !ending.is_empty() {
            let mut upload = self.final_upload(FinalUploadRequest {
                drives: ending,
                end: self.clock.now() + FINAL_UPLOAD_WAIT,
                mode: FinalMode::Abandon {
                    request_message_id: change.request_message_id.clone(),
                    connection: change.connection.clone(),
                },
            });
            let ended = tokio::select! {
                lost = upload.ended() => Some(lost),
                () = stop => None,
            };
            match ended {
                Some(lost) => lost_changes = lost,
                None => {
                    upload.keep_now();
                    upload.ended().await;
                    return None;
                }
            }
        }
        for drive in &removed {
            self.remove_drive(drive).await;
        }
        for drive in &changed {
            self.send(Message::Detach((*drive).to_owned()));
        }
        let failures = self.mount_drives(change.new).await;
        Some(Applied {
            failures,
            lost_changes,
        })
    }

    /// Detaches `drive` and deletes its rows and content folder.
    async fn remove_drive(&self, drive: &str) {
        let (reply, removed) = oneshot::channel();
        self.send(Message::RemoveDrive {
            drive: drive.to_owned(),
            reply,
        });
        let _ = removed.await;
    }
}

impl Owner {
    /// Step 2 for `drive`: its mount detached, its rows deleted in one
    /// transaction, then its content folder.
    pub(crate) async fn remove_drive(&mut self, drive: &str) {
        self.detach(drive);
        let tables = self.state.tables();
        let of_drive = |key: &&crate::state::Key| key.0 == drive;
        let write = Write {
            files_removed: tables.files.keys().filter(of_drive).cloned().collect(),
            pending_removed: tables
                .pending_changes
                .keys()
                .filter(of_drive)
                .cloned()
                .collect(),
            cursor_removed: vec![drive.to_owned()],
            inflight_removed_drives: vec![drive.to_owned()],
            mount_errors_removed: vec![drive.to_owned()],
            listing_removed_drives: vec![drive.to_owned()],
            ..Write::default()
        };
        if self.write(write).await.is_err() {
            return;
        }
        let _ = std::fs::remove_dir_all(self.session_dir.join("content").join(drive));
        self.unmounted.remove(drive);
        self.closed.remove(drive);
        self.inodes.retain(|(each, _), _| each != drive);
        self.uploads.last_write.retain(|(each, _), _| each != drive);
        self.forget_settled();
    }
}
