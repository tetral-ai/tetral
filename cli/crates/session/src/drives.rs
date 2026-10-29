//! Preparation step 4's drive mounts: the Session's drive sync, opened from
//! `sessions/<session_id>/sync-state.db` at its first preparation that binds
//! drives, mounts each bound drive and reports the new failures in
//! `drive_mount_failures`. A sync state that cannot be opened fails every
//! bound drive with its error, reported once per drive and text in this
//! App Server process, and is opened again at the next preparation.

use std::collections::HashMap;
use std::sync::Arc;

use drive::cache::ContentCache;
use drive::mount::{Bound, Failure, Mounting, Registry};
use drive::state::{FILE_NAME, SyncState};
use drive::transfer::Binding as DriveBinding;
use drive::{DriveSync, DriveTransfer, SyncConfig};
use protocol::connection::DriveAccess;
use protocol::field::AbsolutePath;
use protocol::id::Id;
use protocol::ports::TaskOwnership;
use protocol::tool::DriveMountFailure;
use tokio::sync::watch;

use crate::owner::{SessionMessage, SessionOwner, TaskStarter};
use crate::record::Binding;

/// What the drive syncs of an App Server's Sessions are built with.
pub struct DriveContext {
    pub transfer: Arc<dyn DriveTransfer>,
    pub cache: ContentCache,
    pub mounting: Mounting,
    pub registry: Arc<dyn Registry>,
}

impl std::fmt::Debug for DriveContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriveContext")
            .field("mounting", &self.mounting)
            .finish_non_exhaustive()
    }
}

/// The drive context, once the Connection owner that provides its transfer
/// client has started.
pub type DriveContexts = watch::Receiver<Option<Arc<DriveContext>>>;

/// A context that never comes, for owners that bind no drives.
pub fn no_drives() -> DriveContexts {
    watch::channel(None).1
}

/// The Session's drive sync.
#[derive(Default)]
pub(crate) enum DriveSyncSlot {
    #[default]
    Closed,
    /// Being opened; the operations whose mounts wait for it.
    Opening(Vec<u64>),
    Open(DriveSync),
}

/// Each bound drive of `binding` as the mount check takes it.
fn bound(binding: &Binding) -> Vec<Bound> {
    binding
        .drives
        .iter()
        .map(|drive| Bound {
            drive_id: drive.drive_id.clone(),
            read_only: drive.access == DriveAccess::ReadOnly,
            exclude: binding
                .drive_exclude
                .iter()
                .filter(|exclude| exclude.drive_id.as_str() == drive.drive_id.as_str())
                .flat_map(|exclude| exclude.paths.iter().map(|path| path.as_str().to_owned()))
                .collect(),
        })
        .collect()
}

pub(crate) fn failures(failures: Vec<Failure>) -> Vec<DriveMountFailure> {
    failures
        .into_iter()
        .filter_map(|failure| {
            Some(DriveMountFailure {
                drive_id: Id::new(failure.drive_id).ok()?,
                error: protocol::field::NonEmptyString::new(failure.error).ok()?,
            })
        })
        .collect()
}

impl<S: TaskStarter> SessionOwner<S> {
    /// Step 4's mounts of `operation`, whose probe found `cwd`; the clones
    /// follow them.
    pub(crate) fn mount_drives(&mut self, operation: u64, cwd: AbsolutePath) {
        let Some(binding) = self.current_binding().filter(|b| b.operation == operation) else {
            return;
        };
        binding.cwd = Some(cwd.clone());
        if binding.drives.is_empty() {
            return self.clone_repositories(operation, cwd);
        }
        match &mut self.drive_sync {
            DriveSyncSlot::Open(sync) => {
                let sync = sync.clone();
                self.spawn_mounts(operation, sync);
            }
            DriveSyncSlot::Opening(waiting) => waiting.push(operation),
            DriveSyncSlot::Closed => {
                self.drive_sync = DriveSyncSlot::Opening(vec![operation]);
                self.open_drive_sync();
            }
        }
    }

    fn open_drive_sync(&self) {
        let Some(preparation) = &self.preparation else {
            return;
        };
        let mut contexts = preparation.shared.drives.clone();
        let session_id = preparation.session_id.clone();
        let root = preparation.shared.paths.canonical.clone();
        let directory = state::paths::session_directory(&root, session_id.as_str());
        let tasks: Arc<dyn TaskOwnership> = crate::owner::task_ownership(self.mailbox.clone());
        let mailbox = self.mailbox.clone();
        tokio::spawn(async move {
            let context = loop {
                if let Some(context) = contexts.borrow().clone() {
                    break context;
                }
                if contexts.changed().await.is_err() {
                    return;
                }
            };
            let path = directory.join(FILE_NAME);
            let state = if path.exists() {
                SyncState::open(&path)
                    .await
                    .map_err(|error| error.to_string())
            } else {
                match state::files::create_private_dirs(&directory) {
                    Ok(()) => SyncState::create(&path)
                        .await
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(format!("sync state: {}", error.source)),
                }
            };
            let opened = state.map(|state| {
                let sync = DriveSync::spawn(SyncConfig {
                    session_id: session_id.clone(),
                    root,
                    state,
                    transfer: context.transfer.clone(),
                    cache: context.cache.clone(),
                    tasks,
                    mounting: context.mounting.clone(),
                    registry: context.registry.clone(),
                });
                context.registry.add(session_id, sync.clone());
                sync
            });
            let _ = mailbox.send(SessionMessage::DriveSyncOpened(opened));
        });
    }

    /// The drive sync opened, or could not be: each waiting operation that
    /// is still current mounts its drives, or fails them all.
    pub(crate) fn drive_sync_opened(&mut self, opened: Result<DriveSync, String>) {
        let waiting = match std::mem::take(&mut self.drive_sync) {
            DriveSyncSlot::Opening(waiting) => waiting,
            other => {
                self.drive_sync = other;
                return;
            }
        };
        match opened {
            Ok(sync) => {
                self.drive_sync = DriveSyncSlot::Open(sync.clone());
                if let Some(binding) = self.current_binding()
                    && binding.state == protocol::local::SessionState::Prepared
                {
                    sync.binding_current(DriveBinding {
                        connection_id: binding.connection_id.clone(),
                        binding_id: binding.binding_id.clone(),
                    });
                }
                for operation in waiting {
                    if self.is_preparing(operation) {
                        self.spawn_mounts(operation, sync.clone());
                    }
                }
            }
            Err(error) => {
                for operation in waiting {
                    let Some(binding) = self
                        .current_binding()
                        .filter(|binding| binding.operation == operation)
                    else {
                        continue;
                    };
                    let all: Vec<Failure> = binding
                        .drives
                        .iter()
                        .map(|drive| Failure {
                            drive_id: drive.drive_id.as_str().to_owned(),
                            error: error.clone(),
                        })
                        .collect();
                    let new = new_open_failures(&mut self.open_failures, all);
                    self.mounts_done(operation, new);
                }
            }
        }
    }

    fn spawn_mounts(&self, operation: u64, sync: DriveSync) {
        let Some(binding) = self
            .record
            .as_ref()
            .and_then(|record| record.current.as_ref())
            .filter(|binding| binding.operation == operation)
        else {
            return;
        };
        let bound = bound(binding);
        let mailbox = self.mailbox.clone();
        tokio::spawn(async move {
            let failures = sync.mount_drives(bound).await;
            let _ = mailbox.send(SessionMessage::MountsDone {
                operation,
                failures,
            });
        });
    }

    /// The mounts of `operation` ended with the new `failures`, which its
    /// report lists; the clones follow while it is still current.
    pub(crate) fn mounts_done(&mut self, operation: u64, failures: Vec<Failure>) {
        if !self.is_preparing(operation) {
            return;
        }
        let Some(binding) = self.current_binding() else {
            return;
        };
        let Some(cwd) = binding.cwd.clone() else {
            return;
        };
        binding.drive_mount_failures = failures;
        self.clone_repositories(operation, cwd);
    }

    /// The report of a prepared binding is built: the drive sync keeps the
    /// texts of the failures it lists.
    pub(crate) fn mount_failures_reported(&self, failures: &[Failure]) {
        if let DriveSyncSlot::Open(sync) = &self.drive_sync
            && !failures.is_empty()
        {
            sync.reported(failures.to_vec());
        }
    }

    /// The binding of the current connection is `prepared`: the drive
    /// sync's requests carry it.
    pub(crate) fn drive_binding_current(&self) {
        let (DriveSyncSlot::Open(sync), Some(binding)) = (
            &self.drive_sync,
            self.record
                .as_ref()
                .and_then(|record| record.current.as_ref()),
        ) else {
            return;
        };
        sync.binding_current(DriveBinding {
            connection_id: binding.connection_id.clone(),
            binding_id: binding.binding_id.clone(),
        });
    }

    pub(crate) fn drive_binding_gone(&self) {
        if let DriveSyncSlot::Open(sync) = &self.drive_sync {
            sync.binding_gone();
        }
    }
}

/// The failures among `failures` whose text differs from the one last
/// reported for their drive in this process.
fn new_open_failures(
    reported: &mut HashMap<String, String>,
    failures: Vec<Failure>,
) -> Vec<Failure> {
    failures
        .into_iter()
        .filter(|failure| {
            reported.insert(failure.drive_id.clone(), failure.error.clone())
                != Some(failure.error.clone())
        })
        .collect()
}
