//! Content uploads of pending writes: their triggers (2 seconds without a
//! write while an `exec_command` task of the Session runs, the exit of the
//! writing task, a sync point, the binding becoming current after a start or
//! on a new connection, a final upload), the machine's upload slots, hashing
//! and the PUT's answers. A write to a file cancels its upload, waiting for a
//! slot or running; the next trigger uploads the new content.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use protocol::field::Sha256;
use protocol::id::Id;
use sha2::Digest;
use tokio::task::AbortHandle;

use crate::state::{Key, PendingChange, Write};
use crate::sync::{Message, Owner};
use crate::transfer::{Content, Ended, Request, Upload};

/// The time without a write after which a written file uploads while a
/// command of the Session runs.
pub const QUIET: Duration = Duration::from_secs(2);

/// An upload in progress: waiting for its slot, hashing or sending.
pub(crate) struct Uploading {
    pub id: u64,
    /// The write generation of the file it uploads.
    pub generation: u64,
    pub task: AbortHandle,
}

/// How an upload ended.
#[derive(Debug)]
pub(crate) enum Uploaded {
    /// The drive holds the content of this hash and size.
    Done { sha256: String, size: i64 },
    /// `400 content_hash_mismatch`: the file is read again.
    ReRead,
    /// `413 file_too_large`, with the error's message.
    TooLarge(String),
    /// A permanent answer: a fault.
    Fault { status: u16, code: String },
    /// The content file could not be read.
    Unreadable,
}

/// The upload state of one Session.
#[derive(Default)]
pub(crate) struct Uploads {
    pub running: HashMap<Key, Uploading>,
    pub ids: u64,
    /// Bumped by every write of a path, so that an older upload's end is
    /// dropped.
    pub generations: HashMap<Key, u64>,
    /// The clock time of each path's last write in this run.
    pub last_write: HashMap<Key, Duration>,
    /// The hashes a PUT to each drive answered `200` during this run.
    pub put: HashSet<(String, String)>,
    /// The size of each content an upload of a drive hashed, by drive and
    /// hash.
    pub sizes: HashMap<(String, String), i64>,
    /// The `exec_command` tasks of the Session that run.
    pub running_tasks: HashSet<String>,
    /// The writes whose content a commit answered `409 content_missing`:
    /// their next upload sends it, a `cache` file's from its blob.
    pub missing: HashSet<Key>,
}

fn sha256_of(path: &std::path::Path) -> std::io::Result<(String, i64)> {
    let mut file = std::fs::File::open(path)?;
    file.sync_all()?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0u8; 1 << 16];
    let mut size: i64 = 0;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as i64;
    }
    let hex = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((hex, size))
}

impl Owner {
    pub(crate) fn key(drive: &str, path: &str) -> Key {
        (drive.to_owned(), path.to_owned())
    }

    /// A process changed `key` through a mount: its upload is cancelled
    /// and an earlier result's `uploading` listing no longer holds.
    pub(crate) fn local_change(&mut self, key: &Key) {
        *self.uploads.generations.entry(key.clone()).or_default() += 1;
        self.uploads
            .last_write
            .insert(key.clone(), self.clock.now());
        self.listed_uploading.remove(key);
        self.cancel_upload(key);
    }

    pub(crate) fn cancel_upload(&mut self, key: &Key) {
        if let Some(upload) = self.uploads.running.remove(key) {
            upload.task.abort();
            self.cache
                .upload_slot_withdraw(self.session_id.clone(), upload.id);
            self.cache
                .upload_slot_free(self.session_id.clone(), upload.id);
        }
    }

    /// The pending write at `key` that has no upload, is not uploaded and
    /// has no fault.
    pub(crate) fn uploadable(&self, key: &Key) -> bool {
        self.state
            .tables()
            .pending_changes
            .get(key)
            .is_some_and(|change| {
                change.change == "write" && !change.uploaded && change.fault_code.is_none()
            })
            && !self.uploads.running.contains_key(key)
            && !self.closed.contains(&key.0)
    }

    /// Starts the upload of `key` when it is uploadable; `traceparent` is
    /// the sync point's.
    pub(crate) fn start_upload(&mut self, key: &Key, traceparent: Option<String>) {
        if !self.uploadable(key) {
            return;
        }
        let Some(row) = self.state.tables().files.get(key).cloned() else {
            return;
        };
        let Ok(drive_id) = Id::new(key.0.clone()) else {
            return;
        };
        self.uploads.ids += 1;
        let id = self.uploads.ids;
        let generation = self.uploads.generations.get(key).copied().unwrap_or(0);
        // Content the drive already holds needs no request.
        let mut held: HashSet<String> = self
            .state
            .tables()
            .files
            .values()
            .filter(|each| each.drive_id == key.0)
            .filter_map(|each| each.synced_sha256.clone())
            .collect();
        held.extend(
            self.uploads
                .put
                .iter()
                .filter(|(drive, _)| *drive == key.0)
                .map(|(_, sha256)| sha256.clone()),
        );
        let missing = self.uploads.missing.remove(key);
        if missing && let Some(sha256) = &row.local_sha256 {
            held.remove(sha256);
        }
        let source = match row.content.as_deref() {
            Some("session") => Source::File(self.content_path(&key.0, row.file_id)),
            _ if missing => Source::File(
                self.blobs
                    .join(row.local_sha256.as_deref().unwrap_or_default()),
            ),
            _ => Source::Cache {
                sha256: row.local_sha256.clone().unwrap_or_default(),
                size: row.size.unwrap_or(0),
            },
        };
        let grant = self.cache.upload_slot(self.session_id.clone(), id);
        let transfer = self.transfer.clone();
        let binding = self.binding.subscribe();
        let cache = self.cache.clone();
        let mailbox = self.mailbox.clone();
        let task_key = key.clone();
        let task = tokio::spawn(async move {
            if grant.await.is_err() {
                return;
            }
            let ended = upload(
                source,
                held,
                drive_id,
                binding,
                transfer,
                cache,
                traceparent,
            )
            .await;
            let _ = mailbox.send(Message::UploadEnded {
                key: task_key,
                id,
                generation,
                ended,
            });
        });
        self.uploads.running.insert(
            key.clone(),
            Uploading {
                id,
                generation,
                task: task.abort_handle(),
            },
        );
    }

    /// An upload ended; an end of an older write is dropped.
    pub(crate) async fn upload_ended(
        &mut self,
        key: Key,
        id: u64,
        generation: u64,
        ended: Uploaded,
    ) {
        let current = self
            .uploads
            .running
            .get(&key)
            .is_some_and(|upload| upload.id == id && upload.generation == generation);
        if !current {
            return;
        }
        self.uploads.running.remove(&key);
        self.cache.upload_slot_free(self.session_id.clone(), id);
        let Some(change) = self.state.tables().pending_changes.get(&key).cloned() else {
            return;
        };
        match ended {
            Uploaded::Done { sha256, size } => {
                self.uploads.put.insert((key.0.clone(), sha256.clone()));
                self.uploads
                    .sizes
                    .insert((key.0.clone(), sha256.clone()), size);
                let _ = self
                    .write(Write {
                        pending: vec![PendingChange {
                            content_sha256: Some(sha256),
                            uploaded: true,
                            ..change
                        }],
                        ..Write::default()
                    })
                    .await;
            }
            Uploaded::ReRead => self.start_upload(&key, None),
            Uploaded::TooLarge(message) => {
                self.reject_limit(&key, &change, message).await;
            }
            Uploaded::Fault { status, code } => {
                self.fault(&key, "upload_content", status, &code).await;
            }
            Uploaded::Unreadable => {
                self.fault(&key, "upload_content", 0, "content_unreadable")
                    .await;
            }
        }
        // A sync point whose wait this ends commits its covered changes.
        self.sync_points_progress().await;
        self.changes_ready().await;
        self.send_reports();
    }

    /// A fault keeps the change pending with its code; it is tried again
    /// only at a retry point.
    pub(crate) async fn fault(
        &mut self,
        key: &Key,
        operation: &'static str,
        status: u16,
        code: &str,
    ) {
        let Some(change) = self.state.tables().pending_changes.get(key).cloned() else {
            return;
        };
        observe::warn!(
            "drive.upload_fault",
            drive_transfer.operation = observe::Id(operation),
            http.response.status_code = i64::from(status),
            error.code = observe::Code(crate::upload::declared(code))
        );
        self.listed_uploading.remove(key);
        self.faulted(key);
        let _ = self
            .write(Write {
                pending: vec![PendingChange {
                    fault_code: Some(code.to_owned()),
                    ..change
                }],
                ..Write::default()
            })
            .await;
    }

    /// A retry point: every faulted change is tried again.
    pub(crate) async fn retry_faults(&mut self, keys: Option<&HashSet<Key>>) {
        let cleared: Vec<PendingChange> = self
            .state
            .tables()
            .pending_changes
            .iter()
            .filter(|(key, change)| {
                change.fault_code.is_some() && keys.is_none_or(|keys| keys.contains(*key))
            })
            .map(|(_, change)| PendingChange {
                fault_code: None,
                ..change.clone()
            })
            .collect();
        if !cleared.is_empty() {
            let _ = self
                .write(Write {
                    pending: cleared,
                    ..Write::default()
                })
                .await;
        }
    }

    /// Starts the uploads due by the quiet period.
    pub(crate) fn quiet_uploads(&mut self) {
        if self.uploads.running_tasks.is_empty() {
            return;
        }
        let now = self.clock.now();
        let keys: Vec<(Key, Duration)> = self
            .uploads
            .last_write
            .iter()
            .map(|(key, at)| (key.clone(), *at))
            .collect();
        for (key, at) in keys {
            if !self.uploadable(&key) {
                self.uploads.last_write.remove(&key);
            } else if at + QUIET <= now {
                self.uploads.last_write.remove(&key);
                self.start_upload(&key, None);
            }
        }
    }

    /// The next time the quiet period of a written file ends.
    pub(crate) fn quiet_wake(&self) -> Option<Duration> {
        if self.uploads.running_tasks.is_empty() {
            return None;
        }
        self.uploads
            .last_write
            .iter()
            .filter(|(key, _)| self.uploadable(key))
            .map(|(_, at)| *at + QUIET)
            .min()
    }

    /// The `exec_command` task `task_id` started.
    pub(crate) fn task_started(&mut self, task_id: String) {
        self.uploads.running_tasks.insert(task_id);
    }

    /// The task `task_id` exited: the uploads of its changes start at once.
    pub(crate) fn task_exited(&mut self, task_id: &str) {
        self.uploads.running_tasks.remove(task_id);
        let keys: Vec<Key> = self
            .state
            .tables()
            .pending_changes
            .iter()
            .filter(|(_, change)| change.task_id.as_deref() == Some(task_id))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.start_upload(&key, None);
        }
    }
}

/// Where an upload's bytes are.
enum Source {
    File(PathBuf),
    /// A renamed file whose content is the cache blob of this hash.
    Cache {
        sha256: String,
        size: i64,
    },
}

/// Flushes and hashes the content, then sends the PUT unless the drive
/// holds it.
async fn upload(
    source: Source,
    held: HashSet<String>,
    drive_id: Id,
    binding: crate::transfer::BindingWatch,
    transfer: Arc<dyn crate::DriveTransfer>,
    cache: crate::cache::ContentCache,
    traceparent: Option<String>,
) -> Uploaded {
    let (path, sha256, size) = match source {
        Source::Cache { sha256, size } => return Uploaded::Done { sha256, size },
        Source::File(path) => {
            let hashed = {
                let path = path.clone();
                tokio::task::spawn_blocking(move || sha256_of(&path)).await
            };
            match hashed {
                Ok(Ok((sha256, size))) => (path, sha256, size),
                Ok(Err(_)) | Err(_) => return Uploaded::Unreadable,
            }
        }
    };
    if held.contains(&sha256) {
        return Uploaded::Done { sha256, size };
    }
    let Ok(hash) = Sha256::new(sha256.clone()) else {
        return Uploaded::Unreadable;
    };
    let request = Request {
        drive_id,
        binding,
        traceparent,
        body: Upload {
            sha256: hash,
            content: Content::File {
                path,
                size: size as u64,
            },
            rate_limited: Some(Arc::new(move || cache.upload_rate_limited())),
        },
    };
    match transfer.upload_content(request).await {
        Ok(_) => Uploaded::Done { sha256, size },
        Err(Ended::ReRead) => Uploaded::ReRead,
        Err(Ended::Decision(body)) => Uploaded::TooLarge(body.message),
        Err(Ended::Permanent { status, code }) => Uploaded::Fault {
            status,
            code: code.unwrap_or("invalid_response").to_owned(),
        },
        Err(Ended::CursorExpired) | Err(Ended::Sink(_)) => Uploaded::Fault {
            status: 0,
            code: "invalid_response".to_owned(),
        },
    }
}

/// The codes `drive.upload_fault` declares; another code is recorded as
/// `invalid_response`.
const DECLARED: [&str; 13] = [
    "content_unreadable",
    "invalid_message",
    "invalid_response",
    "drive_access_denied",
    "not_found",
    "content_not_found",
    "connection_conflict",
    "previous_connection_active",
    "message_conflict",
    "unknown_instruction",
    "request_too_large",
    "range_not_satisfiable",
    "rate_limited",
];

pub(crate) fn declared(code: &str) -> &'static str {
    DECLARED
        .iter()
        .find(|declared| **declared == code)
        .copied()
        .unwrap_or("invalid_response")
}
