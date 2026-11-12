//! Content uploads of pending writes: their triggers (2 seconds without a
//! write while an `exec_command` task of the Session runs, the exit of the
//! writing task, a sync point, the binding becoming current after a start or
//! on a new connection, a final upload), the machine's upload slots, hashing
//! and the PUT's answers. A write to a file cancels its upload, waiting for a
//! slot or running; the next trigger uploads the new content.

use std::collections::{BTreeSet, HashMap, HashSet};
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

/// The clock time of each path's last write in this run, also kept in time
/// order: the owner reads the earliest before each message.
#[derive(Default)]
pub(crate) struct LastWrites {
    at: HashMap<Key, Duration>,
    order: BTreeSet<(Duration, Key)>,
}

impl LastWrites {
    pub fn insert(&mut self, key: Key, at: Duration) {
        if let Some(before) = self.at.insert(key.clone(), at) {
            self.order.remove(&(before, key.clone()));
        }
        self.order.insert((at, key));
    }

    pub fn remove(&mut self, key: &Key) {
        if let Some(at) = self.at.remove(key) {
            self.order.remove(&(at, key.clone()));
        }
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&Key) -> bool) {
        self.at.retain(|key, _| keep(key));
        self.order.retain(|(_, key)| keep(key));
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Duration)> {
        self.at.iter()
    }

    /// The earliest last write.
    pub fn earliest(&self) -> Option<Duration> {
        self.order.first().map(|(at, _)| *at)
    }
}

/// An upload in progress: waiting for its slot, hashing or sending.
pub(crate) struct Uploading {
    pub id: u64,
    /// The write generation of the file it uploads.
    pub generation: u64,
    pub task: AbortHandle,
    pub span: tracing::Span,
}

/// How an upload ended.
#[derive(Debug)]
pub(crate) enum Uploaded {
    Rebind(Option<crate::transfer::Binding>),
    /// The drive holds the content of this hash and size.
    Done {
        sha256: String,
        size: i64,
    },
    /// `400 content_hash_mismatch`: the file is read again.
    ReRead,
    /// `413 file_too_large`, with the error's message.
    TooLarge(String),
    /// A permanent answer: a fault.
    Fault {
        status: u16,
        code: String,
    },
    /// The content file could not be read, with the cause.
    Unreadable(observe::Cause),
}

/// The upload state of one Session.
#[derive(Default)]
pub(crate) struct Uploads {
    pub running: HashMap<Key, Uploading>,
    pub ids: u64,
    /// Bumped by every write of a path, so that an older upload's end is
    /// dropped.
    pub generations: HashMap<Key, u64>,
    pub last_write: LastWrites,
    /// The hashes a PUT to each drive answered `200` during this run.
    pub put: HashSet<(String, String)>,
    /// The size of each content an upload of a drive hashed, by drive and
    /// hash.
    pub sizes: HashMap<(String, String), i64>,
    /// The `exec_command` tasks of the Session that run.
    pub running_tasks: HashSet<String>,
    /// The writes whose content a commit answered `409 content_missing`:
    /// their next upload sends it, a `cache` file's from its blob.
    pub missing: HashMap<Key, String>,
}

fn sha256_of(
    path: &std::path::Path,
    syncer: Option<&crate::content_syncs::Syncer>,
) -> std::io::Result<(String, i64)> {
    let mut file = std::fs::File::open(path)?;
    if let Some(syncer) = syncer {
        syncer(&file)?;
    }
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
    /// and sync points cover it again.
    pub(crate) fn local_change(&mut self, key: &Key) {
        *self.uploads.generations.entry(key.clone()).or_default() += 1;
        self.uploads
            .last_write
            .insert(key.clone(), self.clock.now());
        self.uncovered.remove(key);
        self.cancel_upload(key);
    }

    pub(crate) fn cancel_upload(&mut self, key: &Key) {
        if let Some(upload) = self.uploads.running.remove(key) {
            self.cache
                .upload_slot_withdraw(self.session_id.clone(), upload.id);
            upload.task.abort();
        }
    }

    /// Withdraws the entire queue before workers can return their slots,
    /// so a cancelled upload cannot start a queued sibling.
    pub(crate) fn cancel_uploads(&mut self) {
        self.cache.upload_slots_withdraw(self.session_id.clone());
        for (_, upload) in self.uploads.running.drain() {
            upload.task.abort();
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
            && self.binding.borrow().is_some()
            && !self.uploads.running.contains_key(key)
            && !self.closed.contains(&key.0)
    }

    /// Starts the upload of `key` when it is uploadable; `traceparent` is
    /// the sync point's.
    pub(crate) fn start_upload(&mut self, key: &Key, traceparent: Option<String>) {
        if !self.uploadable(key) {
            return;
        }
        let Ok(drive_id) = Id::new(key.0.clone()) else {
            return;
        };
        let span = self.transfer_span(&key.0, traceparent.as_deref());
        self.uploads.ids += 1;
        let id = self.uploads.ids;
        let generation = self.uploads.generations.get(key).copied().unwrap_or(0);
        let grant = self.cache.upload_slot(self.session_id.clone(), id);
        let transfer = self.transfer.clone();
        let binding = self.binding.subscribe();
        let cache = self.cache.clone();
        let mailbox = self.mailbox.clone();
        let task_key = key.clone();
        let slot = UploadSlot {
            cache: cache.clone(),
            session_id: self.session_id.clone(),
            id,
        };
        let task = tokio::spawn(async move {
            let _slot = slot;
            if grant.await.is_err() {
                return;
            }
            // Queued uploads retain only their key. Build the content and
            // known-hash snapshot after receiving one of the eight grants.
            let (reply, snapshot) = tokio::sync::oneshot::channel();
            if mailbox
                .send(Message::UploadGranted {
                    key: task_key.clone(),
                    id,
                    generation,
                    reply,
                })
                .is_err()
            {
                return;
            }
            let ended = match snapshot.await {
                Ok(Some(input)) => {
                    upload(
                        input.source,
                        input.held,
                        drive_id,
                        binding,
                        transfer,
                        cache,
                        traceparent,
                    )
                    .await
                }
                _ => Uploaded::Unreadable(observe::Cause::system(
                    "the upload's content source was gone when its turn came",
                )),
            };
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
                span,
            },
        );
    }

    /// Captures only the generation that still owns this granted upload.
    pub(crate) fn upload_input(&self, key: &Key, id: u64, generation: u64) -> Option<UploadInput> {
        if !self
            .uploads
            .running
            .get(key)
            .is_some_and(|upload| upload.id == id && upload.generation == generation)
        {
            return None;
        }
        let row = self.state.tables().files.get(key)?;
        let mut held: HashSet<String> = self
            .state
            .tables()
            .files
            .values()
            .filter(|row| row.drive_id == key.0)
            .filter_map(|row| row.synced_sha256.clone())
            .collect();
        held.extend(
            self.uploads
                .put
                .iter()
                .filter(|(drive, _)| *drive == key.0)
                .map(|(_, hash)| hash.clone()),
        );
        let missing = self.uploads.missing.get(key);
        if let Some(hash) = missing {
            // A sibling's stale version is not proof that this blob still exists.
            held.remove(hash);
        }
        let source = match row.content.as_deref() {
            Some("session") => Source::File {
                path: self.content_path(&key.0, row.file_id),
                syncer: self
                    .content_syncs
                    .unsynced(&key.0, row.file_id)
                    .map(|_| self.syncer.clone()),
            },
            _ if missing.is_some() => Source::File {
                path: self
                    .blobs
                    .join(row.local_sha256.as_deref().unwrap_or_default()),
                syncer: None,
            },
            _ => Source::Cache {
                sha256: row.local_sha256.clone().unwrap_or_default(),
                size: row.size.unwrap_or(0),
            },
        };
        Some(UploadInput { source, held })
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
        let span = self.uploads.running.remove(&key).unwrap().span;
        let Some(change) = self.state.tables().pending_changes.get(&key).cloned() else {
            return;
        };
        match ended {
            Uploaded::Done { sha256, size } => {
                self.uploads.missing.remove(&key);
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
            Uploaded::Rebind(binding) => {
                if *self.binding.borrow() == binding {
                    self.binding.send_replace(None);
                    self.cancel_uploads();
                } else {
                    self.start_upload(&key, None);
                }
            }
            Uploaded::TooLarge(message) => {
                self.reject_limit(&key, &change, message).await;
            }
            Uploaded::Fault { status, code } => {
                self.fault_changes(
                    std::slice::from_ref(&key),
                    "upload_content",
                    status,
                    &code,
                    None,
                    &span,
                )
                .await;
            }
            Uploaded::Unreadable(cause) => {
                self.fault_changes(
                    std::slice::from_ref(&key),
                    "upload_content",
                    0,
                    "content_unreadable",
                    Some(cause),
                    &span,
                )
                .await;
            }
        }
        // A sync point whose wait this ends commits its covered changes.
        self.sync_points_progress().await;
        self.changes_ready().await;
        self.send_reports();
    }

    /// A fault keeps the change pending with its code; it is tried again
    /// only at a retry point. `cause` is that of a fault of this CLI's own;
    /// one that Engine answered has none.
    pub(crate) async fn fault_changes(
        &mut self,
        keys: &[Key],
        operation: &'static str,
        status: u16,
        code: &str,
        cause: Option<observe::Cause>,
        span: &tracing::Span,
    ) {
        let pending: Vec<_> = keys
            .iter()
            .filter_map(|key| self.state.tables().pending_changes.get(key))
            .map(|change| PendingChange {
                fault_code: Some(code.to_owned()),
                ..change.clone()
            })
            .collect();
        if pending.is_empty() {
            return;
        }
        span.in_scope(|| match cause {
            Some(cause) => observe::warn!(
                "drive.upload_fault",
                drive_transfer.operation = observe::Id(operation),
                drive_transfer.changes = pending.len() as i64,
                http.response.status_code = i64::from(status),
                error.code = observe::Code(crate::upload::declared(code)),
                error.message = cause
            ),
            None => observe::warn!(
                "drive.upload_fault",
                drive_transfer.operation = observe::Id(operation),
                drive_transfer.changes = pending.len() as i64,
                http.response.status_code = i64::from(status),
                error.code = observe::Code(crate::upload::declared(code))
            ),
        });
        for key in keys {
            self.uncovered.remove(key);
            self.faulted(key);
        }
        let _ = self
            .write(Write {
                pending,
                ..Write::default()
            })
            .await;
    }

    pub(crate) fn transfer_span(&self, drive: &str, traceparent: Option<&str>) -> tracing::Span {
        use observe::Identity;
        let binding = self.binding.borrow();
        let mut identities = vec![
            (Identity::Session, self.session_id.as_str()),
            (Identity::Drive, drive),
        ];
        if let Some(binding) = binding.as_ref() {
            identities.push((Identity::Binding, binding.binding_id.as_str()));
            identities.push((Identity::Connection, binding.connection_id.as_str()));
        }
        let trace = traceparent.and_then(observe::TraceContext::parse);
        observe::correlation_span(&identities, trace.as_ref())
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

    /// The next time the quiet period of a written file ends; whether the
    /// file uploads is decided then.
    pub(crate) fn quiet_wake(&self) -> Option<Duration> {
        if self.uploads.running_tasks.is_empty() {
            return None;
        }
        self.uploads.last_write.earliest().map(|at| at + QUIET)
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

/// Drops after the upload future (and its HTTP attempt) is cancelled or ends.
/// Withdrawing a queued request is separate from returning an actual grant.
struct UploadSlot {
    cache: crate::cache::ContentCache,
    session_id: protocol::id::SafeId,
    id: u64,
}

impl Drop for UploadSlot {
    fn drop(&mut self) {
        self.cache
            .upload_slot_free(self.session_id.clone(), self.id);
    }
}

/// The bounded snapshot retained by a granted upload, never a queued one.
pub(crate) struct UploadInput {
    source: Source,
    held: HashSet<String>,
}

/// Where an upload's bytes are.
enum Source {
    /// `syncer` is set when the file holds a write no sync made durable.
    File {
        path: PathBuf,
        syncer: Option<crate::content_syncs::Syncer>,
    },
    /// A renamed file whose content is the cache blob of this hash.
    Cache { sha256: String, size: i64 },
}

/// Syncs the content if it needs it, hashes it, then sends the PUT unless
/// the drive holds it.
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
        Source::File { path, syncer } => {
            let hashed = {
                let path = path.clone();
                tokio::task::spawn_blocking(move || sha256_of(&path, syncer.as_ref())).await
            };
            match hashed {
                Ok(Ok((sha256, size))) => (path, sha256, size),
                Ok(Err(error)) => {
                    return Uploaded::Unreadable(observe::Cause::system(&error.to_string()));
                }
                Err(ended) => {
                    return Uploaded::Unreadable(observe::Cause::system(&ended.to_string()));
                }
            }
        }
    };
    if held.contains(&sha256) {
        return Uploaded::Done { sha256, size };
    }
    let Ok(hash) = Sha256::new(sha256.clone()) else {
        return Uploaded::Unreadable(observe::Cause::system(
            "the content's hash is not a SHA-256",
        ));
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
        Err(Ended::Rebind(binding)) => Uploaded::Rebind(binding),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_earliest_last_write_follows_rewrites_and_removals() {
        let key = |path: &str| ("drive_a".to_owned(), path.to_owned());
        let second = Duration::from_secs;
        let mut writes = LastWrites::default();
        writes.insert(key("a"), second(1));
        writes.insert(key("b"), second(2));
        writes.insert(key("a"), second(3));
        assert_eq!(
            writes.earliest(),
            Some(second(2)),
            "a rewrite moves its time"
        );
        writes.remove(&key("b"));
        assert_eq!(writes.earliest(), Some(second(3)));
        writes.insert(key("c"), second(4));
        writes.retain(|(_, path)| path != "a");
        assert_eq!(writes.earliest(), Some(second(4)));
        writes.remove(&key("c"));
        assert_eq!(writes.earliest(), None);
        assert_eq!(writes.iter().count(), 0);
    }
}
