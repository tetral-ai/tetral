//! The Drive sync owner of one Session: its sync state, its current
//! binding, and the catch-up of each mounted drive. A catch-up starts at a
//! drive's mount, after every `connected`, at a `drive.changed` above the
//! drive's `applied_seq` and after `410 drive_cursor_expired`; one runs at a
//! time per drive, and a trigger during one makes one more run after it.
//! A check before a command waits until every mounted drive has applied
//! the records through its highest notice and finished a catch-up that
//! began after the latest `connected`. A list request answered permanently
//! unmounts its drive with the failed listing's error.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

use protocol::drive_transfer::{ListChangesResponse, ListFilesResponse, Operation};
use protocol::id::Id;
use protocol::path::DrivePath;
use tokio::sync::{mpsc, oneshot, watch};

use crate::catchup::{self, Record, Version};
use crate::state::{ListingRow, SyncState, Tables, Write};
use crate::transfer::{Binding, DriveTransfer, Ended, ListChanges, ListFiles, Request};

/// A handle on a Session's Drive sync owner.
#[derive(Clone)]
pub struct DriveSync {
    mailbox: mpsc::UnboundedSender<Message>,
}

impl std::fmt::Debug for DriveSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("drive sync")
    }
}

enum Message {
    Connected,
    DriveChanged {
        drive_id: Id,
        seq: i64,
    },
    BindingCurrent(Binding),
    BindingGone,
    Mounted {
        drive_id: Id,
        exclude: Vec<String>,
    },
    Detached {
        drive_id: Id,
    },
    Check(oneshot::Sender<()>),
    Tables(oneshot::Sender<Tables>),
    Unmounted(oneshot::Sender<BTreeMap<String, String>>),
    Answer {
        drive_id: Id,
        generation: u64,
        answer: Answer,
    },
}

enum Answer {
    Files {
        first: bool,
        result: Result<ListFilesResponse, Ended>,
    },
    Changes(Result<ListChangesResponse, Ended>),
}

impl DriveSync {
    /// Starts the owner of `state` with no current binding.
    pub fn spawn(state: SyncState, transfer: Arc<dyn DriveTransfer>) -> DriveSync {
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let owner = Owner {
            state,
            transfer,
            binding: watch::Sender::new(None),
            mailbox: mailbox.clone(),
            mounts: HashMap::new(),
            notified: HashMap::new(),
            unmounted: BTreeMap::new(),
            connected: 0,
            generations: 0,
            checks: Vec::new(),
        };
        tokio::spawn(owner.run(inbox));
        DriveSync { mailbox }
    }

    /// The connection owner saw `connected`, resumed or not.
    pub fn connected(&self) {
        self.send(Message::Connected);
    }

    pub fn drive_changed(&self, drive_id: Id, seq: i64) {
        self.send(Message::DriveChanged { drive_id, seq });
    }

    /// The Session's binding on the current connection, from the ACK of its
    /// `prepared` report.
    pub fn binding_current(&self, binding: Binding) {
        self.send(Message::BindingCurrent(binding));
    }

    /// The Session has no current binding: every request waits.
    pub fn binding_gone(&self) {
        self.send(Message::BindingGone);
    }

    /// `drive_id` is mounted, with the `drive_exclude` entries this
    /// computer has for it; its catch-up starts.
    pub fn mounted(&self, drive_id: Id, exclude: Vec<String>) {
        self.send(Message::Mounted { drive_id, exclude });
    }

    pub fn detached(&self, drive_id: Id) {
        self.send(Message::Detached { drive_id });
    }

    /// Ends once every mounted drive has applied the records through its
    /// highest notice, runs no catch-up and has finished one that began
    /// after the latest `connected`.
    pub async fn check(&self) {
        let (reply, done) = oneshot::channel();
        self.send(Message::Check(reply));
        let _ = done.await;
    }

    /// The tables as the last committed write left them.
    pub async fn tables(&self) -> Tables {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Tables(reply));
        answer.await.unwrap_or_default()
    }

    /// The drives a failed listing unmounted, with its error.
    pub async fn unmounted(&self) -> BTreeMap<String, String> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Unmounted(reply));
        answer.await.unwrap_or_default()
    }

    fn send(&self, message: Message) {
        let _ = self.mailbox.send(message);
    }
}

/// A mounted drive and its catch-up.
struct Mount {
    exclude: Vec<String>,
    /// Tells the answers of this mount from those of an earlier one.
    generation: u64,
    running: Option<Running>,
    again: bool,
    /// The `connected` count when the last finished catch-up began.
    finished_began: Option<u64>,
}

struct Running {
    began: u64,
    /// The first page's `start_seq` of a listing in progress.
    start_seq: Option<i64>,
}

struct Owner {
    state: SyncState,
    transfer: Arc<dyn DriveTransfer>,
    binding: watch::Sender<Option<Binding>>,
    mailbox: mpsc::UnboundedSender<Message>,
    mounts: HashMap<String, Mount>,
    /// The highest `drive.changed` seq received for each drive.
    notified: HashMap<String, i64>,
    unmounted: BTreeMap<String, String>,
    connected: u64,
    generations: u64,
    checks: Vec<oneshot::Sender<()>>,
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)
        })
}

impl Owner {
    async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<Message>) {
        while let Some(message) = inbox.recv().await {
            self.handle(message).await;
            self.answer_checks();
        }
    }

    async fn handle(&mut self, message: Message) {
        match message {
            Message::Connected => {
                self.connected += 1;
                let drives: Vec<String> = self.mounts.keys().cloned().collect();
                for drive in drives {
                    self.trigger(&drive);
                }
            }
            Message::DriveChanged { drive_id, seq } => {
                let drive = drive_id.as_str().to_owned();
                let notified = self.notified.entry(drive.clone()).or_default();
                *notified = (*notified).max(seq);
                if seq > self.applied_seq(&drive) && self.mounts.contains_key(&drive) {
                    self.trigger(&drive);
                }
            }
            Message::BindingCurrent(binding) => {
                self.binding.send_replace(Some(binding));
            }
            Message::BindingGone => {
                self.binding.send_replace(None);
            }
            Message::Mounted { drive_id, exclude } => {
                let drive = drive_id.as_str().to_owned();
                self.generations += 1;
                self.unmounted.remove(&drive);
                self.mounts.insert(
                    drive.clone(),
                    Mount {
                        exclude,
                        generation: self.generations,
                        running: None,
                        again: false,
                        finished_began: None,
                    },
                );
                self.trigger(&drive);
            }
            Message::Detached { drive_id } => {
                self.mounts.remove(drive_id.as_str());
            }
            Message::Check(reply) => self.checks.push(reply),
            Message::Tables(reply) => {
                let _ = reply.send(self.state.tables().clone());
            }
            Message::Unmounted(reply) => {
                let _ = reply.send(self.unmounted.clone());
            }
            Message::Answer {
                drive_id,
                generation,
                answer,
            } => {
                let drive = drive_id.as_str().to_owned();
                let current = self
                    .mounts
                    .get(&drive)
                    .is_some_and(|mount| mount.generation == generation && mount.running.is_some());
                if current {
                    self.answered(&drive, answer).await;
                }
            }
        }
    }

    fn applied_seq(&self, drive: &str) -> i64 {
        self.state
            .tables()
            .drive_cursor
            .get(drive)
            .map_or(0, |cursor| cursor.applied_seq)
    }

    /// Starts a catch-up of `drive`, or one more after the running one.
    fn trigger(&mut self, drive: &str) {
        let connected = self.connected;
        let Some(mount) = self.mounts.get_mut(drive) else {
            return;
        };
        if mount.running.is_some() {
            mount.again = true;
            return;
        }
        mount.running = Some(Running {
            began: connected,
            start_seq: None,
        });
        if self.state.tables().drive_cursor.contains_key(drive) {
            let after_seq = self.applied_seq(drive);
            self.list_changes(drive, after_seq, None);
        } else {
            self.list_files(drive, None);
        }
    }

    fn request<B>(&self, drive: &str, body: B) -> Request<B> {
        Request {
            drive_id: Id::new(drive).expect("a mounted drive's id"),
            binding: self.binding.subscribe(),
            traceparent: None,
            body,
        }
    }

    fn generation(&self, drive: &str) -> u64 {
        self.mounts.get(drive).map_or(0, |mount| mount.generation)
    }

    fn list_files(&self, drive: &str, after_path: Option<DrivePath>) {
        let first = after_path.is_none();
        let request = self.request(drive, ListFiles { after_path });
        let (transfer, mailbox) = (self.transfer.clone(), self.mailbox.clone());
        let (drive_id, generation) = (request.drive_id.clone(), self.generation(drive));
        tokio::spawn(async move {
            let result = transfer.list_files(request).await;
            let _ = mailbox.send(Message::Answer {
                drive_id,
                generation,
                answer: Answer::Files { first, result },
            });
        });
    }

    fn list_changes(&self, drive: &str, after_seq: i64, after_index: Option<i64>) {
        let request = self.request(
            drive,
            ListChanges {
                after_seq,
                after_index,
            },
        );
        let (transfer, mailbox) = (self.transfer.clone(), self.mailbox.clone());
        let (drive_id, generation) = (request.drive_id.clone(), self.generation(drive));
        tokio::spawn(async move {
            let result = transfer.list_changes(request).await;
            let _ = mailbox.send(Message::Answer {
                drive_id,
                generation,
                answer: Answer::Changes(result),
            });
        });
    }

    async fn answered(&mut self, drive: &str, answer: Answer) {
        let outcome = match answer {
            Answer::Files { first, result } => match result {
                Ok(page) => self.files_page(drive, first, page).await,
                Err(Ended::CursorExpired) => Err("list files: invalid answer".to_owned()),
                Err(ended) => Err(failed("list files", &ended)),
            },
            Answer::Changes(result) => match result {
                Ok(page) => self.changes_page(drive, page).await,
                Err(Ended::CursorExpired) => {
                    // A relisting, within the same catch-up.
                    self.list_files(drive, None);
                    Ok(())
                }
                Err(ended) => Err(failed("list changes", &ended)),
            },
        };
        if let Err(error) = outcome {
            // The mount is detached and the drive is unmounted.
            self.mounts.remove(drive);
            self.unmounted.insert(drive.to_owned(), error);
        }
    }

    async fn files_page(
        &mut self,
        drive: &str,
        first: bool,
        page: ListFilesResponse,
    ) -> Result<(), String> {
        let exclude = self.exclude(drive);
        let mut write = Write::default();
        if first {
            write.listing_removed_drives.push(drive.to_owned());
            let start_seq = page.start_seq.map_or(0, |seq| seq.get());
            if let Some(running) = self
                .mounts
                .get_mut(drive)
                .and_then(|mount| mount.running.as_mut())
            {
                running.start_seq = Some(start_seq);
            }
        }
        write.listing = page
            .files
            .iter()
            .filter(|file| !catchup::excluded(file.path.as_str(), &exclude))
            .map(|file| ListingRow {
                drive_id: drive.to_owned(),
                path: file.path.as_str().to_owned(),
                version_id: file.version_id.as_str().to_owned(),
                content_sha256: file.content_sha256.as_str().to_owned(),
                size_bytes: file.size_bytes.get(),
                executable: file.executable,
            })
            .collect();
        self.write(write).await?;
        if page.has_more {
            let last = page.files.last().map(|file| file.path.clone());
            self.list_files(drive, last);
            return Ok(());
        }
        let start_seq = self
            .mounts
            .get(drive)
            .and_then(|mount| mount.running.as_ref())
            .and_then(|running| running.start_seq)
            .unwrap_or(0);
        let write = catchup::finish_listing(
            self.state.tables(),
            drive,
            start_seq,
            rand::random::<i64>(),
            now_ns(),
        );
        self.write(write).await?;
        self.list_changes(drive, start_seq, None);
        Ok(())
    }

    async fn changes_page(&mut self, drive: &str, page: ListChangesResponse) -> Result<(), String> {
        let exclude = self.exclude(drive);
        let seqs: Vec<i64> = page.records.iter().map(|record| record.seq.get()).collect();
        let applied = catchup::page_applied_seq(self.applied_seq(drive), &seqs, page.has_more);
        let records: Vec<Record> = page
            .records
            .iter()
            .filter(|record| !catchup::excluded(record.path.as_str(), &exclude))
            .map(|record| Record {
                path: record.path.as_str().to_owned(),
                state: match (record.operation, &record.content_sha256.0) {
                    (Operation::Deleted, _) | (_, None) => None,
                    (_, Some(sha256)) => Some(Version {
                        version_id: record.version_id.as_str().to_owned(),
                        sha256: sha256.as_str().to_owned(),
                        size: record.size_bytes.get(),
                        executable: record.executable,
                    }),
                },
            })
            .collect();
        let write = catchup::apply_page(self.state.tables(), drive, &records, applied, now_ns());
        self.write(write).await?;
        if page.has_more {
            let last = page
                .records
                .last()
                .expect("a page with more holds a record");
            self.list_changes(drive, last.seq.get(), Some(last.index.get()));
            return Ok(());
        }
        let Some(mount) = self.mounts.get_mut(drive) else {
            return Ok(());
        };
        let running = mount
            .running
            .take()
            .expect("an answer of a running catch-up");
        mount.finished_began = Some(running.began);
        if std::mem::take(&mut mount.again) {
            self.trigger(drive);
        }
        Ok(())
    }

    fn exclude(&self, drive: &str) -> Vec<String> {
        self.mounts
            .get(drive)
            .map(|mount| mount.exclude.clone())
            .unwrap_or_default()
    }

    async fn write(&mut self, write: Write) -> Result<(), String> {
        self.state
            .apply(write)
            .await
            .map_err(|error| format!("sync state: {error}"))
    }

    fn ready(&self) -> bool {
        self.mounts.iter().all(|(drive, mount)| {
            mount.running.is_none()
                && self.applied_seq(drive) >= self.notified.get(drive).copied().unwrap_or(0)
                && mount.finished_began == Some(self.connected)
        })
    }

    fn answer_checks(&mut self) {
        if !self.checks.is_empty() && self.ready() {
            for check in self.checks.drain(..) {
                let _ = check.send(());
            }
        }
    }
}

/// The error of a failed listing: `<request>: <status> <code>`, or
/// `<request>: invalid answer` for an answer that fails its schema.
fn failed(request: &str, ended: &Ended) -> String {
    match ended {
        Ended::Permanent {
            status,
            code: Some(code),
        } => format!("{request}: {status} {code}"),
        _ => format!("{request}: invalid answer"),
    }
}
