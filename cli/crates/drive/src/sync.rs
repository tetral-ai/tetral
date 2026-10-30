//! The Drive sync owner of one Session: its sync state, its current
//! binding, and the catch-up of each mounted drive. A catch-up starts at a
//! drive's mount, after every `connected`, at a `drive.changed` above the
//! drive's `applied_seq` and after `410 drive_cursor_expired`; one runs at a
//! time per drive, and a trigger during one makes one more run after it.
//! A check before a command waits until every mounted drive has applied
//! the records through its highest notice and finished a catch-up that
//! began after the latest `connected`. A list request answered permanently
//! unmounts its drive with the failed listing's error.
//!
//! The owner also answers the mounts' operations ([`crate::ops`]). An
//! operation on a drive whose first listing has not finished waits for it,
//! and a read whose blob is missing waits for a running catch-up of its
//! drive; a detached drive drops them, which fails them with `EIO`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use protocol::drive_transfer::CommitResponse;
use protocol::drive_transfer::{ListChangesResponse, ListFilesResponse, Operation};
use protocol::id::{Id, SafeId};
use protocol::path::DrivePath;
use protocol::ports::TaskOwnership;
use rustix::io::Errno;
use state::clock::Clock;
use tokio::sync::{mpsc, oneshot, watch};

use crate::cache::ContentCache;
use crate::catchup::{self, Record, Version};
use crate::commit::{Flight, Rejected};
use crate::mount::{Bound, Failure, Mounted, Mounting, Registry};
use crate::ops::{Done, Notice, Op};
use crate::paths;
use crate::state::{Key, ListingRow, SyncState, Tables, Write};
use crate::sync_point::{SyncOutcome, SyncPoint, SyncPointRequest};
use crate::transfer::{Binding, DriveTransfer, Ended, ListChanges, ListFiles, Request};
use crate::upload::{Uploaded, Uploads};

/// What a Session's Drive sync owner is built with.
pub struct SyncConfig {
    pub session_id: SafeId,
    /// The canonical state root.
    pub root: PathBuf,
    pub state: SyncState,
    pub transfer: Arc<dyn DriveTransfer>,
    pub cache: ContentCache,
    pub tasks: Arc<dyn TaskOwnership>,
    pub mounting: Mounting,
    pub registry: Arc<dyn Registry>,
    pub clock: Clock,
    /// The report outbox, for drive sync reports.
    pub reports: Arc<dyn protocol::ports::ReportSink>,
}

/// A handle on a Session's Drive sync owner.
#[derive(Clone)]
pub struct DriveSync {
    pub(crate) mailbox: mpsc::UnboundedSender<Message>,
    pub(crate) session_id: SafeId,
    pub(crate) cache: ContentCache,
    pub(crate) tasks: Arc<dyn TaskOwnership>,
    pub(crate) clock: Clock,
    /// The drives the Session's binding binds, as last mounted.
    bound: Arc<std::sync::Mutex<Vec<Bound>>>,
    /// The new mount failures of each `exec_command`'s check, for its first
    /// report.
    first_failures: Arc<std::sync::Mutex<HashMap<String, Vec<Failure>>>>,
    /// `sessions/<session_id>` under the canonical root.
    session_dir: PathBuf,
    mounting: Mounting,
    pub(crate) final_ids: Arc<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for DriveSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("drive sync")
    }
}

pub(crate) type Reply = oneshot::Sender<Result<Done, Errno>>;

pub(crate) enum Message {
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
    Op {
        drive_id: String,
        op: Op,
        reply: Reply,
    },
    /// A read of a [`Source::Cache`] ended.
    ReadEnded(u64),
    /// The reads waiting for blobs: machine path, hash and size.
    Reads(oneshot::Sender<Vec<(PathBuf, String, u64)>>),
    Notices {
        task_id: String,
        reply: oneshot::Sender<Vec<Notice>>,
    },
    /// Mounts each bound drive that is not mounted; the failures.
    CheckMounts {
        bound: Vec<Bound>,
        reply: oneshot::Sender<Vec<Failure>>,
    },
    /// A check before a command begins: the held failures, and the failed
    /// listings until its end go to it.
    BeginCheck(oneshot::Sender<(u64, Vec<Failure>)>),
    /// The check ends: `earlier` and its failed listings, those that are new.
    EndCheck {
        check: u64,
        earlier: Vec<Failure>,
        reply: oneshot::Sender<Vec<Failure>>,
    },
    /// The new failures among `failures`, judged against the rows.
    Judge {
        failures: Vec<Failure>,
        reply: oneshot::Sender<Vec<Failure>>,
    },
    /// These failures were reported: their rows hold their texts.
    Reported(Vec<Failure>),
    Detach(String),
    DetachAll,
    UploadEnded {
        key: Key,
        id: u64,
        generation: u64,
        ended: Uploaded,
    },
    CommitAnswered {
        drive: String,
        id: u64,
        answer: Result<CommitResponse, Ended>,
    },
    SyncPoint {
        request: SyncPointRequest,
        began: std::time::Duration,
        reply: oneshot::Sender<SyncOutcome>,
    },
    TaskStarted(String),
    TaskExited(String),
    FinalUpload {
        id: u64,
        request: crate::final_upload::FinalUploadRequest,
        reply: oneshot::Sender<Vec<protocol::connection::LostChange>>,
    },
    /// The final upload `id` ends at once in keep mode.
    FinalKeep(u64),
    FinalRelease(u64),
    RemoveDrive {
        drive: String,
        reply: oneshot::Sender<()>,
    },
    Close(oneshot::Sender<()>),
}

pub(crate) enum Answer {
    Files {
        first: bool,
        result: Result<ListFilesResponse, Ended>,
    },
    Changes(Result<ListChangesResponse, Ended>),
}

impl DriveSync {
    /// Starts the owner of `config.state` with no current binding.
    pub fn spawn(config: SyncConfig) -> DriveSync {
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let inodes = config
            .state
            .tables()
            .files
            .values()
            .map(|row| ((row.drive_id.clone(), row.file_id), row.path.clone()))
            .collect();
        // The mount points are named as `/proc/self/mountinfo` lists them.
        let root = config
            .root
            .canonicalize()
            .unwrap_or_else(|_| config.root.clone());
        let owner = Owner {
            state: config.state,
            transfer: config.transfer,
            cache: config.cache.clone(),
            clock: config.clock.clone(),
            uploads: Uploads::default(),
            listed: HashSet::new(),
            listed_uploading: HashSet::new(),
            reported_tasks: HashSet::new(),
            startup_due: HashSet::new(),
            final_uploads: BTreeMap::new(),
            closed: HashSet::new(),
            flights: HashMap::new(),
            flight_ids: 0,
            recorded_during_flight: HashSet::new(),
            sync_points: BTreeMap::new(),
            sync_point_ids: 0,
            unreported: Vec::new(),
            reports: config.reports,
            last_connection: None,
            binding: watch::Sender::new(None),
            mailbox: mailbox.clone(),
            mounts: HashMap::new(),
            notified: HashMap::new(),
            unmounted: BTreeMap::new(),
            connected: 0,
            generations: 0,
            checks: Vec::new(),
            session_dir: state::paths::session_directory(&root, config.session_id.as_str()),
            blobs: root.join("cache/blobs"),
            session_id: config.session_id.clone(),
            inodes,
            reads: HashMap::new(),
            read_tokens: 0,
            notices: BTreeMap::new(),
            mounting: config.mounting,
            registry: config.registry,
            mounted: HashMap::new(),
            held: Vec::new(),
            checking: HashMap::new(),
            check_ids: 0,
            clean: std::collections::HashSet::new(),
            handle: None,
        };
        let sync = DriveSync {
            mailbox,
            session_id: config.session_id,
            cache: config.cache,
            tasks: config.tasks,
            clock: config.clock,
            bound: Arc::default(),
            first_failures: Arc::default(),
            session_dir: owner.session_dir.clone(),
            mounting: owner.mounting.clone(),
            final_ids: Arc::default(),
        };
        let mut owner = owner;
        owner.handle = Some(sync.clone());
        tokio::spawn(owner.run(inbox));
        sync
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

    /// Mounts each drive of `bound` that is not mounted, as a preparation
    /// or a `session.drives` does; the failures that are new.
    pub async fn mount_drives(&self, bound: Vec<Bound>) -> Vec<Failure> {
        *self.bound.lock().expect("bound lock") = bound.clone();
        let (reply, answer) = oneshot::channel();
        self.send(Message::CheckMounts { bound, reply });
        let failures = answer.await.unwrap_or_default();
        let (reply, answer) = oneshot::channel();
        self.send(Message::Judge { failures, reply });
        answer.await.unwrap_or_default()
    }

    /// The check before an `exec_command` starts or a `view_image` reads:
    /// the held failed listings, then each drive's mount, then the wait for
    /// catch-up and its failed listings; the failures that are new, in that
    /// order. Their rows change only when [`DriveSync::reported`] is told.
    pub async fn check_drives(&self, bound: Vec<Bound>) -> Vec<Failure> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::BeginCheck(reply));
        let Ok((check, mut earlier)) = answer.await else {
            return Vec::new();
        };
        let (reply, answer) = oneshot::channel();
        self.send(Message::CheckMounts { bound, reply });
        earlier.extend(answer.await.unwrap_or_default());
        self.check().await;
        let (reply, answer) = oneshot::channel();
        self.send(Message::EndCheck {
            check,
            earlier,
            reply,
        });
        answer.await.unwrap_or_default()
    }

    /// These failures were reported; a drive whose first catch-up since its
    /// mount finished keeps no row.
    pub fn reported(&self, failures: Vec<Failure>) {
        self.send(Message::Reported(failures));
    }

    /// Detaches `drive_id`'s mount: it gets no more notices, and every later
    /// request of its FUSE session answers `EIO`.
    pub fn detach(&self, drive_id: &Id) {
        self.send(Message::Detach(drive_id.as_str().to_owned()));
    }

    /// Detaches every mount, as the App Server's shutdown does.
    pub fn detach_all(&self) {
        self.send(Message::DetachAll);
    }

    /// The sync point of a call whose result is being built; it begins at
    /// this call, and the future gives its outcome.
    pub fn sync_point(
        &self,
        request: SyncPointRequest,
    ) -> impl std::future::Future<Output = SyncOutcome> + use<> {
        let (reply, answer) = oneshot::channel();
        let began = self.clock.now();
        self.send(Message::SyncPoint {
            request,
            began,
            reply,
        });
        async move { answer.await.unwrap_or_default() }
    }

    /// The drive sync's clock, which final uploads end by.
    pub fn now(&self) -> std::time::Duration {
        self.clock.now()
    }

    /// An `exec_command` task of the Session started.
    pub fn task_started(&self, task_id: &Id) {
        self.send(Message::TaskStarted(task_id.as_str().to_owned()));
    }

    /// A task of the Session exited: the uploads of its changes start.
    pub fn task_exited(&self, task_id: &Id) {
        self.send(Message::TaskExited(task_id.as_str().to_owned()));
    }

    /// Ends the owner once it has closed the sync state; the requests in
    /// flight then have no effect.
    pub async fn close(&self) {
        let (reply, closed) = oneshot::channel();
        self.send(Message::Close(reply));
        let _ = closed.await;
    }

    /// The drives a failed listing unmounted, with its error.
    pub async fn unmounted(&self) -> BTreeMap<String, String> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Unmounted(reply));
        answer.await.unwrap_or_default()
    }

    pub(crate) fn send(&self, message: Message) {
        let _ = self.mailbox.send(message);
    }
}

/// A mounted drive and its catch-up.
pub(crate) struct Mount {
    pub(crate) exclude: Vec<String>,
    /// Tells the answers of this mount from those of an earlier one.
    generation: u64,
    running: Option<Running>,
    again: bool,
    /// The `connected` count when the last finished catch-up began.
    finished_began: Option<u64>,
    /// Whether no catch-up of this mount has finished yet.
    first: bool,
    /// Operations that wait for the first listing to finish.
    pub(crate) until_listed: Vec<(Op, Reply)>,
    /// Reads of missing blobs that wait for the running catch-up to end.
    pub(crate) until_caught_up: Vec<(Op, Reply)>,
}

impl Mount {
    pub(crate) fn catching_up(&self) -> bool {
        self.running.is_some()
    }
}

struct Running {
    began: u64,
    /// The first page's `start_seq` of a listing in progress.
    start_seq: Option<i64>,
}

pub(crate) struct Owner {
    pub(crate) state: SyncState,
    pub(crate) transfer: Arc<dyn DriveTransfer>,
    pub(crate) cache: ContentCache,
    pub(crate) clock: Clock,
    pub(crate) uploads: Uploads,
    /// The changes a result listed `uploading` or `upload_pending`.
    pub(crate) listed: HashSet<Key>,
    /// The changes a result listed `uploading` that have neither changed
    /// nor faulted since.
    pub(crate) listed_uploading: HashSet<Key>,
    /// The tasks that finished their first sync point.
    pub(crate) reported_tasks: HashSet<String>,
    /// The changes pending at start, when the binding became current on a
    /// new connection or at a final upload, until a commit carries them.
    pub(crate) startup_due: HashSet<Key>,
    /// The final uploads in progress.
    pub(crate) final_uploads: BTreeMap<u64, crate::final_upload::Final>,
    /// The drives whose final upload abandoned changes: nothing of them is
    /// uploaded or committed until they are mounted again.
    pub(crate) closed: HashSet<String>,
    /// The commit in flight of each drive mount.
    pub(crate) flights: HashMap<String, Flight>,
    pub(crate) flight_ids: u64,
    /// The paths of commits in flight that a change record reached.
    pub(crate) recorded_during_flight: HashSet<Key>,
    pub(crate) sync_points: BTreeMap<u64, SyncPoint>,
    pub(crate) sync_point_ids: u64,
    /// The rejections no sync point carries, for drive sync reports.
    pub(crate) unreported: Vec<Rejected>,
    pub(crate) reports: Arc<dyn protocol::ports::ReportSink>,
    /// The connection of the binding last current.
    last_connection: Option<Id>,
    pub(crate) binding: watch::Sender<Option<Binding>>,
    pub(crate) mailbox: mpsc::UnboundedSender<Message>,
    pub(crate) mounts: HashMap<String, Mount>,
    /// The highest `drive.changed` seq received for each drive.
    notified: HashMap<String, i64>,
    pub(crate) unmounted: BTreeMap<String, String>,
    connected: u64,
    generations: u64,
    checks: Vec<oneshot::Sender<()>>,
    /// `sessions/<session_id>` under the canonical state root.
    pub(crate) session_dir: PathBuf,
    pub(crate) blobs: PathBuf,
    pub(crate) session_id: SafeId,
    /// The path of each `file_id` of each drive, as last written.
    pub(crate) inodes: HashMap<(String, i64), String>,
    /// The reads of cache blobs in progress: drive, path, hash and size.
    pub(crate) reads: HashMap<u64, (String, String, String, u64)>,
    pub(crate) read_tokens: u64,
    /// The local-only notices kept for each task.
    pub(crate) notices: BTreeMap<String, Vec<Notice>>,
    pub(crate) mounting: Mounting,
    pub(crate) registry: Arc<dyn Registry>,
    /// The FUSE mount of each drive this App Server mounted.
    pub(crate) mounted: HashMap<String, Mounted>,
    /// Failed listings that ended outside every check, new when they ended.
    pub(crate) held: Vec<Failure>,
    /// The failed listings of each check in progress.
    pub(crate) checking: HashMap<u64, Vec<Failure>>,
    pub(crate) check_ids: u64,
    /// The drives whose first catch-up since their mount finished.
    pub(crate) clean: std::collections::HashSet<String>,
    /// A handle on this owner, for the mounts' adapters.
    pub(crate) handle: Option<DriveSync>,
}

pub(crate) fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)
        })
}

impl Owner {
    async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<Message>) {
        // App Server start is a retry point; the stored commits go first.
        self.retry_faults(None).await;
        self.startup_due = self
            .state
            .tables()
            .pending_changes
            .keys()
            .cloned()
            .collect();
        self.resend_stored();
        loop {
            let wake = [self.quiet_wake(), self.sync_point_wake(), self.final_wake()]
                .into_iter()
                .flatten()
                .min();
            let message = match wake {
                Some(at) => tokio::select! {
                    message = inbox.recv() => message,
                    () = self.clock.sleep_until(at) => {
                        self.quiet_uploads();
                        self.sync_points_progress().await;
                        self.final_uploads_progress().await;
                        continue;
                    }
                },
                None => inbox.recv().await,
            };
            let message = match message {
                Some(Message::Close(reply)) => {
                    self.state.close().await;
                    let _ = reply.send(());
                    return;
                }
                Some(message) => message,
                None => return,
            };
            self.handle(message).await;
            self.final_uploads_progress().await;
            self.answer_checks();
        }
    }

    /// The binding became current; on a new connection, or the first time
    /// after start, every pending change uploads and is due, faults
    /// included.
    async fn binding_current(&mut self, binding: Binding) {
        let new = self.last_connection.as_ref() != Some(&binding.connection_id);
        self.last_connection = Some(binding.connection_id.clone());
        self.binding.send_replace(Some(binding));
        self.send_reports();
        if !new {
            return;
        }
        self.retry_faults(None).await;
        let keys: Vec<Key> = self
            .state
            .tables()
            .pending_changes
            .keys()
            .cloned()
            .collect();
        for key in &keys {
            self.start_upload(key, None);
        }
        self.startup_due.extend(keys);
        // A commit a final upload cancelled in keep mode goes first.
        self.resend_stored();
        self.changes_ready().await;
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
            Message::BindingCurrent(binding) => self.binding_current(binding).await,
            Message::BindingGone => {
                self.binding.send_replace(None);
            }
            Message::Mounted { drive_id, exclude } => self.mount_catch_up(drive_id, exclude),
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
            Message::Op {
                drive_id,
                op,
                reply,
            } => self.op(drive_id, op, reply).await,
            Message::ReadEnded(token) => {
                self.reads.remove(&token);
            }
            Message::Reads(reply) => {
                let _ = reply.send(self.waiting_reads());
            }
            Message::Notices { task_id, reply } => {
                let _ = reply.send(self.notices.remove(&task_id).unwrap_or_default());
            }
            Message::CheckMounts { bound, reply } => {
                let failures = self.check_mounts(bound);
                let _ = reply.send(failures);
            }
            Message::BeginCheck(reply) => {
                self.check_ids += 1;
                self.checking.insert(self.check_ids, Vec::new());
                let _ = reply.send((self.check_ids, std::mem::take(&mut self.held)));
            }
            Message::EndCheck {
                check,
                mut earlier,
                reply,
            } => {
                earlier.extend(self.checking.remove(&check).unwrap_or_default());
                let _ = reply.send(self.judge(earlier));
            }
            Message::Judge { failures, reply } => {
                let _ = reply.send(self.judge(failures));
            }
            Message::Reported(failures) => self.reported(failures).await,
            Message::Detach(drive) => self.detach(&drive),
            Message::DetachAll => {
                let drives: Vec<String> = self.mounted.keys().cloned().collect();
                for drive in drives {
                    self.detach(&drive);
                }
            }
            Message::UploadEnded {
                key,
                id,
                generation,
                ended,
            } => self.upload_ended(key, id, generation, ended).await,
            Message::CommitAnswered { drive, id, answer } => {
                self.commit_answered(drive, id, answer).await;
            }
            Message::SyncPoint {
                request,
                began,
                reply,
            } => self.sync_point(request, began, reply).await,
            Message::TaskStarted(task) => self.task_started(task),
            Message::TaskExited(task) => self.task_exited(&task),
            Message::FinalUpload { id, request, reply } => {
                self.final_upload(id, request, reply).await;
            }
            Message::FinalKeep(id) => self.final_keep(id).await,
            Message::FinalRelease(id) => self.final_release(id),
            Message::RemoveDrive { drive, reply } => {
                self.remove_drive(&drive).await;
                let _ = reply.send(());
            }
            Message::Close(_) => unreachable!("the run loop ends at a close"),
        }
    }

    fn applied_seq(&self, drive: &str) -> i64 {
        self.state
            .tables()
            .drive_cursor
            .get(drive)
            .map_or(0, |cursor| cursor.applied_seq)
    }

    /// `drive_id` is mounted with `exclude`: a new mount, whose catch-up
    /// starts.
    pub(crate) fn mount_catch_up(&mut self, drive_id: Id, exclude: Vec<String>) {
        let drive = drive_id.as_str().to_owned();
        self.generations += 1;
        self.unmounted.remove(&drive);
        self.clean.remove(&drive);
        self.closed.remove(&drive);
        self.mounts.insert(
            drive.clone(),
            Mount {
                exclude,
                generation: self.generations,
                running: None,
                again: false,
                finished_began: None,
                first: true,
                until_listed: Vec::new(),
                until_caught_up: Vec::new(),
            },
        );
        self.trigger(&drive);
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
            self.detach(drive);
            self.listing_failed(drive, &error);
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
            .filter(|file| !paths::excluded(file.path.as_str(), &exclude))
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
        self.write_replacing(drive, write).await?;
        self.list_changes(drive, start_seq, None);
        let waiting = self
            .mounts
            .get_mut(drive)
            .map(|mount| std::mem::take(&mut mount.until_listed))
            .unwrap_or_default();
        for (op, reply) in waiting {
            self.op(drive.to_owned(), op, reply).await;
        }
        Ok(())
    }

    async fn changes_page(&mut self, drive: &str, page: ListChangesResponse) -> Result<(), String> {
        let exclude = self.exclude(drive);
        let seqs: Vec<i64> = page.records.iter().map(|record| record.seq.get()).collect();
        let applied = catchup::page_applied_seq(self.applied_seq(drive), &seqs, page.has_more);
        let records: Vec<Record> = page
            .records
            .iter()
            .filter(|record| !paths::excluded(record.path.as_str(), &exclude))
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
        self.write_replacing(drive, write).await?;
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
        let first = std::mem::take(&mut mount.first);
        let again = std::mem::take(&mut mount.again);
        let waiting = std::mem::take(&mut mount.until_caught_up);
        for (op, reply) in waiting {
            self.op(drive.to_owned(), op, reply).await;
        }
        if first {
            self.caught_up_first(drive).await?;
        }
        if again {
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

    /// Commits a catch-up's `write`, then invalidates the kernel's cached
    /// pages of each file whose content it replaced.
    async fn write_replacing(&mut self, drive: &str, write: Write) -> Result<(), String> {
        let replaced = catchup::replaced(self.state.tables(), &write);
        if self.flights.contains_key(drive) {
            let tables = self.state.tables();
            let reached = write
                .files
                .iter()
                .map(|row| (row.drive_id.clone(), row.path.clone()))
                .chain(write.files_removed.iter().cloned())
                .filter(|key| tables.inflight_commits.contains_key(key));
            let reached: Vec<Key> = reached.collect();
            self.recorded_during_flight.extend(reached);
        }
        self.write(write).await?;
        if let Some(mounted) = self.mounted.get(drive) {
            for file_id in replaced {
                mounted.invalidate(file_id as u64);
            }
        }
        Ok(())
    }

    /// Commits `write` and keeps the path of each `file_id` it names.
    pub(crate) async fn write(&mut self, write: Write) -> Result<(), String> {
        let tables = self.state.tables();
        let gone: Vec<(String, i64)> = write
            .files_removed
            .iter()
            .filter_map(|key| tables.files.get(key))
            .map(|row| (row.drive_id.clone(), row.file_id))
            .collect();
        let named: Vec<((String, i64), String)> = write
            .files
            .iter()
            .map(|row| ((row.drive_id.clone(), row.file_id), row.path.clone()))
            .collect();
        self.state
            .apply(write)
            .await
            .map_err(|error| format!("sync state: {error}"))?;
        for key in gone {
            self.inodes.remove(&key);
        }
        self.inodes.extend(named);
        Ok(())
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

impl protocol::ports::DriveSyncPort for DriveSync {
    fn check(&self, check: protocol::ports::DriveCheck) -> protocol::ports::DriveAnswer {
        let sync = self.clone();
        Box::pin(async move {
            let bound = sync.bound.lock().expect("bound lock").clone();
            let failures = sync.check_drives(bound).await;
            if !check.view_image {
                if !failures.is_empty() {
                    sync.first_failures
                        .lock()
                        .expect("failures lock")
                        .insert(check.task_id.as_str().to_owned(), failures);
                }
                return None;
            }
            sync.fitted(crate::outcome::Facts {
                unmounted: unmounted(failures),
                ..crate::outcome::Facts::default()
            })
        })
    }

    fn sync_point(&self, point: protocol::ports::DriveSyncPoint) -> protocol::ports::DriveAnswer {
        let sync = self.clone();
        let task = point.task_id.as_str().to_owned();
        let outcome = self.sync_point(SyncPointRequest {
            task_id: Some(task.clone()),
            wait: point.wait,
            traceparent: point.traceparent,
        });
        Box::pin(async move {
            let outcome = outcome.await;
            let failures = match point.first {
                true => sync
                    .first_failures
                    .lock()
                    .expect("failures lock")
                    .remove(&task),
                false => None,
            };
            let downloading = match point.running {
                true => sync.downloading().await,
                false => Vec::new(),
            };
            sync.fitted(crate::outcome::Facts {
                committed: outcome.committed,
                deleted: outcome.deleted,
                rejected: outcome.rejected,
                not_synced: outcome.not_synced,
                downloading: downloading
                    .into_iter()
                    .filter_map(|download| {
                        Some(protocol::tool::DriveDownload {
                            path: protocol::field::AbsolutePath::new(
                                download.path.to_string_lossy().into_owned(),
                            )
                            .ok()?,
                            downloaded_bytes: protocol::field::Count::saturating(
                                download.downloaded_bytes,
                            ),
                            size_bytes: protocol::field::Count::saturating(download.size_bytes),
                        })
                    })
                    .collect(),
                unmounted: unmounted(failures.unwrap_or_default()),
            })
        })
    }

    fn drives_variable(&self) -> Option<PathBuf> {
        let binds = !self.bound.lock().expect("bound lock").is_empty();
        crate::mount::drives_variable(&self.session_dir, binds, &self.mounting)
    }
}

impl DriveSync {
    /// Fits `facts`; the `unmounted` entries placed are reported, and the
    /// others stay new for the next check.
    fn fitted(&self, facts: crate::outcome::Facts) -> Option<protocol::tool::DriveSyncOutcome> {
        let fitted = crate::outcome::fit(facts);
        if !fitted.placed.is_empty() {
            self.reported(
                fitted
                    .placed
                    .iter()
                    .map(|failure| Failure {
                        drive_id: failure.drive_id.as_str().to_owned(),
                        error: failure.error.as_str().to_owned(),
                    })
                    .collect(),
            );
        }
        fitted.drive_sync
    }
}

fn unmounted(failures: Vec<Failure>) -> Vec<protocol::tool::DriveMountFailure> {
    failures
        .into_iter()
        .filter_map(|failure| {
            Some(protocol::tool::DriveMountFailure {
                drive_id: Id::new(failure.drive_id).ok()?,
                error: protocol::field::NonEmptyString::new(failure.error).ok()?,
            })
        })
        .collect()
}
