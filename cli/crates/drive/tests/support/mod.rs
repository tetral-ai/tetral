//! Drive syncs of one or more Sessions over a stub `DriveTransfer` whose
//! upload and commit answers the test holds and releases, a Content cache
//! they share, and a clock the test moves.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drive::cache::{CacheConfig, ContentCache};
use drive::mount::{Mounting, Registry};
use drive::ops::{SetAttr, TOP_INODE};
use drive::state::{FileRow, PendingChange, SyncState, Tables, Write};
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use drive::{DriveSync, SyncConfig, SyncOutcome, SyncPointRequest};
use protocol::drive_transfer::{
    CommitChange, CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::errors::ErrorBody;
use protocol::id::{Id, SafeId};
use protocol::ports::{TaskOwnerReply, TaskOwnership};
use rustix::io::Errno;
use serde_json::{Value, json};
use sha2::Digest;
use state::clock::Clock;
use tokio::sync::{mpsc, oneshot};

pub const DRIVE: &str = "drive_a";
/// Process `n` belongs to task `task_<n>`; process 0 to no task.
pub const NO_TASK: u32 = 0;

pub fn sha256(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn key(path: &str) -> (String, String) {
    (DRIVE.to_owned(), path.to_owned())
}

pub fn task(pid: u32) -> String {
    format!("task_{pid}")
}

/// A request the stub holds until the test answers it.
pub enum Call {
    Upload {
        session: String,
        sha256: String,
        traceparent: Option<String>,
        answer: oneshot::Sender<Result<UploadContentResponse, Ended>>,
    },
    Commit {
        session: String,
        changes: Vec<CommitChange>,
        traceparent: Option<String>,
        answer: oneshot::Sender<Result<CommitResponse, Ended>>,
    },
}

impl Call {
    pub fn sha256(&self) -> &str {
        match self {
            Call::Upload { sha256, .. } => sha256,
            Call::Commit { .. } => panic!("a commit, not an upload"),
        }
    }

    pub fn changes(&self) -> &[CommitChange] {
        match self {
            Call::Commit { changes, .. } => changes,
            Call::Upload { sha256, .. } => panic!("an upload of {sha256}, not a commit"),
        }
    }

    pub fn traceparent(&self) -> Option<&str> {
        match self {
            Call::Upload { traceparent, .. } | Call::Commit { traceparent, .. } => {
                traceparent.as_deref()
            }
        }
    }

    pub fn session(&self) -> &str {
        match self {
            Call::Upload { session, .. } | Call::Commit { session, .. } => session,
        }
    }

    /// Answers an upload `200`.
    pub fn uploaded(self) {
        let Call::Upload { sha256, answer, .. } = self else {
            panic!("not an upload");
        };
        let _ = answer.send(Ok(serde_json::from_value(
            json!({ "content_sha256": sha256, "size_bytes": 1 }),
        )
        .unwrap()));
    }

    pub fn upload_ended(self, ended: Ended) {
        let Call::Upload { answer, .. } = self else {
            panic!("not an upload");
        };
        let _ = answer.send(Err(ended));
    }

    /// Answers a commit with `results`, one JSON result per change.
    pub fn answer(self, results: Vec<Value>) {
        let Call::Commit { answer, .. } = self else {
            panic!("not a commit");
        };
        let _ = answer.send(Ok(serde_json::from_value(
            json!({ "seq": 2, "results": results }),
        )
        .unwrap()));
    }

    /// Accepts every change of a commit.
    pub fn accepted(self) {
        let results = self
            .changes()
            .iter()
            .map(|change| json!({ "status": "accepted", "version_id": format!("v_{}", change.path().len()) }))
            .collect();
        self.answer(results);
    }

    pub fn commit_ended(self, ended: Ended) {
        let Call::Commit { answer, .. } = self else {
            panic!("not a commit");
        };
        let _ = answer.send(Err(ended));
    }
}

/// The drive the stub lists, its content, and the held requests.
pub struct Stub {
    pub files: Mutex<Vec<Value>>,
    pub records: Mutex<Vec<Value>>,
    pub contents: Mutex<HashMap<String, Vec<u8>>>,
    /// Hashes whose downloads stop after their first byte and never end.
    pub held_downloads: Mutex<std::collections::HashSet<String>>,
    calls: mpsc::UnboundedSender<Call>,
    /// The upload-content calls in progress, and the most at once.
    pub puts_open: AtomicU64,
    pub puts_most: AtomicU64,
}

impl Stub {
    pub fn content(&self, content: &str) -> String {
        let hash = sha256(content.as_bytes());
        self.contents
            .lock()
            .unwrap()
            .insert(hash.clone(), content.as_bytes().to_vec());
        hash
    }

    pub fn list(&self, path: &str, content: &str) {
        let hash = self.content(content);
        self.files.lock().unwrap().push(json!({
            "path": path,
            "version_id": format!("v_{path}"),
            "content_sha256": hash,
            "size_bytes": content.len(),
            "executable": false,
        }));
    }
}

/// Lowers the count of PUTs in progress when the request ends or is
/// cancelled.
struct Open<'a>(&'a AtomicU64);

impl Drop for Open<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A stub per Session that names it in its calls.
pub struct SessionStub {
    pub stub: Arc<Stub>,
    pub session: String,
}

impl DriveTransfer for SessionStub {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let page = json!({
            "files": self.stub.files.lock().unwrap().clone(),
            "has_more": false,
            "start_seq": 1,
        });
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let after = request.body.after_seq;
        let records: Vec<Value> = self
            .stub
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record["seq"].as_i64().unwrap() > after)
            .cloned()
            .collect();
        let page = json!({ "records": records, "has_more": false });
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        Box::pin(async move {
            let open = self.stub.puts_open.fetch_add(1, Ordering::SeqCst) + 1;
            let _open = Open(&self.stub.puts_open);
            self.stub.puts_most.fetch_max(open, Ordering::SeqCst);
            let (answer, answered) = oneshot::channel();
            let _ = self.stub.calls.send(Call::Upload {
                session: self.session.clone(),
                sha256: request.body.sha256.as_str().to_owned(),
                traceparent: request.traceparent,
                answer,
            });
            match answered.await {
                Ok(answer) => answer,
                Err(_) => std::future::pending().await,
            }
        })
    }

    fn commit(&self, request: Request<Commit>) -> Transfer<'_, CommitResponse> {
        Box::pin(async move {
            let (answer, answered) = oneshot::channel();
            let _ = self.stub.calls.send(Call::Commit {
                session: self.session.clone(),
                changes: request.body.changes,
                traceparent: request.traceparent,
                answer,
            });
            // A dropped answer is one that never arrives.
            match answered.await {
                Ok(answer) => answer,
                Err(_) => std::future::pending().await,
            }
        })
    }

    fn download_content<'a>(
        &'a self,
        request: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        Box::pin(async move {
            let hash = request.body.sha256.as_str();
            let bytes = self.stub.contents.lock().unwrap()[hash].clone();
            if self.stub.held_downloads.lock().unwrap().contains(hash) {
                sink.append(&bytes[..1]).map_err(Ended::Sink)?;
                return std::future::pending().await;
            }
            sink.append(&bytes).map_err(Ended::Sink)
        })
    }
}

struct NoRegistry;

impl Registry for NoRegistry {
    fn add(&self, _: SafeId, _: DriveSync) {}
    fn remove(&self, _: SafeId) {}
    fn mounted(&self, _: Id, _: SafeId) {}
    fn detached(&self, _: Id, _: SafeId) {}
}

struct Tasks;

impl TaskOwnership for Tasks {
    fn task_of(&self, pid: u32, reply: TaskOwnerReply) {
        reply((pid != NO_TASK).then(|| Id::new(task(pid)).unwrap()));
    }
}

/// The time of the test's clock, which only the test moves.
#[derive(Clone, Default)]
pub struct TestClock(Arc<AtomicU64>);

impl TestClock {
    pub fn clock(&self) -> Clock {
        let nanos = self.0.clone();
        Clock::new(
            move || Duration::from_nanos(nanos.load(Ordering::SeqCst)),
            Duration::from_millis(2),
        )
    }

    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_nanos() as u64, Ordering::SeqCst);
    }
}

/// The drive sync reports handed to the outbox, each validated; with
/// `full`, the ordinary budget refuses them and only the reserve has room.
#[derive(Default)]
pub struct Reports {
    pub sent: Mutex<Vec<Value>>,
    pub full: std::sync::atomic::AtomicBool,
    pub reserved: AtomicU64,
}

impl protocol::ports::ReportSink for Reports {
    fn submit(&self, submission: protocol::ports::Submission) -> protocol::ports::ReportId {
        self.reserved.fetch_add(1, Ordering::SeqCst);
        self.record(submission)
    }

    fn try_submit(
        &self,
        submission: protocol::ports::Submission,
    ) -> Result<protocol::ports::ReportId, protocol::ports::ReportAdmissionError> {
        if self.full.load(Ordering::SeqCst) {
            return Err(protocol::ports::ReportAdmissionError::ResourceExhausted);
        }
        Ok(self.record(submission))
    }

    fn cancel(&self, _: protocol::ports::ReportId) {}
    fn add_final(&self, _: protocol::ports::ReportId, _: Id) {}
}

impl Reports {
    fn record(&self, submission: protocol::ports::Submission) -> protocol::ports::ReportId {
        let message = Id::new(protocol::ports::LONGEST_REPORT_MESSAGE_ID).unwrap();
        let bytes = (submission.build)(&submission.connection_id, &message).unwrap();
        protocol::decode::<protocol::tool::DriveSyncReport>(&bytes).expect("a valid report");
        let mut sent = self.sent.lock().unwrap();
        sent.push(serde_json::from_slice(&bytes).unwrap());
        protocol::ports::ReportId(sent.len() as u64)
    }
}

/// What the Sessions of one test share.
pub struct Env {
    pub reports: Arc<Reports>,
    pub root: tempfile::TempDir,
    pub stub: Arc<Stub>,
    pub cache: ContentCache,
    pub clock: TestClock,
    /// How the Sessions' drives are mounted: not at all unless a test asks
    /// for real FUSE mounts.
    pub mounting: Mounting,
    calls: mpsc::UnboundedReceiver<Call>,
}

impl Env {
    pub fn new() -> Env {
        let root = tempfile::tempdir().unwrap();
        let (sender, calls) = mpsc::unbounded_channel();
        let stub = Arc::new(Stub {
            files: Mutex::default(),
            records: Mutex::default(),
            contents: Mutex::default(),
            held_downloads: Mutex::default(),
            calls: sender,
            puts_open: AtomicU64::new(0),
            puts_most: AtomicU64::new(0),
        });
        let clock = TestClock::default();
        let cache = ContentCache::spawn(CacheConfig {
            root: root.path().to_path_buf(),
            transfer: Arc::new(SessionStub {
                stub: stub.clone(),
                session: "cache".to_owned(),
            }),
            clock: clock.clock(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        Env {
            reports: Arc::default(),
            root,
            stub,
            cache,
            clock,
            mounting: Mounting::Unavailable("no FUSE: test".to_owned()),
            calls,
        }
    }

    pub fn session_dir(&self, session: &str) -> PathBuf {
        self.root.path().join("sessions").join(session)
    }

    /// A Session with `seed` written into its new sync state, its binding
    /// current and, when `mount` is set, `DRIVE` mounted and caught up.
    pub async fn session(&self, session: &str, seed: Write, mount: bool) -> Session {
        let directory = self.session_dir(session);
        std::fs::create_dir_all(&directory).unwrap();
        let mut state = SyncState::create(&directory.join("sync-state.db"))
            .await
            .unwrap();
        state.apply(seed).await.unwrap();
        self.run(session, state, mount).await
    }

    /// The Session's drive sync started again from its `sync-state.db`.
    pub async fn reopen(&self, session: &str, mount: bool) -> Session {
        let path = self.session_dir(session).join("sync-state.db");
        let state = SyncState::open(&path).await.unwrap();
        self.run(session, state, mount).await
    }

    async fn run(&self, session: &str, state: SyncState, mount: bool) -> Session {
        let sync = DriveSync::spawn(SyncConfig {
            session_id: SafeId::new(session).unwrap(),
            root: self.root.path().to_path_buf(),
            state,
            transfer: Arc::new(SessionStub {
                stub: self.stub.clone(),
                session: session.to_owned(),
            }),
            cache: self.cache.clone(),
            tasks: Arc::new(Tasks),
            mounting: self.mounting.clone(),
            registry: Arc::new(NoRegistry),
            clock: self.clock.clock(),
            reports: self.reports.clone(),
        });
        sync.binding_current(Binding {
            connection_id: Id::new("conn_a").unwrap(),
            binding_id: Id::new("bind_a").unwrap(),
        });
        let session = Session {
            sync,
            drive: Id::new(DRIVE).unwrap(),
            directory: self.session_dir(session),
        };
        if mount {
            session.sync.mounted(session.drive.clone(), Vec::new());
            session.caught_up().await;
        }
        session
    }

    /// The next request the stub holds.
    pub async fn call(&mut self) -> Call {
        tokio::time::timeout(Duration::from_secs(10), self.calls.recv())
            .await
            .expect("a request within 10 s")
            .expect("the stub is alive")
    }

    /// No request arrives within `wait` of real time.
    pub async fn no_call(&mut self, wait: Duration) {
        if let Ok(Some(call)) = tokio::time::timeout(wait, self.calls.recv()).await {
            match call {
                Call::Upload { sha256, .. } => panic!("an upload of {sha256}"),
                Call::Commit { changes, .. } => panic!("a commit of {changes:?}"),
            }
        }
    }

    /// The clock moved by `by`, and the owners given time to act on it.
    pub async fn advance(&self, by: Duration) {
        self.clock.advance(by);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub struct Session {
    pub sync: DriveSync,
    pub drive: Id,
    pub directory: PathBuf,
}

impl Session {
    pub async fn caught_up(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.sync.check())
            .await
            .expect("caught up");
    }

    pub async fn tables(&self) -> Tables {
        self.sync.tables().await
    }

    /// Waits until the tables meet `done`.
    pub async fn until(&self, done: impl Fn(&Tables) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !done(&self.tables().await) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the tables within 10 s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub fn machine_path(&self, path: &str) -> String {
        self.directory
            .join("drives")
            .join(DRIVE)
            .join(path)
            .to_string_lossy()
            .into_owned()
    }

    async fn ino(&self, path: &str) -> Result<u64, Errno> {
        let attr = self
            .sync
            .lookup(&self.drive, TOP_INODE, path.as_bytes())
            .await?;
        Ok(attr.ino)
    }

    /// Writes `content` as the whole of the top-level file `path`, made by
    /// process `pid`.
    pub async fn write(&self, path: &str, content: &str, pid: u32) {
        self.write_in(&self.drive.clone(), path, content, pid).await;
    }

    /// [`Session::write`] in the drive `drive`.
    pub async fn write_in(&self, drive: &Id, path: &str, content: &str, pid: u32) {
        let ino = match self
            .sync
            .lookup(drive, TOP_INODE, path.as_bytes())
            .await
            .map(|attr| attr.ino)
        {
            Ok(ino) => {
                let set = SetAttr {
                    size: Some(0),
                    ..SetAttr::default()
                };
                self.sync.setattr(drive, ino, set, Some(pid)).await.unwrap();
                ino
            }
            Err(_) => {
                self.sync
                    .create(drive, TOP_INODE, path.as_bytes(), 0o644, Some(pid))
                    .await
                    .unwrap()
                    .ino
            }
        };
        self.sync
            .write(drive, ino, 0, content.as_bytes(), Some(pid))
            .await
            .unwrap();
    }

    pub async fn remove(&self, path: &str, pid: u32) {
        self.sync
            .unlink(&self.drive, TOP_INODE, path.as_bytes(), Some(pid))
            .await
            .unwrap();
    }

    pub async fn read(&self, path: &str) -> Result<String, Errno> {
        let ino = self.ino(path).await?;
        let bytes = self.sync.read(&self.drive, ino, 0, 4_096).await?;
        Ok(String::from_utf8(bytes).unwrap())
    }

    /// Starts a sync point; its outcome arrives through the handle.
    pub fn sync_point(
        &self,
        task_id: Option<String>,
        wait: Duration,
        traceparent: Option<&str>,
    ) -> tokio::task::JoinHandle<SyncOutcome> {
        let sync = self.sync.clone();
        let request = SyncPointRequest {
            task_id,
            wait,
            traceparent: traceparent.map(str::to_owned),
        };
        tokio::spawn(sync.sync_point(request))
    }

    pub fn task_started(&self, pid: u32) {
        self.sync.task_started(&Id::new(task(pid)).unwrap());
    }

    pub fn task_exited(&self, pid: u32) {
        self.sync.task_exited(&Id::new(task(pid)).unwrap());
    }
}

/// Waits until `done` holds.
pub async fn eventually(done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "within 10 s");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The outcome of a sync point started with [`Session::sync_point`].
pub async fn outcome(point: tokio::task::JoinHandle<SyncOutcome>) -> SyncOutcome {
    tokio::time::timeout(Duration::from_secs(10), point)
        .await
        .expect("an outcome within 10 s")
        .unwrap()
}

/// The WARN and ERROR records written while the guard lives, on this
/// thread and the tasks of a current-thread runtime.
pub struct Records {
    bytes: Arc<Mutex<Vec<u8>>>,
    sinks: Arc<observe::Sinks>,
    _guard: tracing::subscriber::DefaultGuard,
}

#[derive(Clone)]
struct Bytes(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Bytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Records {
    pub fn start() -> Records {
        use tracing_subscriber::layer::SubscriberExt;
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sinks = Arc::new(observe::Sinks::new(
            observe::Sink::spawn_stderr(Box::new(Bytes(bytes.clone())), observe::registry()),
            observe::FlushClock {
                now: state::seams::now,
                recheck: Duration::from_millis(1),
            },
        ));
        let subscriber = tracing_subscriber::registry().with(observe::RecordLayer::new(
            observe::Level::Warn,
            sinks.clone(),
        ));
        Records {
            bytes,
            sinks,
            _guard: tracing::subscriber::set_default(subscriber),
        }
    }

    /// The records written so far, one JSON object each.
    pub fn lines(&self) -> Vec<Value> {
        self.sinks.flush(Duration::from_secs(2));
        String::from_utf8(self.bytes.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

/// An error body of `code` with `message`, and `missing` for
/// `content_missing`.
pub fn error_body(code: &str, message: &str, missing: &[&str]) -> ErrorBody {
    let mut body = json!({ "code": code, "message": message });
    if !missing.is_empty() {
        body["missing_sha256"] = json!(missing);
    }
    serde_json::from_value(body).unwrap()
}

/// `(path, change, base)` of each change of a commit.
pub fn summary(changes: &[CommitChange]) -> Vec<(String, &'static str, Option<String>)> {
    changes
        .iter()
        .map(|change| match change {
            CommitChange::Write {
                path, base_sha256, ..
            } => (
                path.clone(),
                "write",
                base_sha256.0.as_ref().map(|hash| hash.as_str().to_owned()),
            ),
            CommitChange::Delete { path, base_sha256 } => (
                path.clone(),
                "delete",
                Some(base_sha256.as_str().to_owned()),
            ),
        })
        .collect()
}

/// An uploaded write of a new file at `path`, pending since before the
/// start.
pub fn uploaded_write(path: &str) -> (FileRow, PendingChange) {
    let hash = sha256(path.as_bytes());
    let row = FileRow {
        drive_id: DRIVE.to_owned(),
        path: path.to_owned(),
        file_id: 0,
        kind: "file".to_owned(),
        present: true,
        version_id: None,
        synced_sha256: None,
        synced_size: None,
        synced_executable: None,
        content: Some("session".to_owned()),
        local_sha256: Some(hash.clone()),
        size: Some(path.len() as i64),
        executable: Some(false),
        mtime_ns: Some(1),
        ctime_ns: Some(1),
        origin: None,
        link_target: None,
        local_only: None,
    };
    let change = PendingChange {
        drive_id: DRIVE.to_owned(),
        path: path.to_owned(),
        change: "write".to_owned(),
        base_sha256: None,
        content_sha256: Some(hash),
        executable: Some(false),
        uploaded: true,
        last_write_at: 1,
        task_id: None,
        fault_code: None,
    };
    (row, change)
}

/// Uploaded writes at `writes` and deletions at `deletes`, each with a
/// distinct `file_id`.
pub fn seed(writes: &[String], deletes: &[String]) -> Write {
    let mut seed = Write::default();
    for (index, path) in writes.iter().enumerate() {
        let (mut row, change) = uploaded_write(path);
        row.file_id = index as i64 + 2;
        seed.files.push(row);
        seed.pending.push(change);
    }
    for path in deletes {
        seed.pending.push(PendingChange {
            change: "delete".to_owned(),
            base_sha256: Some(sha256(b"gone")),
            content_sha256: None,
            uploaded: false,
            ..uploaded_write(path).1
        });
    }
    seed
}

/// Lazily unmounts what a test left mounted under `root`.
pub struct Unmount(pub PathBuf);

impl Drop for Unmount {
    fn drop(&mut self) {
        let Ok(root) = self.0.canonicalize() else {
            return;
        };
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for point in mountinfo.lines().filter_map(|line| line.split(' ').nth(4)) {
            if std::path::Path::new(point).starts_with(&root) {
                let _ = std::process::Command::new("fusermount3")
                    .args(["-u", "-z", point])
                    .status();
            }
        }
    }
}

/// The Connection owner's side: keeps the entries it is given.
#[derive(Default)]
pub struct Keeper(pub Mutex<Vec<protocol::connection::LostChange>>);

impl protocol::ports::ConnectionPort for Keeper {
    fn probe_slot(&self, _: &SafeId, _: protocol::ports::ProbeRequest) {}
    fn probe_slot_withdraw(&self, _: &SafeId, _: protocol::ports::ProbeRequest) {}
    fn probe_slot_free(&self) {}
    fn fetch_git_ticket(&self, _: &SafeId, _: protocol::ports::TicketRequest, _: &Id, _: &Id) {}
    fn record_gone(&self, _: &SafeId) {}
    fn tasks_gone(&self, _: &SafeId) {}
    fn task_staged(&self, _: Duration) {}
    fn task_unstaged(&self, _: Duration) {}
    fn task_started(&self, _: Option<Duration>) {}
    fn task_ended(&self) {}
    fn task_refused(&self) {}
    fn keep_lost_changes(
        &self,
        entries: Vec<protocol::connection::LostChange>,
        kept: protocol::ports::KeptReply,
    ) {
        self.0.lock().unwrap().extend(entries);
        kept();
    }
}
