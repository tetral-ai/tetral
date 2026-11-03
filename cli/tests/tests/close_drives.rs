//! `session.close` of a Session that binds a drive: the real Session binding
//! owner of a `Factory`, its real drive sync with a FUSE mount, the real
//! directory probe of a `test-seams` `tetral`, a stub `DriveTransfer` that
//! holds every upload until the test answers it, a stub connection port that
//! keeps `lost_changes`, and a drive clock the test moves.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

use drive::DriveSync;
use drive::cache::{CacheConfig, ContentCache};
use drive::mount::{Mounting, Registry};
use drive::mountinfo::current_device;
use drive::transfer::{
    Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles, Request, Transfer,
    Upload,
};
use execution::cleanup::Leftovers;
use protocol::connection::{DriveAccess, DriveBinding, LostChange};
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::id::{Id, SafeId};
use protocol::local::SessionEntry;
use protocol::ports::{
    Close, ConnectionPort, Drives, KeptReply, Prepare, ProbeRequest, ProbeSlot,
    ReportAdmissionError, ReportId, ReportOutcome, ReportSink, SessionFactory, SessionHandle,
    Submission, TicketRequest,
};
use serde_json::{Value, json};
use session::drives::DriveContext;
use session::{Factory, NoTasks, RootPaths, Shared, Times, ToolContext};
use state::clock::Clock;
use tetral_tests::seams::{SOCKET_VARIABLE, Seams};
use tokio::sync::{mpsc, oneshot, watch};

const SESSION: &str = "sess_a";
const DRIVE: &str = "drive_a";
const OTHER: &str = "drive_b";

/// A held upload: its hash, and the way to answer it.
type HeldUpload = (
    String,
    oneshot::Sender<Result<UploadContentResponse, Ended>>,
);

/// The seam harness of this test process, which counts `sync_state.write`.
static SEAMS: LazyLock<(tempfile::TempDir, Seams)> = LazyLock::new(|| {
    let directory = tempfile::tempdir().unwrap();
    let seams = Seams::serve(directory.path(), Duration::ZERO);
    // SAFETY: set once, before any thread of this process reads it.
    unsafe { std::env::set_var(SOCKET_VARIABLE, seams.socket()) };
    (directory, seams)
});

/// The tests share the process's counters, so they run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Lists an empty drive and holds every commit, and every upload until the
/// test answers it.
struct Stub(mpsc::UnboundedSender<HeldUpload>);

impl DriveTransfer for Stub {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let page = json!({ "files": [], "has_more": false, "start_seq": 1 });
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn list_changes(&self, _: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let page = json!({ "records": [], "has_more": false });
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        let (answer, answered) = oneshot::channel();
        let _ = self
            .0
            .send((request.body.sha256.as_str().to_owned(), answer));
        Box::pin(async move {
            match answered.await {
                Ok(answer) => answer,
                Err(_) => std::future::pending().await,
            }
        })
    }

    fn commit(&self, _: Request<Commit>) -> Transfer<'_, CommitResponse> {
        Box::pin(std::future::pending())
    }

    fn download_content<'a>(
        &'a self,
        _: Request<Download>,
        _: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        Box::pin(std::future::pending())
    }
}

#[derive(Default)]
struct SyncRegistry(Mutex<Option<DriveSync>>);

impl Registry for SyncRegistry {
    fn add(&self, _: SafeId, sync: DriveSync) {
        *self.0.lock().unwrap() = Some(sync);
    }
    fn remove(&self, _: SafeId) {}
    fn mounted(&self, _: Id, _: SafeId) {}
    fn detached(&self, _: Id, _: SafeId) {}
}

/// Grants every probe slot at once and keeps the `lost_changes` entries it
/// is given.
#[derive(Default)]
struct Port {
    owner: OnceLock<Arc<dyn SessionHandle>>,
    kept: Mutex<Vec<LostChange>>,
    hold_kept: Mutex<Option<mpsc::UnboundedSender<KeptReply>>>,
    idle: AtomicU64,
}

impl ConnectionPort for Port {
    fn probe_slot(&self, _: &SafeId, request: ProbeRequest) {
        self.owner
            .get()
            .unwrap()
            .probe_slot(request, ProbeSlot::Granted);
    }
    fn probe_slot_withdraw(&self, _: &SafeId, _: ProbeRequest) {}
    fn probe_slot_free(&self) {}
    fn fetch_git_ticket(&self, _: &SafeId, _: TicketRequest, _: &Id, _: &Id) {}
    fn record_gone(&self, _: &SafeId) {}
    fn tasks_gone(&self, _: &SafeId) {}
    fn task_staged(&self, _: Duration) {}
    fn task_unstaged(&self, _: Duration) {}
    fn task_started(&self, _: Option<Duration>) {}
    fn task_ended(&self) {}
    fn task_refused(&self) {}
    fn keep_lost_changes(&self, entries: Vec<LostChange>, kept: KeptReply) {
        self.kept.lock().unwrap().extend(entries);
        if let Some(held) = self.hold_kept.lock().unwrap().as_ref() {
            held.send(kept).unwrap();
        } else {
            kept();
        }
    }
    fn owner_idle(&self, _: &SafeId, _: u64) {
        self.idle.fetch_add(1, Ordering::SeqCst);
    }
}

/// Acknowledges every report at once and tells the test its body.
struct Reports(mpsc::UnboundedSender<Value>, AtomicU64);

impl ReportSink for Reports {
    fn submit(&self, submission: Submission) -> ReportId {
        let id = ReportId(self.1.fetch_add(1, Ordering::Relaxed));
        let message = Id::new(format!("r{}", id.0)).unwrap();
        let bytes = (submission.build)(&submission.connection_id, &message).unwrap();
        let _ = self.0.send(serde_json::from_slice(&bytes).unwrap());
        if let Some(listener) = submission.listener {
            listener.outcome(id, ReportOutcome::Acked);
        }
        id
    }

    fn try_submit(&self, submission: Submission) -> Result<ReportId, ReportAdmissionError> {
        Ok(self.submit(submission))
    }

    fn cancel(&self, _: ReportId) {}
    fn add_final(&self, _: ReportId, _: Id) {}
}

/// Lazily unmounts what is left mounted under a directory.
struct Unmount(PathBuf);

impl Drop for Unmount {
    fn drop(&mut self) {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for point in mountinfo.lines().filter_map(|line| line.split(' ').nth(4)) {
            if Path::new(point).starts_with(&self.0) {
                let _ = std::process::Command::new("fusermount3")
                    .args(["-u", "-z", point])
                    .status();
            }
        }
    }
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

/// One Session binding owner on a fresh state root.
struct Env {
    _unmount: Unmount,
    _directory: tempfile::TempDir,
    root: PathBuf,
    project: PathBuf,
    nanos: Arc<AtomicU64>,
    uploads: mpsc::UnboundedReceiver<HeldUpload>,
    reports: mpsc::UnboundedReceiver<Value>,
    port: Arc<Port>,
    owner: Arc<dyn SessionHandle>,
    registry: Arc<SyncRegistry>,
    /// The Sessions' drive context, and the one a held context publishes.
    drives: (watch::Sender<Option<Arc<DriveContext>>>, Arc<DriveContext>),
}

impl Env {
    fn new() -> Env {
        Env::with_drives(true)
    }

    /// An `Env` whose drive context is published only when `published`;
    /// [`Env::publish`] publishes it later.
    fn with_drives(published: bool) -> Env {
        let seams = &SEAMS.1;
        let directory = tempfile::tempdir().unwrap();
        let canonical = directory.path().canonicalize().unwrap();
        let root = canonical.join("state");
        let project = canonical.join("project");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&project).unwrap();
        let tetral = tetral_tests::binaries::tetral().canonicalize().unwrap();
        let nanos = Arc::new(AtomicU64::new(0));
        let clock = {
            let nanos = nanos.clone();
            Clock::new(
                move || Duration::from_nanos(nanos.load(Ordering::SeqCst)),
                Duration::from_millis(2),
            )
        };
        let (sent, uploads) = mpsc::unbounded_channel();
        let transfer: Arc<dyn DriveTransfer> = Arc::new(Stub(sent));
        let cache = ContentCache::spawn(CacheConfig {
            root: root.clone(),
            transfer: transfer.clone(),
            clock: clock.clone(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let registry = Arc::new(SyncRegistry::default());
        let context = Arc::new(DriveContext {
            transfer,
            cache,
            mounting: Mounting::Fuse,
            registry: registry.clone(),
            clock,
        });
        let (publish, drives) = watch::channel(published.then(|| context.clone()));
        let factory = Factory::new(
            Shared {
                paths: Arc::new(RootPaths {
                    given: root.clone(),
                    canonical: root.clone(),
                    canonical_utf8: root.to_str().unwrap().to_owned(),
                    probe_program: tetral.clone(),
                }),
                times: Times::default(),
                tools: Arc::new(ToolContext {
                    capability: None,
                    socket_directory: canonical.join("sockets"),
                    tetral,
                    cua_driver: None,
                }),
                leftovers: Leftovers::none(),
                drives,
            },
            NoTasks,
        );
        let (sent, reports) = mpsc::unbounded_channel();
        let port = Arc::new(Port::default());
        let owner = factory.create(
            SafeId::new(SESSION).unwrap(),
            port.clone(),
            Arc::new(Reports(sent, AtomicU64::new(1))),
        );
        let _ = port.owner.set(owner.clone());
        let _ = seams;
        Env {
            _unmount: Unmount(canonical.clone()),
            _directory: directory,
            root,
            project,
            nanos,
            uploads,
            reports,
            port,
            owner,
            registry,
            drives: (publish, context),
        }
    }

    /// Publishes the drive context a held `Env` keeps back.
    fn publish(&self) {
        let _ = self.drives.0.send(Some(self.drives.1.clone()));
    }

    fn advance(&self, step: Duration) {
        self.nanos
            .fetch_add(step.as_nanos() as u64, Ordering::SeqCst);
    }

    fn mount_point(&self) -> PathBuf {
        self.mount_point_of(DRIVE)
    }

    fn mount_point_of(&self, drive: &str) -> PathBuf {
        self.root
            .join("sessions")
            .join(SESSION)
            .join("drives")
            .join(drive)
    }

    /// Prepares `binding` on `conn_a`, binding the drive read-write, with
    /// `cwd` as its working directory.
    fn prepare(&self, message: &str, binding: &str, cwd: &Path) {
        self.prepare_drives(message, binding, cwd, &[DRIVE]);
    }

    /// [`Env::prepare`] binding each of `drives` read-write.
    fn prepare_drives(&self, message: &str, binding: &str, cwd: &Path, drives: &[&str]) {
        let drives: Vec<Value> = drives
            .iter()
            .map(|drive| json!({ "drive_id": drive, "access": "read_write" }))
            .collect();
        let payload = json!({
            "session_id": SESSION,
            "binding_id": binding,
            "cwd": cwd,
            "drives": drives,
            "drive_exclude": [],
            "repositories": [],
        });
        self.owner.prepare(Prepare {
            connection_id: id("conn_a"),
            message_id: id(message),
            addressed_connection_id: id("conn_a"),
            payload: serde_json::from_value(payload).unwrap(),
            fingerprint: format!("{binding:0>64}"),
            traceparent: None,
        });
    }

    fn close(&self, connection: &str, message: &str, binding: &str) {
        self.owner.close(Close {
            connection_id: id(connection),
            message_id: id(message),
            binding_id: id(binding),
        });
    }

    /// The next report, within 30 seconds.
    async fn report(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(30), self.reports.recv())
            .await
            .expect("a report within 30 s")
            .unwrap()
    }

    /// The owner's entries, once it has handled every earlier message.
    async fn entries(&self) -> Vec<SessionEntry> {
        let (reply, entries) = tokio::sync::oneshot::channel();
        self.owner.entries(Box::new(move |entries| {
            let _ = reply.send(entries);
        }));
        entries.await.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_stopped_by_the_connections_end_abandons_its_changes_once_under_the_late_close_whose_report_is_saved()
 {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    let project = env.project.clone();
    env.prepare("prepare_1", "bind_a", &project);
    let prepared = env.report().await;
    assert_eq!(prepared["status"], "prepared", "{prepared}");
    let file = env.mount_point().join("f");
    tokio::task::spawn_blocking(move || std::fs::write(file, "new"))
        .await
        .unwrap()
        .unwrap();

    // The close's final upload holds the content upload of `f`.
    env.close("conn_a", "close_1", "bind_a");
    let (hash, held) = tokio::time::timeout(Duration::from_secs(30), env.uploads.recv())
        .await
        .expect("f's upload within 30 s")
        .unwrap();
    env.advance(Duration::from_secs(10));
    env.owner.connection_ended(id("conn_a"));
    env.entries().await;
    assert!(
        env.reports.try_recv().is_err(),
        "no report on the old connection"
    );
    assert!(env.port.kept.lock().unwrap().is_empty());

    env.close("conn_b", "close_2", "bind_a");
    let closed = env.report().await;
    let expected = json!([{
        "request_message_id": "close_2", "drive_id": DRIVE, "paths": ["f"], "paths_total": 1,
    }]);
    assert_eq!(
        (
            &closed["status"],
            &closed["request_message_id"],
            &closed["connection_id"]
        ),
        (&json!("closed"), &json!("close_2"), &json!("conn_b")),
        "{closed}"
    );
    assert_eq!(closed["lost_changes"], expected);
    assert_eq!(
        serde_json::to_value(&*env.port.kept.lock().unwrap()).unwrap(),
        expected
    );
    let session = env.root.join("sessions").join(SESSION);
    assert!(!session.exists());

    let seams = &SEAMS.1;
    let writes = seams.count("sync_state.write");
    assert!(writes > 0, "the harness counts the drive sync's writes");
    // The held upload's answer comes after the delete; nothing writes it.
    let _ = held.send(Ok(serde_json::from_value(
        json!({ "content_sha256": hash, "size_bytes": 3 }),
    )
    .unwrap()));
    env.advance(Duration::from_secs(60));
    tokio::time::sleep(Duration::from_millis(500)).await;
    env.entries().await;
    assert_eq!(seams.count("sync_state.write"), writes);
    assert!(
        env.uploads.try_recv().is_err(),
        "no upload after the delete"
    );
    assert!(!session.exists());

    // A repeat on the same connection gets the saved report; one on a
    // later connection is a late close with nothing left to abandon.
    env.close("conn_b", "close_3", "bind_a");
    let repeated = env.report().await;
    assert_eq!(repeated["request_message_id"], "close_3");
    assert_eq!(repeated["lost_changes"], expected);
    env.owner.connection_ended(id("conn_b"));
    env.close("conn_c", "close_4", "bind_a");
    let later = env.report().await;
    assert_eq!(
        (&later["status"], &later["request_message_id"]),
        (&json!("closed"), &json!("close_4")),
        "{later}"
    );
    assert!(later.get("lost_changes").is_none());
    assert_eq!(env.port.kept.lock().unwrap().len(), 1);
}

/// Prepares `bind_a` with the drive and writes `f` into its mount.
async fn prepared_with_a_write(env: &mut Env) {
    let project = env.project.clone();
    env.prepare("prepare_1", "bind_a", &project);
    let prepared = env.report().await;
    assert_eq!(prepared["status"], "prepared", "{prepared}");
    let file = env.mount_point().join("f");
    tokio::task::spawn_blocking(move || std::fs::write(file, "new"))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_abandons_what_its_held_upload_covers_30_seconds_after_its_admission() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    prepared_with_a_write(&mut env).await;
    env.close("conn_a", "close_1", "bind_a");
    tokio::time::timeout(Duration::from_secs(30), env.uploads.recv())
        .await
        .expect("f's upload within 30 s")
        .unwrap();
    env.advance(Duration::from_secs(29));
    tokio::time::sleep(Duration::from_millis(300)).await;
    env.entries().await;
    assert!(env.reports.try_recv().is_err(), "no report at 29 s");
    env.advance(Duration::from_secs(1));
    let closed = env.report().await;
    assert_eq!(closed["status"], "closed", "{closed}");
    assert_eq!(
        closed["lost_changes"],
        json!([{ "request_message_id": "close_1", "drive_id": DRIVE, "paths": ["f"], "paths_total": 1 }])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_during_a_session_drives_uploads_only_after_its_applied_report() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    let project = env.project.clone();
    env.prepare_drives("prepare_1", "bind_a", &project, &[DRIVE, OTHER]);
    let prepared = env.report().await;
    assert_eq!(prepared["status"], "prepared", "{prepared}");
    for (drive, name) in [(DRIVE, "f"), (OTHER, "g")] {
        let file = env.mount_point_of(drive).join(name);
        tokio::task::spawn_blocking(move || std::fs::write(file, "new"))
            .await
            .unwrap()
            .unwrap();
    }
    // The session.drives keeps only the other drive.
    env.owner.drives(Drives {
        connection_id: id("conn_a"),
        message_id: id("drives_1"),
        binding_id: id("bind_a"),
        drives: vec![DriveBinding {
            drive_id: SafeId::new(OTHER).unwrap(),
            access: DriveAccess::ReadWrite,
        }],
        traceparent: None,
    });
    tokio::time::timeout(Duration::from_secs(30), env.uploads.recv())
        .await
        .expect("f's upload within 30 s")
        .unwrap();
    env.close("conn_a", "close_1", "bind_a");
    env.entries().await;
    env.advance(Duration::from_secs(30));
    let applied = env.report().await;
    assert_eq!(
        (&applied["status"], &applied["request_message_id"]),
        (&json!("applied"), &json!("drives_1")),
        "{applied}"
    );
    assert_eq!(applied["lost_changes"][0]["paths"], json!(["f"]));
    // The close's own final upload starts only now, 30 seconds after its
    // admission, so it abandons what it holds at once.
    let closed = env.report().await;
    assert_eq!(closed["status"], "closed", "{closed}");
    assert_eq!(
        closed["lost_changes"],
        json!([{ "request_message_id": "close_1", "drive_id": OTHER, "paths": ["g"], "paths_total": 1 }])
    );
    assert_eq!(env.port.kept.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_of_a_replaced_binding_leaves_the_live_bindings_mount() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    let missing = env.project.join("missing");
    env.prepare("prepare_1", "bind_failed", &missing);
    let failed = env.report().await;
    assert_eq!(failed["status"], "failed", "{failed}");
    let project = env.project.clone();
    env.prepare("prepare_2", "bind_live", &project);
    let prepared = env.report().await;
    assert_eq!(prepared["status"], "prepared", "{prepared}");
    let point = env.mount_point();
    let device = current_device(&point).unwrap();
    assert!(device.is_some());

    env.close("conn_a", "close_1", "bind_failed");
    let closed = env.report().await;
    assert_eq!(
        (&closed["status"], &closed["binding_id"]),
        (&json!("closed"), &json!("bind_failed")),
        "{closed}"
    );
    assert!(closed.get("lost_changes").is_none());
    let entries = env.entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].binding_id.as_str(), "bind_live");
    assert_eq!(
        serde_json::to_value(entries[0].state).unwrap(),
        json!("prepared")
    );
    assert_eq!(current_device(&point).unwrap(), device);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_of_a_failed_binding_abandons_the_pending_changes_of_its_sync_state() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    // A change still pending from an earlier App Server.
    let directory = env.root.join("sessions").join(SESSION);
    state::files::create_private_dirs(&directory).unwrap();
    let mut sync_state = drive::state::SyncState::create(&directory.join(drive::state::FILE_NAME))
        .await
        .unwrap();
    sync_state
        .apply(drive::state::Write {
            pending: vec![drive::state::PendingChange {
                drive_id: DRIVE.to_owned(),
                path: "f".to_owned(),
                change: "delete".to_owned(),
                base_sha256: Some("a".repeat(64)),
                content_sha256: None,
                executable: None,
                uploaded: false,
                last_write_at: 0,
                task_id: None,
                fault_code: None,
            }],
            ..drive::state::Write::default()
        })
        .await
        .unwrap();
    sync_state.close().await;
    let missing = env.project.join("missing");
    env.prepare("prepare_1", "bind_a", &missing);
    let failed = env.report().await;
    assert_eq!(failed["status"], "failed", "{failed}");

    env.close("conn_a", "close_1", "bind_a");
    let closed = env.report().await;
    assert_eq!(closed["status"], "closed", "{closed}");
    assert_eq!(
        closed["lost_changes"],
        json!([{ "request_message_id": "close_1", "drive_id": DRIVE, "paths": ["f"], "paths_total": 1 }])
    );
    assert!(!directory.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_admitted_while_the_drive_sync_opens_waits_for_the_open_and_leaves_no_file() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::with_drives(false);
    let seams = &SEAMS.1;
    let opening = seams.count("drive_sync.opening");
    let project = env.project.clone();
    env.prepare("prep_a", "bind_a", &project);
    // The open waits for the drive context, which the test holds back.
    seams.wait_for_count("drive_sync.opening", opening + 1);
    env.close("conn_a", "close_a", "bind_a");
    tokio::time::sleep(Duration::from_millis(500)).await;
    env.entries().await;
    assert!(
        env.reports.try_recv().is_err(),
        "the close waits for the open"
    );

    env.publish();
    let closed = loop {
        let report = env.report().await;
        if report["request_message_id"] == "close_a" {
            break report;
        }
    };
    assert_eq!(closed["status"], "closed", "{closed}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    env.entries().await;
    let session = env.root.join("sessions").join(SESSION);
    assert!(!session.exists(), "no file of the Session is left");
}

/// Developer writes have no tool-task owner.
struct Developer;

impl protocol::ports::TaskOwnership for Developer {
    fn task_of(&self, _: u32, reply: protocol::ports::TaskOwnerReply) {
        reply(None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_close_releases_running_and_queued_uploads_for_another_session() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    env.prepare("prepare_1", "bind_a", &env.project);
    let prepared = env.report().await;
    assert_eq!(prepared["status"], "prepared", "{prepared}");
    let point = env.mount_point();
    tokio::task::spawn_blocking(move || {
        for index in 0..9 {
            std::fs::write(
                point.join(format!("file{index}")),
                format!("content {index}"),
            )
            .unwrap();
        }
    })
    .await
    .unwrap();
    let sync = env.registry.0.lock().unwrap().clone().unwrap();
    let request = || drive::SyncPointRequest {
        task_id: None,
        wait: Duration::from_secs(30),
        traceparent: None,
    };
    let _first_result = sync.sync_point(request());
    let mut held = Vec::new();
    for _ in 0..8 {
        held.push(
            tokio::time::timeout(Duration::from_secs(10), env.uploads.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    // A second Session shares the machine's actual Content cache and
    // waits behind the first Session's eight running uploads.
    let directory = env.root.join("sessions/sess_b");
    std::fs::create_dir_all(&directory).unwrap();
    let state = drive::state::SyncState::create(&directory.join("sync-state.db"))
        .await
        .unwrap();
    let (sent, _) = mpsc::unbounded_channel();
    let context = &env.drives.1;
    let other = DriveSync::spawn(drive::SyncConfig {
        commit_batch_size: drive::commit::MAX_CHANGES,
        session_id: SafeId::new("sess_b").unwrap(),
        root: env.root.clone(),
        state,
        transfer: context.transfer.clone(),
        cache: context.cache.clone(),
        tasks: Arc::new(Developer),
        mounting: Mounting::Unavailable("operation-table test".into()),
        registry: Arc::new(SyncRegistry::default()),
        clock: context.clock.clone(),
        reports: Arc::new(Reports(sent, AtomicU64::new(1))),
    });
    other.binding_current(drive::transfer::Binding {
        connection_id: id("conn_b"),
        binding_id: id("bind_b"),
    });
    other.mounted(id(DRIVE), Vec::new());
    other.check().await;
    let attr = other
        .create(&id(DRIVE), drive::ops::TOP_INODE, b"other", 0o644, None)
        .await
        .unwrap();
    other
        .write(&id(DRIVE), attr.ino, 0, b"other session", None)
        .await
        .unwrap();
    let _other_result = other.sync_point(request());
    other.tables().await;
    assert!(env.uploads.try_recv().is_err());

    // No ordinary final-upload path has run. The old connection ends,
    // then the next connection asks to remove the old local computer.
    env.owner.connection_ended(id("conn_a"));
    env.entries().await;
    env.close("conn_b", "late_close", "bind_a");
    let closed = env.report().await;
    assert_eq!(closed["status"], "closed", "{closed}");
    let (hash, _answer) = tokio::time::timeout(Duration::from_secs(5), env.uploads.recv())
        .await
        .expect("the other Session can upload after late close")
        .unwrap();
    use sha2::Digest;
    let expected: String = sha2::Sha256::digest(b"other session")
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(
        hash, expected,
        "no queued upload of the closed Session starts"
    );
    for (_, answer) in held {
        assert!(answer.is_closed(), "late close cancels running transfers");
    }
    other.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_reclaims_only_content_unreferenced_by_remaining_persistent_sessions() {
    let _serial = SERIAL.lock().await;
    for unreadable in [false, true] {
        let mut env = Env::new();
        let project = env.project.clone();
        env.prepare("prepare_a", "bind_a", &project);
        assert_eq!(env.report().await["status"], "prepared");
        let other = env.root.join("sessions/sess_unopened");
        state::files::create_private_dirs(&other).unwrap();
        let db_path = other.join(drive::state::FILE_NAME);
        let mut db = drive::state::SyncState::create(&db_path).await.unwrap();
        db.apply(drive::state::Write {
            listing: vec![drive::state::ListingRow {
                drive_id: DRIVE.into(),
                path: "shared".into(),
                version_id: "ver_shared".into(),
                content_sha256: "b".repeat(64),
                size_bytes: 6,
                executable: false,
            }],
            ..drive::state::Write::default()
        })
        .await
        .unwrap();
        db.close().await;
        if unreadable {
            std::fs::write(&db_path, b"unreadable database").unwrap();
        }
        let exclusive = env.root.join("cache/blobs").join("a".repeat(64));
        let shared = env.root.join("cache/blobs").join("b".repeat(64));
        std::fs::write(&exclusive, b"only A").unwrap();
        std::fs::write(&shared, b"shared").unwrap();
        // An already opened descriptor remains usable after cache unlink.
        let mut opened = std::fs::File::open(&exclusive).unwrap();
        env.close("conn_a", "close_a", "bind_a");
        assert_eq!(env.report().await["status"], "closed");
        assert!(!env.root.join("sessions").join(SESSION).exists());
        assert_eq!(exclusive.exists(), unreadable);
        assert_eq!(std::fs::read(shared).unwrap(), b"shared");
        let mut contents = String::new();
        std::io::Read::read_to_string(&mut opened, &mut contents).unwrap();
        assert_eq!(contents, "only A");
        assert!(db_path.exists());
    }
}

async fn persist_pending(env: &Env) {
    let directory = env.root.join("sessions").join(SESSION);
    state::files::create_private_dirs(&directory).unwrap();
    let mut db = drive::state::SyncState::create(&directory.join(drive::state::FILE_NAME))
        .await
        .unwrap();
    db.apply(drive::state::Write {
        pending: vec![drive::state::PendingChange {
            drive_id: DRIVE.into(),
            path: "pending".into(),
            change: "delete".into(),
            base_sha256: Some("a".repeat(64)),
            content_sha256: None,
            executable: None,
            uploaded: false,
            last_write_at: 0,
            task_id: None,
            fault_code: None,
        }],
        ..drive::state::Write::default()
    })
    .await
    .unwrap();
    db.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_late_close_keeps_pending_state_until_a_live_requester_takes_over() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::with_drives(false);
    persist_pending(&env).await;
    let opening = SEAMS.1.count("drive_sync.opening");
    env.prepare("prep_a", "bind_a", &env.project);
    SEAMS.1.wait_for_count("drive_sync.opening", opening + 1);
    env.owner.connection_ended(id("conn_a"));
    env.close("conn_b", "close_b", "bind_a");
    env.owner.connection_ended(id("conn_b"));
    env.entries().await;
    env.publish();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if env.registry.0.lock().unwrap().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        env.entries().await;
    })
    .await
    .unwrap();
    // Let the released opening completion and close progress run. Neither
    // the invalid close nor idle time may abandon the durable change.
    tokio::time::sleep(Duration::from_millis(200)).await;
    env.entries().await;
    let directory = env.root.join("sessions").join(SESSION);
    assert!(
        directory.exists(),
        "disconnected late close preserves local state"
    );
    assert!(env.port.kept.lock().unwrap().is_empty());
    assert!(env.reports.try_recv().is_err());
    env.close("conn_c", "close_c", "bind_a");
    let closed = env.report().await;
    assert_eq!(closed["request_message_id"], "close_c");
    assert_eq!(closed["lost_changes"][0]["request_message_id"], "close_c");
    assert_eq!(closed["lost_changes"][0]["paths"], json!(["pending"]));
    assert_eq!(env.port.kept.lock().unwrap().len(), 1);
    assert!(!directory.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_joined_during_deletion_saves_the_result_for_the_live_connection() {
    let _serial = SERIAL.lock().await;
    let mut env = Env::new();
    prepared_with_a_write(&mut env).await;
    env.owner.connection_ended(id("conn_a"));
    let (sent, mut held) = mpsc::unbounded_channel();
    *env.port.hold_kept.lock().unwrap() = Some(sent);
    env.close("conn_b", "close_b", "bind_a");
    let kept = tokio::time::timeout(Duration::from_secs(10), held.recv())
        .await
        .unwrap()
        .unwrap();
    env.owner.connection_ended(id("conn_b"));
    env.close("conn_c", "close_c", "bind_a");
    env.entries().await;
    kept();
    let closed = env.report().await;
    assert_eq!(
        closed["connection_id"], "conn_c",
        "no report goes to the ended connection"
    );
    assert_eq!(closed["request_message_id"], "close_c");
    assert_eq!(
        closed["lost_changes"][0]["request_message_id"], "close_b",
        "already kept loss evidence retains its original identity"
    );
    env.close("conn_c", "repeat_c", "bind_a");
    let repeat = env.report().await;
    assert_eq!(repeat["request_message_id"], "repeat_c");
    assert_eq!(repeat["lost_changes"], closed["lost_changes"]);
    assert_eq!(env.port.kept.lock().unwrap().len(), 1);
    env.owner.connection_ended(id("conn_c"));
    env.entries().await;
    assert_eq!(
        env.port.idle.load(Ordering::SeqCst),
        1,
        "ended connection releases the saved result and owner"
    );
}
