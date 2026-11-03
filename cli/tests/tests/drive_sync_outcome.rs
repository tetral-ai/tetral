//! Task supervisors given a Session's drive sync through `DriveSyncPort` as
//! they start, with real FUSE mounts, the real restricted process of a
//! `test-seams` `tetral`, a stub `DriveTransfer` that holds every upload
//! and each download until the test releases it, and a clock the test
//! moves: the drive path a restricted command reaches, and the results a
//! mount failure and a download appear in.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use drive::cache::{CacheConfig, ContentCache};
use drive::mount::{Bound, Mounting, Registry};
use drive::state::SyncState;
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use drive::{DriveSync, SyncConfig};
use execution::isolation::{self, Isolation};
use execution::limits::Visible;
use execution::policy::{DriveMount, SessionDrives, TaskPermissions, TranslationInputs};
use execution::report::Correlation;
use execution::store::{LOG_HEAD_BYTES, TaskFiles};
use execution::supervisor::{
    self, Bounds, Context, Control, SupervisorMailbox, SupervisorMessage, TaskDrives, TaskListener,
    TaskSpec,
};
use protocol::connection::DriveAccess;
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::id::{Id, SafeId};
use protocol::ports::{
    ReportAdmissionError, ReportId, ReportSink, Submission, TaskOwnerReply, TaskOwnership,
};
use serde_json::{Value, json};
use state::clock::Clock;
use tokio::sync::mpsc;

const SESSION: &str = "sess_a";
const DRIVE: &str = "drive_a";

/// Lists `files` and holds every upload and commit; each upload's hash is
/// told to the test. A download is told to the test and gives `content`
/// once the test releases it. The next listing of each drive in `denied`
/// is refused `403 drive_access_denied`.
type RemoteFiles = std::collections::HashMap<String, (Vec<Value>, &'static [u8])>;

struct Stub {
    files: Vec<Value>,
    overrides: Arc<std::sync::Mutex<RemoteFiles>>,
    denied: Arc<std::sync::Mutex<Vec<String>>>,
    uploads: mpsc::UnboundedSender<String>,
    downloads: mpsc::UnboundedSender<(Id, Binding)>,
    release: Arc<tokio::sync::Notify>,
    content: &'static [u8],
    ready_upload: Option<String>,
    commits: mpsc::UnboundedSender<CommitAnswer>,
}

impl Stub {
    fn refused(&self, drive_id: &Id) -> bool {
        let mut denied = self.denied.lock().unwrap();
        let before = denied.len();
        denied.retain(|drive| drive != drive_id.as_str());
        denied.len() < before
    }
}

type CommitAnswer = (
    Vec<protocol::drive_transfer::CommitChange>,
    tokio::sync::oneshot::Sender<Result<CommitResponse, Ended>>,
);

const DENIED: Ended = Ended::Permanent {
    status: 403,
    code: Some("drive_access_denied"),
};

impl DriveTransfer for Stub {
    fn list_files(&self, request: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let refused = self.refused(&request.drive_id);
        let files = self
            .overrides
            .lock()
            .unwrap()
            .get(request.drive_id.as_str())
            .map(|(files, _)| files.clone())
            .unwrap_or_else(|| self.files.clone());
        let page = json!({ "files": files, "has_more": false, "start_seq": 1 });
        Box::pin(async move {
            if refused {
                return Err(DENIED);
            }
            Ok(serde_json::from_value(page).unwrap())
        })
    }

    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let refused = self.refused(&request.drive_id);
        let page = json!({ "records": [], "has_more": false });
        Box::pin(async move {
            if refused {
                return Err(DENIED);
            }
            Ok(serde_json::from_value(page).unwrap())
        })
    }

    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        let _ = self.uploads.send(request.body.sha256.as_str().to_owned());
        if self.ready_upload.as_deref() == Some(request.body.sha256.as_str()) {
            Box::pin(async move {
                Ok(serde_json::from_value(json!({
                    "content_sha256": request.body.sha256, "size_bytes": 5
                }))
                .unwrap())
            })
        } else {
            Box::pin(std::future::pending())
        }
    }

    fn commit(&self, request: Request<Commit>) -> Transfer<'_, CommitResponse> {
        let (answer, received) = tokio::sync::oneshot::channel();
        let _ = self.commits.send((request.body.changes, answer));
        Box::pin(async move { received.await.unwrap() })
    }

    fn download_content<'a>(
        &'a self,
        request: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        let content = self
            .overrides
            .lock()
            .unwrap()
            .get(request.drive_id.as_str())
            .map(|(_, content)| *content)
            .unwrap_or(self.content);
        let binding = request.binding.borrow().clone().unwrap();
        let _ = self.downloads.send((request.drive_id, binding));
        Box::pin(async move {
            self.release.notified().await;
            sink.append(content).map_err(Ended::Sink)
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

/// Every process belongs to the one task.
struct OneTask;

impl TaskOwnership for OneTask {
    fn task_of(&self, _: u32, reply: TaskOwnerReply) {
        reply(Some(Id::new("task").unwrap()));
    }
}

struct NoEvents;

impl TaskListener for NoEvents {
    fn started(&self, _: u64) {}
    fn gone(&self) {}
    fn withdrawn(&self) {}
}

/// Records each report's message.
struct Reports(mpsc::UnboundedSender<Value>, AtomicU64);

impl ReportSink for Reports {
    fn submit(&self, submission: Submission) -> ReportId {
        let id = ReportId(self.1.fetch_add(1, Ordering::Relaxed));
        let message = Id::new(format!("r{}", id.0)).unwrap();
        let bytes = (submission.build)(&submission.connection_id, &message).unwrap();
        protocol::decode::<protocol::tool::ToolStatus>(&bytes).expect("a valid report");
        let _ = self.0.send(serde_json::from_slice(&bytes).unwrap());
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

/// A Session's drive sync with each drive of `bound` mounted, under a
/// state root given through a symbolic link.
struct Fixture {
    tetral: PathBuf,
    canonical: PathBuf,
    real_root: PathBuf,
    project: PathBuf,
    nanos: Arc<AtomicU64>,
    sync: DriveSync,
    port: Arc<dyn protocol::ports::DriveSyncPort>,
    drives: PathBuf,
    bound: Vec<&'static str>,
    access: DriveAccess,
    policy_override: Option<Value>,
    transfer: Arc<Stub>,
    cache: ContentCache,
    clock: Clock,
    uploaded: mpsc::UnboundedReceiver<String>,
    commits: mpsc::UnboundedReceiver<CommitAnswer>,
    downloads: mpsc::UnboundedReceiver<(Id, Binding)>,
    release: Arc<tokio::sync::Notify>,
    denied: Arc<std::sync::Mutex<Vec<String>>>,
    _unmount: Unmount,
    _directory: tempfile::TempDir,
}

impl Fixture {
    /// The first listing of each drive of `failing` is refused, which
    /// unmounts it and holds the failure for the next check.
    async fn new(
        bound: Vec<&'static str>,
        failing: &[&str],
        files: Vec<Value>,
        content: &'static [u8],
    ) -> Fixture {
        Self::with_ready_upload(bound, failing, files, content, None).await
    }

    async fn with_ready_upload(
        bound: Vec<&'static str>,
        failing: &[&str],
        files: Vec<Value>,
        content: &'static [u8],
        ready_upload: Option<String>,
    ) -> Fixture {
        let tetral = tetral_tests::binaries::tetral().canonicalize().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let canonical = directory.path().canonicalize().unwrap();
        let unmount = Unmount(canonical.clone());
        let real_root = canonical.join("state");
        std::fs::create_dir(&real_root).unwrap();
        state::files::create_private_dirs(&real_root.join(state::paths::CREDENTIAL_DIRECTORY))
            .unwrap();
        state::files::create_private_dirs(&real_root.join(state::paths::RUN)).unwrap();
        // `$TETRAL_HOME` is given through a symbolic link; `R` is its target.
        let linked_root = canonical.join("home-link");
        std::os::unix::fs::symlink(&real_root, &linked_root).unwrap();
        let project = canonical.join("project");
        std::fs::create_dir(&project).unwrap();

        let nanos = Arc::new(AtomicU64::new(0));
        let clock = {
            let nanos = nanos.clone();
            Clock::new(
                move || Duration::from_nanos(nanos.load(Ordering::SeqCst)),
                Duration::from_millis(2),
            )
        };
        let (uploads, uploaded) = mpsc::unbounded_channel();
        let (send_commits, commits) = mpsc::unbounded_channel();
        let (downloaded, downloads) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let denied = Arc::new(std::sync::Mutex::new(
            failing.iter().map(|drive| (*drive).to_owned()).collect(),
        ));
        let transfer = Arc::new(Stub {
            files,
            overrides: Arc::default(),
            denied: denied.clone(),
            uploads,
            downloads: downloaded,
            release: release.clone(),
            content,
            ready_upload,
            commits: send_commits,
        });
        let cache = ContentCache::spawn(CacheConfig {
            root: linked_root.clone(),
            transfer: transfer.clone(),
            clock: clock.clone(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let session_dir = linked_root.join("sessions").join(SESSION);
        state::files::create_private_dirs(&session_dir).unwrap();
        let sync_state = SyncState::create(&session_dir.join("sync-state.db"))
            .await
            .unwrap();
        let sync = DriveSync::spawn(SyncConfig {
            commit_batch_size: drive::commit::MAX_CHANGES,
            session_id: SafeId::new(SESSION).unwrap(),
            root: linked_root.clone(),
            state: sync_state,
            transfer: transfer.clone(),
            cache: cache.clone(),
            tasks: Arc::new(OneTask),
            mounting: Mounting::Fuse,
            registry: Arc::new(NoRegistry),
            clock: clock.clone(),
            reports: Arc::new(NoReports),
        });
        sync.binding_current(Binding {
            connection_id: id("conn_a"),
            binding_id: id("bind_a"),
        });
        let failures = sync
            .mount_drives(
                bound
                    .iter()
                    .map(|drive| Bound {
                        drive_id: SafeId::new(*drive).unwrap(),
                        read_only: false,
                        exclude: Vec::new(),
                    })
                    .collect(),
            )
            .await;
        assert!(failures.is_empty(), "{failures:?}");
        tokio::time::timeout(Duration::from_secs(10), sync.check())
            .await
            .expect("the drives caught up");
        until_unmounted(&sync, failing).await;
        let port: Arc<dyn protocol::ports::DriveSyncPort> = Arc::new(sync.clone());
        let drives = port
            .drives_variable()
            .expect("a Session that binds a mounted drive");
        Fixture {
            tetral,
            canonical,
            real_root,
            project,
            nanos,
            sync,
            port,
            drives,
            bound,
            access: DriveAccess::ReadWrite,
            policy_override: None,
            transfer,
            cache,
            clock,
            uploaded,
            commits,
            downloads,
            release,
            denied,
            _unmount: unmount,
            _directory: directory,
        }
    }

    fn policy(&self) -> Value {
        if let Some(policy) = &self.policy_override {
            return policy.clone();
        }
        json!({
            "schema_version": 1,
            "filesystem": { "rules": [{ "path": self.project, "access": "read_write" }] },
            "network": { "mode": "disabled" },
        })
    }

    fn inputs(&self) -> TranslationInputs {
        let temporary = self.canonical.join("temporary");
        TranslationInputs {
            state_root: self.real_root.clone(),
            socket_directory: self.canonical.join("socket"),
            task_directories: temporary.clone(),
            task_temporary_directory: temporary.join("task"),
            drives: Some(SessionDrives {
                directory: self.drives.clone(),
                mounts: self
                    .bound
                    .iter()
                    .map(|drive| DriveMount {
                        path: self.drives.join(drive),
                        access: self.access,
                    })
                    .collect(),
            }),
            trusted_programs: isolation::trusted_programs(&self.tetral, None, &self.project),
        }
    }

    /// Starts the task `task` of `tool`, admitted while the binding had no
    /// drive, given its drives as it starts; its reports, each validated.
    fn start(
        &self,
        task: &str,
        tool: Value,
    ) -> (SupervisorMailbox, mpsc::UnboundedReceiver<Value>) {
        self.start_with(task, tool, self.port.clone(), Arc::new(NoEvents))
    }

    /// [`Fixture::start`] with another drive sync port and task listener.
    fn start_with(
        &self,
        task: &str,
        tool: Value,
        port: Arc<dyn protocol::ports::DriveSyncPort>,
        listener: Arc<dyn TaskListener>,
    ) -> (SupervisorMailbox, mpsc::UnboundedReceiver<Value>) {
        let policy = self.policy();
        let inputs = self.inputs();
        let admitted = TranslationInputs {
            drives: None,
            ..inputs.clone()
        };
        let temporary = self.canonical.join("temporary");
        let (sent, reports) = mpsc::unbounded_channel();
        let spec = TaskSpec {
            connection_id: id("conn_a"),
            correlation: correlation(task, &format!("execute_{task}")),
            permissions: TaskPermissions::admit(Some(&policy), &admitted).unwrap(),
            limits: Visible::requested(None),
            cwd: self.project.clone(),
            files: TaskFiles::new(
                self.real_root.join(task),
                temporary.join(task),
                LOG_HEAD_BYTES,
            ),
            traceparent: None,
            received: state::seams::now(),
            record: self.real_root.join(format!("record_{task}")),
            listener,
            drive_sync: None,
        };
        let mailbox = supervisor::spawn(
            spec,
            serde_json::from_value(tool).unwrap(),
            Context {
                tetral: self.tetral.clone(),
                isolation: Isolation::new(),
                reports: Arc::new(Reports(sent, AtomicU64::new(1))),
                bounds: Bounds::default(),
                boot_id: None,
                activity: Arc::new(protocol::ports::NoActivity),
            },
        );
        mailbox
            .send(SupervisorMessage::Drives(Box::new(TaskDrives {
                permissions: TaskPermissions::admit(Some(&policy), &inputs).unwrap(),
                drive_sync: Some(port),
            })))
            .unwrap();
        mailbox.send(SupervisorMessage::Start).unwrap();
        (mailbox, reports)
    }

    /// Refuses the next listing of `drive` and starts a catch-up of every
    /// mounted drive, which unmounts `drive`.
    async fn deny(&self, drive: &'static str) {
        self.denied.lock().unwrap().push(drive.to_owned());
        self.sync.connected();
        until_unmounted(&self.sync, &[drive]).await;
    }
}

/// Waits until each drive of `drives` is unmounted by a failed listing.
async fn until_unmounted(sync: &DriveSync, drives: &[&str]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let unmounted = sync.unmounted().await;
        if drives.iter().all(|drive| unmounted.contains_key(*drive)) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{drives:?} unmounted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn correlation(task: &str, message: &str) -> Correlation {
    Correlation {
        session_id: id(SESSION),
        binding_id: id("bind_a"),
        tool_use_event_id: id("event"),
        task_id: id(task),
        request_message_id: id(message),
    }
}

async fn next(reports: &mut mpsc::UnboundedReceiver<Value>, what: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(30), reports.recv())
        .await
        .unwrap_or_else(|_| panic!("{what} within 30 s"))
        .unwrap()
}

/// The `drive_id` of each `unmounted` entry of a result's `drive_sync`.
fn unmounted(report: &Value) -> Vec<String> {
    report["result"]["drive_sync"]["unmounted"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| entry["drive_id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restricted_command_reaches_the_drive_through_tetral_drives_under_a_linked_root() {
    let mut fixture = Fixture::new(vec![DRIVE], &[], Vec::new(), b"").await;
    let drives = fixture.drives.clone();
    let expected = fixture
        .real_root
        .join("sessions")
        .join(SESSION)
        .join("drives");
    assert_eq!(drives, expected, "the canonical root, not the link");
    let cmd = format!(
        "printf %s \"$TETRAL_DRIVES\" && ls \"$TETRAL_DRIVES\"/{DRIVE} && echo written > \"$TETRAL_DRIVES\"/{DRIVE}/f"
    );
    let (_mailbox, mut reports) = fixture.start(
        "task",
        json!({ "tool": "exec_command", "input": { "cmd": cmd, "wait_ms": 30_000 } }),
    );

    // The terminal result's sync point covers `f`; its upload is held past
    // the sync point's wait.
    let hash = tokio::time::timeout(Duration::from_secs(60), fixture.uploaded.recv())
        .await
        .expect("f's upload within 60 s")
        .unwrap();
    assert_eq!(hash, sha256_hex(b"written\n"));
    fixture
        .nanos
        .fetch_add(supervisor::SYNC_WAIT.as_nanos() as u64, Ordering::SeqCst);
    let report = next(&mut reports, "the terminal report").await;
    let result = &report["result"];
    assert_eq!(result["result"]["exit_code"], 0, "{report}");
    assert_eq!(
        result["result"]["stdout"]["text"].as_str().unwrap(),
        drives.to_str().unwrap()
    );
    assert_eq!(
        result["drive_sync"]["not_synced"],
        json!([{
            "path": drives.join(DRIVE).join("f"),
            "reason": "uploading",
        }])
    );
    fixture.sync.detach_all();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mount_failure_is_in_the_first_report_and_a_view_image_result_and_downloads_only_in_running_ones()
 {
    const DENIED_FIRST: &str = "drive_b";
    const LATER: &str = "drive_c";
    let listed = json!({
        "path": "c.txt",
        "version_id": "v_c",
        "content_sha256": sha256_hex(b"sea"),
        "size_bytes": 3,
        "executable": false,
    });
    let mut fixture = Fixture::new(
        vec![DRIVE, DENIED_FIRST, LATER],
        &[DENIED_FIRST],
        vec![listed],
        b"sea",
    )
    .await;
    let path = fixture.drives.join(DRIVE).join("c.txt");
    // The failed listing of drive_b is held for the command's check.
    let cmd = format!("cat \"$TETRAL_DRIVES\"/{DRIVE}/c.txt");
    let (mailbox, mut reports) = fixture.start(
        "task",
        json!({ "tool": "exec_command", "input": { "cmd": cmd, "wait_ms": 5_000, "on_wait_expiry": "detach" } }),
    );
    tokio::time::timeout(Duration::from_secs(60), fixture.downloads.recv())
        .await
        .expect("c.txt's download within 60 s")
        .unwrap();
    let downloading = json!([{ "path": path, "size_bytes": 3, "downloaded_bytes": 0 }]);

    // The first report carries the failed mount and the download.
    let running = next(&mut reports, "the running report").await;
    assert_eq!(running["result"]["status"], "running", "{running}");
    assert_eq!(unmounted(&running), [DENIED_FIRST]);
    assert_eq!(running["result"]["drive_sync"]["downloading"], downloading);

    // A stdin reply carries the download alone.
    mailbox
        .send(SupervisorMessage::Stdin(Box::new(Control {
            connection_id: id("conn_a"),
            correlation: correlation("task", "stdin"),
            traceparent: None,
            payload: serde_json::from_value(json!({
                "session_id": SESSION, "binding_id": "bind_a",
                "tool_use_event_id": "event", "task_id": "task", "chars": "",
            }))
            .unwrap(),
        })))
        .unwrap();
    let reply = next(&mut reports, "the stdin reply").await;
    assert_eq!(unmounted(&reply), Vec::<String>::new(), "{reply}");
    assert_eq!(reply["result"]["drive_sync"]["downloading"], downloading);

    // The finished result carries neither.
    fixture.release.notify_one();
    let terminal = next(&mut reports, "the terminal report").await;
    assert_eq!(
        terminal["result"]["result"]["stdout"]["text"], "sea",
        "{terminal}"
    );
    assert!(terminal["result"].get("drive_sync").is_none(), "{terminal}");

    // A view_image result carries the failure its own check found new,
    // and not the one the command's check reported.
    let image = fixture.project.join("pixel.png");
    std::fs::write(&image, PIXEL).unwrap();
    fixture.deny(LATER).await;
    let (_mailbox, mut reports) = fixture.start(
        "image",
        json!({ "tool": "view_image", "input": { "path": image } }),
    );
    let viewed = next(&mut reports, "the view_image result").await;
    assert_eq!(unmounted(&viewed), [LATER], "{viewed}");
    assert!(
        viewed["result"]["drive_sync"].get("downloading").is_none(),
        "{viewed}"
    );
    fixture.sync.detach_all();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_command_whose_check_found_a_failed_mount_runs_without_that_drive_beside_a_mounted_one() {
    // The command's check sends its failure to the drive sync just before
    // the launch reads the mounted drive's root, which that sync answers.
    const FAILED: &str = "drive_b";
    let mut fixture = Fixture::new(vec![DRIVE], &[], Vec::new(), b"").await;
    std::fs::write(fixture.drives.join(FAILED), b"").unwrap();
    let bound = [DRIVE, FAILED].map(|drive| Bound {
        drive_id: SafeId::new(drive).unwrap(),
        read_only: false,
        exclude: Vec::new(),
    });
    let failures = fixture.sync.mount_drives(bound.to_vec()).await;
    assert_eq!(failures.len(), 1, "{failures:?}");
    // The policy names both drives, as the binding binds them.
    fixture.bound = vec![DRIVE, FAILED];
    let cmd = format!("echo started && echo written > \"$TETRAL_DRIVES\"/{DRIVE}/f");
    let (_mailbox, mut reports) = fixture.start(
        "task",
        json!({ "tool": "exec_command", "input": { "cmd": cmd, "wait_ms": 30_000 } }),
    );
    let hash = tokio::time::timeout(Duration::from_secs(60), fixture.uploaded.recv())
        .await
        .expect("f's upload within 60 s")
        .unwrap();
    assert_eq!(hash, sha256_hex(b"written\n"));
    fixture
        .nanos
        .fetch_add(supervisor::SYNC_WAIT.as_nanos() as u64, Ordering::SeqCst);
    let terminal = next(&mut reports, "the terminal report").await;
    assert_eq!(terminal["result"]["result"]["exit_code"], 0, "{terminal}");
    assert_eq!(
        terminal["result"]["result"]["stdout"]["text"], "started\n",
        "{terminal}"
    );
    assert_eq!(unmounted(&terminal), [FAILED]);

    // A later command reads the mounted drive; the failure is not reported
    // again.
    let cmd = format!("cat \"$TETRAL_DRIVES\"/{DRIVE}/f");
    let (_mailbox, mut reports) = fixture.start(
        "later",
        json!({ "tool": "exec_command", "input": { "cmd": cmd, "wait_ms": 30_000 } }),
    );
    // Its terminal sync point waits for f's held upload: the clock moves
    // past each wait until the report comes.
    let mut waits = 0;
    let later = loop {
        waits += 1;
        assert!(waits <= 30, "the later terminal report within 30 waits");
        fixture
            .nanos
            .fetch_add(supervisor::SYNC_WAIT.as_nanos() as u64, Ordering::SeqCst);
        if let Ok(report) = tokio::time::timeout(Duration::from_secs(1), reports.recv()).await {
            break report.unwrap();
        }
    };
    assert_eq!(later["result"]["result"]["exit_code"], 0, "{later}");
    assert_eq!(
        later["result"]["result"]["stdout"]["text"], "written\n",
        "{later}"
    );
    assert_eq!(unmounted(&later), Vec::<String>::new(), "{later}");
    fixture.sync.detach_all();
}

/// A drive sync port whose checks find nothing and whose `running` sync
/// points answer only once the test releases them.
#[derive(Debug, Default)]
struct HeldRunning {
    running: AtomicU64,
    release: Arc<tokio::sync::Notify>,
}

impl protocol::ports::DriveSyncPort for HeldRunning {
    fn check(&self, _: protocol::ports::DriveCheck) -> protocol::ports::DriveCheckAnswer {
        Box::pin(async { protocol::ports::DriveChecked::default() })
    }

    fn sync_point(&self, point: protocol::ports::DriveSyncPoint) -> protocol::ports::DriveAnswer {
        if !point.running {
            return Box::pin(async { None });
        }
        self.running.fetch_add(1, Ordering::SeqCst);
        let release = self.release.clone();
        Box::pin(async move {
            release.notified().await;
            None
        })
    }

    fn drives_variable(&self) -> Option<PathBuf> {
        None
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_while_the_running_report_waits_for_its_sync_point_ends_the_command_at_once() {
    let fixture = Fixture::new(vec![DRIVE], &[], Vec::new(), b"").await;
    let port = Arc::new(HeldRunning::default());
    let ticks = fixture.project.join("ticks");
    let cmd = format!(
        "while true; do echo tick >> '{}'; sleep 0.05; done",
        ticks.display()
    );
    let (mailbox, mut reports) = fixture.start_with(
        "task",
        json!({ "tool": "exec_command", "input": {
            "cmd": cmd, "wait_ms": 5_000, "on_wait_expiry": "detach",
        } }),
        port.clone(),
        Arc::new(NoEvents),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while port.running.load(Ordering::SeqCst) == 0 {
        if let Ok(report) = reports.try_recv() {
            panic!("a report before the running report's sync point: {report}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the running report's sync point"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Held sync point: the cancel is answered and the command ends within
    // the termination's bounds.
    mailbox
        .send(SupervisorMessage::Cancel(Box::new(Control {
            connection_id: id("conn_a"),
            correlation: correlation("task", "cancel"),
            traceparent: None,
            payload: serde_json::from_value(json!({
                "session_id": SESSION, "binding_id": "bind_a",
                "tool_use_event_id": "event", "task_id": "task",
            }))
            .unwrap(),
        })))
        .unwrap();
    let bound = Duration::from_secs(10);
    let reply = tokio::time::timeout(bound, reports.recv())
        .await
        .expect("the cancel's reply while the sync point is held")
        .unwrap();
    assert_eq!(reply["request_message_id"], "cancel", "{reply}");
    assert_eq!(reply["result"]["result"]["cancelled"], true, "{reply}");
    // The termination ends within its bounds; the command then writes no
    // more ticks.
    let size = || std::fs::metadata(&ticks).map_or(0, |metadata| metadata.len());
    let deadline = tokio::time::Instant::now() + bound;
    loop {
        let before = size();
        tokio::time::sleep(Duration::from_millis(500)).await;
        if size() == before {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the command ended while the sync point is held"
        );
    }
    assert!(size() > 0, "the command ran");
    assert!(
        reports.try_recv().is_err(),
        "the running report waits for its sync point"
    );

    // Released: the running report, then the terminal one.
    port.release.notify_one();
    let running = next(&mut reports, "the running report").await;
    assert_eq!(running["phase"], "running", "{running}");
    let terminal = next(&mut reports, "the terminal report").await;
    assert_eq!(terminal["phase"], "terminal", "{terminal}");
    assert_eq!(
        terminal["result"]["result"]["cancelled"], true,
        "{terminal}"
    );
    fixture.sync.detach_all();
}

/// A one-pixel PNG.
const PIXEL: &[u8] = &[
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
    0x42, 0x60, 0x82,
];

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A report outbox for owners whose tests make no drive sync report.
struct NoReports;

impl protocol::ports::ReportSink for NoReports {
    fn submit(&self, _: protocol::ports::Submission) -> protocol::ports::ReportId {
        panic!("no drive sync report is expected")
    }
    fn cancel(&self, _: protocol::ports::ReportId) {}
    fn add_final(&self, _: protocol::ports::ReportId, _: protocol::id::Id) {}
}

struct Gone(mpsc::UnboundedSender<()>);
impl TaskListener for Gone {
    fn started(&self, _: u64) {}
    fn gone(&self) {
        let _ = self.0.send(());
    }
    fn withdrawn(&self) {}
}

struct Kept;
impl protocol::ports::ConnectionPort for Kept {
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
        _: Vec<protocol::connection::LostChange>,
        kept: protocol::ports::KeptReply,
    ) {
        kept();
    }
    fn owner_idle(&self, _: &SafeId, _: u64) {}
}

#[tokio::test]
async fn an_exited_process_releases_close_to_commit_ready_content_while_an_upload_is_held() {
    let mut fixture = Fixture::with_ready_upload(
        vec![DRIVE],
        &[],
        Vec::new(),
        b"",
        Some(sha256_hex(b"ready")),
    )
    .await;
    let (sent_gone, mut gone) = mpsc::unbounded_channel();
    let (mailbox, mut reports) = fixture.start_with("task", json!({
        "tool":"exec_command", "input": {
            "cmd":"printf ready > \"$TETRAL_DRIVES/drive_a/ready\"; printf held > \"$TETRAL_DRIVES/drive_a/held\"",
            "wait_ms":5000, "on_wait_expiry":"detach"
        }
    }), fixture.port.clone(), Arc::new(Gone(sent_gone)));
    tokio::time::timeout(Duration::from_secs(10), gone.recv())
        .await
        .expect("a reaped process releases the close wait before terminal synchronization")
        .unwrap();
    let mut uploads = Vec::new();
    for _ in 0..2 {
        uploads.push(
            tokio::time::timeout(Duration::from_secs(10), fixture.uploaded.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    uploads.sort();
    let mut expected = vec![sha256_hex(b"ready"), sha256_hex(b"held")];
    expected.sort();
    assert_eq!(uploads, expected);
    assert!(
        reports.try_recv().is_err(),
        "the result is still waiting for held content"
    );
    let mut final_upload = fixture.sync.final_upload(drive::FinalUploadRequest {
        session_close: true,
        drives: vec![id(DRIVE)],
        end: Duration::from_secs(10),
        mode: drive::FinalMode::Abandon {
            request_message_id: id("close_a"),
            connection: Arc::new(Kept),
        },
    });
    let (changes, answer) = tokio::time::timeout(Duration::from_secs(10), fixture.commits.recv())
        .await
        .expect("close commits ready content before its existing deadline")
        .unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path(), "ready");
    answer
        .send(Ok(serde_json::from_value(
            json!({"seq":2, "results":[{"status":"accepted", "version_id":"v_ready"}]}),
        )
        .unwrap()))
        .unwrap();
    let terminal = next(&mut reports, "the synchronized terminal result").await;
    assert_eq!(terminal["phase"], "terminal");
    assert_eq!(
        terminal["result"]["drive_sync"]["committed"], 1,
        "{terminal}"
    );
    fixture
        .nanos
        .store(Duration::from_secs(10).as_nanos() as u64, Ordering::SeqCst);
    let lost = tokio::time::timeout(Duration::from_secs(10), final_upload.ended())
        .await
        .unwrap();
    let lost = serde_json::to_value(lost).unwrap();
    assert_eq!(lost[0]["paths"], json!(["held"]));
    assert!(fixture.sync.tables().await.pending_changes.is_empty());
    let _ = mailbox.send(SupervisorMessage::ConnectionEnded);
    fixture.sync.detach_all();
    fixture.sync.close().await;
}

fn visible_remote(bytes: &[u8]) -> Vec<Value> {
    vec![
        json!({ "path": "remote.txt", "version_id": "ver_visible", "content_sha256": sha256_hex(bytes), "size_bytes": bytes.len(), "executable": false }),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn real_drives_keep_authorized_reads_and_binding_and_tool_write_limits() {
    for network in ["disabled", "direct"] {
        for (shape, read_only) in [
            ("cloud", true),
            ("cloud", false),
            ("user", true),
            ("user", false),
            ("reviewer", false),
            ("ancestor", false),
        ] {
            let mut fixture =
                Fixture::new(vec![DRIVE], &[], visible_remote(b"current"), b"current").await;
            if read_only {
                fixture.sync.detach(&id(DRIVE));
                let failures = fixture
                    .sync
                    .mount_drives(vec![Bound {
                        drive_id: SafeId::new(DRIVE).unwrap(),
                        read_only: true,
                        exclude: vec![],
                    }])
                    .await;
                assert!(failures.is_empty(), "{failures:?}");
                fixture.sync.check().await;
                fixture.access = DriveAccess::ReadOnly;
            }
            let sibling_id = "drive_sibling";
            fixture
                .transfer
                .overrides
                .lock()
                .unwrap()
                .insert(sibling_id.into(), (visible_remote(b"sibling"), b"sibling"));
            let sibling_dir = fixture.real_root.join("sessions/sess_sibling");
            state::files::create_private_dirs(&sibling_dir).unwrap();
            let sibling = DriveSync::spawn(SyncConfig {
                commit_batch_size: drive::commit::MAX_CHANGES,
                session_id: SafeId::new("sess_sibling").unwrap(),
                root: fixture.real_root.clone(),
                state: SyncState::create(&sibling_dir.join("sync-state.db"))
                    .await
                    .unwrap(),
                transfer: fixture.transfer.clone(),
                cache: fixture.cache.clone(),
                clock: fixture.clock.clone(),
                tasks: Arc::new(OneTask),
                mounting: Mounting::Fuse,
                registry: Arc::new(NoRegistry),
                reports: Arc::new(NoReports),
            });
            sibling.binding_current(Binding {
                connection_id: id("conn_sibling"),
                binding_id: id("bind_sibling"),
            });
            assert!(
                sibling
                    .mount_drives(vec![Bound {
                        drive_id: SafeId::new(sibling_id).unwrap(),
                        read_only: false,
                        exclude: vec![]
                    }])
                    .await
                    .is_empty()
            );
            sibling.check().await;
            let mount = fixture.drives.join(DRIVE);
            let sibling_mount = sibling_dir.join("drives").join(sibling_id);
            let credential = fixture.real_root.join(state::paths::CREDENTIALS);
            let private_control = fixture.real_root.join("run/private");
            let ordinary = fixture.real_root.join("config.toml");
            std::fs::write(&credential, b"private credential").unwrap();
            std::fs::write(&private_control, b"private control").unwrap();
            std::fs::write(&ordinary, b"ordinary state").unwrap();
            let alias = fixture.project.join("drive-link");
            std::os::unix::fs::symlink(&mount, &alias).unwrap();
            let rules = match shape {
                "cloud" => json!([{ "path": "/", "access": "read_write" }]),
                "user" => {
                    json!([{ "path": "/", "access": "read" }, { "path": fixture.project, "access": "read_write" }])
                }
                "reviewer" => json!([{ "path": "/", "access": "read" }]),
                _ => {
                    json!([{ "path": "/", "access": "read" }, { "path": fixture.canonical, "access": "read_write" }])
                }
            };
            fixture.policy_override = Some(
                json!({ "schema_version": 1, "filesystem": { "rules": rules }, "network": { "mode": network } }),
            );
            let writable = !read_only && shape != "reviewer";
            let python = r#"
import os, pathlib, subprocess, sys
mount, sibling, credential, control, ordinary, alias, writable = sys.argv[1:]
p, other = pathlib.Path(mount), pathlib.Path(sibling)
def denied(operation):
    try: operation()
    except OSError: return
    raise AssertionError('forbidden operation succeeded')
assert 'remote.txt' in os.listdir(p)
assert (p/'remote.txt').read_bytes() == b'current'
assert (other/'remote.txt').read_bytes() == b'sibling'
assert subprocess.check_output(['cat', str(pathlib.Path(alias)/'remote.txt')]) == b'current'
assert pathlib.Path(ordinary).read_bytes() == b'ordinary state'
denied(lambda: pathlib.Path(ordinary).write_bytes(b'changed'))
for path in [credential, control]:
    denied(lambda: pathlib.Path(path).read_bytes())
    denied(lambda: pathlib.Path(path).write_bytes(b'changed'))
denied(lambda: (p.parent/'outside-drive').write_bytes(b'changed'))
denied(lambda: (other/'remote.txt').write_bytes(b'changed'))
denied(lambda: (other/'created').write_bytes(b'changed'))
if writable == 'true':
    (p/'new').write_bytes(b'new')
    (p/'new').rename(p/'renamed')
    assert (p/'renamed').read_bytes() == b'new'
    (p/'renamed').unlink()
else:
    denied(lambda: (p/'new').write_bytes(b'new'))
    denied(lambda: (p/'remote.txt').write_bytes(b'changed'))
    denied(lambda: (p/'remote.txt').rename(p/'renamed'))
    denied(lambda: (p/'remote.txt').unlink())
print('permissions verified')
"#;
            let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
            let command = format!(
                "python3 -c {} {}",
                quote(python),
                [
                    mount.as_path(),
                    sibling_mount.as_path(),
                    credential.as_path(),
                    private_control.as_path(),
                    ordinary.as_path(),
                    alias.as_path()
                ]
                .iter()
                .map(|path| quote(path.to_str().unwrap()))
                .chain([quote(if writable { "true" } else { "false" })])
                .collect::<Vec<_>>()
                .join(" ")
            );
            let (_mailbox, mut reports) = fixture.start(
                "task",
                json!({ "tool": "exec_command", "input": { "cmd": command, "wait_ms": 30_000 } }),
            );
            for (drive, connection, binding) in [
                (DRIVE, "conn_a", "bind_a"),
                (sibling_id, "conn_sibling", "bind_sibling"),
            ] {
                let (actual_drive, actual_binding) =
                    tokio::time::timeout(Duration::from_secs(10), fixture.downloads.recv())
                        .await
                        .expect("the uncached read starts a download")
                        .unwrap();
                assert_eq!(actual_drive.as_str(), drive, "{shape} {network}");
                assert_eq!(
                    actual_binding,
                    Binding {
                        connection_id: id(connection),
                        binding_id: id(binding)
                    }
                );
                fixture.release.notify_one();
            }
            let report = next(&mut reports, "the permission checks").await;
            assert_eq!(
                report["result"]["result"]["exit_code"], 0,
                "{shape} read_only={read_only} {network}: {report}"
            );
            assert_eq!(
                report["result"]["result"]["stdout"]["text"],
                "permissions verified\n"
            );
            assert_eq!(std::fs::read(credential).unwrap(), b"private credential");
            assert_eq!(std::fs::read(ordinary).unwrap(), b"ordinary state");
            assert!(sibling.tables().await.pending_changes.is_empty());
            fixture.sync.detach_all();
            sibling.detach_all();
            fixture.sync.close().await;
            sibling.close().await;
        }
    }
}
