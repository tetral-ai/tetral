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
struct Stub {
    files: Vec<Value>,
    denied: Arc<std::sync::Mutex<Vec<String>>>,
    uploads: mpsc::UnboundedSender<String>,
    downloads: mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Notify>,
    content: &'static [u8],
}

impl Stub {
    fn refused(&self, drive_id: &Id) -> bool {
        let mut denied = self.denied.lock().unwrap();
        let before = denied.len();
        denied.retain(|drive| drive != drive_id.as_str());
        denied.len() < before
    }
}

const DENIED: Ended = Ended::Permanent {
    status: 403,
    code: Some("drive_access_denied"),
};

impl DriveTransfer for Stub {
    fn list_files(&self, request: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let refused = self.refused(&request.drive_id);
        let page = json!({ "files": self.files, "has_more": false, "start_seq": 1 });
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
        Box::pin(std::future::pending())
    }

    fn commit(&self, _: Request<Commit>) -> Transfer<'_, CommitResponse> {
        Box::pin(std::future::pending())
    }

    fn download_content<'a>(
        &'a self,
        _: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        let _ = self.downloads.send(());
        Box::pin(async move {
            self.release.notified().await;
            sink.append(self.content).map_err(Ended::Sink)
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
    uploaded: mpsc::UnboundedReceiver<String>,
    downloads: mpsc::UnboundedReceiver<()>,
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
        let tetral = tetral_tests::binaries::tetral().canonicalize().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let canonical = directory.path().canonicalize().unwrap();
        let unmount = Unmount(canonical.clone());
        let real_root = canonical.join("state");
        std::fs::create_dir(&real_root).unwrap();
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
        let (downloaded, downloads) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let denied = Arc::new(std::sync::Mutex::new(
            failing.iter().map(|drive| (*drive).to_owned()).collect(),
        ));
        let transfer: Arc<dyn DriveTransfer> = Arc::new(Stub {
            files,
            denied: denied.clone(),
            uploads,
            downloads: downloaded,
            release: release.clone(),
            content,
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
            session_id: SafeId::new(SESSION).unwrap(),
            root: linked_root.clone(),
            state: sync_state,
            transfer,
            cache,
            tasks: Arc::new(OneTask),
            mounting: Mounting::Fuse,
            registry: Arc::new(NoRegistry),
            clock,
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
            uploaded,
            downloads,
            release,
            denied,
            _unmount: unmount,
            _directory: directory,
        }
    }

    fn policy(&self) -> Value {
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
                        access: DriveAccess::ReadWrite,
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
            listener: Arc::new(NoEvents),
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
                drive_sync: Some(self.port.clone()),
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
        json!({ "tool": "exec_command", "input": { "cmd": cmd, "wait_ms": 1_000, "on_wait_expiry": "detach" } }),
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
