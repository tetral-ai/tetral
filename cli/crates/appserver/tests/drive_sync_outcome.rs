//! The drive path a restricted command reaches: a Task supervisor wired to
//! a Session's drive sync through `DriveSyncPort`, with a real FUSE mount,
//! the real restricted process of a `test-seams` `tetral`, a stub
//! `DriveTransfer` that holds every upload, and a clock the test moves.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use drive::cache::{CacheConfig, ContentCache};
use drive::mount::{Bound, Mounting, Registry};
use drive::state::SyncState;
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use drive::{DriveSync, SyncConfig};
use execution::isolation::{self, Isolation};
use execution::limits::Visible;
use execution::policy::{DriveMount, SessionDrives, TaskPermissions, TranslationInputs};
use execution::report::Correlation;
use execution::store::{LOG_HEAD_BYTES, TaskFiles};
use execution::supervisor::{self, Bounds, Context, SupervisorMessage, TaskListener, TaskSpec};
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

/// Lists an empty drive and holds every upload and commit; each upload's
/// hash is told to the test.
struct Stub(mpsc::UnboundedSender<String>);

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
        let _ = self.0.send(request.body.sha256.as_str().to_owned());
        Box::pin(std::future::pending())
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

#[tokio::test(flavor = "multi_thread")]
async fn a_restricted_command_reaches_the_drive_through_tetral_drives_under_a_linked_root() {
    let tetral = tetral_tests::binaries::tetral().canonicalize().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let canonical = directory.path().canonicalize().unwrap();
    let _unmount = Unmount(canonical.clone());
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
    let (uploads, mut uploaded) = mpsc::unbounded_channel();
    let transfer: Arc<dyn DriveTransfer> = Arc::new(Stub(uploads));
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
        .mount_drives(vec![Bound {
            drive_id: SafeId::new(DRIVE).unwrap(),
            read_only: false,
            exclude: Vec::new(),
        }])
        .await;
    assert!(failures.is_empty(), "{failures:?}");
    let port: Arc<dyn protocol::ports::DriveSyncPort> = Arc::new(sync.clone());
    let drives = port
        .drives_variable()
        .expect("a Session that binds a drive");
    let expected = real_root.join("sessions").join(SESSION).join("drives");
    assert_eq!(drives, expected, "the canonical root, not the link");

    let policy = json!({
        "schema_version": 1,
        "filesystem": { "rules": [{ "path": project, "access": "read_write" }] },
        "network": { "mode": "disabled" },
    });
    let temporary = canonical.join("temporary");
    let inputs = TranslationInputs {
        state_root: real_root.clone(),
        socket_directory: canonical.join("socket"),
        task_directories: temporary.clone(),
        task_temporary_directory: temporary.join("task"),
        drives: Some(SessionDrives {
            directory: drives.clone(),
            mounts: vec![DriveMount {
                path: drives.join(DRIVE),
                access: DriveAccess::ReadWrite,
            }],
        }),
        trusted_programs: isolation::trusted_programs(&tetral, None, &project),
    };
    let (sent, mut reports) = mpsc::unbounded_channel();
    let spec = TaskSpec {
        connection_id: id("conn_a"),
        correlation: Correlation {
            session_id: id(SESSION),
            binding_id: id("bind_a"),
            tool_use_event_id: id("event"),
            task_id: id("task"),
            request_message_id: id("execute"),
        },
        permissions: TaskPermissions::admit(Some(&policy), &inputs).unwrap(),
        limits: Visible::requested(None),
        cwd: project.clone(),
        files: TaskFiles::new(
            real_root.join("task"),
            temporary.join("task"),
            LOG_HEAD_BYTES,
        ),
        traceparent: None,
        received: state::seams::now(),
        record: real_root.join("record"),
        listener: Arc::new(NoEvents),
        drive_sync: Some(port),
    };
    let cmd = format!(
        "printf %s \"$TETRAL_DRIVES\" && ls \"$TETRAL_DRIVES\"/{DRIVE} && echo written > \"$TETRAL_DRIVES\"/{DRIVE}/f"
    );
    let tool = serde_json::from_value(json!({
        "tool": "exec_command",
        "input": { "cmd": cmd, "wait_ms": 30_000 },
    }))
    .unwrap();
    let mailbox = supervisor::spawn(
        spec,
        tool,
        Context {
            tetral,
            isolation: Isolation::new(),
            reports: Arc::new(Reports(sent, AtomicU64::new(1))),
            bounds: Bounds::default(),
            boot_id: None,
        },
    );
    mailbox.send(SupervisorMessage::Start).unwrap();

    // The terminal result's sync point covers `f`; its upload is held past
    // the sync point's wait.
    let hash = tokio::time::timeout(Duration::from_secs(60), uploaded.recv())
        .await
        .expect("f's upload within 60 s")
        .unwrap();
    assert_eq!(hash, sha256_hex(b"written\n"));
    nanos.fetch_add(supervisor::SYNC_WAIT.as_nanos() as u64, Ordering::SeqCst);
    let report = tokio::time::timeout(Duration::from_secs(30), reports.recv())
        .await
        .expect("the terminal report")
        .unwrap();
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
    sync.detach_all();
}

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
