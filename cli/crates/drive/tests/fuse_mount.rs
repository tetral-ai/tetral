//! The mount check against real FUSE mounts under a temporary state root:
//! a mount of this App Server is left alone, a mount whose server is
//! stopped is force-unmounted without the check hanging, mount points and
//! their failures, the new-failure rule across checks and restarts, failed
//! listings at checks, a state root reached through a symbolic link,
//! `TETRAL_DRIVES`, I/O under a detached mount, and a record replacing a
//! file that a read through the mount is waiting on.
#![cfg(target_os = "linux")]

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drive::cache::{CacheConfig, ContentCache};
use drive::mount::{Bound, Failure, Mounting, Registry, drives_variable};
use drive::mountinfo::current_device;
use drive::state::SyncState;
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use drive::{DriveSync, SyncConfig};
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::id::{Id, SafeId};
use protocol::ports::{DriveSyncPort, TaskOwnerReply, TaskOwnership};
use serde_json::json;
use state::clock::Clock;

const SESSION: &str = "sess_a";
const HELPER: &str = "TETRAL_TEST_FUSE_HELPER";

/// How the stub answers a list request.
#[derive(Debug, Clone, Copy)]
enum Answer {
    Listed,
    Denied,
    Invalid,
}

/// A drive of one file whose list answers the test scripts; unscripted
/// requests are answered normally.
#[derive(Default)]
struct Stub {
    files: Mutex<VecDeque<Answer>>,
    changes: Mutex<VecDeque<Answer>>,
    /// The next list of changes waits for the notice, then answers these
    /// records.
    held_changes: Mutex<Option<(Arc<tokio::sync::Notify>, serde_json::Value)>>,
    /// The bytes each download answers.
    content: Vec<u8>,
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn ended(answer: Answer) -> Option<Ended> {
    match answer {
        Answer::Listed => None,
        Answer::Denied => Some(Ended::Permanent {
            status: 403,
            code: Some("drive_access_denied"),
        }),
        Answer::Invalid => Some(Ended::Permanent {
            status: 200,
            code: None,
        }),
    }
}

impl DriveTransfer for Stub {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let answer = self
            .files
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Answer::Listed);
        Box::pin(async move {
            if let Some(ended) = ended(answer) {
                return Err(ended);
            }
            Ok(serde_json::from_value(json!({
                "files": [{
                    "path": "a.txt", "version_id": "v_a", "content_sha256": "a".repeat(64),
                    "size_bytes": 1, "executable": false,
                }],
                "has_more": false,
                "start_seq": 1,
            }))
            .unwrap())
        })
    }

    fn list_changes(&self, _: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let answer = self
            .changes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Answer::Listed);
        let held = self.held_changes.lock().unwrap().take();
        Box::pin(async move {
            if let Some(ended) = ended(answer) {
                return Err(ended);
            }
            let records = match held {
                Some((notice, records)) => {
                    notice.notified().await;
                    records
                }
                None => json!([]),
            };
            Ok(serde_json::from_value(json!({ "records": records, "has_more": false })).unwrap())
        })
    }

    fn upload_content(&self, _: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        unimplemented!()
    }

    fn commit(&self, _: Request<Commit>) -> Transfer<'_, CommitResponse> {
        unimplemented!()
    }

    fn download_content<'a>(
        &'a self,
        _: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        Box::pin(async move {
            sink.append(&self.content).map_err(Ended::Sink)?;
            Ok(())
        })
    }
}

struct NoTasks;

impl TaskOwnership for NoTasks {
    fn task_of(&self, _: u32, reply: TaskOwnerReply) {
        reply(None);
    }
}

/// The registry calls: `+drive` for a mount, `-drive` for a detach.
#[derive(Default)]
struct Calls(Mutex<Vec<String>>);

impl Registry for Calls {
    fn add(&self, _: SafeId, _: DriveSync) {}
    fn remove(&self, _: SafeId) {}
    fn mounted(&self, drive_id: Id, _: SafeId) {
        self.0
            .lock()
            .unwrap()
            .push(format!("+{}", drive_id.as_str()));
    }
    fn detached(&self, drive_id: Id, _: SafeId) {
        self.0
            .lock()
            .unwrap()
            .push(format!("-{}", drive_id.as_str()));
    }
}

struct Harness {
    _directory: tempfile::TempDir,
    /// The state root as given; a symbolic link in one test.
    root: PathBuf,
    stub: Arc<Stub>,
    cache: ContentCache,
    calls: Arc<Calls>,
    sync: DriveSync,
}

impl Harness {
    async fn start() -> Harness {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        std::fs::create_dir(&root).unwrap();
        Harness::start_at(directory, root).await
    }

    async fn start_at(directory: tempfile::TempDir, root: PathBuf) -> Harness {
        Harness::start_with(directory, root, Stub::default()).await
    }

    async fn start_with(directory: tempfile::TempDir, root: PathBuf, stub: Stub) -> Harness {
        let stub = Arc::new(stub);
        let transfer: Arc<dyn DriveTransfer> = stub.clone();
        let cache = ContentCache::spawn(CacheConfig {
            root: root.clone(),
            transfer,
            clock: Clock::seams(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let session = root.join("sessions").join(SESSION);
        std::fs::create_dir_all(&session).unwrap();
        let state = SyncState::create(&session.join("sync-state.db"))
            .await
            .unwrap();
        let calls = Arc::new(Calls::default());
        let sync = spawn(&root, &stub, &cache, &calls, state);
        Harness {
            _directory: directory,
            root,
            stub,
            cache,
            calls,
            sync,
        }
    }

    /// A new drive sync over the same sync state, as after an App Server
    /// restart; the old one's mounts are detached first.
    async fn restart(&mut self, drives: &[&str]) {
        for drive in drives {
            self.sync.detach(&Id::new(*drive).unwrap());
        }
        self.sync.tables().await;
        let state = SyncState::open(&self.session().join("sync-state.db"))
            .await
            .unwrap();
        self.sync = spawn(&self.root, &self.stub, &self.cache, &self.calls, state);
    }

    fn session(&self) -> PathBuf {
        self.root.join("sessions").join(SESSION)
    }

    /// `P`, under the canonical root.
    fn point(&self, drive: &str) -> PathBuf {
        self.root
            .canonicalize()
            .unwrap()
            .join("sessions")
            .join(SESSION)
            .join("drives")
            .join(drive)
    }

    fn device(&self, drive: &str) -> Option<String> {
        current_device(&self.point(drive)).unwrap()
    }

    fn calls(&self) -> Vec<String> {
        self.calls.0.lock().unwrap().clone()
    }

    /// A preparation's mounts; their new failures, then reported.
    async fn prepare(&self, drives: &[&str]) -> Vec<(String, String)> {
        let failures = self.sync.mount_drives(bound(drives)).await;
        self.sync.reported(failures.clone());
        texts(failures)
    }

    /// The check before a command; its new failures, then reported.
    async fn check(&self, drives: &[&str]) -> Vec<(String, String)> {
        let failures = tokio::time::timeout(
            Duration::from_secs(20),
            self.sync.check_drives(bound(drives)),
        )
        .await
        .expect("the check returns");
        self.sync.reported(failures.clone());
        texts(failures)
    }

    async fn detach_all(&self, drives: &[&str]) {
        for drive in drives {
            self.sync.detach(&Id::new(*drive).unwrap());
        }
        self.sync.tables().await;
    }
}

impl Drop for Harness {
    /// Lazily unmounts what a failed test left mounted under the root.
    fn drop(&mut self) {
        let Ok(root) = self.root.canonicalize() else {
            return;
        };
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for point in mountinfo.lines().filter_map(|line| line.split(' ').nth(4)) {
            if Path::new(point).starts_with(&root) {
                let _ = Command::new("fusermount3")
                    .args(["-u", "-z", point])
                    .status();
            }
        }
    }
}

fn spawn(
    root: &Path,
    stub: &Arc<Stub>,
    cache: &ContentCache,
    calls: &Arc<Calls>,
    state: SyncState,
) -> DriveSync {
    let sync = DriveSync::spawn(SyncConfig {
        session_id: SafeId::new(SESSION).unwrap(),
        root: root.to_path_buf(),
        state,
        transfer: stub.clone(),
        cache: cache.clone(),
        tasks: Arc::new(NoTasks),
        mounting: Mounting::Fuse,
        registry: calls.clone(),
        clock: state::clock::Clock::seams(),
        reports: std::sync::Arc::new(NoReports),
    });
    sync.binding_current(Binding {
        connection_id: Id::new("conn_a").unwrap(),
        binding_id: Id::new("bind_a").unwrap(),
    });
    sync
}

fn bound(drives: &[&str]) -> Vec<Bound> {
    drives
        .iter()
        .map(|drive| Bound {
            drive_id: SafeId::new(*drive).unwrap(),
            read_only: false,
            exclude: Vec::new(),
        })
        .collect()
}

fn texts(failures: Vec<Failure>) -> Vec<(String, String)> {
    failures
        .into_iter()
        .map(|failure| (failure.drive_id, failure.error))
        .collect()
}

fn failure(drive: &str, error: &str) -> (String, String) {
    (drive.to_owned(), error.to_owned())
}

const NOT_A_DIRECTORY: &str = "mount point: Not a directory (os error 20)";

/// Runs `work` on a blocking thread, since FUSE requests from this process
/// are served by tasks of the test's runtime.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_of_this_app_server_is_left_alone_and_mount_points_are_made_or_mounted_over() {
    let harness = Harness::start().await;
    std::fs::create_dir_all(harness.point("drive_old")).unwrap();
    std::fs::write(harness.point("drive_old").join("old.txt"), "old").unwrap();
    std::fs::write(harness.point("drive_file"), "not a folder").unwrap();

    let failures = harness
        .prepare(&["drive_new", "drive_old", "drive_file"])
        .await;
    assert_eq!(failures, [failure("drive_file", NOT_A_DIRECTORY)]);
    // Set while one of the bound drives failed.
    assert_eq!(
        DriveSyncPort::drives_variable(&harness.sync),
        harness.point("drive_new").parent().map(Path::to_path_buf)
    );
    let device = harness.device("drive_new").expect("drive_new is mounted");
    assert!(harness.device("drive_old").is_some());
    harness.check(&["drive_new", "drive_old"]).await;
    let listed = {
        let point = harness.point("drive_old");
        blocking(move || {
            std::fs::read_dir(point)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect::<Vec<_>>()
        })
        .await
    };
    assert_eq!(listed, ["a.txt"], "old.txt stays hidden under the mount");

    // A hard link fails with EPERM and stores nothing.
    let before = harness.sync.tables().await;
    let linked = {
        let point = harness.point("drive_old");
        blocking(move || std::fs::hard_link(point.join("a.txt"), point.join("b.txt"))).await
    };
    assert_eq!(
        linked.unwrap_err().raw_os_error(),
        Some(rustix::io::Errno::PERM.raw_os_error())
    );
    assert_eq!(harness.sync.tables().await, before);
    let content = harness.session().join("content").join("drive_old");
    assert_eq!(std::fs::read_dir(content).map_or(0, Iterator::count), 0);

    let calls = harness.calls();
    harness.check(&["drive_new", "drive_old"]).await;
    assert_eq!(harness.device("drive_new"), Some(device));
    assert_eq!(harness.calls(), calls, "no new mount");
    harness.detach_all(&["drive_new", "drive_old"]).await;
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(
        mode(&harness.point("drive_new")),
        0o700,
        "the made mount point"
    );
}

/// Mounts an empty filesystem at the path in `HELPER` and waits; run as
/// the helper process of the test below.
#[test]
fn helper_server() {
    let Some(point) = std::env::var_os(HELPER) else {
        return;
    };
    struct Empty;
    impl fuser::Filesystem for Empty {}
    let session = fuser::Session::new(Empty, &point, &fuser::Config::default()).unwrap();
    println!("mounted");
    std::io::stdout().flush().unwrap();
    session.run().unwrap();
}

/// Kills the helper even when the test fails.
struct Helper(Child);

impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_whose_server_is_stopped_is_force_unmounted_and_the_drive_mounted() {
    let harness = Harness::start().await;
    let point = harness.point("drive_a");
    std::fs::create_dir_all(&point).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "helper_server",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HELPER, &point)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let helper = Helper(child);
    assert!(lines.any(|line| line.unwrap().contains("mounted")));
    assert!(harness.device("drive_a").is_some(), "the helper's mount");
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(helper.0.id() as i32).unwrap(),
        rustix::process::Signal::STOP,
    )
    .unwrap();

    // A check that inspected the mount point first would never return.
    assert!(harness.check(&["drive_a"]).await.is_empty());
    assert!(harness.device("drive_a").is_some(), "the drive is mounted");
    // The device number of the released helper mount may be given again,
    // so the listing tells the mounts apart.
    let listed = blocking(move || {
        std::fs::read_dir(point)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>()
    })
    .await;
    assert_eq!(listed, ["a.txt"]);
    assert_eq!(harness.calls(), ["+drive_a"]);
    harness.detach_all(&["drive_a"]).await;
    drop(helper);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lasting_failure_is_reported_once_per_session_until_its_text_changes_or_a_mount_succeeds()
{
    let mut harness = Harness::start().await;
    let drives = harness.session().join("drives");
    std::fs::create_dir_all(&drives).unwrap();
    std::fs::write(harness.point("drive_a"), "").unwrap();

    let denied = || {
        format!(
            "mount point: {}",
            std::io::Error::from_raw_os_error(rustix::io::Errno::ACCESS.raw_os_error())
        )
    };
    let rows: Vec<(&str, Vec<(String, String)>)> = vec![
        ("first", harness.check(&["drive_a"]).await),
        ("again", harness.check(&["drive_a"]).await),
        ("after a restart", {
            harness.restart(&[]).await;
            harness.check(&["drive_a"]).await
        }),
        ("another error", {
            std::fs::remove_file(harness.point("drive_a")).unwrap();
            std::fs::set_permissions(&drives, std::fs::Permissions::from_mode(0o500)).unwrap();
            harness.check(&["drive_a"]).await
        }),
        ("a mount with a complete catch-up", {
            std::fs::set_permissions(&drives, std::fs::Permissions::from_mode(0o700)).unwrap();
            harness.check(&["drive_a"]).await
        }),
        ("the first error again", {
            harness.detach_all(&["drive_a"]).await;
            blocking({
                let point = harness.point("drive_a");
                move || {
                    std::fs::remove_dir(&point).unwrap();
                    std::fs::write(&point, "").unwrap();
                }
            })
            .await;
            harness.check(&["drive_a"]).await
        }),
    ];
    assert_eq!(
        rows,
        [
            ("first", vec![failure("drive_a", NOT_A_DIRECTORY)]),
            ("again", vec![]),
            ("after a restart", vec![]),
            ("another error", vec![failure("drive_a", &denied())]),
            ("a mount with a complete catch-up", vec![]),
            (
                "the first error again",
                vec![failure("drive_a", NOT_A_DIRECTORY)]
            ),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_listings_are_reported_at_the_checks_once_per_distinct_error() {
    let harness = Harness::start().await;
    let denied = "list files: 403 drive_access_denied";
    harness
        .stub
        .files
        .lock()
        .unwrap()
        .extend([Answer::Denied; 3]);
    let three: Vec<Vec<(String, String)>> = vec![
        harness.check(&["drive_a"]).await,
        harness.check(&["drive_a"]).await,
        harness.check(&["drive_a"]).await,
    ];
    assert_eq!(three, [vec![failure("drive_a", denied)], vec![], vec![]]);

    // A failed listing after a preparation's mount is held, and reported
    // with the next check's own failed listing.
    let harness = Harness::start().await;
    harness.stub.files.lock().unwrap().push_back(Answer::Denied);
    assert!(harness.prepare(&["drive_a"]).await.is_empty());
    for _ in 0..500 {
        if !harness.sync.unmounted().await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    harness
        .stub
        .changes
        .lock()
        .unwrap()
        .push_back(Answer::Invalid);
    assert_eq!(
        harness.check(&["drive_a"]).await,
        [
            failure("drive_a", denied),
            failure("drive_a", "list changes: invalid answer"),
        ]
    );
    assert!(harness.check(&["drive_a"]).await.is_empty());
    assert!(harness.sync.tables().await.mount_errors.is_empty());
    harness.detach_all(&["drive_a"]).await;

    // A check dropped while it waits for a catch-up leaves later failed
    // listings to the next check.
    let harness = Harness::start().await;
    assert!(harness.check(&["drive_a"]).await.is_empty());
    let notice = Arc::new(tokio::sync::Notify::new());
    *harness.stub.held_changes.lock().unwrap() = Some((notice.clone(), json!([])));
    harness.sync.connected();
    let sync = harness.sync.clone();
    let dropped = tokio::spawn(async move { sync.check_drives(bound(&["drive_a"])).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    dropped.abort();
    let _ = dropped.await;
    notice.notify_one();
    harness.sync.tables().await;
    harness
        .stub
        .changes
        .lock()
        .unwrap()
        .push_back(Answer::Invalid);
    harness.sync.connected();
    for _ in 0..500 {
        if harness.stub.changes.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        harness.check(&["drive_a"]).await,
        [failure("drive_a", "list changes: invalid answer")]
    );
    harness.detach_all(&["drive_a"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_state_root_through_a_symbolic_link_finds_its_own_mount() {
    let directory = tempfile::tempdir().unwrap();
    let real = directory.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let link = directory.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let harness = Harness::start_at(directory, link).await;
    assert!(harness.check(&["drive_a"]).await.is_empty());
    harness.check(&["drive_a"]).await;
    assert_eq!(harness.calls(), ["+drive_a"]);
    harness.detach_all(&["drive_a"]).await;
}

#[test]
fn tetral_drives_is_unset_without_a_bound_drive_or_on_a_machine_that_cannot_mount() {
    let session = Path::new("/r/sessions/sess_a");
    let unavailable = Mounting::Unavailable("no FUSE: test".to_owned());
    assert_eq!(drives_variable(session, false, &Mounting::Fuse), None);
    assert_eq!(drives_variable(session, true, &unavailable), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn io_under_a_detached_mount_fails_with_eio_and_records_nothing() {
    let harness = Harness::start().await;
    assert!(harness.check(&["drive_a"]).await.is_empty());
    let mut child = Command::new("sh")
        .args(["-c", "read line; echo data > new.txt; echo status=$?"])
        .current_dir(harness.point("drive_a"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    harness.sync.detach(&Id::new("drive_a").unwrap());
    // The drive is mounted again; the old mount's requests still fail.
    assert!(harness.check(&["drive_a"]).await.is_empty());
    let output = blocking(move || {
        child.stdin.take().unwrap().write_all(b"go\n").unwrap();
        child.wait_with_output().unwrap()
    })
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Input/output error"), "{stderr}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.starts_with("status=") && stdout.trim() != "status=0",
        "{stdout}"
    );
    assert!(harness.sync.tables().await.pending_changes.is_empty());
    assert_eq!(harness.calls(), ["+drive_a", "-drive_a", "+drive_a"]);
    harness.detach_all(&["drive_a"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_replacing_a_file_whose_read_waits_for_the_catch_up_answers_the_read() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let content = b"new".to_vec();
    let replaced = sha256(&content);
    let harness = Harness::start_with(
        directory,
        root,
        Stub {
            content,
            ..Stub::default()
        },
    )
    .await;
    assert!(harness.check(&["drive_a"]).await.is_empty());
    let notice = Arc::new(tokio::sync::Notify::new());
    *harness.stub.held_changes.lock().unwrap() = Some((
        notice.clone(),
        json!([{
            "seq": 2, "index": 0, "path": "a.txt", "operation": "modified",
            "version_id": "v_a2", "content_sha256": replaced,
            "size_bytes": 3, "executable": false,
        }]),
    ));
    harness.sync.connected();
    // The read of `a.txt`, whose blob is not cached, waits for the catch-up
    // with the kernel's page of the file locked.
    let file = harness.point("drive_a").join("a.txt");
    let read = tokio::task::spawn_blocking(move || std::fs::read(file));
    tokio::time::sleep(Duration::from_millis(500)).await;
    notice.notify_one();
    let read = tokio::time::timeout(Duration::from_secs(20), read)
        .await
        .expect("the read is answered")
        .unwrap();
    assert_eq!(read.unwrap(), b"new");
    let point = harness.point("drive_a");
    let size = tokio::time::timeout(
        Duration::from_secs(20),
        blocking(move || std::fs::metadata(point.join("a.txt")).map(|meta| meta.len())),
    )
    .await
    .expect("the mount answers");
    assert_eq!(size.unwrap(), 3);
    harness.detach_all(&["drive_a"]).await;
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
