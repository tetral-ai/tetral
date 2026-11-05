//! Real restricted processes on macOS, under `sandbox-exec` with the
//! Seatbelt profile of Codex's sandbox manager and the CLI's connection
//! rules. Each case runs commands through the isolation's availability
//! check and launch, by starting `tetral` as its isolation harness, or as
//! tool calls of a running App Server, which meet its own state root,
//! control socket, task directories and configuration, and observes from
//! outside every sandbox what the commands could reach. The drive server's
//! case mounts a drive through a real NFS server, served by an in-process
//! drive sync whose transfer lists the drive empty.

#![cfg(target_os = "macos")]

use std::fs;
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use drive::mount::{Bound, Registry};
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use drive::{DriveSync, SyncConfig};
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::id::{Id, SafeId};
use protocol::ports::{ReportId, ReportSink, Submission, TaskOwnerReply, TaskOwnership};
use serde_json::json;
use tetral_tests::app_server::wait_until;
use tetral_tests::isolation::{
    self, Fails, SYSTEM_PATH, Served, Succeeds, World, check_probes, harness, quoted, read_write,
    script,
};
use tetral_tests::machine::task_parent;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{TaskMachine, WAIT_CHECKED, policy};

#[test]
fn every_grant_reaches_only_what_its_rules_allow_and_never_the_protected_paths() {
    let mut case = tetral_tests::record!()
        .case("every grant reaches only what its rules allow and never the protected paths");
    isolation::grants_reach_only_what_their_rules_allow(&mut case, &World::new());
    case.pass();
}

#[test]
fn a_missing_deny_path_under_a_directory_of_another_user_stays_uncreatable_and_the_command_runs() {
    let mut case = tetral_tests::record!().case(
        "a missing deny path under a directory of another user stays uncreatable and the command runs",
    );
    if !case.runs_as_non_root("root may create entries in /") {
        return;
    }
    isolation::a_missing_deny_path_stays_uncreatable(&mut case, &World::new());
    case.pass();
}

#[test]
fn a_rule_for_a_regular_file_and_a_deny_path_under_one_leave_the_command_running() {
    let mut case = tetral_tests::record!().case(
        "a read_write rule for a regular file, and a deny path under a regular file, in a writable project",
    );
    isolation::regular_file_rules_leave_the_command_running(&mut case, &World::new());
    case.pass();
}

#[test]
fn the_probes_run_again_as_calls_of_an_app_server_against_its_own_paths() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "the isolation probes as exec_command calls of an App Server started from <p>/bin/tetral, and those that read or write one file as view_image and edit calls, against its own state root, control socket and task directories",
    );
    let served = Served::start(&mut case);
    isolation::grants_reach_only_what_their_rules_allow(&mut case, &served);
    isolation::regular_file_rules_leave_the_command_running(&mut case, &served);
    isolation::file_probes(&mut case, &served);
    case.pass();
}

#[test]
fn a_missing_deny_path_stays_uncreatable_as_an_exec_command_call() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a missing deny path under a directory of another user, as an exec_command call of an App Server",
    );
    if !case.runs_as_non_root("root may create entries in /") {
        return;
    }
    let served = Served::start(&mut case);
    isolation::a_missing_deny_path_stays_uncreatable(&mut case, &served);
    case.pass();
}

/// A Python command that connects to the Unix socket at `path`.
fn connect(path: &str) -> String {
    format!(
        "python3 -c 'import socket, sys; socket.socket(socket.AF_UNIX).connect(sys.argv[1])' {path}"
    )
}

#[test]
fn under_direct_a_command_reaches_unix_sockets_only_in_its_own_temporary_directory() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "under direct: a task listening on a Unix socket in its temporary directory, and another task that connects to it and to the App Server's control socket, and listens on and connects to a socket in its own temporary directory",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let direct = policy(&tasks.project, "direct");

    // The listener names its socket in `listening` once it listens, and
    // runs until `stop` exists.
    let listen = r#"python3 -c 'import os, socket, time
path = os.path.join(os.environ["TMPDIR"], "s")
listener = socket.socket(socket.AF_UNIX)
listener.bind(path)
listener.listen(8)
open("listening.part", "w").write(path)
os.rename("listening.part", "listening")
while not os.path.exists("stop"):
    time.sleep(0.05)'"#;
    let checked = tasks.machine().seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_listener",
        json!({"cmd": listen, "wait_ms": 250, "on_wait_expiry": "detach"}),
        direct.clone(),
    );
    tasks.wait_for_start(checked);
    tasks.machine().seams.advance(Duration::from_millis(250));
    tasks.report("task_listener", "running");
    let listening = tasks.project.join("listening");
    wait_until("the listener's socket", || listening.exists());
    let other = PathBuf::from(fs::read_to_string(&listening).unwrap());
    assert!(other.starts_with(task_parent(&tasks.root)), "{other:?}");
    UnixStream::connect(&other).expect("the listener answers outside every sandbox");

    let control = tasks.machine().app_server.socket.canonicalize().unwrap();
    let probes = [
        (
            "connect to the control socket S/<h>.sock",
            connect(&quoted(&control)),
            Fails,
        ),
        (
            "connect to the socket another task listens on in its temporary directory",
            connect(&quoted(&other)),
            Fails,
        ),
        (
            "listen on and connect to $TMPDIR/s",
            r#"python3 -c 'import os, socket
path = os.path.join(os.environ["TMPDIR"], "s")
listener = socket.socket(socket.AF_UNIX)
listener.bind(path)
listener.listen(1)
socket.socket(socket.AF_UNIX).connect(path)'"#
                .to_owned(),
            Succeeds,
        ),
    ];
    tasks.execute(
        "task_connect",
        json!({"cmd": script(&probes)}),
        direct.clone(),
    );
    let result = &tasks.terminal("task_connect")["result"]["result"];
    let outcome = json!({
        "exit_code": result["exit_code"],
        "stdout": result["stdout"]["text"],
        "stderr": result["stderr"]["text"],
    });
    check_probes(&mut case, &direct, &outcome, &probes);
    fs::write(tasks.project.join("stop"), "").unwrap();
    let listener = tasks.terminal("task_listener");
    assert_eq!(listener["result"]["result"]["exit_code"], 0, "{listener:#}");
    case.pass();
}

/// A drive transfer that lists every drive empty and never answers an
/// upload, a commit or a download.
struct EmptyDrives;

impl DriveTransfer for EmptyDrives {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let page = json!({"files": [], "has_more": false, "start_seq": 1});
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn list_changes(&self, _: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let page = json!({"records": [], "has_more": false});
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn upload_content(&self, _: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
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

struct NoTask;

impl TaskOwnership for NoTask {
    fn task_of(&self, _: u32, reply: TaskOwnerReply) {
        reply(None);
    }
}

struct NoReports;

impl ReportSink for NoReports {
    fn submit(&self, _: Submission) -> ReportId {
        ReportId(0)
    }
    fn cancel(&self, _: ReportId) {}
    fn add_final(&self, _: ReportId, _: Id) {}
}

/// Force-unmounts what the test left in the Session's drive directory.
struct Unmount(PathBuf);

impl Drop for Unmount {
    fn drop(&mut self) {
        let _ = drive::mount::detach_remaining(&self.0);
    }
}

#[test]
fn with_drives_enabled_under_direct_a_command_uses_its_own_drive_but_not_the_drive_server() {
    let mut case = tetral_tests::record!().case(
        "with drives enabled, under direct: a TCP connect to 127.0.0.1:<P> of the NFS server serving the Session's drive, one to another listener on 127.0.0.1, and a write and read of a file on the Session's own mounted drive",
    );
    let world = World::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mounting = drive::mount_macos::probe(true);
    let port = mounting.nfs_port().expect("the NFS server listens");
    let session = world.root.join("sessions/S3");
    let _unmount = Unmount(session.clone());
    let sync = runtime.block_on(async {
        state::files::create_private_dirs(&session).unwrap();
        let start = std::time::Instant::now();
        let clock = state::clock::Clock::new(move || start.elapsed(), Duration::from_millis(2));
        let transfer = Arc::new(EmptyDrives);
        let cache = drive::cache::ContentCache::spawn(drive::cache::CacheConfig {
            root: world.root.clone(),
            transfer: transfer.clone(),
            clock: clock.clone(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let sync = DriveSync::spawn(SyncConfig {
            commit_batch_size: drive::commit::MAX_CHANGES,
            session_id: SafeId::new("S3").unwrap(),
            root: world.root.clone(),
            state: drive::state::SyncState::create(&session.join("sync-state.db"))
                .await
                .unwrap(),
            transfer,
            cache,
            tasks: Arc::new(NoTask),
            mounting,
            registry: Arc::new(NoRegistry),
            clock,
            reports: Arc::new(NoReports),
        });
        sync.binding_current(Binding {
            connection_id: Id::new("conn_a").unwrap(),
            binding_id: Id::new("bind_a").unwrap(),
        });
        let bound = vec![Bound {
            drive_id: SafeId::new("d3").unwrap(),
            read_only: false,
            exclude: Vec::new(),
        }];
        let failures = sync.mount_drives(bound).await;
        assert!(failures.is_empty(), "{failures:?}");
        tokio::time::timeout(Duration::from_secs(10), sync.check())
            .await
            .expect("the drive caught up");
        sync
    });
    let drives = session.join("drives");
    let mount = drives.join("d3");
    assert!(
        drive::mount_macos::listed(&mount).unwrap().is_some(),
        "the drive is mounted at {mount:?}"
    );
    let other = TcpListener::bind("127.0.0.1:0").unwrap();
    let connect = |port: u16| {
        format!("python3 -c 'import socket; socket.create_connection((\"127.0.0.1\", {port}), 5)'")
    };
    let file = quoted(&mount.join("written"));
    let probes = [
        (
            "connect to 127.0.0.1:<P>, the NFS server",
            connect(port),
            Fails,
        ),
        (
            "connect to another listener on 127.0.0.1",
            connect(other.local_addr().unwrap().port()),
            Succeeds,
        ),
        (
            "write and read back a file on the Session's own mounted drive",
            format!("printf drive > {file} && test \"$(cat {file})\" = drive"),
            Succeeds,
        ),
    ];
    let direct = read_write(&world.project);
    let mut request = world.request(
        &direct,
        &world.project,
        &["/bin/bash", "-c", &script(&probes)],
        SYSTEM_PATH,
    );
    request["drives"] = json!({
        "directory": drives,
        "mounts": [{"path": mount, "access": "read_write"}],
    });
    request["drive_server_port"] = json!(port);
    let outcome = harness(&world.tetral, &request, SYSTEM_PATH, None);
    check_probes(&mut case, &direct, &outcome, &probes);
    runtime.block_on(sync.close());
    case.pass();
}

#[test]
fn disabled_blocks_tcp_and_udp_and_direct_reaches_them_and_local_services() {
    let mut case = tetral_tests::record!()
        .case("disabled blocks TCP and UDP, and direct reaches them and local services");
    isolation::network_reach(&mut case);
    case.pass();
}

#[test]
fn a_disabled_task_leaves_the_connection_its_heartbeats_and_a_direct_tasks_connect_working() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a disabled task running across 20 seconds of the clock while a direct task connects to a TCP listener on the host's LAN address",
    );
    isolation::disabled_task_beside_direct(&mut case);
    case.pass();
}

#[test]
fn a_sandbox_exec_that_cannot_apply_a_profile_rejects_the_call_before_any_process_starts() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a tool.execute on an App Server whose availability check runs a sandbox-exec that exits 71",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start_without_isolation(&mut case, directory.path());
    let marker = tasks.project.join("marker");
    let spawned_before = tasks.machine().seams.count("process.spawn");
    tasks.execute(
        "task_unusable",
        json!({"cmd": format!("echo > {}", quoted(&marker))}),
        policy(&tasks.project, "direct"),
    );
    let report = tasks.report("task_unusable", "rejected");
    let spawned = tasks.machine().seams.count("process.spawn") - spawned_before;
    case.observe(format!(
        "rejected {}; process.spawn {spawned}; marker {}",
        report["error"],
        if marker.exists() { "written" } else { "absent" }
    ));
    assert_eq!(report["error"]["code"], "sandbox_unavailable", "{report}");
    assert_eq!(
        report["error"]["message"],
        "sandbox-exec cannot apply a sandbox profile on this machine"
    );
    assert_eq!((spawned, marker.exists()), (0, false));
    case.pass();
}
