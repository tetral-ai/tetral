//! `tetral server run`, `server status` and `server stop` as separate
//! processes, and the local API as the SDK and raw HTTP reach it: status
//! while Engine is unreachable, shutdown on request, signal and with a
//! blocked handler, one owner per root, restart after SIGKILL with the killed
//! App Server's frozen reports deleted, many clients
//! at once, the socket's path, modes and hourly freshness, every result and
//! failure code of the server commands, no key in status, the log file's
//! records and a log file that cannot be opened, the removal of interrupted replacements at start, and the
//! seam protocol's barriers, clock and counters through the harness.

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rustix::fs::{AtFlags, CWD, FlockOperation, Timespec, Timestamps};
use serde_json::{Value, json};
use state::paths::StateRoot;
use tetral_tests::app_server::{AppServer, request, socket_path};
use tetral_tests::record::Case;
use tetral_tests::run::{self, Run, Tetral};
use tetral_tests::seams::{Seams, WAIT_LIMIT};
use tetral_tests::snapshot::snapshot;
use tetral_tests::socket_directory::{self, socket_dir};

const APP: &str = "app_one";
const KEY: &str = "tetral_ck_canary_status_4f1d";

/// A loopback address where nothing listens, so that every connection
/// attempt fails at once and Engine stays unreachable.
const UNREACHABLE_ENGINE: &str = "http://127.0.0.1:1";

fn bootstrap() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "server_url": UNREACHABLE_ENGINE,
        "instance_id": "inst_one",
        "credential_id": "ccred_one",
        "connection_key": KEY,
        "expires_at": "2027-01-01T00:00:00Z",
    }))
    .unwrap()
}

/// A root that `configure` installed, without a TTY: the harness pipes its
/// stdin.
fn configured_root(case: &mut Case, directory: &Path) -> PathBuf {
    let root = directory.join("root");
    let run = Tetral::new(["configure", "--app", APP, "--from-stdin"])
        .home(&root)
        .stdin(bootstrap())
        .run();
    case.command("tetral configure --app app_one --from-stdin", &run.exit());
    assert_eq!(run.object(), json!({}));
    root
}

fn server(command: &str, root: &Path) -> Tetral {
    Tetral::new(["server", command, "--app", APP]).home(root)
}

fn start(case: &mut Case, root: &Path) -> AppServer {
    let app_server = AppServer::start(server("run", root), root);
    case.command("tetral server run --app app_one", "running");
    app_server
}

fn client(root: &Path) -> sdk::Client {
    sdk::Client::new(&StateRoot::new(root.as_os_str()).unwrap()).unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().mode() & 0o7777
}

fn expected_status(server_state: &str, connection_state: &str) -> Value {
    json!({
        "server_state": server_state,
        "connection_state": connection_state,
        "instance_id": "inst_one",
        "connection_id": null,
        "credential_id": "ccred_one",
        "expires_at": "2027-01-01T00:00:00Z",
        "cli_version": env!("CARGO_PKG_VERSION"),
        "protocol_version": 1,
        "last_error": null,
    })
}

/// A running App Server's status while Engine is unreachable, whose
/// connection is `connecting` during an attempt and `disconnected` between
/// attempts, with the state read as `disconnected`.
fn while_unreachable(mut status: Value) -> Value {
    let state = status["connection_state"].clone();
    assert!(
        state == "connecting" || state == "disconnected",
        "connection_state {state} while Engine is unreachable"
    );
    status["connection_state"] = json!("disconnected");
    status
}

/// Runs a command that must fail with `code` and leave `root` unchanged.
fn fails_and_changes_nothing(
    case: &mut Case,
    root: &Path,
    command: &str,
    tetral: Tetral,
    code: &str,
) -> Run {
    let before = snapshot(root);
    let run = tetral.run();
    case.command(command, &run.exit());
    assert_eq!(run.error_code(), code, "{command}: {}", run.message());
    assert_eq!(snapshot(root), before, "{command} changed the root");
    case.observe(format!("{code}: {}; the root is unchanged", run.message()));
    run
}

/// The records a run wrote to stderr.
fn records(run: &Run) -> impl Iterator<Item = Value> + '_ {
    std::str::from_utf8(&run.stderr)
        .expect("stderr is UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("each stderr line is one record"))
}

/// Whether nothing holds the root's lock: the test takes it without waiting.
fn lock_is_free(root: &Path) -> bool {
    let file = File::open(root.join("run/app-server.lock")).unwrap();
    rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive).is_ok()
}

#[test]
fn status_answers_without_engine_and_shutdown_ends_the_process() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();

    let mut case = record.case("status, wrong routes and shutdown through the SDK");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    for (method, path, status) in [("GET", "/v1/unknown", 404), ("GET", "/v1/shutdown", 405)] {
        let response = request(&app_server.socket, method, path).unwrap();
        assert_eq!(
            (response.status, response.body.as_slice()),
            (status, &b""[..])
        );
        case.observe(format!("{method} {path}: {status} with an empty body"));
    }
    let runtime = runtime();
    let client = client(&root);
    let status = runtime.block_on(client.status()).unwrap();
    assert_eq!(
        while_unreachable(serde_json::to_value(&status).unwrap()),
        expected_status("running", "disconnected")
    );
    case.observe("status through the SDK: running, connection disconnected");
    let limited =
        sdk::Client::with_response_limit(&StateRoot::new(root.as_os_str()).unwrap(), 64).unwrap();
    let refused = runtime.block_on(limited.status());
    assert!(matches!(refused, Err(sdk::Error::Decode(_))), "{refused:?}");
    case.observe("an SDK client limited to 64 bytes refuses the status body");
    let raw = request(&app_server.socket, "GET", "/v1/status").unwrap();
    assert!(!String::from_utf8_lossy(&raw.body).contains("tetral_ck_"));
    case.observe("no connection key in the status body");
    runtime.block_on(client.shutdown()).unwrap();
    case.observe("shutdown through the SDK: 202 with the body {}");
    let (run, took) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert!(took < Duration::from_secs(5), "the exit took {took:?}");
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    assert!(!String::from_utf8_lossy(&run.stdout).contains("tetral_ck_"));
    assert!(fs::symlink_metadata(socket_path(&root)).is_err());
    case.observe(format!(
        "exit 0 within {took:?}; stdout holds the status as of the exit; the socket is gone"
    ));
    drop(case);

    let mut case = record.case("SIGINT and SIGTERM each end the process");
    for (signal, name) in [
        (rustix::process::Signal::INT, "SIGINT"),
        (rustix::process::Signal::TERM, "SIGTERM"),
    ] {
        let app_server = start(&mut case, &root);
        let pid = rustix::process::Pid::from_raw(app_server.pid() as i32).unwrap();
        rustix::process::kill_process(pid, signal).unwrap();
        let (run, took) = app_server.exited();
        case.command(&format!("{name} to tetral server run"), &run.exit());
        assert!(took < Duration::from_secs(5), "the exit took {took:?}");
        assert_eq!(run.object(), expected_status("stopping", "stopped"));
        assert!(lock_is_free(&root));
        case.observe(format!("{name}: exit 0 within {took:?} with the status"));
    }
}

#[test]
fn one_app_server_owns_a_root_and_others_disturb_nothing() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("a second server run and configure while an App Server runs");
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    let socket_before = fs::symlink_metadata(&app_server.socket).unwrap();
    let status_before = while_unreachable(app_server.status());
    let configuration = |root: &Path| {
        (
            fs::read(root.join("config.toml")).unwrap(),
            fs::read(root.join("credentials.json")).unwrap(),
        )
    };
    let files_before = configuration(&root);
    let runs = [
        (
            "tetral server run --app app_one (second)",
            server("run", &root),
        ),
        (
            "tetral configure --app app_one --from-stdin while an App Server runs",
            Tetral::new(["configure", "--app", APP, "--from-stdin"])
                .home(&root)
                .stdin(bootstrap()),
        ),
    ];
    for (command, tetral) in runs {
        let run = tetral.run();
        case.command(command, &run.exit());
        assert_eq!(run.error_code(), "server_running", "{command}");
        let socket_after = fs::symlink_metadata(&app_server.socket).unwrap();
        assert_eq!(
            (
                socket_after.ino(),
                socket_after.mtime(),
                socket_after.mtime_nsec()
            ),
            (
                socket_before.ino(),
                socket_before.mtime(),
                socket_before.mtime_nsec()
            )
        );
        assert_eq!(while_unreachable(app_server.status()), status_before);
        assert_eq!(configuration(&root), files_before);
        case.observe(format!(
            "{command}: server_running; the socket, the status and both configuration files are unchanged"
        ));
    }
    drop(app_server);
    drop(case);

    let mut case = record.case("configure while the test holds the root's lock");
    let lock = File::open(root.join("run/app-server.lock")).unwrap();
    rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive).unwrap();
    let configure = Tetral::new(["configure", "--app", APP, "--from-stdin"])
        .home(&root)
        .stdin(bootstrap());
    fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral configure --app app_one --from-stdin with the lock held",
        configure,
        "server_running",
    );
}

#[test]
fn a_killed_app_server_leaves_a_socket_and_reports_that_the_next_start_replaces() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("SIGKILL, then another server run");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    let socket = app_server.socket.clone();
    app_server.kill();
    case.command("SIGKILL to tetral server run", "killed");
    assert!(
        fs::symlink_metadata(&socket)
            .unwrap()
            .file_type()
            .is_socket()
    );
    assert!(request(&socket, "GET", "/v1/status").is_none());
    case.observe("the killed App Server's socket remains and refuses connections");
    // A report frozen by the killed App Server and never acknowledged.
    fs::create_dir_all(root.join("outbox/1")).unwrap();
    fs::write(root.join("outbox/1/r1.json"), b"{}").unwrap();
    let next = start(&mut case, &root);
    assert_eq!(
        while_unreachable(next.status()),
        expected_status("running", "disconnected")
    );
    assert_eq!(fs::read_dir(root.join("outbox")).unwrap().count(), 0);
    assert_eq!(mode(&root.join("outbox")), 0o700);
    case.observe("the next server run serves status at the same path, and its outbox/ is empty with mode 0700");
}

#[test]
fn sixteen_sdk_clients_at_once_all_succeed() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "16 SDK clients call status and sessions at once while another connection holds half a request",
    );
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    // A listener that serves one connection at a time stays on this one, so
    // the calls below can succeed only on a listener that serves them
    // together.
    let mut half = UnixStream::connect(&app_server.socket).unwrap();
    half.write_all(b"GET /v1/status HTTP/1.1\r\nHost: local")
        .unwrap();
    let runtime = runtime();
    let together = Arc::new(tokio::sync::Barrier::new(16));
    let answers = runtime.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, async {
            let calls: Vec<_> = (0..16)
                .map(|_| {
                    let client = client(&root);
                    let together = together.clone();
                    tokio::spawn(async move {
                        together.wait().await;
                        let (status, sessions) = tokio::join!(client.status(), client.sessions());
                        (status.unwrap(), sessions.unwrap())
                    })
                })
                .collect();
            let mut answers = Vec::new();
            for call in calls {
                answers.push(call.await.unwrap());
            }
            answers
        })
        .await
    });
    let answers = answers.expect("the calls are answered while a connection holds half a request");
    assert_eq!(answers.len(), 16);
    for (status, sessions) in answers {
        assert_eq!(
            while_unreachable(serde_json::to_value(status).unwrap()),
            expected_status("running", "disconnected")
        );
        assert!(sessions.sessions.is_empty());
    }
    case.observe(
        "all 16 status and 16 sessions calls succeeded while the half request stayed open",
    );
    drop(half);
}

#[test]
fn server_commands_write_their_results_and_each_failure_code() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("server run, status and stop succeed");
    let root = configured_root(&mut case, directory.path());
    fs::write(root.join(".replace-a1b2c3"), "partial").unwrap();
    let app_server = start(&mut case, &root);
    let status = server("status", &root).run();
    case.command("tetral server status --app app_one", &status.exit());
    assert_eq!(
        while_unreachable(status.object()),
        expected_status("running", "disconnected")
    );
    assert!(
        !root.join(".replace-a1b2c3").exists(),
        "server run removes the temporary file of an interrupted replacement"
    );
    let stop = server("stop", &root).run();
    case.command("tetral server stop --app app_one", &stop.exit());
    assert_eq!(stop.object(), json!({}));
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    case.observe(
        "status writes the status object, stop writes {}, run writes the status as of its exit; the temporary file of an interrupted replacement is gone once the App Server runs",
    );
    drop(case);

    let mut case = record.case("server_running and server_not_running");
    let app_server = start(&mut case, &root);
    let second = server("run", &root).run();
    case.command("tetral server run --app app_one (second)", &second.exit());
    assert_eq!(second.error_code(), "server_running");
    drop(app_server);
    for command in ["status", "stop"] {
        fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("tetral server {command} --app app_one with no App Server"),
            server(command, &root),
            "server_not_running",
        );
    }
    drop(case);

    let mut case = record.case("app_mismatch and unsafe_control_path");
    for command in ["run", "status", "stop"] {
        let run = fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("tetral server {command} --app app_two"),
            Tetral::new(["server", command, "--app", "app_two"]).home(&root),
            "app_mismatch",
        );
        if command == "run" {
            assert!(
                records(&run).any(|record| record["event"] == "cold_start.phase_started"
                    && record["cold_start.phase"] == "config"),
                "server run records the start of its config phase before its checks"
            );
            case.observe("the refused server run recorded the start of its config phase");
        }
    }
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    for command in ["run", "status", "stop"] {
        fails_and_changes_nothing(
            &mut case,
            &root,
            &format!("tetral server {command} --app app_one on a root of mode 0755"),
            server(command, &root),
            "unsafe_control_path",
        );
    }
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    drop(case);

    let mut case = record.case("invalid_arguments");
    let run_with = |arguments: &[&str]| Tetral::new(["server"].iter().chain(arguments)).home(&root);
    let rows: Vec<(&str, Tetral)> = vec![
        (
            "server run with no TETRAL_HOME",
            Tetral::new(["server", "run", "--app", APP]),
        ),
        (
            "server status with TETRAL_HOME=root, a relative path",
            Tetral::new(["server", "status", "--app", APP]).env("TETRAL_HOME", "root"),
        ),
        ("server stop with no --app", run_with(&["stop"])),
        (
            "server run --idle-exit 0s",
            run_with(&["run", "--app", APP, "--idle-exit", "0s"]),
        ),
        (
            "server run --idle-exit 15x",
            run_with(&["run", "--app", APP, "--idle-exit", "15x"]),
        ),
        (
            "server run --idle-exit m",
            run_with(&["run", "--app", APP, "--idle-exit", "m"]),
        ),
        (
            "server run --idle-exit 1.5h",
            run_with(&["run", "--app", APP, "--idle-exit", "1.5h"]),
        ),
        (
            "server run --idle-exit -5s",
            run_with(&["run", "--app", APP, "--idle-exit", "-5s"]),
        ),
        ("server restart", run_with(&["restart", "--app", APP])),
    ];
    for (command, tetral) in rows {
        fails_and_changes_nothing(&mut case, &root, command, tetral, "invalid_arguments");
    }
    drop(case);

    let mut case =
        record.case("invalid_configuration for server run, with a report left in outbox/");
    fs::create_dir_all(root.join("outbox/1")).unwrap();
    fs::write(root.join("outbox/1/r1.json"), b"{}").unwrap();
    let credentials = fs::read(root.join("credentials.json")).unwrap();
    let config = fs::read_to_string(root.join("config.toml")).unwrap();
    fs::remove_file(root.join("credentials.json")).unwrap();
    fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral server run with credentials.json deleted",
        server("run", &root),
        "invalid_configuration",
    );
    fs::write(root.join("credentials.json"), &credentials).unwrap();
    fs::set_permissions(
        root.join("credentials.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let other_instance = config.replace("inst_one", "inst_two");
    assert_ne!(other_instance, config);
    fs::write(root.join("config.toml"), other_instance).unwrap();
    fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral server run with config.toml naming instance inst_two",
        server("run", &root),
        "invalid_configuration",
    );
    fs::write(root.join("config.toml"), "app = [").unwrap();
    fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral server run with a config.toml that is not TOML",
        server("run", &root),
        "invalid_configuration",
    );
    fs::write(root.join("config.toml"), config).unwrap();
    fails_and_changes_nothing(
        &mut case,
        directory.path(),
        "tetral server run on a root that does not exist",
        server("run", &directory.path().join("missing/root")),
        "invalid_configuration",
    );
    drop(case);

    let mut case = record.case("io_failed for server run");
    if case.runs_as_non_root("root may write into a directory of mode 0500") {
        fs::remove_file(root.join("run/app-server.lock")).unwrap();
        fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o500)).unwrap();
        let run = fails_and_changes_nothing(
            &mut case,
            &root,
            "tetral server run with run/ of mode 0500 and no lock file",
            server("run", &root),
            "io_failed",
        );
        assert!(
            run.message()
                .contains(root.join("run/app-server.lock").to_str().unwrap()),
            "{}",
            run.message()
        );
        fs::set_permissions(root.join("run"), fs::Permissions::from_mode(0o700)).unwrap();
    }
}

/// A directory whose path is exactly `length` bytes, under `parent`.
fn directory_of_length(parent: &Path, length: usize) -> PathBuf {
    let prefix = parent.as_os_str().len() + 1;
    let path = parent.join("d".repeat(length - prefix));
    fs::create_dir(&path).unwrap();
    assert_eq!(path.as_os_str().len(), length);
    path
}

fn modified(path: &Path) -> SystemTime {
    fs::symlink_metadata(path).unwrap().modified().unwrap()
}

#[test]
fn the_socket_lies_in_a_private_directory_and_stays_fresh() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();

    let mut case = record.case("a root under a 200-byte directory path");
    let directory = tempfile::tempdir().unwrap();
    let long = directory_of_length(directory.path(), 200);
    let root = configured_root(&mut case, &long);
    let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
    let app_server = AppServer::start(server("run", &root).seams(&seams), &root);
    case.command("tetral server run --app app_one", "running");
    assert_eq!(app_server.socket, socket_path(&root));
    assert_eq!(mode(&socket_dir()), 0o700);
    assert_eq!(mode(&app_server.socket), 0o600);
    assert!(app_server.socket.as_os_str().len() <= 57);
    let status = server("status", &root).run();
    assert_eq!(
        while_unreachable(status.object()),
        expected_status("running", "disconnected")
    );
    runtime().block_on(client(&root).status()).unwrap();
    case.observe(format!(
        "served at {} ({} bytes), S 0700, the socket 0600; server status and the SDK reach it",
        app_server.socket.display(),
        app_server.socket.as_os_str().len()
    ));
    drop(case);

    let mut case =
        record.case("each hour of the controlled clock refreshes the socket's modification time");
    let long_ago = Timestamps {
        last_access: Timespec {
            tv_sec: 946_684_800,
            tv_nsec: 0,
        },
        last_modification: Timespec {
            tv_sec: 946_684_800,
            tv_nsec: 0,
        },
    };
    for hour in 1..=2 {
        rustix::fs::utimensat(CWD, &app_server.socket, &long_ago, AtFlags::empty()).unwrap();
        let before = SystemTime::now();
        seams.advance(Duration::from_secs(60 * 60));
        seams.wait_for_count("socket.touched", hour);
        let touched = modified(&app_server.socket);
        assert!(touched >= before - Duration::from_secs(1), "{touched:?}");
        assert!(touched <= SystemTime::now());
        case.observe(format!(
            "hour {hour}: the modification time moved from 2000-01-01 to now"
        ));
    }
    drop(case);

    let mut case = record.case("server stop removes the socket and keeps S");
    let stop = server("stop", &root).run();
    case.command("tetral server stop --app app_one", &stop.exit());
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert!(fs::symlink_metadata(socket_path(&root)).is_err());
    assert_eq!(mode(&socket_dir()), 0o700);
    case.observe("the socket is gone and S remains with mode 0700");
}

#[test]
fn an_unsafe_socket_directory_stops_server_run_and_stays_as_it_is() {
    let mut socket_dir = socket_directory::replaced();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("S with mode 0755");
    let root = configured_root(&mut case, directory.path());
    socket_dir.directory_with_mode(0o755);
    let before = snapshot(&socket_dir.path());
    let run = fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral server run with S of mode 0755",
        server("run", &root),
        "unsafe_control_path",
    );
    assert!(run.message().contains(socket_dir.path().to_str().unwrap()));
    assert_eq!(snapshot(&socket_dir.path()), before);
    fs::remove_dir(socket_dir.path()).unwrap();
    drop(case);

    let mut case = record.case("S as a symbolic link");
    let target = directory.path().join("socket-target");
    fs::create_dir(&target).unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
    socket_dir.link_to(&target);
    fails_and_changes_nothing(
        &mut case,
        &root,
        "tetral server run with S a symbolic link",
        server("run", &root),
        "unsafe_control_path",
    );
    assert_eq!(fs::read_link(socket_dir.path()).unwrap(), target);
    assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
    fs::remove_file(socket_dir.path()).unwrap();
    drop(case);

    let mut case = record.case("S owned by another user");
    if case.runs_as_non_root("root owns every directory it makes") {
        match socket_dir.directory_owned_by_root() {
            Ok(()) => {
                let run = fails_and_changes_nothing(
                    &mut case,
                    &root,
                    "tetral server run with S owned by root",
                    server("run", &root),
                    "unsafe_control_path",
                );
                assert!(run.message().contains("owned by another user"));
            }
            Err(reason) => case.not_run(reason),
        }
    }
}

#[test]
fn the_log_file_takes_every_record_of_server_run_or_its_failure_is_recorded() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("the log file holds the records made before it opened and at exit");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    let shutdown = request(&app_server.socket, "POST", "/v1/shutdown").unwrap();
    assert_eq!(shutdown.status, 202);
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert!(
        records(&run).any(|record| record["event"] == "cold_start.phase_started"
            && record["cold_start.phase"] == "config"),
        "the config phase's start is recorded before the log file opens"
    );
    let file = fs::read(root.join("logs/tetral.log")).unwrap();
    assert_eq!(
        String::from_utf8(file).unwrap(),
        String::from_utf8(run.stderr.clone()).unwrap(),
        "tetral.log holds exactly the records of stderr"
    );
    case.observe(
        "logs/tetral.log holds exactly the records of stderr, the config phase's start among them",
    );
    drop(case);

    let mut case = record.case("a file stands where the log directory belongs");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    File::create(root.join("logs")).unwrap();
    let app_server = start(&mut case, &root);
    assert_eq!(app_server.status()["server_state"], "running");
    let shutdown = request(&app_server.socket, "POST", "/v1/shutdown").unwrap();
    assert_eq!(shutdown.status, 202);
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    let failures: Vec<Value> = String::from_utf8(run.stderr)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| record["event"] == "log.file_open_failed")
        .collect();
    assert_eq!(failures.len(), 1, "{failures:?}");
    for (field, value) in [
        ("level", "WARN"),
        ("log.type", "app"),
        ("error.code", "log_file_open_failed"),
        ("error.message_safe", "the log file could not be opened"),
    ] {
        assert_eq!(failures[0][field], value, "{field}");
    }
    assert!(root.join("logs").is_file());
    case.observe(
        "stderr holds one WARN log.file_open_failed record; the App Server served status and exited 0 on shutdown",
    );
}

#[test]
fn shutdown_ends_the_process_while_a_handler_is_blocked() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("a client holds a connection with half a request sent");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
    let app_server = AppServer::start(server("run", &root).seams(&seams), &root);
    case.command("tetral server run --app app_one", "running");
    let mut half = UnixStream::connect(&app_server.socket).unwrap();
    half.write_all(b"GET /v1/status HTTP/1.1\r\nHost: local")
        .unwrap();
    let posted = std::time::Instant::now();
    let shutdown = request(&app_server.socket, "POST", "/v1/shutdown").unwrap();
    assert_eq!((shutdown.status, shutdown.json()), (202, json!({})));
    // Moved once the Connection owner has stopped, so that only the blocked
    // handler meets the deadline.
    seams.wait_for_count("shutdown.waiting", 1);
    seams.advance(Duration::from_millis(4_999));
    seams.wait_for_more("shutdown.checked", 2);
    assert!(fs::symlink_metadata(socket_path(&root)).is_ok());
    seams.advance(Duration::from_millis(1));
    let (run, _) = app_server.exited();
    let took = posted.elapsed();
    case.command("tetral server run --app app_one", &run.exit());
    assert!(took < Duration::from_secs(5), "the exit took {took:?}");
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    assert!(fs::symlink_metadata(socket_path(&root)).is_err());
    assert!(lock_is_free(&root));
    case.observe(format!(
        "the socket was still there once the shutdown had read 4.999 seconds on its controlled clock; at 5 seconds the process exited 0, {took:?} after the POST; the socket is gone and the lock is free"
    ));
    drop(half);
}

#[test]
fn the_seams_hold_a_barrier_and_drive_the_clock_of_the_process_under_test() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
    let root = directory.path().join("root");

    let mut case = record.case("configure waits at configure.locked until the test releases it");
    seams.hold("configure.locked");
    let mut child = Tetral::new(["configure", "--app", APP, "--from-stdin"])
        .home(&root)
        .stdin(bootstrap())
        .seams(&seams)
        .spawn();
    seams.wait_until_waiting("configure.locked", 1);
    assert!(
        child.try_wait().unwrap().is_none(),
        "configure exited while the barrier was held"
    );
    seams.release("configure.locked");
    let run = run::finish(child);
    case.command(
        "tetral configure --app app_one --from-stdin, held at configure.locked",
        &run.exit(),
    );
    assert_eq!(run.object(), json!({}));
    case.observe(
        "configure was still running at the held barrier and exited 0 with {} after its release",
    );
    drop(case);

    let mut case =
        record.case("one hour of the controlled clock makes server run touch its socket once");
    let app_server = AppServer::start(server("run", &root).seams(&seams), &root);
    case.command("tetral server run --app app_one", "running");
    assert_eq!(seams.count("socket.touched"), 0);
    seams.advance(Duration::from_secs(60 * 60));
    seams.wait_for_count("socket.touched", 1);
    assert_eq!(seams.count("socket.touched"), 1);
    let stop = server("stop", &root).run();
    case.command("tetral server stop --app app_one", &stop.exit());
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(seams.count("socket.touched"), 1);
    case.observe(
        "socket.touched read 0 at start, 1 after the clock moved one hour, and still 1 once the process exited",
    );
}
