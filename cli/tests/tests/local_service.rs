//! `tetral server run`, `server status` and `server stop` as separate
//! processes, and the local API as the SDK and raw HTTP reach it: status
//! while Engine is unreachable, shutdown on request, signal and with a
//! blocked handler, one owner per root, restart after SIGKILL with the stale
//! socket replaced, many clients
//! at once, the socket's path, modes and hourly freshness, every result and
//! failure code of the server commands, no key in status, the log file's
//! records and a log file that cannot be opened, the removal of interrupted replacements at start, the
//! seam protocol's barriers, clock and counters through the harness, the
//! log file's rotation, the local metrics and their pressure and task
//! series, and `tetral diag`'s bundle.

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

#[test]
fn command_exit_flush_waits_for_blocked_stderr_and_respects_its_deadline() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    for drain in [true, false] {
        let mut case = record.case(if drain {
            "an early command failure flushes its final queued log after stderr is drained"
        } else {
            "an early command failure exits at the shared flush deadline with stderr still full"
        });
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root-is-a-file");
        fs::write(&root, b"not a state directory").unwrap();
        let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
        let mut child = server("run", &root)
            .seams(&seams)
            .stderr_backpressure()
            .spawn();
        // Startup reads the clock for the harness check and process start;
        // the third read fixes the flush deadline. The prefilled pipe prevents its
        // queued config-start record from completing before we drain it.
        // A command with its flush removed exits instead, and the final-log
        // assertion below observes the missing record after that exit.
        tetral_tests::app_server::wait_until("exit flush or command exit", || {
            seams.clock_reads() >= 3 || child.try_wait().unwrap().is_some()
        });
        if drain {
            child.drain_stderr();
        } else {
            assert!(
                child.try_wait().unwrap().is_none(),
                "flush returned before its deadline"
            );
            seams.advance(Duration::from_secs(1));
            tetral_tests::app_server::wait_until("the command's flush deadline", || {
                child.try_wait().unwrap().is_some()
            });
            child.drain_stderr();
        }
        let run = run::finish(child);
        case.command(
            "tetral server run --app app_one with a prefilled stderr pipe",
            &run.exit(),
        );
        assert_eq!(run.error_code(), "unsafe_control_path");
        let logs: Vec<Value> = std::str::from_utf8(&run.stderr)
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("complete log record"))
            .collect();
        if drain {
            assert!(
                logs.iter()
                    .any(|log| log["event"] == "cold_start.phase_started"
                        && log["cold_start.phase"] == "config"),
                "the command lost its final queued record: {logs:?}"
            );
            assert_eq!(seams.now(), Duration::from_secs(1_000));
            case.observe("with the seam clock frozen, draining stderr delivered the final queued config-start record and the command then exited with its expected error");
        } else {
            assert!(
                logs.is_empty(),
                "stderr remained full until after the command exited"
            );
            case.observe("the full stderr pipe remained undrained; advancing the shared exit-flush clock by one second allowed the expected error exit without waiting for the log writer");
        }
        case.pass();
    }
}

#[test]
fn socket_cleanup_finishes_before_a_successor_can_own_the_root() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("socket removal while the predecessor still owns the root lock");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
    seams.hold("shutdown.socket_removed");
    let predecessor = AppServer::start(server("run", &root).seams(&seams), &root);
    let accepted = server("stop", &root).run();
    assert_eq!(accepted.status.code(), Some(0), "{}", accepted.exit());
    assert_eq!(accepted.object(), json!({}));
    seams.wait_until_waiting("shutdown.socket_removed", 1);
    assert!(!predecessor.socket.exists());
    assert!(!lock_is_free(&root));
    let contender = server("run", &root).run();
    assert_eq!(contender.status.code(), Some(1), "{}", contender.exit());
    assert_eq!(contender.error_code(), "server_running");
    seams.release("shutdown.socket_removed");
    let (run, _) = predecessor.exited();
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    let successor = AppServer::start(server("run", &root), &root);
    assert_eq!(successor.status()["server_state"], "running");
    let accepted = request(&successor.socket, "POST", "/v1/shutdown").unwrap();
    assert_eq!(accepted.status, 202);
    let (run, _) = successor.exited();
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    case.observe("server stop returned successfully while server run still held the root lock; the competing process was refused after unlink and before unlock; the successor then served its own socket");
    case.pass();
}

#[test]
fn a_failure_after_bind_removes_the_socket_name_and_releases_the_root_lock() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("startup unwinds socket ownership after a post-bind error");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
    seams.hold("startup.socket_bound");
    let child = server("run", &root).seams(&seams).spawn();
    seams.wait_until_waiting("startup.socket_bound", 1);
    let socket = socket_path(&root);
    assert!(
        fs::symlink_metadata(&socket)
            .unwrap()
            .file_type()
            .is_socket()
    );
    assert!(!lock_is_free(&root));
    // chmod follows this test-owned dangling link and fails with ENOENT.
    // The owned pathname still exists, so cleanup must actually unlink it.
    fs::remove_file(&socket).unwrap();
    std::os::unix::fs::symlink(directory.path().join("missing"), &socket).unwrap();
    seams.release("startup.socket_bound");
    let result = run::finish(child);
    assert_eq!(result.error_code(), "io_failed");
    assert_eq!(
        fs::symlink_metadata(&socket).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(lock_is_free(&root));
    let successor = AppServer::start(server("run", &root), &root);
    let stop = server("stop", &root).run();
    assert!(stop.status.success(), "{}", stop.exit());
    let (result, _) = successor.exited();
    assert!(result.status.success(), "{}", result.exit());
    case.observe("a real chmod failure after bind removed the residual pathname, released the lock, and allowed a successor to start and stop");
    case.pass();
}

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
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
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
    sdk::Client::with_test_socket_context(
        &StateRoot::new(root.as_os_str()).unwrap(),
        socket_directory::context().paths(),
        sdk::RESPONSE_MAX_BYTES,
    )
    .unwrap()
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
    let _socket_dir = socket_directory::isolated();
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
    let limited = sdk::Client::with_test_socket_context(
        &StateRoot::new(root.as_os_str()).unwrap(),
        socket_directory::context().paths(),
        64,
    )
    .unwrap();
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
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    assert!(!String::from_utf8_lossy(&run.stdout).contains("tetral_ck_"));
    assert!(fs::symlink_metadata(socket_path(&root)).is_err());
    case.observe(format!(
        "exit 0 within {took:?}; stdout holds the status as of the exit; the socket is gone"
    ));
    case.pass();

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
        assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        assert_eq!(run.object(), expected_status("stopping", "stopped"));
        assert!(lock_is_free(&root));
        case.observe(format!("{name}: exit 0 within {took:?} with the status"));
    }
    case.pass();
}

#[test]
fn one_app_server_owns_a_root_and_others_disturb_nothing() {
    let _socket_dir = socket_directory::isolated();
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
    case.pass();

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
    case.pass();
}

#[test]
fn a_killed_app_server_leaves_a_socket_that_the_next_start_replaces() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("SIGKILL, then another server run");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    assert!(
        !root.join("outbox").exists(),
        "reports are not stored on disk"
    );
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
    let next = start(&mut case, &root);
    assert_eq!(next.socket, socket);
    assert_eq!(
        while_unreachable(next.status()),
        expected_status("running", "disconnected")
    );
    assert!(
        !root.join("outbox").exists(),
        "restart creates no report directory"
    );
    case.observe("the next server run serves status at the same socket path without creating a report directory");
    case.pass();
}

#[test]
fn sixteen_sdk_clients_at_once_all_succeed() {
    let _socket_dir = socket_directory::isolated();
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
    case.pass();
}

#[test]
fn server_commands_write_their_results_and_each_failure_code() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("server run, status and stop succeed");
    let root = configured_root(&mut case, directory.path());
    fs::write(root.join(".replace-a1b2c3"), "partial").unwrap();
    let app_server = start(&mut case, &root);
    let status = server("status", &root).run();
    case.command("tetral server status --app app_one", &status.exit());
    assert_eq!(status.status.code(), Some(0), "{}", status.exit());
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
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    assert_eq!(stop.object(), json!({}));
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    case.observe(
        "status writes the status object, stop writes {}, run writes the status as of its exit; the temporary file of an interrupted replacement is gone once the App Server runs",
    );
    case.pass();

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
    case.pass();

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
    case.pass();

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
    case.pass();

    let mut case =
        record.case("invalid_configuration for server run leaves existing files untouched");
    fs::write(root.join("kept.txt"), b"keep").unwrap();
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
    case.pass();

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
        case.pass();
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
    let _socket_dir = socket_directory::isolated();
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
    case.command("tetral server status --app app_one", &status.exit());
    assert_eq!(status.status.code(), Some(0), "{}", status.exit());
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
    case.pass();

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
    case.pass();

    let mut case = record.case("server stop removes the socket and keeps S");
    let stop = server("stop", &root).run();
    case.command("tetral server stop --app app_one", &stop.exit());
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert!(fs::symlink_metadata(socket_path(&root)).is_err());
    assert_eq!(mode(&socket_dir()), 0o700);
    case.observe("the socket is gone and S remains with mode 0700");
    case.pass();
}

#[test]
fn an_unsafe_socket_directory_stops_server_run_and_stays_as_it_is() {
    let mut socket_dir = socket_directory::isolated();
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
    case.pass();

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
    case.pass();

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
                case.pass();
            }
            Err(reason) => case.not_run(reason),
        }
    }
}

#[test]
fn the_log_file_takes_every_record_of_server_run_or_its_failure_is_recorded() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("the log file holds the records made before it opened and at exit");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let app_server = start(&mut case, &root);
    let shutdown = request(&app_server.socket, "POST", "/v1/shutdown").unwrap();
    assert_eq!(shutdown.status, 202);
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
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
    case.pass();

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
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
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
    case.pass();
}

#[test]
fn shutdown_ends_the_process_while_a_handler_is_blocked() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a client holds a connection with half a request sent");
    let directory = tempfile::tempdir().unwrap();
    let root = configured_root(&mut case, directory.path());
    let seams = Seams::serve(directory.path(), Duration::from_secs(1_000));
    let mut app_server = AppServer::start(server("run", &root).seams(&seams), &root);
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
    assert!(app_server.running());
    assert!(fs::symlink_metadata(socket_path(&root)).is_ok());
    seams.advance(Duration::from_millis(1));
    let (run, _) = app_server.exited();
    let took = posted.elapsed();
    case.command("tetral server run --app app_one", &run.exit());
    assert!(took < Duration::from_secs(5), "the exit took {took:?}");
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), expected_status("stopping", "stopped"));
    assert!(fs::symlink_metadata(socket_path(&root)).is_err());
    assert!(lock_is_free(&root));
    case.observe(format!(
        "the process still ran with its socket once the shutdown had read 4.999 seconds after the POST on its controlled clock; at 5 seconds it exited 0, {took:?} after the POST; the socket is gone and the lock is free"
    ));
    drop(half);
    case.pass();
}

#[test]
fn the_seams_hold_a_barrier_and_drive_the_clock_of_the_process_under_test() {
    let _socket_dir = socket_directory::isolated();
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
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    case.observe(
        "configure was still running at the held barrier and exited 0 with {} after its release",
    );
    case.pass();

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
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    let (run, _) = app_server.exited();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(seams.count("socket.touched"), 1);
    case.observe(
        "socket.touched read 0 at start, 1 after the clock moved one hour, and still 1 once the process exited",
    );
    case.pass();
}

/// The metric families `GET /v1/metrics` serves, exactly.
const FAMILIES: [&str; 27] = [
    "tetral_connection_state",
    "tetral_reconnects_total",
    "tetral_startup_phase_seconds",
    "tetral_probes_in_progress",
    "tetral_probes_limit",
    "tetral_probes_queued",
    "tetral_probes_oldest_queued_seconds",
    "tetral_probes_queue_wait_seconds",
    "tetral_probes_rejected_total",
    "tetral_tasks_in_progress",
    "tetral_tasks_queued",
    "tetral_tasks_oldest_queued_seconds",
    "tetral_tasks_queue_wait_seconds",
    "tetral_tasks_rejected_total",
    "tetral_uploads_in_progress",
    "tetral_uploads_limit",
    "tetral_uploads_queued",
    "tetral_uploads_oldest_queued_seconds",
    "tetral_uploads_queue_wait_seconds",
    "tetral_uploads_rejected_total",
    "tetral_downloads_in_progress",
    "tetral_downloads_limit",
    "tetral_downloads_queued",
    "tetral_downloads_oldest_queued_seconds",
    "tetral_downloads_queue_wait_seconds",
    "tetral_downloads_rejected_total",
    "tetral_log_write_failures_total",
];

/// `GET /v1/metrics` of the App Server of `root`, checked to be the text
/// format 0.0.4.
fn metrics(root: &Path) -> String {
    let response = request(&socket_path(root), "GET", "/v1/metrics").expect("an answer");
    assert_eq!(response.status, 200);
    assert_eq!(
        response.header("content-type"),
        Some("text/plain; version=0.0.4")
    );
    String::from_utf8(response.body).unwrap()
}

/// The value of the series `series`, its name and labels as rendered.
fn series(text: &str, series: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("no series {series} in\n{text}"))
        .parse()
        .unwrap()
}

/// Reads every entry of the `tar.gz` at `path`, by name.
fn bundle(path: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(File::open(path).unwrap()));
    archive
        .entries()
        .unwrap()
        .map(|entry| {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes).unwrap();
            (name, bytes)
        })
        .collect()
}

fn diag(root: &Path, output: &Path) -> Run {
    Tetral::new([
        "diag".as_ref(),
        "--app".as_ref(),
        APP.as_ref(),
        "--output".as_ref(),
        output.as_os_str(),
    ])
    .home(root)
    .run()
}

fn holds(bytes: &[u8], needle: &str) -> bool {
    bytes
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

#[test]
fn la_17_the_log_file_rotates_into_three_files_and_stderr_accounts_for_every_record() {
    use tetral_tests::machine::{
        CREDENTIAL, INSTANCE, KEY as ENGINE_KEY, Machine, bootstrap as engine_bootstrap,
        one_credential_engine,
    };
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();

    let mut case = record.case("server run at debug while records totalling 40 MiB are written");
    let server_run = Tetral::new(["server", "run", "--app", APP])
        .current_dir(&tetral_tests::workspace_dir().join("tests"))
        .env("TETRAL_LOG_LEVEL", "debug")
        .output_limit(128 << 20);
    let machine = Machine::start_as(
        &mut case,
        &directory.path().join("machine"),
        &engine_bootstrap(&engine.url, INSTANCE, CREDENTIAL, ENGINE_KEY),
        server_run,
    );
    machine.wait_for("online");
    let logs = machine.root.join("logs");
    // A message of another connection is recorded as one warning and
    // otherwise ignored.
    let flood = |count: usize, message_id: &str| {
        engine.control(
            "send",
            json!({ "instance_id": INSTANCE, "count": count, "event": {
                "protocol_version": 1, "connection_id": "conn_other",
                "message_id": message_id, "type": "heartbeat", "payload": {},
            }}),
        );
    };
    let message_id = format!("msg_flood_{}", "x".repeat(100));
    let lines_with = |marker: &str| -> Vec<String> {
        LOG_NAMES
            .iter()
            .filter_map(|name| fs::read_to_string(logs.join(name)).ok())
            .flat_map(|text| text.lines().map(str::to_owned).collect::<Vec<_>>())
            .filter(|line| line.contains(marker))
            .collect()
    };
    flood(100, &message_id);
    tetral_tests::app_server::wait_until("the first hundred records", || {
        lines_with(&message_id).len() == 100
    });
    let record_bytes = lines_with(&message_id)[0].len() + 1;
    let total = (40 << 20) / record_bytes + 1;
    flood(total - 100, &message_id);
    // The stream is read in order: once a preparation sent after the flood
    // is answered, every flooded record has been written or dropped.
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    let preparation = tetral_tests::sessions::prepare(
        &engine,
        &json!({
            "session_id": "sess_after", "binding_id": "bind_1", "cwd": project,
            "drives": [], "drive_exclude": [], "repositories": [],
        }),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    while tetral_tests::sessions::reports_for(&engine, &preparation).is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the flood was read within 300 s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Then the sinks' queues drain: the log file stops growing.
    let size = || -> u64 {
        LOG_NAMES
            .iter()
            .filter_map(|name| fs::metadata(logs.join(name)).ok())
            .map(|metadata| metadata.len())
            .sum()
    };
    let mut last = size();
    loop {
        std::thread::sleep(Duration::from_millis(500));
        let now = size();
        if now == last {
            break;
        }
        last = now;
    }
    let failures = series(
        &metrics(&machine.root),
        "tetral_log_write_failures_total{sink=\"stderr\"}",
    );
    let stop = server("stop", &machine.root).run();
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    let Machine {
        app_server, seams, ..
    } = machine;
    let (run, _) = app_server.exited();
    drop(seams);
    let flooded = run
        .stderr
        .split(|byte| *byte == b'\n')
        .filter(|line| holds(line, &message_id))
        .count();
    assert_eq!(flooded as f64 + failures, total as f64);
    let mut names: Vec<String> = fs::read_dir(&logs)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, LOG_NAMES);
    assert_eq!(mode(&logs), 0o700);
    for name in LOG_NAMES {
        let path = logs.join(name);
        assert!(fs::metadata(&path).unwrap().len() <= 10_485_760, "{name}");
        assert_eq!(mode(&path), 0o600, "{name}");
        for line in fs::read_to_string(&path).unwrap().lines() {
            let parsed: Value = serde_json::from_str(line).expect("one record per line");
            assert!(parsed["event"].is_string(), "{line}");
        }
    }
    case.observe(format!(
        "{total} records of {record_bytes} bytes: logs/ holds exactly the three files, each at most 10,485,760 bytes, modes 0600 in a 0700 directory, every line one record; stderr's {flooded} plus {failures} counted failures equal the records written"
    ));
    case.pass();

    let mut case = record.case("TETRAL_LOG_LEVEL=verbose");
    let run = server("run", &directory.path().join("unused"))
        .env("TETRAL_LOG_LEVEL", "verbose")
        .run();
    case.command(
        "TETRAL_LOG_LEVEL=verbose tetral server run --app app_one",
        &run.exit(),
    );
    assert_eq!(run.object()["error"]["code"], "invalid_arguments");
    case.observe("server run failed with invalid_arguments");
    case.pass();
}

const LOG_NAMES: [&str; 3] = ["tetral.log", "tetral.log.1", "tetral.log.2"];

#[test]
fn la_18_metrics_serve_exactly_the_local_families_after_a_resume_and_with_a_waiting_probe() {
    use tetral_tests::machine::{
        CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY as ENGINE_KEY, Machine,
        bootstrap as engine_bootstrap, one_credential_engine, seconds,
    };
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();
    let mut case = record.case("a resume, then nine probes started at once");
    let machine = Machine::start(
        &mut case,
        &directory.path().join("machine"),
        &engine_bootstrap(&engine.url, INSTANCE, CREDENTIAL, ENGINE_KEY),
    );
    machine.wait_for("online");
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    assert_eq!(
        machine.wait_for("online")["connection_id"],
        FIRST_CONNECTION
    );
    machine.seams.hold("probe.started");
    for index in 0..9 {
        let project = directory.path().join(format!("project_{index}"));
        fs::create_dir(&project).unwrap();
        tetral_tests::sessions::prepare(
            &engine,
            &json!({
                "session_id": format!("sess_{index}"), "binding_id": "bind_1", "cwd": project,
                "drives": [], "drive_exclude": [], "repositories": [],
            }),
        );
    }
    machine.seams.wait_until_waiting("probe.started", 8);
    tetral_tests::app_server::wait_until("the ninth waiting for a slot", || {
        series(&metrics(&machine.root), "tetral_probes_queued") == 1.0
    });
    let text = metrics(&machine.root);
    let families: Vec<&str> = text
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|line| line.split(' ').next())
        .collect();
    assert_eq!(families, FAMILIES);
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        assert!(
            FAMILIES.iter().any(|family| line.starts_with(family)),
            "{line}"
        );
    }
    assert_eq!(
        series(&text, "tetral_reconnects_total{kind=\"resume\"}"),
        1.0
    );
    assert_eq!(series(&text, "tetral_probes_limit"), 8.0);
    assert_eq!(series(&text, "tetral_probes_queued"), 1.0);
    machine.seams.release("probe.started");
    case.observe("the scrape held exactly the 27 local families; reconnects resume 1, probes limit 8 and queued 1 while the ninth waited; the upload's 429 is counted in-process, as the line's rule for rows that need drive transfer states");
    case.pass();
}

#[test]
fn la_19_diag_bundles_exactly_its_entries_with_or_without_an_app_server_and_refuses_its_output() {
    use tetral_tests::machine::{
        CREDENTIAL, INSTANCE, KEY as ENGINE_KEY, Machine, bootstrap as engine_bootstrap,
        one_credential_engine,
    };
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();

    let mut case = record.case("tetral diag with an App Server running");
    let machine = Machine::start(
        &mut case,
        &directory.path().join("machine"),
        &engine_bootstrap(&engine.url, INSTANCE, CREDENTIAL, ENGINE_KEY),
    );
    machine.wait_for("online");
    let root = machine.root.clone();
    let running = directory.path().join("running.tar.gz");
    let run = diag(&root, &running);
    case.command(
        "tetral diag --app app_one --output running.tar.gz",
        &run.exit(),
    );
    assert_eq!(run.object(), json!({}), "{}", run.exit());
    assert_eq!(mode(&running), 0o600);
    let entries = bundle(&running);
    let names: Vec<&str> = entries.keys().map(String::as_str).collect();
    assert_eq!(
        names,
        [
            "config.toml",
            "logs/",
            "logs/tetral.log",
            "metrics.txt",
            "status.json",
            "system.json"
        ]
    );
    let status: Value = serde_json::from_slice(&entries["status.json"]).unwrap();
    assert_eq!(status["server_state"], "running");
    assert!(holds(&entries["metrics.txt"], "tetral_connection_state"));
    let system: Value = serde_json::from_slice(&entries["system.json"]).unwrap();
    assert_eq!(
        system["cli_version"],
        Tetral::new(["--version"]).run().object()["cli_version"]
    );
    assert!(
        system["os"].is_string() && system["os_version"].is_string() && system["arch"].is_string()
    );
    for (name, bytes) in &entries {
        assert!(
            !holds(bytes, ENGINE_KEY) && !holds(bytes, "tetral_ck_"),
            "{name}"
        );
    }
    case.observe("exit 0 with {}; the bundle has mode 0600 and exactly logs/, logs/tetral.log, config.toml, system.json, status.json (running) and metrics.txt, none holding the key or tetral_ck_");
    case.pass();

    let mut case = record.case("tetral diag with no App Server");
    let stop = server("stop", &root).run();
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    let Machine {
        app_server, seams, ..
    } = machine;
    let _ = app_server.exited();
    drop(seams);
    let stopped = directory.path().join("stopped.tar.gz");
    let run = diag(&root, &stopped);
    case.command(
        "tetral diag --app app_one --output stopped.tar.gz",
        &run.exit(),
    );
    assert_eq!(run.object(), json!({}), "{}", run.exit());
    let entries = bundle(&stopped);
    assert_eq!(
        serde_json::from_slice::<Value>(&entries["status.json"]).unwrap(),
        json!({ "server_state": "not_running" })
    );
    assert!(!entries.contains_key("metrics.txt"));
    case.observe("status.json is {\"server_state\": \"not_running\"} and metrics.txt is absent");
    case.pass();

    let mut case = record.case("an existing --output, then a relative one");
    let before = fs::read(&stopped).unwrap();
    let run = diag(&root, &stopped);
    case.command(
        "tetral diag --app app_one --output stopped.tar.gz (existing)",
        &run.exit(),
    );
    assert_eq!(run.object()["error"]["code"], "invalid_arguments");
    assert_eq!(fs::read(&stopped).unwrap(), before);
    let listing = snapshot(directory.path());
    let run = Tetral::new(["diag", "--app", APP, "--output", "relative.tar.gz"])
        .home(&root)
        .current_dir(directory.path())
        .run();
    case.command(
        "tetral diag --app app_one --output relative.tar.gz",
        &run.exit(),
    );
    assert_eq!(run.object()["error"]["code"], "invalid_arguments");
    assert_eq!(snapshot(directory.path()), listing);
    case.observe("both failed with invalid_arguments; the existing file is unchanged and nothing was written");
    case.pass();
}

#[test]
fn diag_never_bundles_a_key_and_gives_up_on_a_silent_socket_within_five_seconds() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("a root whose credentials carry a canary key");
    let root = configured_root(&mut case, directory.path());
    let logs = root.join("logs");
    fs::create_dir_all(&logs).unwrap();
    fs::set_permissions(&logs, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(logs.join("tetral.log"), "{\"event\":\"a\"}\n").unwrap();
    let clean = directory.path().join("clean.tar.gz");
    let run = diag(&root, &clean);
    assert_eq!(run.object(), json!({}), "{}", run.exit());
    for (name, bytes) in &bundle(&clean) {
        assert!(!holds(bytes, KEY) && !holds(bytes, "tetral_ck_"), "{name}");
    }
    case.observe("no entry of the bundle holds the canary key or tetral_ck_");
    case.pass();

    let mut case = record.case(
        "a log file holding the root's key, which lacks the prefix, then one holding only the prefix",
    );
    let plain = directory.path().join("plain");
    let configured = Tetral::new(["configure", "--app", APP, "--from-stdin"])
        .home(&plain)
        .stdin(
            serde_json::to_vec(&json!({
                "server_url": UNREACHABLE_ENGINE, "instance_id": "inst_one",
                "credential_id": "ccred_one", "connection_key": "canary_key_7d2c",
                "expires_at": "2027-01-01T00:00:00Z",
            }))
            .unwrap(),
        )
        .run();
    assert_eq!(configured.object(), json!({}), "{}", configured.exit());
    let plain_logs = plain.join("logs");
    fs::create_dir_all(&plain_logs).unwrap();
    for planted_text in ["canary_key_7d2c", "tetral_ck_another"] {
        fs::write(
            plain_logs.join("tetral.log"),
            format!("{{\"event\":\"{planted_text}\"}}\n"),
        )
        .unwrap();
        let listing = snapshot(directory.path());
        let planted = directory.path().join("planted.tar.gz");
        let run = diag(&plain, &planted);
        case.command(
            &format!(
                "tetral diag --app app_one --output planted.tar.gz ({planted_text} in the log)"
            ),
            &run.exit(),
        );
        let error = run.object()["error"].clone();
        assert_eq!(error["code"], "io_failed", "{planted_text}");
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains("logs/tetral.log"),
            "{error}"
        );
        assert_eq!(snapshot(directory.path()), listing);
    }
    case.observe("each failed with io_failed naming logs/tetral.log and left no file");
    case.pass();

    let mut case = record.case("a socket that accepts and never answers");
    fs::write(logs.join("tetral.log"), "{\"event\":\"a\"}\n").unwrap();
    let socket = socket_path(&root);
    fs::create_dir_all(socket.parent().unwrap()).unwrap();
    fs::set_permissions(socket.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
    let held = std::thread::spawn(move || {
        let accepted: Vec<_> = listener.incoming().take(2).collect();
        std::thread::sleep(Duration::from_secs(10));
        drop(accepted);
    });
    let silent = directory.path().join("silent.tar.gz");
    let started = std::time::Instant::now();
    let run = diag(&root, &silent);
    let took = started.elapsed();
    assert_eq!(run.object(), json!({}), "{}", run.exit());
    assert!(took < Duration::from_secs(7), "{took:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&bundle(&silent)["status.json"]).unwrap(),
        json!({ "server_state": "not_running" })
    );
    let _ = fs::remove_file(&socket);
    drop(held);
    case.observe(format!(
        "status.json was not_running after {took:?}, the five-second limit and the command's own start"
    ));
    case.pass();
}

/// The replay ledger's room for new operations: its 4,096 entries less its
/// 64 reserved ones.
const LEDGER_ROOM: usize = 4_032;

/// Records `closes` closes of Sessions without a record, which Engine never
/// issued and so refuses their reports: each keeps its ledger entry.
fn fill_ledger(tasks: &tetral_tests::tasks::TaskMachine, closes: usize) {
    let seams = &tasks.machine().seams;
    let connection_id = tasks.machine().status()["connection_id"].clone();
    let routed = seams.count("route.session");
    for index in 0..closes {
        tetral_tests::machine::send(
            &tasks.engine,
            json!({
                "protocol_version": 1,
                "connection_id": connection_id,
                "message_id": format!("msg_fill_{index}"),
                "type": "session.close",
                "payload": { "session_id": format!("sess_fill_{index}"), "binding_id": "bind_1" },
            }),
        );
    }
    seams.wait_for_count("route.session", routed + closes as u64);
}

/// Starts `task_id` as a background command that runs until the file
/// `flag` of the project exists.
fn background(
    tasks: &tetral_tests::tasks::TaskMachine,
    session: &str,
    binding: &str,
    task_id: &str,
) {
    let seams = &tasks.machine().seams;
    let checked = seams.count(tetral_tests::tasks::WAIT_CHECKED);
    tasks.execute_with(
        session,
        binding,
        task_id,
        json!({
            "cmd": format!("while [ ! -e {task_id}.flag ]; do sleep 0.05; done"),
            "wait_ms": 250, "on_wait_expiry": "detach",
        }),
        tetral_tests::tasks::policy(&tasks.project, "disabled"),
        None,
    );
    tasks.wait_for_start(checked);
    seams.advance(Duration::from_millis(250));
    tasks.report(task_id, "running");
}

#[test]
fn a_task_rejection_and_an_admission_a_minute_later_record_one_pressure_episode() {
    use tetral_tests::tasks::{BINDING, SESSION, TaskMachine, policy};
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let mut case = record
        .case("one tool.execute refused resource_exhausted, then one admitted 60 seconds later");
    let tasks = TaskMachine::start(&mut case, directory.path());
    // The running task holds the last entry, so the next is refused.
    fill_ledger(&tasks, LEDGER_ROOM - 1);
    background(&tasks, SESSION, BINDING, "task_holding");
    tasks.execute(
        "task_refused",
        json!({ "cmd": "true" }),
        policy(&tasks.project, "disabled"),
    );
    let report = tasks.report("task_refused", "rejected");
    assert_eq!(report["error"]["code"], "resource_exhausted", "{report:#}");
    let episodes = |event: &str| -> Vec<Value> {
        tasks
            .records(event)
            .into_iter()
            .filter(|record| record["pressure.point"] == "tetral_tasks")
            .collect()
    };
    tetral_tests::app_server::wait_until("the episode's start", || {
        episodes("pressure.rejecting_started").len() == 1
    });
    fs::write(tasks.project.join("task_holding.flag"), "").unwrap();
    tasks.terminal("task_holding");
    tasks.advance(Duration::from_secs(60));
    tasks.execute(
        "task_admitted",
        json!({ "cmd": "true" }),
        policy(&tasks.project, "disabled"),
    );
    tasks.terminal("task_admitted");
    tetral_tests::app_server::wait_until("the episode's end", || {
        !episodes("pressure.rejecting_ended").is_empty()
    });
    let started = episodes("pressure.rejecting_started");
    let ended = episodes("pressure.rejecting_ended");
    assert_eq!((started.len(), ended.len()), (1, 1));
    assert_eq!(started[0]["pressure.cause"], "resource_exhausted");
    assert_eq!(ended[0]["pressure.rejected"], 1, "{:#}", ended[0]);
    case.observe("one pressure.rejecting_started of tetral_tasks with cause resource_exhausted, and at the admission 60 seconds later one pressure.rejecting_ended with pressure.rejected 1");
    case.pass();
}

#[test]
fn the_task_series_show_running_and_staged_tasks_and_the_wait_of_a_started_one() {
    use tetral_tests::machine::answer_fault;
    use tetral_tests::tasks::{BINDING, SESSION, TaskMachine, policy};
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let mut case = record.case("two running tasks and one tool.execute staged for 3 seconds");
    let tasks = TaskMachine::start(&mut case, directory.path());
    background(&tasks, SESSION, BINDING, "task_one");
    background(&tasks, SESSION, BINDING, "task_two");
    answer_fault(
        &tasks.engine,
        json!({ "endpoint": "session-status", "action": "withhold" }),
    );
    let pending = tasks.prepare("sess_pending", "bind_pending", &tasks.project, None);
    tasks.wait_prepared(&pending);
    tasks.execute_with(
        "sess_pending",
        "bind_pending",
        "task_staged",
        json!({ "cmd": "true" }),
        policy(&tasks.project, "disabled"),
        None,
    );
    let root = tasks.root.clone();
    tetral_tests::app_server::wait_until("the staged task counted", || {
        series(&metrics(&root), "tetral_tasks_queued") == 1.0
    });
    tasks.advance(Duration::from_secs(3));
    tetral_tests::app_server::wait_until("the oldest wait at 3 seconds", || {
        series(&metrics(&root), "tetral_tasks_oldest_queued_seconds") == 3.0
    });
    let text = metrics(&root);
    assert_eq!(series(&text, "tetral_tasks_in_progress"), 2.0);
    assert_eq!(series(&text, "tetral_tasks_queued"), 1.0);
    answer_fault(&tasks.engine, json!({ "action": "release" }));
    tasks.terminal("task_staged");
    let text = metrics(&root);
    assert_eq!(series(&text, "tetral_tasks_queue_wait_seconds_count"), 1.0);
    assert_eq!(series(&text, "tetral_tasks_queue_wait_seconds_sum"), 3.0);
    for task in ["task_one", "task_two"] {
        fs::write(tasks.project.join(format!("{task}.flag")), "").unwrap();
        tasks.terminal(task);
    }
    tasks.wait_files_removed(SESSION);
    tetral_tests::app_server::wait_until("no task in progress", || {
        series(&metrics(&root), "tetral_tasks_in_progress") == 0.0
    });
    case.observe("in progress 2, queued 1 and oldest queued 3 seconds; after its start one 3-second wait sample; once the tasks ended, in progress 0");
    case.pass();
}
