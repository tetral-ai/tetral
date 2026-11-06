//! Drive mounts seen from the reference control server: an App Server whose
//! `/dev` has no `fuse` advertises no mounting and reports every bound
//! drive as a mount failure, and a Session whose sync state is of another
//! version reports its drives once per App Server process while another
//! Session mounts its drives. Each App Server writes only its exit status
//! to stdout and only records to stderr, the second although its
//! `fusermount3` writes a line to stdout and to stderr at every run.
#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::app_server::request;
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, wait_for_received,
};
use tetral_tests::sessions::{new_connection, prepare, reports_for};
use tetral_tests::socket_directory;

fn payload(session: &str, binding: &str, cwd: &Path, drives: &[&str]) -> Value {
    let drives: Vec<Value> = drives
        .iter()
        .map(|drive| json!({ "drive_id": drive, "access": "read_write" }))
        .collect();
    json!({
        "session_id": session,
        "binding_id": binding,
        "cwd": cwd,
        "drives": drives,
        "drive_exclude": [],
        "repositories": [],
    })
}

/// The `drive_mount_failures` of the report answering `message_id`, as
/// `(drive_id, error)` pairs; none when the report has no such member.
fn mount_failures(
    engine: &tetral_tests::reference::ReferenceServer,
    id: &str,
) -> Vec<(String, String)> {
    let mut reports = Vec::new();
    tetral_tests::app_server::wait_until(&format!("the report answering {id}"), || {
        reports = reports_for(engine, id);
        !reports.is_empty()
    });
    assert_eq!(reports[0]["status"], "prepared", "{}", reports[0]);
    reports[0]["drive_mount_failures"]
        .as_array()
        .map(|failures| {
            failures
                .iter()
                .map(|failure| {
                    (
                        failure["drive_id"].as_str().unwrap().to_owned(),
                        failure["error"].as_str().unwrap().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Shuts the App Server down, checks that its stdout is only its exit
/// status, and gives every line it wrote to stderr that is not a JSON
/// record.
fn free_text_after_shutdown(machine: Machine) -> Vec<String> {
    let accepted = request(&machine.app_server.socket, "POST", "/v1/shutdown").expect("an answer");
    assert_eq!(accepted.status, 202);
    let Machine {
        app_server, seams, ..
    } = machine;
    let (run, _) = app_server.exited();
    drop(seams);
    assert_eq!(run.object()["connection_state"], "stopped");
    String::from_utf8_lossy(&run.stderr)
        .lines()
        .filter(|line| serde_json::from_str::<Value>(line).is_err())
        .map(str::to_owned)
        .collect()
}

/// Lazily unmounts what the App Server left mounted under a directory once
/// it is killed.
struct Unmount(std::path::PathBuf);

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

#[test]
fn an_app_server_without_fuse_reports_every_bound_drive_with_the_reason() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("an App Server started under a /dev without fuse");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let arguments = ["--die-with-parent", "--dev-bind", "/", "/", "--dev", "/dev"].map(Into::into);
    let machine = Machine::start_under(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        "bwrap",
        &arguments,
    );
    machine.wait_for("online");
    let ready = wait_for_received(&engine, "ready", 1);
    assert_eq!(
        ready[0]["body"]["capabilities"]["drives"],
        json!({ "mount": false })
    );
    case.observe("ready's capabilities.drives is {\"mount\": false}");

    let cwd = directory.path().join("work");
    std::fs::create_dir(&cwd).unwrap();
    let id = prepare(
        &engine,
        &payload("sess_a", "bind_a", &cwd, &["drive_a", "drive_b"]),
    );
    let failures = mount_failures(&engine, &id);
    let drives: Vec<&str> = failures.iter().map(|(drive, _)| drive.as_str()).collect();
    assert_eq!(drives, ["drive_a", "drive_b"]);
    for (_, error) in &failures {
        assert!(error.starts_with("no FUSE: "), "{error}");
    }
    assert!(!machine.root.join("sessions/sess_a/drives").exists());
    case.observe("the report lists drive_a and drive_b with errors starting 'no FUSE: '");

    let free_text = free_text_after_shutdown(machine);
    assert!(free_text.is_empty(), "{free_text:?}");
    case.observe("after the shutdown, stdout held only the exit status and every line on stderr was a JSON record");
    case.pass();
}

#[test]
fn a_sync_state_of_another_version_is_reported_once_and_other_sessions_mount() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a Session whose sync-state.db holds user_version 3, prepared twice, and another Session, on an App Server whose fusermount3 writes a line to stdout and to stderr at every run; then a shutdown",
    );
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let _unmount = Unmount(directory.path().canonicalize().unwrap());
    let programs = directory.path().join("programs");
    std::fs::create_dir(&programs).unwrap();
    let runs = directory.path().join("fusermount3-runs");
    let fusermount3 = programs.join("fusermount3");
    std::fs::write(
        &fusermount3,
        format!(
            "#!/bin/sh\necho run >> '{}'\necho stdout of fusermount3\necho stderr of fusermount3 >&2\nexec /usr/bin/fusermount3 \"$@\"\n",
            runs.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fusermount3, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", programs.display(), std::env::var("PATH").unwrap());
    let machine = Machine::start_with(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        &tetral_tests::workspace_dir().join("tests"),
        &[("PATH", path)],
    );
    machine.wait_for("online");
    let ready = wait_for_received(&engine, "ready", 1);
    assert_eq!(
        ready[0]["body"]["capabilities"]["drives"],
        json!({"mount": true})
    );
    let session = machine.root.join("sessions/sess_v3");
    state::files::create_private_dirs(&session).unwrap();
    let database = session.join("sync-state.db");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        drive::state::SyncState::create(&database)
            .await
            .unwrap()
            .close()
            .await;
    });
    // `user_version` is the big-endian integer at byte 60 of the header.
    let mut bytes = std::fs::read(&database).unwrap();
    bytes[60..64].copy_from_slice(&3u32.to_be_bytes());
    std::fs::write(&database, &bytes).unwrap();
    let cwd = directory.path().join("work");
    std::fs::create_dir(&cwd).unwrap();

    let first = prepare(
        &engine,
        &payload("sess_v3", "bind_a", &cwd, &["drive_a", "drive_b"]),
    );
    let refused = "sync state: version 3 is not supported".to_owned();
    assert_eq!(
        mount_failures(&engine, &first),
        [
            ("drive_a".to_owned(), refused.clone()),
            ("drive_b".to_owned(), refused)
        ]
    );
    assert!(!session.join("drives").exists(), "nothing is mounted");
    case.observe("the first preparation lists both drives with the version error");

    new_connection(&engine, &machine);
    let second = prepare(
        &engine,
        &payload("sess_v3", "bind_b", &cwd, &["drive_a", "drive_b"]),
    );
    assert!(mount_failures(&engine, &second).is_empty());
    assert_eq!(
        std::fs::read(&database).unwrap(),
        bytes,
        "the database is untouched"
    );
    case.observe("the second preparation lists none; the database is unchanged");

    let other = prepare(&engine, &payload("sess_ok", "bind_c", &cwd, &["drive_a"]));
    assert!(mount_failures(&engine, &other).is_empty());
    case.observe("another Session's preparation lists no mount failure");

    let free_text = free_text_after_shutdown(machine);
    let runs = std::fs::read_to_string(&runs).unwrap().lines().count();
    assert!(runs >= 2, "the probe's mount and unmount ran fusermount3");
    assert!(free_text.is_empty(), "{free_text:?}");
    case.observe(format!("fusermount3 ran {runs} times; after the shutdown, stdout held only the exit status and every line on stderr was a JSON record"));
    case.pass();
}

#[test]
fn failed_sync_state_keeps_the_drive_path_without_granting_residual_writes() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("state-open failure preserves TETRAL_DRIVES and launches with absent or read-only residual paths");
    let directory = tempfile::tempdir().unwrap();
    let tasks = tetral_tests::tasks::TaskMachine::start(&mut case, directory.path());
    let session = tasks.root.join("sessions/sess_failed");
    state::files::create_private_dirs(&session).unwrap();
    let database = session.join("sync-state.db");
    std::fs::write(&database, b"not a SQLite database").unwrap();
    let issued = prepare(
        &tasks.engine,
        &payload("sess_failed", "bind_failed", &tasks.project, &["drive_a"]),
    );
    assert_eq!(mount_failures(&tasks.engine, &issued).len(), 1);
    let drives = session.join("drives");
    assert!(!drives.exists());
    let policy = json!({
        "schema_version": 1,
        "filesystem": {"rules": [{"path": "/", "access": "read_write"}]},
        "network": {"mode": "direct"},
    });
    for residual in [false, true] {
        if residual {
            std::fs::create_dir_all(drives.join("drive_a")).unwrap();
            std::fs::write(drives.join("drive_a/ordinary"), "retained").unwrap();
        }
        let task = if residual {
            "task_residual"
        } else {
            "task_absent"
        };
        let command = if residual {
            "set -eu; cat \"$TETRAL_DRIVES/drive_a/ordinary\"; if (echo changed > \"$TETRAL_DRIVES/drive_a/ordinary\") 2>/dev/null; then exit 41; fi; if touch \"$TETRAL_DRIVES/drive_a/new\" 2>/dev/null; then exit 42; fi; printf '\\n%s\\n' \"$TETRAL_DRIVES\""
        } else {
            "set -eu; test ! -d \"$TETRAL_DRIVES\"; printf '%s\\n' \"$TETRAL_DRIVES\""
        };
        tasks.execute_with(
            "sess_failed",
            "bind_failed",
            task,
            json!({"cmd": command, "wait_ms": 0}),
            policy.clone(),
            None,
        );
        let result = tasks.terminal(task);
        assert_eq!(result["result"]["result"]["exit_code"], 0, "{result}");
        let output = result["result"]["result"]["stdout"]["text"]
            .as_str()
            .unwrap();
        assert!(
            output.lines().any(|line| line == drives.to_str().unwrap()),
            "{output}"
        );
        if residual {
            assert!(output.contains("retained"));
            assert_eq!(
                std::fs::read(drives.join("drive_a/ordinary")).unwrap(),
                b"retained"
            );
            assert!(!drives.join("drive_a/new").exists());
        }
    }
    case.observe("both real tasks launched; the configured drive path remained set, absent paths needed no setup, and residual files were readable but not writable under a broad writable caller policy");
    case.pass();
}
