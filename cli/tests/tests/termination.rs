//! The records of tasks whose processes could outlive their App Server,
//! between `tetral server run`, whose ready advertises `tool_execution`,
//! and the reference control server as separate processes, each on its own
//! controlled clock, with real restricted processes: the records an App
//! Server killed with SIGKILL leaves for the next start, also one of another
//! boot. The test seams these tests use are those `tetral_tests::tasks` and
//! `tetral_tests::machine` name.

use std::fs;
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{SESSION, TaskMachine, WAIT_CHECKED, millis, policy};

/// The test seam barrier before a record an earlier App Server left is
/// deleted as confirmed.
const CONFIRMED: &str = "cleanup.confirmed";

/// Starts `cmd` as `task_id`, detached after 250 ms, under `direct`, and
/// gives its `running` report.
fn detached(tasks: &TaskMachine, task_id: &str, cmd: &str) -> Value {
    let checked = tasks.machine().seams.count(WAIT_CHECKED);
    tasks.execute(
        task_id,
        json!({"cmd": cmd, "wait_ms": 250, "on_wait_expiry": "detach"}),
        policy(&tasks.project, "direct"),
    );
    tasks.wait_for_start(checked);
    tasks.machine().seams.advance(millis(250));
    tasks.report(task_id, "running")
}

/// Issues `session.prepare` of `session`'s `binding` at the project and
/// gives the report that answers it: its `status` and error code.
fn prepared(tasks: &TaskMachine, session: &str, binding: &str) -> Value {
    let issued = tasks.prepare(session, binding, &tasks.project, None);
    let mut outcome = Value::Null;
    wait_until(&format!("the report answering {issued}"), || {
        match tasks
            .engine
            .received("session-status")
            .into_iter()
            .find(|report| report["body"]["request_message_id"] == issued.as_str())
        {
            Some(report) => {
                outcome = json!([report["body"]["status"], report["body"]["error"]["code"]]);
                true
            }
            None => false,
        }
    });
    outcome
}

#[test]
fn records_an_app_server_killed_with_sigkill_leaves_are_confirmed_at_the_next_start() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case(
        "a command appending to a marker every 100 ms, the App Server killed with SIGKILL, the record's confirmation held at the next start",
    );
    let mut tasks = TaskMachine::start(&mut case, &directory.path().join("killed"));
    detached(
        &tasks,
        "task_marker",
        "while :; do echo x >> marker; sleep 0.1; done",
    );
    let records = tasks.root.join("execution");
    wait_until("the task's record", || listing(&records).len() == 1);
    let written: Value = serde_json::from_slice(&fs::read(&listing(&records)[0]).unwrap()).unwrap();
    assert_eq!(written["session_id"], SESSION);
    tasks.machine().seams.hold(CONFIRMED);
    tasks.kill_and_restart(|_| {});
    let marker = tasks.project.join("marker");
    let size = fs::metadata(&marker).unwrap().len();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fs::metadata(&marker).unwrap().len(), size);
    let status = tasks.machine().status();
    assert_eq!(
        status["last_error"]["code"], "cleanup_pending",
        "{status:#}"
    );
    assert_eq!(
        prepared(&tasks, SESSION, "bind_restarted"),
        json!(["failed", "cleanup_pending"])
    );
    tasks.machine().seams.release(CONFIRMED);
    // The recorded process may still be dying at the first check, which
    // then signals it and checks again a second later.
    wait_until("last_error cleared", || {
        let cleared = tasks.machine().status()["last_error"].is_null();
        if !cleared {
            tasks.machine().seams.advance(millis(100));
        }
        cleared
    });
    assert!(listing(&records).is_empty());
    assert_eq!(
        prepared(&tasks, SESSION, "bind_confirmed"),
        json!(["prepared", null])
    );
    case.observe("the marker stopped growing; last_error was cleanup_pending and the Session's preparation failed so until the record was confirmed, then null and the preparation succeeded");
    drop(tasks);

    let mut case = record.case(
        "a record of another boot naming a running process started outside any task, written before the App Server starts",
    );
    let mut tasks = TaskMachine::start(&mut case, &directory.path().join("other_boot"));
    let mut outside = std::process::Command::new("sleep")
        .arg("1231")
        .spawn()
        .unwrap();
    let pid = outside.id();
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let start_time: u64 = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap();
    let link = fs::read_link(format!("/proc/{pid}/ns/pid")).unwrap();
    let namespace: u64 = link
        .to_str()
        .unwrap()
        .trim_start_matches("pid:[")
        .trim_end_matches(']')
        .parse()
        .unwrap();
    let path = tasks
        .root
        .join("execution")
        .join("00000000000000000000000000000000.json");
    tasks.kill_and_restart(|_| {
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "session_id": SESSION, "platform": "linux",
                "boot_id": "00000000-0000-0000-0000-000000000000",
                "pid": pid, "start_time": start_time, "pid_namespace": namespace,
            }))
            .unwrap(),
        )
        .unwrap();
    });
    assert!(tasks.machine().status()["last_error"].is_null());
    assert!(!path.exists());
    assert_eq!(
        prepared(&tasks, SESSION, "bind_other_boot"),
        json!(["prepared", null])
    );
    assert!(outside.try_wait().unwrap().is_none());
    outside.kill().unwrap();
    outside.wait().unwrap();
    case.observe("the record was confirmed at the start: last_error null, the record gone, the Session prepared, and the named process still running");
}

fn listing(directory: &Path) -> Vec<std::path::PathBuf> {
    fs::read_dir(directory)
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default()
}
