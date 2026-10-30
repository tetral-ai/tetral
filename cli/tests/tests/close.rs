//! `session.close` of `tetral server run` against the reference control
//! server, as separate processes over real HTTP and SSE: a close staged
//! behind the ready ACK after its preparation, a probe result after its
//! close, the wait for an unreaped probe, an unsafe `session_id`, the close
//! of a replaced binding, and the close of one Session among two with
//! running commands.

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, answer_fault, bootstrap, one_credential_engine, seconds,
    send, wait_for_received,
};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::sessions::{
    closed, entries, failed, online_machine, outcome, prepare, prepared, quiesce, reports_for,
    state_of,
};
use tetral_tests::snapshot::snapshot;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{
    BINDING, GONE_CHECK_INTERVAL, GONE_CHECKED, SESSION, TaskMachine, WAIT_CHECKED, policy,
};

const PROBE_STARTED: &str = "probe.started";

/// A `session.prepare` payload with no drives and no repositories.
fn payload(session: &str, binding: &str, cwd: &Path) -> Value {
    json!({
        "session_id": session,
        "binding_id": binding,
        "cwd": cwd,
        "drives": [],
        "drive_exclude": [],
        "repositories": [],
    })
}

/// Issues `session.close` and gives its `message_id`.
fn close(engine: &ReferenceServer, session: &str, binding: &str) -> String {
    let issued = engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": session, "binding_id": binding },
        }),
    );
    issued["message_id"].as_str().unwrap().to_owned()
}

#[test]
fn a_close_staged_behind_the_ready_ack_runs_after_its_preparation() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let engine = one_credential_engine();

    let mut case = record.case("a session.prepare then a session.close before the ready ACK");
    answer_fault(
        &engine,
        json!({ "endpoint": "ready", "count": 1, "action": "withhold" }),
    );
    let machine = Machine::start(
        &mut case,
        &directory.path().join("machine"),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    wait_for_received(&engine, "ready", 1);
    let preparation = prepare(&engine, &payload("sess_a", "bind_a", &project));
    let closing = close(&engine, "sess_a", "bind_a");
    // The stream is read in order: a later preparation listed as staged
    // shows the close was read before it.
    let later = prepare(&engine, &payload("sess_z", "bind_z", &project));
    wait_until("the later preparation staged", || {
        !entries(&machine, "sess_z").is_empty()
    });
    assert!(reports_for(&engine, &preparation).is_empty());
    answer_fault(&engine, json!({ "action": "release" }));
    assert_eq!(outcome(&engine, &closing), closed());
    assert_eq!(outcome(&engine, &later), prepared(&project));
    quiesce(&machine, 3);
    // A close that overtook its preparation would find no binding, and the
    // preparation after it would report `prepared` and leave a record.
    assert!(reports_for(&engine, &preparation).is_empty());
    assert!(entries(&machine, "sess_a").is_empty());
    case.observe("nothing was reported before the ready ACK; after it the close reported closed, the preparation it closed never reported and the Session left the records");
    case.pass();
}

#[test]
fn a_closed_preparation_stays_closed_and_its_unreaped_probe_holds_the_closed_report() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let engine = one_credential_engine();

    let mut case =
        record.case("a held probe's binding closed through two heartbeats, then released");
    let machine = online_machine(&mut case, &engine, &directory.path().join("machine"));
    let other = prepare(&engine, &payload("sess_b", "bind_b", &project));
    assert_eq!(outcome(&engine, &other), prepared(&project));
    machine.seams.hold(PROBE_STARTED);
    let preparation = prepare(&engine, &payload("sess_a", "bind_a", &project));
    machine.seams.wait_until_waiting(PROBE_STARTED, 1);
    let closing = close(&engine, "sess_a", "bind_a");
    quiesce(&machine, 3);
    // The ready ACK made the first grant; each heartbeat's ACK makes one.
    for (heartbeat, grants) in (1..=2).zip(2..) {
        engine.advance(seconds(15.0));
        wait_for_received(&engine, "heartbeat", heartbeat);
        machine.seams.wait_for_count("connection.granted", grants);
        machine.advance(seconds(15.0));
    }
    assert_eq!(machine.state(), "online");
    assert_eq!(state_of(&machine, "sess_a"), "closing");
    assert!(reports_for(&engine, &closing).is_empty());
    machine.seams.release(PROBE_STARTED);
    assert_eq!(outcome(&engine, &closing), closed());
    assert!(reports_for(&engine, &preparation).is_empty());
    assert!(entries(&machine, "sess_a").is_empty());
    assert_eq!(state_of(&machine, "sess_b"), "prepared");
    case.observe("through two heartbeats the connection stayed online and the binding closing with no report; once the probe was reaped the close reported closed, the preparation never reported and the other binding stayed prepared");
    case.pass();

    let mut case = record.case("a prepared report whose ACK is withheld, then a close");
    answer_fault(
        &engine,
        json!({ "endpoint": "session-status", "count": 1, "action": "withhold" }),
    );
    let pending = prepare(&engine, &payload("sess_c", "bind_c", &project));
    wait_until("the prepared report accepted", || {
        !reports_for(&engine, &pending).is_empty()
    });
    let closing = close(&engine, "sess_c", "bind_c");
    assert_eq!(outcome(&engine, &closing), closed());
    answer_fault(&engine, json!({ "action": "release" }));
    quiesce(&machine, 5);
    assert!(entries(&machine, "sess_c").is_empty());
    assert_eq!(reports_for(&engine, &pending).len(), 1);
    case.observe("the close reported closed while the prepared report's ACK was withheld; that report was not sent again and its late ACK recreated no binding");
    case.pass();
}

#[test]
fn a_late_close_of_session_dot_dot_is_a_protocol_error_and_deletes_nothing() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();

    let mut case = record.case("a late session.close whose session_id is ..");
    let machine = online_machine(&mut case, &engine, &directory.path().join("machine"));
    let before = snapshot(directory.path());
    let connection = machine.status()["connection_id"].clone();
    send(
        &engine,
        json!({
            "protocol_version": 1, "connection_id": connection, "message_id": "msg_close",
            "type": "session.close", "session_id": "..", "binding_id": "bind_a",
            "payload": { "session_id": "..", "binding_id": "bind_a" },
        }),
    );
    let errors = wait_for_received(&engine, "protocol-error", 1);
    assert_eq!(errors[0]["body"]["error"]["code"], "invalid_message");
    assert_eq!(errors[0]["body"]["request_message_id"], "msg_close");
    machine.settle();
    assert!(engine.requests("session-status").is_empty());
    let mut after = snapshot(directory.path());
    let logs = machine.root.join("logs");
    after.retain(|path, _| !path.starts_with(&logs));
    let mut before = before;
    before.retain(|path, _| !path.starts_with(&logs));
    assert_eq!(after, before);
    case.observe("one protocol-error report with invalid_message named the close; no session-status was sent and the state root and its parent are unchanged but for the log");
    case.pass();
}

#[test]
fn a_close_of_a_replaced_binding_is_closed_at_once_and_leaves_the_live_binding() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let engine = one_credential_engine();

    let mut case = record.case("a failed binding replaced by a new one, then closed");
    let machine = online_machine(&mut case, &engine, &directory.path().join("machine"));
    let missing = prepare(
        &engine,
        &payload("sess_a", "bind_failed", &directory.path().join("missing")),
    );
    assert_eq!(outcome(&engine, &missing), failed("cwd_not_found"));
    let live = prepare(&engine, &payload("sess_a", "bind_live", &project));
    assert_eq!(outcome(&engine, &live), prepared(&project));
    let session = machine.root.join("sessions/sess_a");
    let before = snapshot(&session);
    let closing = close(&engine, "sess_a", "bind_failed");
    assert_eq!(outcome(&engine, &closing), closed());
    assert!(
        reports_for(&engine, &closing)[0]
            .get("lost_changes")
            .is_none()
    );
    assert_eq!(state_of(&machine, "sess_a"), "prepared");
    assert_eq!(entries(&machine, "sess_a")[0]["binding_id"], "bind_live");
    assert_eq!(snapshot(&session), before);
    case.observe("the close of the replaced binding reported closed without lost_changes; the live binding stays prepared and the Session's directory is byte-identical");
    case.pass();
}

#[test]
fn closing_one_session_ends_only_its_tasks_and_their_temporary_directories() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("two Sessions each running a command; one closed");
    let tasks = TaskMachine::start(&mut case, directory.path());
    let other = tasks.prepare("sess_other", "bind_other", &tasks.project, None);
    tasks.wait_prepared(&other);
    for (session, binding, task) in [
        (SESSION, BINDING, "task_closed"),
        ("sess_other", "bind_other", "task_kept"),
    ] {
        let checked = tasks.machine().seams.count(WAIT_CHECKED);
        tasks.execute_with(
            session,
            binding,
            task,
            json!({ "cmd": "sleep 300", "wait_ms": 250, "on_wait_expiry": "detach" }),
            policy(&tasks.project, "disabled"),
            None,
        );
        tasks.wait_for_start(checked);
        tasks.machine().seams.advance(Duration::from_millis(250));
        tasks.report(task, "running");
    }
    assert_eq!(tasks.temporary_directories().len(), 2);
    let closing = close(&tasks.engine, SESSION, BINDING);
    // The termination's SIGTERM grace and the checks of the task's end.
    for _ in 0..4 {
        tasks.advance(seconds(1.0));
    }
    // The close waits for the task's end, which the App Server checks again
    // every interval of its clock.
    let seams = &tasks.machine().seams;
    while reports_for(&tasks.engine, &closing).is_empty() {
        let checked = seams.count(GONE_CHECKED);
        tasks.advance(GONE_CHECK_INTERVAL);
        wait_until("the next check of the ended task", || {
            seams.count(GONE_CHECKED) > checked || !reports_for(&tasks.engine, &closing).is_empty()
        });
    }
    assert_eq!(outcome(&tasks.engine, &closing), closed());
    let terminal = tasks.terminal("task_closed");
    assert_eq!(
        terminal["result"]["result"]["cancelled"], true,
        "{terminal:#}"
    );
    assert!(
        tasks
            .reports("task_kept")
            .iter()
            .all(|report| report["phase"] != "terminal")
    );
    assert_eq!(tasks.temporary_directories().len(), 1);
    assert!(!tasks.root.join("sessions").join(SESSION).exists());
    assert!(tasks.root.join("sessions/sess_other").exists());
    case.observe("the closed Session's command ended cancelled, its temporary directory and Session directory were deleted and closed was reported; the other Session's command still runs with its temporary directory");
    case.pass();
}
