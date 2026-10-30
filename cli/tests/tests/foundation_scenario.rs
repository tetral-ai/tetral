//! The foundation units in one run of the actual `tetral` against the
//! reference control server: `configure` from stdin, `server run`, Session
//! preparations across a staged ready ACK, a resumed stream and a fresh
//! connection, a report on an ended connection, a close and `server stop`.

use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tetral_tests::machine::{
    APP, CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap,
    one_credential_engine, seconds, wait_for_received, wait_for_requests,
};
use tetral_tests::run::Tetral;
use tetral_tests::sessions::{
    closed, entries, failed, outcome, prepare, prepared, reports_for, state_of,
};
use tetral_tests::socket_directory;

const PROBE_SPAWN: &str = "probe.spawn";

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

#[test]
fn the_foundation_units_work_together_from_configure_to_server_stop() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let project_a = directory.path().join("project_a");
    let project_b = directory.path().join("project_b");
    std::fs::create_dir(&project_a).unwrap();
    std::fs::create_dir(&project_b).unwrap();
    let engine = one_credential_engine();

    let mut case =
        record.case("configure, server run and a preparation staged behind the ready ACK");
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
    let preparation_a = prepare(&engine, &payload("sess_a", "bind_a", &project_a));
    tetral_tests::app_server::wait_until("the preparation staged", || {
        !entries(&machine, "sess_a").is_empty()
    });
    assert_eq!(machine.seams.count(PROBE_SPAWN), 0);
    engine.control("answer", json!({ "action": "release" }));
    let preparation_b = prepare(&engine, &payload("sess_b", "bind_b", &project_b));
    assert_eq!(outcome(&engine, &preparation_a), prepared(&project_a));
    assert_eq!(outcome(&engine, &preparation_b), prepared(&project_b));
    case.observe("no probe started before the ready ACK; after it both Sessions were prepared");
    case.pass();

    let mut case = record.case("a project directory removed, then the stream broken and resumed");
    std::fs::remove_dir(&project_a).unwrap();
    machine.settle();
    assert_eq!(machine.state(), "online");
    assert_eq!(state_of(&machine, "sess_a"), "prepared");
    assert_eq!(state_of(&machine, "sess_b"), "prepared");
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    let status = machine.wait_for("online");
    assert_eq!(status["connection_id"], FIRST_CONNECTION);
    assert_eq!(state_of(&machine, "sess_a"), "prepared");
    assert_eq!(state_of(&machine, "sess_b"), "prepared");
    case.observe("with the directory removed both records and the connection stayed; the stream resumed on the same connection with both bindings prepared");
    case.pass();

    let mut case = record.case("the resume withheld past the deadline, then a fresh connection");
    let attempts = engine.requests("events").len();
    engine.control("events", json!({ "mode": "hold_headers", "count": 1 }));
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    wait_for_requests(&engine, "events", attempts + 1);
    engine.advance(seconds(44.5));
    machine.advance(seconds(43.81));
    assert_eq!(machine.state(), "disconnected");
    // The CLI retries on its own until Engine's lease of the ended
    // connection lapses.
    for _ in 0..60 {
        if machine.state() == "online" {
            break;
        }
        engine.advance(seconds(1.0));
        machine.advance(seconds(1.0));
    }
    let fresh = machine.wait_for("online")["connection_id"].clone();
    assert_ne!(fresh, FIRST_CONNECTION);
    let again_a = prepare(&engine, &payload("sess_a", "bind_a", &project_a));
    let again_b = prepare(&engine, &payload("sess_b", "bind_b", &project_b));
    assert_eq!(outcome(&engine, &again_a), failed("cwd_not_found"));
    assert_eq!(outcome(&engine, &again_b), prepared(&project_b));
    case.observe("the connection ended at its deadline and a fresh one came online without manual action; prepared again, the missing directory failed with cwd_not_found and the other succeeded");
    case.pass();

    let mut case = record.case("the directory restored and a new binding prepared without a close");
    std::fs::create_dir(&project_a).unwrap();
    let renewed = prepare(&engine, &payload("sess_a", "bind_a2", &project_a));
    assert_eq!(outcome(&engine, &renewed), prepared(&project_a));
    assert_eq!(entries(&machine, "sess_a")[0]["binding_id"], "bind_a2");
    case.observe("the new binding_id replaced the failed binding and was prepared");
    case.pass();

    let mut case = record.case("a report on the first connection");
    let answered = reports_for(&engine, &preparation_a)[0].clone();
    let mut stale = answered.clone();
    stale["message_id"] = json!("msg_stale_report");
    let response = reqwest::blocking::Client::new()
        .post(format!("{}/v1/client/session-status", engine.url))
        .bearer_auth(KEY)
        .json(&stale)
        .send()
        .unwrap();
    assert_eq!(answered["connection_id"], FIRST_CONNECTION);
    assert_eq!(response.status().as_u16(), 409);
    let body: Value = response.json().unwrap();
    assert_eq!(body["code"], "stale_connection", "{body}");
    assert_eq!(machine.status()["connection_id"], fresh);
    case.observe("a report naming the first connection was refused with 409 stale_connection and the fresh connection stayed online");
    case.pass();

    let mut case = record.case("one Session closed, then server stop");
    let closing = engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": "sess_b", "binding_id": "bind_b" },
        }),
    );
    let closing = closing["message_id"].as_str().unwrap().to_owned();
    assert_eq!(outcome(&engine, &closing), closed());
    assert!(entries(&machine, "sess_b").is_empty());
    assert_eq!(state_of(&machine, "sess_a"), "prepared");
    let stopping = Instant::now();
    let stop = Tetral::new(["server", "stop", "--app", APP])
        .home(&machine.root)
        .run();
    case.command("tetral server stop --app app_one", &stop.exit());
    assert_eq!(stop.status.code(), Some(0), "{}", stop.exit());
    let (run, _) = machine.app_server.exited();
    let took = stopping.elapsed();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert!(took < Duration::from_secs(5), "{took:?}");
    case.observe(format!(
        "the closed Session left the records and the other stayed prepared; server stop ended server run {took:?} after it began"
    ));
    case.pass();
}
