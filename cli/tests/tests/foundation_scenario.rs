//! The foundation units in one run of the actual `tetral` against the
//! reference control server: `configure` from stdin, `server run`, Session
//! preparations across a staged ready ACK, a resumed stream and a fresh
//! connection, a report in flight when its connection ends, a close,
//! `server stop`, a start at once after it, and SIGTERM while online.

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
    // What the App Server shows of its connection is what the server knows
    // of it, and what it declared it can do.
    let status = machine.status();
    let event = &engine.requests("events")[0];
    assert_eq!(
        json!([
            status["connection_state"],
            status["instance_id"],
            status["credential_id"],
            status["connection_id"]
        ]),
        json!([
            "online",
            INSTANCE,
            event["query"]["credential_id"],
            event["connection_id"]
        ]),
        "{status}"
    );
    assert_eq!(event["query"]["credential_id"], CREDENTIAL);
    let ready = &engine.received("ready")[0]["body"];
    assert_eq!(
        ready["cli_version"],
        Tetral::new(["--version"]).run().object()["cli_version"]
    );
    let execution = &ready["capabilities"]["tool_execution"];
    assert_eq!(
        json!([execution["network_modes"], execution.get("managed_network")]),
        json!([["disabled", "direct"], null]),
        "{ready}"
    );
    case.observe("no probe started before the ready ACK; after it both Sessions were prepared; the status names the instance, the credential and the connection of the events request, and ready carries the version tetral --version prints and the network modes disabled and direct, with no managed network");
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
    // A preparation whose report the reference server holds before reading
    // it, while the first connection ends.
    let held = || engine.state()["held"].as_array().map_or(0, Vec::len);
    let on_first = || {
        engine
            .requests("session-status")
            .iter()
            .filter(|request| request["connection_id"] == FIRST_CONNECTION)
            .count()
    };
    let sent_on_first = on_first();
    answer_fault(
        &engine,
        json!({ "endpoint": "session-status", "action": "hold_request" }),
    );
    let in_flight = prepare(&engine, &payload("sess_c", "bind_c", &project_b));
    tetral_tests::app_server::wait_until("the report held", || held() == 1);
    let attempts = engine.requests("events").len();
    engine.control("events", json!({ "mode": "hold_headers", "count": 1 }));
    engine.control("break_stream", json!({ "instance_id": INSTANCE }));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    wait_for_requests(&engine, "events", attempts + 1);
    engine.advance(seconds(44.5));
    machine.advance(seconds(43.81));
    assert_eq!(machine.state(), "disconnected");
    // The connection's end stops the report's attempt.
    tetral_tests::app_server::wait_until("the held report withdrawn", || held() == 0);
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

    let mut case = record.case("a report in flight when the first connection ended");
    // Past the first retry wait of a report.
    machine.advance(seconds(1.5));
    assert!(reports_for(&engine, &in_flight).is_empty());
    assert_eq!(
        on_first(),
        sent_on_first,
        "nothing more names the first connection"
    );
    assert_eq!(machine.status()["connection_id"], fresh);
    assert_eq!(machine.state(), "online");
    case.observe("the report held at the reference server when the first connection ended was withdrawn by the CLI and never sent on the fresh connection, which stayed online");
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
    let disconnects = engine.requests("disconnect").len();
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
    assert_eq!(engine.requests("disconnect").len(), disconnects + 1);
    case.observe(format!(
        "the closed Session left the records and the other stayed prepared; server stop ended server run {took:?} after it began, with one disconnect"
    ));
    case.pass();

    let mut case = record.case("server run again at once after the server stop");
    let attempts = engine.requests("events").len();
    let again = Machine {
        app_server: Machine::run(&machine.root, &machine.seams),
        seams: machine.seams,
        root: machine.root,
    };
    case.command("tetral server run --app app_one", "running");
    again.wait_for("online");
    let events = engine.requests("events");
    assert_eq!(events.len(), attempts + 1, "{events:?}");
    assert_eq!(events[attempts]["status"], 200, "{}", events[attempts]);
    case.observe("its first event request was answered 200 and it is online");
    case.pass();

    let mut case = record.case("SIGTERM to the online App Server");
    let disconnects = engine.requests("disconnect").len();
    let pid = rustix::process::Pid::from_raw(again.app_server.pid() as i32).unwrap();
    let signalled = Instant::now();
    rustix::process::kill_process(pid, rustix::process::Signal::TERM).unwrap();
    let (run, _) = again.app_server.exited();
    let took = signalled.elapsed();
    case.command("tetral server run --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert!(took < Duration::from_secs(5), "{took:?}");
    let sent = engine.requests("disconnect");
    assert_eq!(sent.len(), disconnects + 1, "{sent:?}");
    assert_eq!(sent[disconnects]["status"], 200, "{}", sent[disconnects]);
    case.observe(format!(
        "server run exited 0 {took:?} after the signal, with one disconnect answered 200"
    ));
    case.pass();
}
