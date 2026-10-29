//! The Session preparations of the cross-process tests: issuing
//! `session.prepare` on the reference control server, reading the reports
//! that answer it and the App Server's view of its Sessions, and a new
//! connection between preparations.

use std::fs;
use std::path::Path;

use serde_json::{Value, json};

use crate::app_server::{request, wait_until};
use crate::machine::{CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, seconds, send};
use crate::record::Case;
use crate::reference::ReferenceServer;

/// A machine whose App Server is online with the test instance's
/// credential.
pub fn online_machine(case: &mut Case, engine: &ReferenceServer, directory: &Path) -> Machine {
    let machine = Machine::start(
        case,
        directory,
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine
}

/// Issues `session.prepare` with `payload` to `instance` and gives its
/// `message_id`.
pub fn prepare_on(engine: &ReferenceServer, instance: &str, payload: &Value) -> String {
    let issued = engine.control(
        "prepare",
        json!({ "instance_id": instance, "payload": payload }),
    );
    issued["message_id"].as_str().unwrap().to_owned()
}

/// [`prepare_on`] the test instance.
pub fn prepare(engine: &ReferenceServer, payload: &Value) -> String {
    prepare_on(engine, INSTANCE, payload)
}

/// The accepted `session-status` reports that answer `message_id`.
pub fn reports_for(engine: &ReferenceServer, message_id: &str) -> Vec<Value> {
    engine
        .received("session-status")
        .into_iter()
        .map(|received| received["body"].clone())
        .filter(|body| body["request_message_id"] == message_id)
        .collect()
}

/// Waits for the report that answers `message_id` and gives what it says:
/// `prepared` and its `cwd`, or `failed` and its error's code.
pub fn outcome(engine: &ReferenceServer, message_id: &str) -> (String, String) {
    let mut reports = Vec::new();
    wait_until(&format!("the report answering {message_id}"), || {
        reports = reports_for(engine, message_id);
        !reports.is_empty()
    });
    let report = &reports[0];
    let status = report["status"].as_str().unwrap().to_owned();
    let detail = match status.as_str() {
        "prepared" => report["cwd"].as_str().unwrap().to_owned(),
        _ => report["error"]["code"].as_str().unwrap().to_owned(),
    };
    (status, detail)
}

pub fn prepared(cwd: &Path) -> (String, String) {
    (
        "prepared".to_owned(),
        cwd.canonicalize().unwrap().to_str().unwrap().to_owned(),
    )
}

pub fn failed(code: &str) -> (String, String) {
    ("failed".to_owned(), code.to_owned())
}

/// `GET /v1/sessions` on the machine's App Server.
pub fn sessions(machine: &Machine) -> Vec<Value> {
    let response = request(&machine.app_server.socket, "GET", "/v1/sessions").unwrap();
    assert_eq!(response.status, 200);
    response.json()["sessions"].as_array().unwrap().clone()
}

/// The entries of `session` in `GET /v1/sessions`.
pub fn entries(machine: &Machine, session: &str) -> Vec<Value> {
    sessions(machine)
        .into_iter()
        .filter(|entry| entry["session_id"] == session)
        .collect()
}

/// The state of the one entry of `session`.
pub fn state_of(machine: &Machine, session: &str) -> String {
    let entries = entries(machine, session);
    assert_eq!(entries.len(), 1, "{entries:?}");
    entries[0]["state"].as_str().unwrap().to_owned()
}

/// Waits until the Connection owner has routed `routed` Session
/// instructions and everything they caused between it and the Session
/// binding owners has been handled. A read of `GET /v1/sessions` passes
/// through the Connection owner's mailbox and then each owner's, so the first
/// read ends after each owner handled its routed instruction, and the
/// second after the Connection owner handled the slot requests those sent it
/// and after each owner handled the answers.
pub fn quiesce(machine: &Machine, routed: u64) {
    machine.seams.wait_for_count("route.session", routed);
    sessions(machine);
    sessions(machine);
}

/// Ends the connection with a retryable connection `disconnect` and waits
/// until the next connection is online.
pub fn new_connection(engine: &ReferenceServer, machine: &Machine) {
    let old = machine.status()["connection_id"].clone();
    send(
        engine,
        json!({
            "protocol_version": 1,
            "connection_id": old,
            "message_id": format!("msg_disconnect_{}", old.as_str().unwrap()),
            "type": "disconnect",
            "payload": { "scope": "connection", "retryable": true, "code": "stale_connection" },
        }),
    );
    machine.wait_for("disconnected");
    engine.advance(seconds(46.0));
    machine.seams.advance(seconds(1.2));
    let status = machine.wait_for("online");
    assert_ne!(status["connection_id"], old);
}

/// The records of `event` that the App Server has written.
pub fn records(root: &Path, event: &str) -> Vec<Value> {
    let log = fs::read_to_string(root.join("logs/tetral.log")).unwrap_or_default();
    log.lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| record["event"] == event)
        .collect()
}
