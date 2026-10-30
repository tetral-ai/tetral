//! `session.drives` of `tetral server run` against the reference control
//! server, as separate processes over real HTTP and SSE: one handled only
//! after its binding's preparation ACK, one for a closing binding ignored,
//! and one naming a binding the CLI never received refused as a protocol
//! error.

use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{answer_fault, one_credential_engine, send, wait_for_received};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::sessions::{
    online_machine, outcome, prepare, prepared, quiesce, records, reports_for, state_of,
};
use tetral_tests::socket_directory;

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

/// Writes a `session.drives` of no drives on `connection`.
fn send_drives(engine: &ReferenceServer, connection: &Value, message_id: &str, binding: &str) {
    send(
        engine,
        json!({
            "protocol_version": 1,
            "connection_id": connection,
            "message_id": message_id,
            "type": "session.drives",
            "session_id": "sess_a",
            "binding_id": binding,
            "payload": { "session_id": "sess_a", "binding_id": binding, "drives": [] },
        }),
    );
}

#[test]
fn a_session_drives_waits_for_the_preparation_ack_is_ignored_while_closing_and_needs_a_known_binding()
 {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    std::fs::create_dir(&project).unwrap();
    let engine = one_credential_engine();

    let mut case = record.case("a session.drives while the prepared ACK is withheld");
    let root = directory.path().join("machine");
    let machine = online_machine(&mut case, &engine, &root);
    let connection = machine.status()["connection_id"].clone();
    answer_fault(
        &engine,
        json!({ "endpoint": "session-status", "count": 1, "action": "withhold" }),
    );
    let preparation = prepare(&engine, &payload("sess_a", "bind_a", &project));
    wait_for_received(&engine, "session-status", 1);
    send_drives(&engine, &connection, "drives_1", "bind_a");
    quiesce(&machine, 2);
    assert_eq!(state_of(&machine, "sess_a"), "prepared_pending_ack");
    assert!(reports_for(&engine, "drives_1").is_empty());
    engine.control("answer", json!({ "action": "release" }));
    assert_eq!(outcome(&engine, &preparation), prepared(&project));
    wait_until("the applied report", || {
        !reports_for(&engine, "drives_1").is_empty()
    });
    let applied = &reports_for(&engine, "drives_1")[0];
    assert_eq!(
        (&applied["status"], &applied["binding_id"]),
        (&json!("applied"), &json!("bind_a"))
    );
    assert!(applied.get("lost_changes").is_none() && applied.get("drive_mount_failures").is_none());
    case.observe("no applied report while the prepared report's ACK was withheld; after the ACK one applied report answered the session.drives, with no failures and no lost changes");
    case.pass();

    let mut case = record.case("a session.drives for a closing binding");
    engine.control(
        "close",
        json!({
            "instance_id": tetral_tests::machine::INSTANCE,
            "payload": { "session_id": "sess_a", "binding_id": "bind_a" },
        }),
    );
    send_drives(&engine, &connection, "drives_2", "bind_a");
    quiesce(&machine, 4);
    wait_until("the ignored record", || {
        records(&machine.root, "drives.ignored_closing").len() == 1
    });
    assert!(reports_for(&engine, "drives_2").is_empty());
    case.observe("the closing binding's session.drives was not reported and wrote one drives.ignored_closing record; the close's own report belongs to Session close and is checked there");
    case.pass();

    let mut case = record.case("a session.drives naming a binding the CLI never received");
    send_drives(&engine, &connection, "drives_3", "bind_never");
    let errors = wait_for_received(&engine, "protocol-error", 1);
    let error = &errors[0]["body"];
    assert_eq!(
        (&error["error"]["code"], &error["request_message_id"]),
        (&json!("invalid_message"), &json!("drives_3"))
    );
    quiesce(&machine, 5);
    assert!(reports_for(&engine, "drives_3").is_empty());
    assert_eq!(state_of(&machine, "sess_a"), "closing");
    case.observe("one protocol-error report with invalid_message named the session.drives; no session-status answered it and the binding is unchanged");
    case.pass();
}
