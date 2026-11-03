//! The replay ledger and the Report outbox of `tetral server run` against the
//! reference control server, as separate processes over real HTTP and SSE,
//! each on its own controlled clock: refused reports that are never sent
//! again and a refusal that ends the connection, a report whose ACK was lost
//! sent again byte for byte, a Session instruction delivered again and routed
//! once, and protocol errors that name the message they answer only when its
//! `message_id` can be read. The seam counters these tests read are those
//! `tetral_tests::machine` names.

use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap,
    one_credential_engine, seconds, wait_for_received, wait_for_requests,
};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;

/// Past the first wait of a report's retry backoff: 1 second and at most 20%
/// of jitter.
const PAST_FIRST_RETRY: f64 = 1.5;

/// Writes `data` on the instance's stream as one event's data.
fn send_data(engine: &ReferenceServer, data: &str) {
    engine.control("send", json!({ "instance_id": INSTANCE, "data": data }));
}

/// The records of `event` that the App Server has written.
fn records(root: &Path, event: &str) -> Vec<Value> {
    let log = std::fs::read_to_string(root.join("logs/tetral.log")).unwrap_or_default();
    log.lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| record["event"] == event)
        .collect()
}

/// `reports` in the order of their `message_id`: the outbox sends reports
/// concurrently, so the reference server gets them in any order.
fn by_message_id(mut reports: Vec<Value>) -> Vec<Value> {
    reports.sort_by_key(|report| report["message_id"].as_str().unwrap().to_owned());
    reports
}

fn message_ids(requests: &[Value]) -> Vec<&str> {
    requests
        .iter()
        .map(|request| request["message_id"].as_str().unwrap())
        .collect()
}

fn online_machine(
    case: &mut tetral_tests::record::Case,
    engine: &ReferenceServer,
) -> (Machine, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start(
        case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    (machine, directory)
}

#[test]
fn a_report_refused_with_401_or_403_blocks_the_connection_and_stops_delivery() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    for (code, status) in [("credential_revoked", 401), ("cli_upgrade_required", 403)] {
        let mut case = record.case(&format!("a protocol-error report answered {status} {code}"));
        let engine = one_credential_engine();
        let (machine, _directory) = online_machine(&mut case, &engine);
        answer_fault(
            &engine,
            json!({
                "endpoint": "protocol-error", "action": "error", "code": code,
            }),
        );
        send_data(&engine, "not json");
        let sent = wait_for_requests(&engine, "protocol-error", 1);
        assert_eq!(sent[0]["status"], status);
        let blocked = machine.wait_for("blocked");
        assert_eq!(blocked["last_error"]["code"], code);
        assert_eq!(blocked["connection_id"], Value::Null);
        machine.advance(seconds(120.0));
        assert_eq!(machine.state(), "blocked");
        assert_eq!(engine.requests("protocol-error").len(), 1);
        assert_eq!(engine.requests("events").len(), 1);
        case.observe("the real report refusal reached the connection owner, preserved its blocked reason, and started neither another report attempt nor another connection");
        case.pass();
    }
}

#[test]
fn a_refused_report_is_never_sent_again_and_a_stale_connection_ends_the_connection() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("two reports answered 409 unknown_instruction");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    answer_fault(
        &engine,
        json!({
            "endpoint": "protocol-error",
            "count": 2,
            "action": "error",
            "code": "unknown_instruction",
        }),
    );
    send_data(&engine, "not json");
    send_data(&engine, "[\"not an object\"]");
    let refused = wait_for_requests(&engine, "protocol-error", 2);
    for request in &refused {
        assert_eq!(
            (request["status"].as_i64(), request["code"].as_str()),
            (Some(409), Some("unknown_instruction"))
        );
    }
    wait_until("two report.dropped records", || {
        records(&machine.root, "report.dropped").len() == 2
    });
    for dropped in records(&machine.root, "report.dropped") {
        assert_eq!(dropped["http.response.status_code"], 409);
        assert_eq!(dropped["error.code"], "unknown_instruction");
    }
    // A retry of either would be due by now; a later report shows the
    // outbox kept running.
    machine.advance(seconds(PAST_FIRST_RETRY));
    send_data(&engine, "{}");
    wait_for_received(&engine, "protocol-error", 1);
    let sent = by_message_id(engine.requests("protocol-error"));
    assert_eq!(message_ids(&sent), ["r1", "r2", "r3"]);
    machine.seams.wait_for_count("outbox.answered", 3);
    case.observe("both refused reports were recorded as dropped and neither was sent again past the first retry wait; the later report was acknowledged");
    case.pass();

    let mut case = record.case("a report answered 409 stale_connection");
    answer_fault(
        &engine,
        json!({ "endpoint": "protocol-error", "action": "error", "code": "stale_connection" }),
    );
    send_data(&engine, "not json");
    let sent = wait_for_requests(&engine, "protocol-error", 4);
    assert_eq!(
        (sent[3]["status"].as_i64(), sent[3]["code"].as_str()),
        (Some(409), Some("stale_connection"))
    );
    machine.wait_for("disconnected");
    machine.advance(seconds(PAST_FIRST_RETRY));
    let events = wait_for_requests(&engine, "events", 2);
    assert_eq!(
        events[1]["query"]["previous_connection_id"],
        FIRST_CONNECTION
    );
    assert_eq!(events[1]["query"]["resume"], "false");
    assert_eq!(
        message_ids(&by_message_id(engine.requests("protocol-error"))),
        ["r1", "r2", "r3", "r4"]
    );
    case.observe("the connection ended: the next attempt names it as the predecessor of a new connection, and its refused report was not sent again");
    case.pass();
}

#[test]
fn a_report_whose_ack_was_lost_is_sent_again_byte_for_byte_and_recorded_once() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a protocol error accepted and its ACK dropped");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    answer_fault(
        &engine,
        json!({ "endpoint": "protocol-error", "action": "drop" }),
    );
    send_data(&engine, "not json");
    wait_for_requests(&engine, "protocol-error", 1);
    machine.seams.wait_for_count("outbox.answered", 1);
    machine.advance(seconds(PAST_FIRST_RETRY));
    let sent = wait_for_requests(&engine, "protocol-error", 2);
    assert_eq!(message_ids(&sent), ["r1", "r1"]);
    assert_eq!(sent[0]["body_sha256"], sent[1]["body_sha256"]);
    assert_eq!(sent[1]["status"], 200);
    machine.seams.wait_for_count("outbox.answered", 2);
    assert_eq!(engine.received("protocol-error").len(), 1);
    case.observe("the retry kept message_id r1 and the body's SHA-256, got the replayed ACK, and the server recorded one report");
    case.pass();
}

#[test]
fn each_preparation_instruction_has_one_final_report_identity_across_retries() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!()
        .case("a preparation result retries one identity and a new instruction owns another");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    // The lexical state-root refusal produces a real Session result without
    // launching a directory probe or any restricted task.
    let payload = json!({
        "session_id": "sess_final", "binding_id": "bind_final",
        "cwd": machine.root, "drives": [], "drive_exclude": [], "repositories": [],
    });
    answer_fault(
        &engine,
        json!({ "endpoint": "session-status", "action": "drop" }),
    );
    let first = engine.control(
        "prepare",
        json!({ "instance_id": INSTANCE, "payload": payload }),
    );
    let first_request = first["message_id"].as_str().unwrap();
    wait_for_requests(&engine, "session-status", 1);
    machine.seams.wait_for_count("outbox.answered", 1);
    engine.control(
        "send",
        json!({ "instance_id": INSTANCE, "resend": first_request }),
    );
    let later = engine.control(
        "prepare",
        json!({ "instance_id": INSTANCE, "payload": payload }),
    );
    let later_request = later["message_id"].as_str().unwrap();
    assert_ne!(first_request, later_request);
    wait_for_received(&engine, "session-status", 2);
    // The second instruction is later on the same stream, so its response
    // proves the replay was handled while the first report still lacked ACK.
    machine.seams.wait_for_count("outbox.answered", 2);
    machine.advance(seconds(PAST_FIRST_RETRY));
    let attempts = wait_for_requests(&engine, "session-status", 3);
    assert_eq!(attempts[0]["message_id"], attempts[2]["message_id"]);
    assert_eq!(attempts[0]["body_sha256"], attempts[2]["body_sha256"]);
    assert_eq!(attempts[2]["status"], 200);
    machine.seams.wait_for_count("outbox.answered", 3);
    let accepted = engine.received("session-status");
    let mut final_ids = Vec::new();
    for request in [first_request, later_request] {
        let reports: Vec<_> = accepted
            .iter()
            .map(|entry| &entry["body"])
            .filter(|body| {
                body["connection_id"] == FIRST_CONNECTION
                    && body["request_message_id"] == request
                    && body["session_id"] == "sess_final"
                    && body["binding_id"] == "bind_final"
            })
            .collect();
        let ids: std::collections::BTreeSet<_> = reports
            .iter()
            .map(|body| body["message_id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids.len(),
            1,
            "one final report identity for {request}: {reports:?}"
        );
        assert_eq!(
            reports.len(),
            1,
            "retry stored another final report for {request}"
        );
        for body in reports {
            assert_eq!(body["status"], "failed");
            assert_eq!(body["error"]["code"], "cwd_forbidden");
        }
        final_ids.push(*ids.first().unwrap());
    }
    assert_ne!(final_ids[0], final_ids[1]);
    let sent = engine.requests("session-status");
    assert_eq!(
        sent.len(),
        3,
        "two attempts for the first instruction, one for the second"
    );
    for final_id in final_ids {
        assert!(
            sent.iter()
                .any(|attempt| attempt["message_id"] == final_id && attempt["status"] == 200)
        );
    }
    assert_eq!(
        machine.routed(),
        2,
        "the original instruction retry is not dispatched again"
    );
    let state = engine.state();
    let connection = state["connections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|connection| connection["connection_id"] == FIRST_CONNECTION)
        .unwrap();
    for request in [first_request, later_request] {
        let issued = connection["issued"]
            .as_array()
            .unwrap()
            .iter()
            .find(|instruction| instruction["message_id"] == request)
            .unwrap();
        assert_eq!(issued["reported"], true);
    }
    case.observe("the state-root refusal produced one final message ID per connection/request/Session/binding; a lost ACK retried identical bytes, replaying the instruction added no report, and a new instruction received a distinct result and matching ACK");
    case.pass();
}

#[test]
fn a_protocol_error_names_the_message_it_answers_only_when_its_message_id_can_be_read() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("unparseable messages with and without a readable message_id");
    let engine = one_credential_engine();
    let (_machine, _directory) = online_machine(&mut case, &engine);
    send_data(&engine, "not json");
    send_data(
        &engine,
        r#"{"message_id":"msg_readable","type":"session.close"}"#,
    );
    send_data(&engine, r#"{"message_id":7,"type":"session.close"}"#);
    let reports = by_message_id(wait_for_received(&engine, "protocol-error", 3));
    let correlations: Vec<(&str, Option<&str>)> = reports
        .iter()
        .map(|report| {
            let body = &report["body"];
            assert_eq!(body["error"]["code"], "invalid_message");
            (
                body["message_id"].as_str().unwrap(),
                body.get("request_message_id")
                    .map(|id| id.as_str().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        correlations,
        [("r1", None), ("r2", Some("msg_readable")), ("r3", None)]
    );
    for report in &reports {
        let body = report["body"].to_string();
        assert!(
            !body.contains("not json") && !body.contains("session.close"),
            "{body}"
        );
    }
    case.observe("only the message whose message_id reads as an ID is named as request_message_id; no report carries the invalid content");
    case.pass();
}

#[test]
fn a_session_instruction_delivered_again_is_routed_once_and_its_message_id_with_other_content_is_a_conflict()
 {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a session.close delivered again, reordered, then changed");
    let engine = one_credential_engine();
    let (machine, _directory) = online_machine(&mut case, &engine);
    send_data(
        &engine,
        r#"{"protocol_version":1,"connection_id":"conn_1","message_id":"msg_close","type":"session.close","payload":{"session_id":"sess_a","binding_id":"bind_a"}}"#,
    );
    send_data(
        &engine,
        "{ \"payload\" :\t{ \"binding_id\" : \"bind_a\" , \"session_id\" : \"sess_a\" } , \"type\" : \"session.close\" , \"message_id\" : \"msg_close\" , \"connection_id\" : \"conn_1\" , \"protocol_version\" : 1 }",
    );
    send_data(
        &engine,
        r#"{"protocol_version":1,"connection_id":"conn_1","message_id":"msg_close","type":"session.close","payload":{"session_id":"sess_a","binding_id":"bind_b"}}"#,
    );
    send_data(&engine, "not json");
    // The Connection owner handles events in stream order and numbers its
    // reports in that order; the late close's own `closed` report may take
    // any number between them.
    wait_for_received(&engine, "protocol-error", 2);
    wait_until("the late close's report", || {
        !engine.requests("session-status").is_empty()
    });
    let mut reports = engine.received("protocol-error");
    reports.sort_by_key(|report| {
        report["body"]["message_id"].as_str().unwrap()[1..]
            .parse::<u64>()
            .unwrap()
    });
    let reported: Vec<(&str, Option<&str>)> = reports
        .iter()
        .map(|report| {
            let body = &report["body"];
            (
                body["error"]["code"].as_str().unwrap(),
                body.get("request_message_id")
                    .map(|id| id.as_str().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        reported,
        [
            ("message_conflict", Some("msg_close")),
            ("invalid_message", None)
        ]
    );
    assert_eq!(machine.routed(), 1);
    assert_eq!(engine.requests("session-status").len(), 1);
    case.observe("the close was routed once and its late close sent one closed report; the copy with members reordered and whitespace added started nothing; the changed binding_id gave one message_conflict naming msg_close and was not routed");
    case.pass();
}
