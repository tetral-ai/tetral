//! The replay ledger and the Report outbox of `tetral server run` against the
//! reference control server, as separate processes over real HTTP and SSE,
//! each on its own controlled clock: refused reports that are never sent
//! again and a refusal that ends the connection, a report whose ACK was lost
//! sent again byte for byte, a Session instruction delivered again and routed
//! once, and protocol errors that name the message they answer only when its
//! `message_id` can be read. The seam counters these tests read are those
//! `tetral_tests::machine` names.

use std::path::{Path, PathBuf};

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

/// The report files frozen under `outbox/`.
fn frozen_reports(root: &Path) -> Vec<PathBuf> {
    let mut reports = Vec::new();
    for connection in std::fs::read_dir(root.join("outbox")).unwrap() {
        for report in std::fs::read_dir(connection.unwrap().path()).unwrap() {
            reports.push(report.unwrap().path());
        }
    }
    reports
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
fn a_refused_report_is_never_sent_again_and_a_stale_connection_ends_the_connection() {
    let _socket_dir = socket_directory::shared();
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
    // The later report's file goes once the CLI applies its ACK, after the
    // reference server has recorded the report.
    wait_until("no report file left", || {
        frozen_reports(&machine.root).is_empty()
    });
    case.observe("both refused reports were recorded as dropped, their files deleted, and neither was sent again past the first retry wait");

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
    assert!(frozen_reports(&machine.root).is_empty());
    assert_eq!(
        message_ids(&by_message_id(engine.requests("protocol-error"))),
        ["r1", "r2", "r3", "r4"]
    );
    case.observe("the connection ended: the next attempt names it as the predecessor of a new connection, and its refused report was deleted and not sent again");
}

#[test]
fn a_report_whose_ack_was_lost_is_sent_again_byte_for_byte_and_recorded_once() {
    let _socket_dir = socket_directory::shared();
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
    assert_eq!(frozen_reports(&machine.root).len(), 1);
    machine.seams.wait_for_count("outbox.answered", 1);
    machine.advance(seconds(PAST_FIRST_RETRY));
    let sent = wait_for_requests(&engine, "protocol-error", 2);
    assert_eq!(message_ids(&sent), ["r1", "r1"]);
    assert_eq!(sent[0]["body_sha256"], sent[1]["body_sha256"]);
    assert_eq!(sent[1]["status"], 200);
    wait_until("the frozen report deleted at its ACK", || {
        frozen_reports(&machine.root).is_empty()
    });
    assert_eq!(engine.received("protocol-error").len(), 1);
    case.observe("the retry kept message_id r1 and the body's SHA-256, got the replayed ACK, which deleted the frozen file, and the server recorded one report");
}

#[test]
fn a_protocol_error_names_the_message_it_answers_only_when_its_message_id_can_be_read() {
    let _socket_dir = socket_directory::shared();
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
}

#[test]
fn a_session_instruction_delivered_again_is_routed_once_and_its_message_id_with_other_content_is_a_conflict()
 {
    let _socket_dir = socket_directory::shared();
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
    // Events are handled in stream order and reports are numbered in the
    // order they are submitted, so the last event's report is r2 only when
    // exactly one protocol error came before it.
    let reports = by_message_id(wait_for_received(&engine, "protocol-error", 2));
    let reported: Vec<(&str, &str, Option<&str>)> = reports
        .iter()
        .map(|report| {
            let body = &report["body"];
            (
                body["message_id"].as_str().unwrap(),
                body["error"]["code"].as_str().unwrap(),
                body.get("request_message_id")
                    .map(|id| id.as_str().unwrap()),
            )
        })
        .collect();
    assert_eq!(
        reported,
        [
            ("r1", "message_conflict", Some("msg_close")),
            ("r2", "invalid_message", None)
        ]
    );
    assert_eq!(machine.routed(), 1);
    case.observe("the close was routed once; the copy with members reordered and whitespace added started nothing; the changed binding_id gave one message_conflict naming msg_close and was not routed");
}
