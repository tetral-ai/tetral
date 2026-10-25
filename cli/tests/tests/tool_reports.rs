//! The reports of tool instructions through the Report outbox, between
//! `tetral server run`, whose ready advertises `tool_execution`, and the
//! reference control server as separate processes, each on its own
//! controlled clock: an ACK that names another report and an ACK without
//! `accepted` release nothing, and the report is sent again with its
//! `message_id` and bytes until its matching ACK. The seam counters these
//! tests read are those `tetral_tests::machine` names.

use std::path::{Path, PathBuf};

use serde_json::json;
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, answer_fault, bootstrap, one_credential_engine, seconds,
    wait_for_received, wait_for_requests,
};
use tetral_tests::socket_directory;

/// Past the first and the second wait of a report's retry backoff: 1 and 2
/// seconds, each with at most 20% of jitter.
const PAST_RETRY: [f64; 2] = [1.5, 2.5];

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

#[test]
fn an_ack_naming_another_report_or_without_accepted_releases_nothing_and_the_report_is_sent_again_unchanged()
 {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "the rejection of a tool.execute answered with an ACK for another message_id, then one without accepted",
    );
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let machine = Machine::start_with_tools(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    for action in ["wrong_identity", "without_accepted"] {
        answer_fault(
            &engine,
            json!({ "endpoint": "tool-status", "action": action }),
        );
    }
    // A Session this machine never prepared has no binding, so the
    // instruction is rejected and the rejection is the one report.
    let issued = engine.control(
        "execute",
        json!({
            "instance_id": INSTANCE,
            "payload": {
                "session_id": "sess_unprepared", "binding_id": "bind_unprepared",
                "tool_use_event_id": "call_1", "task_id": "task_1",
                "tool": "exec_command", "input": {"cmd": "true"},
                "sandbox_policy": {
                    "schema_version": 1, "filesystem": {"rules": []}, "network": {"mode": "disabled"},
                },
            },
        }),
    );
    wait_for_requests(&engine, "tool-status", 1);
    for (answered, past) in [(1, PAST_RETRY[0]), (2, PAST_RETRY[1])] {
        machine.seams.wait_for_count("outbox.answered", answered);
        assert_eq!(
            frozen_reports(&machine.root).len(),
            1,
            "after answer {answered}"
        );
        machine.advance(seconds(past));
    }
    let sent = wait_for_requests(&engine, "tool-status", 3);
    let sent: Vec<(&str, &str, i64)> = sent
        .iter()
        .map(|request| {
            (
                request["message_id"].as_str().unwrap(),
                request["body_sha256"].as_str().unwrap(),
                request["status"].as_i64().unwrap(),
            )
        })
        .collect();
    assert!(
        sent.iter()
            .all(|request| (request.0, request.1, request.2) == ("r1", sent[0].1, 200)),
        "{sent:?}"
    );
    wait_until("the frozen report deleted at its matching ACK", || {
        frozen_reports(&machine.root).is_empty()
    });
    let received = wait_for_received(&engine, "tool-status", 1);
    assert_eq!(received.len(), 1);
    let report = &received[0]["body"];
    assert_eq!(
        (
            &report["kind"],
            &report["phase"],
            &report["error"]["code"],
            &report["request_message_id"],
            &report["task_id"],
        ),
        (
            &json!("execution"),
            &json!("rejected"),
            &json!("binding_unavailable"),
            &issued["message_id"],
            &json!("task_1"),
        )
    );
    case.observe("the rejection was sent three times as r1 with one body SHA-256, each answered 200; the ACK for another message_id and the ACK without accepted left the frozen file, the matching ACK deleted it");
}
