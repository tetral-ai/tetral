//! The reports of tool instructions through the Report outbox, between
//! `tetral server run`, whose ready advertises `tool_execution`, and the
//! reference control server as separate processes, each on its own
//! controlled clock: an ACK that names another report and an ACK without
//! `accepted` release nothing, and the report is sent again with its
//! `message_id` and bytes until its matching ACK; a media report read
//! slowly completes within its size-scaled total while heartbeats keep
//! their timing, and one without an answer fails 10 seconds after its last
//! byte. The seam counters these tests read are those
//! `tetral_tests::machine` names.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, answer_fault, bootstrap, one_credential_engine, seconds,
    wait_for_received, wait_for_requests,
};
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, HEARTBEAT_INTERVAL, SESSION, TaskMachine, policy};

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

/// How fast the reference control server reads a throttled report: 65,536
/// bytes for each second of its clock.
const THROTTLE_BYTES_PER_SECOND: u64 = 65_536;

/// A JPEG that an encoder wrote, made `size` bytes by comment segments
/// after its APP0 segment, which ends at byte 20.
fn jpeg(size: usize) -> Vec<u8> {
    let image = include_bytes!("../../crates/protocol/tests/images/baseline.jpg");
    let (head, tail) = image.split_at(20);
    let mut padded = head.to_vec();
    let mut room = size - image.len();
    while room > 0 {
        let data = (room - 4).min(65_533);
        padded.extend_from_slice(&[0xff, 0xfe]);
        padded.extend_from_slice(&u16::try_from(data + 2).unwrap().to_be_bytes());
        padded.resize(padded.len() + data, b' ');
        room -= 4 + data;
    }
    [padded, tail.to_vec()].concat()
}

/// Issues a `view_image` of `path` as `task_id` of the prepared binding.
fn view_image(tasks: &TaskMachine, task_id: &str, path: &str) {
    tasks.engine.control(
        "execute",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": SESSION, "binding_id": BINDING,
            "tool_use_event_id": format!("call_{task_id}"), "task_id": task_id,
            "tool": "view_image", "input": {"path": path},
            "sandbox_policy": policy(&tasks.project, "disabled"),
        }}),
    );
}

/// The accepted report of `task_id`, if any.
fn accepted(tasks: &TaskMachine, task_id: &str) -> Option<Value> {
    tasks
        .engine
        .received("tool-status")
        .into_iter()
        .map(|received| received["body"].clone())
        .find(|body| body["task_id"] == task_id)
}

/// The requests that carried the report `message_id`.
fn sends(tasks: &TaskMachine, message_id: &Value) -> Vec<Value> {
    tasks
        .engine
        .requests("tool-status")
        .into_iter()
        .filter(|request| request["message_id"] == *message_id)
        .collect()
}

#[test]
fn a_throttled_media_report_completes_past_ten_seconds_and_one_without_an_answer_fails_at_ten() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a view_image report of about 1 MiB read at 65,536 bytes a second while both clocks move \
         a second at a time; then two such reports read whole with their answers withheld, one \
         released after 9 seconds, one past 10",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    std::fs::write(tasks.project.join("large.jpg"), jpeg(780_000)).unwrap();

    answer_fault(
        &tasks.engine,
        json!({
            "endpoint": "tool-status", "action": "throttle",
            "bytes_per_second": THROTTLE_BYTES_PER_SECOND,
        }),
    );
    let heartbeats = tasks.engine.received("heartbeat").len();
    view_image(&tasks, "task_throttled", "large.jpg");
    let read = || tasks.engine.state()["throttled"][0]["read"].as_u64();
    wait_until("the throttled read", || read().is_some());
    let mut elapsed = Duration::ZERO;
    let mut read_at_heartbeat = None;
    while accepted(&tasks, "task_throttled").is_none() {
        let grants = seams.count("connection.granted");
        tasks.engine.advance(Duration::from_secs(1));
        elapsed += Duration::from_secs(1);
        let allowed = elapsed.as_secs() * THROTTLE_BYTES_PER_SECOND;
        wait_until("the throttled read of the second", || {
            read() == Some(allowed) || accepted(&tasks, "task_throttled").is_some()
        });
        if elapsed == HEARTBEAT_INTERVAL {
            wait_for_received(&tasks.engine, "heartbeat", heartbeats + 1);
            seams.wait_for_count("connection.granted", grants + 1);
            read_at_heartbeat = read();
        }
        seams.advance(Duration::from_secs(1));
    }
    let report = accepted(&tasks, "task_throttled").unwrap();
    let size = serde_json::to_vec(&report).unwrap().len();
    // Its total is 10 seconds for each 65,536 bytes started: 160 seconds.
    assert!((983_041..=1_048_576).contains(&size), "{size}");
    assert_eq!(report["result"]["status"], "success");
    assert!(elapsed > Duration::from_secs(10), "{elapsed:?}");
    assert_eq!(sends(&tasks, &report["message_id"]).len(), 1);
    assert!(
        read_at_heartbeat.is_some_and(|read| (read as usize) < size),
        "{read_at_heartbeat:?}"
    );

    for (task, stalled, released) in [
        ("task_answered", seconds(9.0), 1),
        ("task_unanswered", seconds(10.0), 2),
    ] {
        answer_fault(
            &tasks.engine,
            json!({"endpoint": "tool-status", "action": "withhold"}),
        );
        let answered = tasks.answers_settled();
        view_image(&tasks, task, "large.jpg");
        wait_until("the withheld report", || accepted(&tasks, task).is_some());
        let message_id = accepted(&tasks, task).unwrap()["message_id"].clone();
        seams.advance(stalled);
        if released == 1 {
            tasks.engine.control("answer", json!({"action": "release"}));
            seams.wait_for_count("outbox.answered", answered + 1);
        } else {
            seams.wait_for_count("outbox.answered", answered + 1);
            seams.advance(seconds(1.5));
            wait_until("the report sent again", || {
                sends(&tasks, &message_id).len() == 2
            });
            tasks.engine.control("answer", json!({"action": "release"}));
        }
        let sent = sends(&tasks, &message_id);
        assert_eq!(sent.len(), released, "{task}: {sent:?}");
        assert!(
            sent.iter()
                .all(|request| request["body_sha256"] == sent[0]["body_sha256"]),
            "{sent:?}"
        );
    }
    case.observe(format!(
        "the {size}-byte report accepted on its one attempt after {elapsed:?} of both clocks, \
         the heartbeat of 15 seconds acknowledged with {read_at_heartbeat:?} bytes read; the \
         answer withheld for 9 seconds after the last byte ended the first attempt, the one \
         withheld for 10 seconds failed it and the report was sent again unchanged"
    ));
}
