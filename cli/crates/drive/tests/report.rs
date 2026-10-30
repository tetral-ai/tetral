//! Drive sync reports of the rejections no sync point carries: the size of
//! a report, one report per recorded task, a task named after a restart on
//! a new connection, nothing sent again on another connection, and a
//! report the ordinary budget refuses.

mod support;

use std::sync::atomic::Ordering;

use drive::report::{REPORT_MAX_BYTES, body};
use drive::transfer::Binding;
use protocol::field::AbsolutePath;
use protocol::id::{Id, SafeId};
use protocol::ports::LONGEST_REPORT_MESSAGE_ID;
use protocol::tool::{ChangeKind, DriveRejection, DriveSyncReport, RejectionReason};
use serde_json::{Value, json};
use std::time::Duration;

use support::{Env, Records, outcome, seed, task};

fn binding(connection: &str, binding: &str) -> Binding {
    Binding {
        connection_id: Id::new(connection).unwrap(),
        binding_id: Id::new(binding).unwrap(),
    }
}

fn rejected_all(changes: usize) -> Vec<Value> {
    (0..changes)
        .map(|_| json!({ "status": "rejected", "reason": "path_rule", "rule": "no such names" }))
        .collect()
}

/// Uploaded writes at `paths`, recorded with `tasks`.
fn seed_tasks(paths: &[(&str, Option<&str>)]) -> drive::state::Write {
    let names: Vec<String> = paths.iter().map(|(path, _)| (*path).to_owned()).collect();
    let mut write = seed(&names, &[]);
    for (change, (_, task)) in write.pending.iter_mut().zip(paths) {
        change.task_id = task.map(str::to_owned);
    }
    write
}

fn task_ids(reports: &[Value]) -> Vec<Value> {
    let mut ids: Vec<Value> = reports
        .iter()
        .map(|report| report.get("task_id").cloned().unwrap_or(Value::Null))
        .collect();
    ids.sort_by_key(|id| id.to_string());
    ids
}

#[test]
fn a_group_of_120_long_rejections_fits_131072_bytes_with_its_total() {
    let path = |index: usize| {
        let path = format!("/state/sessions/sess_a/drives/drive_a/{index:03}");
        format!("{path}{}", "p".repeat(1024 - path.len()))
    };
    let rejected = (0..120)
        .map(|index| DriveRejection {
            path: AbsolutePath::new(path(index)).unwrap(),
            change: ChangeKind::Write,
            reason: RejectionReason::PathRule,
            rule: protocol::field::NonEmptyString::new("no such names").ok(),
            current_version_id: None,
            current_writer: None,
        })
        .collect();
    let build = body(
        &binding("conn_a", "bind_a"),
        &SafeId::new("sess_a").unwrap(),
        Some(Id::new("task_a").unwrap()),
        rejected,
    );
    let message = Id::new(LONGEST_REPORT_MESSAGE_ID).unwrap();
    let report = build(Id::new("conn_a").unwrap(), message);
    let bytes = protocol::encode(&report);
    assert!(bytes.len() <= REPORT_MAX_BYTES, "{}", bytes.len());
    let decoded: DriveSyncReport = protocol::decode(&bytes).expect("a valid report");
    assert!(serde_json::to_vec(&decoded.drive_sync).unwrap().len() <= 65_536);
    assert!(decoded.drive_sync.rejected.len() <= 100);
    assert_eq!(
        decoded.drive_sync.rejected_total.map(|total| total.get()),
        Some(120)
    );
}

#[tokio::test]
async fn one_commit_answer_gives_one_report_per_recorded_task() {
    let mut env = Env::new();
    let paths = [("a", Some("task_a")), ("b", Some("task_b")), ("c", None)];
    let _session = env.session("sess_a", seed_tasks(&paths), false).await;
    let commit = env.call().await;
    commit.answer(rejected_all(3));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 3).await;
    let reports = env.reports.sent.lock().unwrap().clone();
    assert_eq!(
        task_ids(&reports),
        [json!("task_a"), json!("task_b"), Value::Null]
    );
    for report in &reports {
        assert_eq!(
            report["drive_sync"]["rejected"].as_array().unwrap().len(),
            1
        );
        assert_eq!(report["drive_sync"]["committed"], 0);
    }
}

#[tokio::test]
async fn a_task_recorded_before_a_restart_is_named_on_the_new_connection() {
    let mut env = Env::new();
    let session = env
        .session("sess_a", seed_tasks(&[("a", Some("task_t"))]), false)
        .await;
    drop(env.call().await);
    session.sync.close().await;
    let session = env.reopen("sess_a", false).await;
    let resent = env.call().await;
    // Without a current binding the report waits for the next one.
    session.sync.binding_gone();
    session.tables().await;
    resent.answer(rejected_all(1));
    session
        .until(|tables| tables.pending_changes.is_empty())
        .await;
    assert!(env.reports.sent.lock().unwrap().is_empty());
    session.sync.binding_current(binding("conn_b", "bind_b"));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    let report = env.reports.sent.lock().unwrap()[0].clone();
    assert_eq!(report["task_id"], "task_t");
    assert_eq!(report["connection_id"], "conn_b");
    assert_eq!(report["binding_id"], "bind_b");

    // The report belongs to its connection: the next one sends nothing.
    session.sync.binding_current(binding("conn_c", "bind_c"));
    session.tables().await;
    assert_eq!(env.reports.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_report_the_ordinary_budget_refuses_is_dropped_and_logged() {
    let mut env = Env::new();
    let session = env
        .session("sess_a", seed_tasks(&[("a", None)]), true)
        .await;
    let records = Records::start();
    env.reports.full.store(true, Ordering::SeqCst);
    env.call().await.answer(rejected_all(1));
    session
        .until(|tables| tables.pending_changes.is_empty())
        .await;
    let dropped = records
        .lines()
        .into_iter()
        .find(|line| line["event"] == "report.dropped")
        .unwrap_or_else(|| {
            panic!(
                "a report.dropped record: {:?} {:?}",
                records.lines(),
                env.reports.sent.lock().unwrap()
            )
        });
    assert_eq!(dropped["error.code"], "resource_exhausted");
    assert_eq!(
        env.reports.reserved.load(Ordering::SeqCst),
        0,
        "the reserve stays free"
    );
    assert!(env.reports.sent.lock().unwrap().is_empty());

    env.reports.full.store(false, Ordering::SeqCst);
    // A background write of task 1, after its first sync point.
    outcome(session.sync_point(Some(task(1)), Duration::ZERO, None)).await;
    session.write("b", "bee", 1).await;
    session.task_exited(1);
    env.call().await.uploaded();
    env.call().await.answer(rejected_all(1));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    let rejected = env.reports.sent.lock().unwrap()[0]["drive_sync"]["rejected"].clone();
    assert_eq!(rejected.as_array().unwrap().len(), 1);
    assert!(rejected[0]["path"].as_str().unwrap().ends_with("/b"));
}
