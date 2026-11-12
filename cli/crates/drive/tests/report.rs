//! Durable background rejection delivery through real SQLite settlement.

mod support;

use std::sync::atomic::Ordering;

use drive::report::{REPORT_MAX_BYTES, body};
use drive::transfer::{Binding, Ended};
use protocol::field::AbsolutePath;
use protocol::id::{Id, SafeId};
use protocol::ports::{LONGEST_REPORT_MESSAGE_ID, ReportId, ReportOutcome};
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
async fn a_background_upload_refused_413_gives_its_task_a_limit_report() {
    let mut env = Env::new();
    let session = env
        .session("sess_a", drive::state::Write::default(), true)
        .await;
    // A background write of task 1, after its first sync point.
    outcome(session.sync_point(Some(task(1)), Duration::ZERO, None)).await;
    session.write("big", "large", 1).await;
    session.task_exited(1);
    let message = "a file holds at most 500000000 bytes";
    env.call()
        .await
        .upload_ended(Ended::Decision(support::error_body(
            "file_too_large",
            message,
            &[],
        )));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    let report = env.reports.sent.lock().unwrap()[0].clone();
    assert_eq!(report["task_id"], json!(task(1)));
    let rejected = &report["drive_sync"]["rejected"];
    assert_eq!(rejected.as_array().unwrap().len(), 1);
    assert!(rejected[0]["path"].as_str().unwrap().ends_with("/big"));
    assert_eq!(
        (&rejected[0]["reason"], &rejected[0]["rule"]),
        (&json!("limit"), &json!(message))
    );
    env.no_call(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn a_task_recorded_before_its_state_was_reloaded_is_named_on_the_new_connection() {
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

    // A matching ACK removes its durable notices; the next connection sends nothing.
    env.reports.outcome(1, ReportOutcome::Acked);
    session
        .until(|tables| tables.failure_notices.is_empty())
        .await;
    session.sync.binding_current(binding("conn_c", "bind_c"));
    session.tables().await;
    assert_eq!(env.reports.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn budget_refusal_retries_without_an_edit_and_logs_once_per_episode() {
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
    assert_eq!(session.tables().await.failure_notices.len(), 1);
    assert_eq!(env.reports.attempts.load(Ordering::SeqCst), 1);
    for _ in 0..5 {
        session.tables().await;
    }
    env.advance(Duration::from_millis(999)).await;
    assert_eq!(
        env.reports.attempts.load(Ordering::SeqCst),
        1,
        "no busy retry"
    );
    env.advance(Duration::from_millis(1)).await;
    support::eventually(|| env.reports.attempts.load(Ordering::SeqCst) == 2).await;
    let deferred: Vec<_> = records
        .lines()
        .into_iter()
        .filter(|line| line["event"] == "drive.report_deferred")
        .collect();
    assert_eq!(deferred.len(), 1);
    assert_eq!(deferred[0]["error.code"], "resource_exhausted");
    assert_eq!(
        deferred[0]["error.message"],
        "the report does not fit the retained-report budget"
    );
    assert_eq!(env.reports.reserved.load(Ordering::SeqCst), 0);
    env.reports.full.store(false, Ordering::SeqCst);
    env.advance(Duration::from_secs(1)).await;
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    assert_eq!(
        session.tables().await.failure_notices.len(),
        1,
        "admission is not ACK"
    );
    env.reports.outcome(1, ReportOutcome::Acked);
    session
        .until(|tables| tables.failure_notices.is_empty())
        .await;
    session.sync.close().await;
    let session = env.reopen("sess_a", false).await;
    assert!(session.tables().await.failure_notices.is_empty());
    assert_eq!(env.reports.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn settled_notices_survive_a_reloaded_state_a_disconnect_and_a_missing_or_wrong_ack() {
    let mut env = Env::new();
    let session = env
        .session("sess_a", seed_tasks(&[("a", Some("task_t"))]), false)
        .await;
    env.reports.full.store(true, Ordering::SeqCst);
    env.call().await.answer(rejected_all(1));
    session
        .until(|tables| tables.pending_changes.is_empty())
        .await;
    assert_eq!(session.tables().await.failure_notices.len(), 1);
    session.sync.close().await;
    env.reports.full.store(false, Ordering::SeqCst);
    let session = env.reopen("sess_a", false).await;
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    let listener = env.reports.listeners.lock().unwrap()[&ReportId(1)].clone();
    listener.outcome(ReportId(999), ReportOutcome::Acked);
    assert_eq!(session.tables().await.failure_notices.len(), 1);
    session.sync.binding_gone();
    env.reports.outcome(1, ReportOutcome::Dropped);
    session.tables().await;
    env.advance(Duration::from_secs(5)).await;
    assert_eq!(env.reports.sent.lock().unwrap().len(), 1, "no unbound send");
    session.sync.binding_current(binding("conn_b", "bind_b"));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 2).await;
    let second = env.reports.sent.lock().unwrap()[1].clone();
    assert_eq!(second["connection_id"], "conn_b");
    assert_eq!(second["binding_id"], "bind_b");
    assert_eq!(second["task_id"], "task_t");
    // Loss of an ACK permits a duplicate after restart, without another write.
    session.sync.close().await;
    let session = env.reopen("sess_a", false).await;
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 3).await;
    assert_eq!(
        env.reports.sent.lock().unwrap()[2]["drive_sync"],
        second["drive_sync"]
    );
    env.reports.outcome(3, ReportOutcome::Acked);
    session
        .until(|tables| tables.failure_notices.is_empty())
        .await;
}

#[tokio::test]
async fn a_dropped_attempt_retries_on_the_same_binding() {
    let mut env = Env::new();
    let session = env
        .session("sess_a", seed_tasks(&[("a", None)]), false)
        .await;
    env.call().await.answer(rejected_all(1));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    env.reports.outcome(1, ReportOutcome::Dropped);
    assert_eq!(session.tables().await.failure_notices.len(), 1);
    env.advance(Duration::from_secs(1)).await;
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 2).await;
    env.reports.outcome(2, ReportOutcome::Acked);
    session
        .until(|tables| tables.failure_notices.is_empty())
        .await;
}

#[tokio::test]
async fn removing_an_individual_drive_retains_its_unacknowledged_notice() {
    let mut env = Env::new();
    let session = env
        .session("sess_a", seed_tasks(&[("a", None)]), false)
        .await;
    env.call().await.answer(rejected_all(1));
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    let before = session.tables().await.failure_notices;
    let applied = session
        .sync
        .change_binding(
            drive::binding_change::BindingChange {
                old: vec![drive::mount::Bound {
                    drive_id: SafeId::new(support::DRIVE).unwrap(),
                    read_only: false,
                    exclude: vec![],
                }],
                new: vec![],
                request_message_id: Id::new("remove_drive").unwrap(),
                connection: std::sync::Arc::new(support::Keeper::default()),
            },
            std::future::pending(),
        )
        .await
        .unwrap();
    assert!(applied.lost_changes.is_empty());
    let tables = session.tables().await;
    assert!(tables.files.is_empty());
    assert_eq!(tables.failure_notices, before);
    env.reports.outcome(1, ReportOutcome::Dropped);
    session.tables().await;
    env.advance(Duration::from_secs(1)).await;
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 2).await;
    env.reports.outcome(2, ReportOutcome::Acked);
    session
        .until(|tables| tables.failure_notices.is_empty())
        .await;
}
