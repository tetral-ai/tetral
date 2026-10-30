//! Uploads and sync points of Sessions' drive syncs against a stub
//! `DriveTransfer` that holds every upload and commit until the test
//! answers it, with a clock the test moves: the quiet period, the machine's
//! upload slots, the waits of results, a commit in flight at the end of a
//! wait, faults, and the trace context of the requests.

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use drive::state::Write;
use drive::sync_point::RESULT_WAIT;
use drive::transfer::Ended;
use drive::upload::QUIET;
use protocol::tool::NotSyncedReason;
use support::{Call, Env, NO_TASK, outcome, sha256, task};

const SHORT: Duration = Duration::from_millis(150);

fn reasons(outcome: &drive::SyncOutcome) -> Vec<(String, NotSyncedReason)> {
    outcome
        .not_synced
        .iter()
        .map(|entry| (entry.path.as_str().to_owned(), entry.reason))
        .collect()
}

#[tokio::test]
async fn an_upload_starts_two_seconds_after_the_last_write_or_at_the_task_exit_and_a_write_cancels_it()
 {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.task_started(1);
    session.write("a", "on", 1).await;
    // A second write starts the quiet period again.
    env.advance(Duration::from_millis(1_500)).await;
    session.write("a", "one", 1).await;
    env.advance(QUIET - Duration::from_millis(100)).await;
    env.no_call(SHORT).await;
    env.advance(Duration::from_millis(100)).await;
    let put = env.call().await;
    assert_eq!(put.sha256(), sha256(b"one"));

    // A write during the PUT cancels it; the next PUT names the new hash.
    session.write("a", "two", 1).await;
    support::eventually(|| env.stub.puts_open.load(Ordering::SeqCst) == 0).await;
    drop(put);
    env.advance(QUIET).await;
    assert_eq!(env.call().await.sha256(), sha256(b"two"));

    // The writing task's exit starts the upload at once.
    session.task_started(2);
    session.write("b", "bee", 2).await;
    env.no_call(SHORT).await;
    session.task_exited(2);
    assert_eq!(env.call().await.sha256(), sha256(b"bee"));
}

#[tokio::test]
async fn three_sessions_share_eight_upload_slots_in_the_order_their_uploads_became_ready() {
    let mut env = Env::new();
    let a = env.session("sess_a", Write::default(), true).await;
    let b = env.session("sess_b", Write::default(), true).await;
    let c = env.session("sess_c", Write::default(), true).await;
    for (session, count, name) in [(&a, 20, "a"), (&b, 1, "b"), (&c, 5, "c")] {
        for index in 0..count {
            session
                .write(&format!("{name}{index:02}"), &format!("{name}{index}"), 1)
                .await;
        }
    }
    // A's exit asks for all its slots at once: once its first PUT comes,
    // the rest of A's uploads wait before B's and C's.
    a.task_exited(1);
    let mut held = vec![env.call().await];
    b.task_exited(1);
    c.task_exited(1);
    for _ in 1..8 {
        held.push(env.call().await);
    }
    env.no_call(SHORT).await;
    assert!(held.iter().all(|put| put.session() == "sess_a"));

    // A write to one of A's waiting files takes it out of the queue, and a
    // write during one of A's held PUTs cancels it and frees its slot.
    a.write("a19", "a19 again", 1).await;
    a.write("a00", "a00 again", 1).await;
    let cancelled = held
        .iter()
        .position(|put| put.sha256() == sha256(b"a0"))
        .expect("a00's PUT is held");
    held.remove(cancelled);
    let mut order = Vec::new();
    let next = env.call().await;
    order.push((next.session().to_owned(), next.sha256().to_owned()));
    held.push(next);
    assert_eq!(env.stub.puts_open.load(Ordering::SeqCst), 8);
    // 11 of A, 1 of B and 5 of C were waiting.
    while order.len() < 17 {
        held.remove(0).uploaded();
        let next = env.call().await;
        order.push((next.session().to_owned(), next.sha256().to_owned()));
        held.push(next);
        assert!(env.stub.puts_most.load(Ordering::SeqCst) <= 8);
    }
    for put in held {
        put.uploaded();
    }
    env.no_call(SHORT).await;
    let b_at = order
        .iter()
        .position(|(session, _)| session == "sess_b")
        .unwrap();
    let a_ninth = order
        .iter()
        .position(|(session, _)| session == "sess_a")
        .unwrap();
    assert!(b_at < a_ninth, "B's PUT starts before A's ninth: {order:?}");
    for old in [sha256(b"a19"), sha256(b"a0")] {
        assert!(
            order.iter().all(|(_, hash)| *hash != old),
            "no PUT of an old hash"
        );
    }
}

#[tokio::test]
async fn an_exited_command_waits_thirty_seconds_and_a_running_result_waits_for_nothing() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "one", 1).await;
    session.task_exited(1);
    let _put = env.call().await;

    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    env.advance(RESULT_WAIT - Duration::from_secs(1)).await;
    assert!(!point.is_finished(), "no result before 30 s");
    env.advance(Duration::from_secs(1)).await;
    let first = outcome(point).await;
    assert_eq!(
        reasons(&first),
        [(session.machine_path("a"), NotSyncedReason::Uploading)]
    );

    let running = outcome(session.sync_point(Some(task(1)), Duration::ZERO, None)).await;
    assert!(running.not_synced.is_empty(), "{running:?}");

    session.write("b", "bee", 2).await;
    let point = session.sync_point(Some(task(2)), Duration::ZERO, None);
    let put = env.call().await;
    assert_eq!(put.sha256(), sha256(b"bee"));
    let running = outcome(point).await;
    assert_eq!(
        reasons(&running),
        [(session.machine_path("b"), NotSyncedReason::Uploading)]
    );
}

#[tokio::test]
async fn a_commit_in_flight_when_the_wait_ends_lists_the_covered_changes_and_commits_them_after() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "one", 1).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    env.call().await.uploaded();
    let first = env.call().await;
    assert_eq!(
        support::summary(first.changes()),
        [("a".to_owned(), "write", None)]
    );
    // The first result stops waiting for the answer at its deadline.
    env.advance(drive::transfer::total(1)).await;
    let _ = outcome(point).await;

    // Without a task, only the listing makes it due after the answer.
    session.write("b", "bee", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let second = outcome(point).await;
    assert_eq!(
        reasons(&second),
        [(session.machine_path("b"), NotSyncedReason::Uploading)]
    );
    first.accepted();
    let next = env.call().await;
    assert_eq!(
        support::summary(next.changes()),
        [("b".to_owned(), "write", None)]
    );
}

#[tokio::test]
async fn a_sync_point_commits_its_covered_changes_together_once_its_wait_ends() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    // After its first sync point, task 1's changes are background writes.
    outcome(session.sync_point(Some(task(1)), Duration::ZERO, None)).await;
    session.write("a", "one", 1).await;
    session.write("b", "bee", 1).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    let mut puts = vec![env.call().await, env.call().await];
    puts.pop().unwrap().uploaded();
    env.no_call(SHORT).await;
    puts.pop().unwrap().uploaded();
    let commit = env.call().await;
    assert_eq!(commit.changes().len(), 2);
    commit.accepted();
    assert_eq!(outcome(point).await.committed, 2);
}

#[tokio::test]
async fn a_fault_is_listed_upload_pending_and_tried_again_only_at_the_next_covering_sync_point() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "one", 1).await;
    let records = support::Records::start();
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    env.call().await.upload_ended(Ended::Permanent {
        status: 400,
        code: Some("invalid_message"),
    });
    let faulted = outcome(point).await;
    let fault = records
        .lines()
        .into_iter()
        .find(|line| line["event"] == "drive.upload_fault")
        .unwrap_or_else(|| panic!("a drive.upload_fault record: {:?}", records.lines()));
    assert_eq!(fault["level"], "WARN", "{fault}");
    assert_eq!(
        fault["drive_transfer.operation"], "upload_content",
        "{fault}"
    );
    assert_eq!(fault["http.response.status_code"], 400, "{fault}");
    assert_eq!(fault["error.code"], "invalid_message", "{fault}");
    assert_eq!(
        reasons(&faulted),
        [(session.machine_path("a"), NotSyncedReason::UploadPending)]
    );
    let tables = session.tables().await;
    let row = &tables.pending_changes[&support::key("a")];
    assert_eq!(row.fault_code.as_deref(), Some("invalid_message"));
    // No trigger but a covering sync point tries it again.
    session.task_started(1);
    session.task_exited(1);
    env.advance(QUIET * 2).await;
    env.no_call(SHORT).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    env.call().await.accepted();
    assert_eq!(outcome(point).await.committed, 1);

    // Listed `uploading`, then faulted: the next sync point covers it and
    // lists it `upload_pending`.
    session.write("b", "bee", NO_TASK).await;
    let point = session.sync_point(None, Duration::ZERO, None);
    let put = env.call().await;
    let listed = outcome(point).await;
    assert_eq!(
        reasons(&listed),
        [(session.machine_path("b"), NotSyncedReason::Uploading)]
    );
    put.upload_ended(Ended::Permanent {
        status: 400,
        code: Some("invalid_message"),
    });
    session
        .until(|tables| {
            tables.pending_changes[&support::key("b")]
                .fault_code
                .is_some()
        })
        .await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.upload_ended(Ended::Permanent {
        status: 400,
        code: Some("invalid_message"),
    });
    assert_eq!(
        reasons(&outcome(point).await),
        [(session.machine_path("b"), NotSyncedReason::UploadPending)]
    );
}

#[tokio::test]
async fn a_sync_point_s_requests_carry_its_traceparent_and_a_quiet_period_upload_none() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    session.write("a", "one", 1).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, Some(traceparent));
    let put = env.call().await;
    assert_eq!(put.traceparent(), Some(traceparent));
    put.uploaded();
    let commit = env.call().await;
    assert_eq!(commit.traceparent(), Some(traceparent));
    commit.accepted();
    assert_eq!(outcome(point).await.committed, 1);

    session.task_started(2);
    session.write("b", "bee", 2).await;
    env.advance(QUIET).await;
    let put = env.call().await;
    assert!(matches!(put, Call::Upload { .. }));
    assert_eq!(put.traceparent(), None);
}

#[tokio::test]
async fn a_covered_write_removed_during_the_wait_ends_the_wait() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "gone soon", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    let _held = env.call().await;
    session.remove("a", NO_TASK).await;
    let result = tokio::time::timeout(Duration::from_secs(10), outcome(point))
        .await
        .expect("the result before the wait passes");
    assert!(result.not_synced.is_empty(), "{:?}", result.not_synced);
}
