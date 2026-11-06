//! Uploads and sync points of Sessions' drive syncs against a stub
//! `DriveTransfer` that holds every upload and commit until the test
//! answers it, with a clock the test moves: the quiet period, the machine's
//! upload slots, the waits of results, a commit in flight at the end of a
//! wait, faults, and the trace context of the requests.

use super::support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use drive::state::Write;
use drive::transfer::Ended;
use drive::upload::QUIET;
use protocol::tool::NotSyncedReason;
use support::RESULT_WAIT;
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

#[tokio::test]
async fn a_result_finishes_before_accepting_later_rewrites_of_its_path() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.task_started(1);
    session.write("a", "first", 1).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    env.call().await.uploaded();
    let first = env.call().await;

    // This generation becomes ready while the first generation's commit
    // is held. It must not extend the original result or increase its count.
    session.write("a", "second", 1).await;
    env.advance(QUIET).await;
    env.call().await.uploaded();
    session
        .until(|tables| tables.pending_changes[&support::key("a")].uploaded)
        .await;
    first.accepted();
    let result = outcome(point).await;
    assert_eq!(result.committed, 1);
    assert!(result.not_synced.is_empty());

    // Reporting the task makes later writes due through ordinary background
    // processing. A second rewrite also cannot change the completed result.
    let second = env.call().await;
    assert_eq!(
        support::summary(second.changes()),
        [("a".to_owned(), "write", Some(sha256(b"first")))]
    );
    session.write("a", "third", 1).await;
    env.advance(QUIET).await;
    env.call().await.uploaded();
    session
        .until(|tables| tables.pending_changes[&support::key("a")].uploaded)
        .await;
    second.accepted();
    let third = env.call().await;
    assert_eq!(
        support::summary(third.changes()),
        [("a".to_owned(), "write", Some(sha256(b"second")))]
    );
    third.accepted();
    session
        .until(|tables| tables.pending_changes.is_empty())
        .await;
    assert_eq!(session.read("a").await.unwrap(), "third");
}

#[tokio::test]
async fn split_commits_freeze_generations_and_exclude_new_background_work() {
    let mut env = Env::new();
    env.commit_batch_size = 1;
    let session = env.session("sess_a", Write::default(), true).await;
    session.task_started(1);
    for path in ["a", "b", "c"] {
        session.write(path, path, 1).await;
    }
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    for _ in 0..3 {
        env.call().await.uploaded();
    }
    let first = env.call().await;
    assert_eq!(first.changes()[0].path(), "a");
    session.write("c", "new c", 1).await;
    session.write("z", "later", 1).await;
    env.advance(QUIET).await;
    for _ in 0..2 {
        env.call().await.uploaded();
    }
    session
        .until(|tables| {
            tables
                .pending_changes
                .values()
                .all(|change| change.uploaded)
        })
        .await;
    first.accepted();
    let second = env.call().await;
    assert_eq!(second.changes()[0].path(), "b");
    second.accepted();
    let result = outcome(point).await;
    assert_eq!(
        result.committed, 2,
        "later generations cannot join split batches"
    );
    assert_eq!(
        reasons(&result),
        [(session.machine_path("c"), NotSyncedReason::Uploading)]
    );
    for path in ["c", "z"] {
        let background = env.call().await;
        assert_eq!(background.changes()[0].path(), path);
        background.accepted();
    }
    session
        .until(|tables| tables.pending_changes.is_empty())
        .await;
    assert_eq!(session.read("c").await.unwrap(), "new c");
    session.sync.close().await;
}

#[tokio::test]
async fn an_undecided_split_commit_lists_sent_and_unsent_batches() {
    for content_missing in [false, true] {
        let mut env = Env::new();
        env.commit_batch_size = 1;
        let session = env.session("sess_a", Write::default(), true).await;
        for path in ["a", "b", "c"] {
            session.write(path, path, 1).await;
        }
        let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
        for _ in 0..3 {
            env.call().await.uploaded();
        }
        let first = env.call().await;
        assert_eq!(first.changes()[0].path(), "a");
        if content_missing {
            first.commit_ended(Ended::Decision(support::error_body(
                "content_missing",
                "missing",
                &[&sha256(b"a")],
            )));
        } else {
            let bytes = serde_json::to_vec(first.changes()).unwrap().len() as u64;
            env.advance(drive::transfer::total(bytes)).await;
            // The request remains live; its answer is still undecided.
            drop(first);
        }
        let result = outcome(point).await;
        assert_eq!(result.committed, 0);
        assert_eq!(
            reasons(&result),
            ["a", "b", "c"].map(|path| (session.machine_path(path), NotSyncedReason::Uploading))
        );
        session.sync.close().await;
    }
}

#[tokio::test]
async fn the_commit_step_includes_background_work_already_due_at_its_start_only() {
    let mut env = Env::new();
    env.commit_batch_size = 1;
    let session = env.session("sess_a", Write::default(), true).await;
    session.task_started(2);
    outcome(session.sync_point(Some(task(2)), Duration::ZERO, None)).await;
    session.write("a", "a", 1).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    let a_upload = env.call().await;
    session.write("b", "b", 2).await;
    env.advance(QUIET).await;
    env.call().await.uploaded();
    session
        .until(|tables| tables.pending_changes[&support::key("b")].uploaded)
        .await;
    a_upload.uploaded();
    let first = env.call().await;
    assert_eq!(first.changes()[0].path(), "a");
    session.write("c", "c", 2).await;
    env.advance(QUIET).await;
    env.call().await.uploaded();
    session
        .until(|tables| tables.pending_changes[&support::key("c")].uploaded)
        .await;
    first.accepted();
    let already_due = env.call().await;
    assert_eq!(already_due.changes()[0].path(), "b");
    assert!(
        !point.is_finished(),
        "the frozen background batch still belongs to this commit step"
    );
    already_due.accepted();
    let result = outcome(point).await;
    assert_eq!(
        result.committed, 1,
        "only this result's covered change is counted"
    );
    let later = env.call().await;
    assert_eq!(later.changes()[0].path(), "c");
    later.accepted();
    session
        .until(|tables| tables.pending_changes.is_empty())
        .await;
    session.sync.close().await;
}

#[tokio::test]
async fn a_result_excludes_another_tasks_unreported_changes() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("mine", "mine", 1).await;
    session.write("other", "other", 2).await;
    session.write("background", "background", NO_TASK).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    let first = env.call().await;
    let second = env.call().await;
    let mut hashes = vec![first.sha256().to_owned(), second.sha256().to_owned()];
    hashes.sort();
    let mut expected = vec![sha256(b"mine"), sha256(b"background")];
    expected.sort();
    assert_eq!(hashes, expected);
    assert_eq!(session.sync.inspect().await.upload_requests, 2);
    first.uploaded();
    second.uploaded();
    let commit = env.call().await;
    let paths: Vec<_> = commit
        .changes()
        .iter()
        .map(|change| change.path())
        .collect();
    assert_eq!(paths, ["background", "mine"]);
    commit.accepted();
    assert_eq!(outcome(point).await.committed, 2);
    let tables = session.tables().await;
    assert_eq!(tables.pending_changes.len(), 1);
    assert!(!tables.pending_changes[&support::key("other")].uploaded);
    let point = session.sync_point(Some(task(2)), RESULT_WAIT, None);
    let put = env.call().await;
    assert_eq!(put.sha256(), sha256(b"other"));
    put.uploaded();
    env.call().await.accepted();
    assert_eq!(outcome(point).await.committed, 1);
}

#[tokio::test]
async fn taskless_notices_are_captured_at_start_once_and_only_by_their_session() {
    let mut env = Env::new();
    let a = env.session("sess_notices_a", Write::default(), true).await;
    let b = env.session("sess_notices_b", Write::default(), true).await;
    let link = |session: &support::Session, name: &'static [u8], pid| {
        let sync = session.sync.clone();
        let drive = session.drive.clone();
        async move {
            sync.symlink(&drive, drive::ops::TOP_INODE, name, b"target", pid)
                .await
                .unwrap();
        }
    };
    link(&a, b"before", None).await;
    link(&a, b"owned", Some(1)).await;
    link(&a, b"other", Some(2)).await;
    link(&b, b"isolated", None).await;
    a.write("held", "ordinary", NO_TASK).await;
    let first = a.sync_point(Some(task(1)), RESULT_WAIT, None);
    let upload = env.call().await; // the owner has started the first point
    link(&a, b"after", None).await;
    let second = outcome(a.sync_point(None, Duration::ZERO, None)).await;
    assert!(reasons(&second).contains(&(a.machine_path("after"), NotSyncedReason::Symlink)));
    assert!(
        !reasons(&second)
            .iter()
            .any(|(path, reason)| *reason == NotSyncedReason::Symlink
                && path != &a.machine_path("after"))
    );
    upload.uploaded();
    env.call().await.accepted();
    let first = outcome(first).await;
    assert_eq!(
        reasons(&first),
        vec![
            (a.machine_path("before"), NotSyncedReason::Symlink),
            (a.machine_path("owned"), NotSyncedReason::Symlink),
        ]
    );
    let other = outcome(a.sync_point(Some(task(2)), Duration::ZERO, None)).await;
    assert_eq!(
        reasons(&other),
        vec![(a.machine_path("other"), NotSyncedReason::Symlink)]
    );
    assert!(
        outcome(a.sync_point(None, Duration::ZERO, None))
            .await
            .not_synced
            .is_empty()
    );
    assert_eq!(
        reasons(&outcome(b.sync_point(None, Duration::ZERO, None)).await),
        vec![(b.machine_path("isolated"), NotSyncedReason::Symlink)]
    );
    assert!(
        outcome(b.sync_point(None, Duration::ZERO, None))
            .await
            .not_synced
            .is_empty()
    );
}

#[tokio::test]
async fn an_operation_retains_its_notice_before_settling_the_last_covered_change() {
    let mut env = Env::new();
    let session = env
        .session("sess_notice_order", Write::default(), true)
        .await;
    session.write("marker", "gitdir: local", 1).await;
    let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
    let upload = env.call().await;
    session
        .sync
        .rename(
            &session.drive,
            drive::ops::TOP_INODE,
            b"marker",
            drive::ops::TOP_INODE,
            b".git",
            Some(1),
        )
        .await
        .unwrap();
    let result = outcome(point).await;
    assert_eq!(
        reasons(&result),
        vec![(
            session.machine_path("").trim_end_matches('/').to_owned(),
            NotSyncedReason::GitRepository
        )]
    );
    assert!(session.tables().await.pending_changes.is_empty());
    drop(upload);
}

#[tokio::test]
async fn a_failed_local_transaction_retains_no_taskless_or_attributed_notice() {
    use sqlx::Connection;
    let env = Env::new();
    let session = env
        .session("sess_notice_failure", Write::default(), true)
        .await;
    let options =
        sqlx::sqlite::SqliteConnectOptions::new().filename(session.directory.join("sync-state.db"));
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_notice BEFORE INSERT ON files BEGIN SELECT RAISE(ABORT, 'notice failure'); END").execute(&mut db).await.unwrap();
    for pid in [None, Some(1)] {
        assert!(
            session
                .sync
                .symlink(
                    &session.drive,
                    drive::ops::TOP_INODE,
                    b"failed",
                    b"target",
                    pid
                )
                .await
                .is_err()
        );
    }
    sqlx::raw_sql("DROP TRIGGER fail_notice")
        .execute(&mut db)
        .await
        .unwrap();
    assert!(
        outcome(session.sync_point(Some(task(1)), Duration::ZERO, None))
            .await
            .not_synced
            .is_empty()
    );
    assert!(
        outcome(session.sync_point(None, Duration::ZERO, None))
            .await
            .not_synced
            .is_empty()
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn platform_renames_preserve_ordinary_pending_and_inflight_bases_without_local_uploads() {
    use drive::ops::TOP_INODE;
    use support::{key, summary};
    for strict in [".nfs.20051025.3080", "._metadata"] {
        for phase in ["pending", "uploading", "committing"] {
            for from_strict in [false, true] {
                let mut env = Env::new();
                env.stub.list("ordinary", "remote-base");
                let session = env.session("sess_platform", Write::default(), true).await;
                session.task_started(1);
                session.write("ordinary", "ordinary-before", 1).await;
                let mut first = None;
                let held = match phase {
                    "pending" => None,
                    "uploading" => {
                        env.advance(QUIET).await;
                        Some(env.call().await)
                    }
                    _ => {
                        first = Some(session.sync_point(Some(task(1)), RESULT_WAIT, None));
                        env.call().await.uploaded();
                        Some(env.call().await)
                    }
                };
                let immutable = held
                    .as_ref()
                    .filter(|_| phase == "committing")
                    .map(|call| serde_json::to_value(call.changes()).unwrap());
                if from_strict {
                    session
                        .write(strict, "local-sidecar-now-ordinary", NO_TASK)
                        .await;
                }
                let (from, to) = if from_strict {
                    (strict, "ordinary")
                } else {
                    ("ordinary", strict)
                };
                session
                    .sync
                    .rename(
                        &session.drive,
                        TOP_INODE,
                        from.as_bytes(),
                        TOP_INODE,
                        to.as_bytes(),
                        Some(1),
                    )
                    .await
                    .unwrap();
                if !from_strict {
                    session.write(strict, "new-local-only-bytes", NO_TASK).await;
                }
                let tables = session.tables().await;
                assert!(
                    !tables.pending_changes.contains_key(&key(strict)),
                    "{strict} {phase} {from_strict}"
                );
                assert_eq!(
                    tables.pending_changes[&key("ordinary")].change,
                    if from_strict { "write" } else { "delete" }
                );
                if phase != "committing" {
                    assert_eq!(
                        tables.pending_changes[&key("ordinary")].base_sha256,
                        Some(sha256(b"remote-base"))
                    );
                }
                if let Some(held) = held {
                    if phase == "committing" {
                        assert_eq!(
                            serde_json::to_value(held.changes()).unwrap(),
                            immutable.unwrap(),
                            "the sent commit remains immutable"
                        );
                        assert_eq!(
                            summary(held.changes()),
                            vec![("ordinary".to_owned(), "write", Some(sha256(b"remote-base")))]
                        );
                        held.accepted();
                        outcome(first.take().unwrap()).await;
                        assert_eq!(
                            session.tables().await.pending_changes[&key("ordinary")].base_sha256,
                            Some(sha256(b"ordinary-before"))
                        );
                    } else {
                        support::eventually(|| env.stub.puts_open.load(Ordering::SeqCst) == 0)
                            .await;
                        drop(held);
                    }
                }
                let mut point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
                let mut saw_ordinary = false;
                loop {
                    tokio::select! {
                        result = &mut point => { result.unwrap(); break; }
                        call = env.call() => match call {
                            call @ Call::Upload { .. } => {
                                assert!(from_strict, "no temporary content upload after ordinary -> local rename");
                                assert_eq!(call.sha256(), sha256(b"local-sidecar-now-ordinary"));
                                call.uploaded();
                            }
                            call @ Call::Commit { .. } => {
                                let expected_base = if phase == "committing" { b"ordinary-before".as_slice() } else { b"remote-base".as_slice() };
                                assert_eq!(summary(call.changes()), vec![("ordinary".to_owned(), if from_strict { "write" } else { "delete" }, Some(sha256(expected_base)))]);
                                saw_ordinary = true;
                                call.accepted();
                            }
                        }
                    }
                }
                assert!(saw_ordinary);
                session
                    .until(|tables| tables.pending_changes.is_empty())
                    .await;
                assert_eq!(
                    session.read(to).await.unwrap(),
                    if from_strict {
                        "local-sidecar-now-ordinary"
                    } else {
                        "new-local-only-bytes"
                    }
                );
                env.no_call(SHORT).await;
                session.sync.close().await;
            }
        }
    }
}
