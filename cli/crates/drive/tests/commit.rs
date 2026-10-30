//! Commits of a Session's drive sync against a stub `DriveTransfer` that
//! holds every upload and commit until the test answers it: splitting,
//! bases, a lost answer across a restart, `409 content_missing`,
//! `413 file_too_large`, the drive winning over a rejected change, and a
//! `429` counted at the upload slots' pressure point.

mod support;

use std::time::Duration;

use drive::ops::TOP_INODE;
use drive::state::Write;
use drive::sync_point::RESULT_WAIT;
use drive::transfer::Ended;
use protocol::drive_transfer::CommitChange;
use protocol::drive_transfer::limits::{COMMIT_MAX_CHANGES, COMMIT_REQUEST_MAX_JSON_BYTES};
use protocol::field::{Nullable, Sha256};
use protocol::tool::{ChangeKind, RejectionReason};
use rustix::io::Errno;
use serde_json::json;
use support::{Env, NO_TASK, error_body, outcome, seed, sha256, summary};

const SHORT: Duration = Duration::from_millis(150);

/// The compact JSON body of a commit of `changes` on the test binding.
fn body_bytes(changes: &[CommitChange]) -> usize {
    serde_json::to_vec(&json!({
        "protocol_version": 1,
        "connection_id": "conn_a",
        "binding_id": "bind_a",
        "changes": changes,
    }))
    .unwrap()
    .len()
}

#[tokio::test]
async fn due_changes_split_at_ten_thousand_deletions_first_and_the_next_commit_follows_the_answer()
{
    let mut env = Env::new();
    let writes: Vec<String> = (0..COMMIT_MAX_CHANGES)
        .map(|index| format!("w{index:05}"))
        .collect();
    let _full = env.session("sess_a", seed(&writes, &[]), false).await;
    let commit = env.call().await;
    assert_eq!(commit.changes().len(), COMMIT_MAX_CHANGES);
    commit.accepted();
    env.no_call(SHORT).await;

    let writes: Vec<String> = (0..COMMIT_MAX_CHANGES - 2)
        .map(|index| format!("w{index:05}"))
        .collect();
    let deletes: Vec<String> = ["z2", "z1", "z3"].map(str::to_owned).to_vec();
    let _over = env.session("sess_b", seed(&writes, &deletes), false).await;
    let first = env.call().await;
    let mut expected: Vec<(String, &str, Option<String>)> = ["z1", "z2", "z3"]
        .iter()
        .map(|path| ((*path).to_owned(), "delete", Some(sha256(b"gone"))))
        .collect();
    expected.extend(writes.iter().map(|path| (path.clone(), "write", None)));
    assert_eq!(summary(first.changes()), expected[..COMMIT_MAX_CHANGES]);
    env.no_call(SHORT).await;
    first.accepted();
    let second = env.call().await;
    assert_eq!(summary(second.changes()), expected[COMMIT_MAX_CHANGES..]);
}

#[tokio::test]
async fn a_commit_ends_where_its_body_would_pass_sixteen_mebibytes() {
    let mut env = Env::new();
    let writes: Vec<String> = (0..300)
        .map(|index| format!("{index:03}{}", "p".repeat(59_997)))
        .collect();
    let _session = env.session("sess_a", seed(&writes, &[]), false).await;
    let first = env.call().await;
    let held = first.changes().to_vec();
    assert!(held.len() < 300);
    assert!(body_bytes(&held) <= COMMIT_REQUEST_MAX_JSON_BYTES);
    let path = writes[held.len()].clone();
    let mut one_more = held.clone();
    one_more.push(CommitChange::Write {
        content_sha256: Sha256::new(sha256(path.as_bytes())).unwrap(),
        path,
        base_sha256: Nullable(None),
        executable: false,
    });
    assert!(body_bytes(&one_more) > COMMIT_REQUEST_MAX_JSON_BYTES);
    first.accepted();
    assert_eq!(env.call().await.changes().len(), 300 - held.len());
}

#[tokio::test]
async fn a_change_during_a_commit_carries_the_committed_base_or_is_discarded_on_rejection() {
    let mut env = Env::new();
    env.stub.list("c", "sea");
    env.stub.list("d", "drive dee");
    let session = env.session("sess_a", Write::default(), true).await;

    // A write during an accepted commit is sent with the committed hash.
    session.write("a", "one", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let commit = env.call().await;
    assert_eq!(summary(commit.changes()), [("a".to_owned(), "write", None)]);
    session.write("a", "two", NO_TASK).await;
    commit.accepted();
    assert_eq!(outcome(point).await.committed, 1);
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let commit = env.call().await;
    assert_eq!(
        summary(commit.changes()),
        [("a".to_owned(), "write", Some(sha256(b"one")))]
    );
    commit.accepted();
    outcome(point).await;
    // The accepted content is adopted into the cache.
    let blob = env.root.path().join("cache/blobs").join(sha256(b"two"));
    assert_eq!(std::fs::read(&blob).unwrap(), b"two");

    // A write during an accepted deletion is sent with a null base.
    session.remove("c", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    let commit = env.call().await;
    assert_eq!(
        summary(commit.changes()),
        [("c".to_owned(), "delete", Some(sha256(b"sea")))]
    );
    session.write("c", "new sea", NO_TASK).await;
    commit.accepted();
    assert_eq!(outcome(point).await.deleted, 1);
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let commit = env.call().await;
    assert_eq!(summary(commit.changes()), [("c".to_owned(), "write", None)]);
    commit.accepted();
    outcome(point).await;

    // A write during a rejected commit is discarded; the file is `current`.
    let current = env.stub.content("drive dee, newer");
    session.write("d", "mine", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let commit = env.call().await;
    session.write("d", "mine again", NO_TASK).await;
    commit.answer(vec![json!({
        "status": "rejected",
        "reason": "conflict",
        "current": {
            "version_id": "v_d2",
            "content_sha256": current,
            "size_bytes": 16,
            "executable": false,
            "writer": { "type": "user_actor", "user_id": "user_x" },
        },
    })]);
    let rejected = outcome(point).await;
    assert_eq!(rejected.rejected.len(), 1);
    let entry = &rejected.rejected[0];
    assert_eq!(entry.path.as_str(), session.machine_path("d"));
    assert_eq!(entry.reason, RejectionReason::Conflict);
    assert_eq!(entry.change, ChangeKind::Write);
    assert_eq!(
        entry.current_version_id.as_ref().map(|id| id.as_str()),
        Some("v_d2")
    );
    assert_eq!(session.read("d").await.unwrap(), "drive dee, newer");
    assert!(
        !session
            .tables()
            .await
            .pending_changes
            .contains_key(&support::key("d"))
    );
}

#[tokio::test]
async fn a_lost_answer_is_sent_again_unchanged_after_a_restart_and_the_rewrite_follows() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "one", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let lost = env.call().await;
    let sent = serde_json::to_vec(lost.changes()).unwrap();
    session.write("a", "two", NO_TASK).await;
    // The answer never arrives; the result stops waiting at the deadline.
    drop(lost);
    env.advance(drive::transfer::total(sent.len() as u64)).await;
    outcome(point).await;
    session.sync.close().await;

    let session = env.reopen("sess_a", true).await;
    let resent = env.call().await;
    assert_eq!(serde_json::to_vec(resent.changes()).unwrap(), sent);
    resent.accepted();
    // After the start, the rewrite uploads and commits on its own.
    env.call().await.uploaded();
    let rewrite = env.call().await;
    assert_eq!(
        summary(rewrite.changes()),
        [("a".to_owned(), "write", Some(sha256(b"one")))]
    );
    rewrite.accepted();
    session
        .until(|tables| tables.pending_changes.is_empty() && tables.inflight_commits.is_empty())
        .await;
}

#[tokio::test]
async fn content_missing_uploads_the_named_content_again_and_resends_the_rest_after_the_result() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "one", NO_TASK).await;
    session.write("b", "bee", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    env.call().await.uploaded();
    let commit = env.call().await;
    assert_eq!(commit.changes().len(), 2);
    let missing = sha256(b"one");
    commit.commit_ended(Ended::Decision(error_body(
        "content_missing",
        "content is missing",
        &[&missing],
    )));
    let listed = outcome(point).await;
    let mut uploading: Vec<&str> = listed
        .not_synced
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    uploading.sort();
    assert_eq!(
        uploading,
        [session.machine_path("a"), session.machine_path("b")]
    );
    let mut seen = [env.call().await, env.call().await];
    seen.sort_by_key(|call| matches!(call, support::Call::Commit { .. }));
    assert_eq!(seen[0].sha256(), missing, "the named content uploads again");
    assert_eq!(
        summary(seen[1].changes()),
        [("b".to_owned(), "write", None)]
    );
}

#[tokio::test]
async fn file_too_large_is_a_limit_rejection_and_removes_a_new_file() {
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("big", "large", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    let message = "a file holds at most 500000000 bytes";
    env.call()
        .await
        .upload_ended(Ended::Decision(error_body("file_too_large", message, &[])));
    let rejected = outcome(point).await;
    assert_eq!(rejected.rejected.len(), 1);
    let entry = &rejected.rejected[0];
    assert_eq!(entry.reason, RejectionReason::Limit);
    assert_eq!(entry.rule.as_ref().map(|rule| rule.as_str()), Some(message));
    assert_eq!(entry.path.as_str(), session.machine_path("big"));
    assert_eq!(session.read("big").await, Err(Errno::NOENT));
    assert!(session.tables().await.pending_changes.is_empty());
    env.no_call(SHORT).await;
}

#[tokio::test]
async fn a_rejection_after_a_newer_record_keeps_the_newer_version() {
    let mut env = Env::new();
    env.stub.list("a", "first");
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "mine", NO_TASK).await;
    let point = session.sync_point(None, RESULT_WAIT, None);
    env.call().await.uploaded();
    let commit = env.call().await;
    let newer = env.stub.content("newer");
    env.stub.records.lock().unwrap().push(json!({
        "seq": 5, "index": 0, "path": "a", "operation": "modified",
        "version_id": "v5", "content_sha256": newer,
        "size_bytes": 5, "executable": false,
    }));
    session.sync.drive_changed(session.drive.clone(), 5);
    session.caught_up().await;
    let older = env.stub.content("older");
    commit.answer(vec![json!({
        "status": "rejected",
        "reason": "conflict",
        "current": {
            "version_id": "v4",
            "content_sha256": older,
            "size_bytes": 5,
            "executable": false,
            "writer": { "type": "user_actor", "user_id": "user_x" },
        },
    })]);
    assert_eq!(outcome(point).await.rejected.len(), 1);
    assert_eq!(session.read("a").await.unwrap(), "newer");
    let row = session.tables().await.files[&support::key("a")].clone();
    assert_eq!(row.version_id.as_deref(), Some("v5"));
}

#[tokio::test]
async fn an_upload_answered_429_counts_one_rate_limited_rejection_in_the_local_metrics() {
    let rejected = || {
        observe::registry()
            .counter(
                "tetral_uploads_rejected_total",
                &[("cause", "rate_limited")],
            )
            .get()
    };
    let before = rejected();
    let mut env = Env::new();
    env.stub.list("f", "old");
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("f", "new", 1).await;
    session.task_exited(1);
    let support::Call::Upload { rate_limited, .. } = env.call().await else {
        panic!("an upload")
    };
    // The transfer client calls it at the 429, before it waits to retry.
    rate_limited.expect("the upload's pressure point is told")();
    tokio::time::timeout(Duration::from_secs(10), async {
        while rejected() == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("counted within 10 s");
    assert_eq!(rejected(), before + 1);
    assert!(observe::render().contains(&format!(
        "tetral_uploads_rejected_total{{cause=\"rate_limited\"}} {}",
        before + 1
    )));
}

#[tokio::test]
async fn changes_pending_before_their_folder_became_a_repository_keep_recording_and_are_committed()
{
    let mut env = Env::new();
    let session = env.session("sess_a", Write::default(), true).await;
    session.write("a", "one", NO_TASK).await;
    session.write("b", "new", NO_TASK).await;
    let drive = session.drive.clone();
    session
        .sync
        .mkdir(&drive, TOP_INODE, b".git", Some(NO_TASK))
        .await
        .unwrap();
    // The removal of the new file `b` settles its pending write, and a
    // later write of `a` records on its own.
    session.remove("b", NO_TASK).await;
    session.write("a", "two", NO_TASK).await;
    let pending: Vec<String> = session
        .tables()
        .await
        .pending_changes
        .into_keys()
        .map(|(_, path)| path)
        .collect();
    assert_eq!(pending, ["a"]);
    let point = session.sync_point(None, RESULT_WAIT, None);
    let upload = env.call().await;
    assert_eq!(upload.sha256(), sha256(b"two"));
    upload.uploaded();
    let commit = env.call().await;
    assert_eq!(summary(commit.changes()), [("a".to_owned(), "write", None)]);
    commit.accepted();
    assert_eq!(outcome(point).await.committed, 1);
    assert!(session.tables().await.pending_changes.is_empty());
}
