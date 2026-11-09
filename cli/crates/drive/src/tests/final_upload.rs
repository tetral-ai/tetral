//! Final uploads: the end at the end time however late it started, what
//! abandoning removes and restores, a commit answered before the end and
//! the rejections it reports, the end once every covered change is decided,
//! keep mode, a released one, and the result waits a Session removal ends,
//! of its own drives only, at its start and for a sync point that reaches
//! the owner while it runs.

use super::support;

use std::sync::Arc;
use std::time::Duration;

use drive::{FinalMode, FinalUpload, FinalUploadRequest};
use protocol::connection::LostChange;
use protocol::drive_transfer::CommitChange;
use protocol::id::Id;
use protocol::tool::NotSyncedReason;
use serde_json::json;

use drive::state::Write;
use drive::transfer::Ended;
use support::{Call, Env, Keeper, RESULT_WAIT, Session, key, outcome, seed, task};

fn abandon(keeper: &Arc<Keeper>) -> FinalMode {
    FinalMode::Abandon {
        request_message_id: Id::new("close_a").unwrap(),
        connection: keeper.clone(),
    }
}

fn start(session: &Session, end: Duration, mode: FinalMode) -> FinalUpload {
    session.sync.final_upload(FinalUploadRequest {
        session_close: false,
        drives: vec![session.drive.clone()],
        end,
        mode,
    })
}

async fn ended(mut upload: FinalUpload) -> Vec<LostChange> {
    tokio::time::timeout(Duration::from_secs(10), upload.ended())
        .await
        .expect("the final upload ends within 10 s")
}

/// A Session whose drive lists `f` and whose seeded `g` has an uploaded
/// write; the commit of `g` is held.
async fn with_held_commit(env: &mut Env) -> (Session, Call) {
    env.stub.list("f", "old");
    let session = env
        .session("sess_a", seed(&["g".to_owned()], &[]), true)
        .await;
    let commit = env.call().await;
    assert_eq!(support::summary(commit.changes())[0].0, "g");
    (session, commit)
}

#[tokio::test]
async fn at_the_end_time_held_changes_are_abandoned_and_their_paths_take_the_drive_version() {
    let mut env = Env::new();
    let (session, commit) = with_held_commit(&mut env).await;
    session.write("f", "new", 1).await;
    let content = session.directory.join("content").join(support::DRIVE);
    assert!(std::fs::read_dir(&content).unwrap().count() > 0);
    // The tasks exit 20 seconds after the close was admitted.
    env.advance(Duration::from_secs(20)).await;
    let keeper = Arc::new(Keeper::default());
    let upload = start(&session, Duration::from_secs(30), abandon(&keeper));
    let put = env.call().await;
    assert_eq!(put.sha256(), support::sha256(b"new"));

    env.advance(Duration::from_secs(9)).await;
    assert!(keeper.0.lock().unwrap().is_empty());
    // Nothing is removed until lost-changes.json holds the entries.
    *keeper.1.lock().unwrap() = Some(Vec::new());
    env.advance(Duration::from_secs(1)).await;
    support::eventually(|| !keeper.0.lock().unwrap().is_empty()).await;
    // The keeper owns the completion callback: the Drive owner cannot
    // pass this boundary and remove bytes until that callback is released.
    assert_eq!(keeper.1.lock().unwrap().as_ref().unwrap().len(), 1);
    assert!(std::fs::read_dir(&content).unwrap().count() > 0);
    for kept in keeper.1.lock().unwrap().take().unwrap() {
        kept();
    }
    let lost = ended(upload).await;
    let Call::Upload { answer: put, .. } = put else {
        unreachable!()
    };
    let Call::Commit { answer: held, .. } = commit else {
        unreachable!()
    };
    assert!(
        put.is_closed() && held.is_closed(),
        "both requests cancelled"
    );
    let expected = json!([{
        "request_message_id": "close_a", "drive_id": support::DRIVE,
        "paths": ["f", "g"], "paths_total": 2,
    }]);
    assert_eq!(serde_json::to_value(&lost).unwrap(), expected);
    assert_eq!(
        serde_json::to_value(&*keeper.0.lock().unwrap()).unwrap(),
        expected
    );

    let tables = session.tables().await;
    assert!(tables.pending_changes.is_empty());
    assert!(tables.inflight_commits.is_empty());
    assert!(!tables.files.contains_key(&key("g")));
    assert_eq!(std::fs::read_dir(&content).unwrap().count(), 0);
    assert_eq!(session.read("f").await.unwrap(), "old");
    // Nothing of the drive is uploaded again.
    let before = session.sync.inspect().await;
    session.write("f", "newer", 2).await;
    session.task_exited(2);
    let after = session.sync.inspect().await;
    assert_eq!(
        (after.upload_requests, after.commit_requests),
        (before.upload_requests, before.commit_requests)
    );
    // Until it is mounted again.
    session.sync.mounted(session.drive.clone(), Vec::new());
    session.caught_up().await;
    session.write("f", "newest", 3).await;
    session.task_exited(3);
    assert_eq!(env.call().await.sha256(), support::sha256(b"newest"));
}

#[tokio::test]
async fn a_commit_answered_before_the_end_counts_though_its_copies_are_not_yet_verified() {
    // Abandoning loses nothing the answer decided; in keep mode a rewrite
    // made after the answer stays pending, unlisted in the result that
    // covered the answered write.
    for keep in [false, true] {
        let mut env = Env::new();
        let session = env.session("sess_a", Write::default(), true).await;
        session.write("a", "one", 1).await;
        let point = session.sync_point(Some(task(1)), RESULT_WAIT, None);
        env.call().await.uploaded();
        let commit = env.call().await;
        let hold = env.cache.verifications.hold();
        commit.accepted();
        // The answer's arrival, sent before the verification began, reaches
        // the owner before the rewrite and the final upload do.
        support::eventually(|| env.cache.verifications.held() == 1).await;
        if keep {
            session.write("a", "two", 2).await;
        }
        let keeper = Arc::new(Keeper::default());
        let mode = if keep {
            FinalMode::Keep
        } else {
            abandon(&keeper)
        };
        let upload = start(&session, Duration::from_secs(30), mode);
        env.advance(Duration::from_secs(30)).await;
        let lost = ended(upload).await;
        drop(hold);
        assert!(lost.is_empty(), "keep {keep}: {lost:?}");
        let result = outcome(point).await;
        assert_eq!(result.committed, 1, "keep {keep}");
        assert!(result.not_synced.is_empty(), "keep {keep}: {result:?}");
        let tables = session.tables().await;
        assert_eq!(tables.pending_changes.contains_key(&key("a")), keep);
        assert!(tables.inflight_commits.is_empty());
    }
}

#[tokio::test]
async fn a_change_rejected_before_the_end_is_reported_though_the_copies_are_not_yet_verified() {
    let mut env = Env::new();
    let mut pending = seed(&["a".to_owned(), "b".to_owned()], &[]);
    pending.pending[1].task_id = Some(task(2));
    let session = env.session("sess_a", pending, true).await;
    let commit = env.call().await;
    let results = commit
        .changes()
        .iter()
        .map(|change| match change.path() {
            "b" => json!({ "status": "rejected", "reason": "path_rule", "rule": "no such names" }),
            _ => json!({ "status": "accepted", "version_id": "v_a" }),
        })
        .collect();
    let hold = env.cache.verifications.hold();
    commit.answer(results);
    support::eventually(|| env.cache.verifications.held() == 1).await;
    let keeper = Arc::new(Keeper::default());
    let upload = start(&session, Duration::from_secs(30), abandon(&keeper));
    env.advance(Duration::from_secs(30)).await;
    let lost = ended(upload).await;
    assert!(lost.is_empty(), "{lost:?}");
    support::eventually(|| env.reports.sent.lock().unwrap().len() == 1).await;
    drop(hold);
    let report = env.reports.sent.lock().unwrap()[0].clone();
    assert_eq!(report["task_id"], json!(task(2)));
    let rejected = report["drive_sync"]["rejected"].as_array().unwrap();
    assert_eq!(rejected.len(), 1);
    assert!(rejected[0]["path"].as_str().unwrap().ends_with("/b"));
}

#[tokio::test]
async fn a_final_upload_tries_faults_again_and_ends_once_every_covered_change_is_decided() {
    let mut env = Env::new();
    let (session, commit) = with_held_commit(&mut env).await;
    session.write("f", "new", 1).await;
    session.task_exited(1);
    env.call().await.upload_ended(Ended::Permanent {
        status: 404,
        code: Some("not_found"),
    });
    session
        .until(|tables| tables.pending_changes[&key("f")].fault_code.is_some())
        .await;
    let keeper = Arc::new(Keeper::default());
    let upload = start(&session, Duration::from_secs(30), abandon(&keeper));
    env.call().await.uploaded();
    commit.accepted();
    env.call().await.accepted();
    assert!(ended(upload).await.is_empty());
    assert!(keeper.0.lock().unwrap().is_empty());
    assert!(session.tables().await.pending_changes.is_empty());
}

#[tokio::test]
async fn keep_mode_leaves_the_changes_pending_and_keeps_no_entry() {
    let mut env = Env::new();
    let (session, commit) = with_held_commit(&mut env).await;
    session.write("f", "new", 1).await;
    let upload = start(&session, Duration::from_secs(30), FinalMode::Keep);
    let put = env.call().await;
    env.advance(Duration::from_secs(30)).await;
    assert!(ended(upload).await.is_empty());
    drop((put, commit));
    let pending = session.tables().await.pending_changes;
    assert!(pending.contains_key(&key("f")) && pending.contains_key(&key("g")));

    // The connection ends during an abandoning one: it ends at once, kept.
    let keeper = Arc::new(Keeper::default());
    let upload = start(&session, Duration::from_secs(60), abandon(&keeper));
    // The saved commit may be resent before the upload receives its slot.
    let mut resent = Vec::new();
    let put = loop {
        let call = env.call().await;
        match call {
            Call::Upload { .. } => break call,
            Call::Commit { .. } => resent.push(call),
        }
    };
    upload.keep_now();
    assert!(ended(upload).await.is_empty());
    assert!(keeper.0.lock().unwrap().is_empty());
    let Call::Upload { answer: put, .. } = put else {
        unreachable!()
    };
    assert!(put.is_closed());
    for call in resent {
        let Call::Commit { answer, .. } = call else {
            unreachable!()
        };
        assert!(answer.is_closed());
    }
    assert_eq!(session.tables().await.pending_changes.len(), 2);

    // The next connection sends the cancelled commit again, unchanged,
    // although `g` was removed since.
    session.remove("g", 2).await;
    session.sync.binding_current(drive::transfer::Binding {
        connection_id: Id::new("conn_b").unwrap(),
        binding_id: Id::new("bind_a").unwrap(),
    });
    let first = env.call().await;
    let stored = match first {
        Call::Commit { .. } => first,
        _ => panic!("the stored commit goes first"),
    };
    let CommitChange::Write { content_sha256, .. } = &stored.changes()[0] else {
        panic!("a write");
    };
    assert_eq!(content_sha256.as_str(), support::sha256(b"g"));
}

#[tokio::test]
async fn a_released_final_upload_ends_at_once_and_cancels_nothing() {
    let mut env = Env::new();
    let (session, commit) = with_held_commit(&mut env).await;
    session.write("f", "new", 1).await;
    let upload = start(&session, Duration::from_secs(30), FinalMode::Keep);
    let put = env.call().await;
    upload.keeper().release();
    assert!(ended(upload).await.is_empty());
    // Its end time passing cancels nothing either.
    env.advance(Duration::from_secs(30)).await;
    session.caught_up().await;
    let (Call::Upload { answer: put, .. }, Call::Commit { answer: held, .. }) = (put, commit)
    else {
        unreachable!()
    };
    assert!(
        !put.is_closed() && !held.is_closed(),
        "both requests still run"
    );
    assert_eq!(session.tables().await.pending_changes.len(), 2);
}

#[tokio::test]
async fn session_close_releases_its_drive_wait_and_keeps_other_drives_waiting() {
    for session_close in [false, true] {
        let mut env = Env::new();
        let session = env
            .session("sess_a", drive::state::Write::default(), true)
            .await;
        let other = Id::new("drive_b").unwrap();
        session.sync.mounted(other.clone(), Vec::new());
        session.caught_up().await;
        for (drive, label) in [(&session.drive, "a"), (&other, "b")] {
            session
                .write_in(drive, "ready", &format!("{label} ready"), 1)
                .await;
            session
                .write_in(drive, "held", &format!("{label} held"), 1)
                .await;
        }
        let point = session.sync_point(Some(support::task(1)), support::RESULT_WAIT, None);
        let mut held = Vec::new();
        for _ in 0..4 {
            let put = env.call().await;
            if [support::sha256(b"a ready"), support::sha256(b"b ready")]
                .contains(&put.sha256().to_owned())
            {
                put.uploaded();
            } else {
                held.push(put);
            }
        }
        session
            .until(|tables| {
                tables
                    .pending_changes
                    .iter()
                    .filter(|((_, path), _)| path == "ready")
                    .all(|(_, change)| change.uploaded)
            })
            .await;
        let keeper = Arc::new(Keeper::default());
        let upload = session.sync.final_upload(FinalUploadRequest {
            session_close,
            drives: vec![session.drive.clone()],
            end: Duration::from_secs(10),
            mode: abandon(&keeper),
        });
        if session_close {
            let ready = env.call().await;
            assert_eq!(ready.changes().len(), 1);
            assert_eq!(ready.changes()[0].path(), "ready");
            let CommitChange::Write { content_sha256, .. } = &ready.changes()[0] else {
                unreachable!()
            };
            assert_eq!(content_sha256.as_str(), support::sha256(b"a ready"));
            ready.accepted();
            session
                .until(|tables| !tables.pending_changes.contains_key(&key("ready")))
                .await;
        }
        assert!(
            !point.is_finished(),
            "the other drive retains the original result wait"
        );
        env.advance(Duration::from_secs(10)).await;
        let lost = serde_json::to_value(ended(upload).await).unwrap();
        assert_eq!(
            lost[0]["paths"],
            if session_close {
                json!(["held"])
            } else {
                json!(["held", "ready"])
            }
        );
        assert!(
            session
                .tables()
                .await
                .pending_changes
                .contains_key(&("drive_b".to_owned(), "ready".to_owned()))
        );
        assert!(!point.is_finished());
        env.advance(Duration::from_secs(20)).await;
        let other_ready = env.call().await;
        let CommitChange::Write { content_sha256, .. } = &other_ready.changes()[0] else {
            unreachable!()
        };
        assert_eq!(content_sha256.as_str(), support::sha256(b"b ready"));
        other_ready.accepted();
        assert_eq!(
            support::outcome(point).await.committed,
            if session_close { 2 } else { 1 }
        );
        drop(held);
        session.sync.close().await;
    }
}

#[tokio::test]
async fn a_sync_point_that_reaches_the_owner_during_a_session_removal_ends_that_wait_at_once() {
    for session_close in [false, true] {
        let mut env = Env::new();
        let session = env
            .session("sess_a", drive::state::Write::default(), true)
            .await;
        let other = Id::new("drive_b").unwrap();
        session.sync.mounted(other.clone(), Vec::new());
        session.caught_up().await;
        session.write("held", "held", 1).await;
        let keeper = Arc::new(Keeper::default());
        let upload = session.sync.final_upload(FinalUploadRequest {
            session_close,
            drives: vec![session.drive.clone()],
            end: Duration::from_secs(10),
            mode: abandon(&keeper),
        });
        let held = env.call().await;
        if session_close {
            session.write_in(&other, "ready", "b ready", 1).await;
        }
        let point = session.sync_point(Some(support::task(1)), support::RESULT_WAIT, None);
        if session_close {
            let other_ready = env.call().await;
            assert_eq!(other_ready.sha256(), support::sha256(b"b ready"));
            session.tables().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!point.is_finished(), "the other drive keeps its wait");
            other_ready.uploaded();
            let commit = env.call().await;
            assert_eq!(commit.changes()[0].path(), "ready");
            commit.accepted();
            let outcome = support::outcome(point).await;
            assert_eq!(outcome.committed, 1);
            let not_synced: Vec<_> = outcome
                .not_synced
                .iter()
                .map(|entry| (entry.path.as_str().to_owned(), entry.reason))
                .collect();
            assert_eq!(
                not_synced,
                [(session.machine_path("held"), NotSyncedReason::Uploading)]
            );
        } else {
            session.tables().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!point.is_finished(), "a binding change keeps the wait");
        }
        drop((held, upload));
        session.sync.close().await;
    }
}
