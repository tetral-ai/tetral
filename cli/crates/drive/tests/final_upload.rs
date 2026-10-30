//! Final uploads: the end at the end time however late it started, what
//! abandoning removes and restores, the end once every covered change is
//! decided, keep mode, and a released one.

mod support;

use std::sync::Arc;
use std::time::Duration;

use drive::{FinalMode, FinalUpload, FinalUploadRequest};
use protocol::connection::LostChange;
use protocol::drive_transfer::CommitChange;
use protocol::id::Id;
use serde_json::json;

use drive::transfer::Ended;
use support::{Call, Env, Keeper, Session, key, seed};

fn abandon(keeper: &Arc<Keeper>) -> FinalMode {
    FinalMode::Abandon {
        request_message_id: Id::new("close_a").unwrap(),
        connection: keeper.clone(),
    }
}

fn start(session: &Session, end: Duration, mode: FinalMode) -> FinalUpload {
    session.sync.final_upload(FinalUploadRequest {
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
    tokio::time::sleep(Duration::from_millis(50)).await;
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
    session.write("f", "newer", 2).await;
    session.task_exited(2);
    env.no_call(Duration::from_millis(200)).await;
    // Until it is mounted again.
    session.sync.mounted(session.drive.clone(), Vec::new());
    session.caught_up().await;
    session.write("f", "newest", 3).await;
    session.task_exited(3);
    assert_eq!(env.call().await.sha256(), support::sha256(b"newest"));
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
    let put = env.call().await;
    upload.keep_now();
    assert!(ended(upload).await.is_empty());
    assert!(keeper.0.lock().unwrap().is_empty());
    let Call::Upload { answer: put, .. } = put else {
        unreachable!()
    };
    assert!(put.is_closed());
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
