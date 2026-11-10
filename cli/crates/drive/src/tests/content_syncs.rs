//! Which flushes, `fsync`s and uploads sync a `session` content file, counted
//! by a syncer that stands in for `File::sync_all` and can fail a sync.

use super::support;

use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use drive::DriveSync;
use drive::binding_change::BindingChange;
use drive::mount::Bound;
use drive::ops::{SetAttr, TOP_INODE};
use drive::state::Write;
use drive::transfer::Ended;
use protocol::id::{Id, SafeId};
use support::{Call, Env, Keeper, NO_TASK, RESULT_WAIT, Session, error_body, outcome, sha256};

#[derive(Clone, Copy, Debug)]
enum Step {
    /// `a` created, empty.
    Create,
    /// `node_modules/a` created, empty: a local-only file, never uploaded.
    CreateLocal,
    /// One byte written into `a`, with no close.
    Write,
    /// `a` truncated to one byte.
    Truncate,
    /// `a` opened, its descriptor held.
    Open,
    /// `a`'s last descriptor released, as its close does.
    Release,
    /// `a` opened and then unlinked, so that only its descriptor reaches it.
    OpenAndUnlink,
    /// `a` unlinked with no descriptor held.
    Unlink,
    /// Checks that the owner holds the write generations of no file.
    Forgotten,
    /// A flush or `fsync` of `a`.
    Sync,
    /// A flush or `fsync` of `a` whose sync fails.
    FailedSync,
    /// The owner closed and started again from its sync state, until the
    /// upload of `a` that the start resends reaches the drive.
    Restart,
    /// A sync point, until the upload of `a` reaches the drive.
    Upload,
    /// The drive, with nothing to upload, removed from the binding and bound
    /// again, so that the next file created takes the same file ID.
    Rebind,
    /// The listed file `c` read, renamed to `d`, and its commit answered
    /// `409 content_missing`, until the upload of its cache blob reaches the
    /// drive.
    BlobUpload,
}

use Step::*;

#[tokio::test]
async fn a_content_file_is_synced_only_when_a_write_is_not_yet_durable() {
    let rows: [(&str, &[Step], u64); 13] = [
        (
            "a write makes the next sync run",
            &[Create, Sync, Write, Sync],
            2,
        ),
        (
            "a truncation makes the next sync run",
            &[Create, Write, Sync, Truncate, Sync],
            2,
        ),
        (
            "a sync after a successful one is skipped",
            &[Create, Write, Sync, Sync],
            1,
        ),
        (
            "a sync after a failed one runs",
            &[Create, Write, FailedSync, Sync, Sync],
            2,
        ),
        (
            "a started owner syncs a file before it skips it",
            &[Create, Write, Sync, Restart, Sync, Sync],
            3,
        ),
        (
            "a file reached only through its descriptor is always synced",
            &[Create, Write, Sync, OpenAndUnlink, Sync, Sync],
            3,
        ),
        (
            "an upload of a write no sync made durable syncs it",
            &[Create, Write, Upload],
            1,
        ),
        (
            "an upload of a synced write does not sync it",
            &[Create, Write, Sync, Upload],
            1,
        ),
        (
            "an upload after the close of a synced write does not sync it",
            &[Create, Open, Write, Sync, Release, Upload],
            1,
        ),
        (
            "an upload from a cache blob does not sync",
            &[BlobUpload],
            0,
        ),
        (
            "an unlinked file's generations are forgotten",
            &[Create, Write, Sync, Unlink, Forgotten],
            1,
        ),
        (
            "a file unlinked while open is forgotten at its last release",
            &[Create, OpenAndUnlink, Write, Sync, Release, Forgotten],
            1,
        ),
        (
            "a file ID used again after the drive is bound again is synced",
            &[CreateLocal, Write, Sync, Rebind, CreateLocal, Sync],
            2,
        ),
    ];
    let mut wrong = Vec::new();
    for (name, steps, expected) in rows {
        let syncs = Arc::new(AtomicU64::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let mut env = Env::new();
        env.spawn = {
            let (syncs, fail) = (syncs.clone(), fail.clone());
            Arc::new(move |config| {
                let (syncs, fail) = (syncs.clone(), fail.clone());
                DriveSync::spawn_with_syncer(
                    config,
                    Arc::new(move |file: &File| {
                        syncs.fetch_add(1, Ordering::SeqCst);
                        if fail.swap(false, Ordering::SeqCst) {
                            return Err(io::Error::other("the test fails this sync"));
                        }
                        file.sync_all()
                    }),
                )
            })
        };
        env.stub.list("c", "sea");
        let mut session = env.session("sess_a", Write::default(), true).await;
        let drive = session.drive.clone();
        let mut ino = 0;
        let mut rebound = None;
        for step in steps {
            match step {
                Create => {
                    ino = session
                        .sync
                        .create(&drive, TOP_INODE, b"a", 0o644, Some(NO_TASK))
                        .await
                        .unwrap()
                        .ino;
                }
                CreateLocal => {
                    let folder = session
                        .sync
                        .mkdir(&drive, TOP_INODE, b"node_modules", Some(NO_TASK))
                        .await
                        .unwrap()
                        .ino;
                    ino = session
                        .sync
                        .create(&drive, folder, b"a", 0o644, Some(NO_TASK))
                        .await
                        .unwrap()
                        .ino;
                    if let Some(before) = rebound {
                        assert_eq!(ino, before, "{name}: the file ID is used again");
                    }
                }
                Write => {
                    session
                        .sync
                        .write(&drive, ino, 0, b"x", Some(NO_TASK))
                        .await
                        .unwrap();
                }
                Truncate => {
                    let set = SetAttr {
                        size: Some(1),
                        ..SetAttr::default()
                    };
                    session
                        .sync
                        .setattr(&drive, ino, set, Some(NO_TASK))
                        .await
                        .unwrap();
                }
                Open => session.sync.open_file(&drive, ino).await.unwrap(),
                Release => session.sync.release_file(&drive, ino).await.unwrap(),
                OpenAndUnlink => {
                    session.sync.open_file(&drive, ino).await.unwrap();
                    session
                        .sync
                        .unlink(&drive, TOP_INODE, b"a", Some(NO_TASK))
                        .await
                        .unwrap();
                }
                Unlink => session
                    .sync
                    .unlink(&drive, TOP_INODE, b"a", Some(NO_TASK))
                    .await
                    .unwrap(),
                Forgotten => {
                    let held = session.sync.inspect().await.content_syncs;
                    if held != 0 {
                        wrong.push(format!("{name}: generation records held: {held}"));
                    }
                }
                Sync => session.sync.fsync(&drive, ino).await.unwrap(),
                FailedSync => {
                    fail.store(true, Ordering::SeqCst);
                    session.sync.fsync(&drive, ino).await.unwrap_err();
                }
                Restart => {
                    session.sync.close().await;
                    session = env.reopen("sess_a", true).await;
                    assert_eq!(env.call().await.sha256(), sha256(b"x"), "{name}");
                    ino = session
                        .sync
                        .lookup(&drive, TOP_INODE, b"a")
                        .await
                        .unwrap()
                        .ino;
                }
                Upload => {
                    let _point = session.sync_point(None, RESULT_WAIT, None);
                    let upload = env.call().await;
                    assert_eq!(upload.sha256(), sha256(b"x"), "{name}");
                }
                BlobUpload => blob_upload(&mut env, &session).await,
                Rebind => {
                    rebind(&session).await;
                    rebound = Some(ino);
                }
            }
        }
        let counted = syncs.load(Ordering::SeqCst);
        if counted != expected {
            wrong.push(format!("{name}: {counted} syncs, not {expected}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

async fn rebind(session: &Session) {
    let bound = Bound {
        drive_id: SafeId::new(support::DRIVE).unwrap(),
        read_only: false,
        exclude: Vec::new(),
    };
    let change = BindingChange {
        old: vec![bound],
        new: Vec::new(),
        request_message_id: Id::new("drives_1").unwrap(),
        connection: Arc::new(Keeper::default()),
    };
    tokio::time::timeout(
        RESULT_WAIT,
        session.sync.change_binding(change, std::future::pending()),
    )
    .await
    .expect("the removal within its wait")
    .expect("not stopped");
    session.sync.mounted(session.drive.clone(), Vec::new());
    session.caught_up().await;
}

async fn blob_upload(env: &mut Env, session: &Session) {
    let drive = session.drive.clone();
    assert_eq!(session.read("c").await.unwrap(), "sea");
    session
        .sync
        .rename(&drive, TOP_INODE, b"c", TOP_INODE, b"d", Some(NO_TASK))
        .await
        .unwrap();
    let point = session.sync_point(None, RESULT_WAIT, None);
    let sea = sha256(b"sea");
    env.call().await.commit_ended(Ended::Decision(error_body(
        "content_missing",
        "content is missing",
        &[&sea],
    )));
    outcome(point).await;
    loop {
        if let call @ Call::Upload { .. } = env.call().await {
            assert_eq!(call.sha256(), sea);
            return;
        }
    }
}
