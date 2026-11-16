//! Real SQLite write failure followed by an accepted commit cannot put
//! unacknowledged changed bytes under the accepted hash in the shared cache.

mod support;

use std::time::Duration;

use drive::state::Write;
use rustix::io::Errno;
use sqlx::Connection;
use support::RESULT_WAIT;
use support::{Env, Records, key, outcome, sha256};

async fn reject_next_file_write(session: &support::Session) -> sqlx::SqliteConnection {
    let options =
        sqlx::sqlite::SqliteConnectOptions::new().filename(session.directory.join("sync-state.db"));
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_file_write BEFORE INSERT ON files BEGIN SELECT RAISE(ABORT, 'storage failure'); END")
        .execute(&mut db).await.unwrap();
    db
}

#[tokio::test]
async fn failed_writes_before_and_during_a_commit_cannot_contaminate_shared_content() {
    for before_flight in [false, true] {
        let records = Records::start();
        let mut env = Env::new();
        env.stub.content("first");
        let a = env.session("sess_a", Write::default(), true).await;
        a.task_started(1);
        a.write("f", "first", 1).await;
        let inode = a.tables().await.files[&key("f")].file_id as u64;
        a.sync.open_file(&a.drive, inode).await.unwrap();
        a.task_exited(1);
        let mut point =
            (!before_flight).then(|| a.sync_point(Some(support::task(1)), RESULT_WAIT, None));
        env.call().await.uploaded();
        let mut commit = if before_flight {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !a.tables().await.pending_changes[&key("f")].uploaded {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(a.tables().await.inflight_commits.is_empty());
            None
        } else {
            Some(env.call().await)
        };
        let mut db = reject_next_file_write(&a).await;
        let failed = a.sync.write(&a.drive, inode, 0, b"other", Some(1)).await;
        assert_eq!(failed, Err(Errno::IO));
        let tables = a.tables().await;
        assert!(tables.pending_changes[&key("f")].uploaded);
        assert!(
            tables
                .inflight_commits
                .values()
                .all(|row| !row.changed_during_flight)
        );
        let source = a
            .directory
            .join("content")
            .join(support::DRIVE)
            .join(inode.to_string());
        assert_eq!(
            std::fs::read(&source).unwrap(),
            b"other",
            "the failed transaction left changed physical bytes"
        );
        sqlx::raw_sql("DROP TRIGGER fail_file_write")
            .execute(&mut db)
            .await
            .unwrap();
        db.close().await.unwrap();
        if before_flight {
            point = Some(a.sync_point(Some(support::task(1)), RESULT_WAIT, None));
            commit = Some(env.call().await);
        }
        commit.unwrap().accepted();
        outcome(point.unwrap()).await;
        let blob = env.root.path().join("cache/blobs").join(sha256(b"first"));
        if blob.exists() {
            assert_eq!(std::fs::read(&blob).unwrap(), b"first");
        }
        assert!(
            !source.exists(),
            "settlement removes untracked failed-write bytes"
        );
        assert!(a.tables().await.pending_changes.is_empty());
        assert_eq!(
            a.read("f").await.unwrap(),
            "first",
            "the writer returns to committed content"
        );
        assert_eq!(
            a.sync.read(&a.drive, inode, 0, 5).await.unwrap(),
            b"first",
            "an existing open must not keep the failed-write source"
        );
        env.stub.list("f", "first");
        env.stub.list("remote", "first");
        let b = env.session("sess_b", Write::default(), true).await;
        assert_eq!(b.read("remote").await.unwrap(), "first");
        let lines = records.lines();
        let failure = lines
            .iter()
            .find(|line| line["event"] == "drive.cache_adoption_failed")
            .expect("integrity failure is recorded");
        assert_eq!(failure["error.code"], "content_integrity_failed");
        assert_eq!(
            failure["error.message"],
            "accepted content does not match its hash and size"
        );
        assert_eq!(failure["session.id"], "sess_a");
        assert_eq!(failure["drive.id"], support::DRIVE);
        assert_eq!(
            std::fs::read_dir(env.root.path().join("cache/tmp"))
                .unwrap()
                .count(),
            0
        );
        a.sync.release_file(&a.drive, inode).await.unwrap();
        a.sync.close().await;
        let restarted = env.reopen("sess_a", true).await;
        assert_eq!(restarted.read("f").await.unwrap(), "first");
        restarted.sync.close().await;
        b.sync.close().await;
    }
}

#[tokio::test]
async fn a_successful_rewrite_stays_pending_while_the_older_commit_is_verified() {
    let records = Records::start();
    let mut env = Env::new();
    let a = env.session("sess_a", Write::default(), true).await;
    a.write("f", "first", 1).await;
    let point = a.sync_point(Some(support::task(1)), RESULT_WAIT, None);
    env.call().await.uploaded();
    let first = env.call().await;
    a.write("f", "newer", 1).await;
    let newer = a.sync_point(Some(support::task(1)), RESULT_WAIT, None);
    first.accepted();
    env.call().await.uploaded();
    let second = env.call().await;
    assert!(
        matches!(&second.changes()[0], protocol::drive_transfer::CommitChange::Write { content_sha256, .. } if content_sha256.as_str() == sha256(b"newer"))
    );
    assert_eq!(a.read("f").await.unwrap(), "newer");
    second.accepted();
    outcome(point).await;
    outcome(newer).await;
    assert_eq!(a.read("f").await.unwrap(), "newer");
    // The rewrite's sync point lists f as uploading when the older commit is
    // still in flight as its wait ends, so its own commit can be answered
    // after that outcome.
    a.until(|tables| tables.pending_changes.is_empty()).await;
    assert!(
        records
            .lines()
            .iter()
            .all(|line| line["event"] != "drive.cache_adoption_failed")
    );
    assert_eq!(
        std::fs::read_dir(env.root.path().join("cache/tmp"))
            .unwrap()
            .count(),
        0
    );
    a.sync.close().await;
}
