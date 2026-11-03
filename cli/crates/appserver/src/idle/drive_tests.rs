//! IdleExit composed with real Connection and DriveSync owners and SQLite.
use super::*;
use crate::test_support::*;
use std::sync::atomic::Ordering;

impl Fixture {
    fn idle(&self) -> IdleExit {
        let mut idle = IdleExit::new(
            Duration::from_secs(60),
            Duration::ZERO,
            self.connection.clone(),
            id("inst_idle"),
            Duration::from_secs(3),
        );
        idle.clock = self.clock.clone();
        idle
    }
}

#[tokio::test]
async fn idle_exit_uploads_and_commits_a_real_pending_write() {
    let mut f = Fixture::new();
    let sync = f.session("sess_success").await;
    assert!(
        f.calls.try_recv().is_err(),
        "a running task has not started its upload"
    );
    let mut idle = f.idle();
    idle.begin().await;
    let put = f.call().await;
    assert!(matches!(&put, Call::Upload(_)));
    put.accept();
    let commit = f.call().await;
    assert!(matches!(&commit, Call::Commit(_)));
    commit.accept();
    assert!(within(idle.uploaded()).await);
    assert!(sync.tables().await.pending_changes.is_empty());
}

#[tokio::test]
async fn delayed_session_collection_keeps_one_deadline_and_retains_uncommitted_bytes() {
    let mut f = Fixture::new();
    f.session("sess_first").await;
    f.session("sess_second").await;
    let mut idle = f.idle();
    let (gate, mut waits) = mpsc::unbounded_channel();
    idle.collection_gate = Some(gate);
    let collecting = tokio::spawn(async move {
        idle.begin().await;
        idle
    });
    // Delay connection collection, then each Session snapshot.
    for at in [1, 2, 2] {
        let release = within(waits.recv()).await.unwrap();
        f.now.store(at, Ordering::SeqCst);
        release.send(()).unwrap();
    }
    let mut idle = within(collecting).await.unwrap();
    let first = f.call().await;
    let second = f.call().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), idle.uploaded())
            .await
            .is_err()
    );
    f.now.store(3, Ordering::SeqCst);
    assert!(within(idle.uploaded()).await);
    assert!(first.cancelled() && second.cancelled());
    f.retained().await;
}

#[tokio::test]
async fn new_activity_withdraws_the_exit_without_cancelling_its_transfers() {
    for preparing in [false, true] {
        let mut f = Fixture::new();
        let sync = f.session("sess_activity").await;
        let mut idle = f.idle();
        idle.begin().await;
        let put = f.call().await;
        idle.activity(if preparing {
            Activity::Preparing(Duration::from_secs(5))
        } else {
            Activity::Arrived(Duration::from_secs(5))
        });
        assert!(
            !within(idle.uploaded()).await,
            "the cancelled round cannot shut down"
        );
        f.now.store(31, Ordering::SeqCst);
        within(sync.check()).await;
        assert!(
            !put.cancelled(),
            "withdrawal leaves ordinary upload running"
        );
        put.accept();
        f.call().await.accept();
        within(async {
            while !sync.tables().await.pending_changes.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if preparing {
            idle.activity(Activity::Prepared(Duration::from_secs(31)));
        }
        assert_eq!(
            idle.due(),
            Some(Duration::from_secs(if preparing { 91 } else { 65 }))
        );
    }
}
