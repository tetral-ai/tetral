//! Local repository removal through the real sync, cache and upload owners.
mod support;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use drive::ops::TOP_INODE;
use drive::state::Write;
use drive::sync_point::RESULT_WAIT;
use protocol::drive_transfer::CommitChange;
use support::{Call, Env, outcome, sha256, task};

fn memory() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
        .collect::<Vec<_>>()
        .join("; ")
}

#[tokio::test]
async fn a_thousand_enrolled_files_share_bounded_uploads_with_another_session() {
    for known in [0, 10_000] {
        let mut env = Env::new();
        for index in 0..known {
            env.stub
                .list(&format!("known-{index:05}"), &format!("remote-{index}"));
        }
        let a = env.session("sess_a", Write::default(), true).await;
        let b = env.session("sess_b", Write::default(), true).await;
        a.sync
            .mkdir(&a.drive, TOP_INODE, b".git", Some(1))
            .await
            .unwrap();
        let mut expected = HashMap::new();
        for index in 0..1_000 {
            let path = format!("new-{index:04}");
            // Distinct payloads; eight large files expose whole-payload accumulation.
            let content = format!(
                "{index:04}{}",
                "x".repeat(if index < 8 { 1 << 20 } else { 1 << 10 })
            );
            expected.insert(path.clone(), sha256(content.as_bytes()));
            a.write(&path, &content, 1).await;
        }
        assert!(a.tables().await.pending_changes.is_empty());
        let before = memory();
        let began = Instant::now();
        a.sync
            .rmdir(&a.drive, TOP_INODE, b".git", Some(2))
            .await
            .unwrap();
        let enrollment = began.elapsed();
        let tables = a.tables().await;
        assert_eq!(tables.pending_changes.len(), 1_000);
        assert!(
            tables
                .pending_changes
                .values()
                .all(|row| row.task_id.as_deref() == Some("task_2"))
        );
        let scheduling = Instant::now();
        a.task_exited(2);
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(env.call().await);
        }
        let progress = Instant::now();
        assert_eq!(a.tables().await.pending_changes.len(), 1_000);
        let response = progress.elapsed();
        eprintln!(
            "known={known} files=1000 small=992x1028B large=8x1048580B enrollment={enrollment:?} scheduling={:?} owner_reply={response:?} before=[{before}] queued=[{}]",
            scheduling.elapsed(),
            memory()
        );
        b.write("other", "other", 3).await;
        b.task_exited(3);
        let point_a = a.sync_point(Some(task(2)), RESULT_WAIT, None);
        let point_b = b.sync_point(Some(task(3)), RESULT_WAIT, None);
        for call in held {
            call.uploaded();
        }
        let mut uploads = 8;
        let mut committed = 0;
        let mut saw_other = false;
        while uploads < 1_001 || committed < 1_001 {
            let call = env.call().await;
            match call {
                Call::Upload { .. } => {
                    uploads += 1;
                    if call.session() == "sess_b" {
                        assert!(uploads <= 10, "the other Session gets a round-robin turn");
                        saw_other = true;
                    }
                    call.uploaded();
                }
                Call::Commit { ref changes, .. } => {
                    assert!(changes.len() <= 10_000);
                    committed += changes.len();
                    for change in changes {
                        let CommitChange::Write {
                            path,
                            content_sha256,
                            ..
                        } = change
                        else {
                            panic!("writes only")
                        };
                        if call.session() == "sess_a" {
                            assert_eq!(content_sha256.as_str(), expected[path]);
                        }
                    }
                    call.accepted();
                }
            }
            assert!(env.stub.puts_most.load(Ordering::SeqCst) <= 8);
        }
        assert!(saw_other);
        assert_eq!(outcome(point_a).await.committed, 1_000);
        assert_eq!(outcome(point_b).await.committed, 1);
        let tables = a.tables().await;
        assert!(tables.pending_changes.is_empty());
        for (path, hash) in expected {
            assert_eq!(
                tables.files[&support::key(&path)].synced_sha256.as_deref(),
                Some(hash.as_str())
            );
        }
        eprintln!("known={known} completed=[{}]", memory());
    }
}
