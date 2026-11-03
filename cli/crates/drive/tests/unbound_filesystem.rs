//! Binding loss through real FUSE: unavailable content never keeps process
//! teardown waiting, and a later binding resumes only on a new file request.
#![cfg(target_os = "linux")]

mod support;

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use drive::mount::{Bound, Mounting};
use drive::state::Write as StateWrite;
use drive::transfer::Binding;
use protocol::id::{Id, SafeId};
use support::{Env, Unmount};

#[tokio::test(flavor = "multi_thread")]
async fn binding_loss_reaps_a_blocked_reader_and_fails_missing_content_until_new_demand() {
    let mut env = Env::new();
    env.mounting = Mounting::Fuse;
    env.stub.list("cold", "unavailable bytes");
    env.stub.list("warm", "cached bytes");
    let hash = support::sha256(b"unavailable bytes");
    env.stub.held_downloads.lock().unwrap().insert(hash.clone());
    let _unmount = Unmount(env.root.path().to_path_buf());
    let session = env.session("sess_a", StateWrite::default(), false).await;
    let failures = session
        .sync
        .mount_drives(vec![Bound {
            drive_id: SafeId::new(support::DRIVE).unwrap(),
            read_only: false,
            exclude: vec![],
        }])
        .await;
    assert!(failures.is_empty(), "{failures:?}");
    session.caught_up().await;
    assert_eq!(session.read("warm").await.unwrap(), "cached bytes");
    let point = session.directory.join("drives").join(support::DRIVE);
    let mut reader = Command::new("cat")
        .arg(point.join("cold"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let progress = env.cache.downloading(&SafeId::new("sess_a").unwrap()).await;
            if progress.iter().any(|(_, count)| *count == 1) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real FUSE read waits on a partial download");
    assert!(reader.try_wait().unwrap().is_none());
    reader.kill().unwrap();
    session.sync.binding_gone();
    session.sync.tables().await;
    let status = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || reader.wait()),
    )
    .await
    .expect("the killed process reaps after binding loss")
    .unwrap()
    .unwrap();
    assert!(!status.success());
    let cold = point.join("cold");
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            assert_eq!(std::fs::read(&cold).unwrap_err().raw_os_error(), Some(5));
            let mut file = std::fs::OpenOptions::new().write(true).open(&cold).unwrap();
            assert_eq!(file.write_all(b"x").unwrap_err().raw_os_error(), Some(5));
        }),
    )
    .await
    .expect("new uncached reads and partial writes return EIO")
    .unwrap();
    assert_eq!(session.read("warm").await.unwrap(), "cached bytes");
    session.sync.binding_current(Binding {
        connection_id: Id::new("conn_a").unwrap(),
        binding_id: Id::new("bind_b").unwrap(),
    });
    session.sync.tables().await;
    // The retained byte proves no partial blob was published or discarded.
    let progress = env.cache.downloading(&SafeId::new("sess_a").unwrap()).await;
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].1, 1);
    env.stub.held_downloads.lock().unwrap().remove(&hash);
    let cold = point.join("cold");
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || std::fs::read(cold)),
    )
    .await
    .expect("new demand resumes the retained bytes")
    .unwrap()
    .unwrap();
    assert_eq!(bytes, b"unavailable bytes");
    session.sync.detach_all();
    session.sync.close().await;
}

#[tokio::test]
async fn binding_loss_ends_initial_namespace_waiters_and_new_requests_fail_until_rebound() {
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    let env = Env::new();
    env.stub.list("available", "listed bytes");
    let (release, gate) = tokio::sync::watch::channel(false);
    *env.stub.listing_gate.lock().unwrap() = Some(gate);
    let session = env
        .session("sess_initial", StateWrite::default(), false)
        .await;
    session.sync.mounted(session.drive.clone(), Vec::new());
    let lookup = session
        .sync
        .lookup(&session.drive, drive::ops::TOP_INODE, b"available");
    tokio::pin!(lookup);
    assert!(
        poll_fn(|cx| Poll::Ready(lookup.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    // Lookup was enqueued before this owner barrier and is waiting for the
    // held initial listing, rather than for a missing content blob.
    assert!(session.sync.tables().await.drive_cursor.is_empty());
    session.sync.binding_gone();
    let ended = tokio::time::timeout(Duration::from_secs(2), &mut lookup)
        .await
        .expect("binding loss answers the existing namespace request");
    assert_eq!(ended.unwrap_err(), rustix::io::Errno::IO);
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            session.sync.readdir(&session.drive, drive::ops::TOP_INODE)
        )
        .await
        .expect("new unbound namespace requests end")
        .unwrap_err(),
        rustix::io::Errno::IO
    );
    session.sync.binding_current(Binding {
        connection_id: Id::new("conn_b").unwrap(),
        binding_id: Id::new("bind_b").unwrap(),
    });
    release.send(true).unwrap();
    let found = tokio::time::timeout(
        Duration::from_secs(2),
        session
            .sync
            .lookup(&session.drive, drive::ops::TOP_INODE, b"available"),
    )
    .await
    .expect("a rebound request completes after listing")
    .unwrap();
    assert!(found.ino > drive::ops::TOP_INODE);
    session.sync.close().await;
}
