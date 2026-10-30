//! A `session.drives` on a drive sync with real FUSE mounts: the final
//! upload of a removed drive and of a drive made read-only before the
//! detach and the remount, the end at 30 seconds with a held PUT, and the
//! connection's end during the final upload.
#![cfg(target_os = "linux")]

mod support;

use std::sync::Arc;
use std::time::Duration;

use drive::binding_change::{Applied, BindingChange};
use drive::mount::{Bound, Mounting};
use drive::mountinfo::current_device;
use drive::state::Write;
use protocol::id::{Id, SafeId};
use serde_json::json;
use tokio::sync::oneshot;

use support::{Env, Keeper, Session, Unmount};

const OTHER: &str = "drive_b";

fn bound(drive: &str, read_only: bool) -> Bound {
    Bound {
        drive_id: SafeId::new(drive).unwrap(),
        read_only,
        exclude: Vec::new(),
    }
}

fn id(text: &str) -> Id {
    Id::new(text).unwrap()
}

/// A Session whose two drives list `f` and are mounted `read_write`, each
/// with a pending write of `f`.
async fn mounted(env: &mut Env) -> (Session, Unmount) {
    env.mounting = Mounting::Fuse;
    env.stub.list("f", "old");
    let unmount = Unmount(env.root.path().to_path_buf());
    let session = env.session("sess_a", Write::default(), false).await;
    let failures = session
        .sync
        .mount_drives(vec![bound(support::DRIVE, false), bound(OTHER, false)])
        .await;
    assert!(failures.is_empty(), "{failures:?}");
    session.caught_up().await;
    session.write("f", "new", 1).await;
    session.write_in(&id(OTHER), "f", "new", 1).await;
    (session, unmount)
}

fn point(session: &Session, drive: &str) -> std::path::PathBuf {
    session.directory.join("drives").join(drive)
}

fn change(keeper: &Arc<Keeper>) -> BindingChange {
    BindingChange {
        old: vec![bound(support::DRIVE, false), bound(OTHER, false)],
        new: vec![bound(OTHER, true)],
        request_message_id: id("drives_1"),
        connection: keeper.clone(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn removed_and_read_only_drives_upload_first_and_abandon_what_is_held_at_30_seconds() {
    let mut env = Env::new();
    let (session, _unmount) = mounted(&mut env).await;
    let keeper = Arc::new(Keeper::default());
    let sync = session.sync.clone();
    let change = change(&keeper);
    let handling =
        tokio::spawn(async move { sync.change_binding(change, std::future::pending()).await });
    // Both uploads start while both drives are still mounted as they were.
    let puts = [env.call().await, env.call().await];
    assert_eq!(puts[0].sha256(), support::sha256(b"new"));
    assert_eq!(puts[1].sha256(), support::sha256(b"new"));
    for drive in [support::DRIVE, OTHER] {
        assert!(current_device(&point(&session, drive)).unwrap().is_some());
    }
    assert!(!handling.is_finished());

    env.advance(Duration::from_secs(30)).await;
    let Applied {
        failures,
        lost_changes,
    } = tokio::time::timeout(Duration::from_secs(10), handling)
        .await
        .expect("handled within 10 s")
        .unwrap()
        .expect("not stopped");
    drop(puts);
    assert!(failures.is_empty(), "{failures:?}");
    let entry = |drive: &str| json!({ "request_message_id": "drives_1", "drive_id": drive, "paths": ["f"], "paths_total": 1 });
    let expected = json!([entry(support::DRIVE), entry(OTHER)]);
    assert_eq!(serde_json::to_value(&lost_changes).unwrap(), expected);
    assert_eq!(
        serde_json::to_value(&*keeper.0.lock().unwrap()).unwrap(),
        expected
    );

    // The removed drive is detached, its rows and content folder gone.
    assert_eq!(
        current_device(&point(&session, support::DRIVE)).unwrap(),
        None
    );
    let tables = session.tables().await;
    assert!(tables.files.keys().all(|(drive, _)| drive == OTHER));
    assert!(tables.pending_changes.is_empty());
    assert!(
        !session
            .directory
            .join("content")
            .join(support::DRIVE)
            .exists()
    );
    // The other is mounted again, read-only, showing the drive's version.
    let file = point(&session, OTHER).join("f");
    let read = tokio::task::spawn_blocking(move || {
        let content = std::fs::read_to_string(&file);
        let written = std::fs::write(&file, "again");
        (content, written)
    })
    .await
    .unwrap();
    assert_eq!(read.0.unwrap(), "old");
    assert_eq!(read.1.unwrap_err().raw_os_error(), Some(libc_erofs()));
    session.sync.detach_all();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_connections_end_during_the_final_upload_changes_nothing() {
    let mut env = Env::new();
    let (session, _unmount) = mounted(&mut env).await;
    let devices: Vec<_> = [support::DRIVE, OTHER]
        .map(|drive| current_device(&point(&session, drive)).unwrap())
        .into();
    let keeper = Arc::new(Keeper::default());
    let sync = session.sync.clone();
    let change = change(&keeper);
    let (stop, stopped) = oneshot::channel::<()>();
    let handling = tokio::spawn(async move {
        sync.change_binding(change, async move {
            let _ = stopped.await;
        })
        .await
    });
    let puts = [env.call().await, env.call().await];
    env.advance(Duration::from_secs(10)).await;
    stop.send(()).unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(10), handling)
        .await
        .expect("stopped within 10 s")
        .unwrap();
    drop(puts);
    assert!(stopped.is_none());
    assert!(keeper.0.lock().unwrap().is_empty());
    assert_eq!(session.tables().await.pending_changes.len(), 2);
    let after: Vec<_> = [support::DRIVE, OTHER]
        .map(|drive| current_device(&point(&session, drive)).unwrap())
        .into();
    assert_eq!(after, devices);
    assert!(devices.iter().all(Option::is_some));
    session.sync.detach_all();
}

/// `EROFS`.
fn libc_erofs() -> i32 {
    rustix::io::Errno::ROFS.raw_os_error()
}
