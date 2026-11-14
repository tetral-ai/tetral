//! Open-file lifetimes through real FUSE, including cache eviction, remote
//! replacement, writes through another descriptor, unlink and last release.
#![cfg(target_os = "linux")]

mod support;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use drive::cache::LISTING_SPACING;
use drive::mount::{Bound, Mounting};
use drive::state::Write as StateWrite;
use protocol::id::SafeId;
use serde_json::json;
use support::{Env, Session, Unmount};

async fn mounted(env: &mut Env) -> (Session, Unmount, PathBuf) {
    env.mounting = Mounting::Fuse;
    let unmount = Unmount(env.root.path().to_path_buf());
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
    let point = session.directory.join("drives").join(support::DRIVE);
    (session, unmount, point)
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::time::timeout(Duration::from_secs(10), tokio::task::spawn_blocking(work))
        .await
        .expect("filesystem operation completes")
        .unwrap()
}

fn descriptors(path: &Path) -> usize {
    let wanted = path.as_os_str().as_encoded_bytes();
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .filter(|target| target.as_os_str().as_encoded_bytes().starts_with(wanted))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn one_open_reuses_an_evicted_blob_and_tracks_the_current_source() {
    let mut env = Env::with_free_space(Arc::new(|| Ok(0)));
    let original = "a".repeat(2 << 20);
    let first_hash = env.stub.content(&original);
    env.stub.list("remote", &original);
    let (session, _unmount, point) = mounted(&mut env).await;
    let remote = point.join("remote");
    let file = blocking(move || {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(remote)
            .unwrap();
        for offset in [0, 1 << 20, (2 << 20) - 1, 0] {
            let mut byte = [0];
            assert_eq!(file.read_at(&mut byte, offset).unwrap(), 1);
            assert_eq!(byte, [b'a']);
        }
        file
    })
    .await;
    assert_eq!(
        env.stub.downloads_started.load(Ordering::SeqCst),
        1,
        "one open downloads its current content once"
    );
    let blob = env.root.path().join("cache/blobs").join(&first_hash);
    env.clock.advance(LISTING_SPACING);
    support::cache::within("low space unlinks the open blob", || !blob.exists()).await;
    assert!(
        descriptors(&blob) > 0,
        "the opened descriptor still owns those bytes"
    );
    let replacement = "b".repeat(2 << 20);
    let new_hash = env.stub.content(&replacement);
    env.stub.records.lock().unwrap().push(json!({
        "seq":2, "index":0, "path":"remote", "operation":"modified", "version_id":"v_new",
        "content_sha256":new_hash, "size_bytes":replacement.len(), "executable":false,
    }));
    session.sync.drive_changed(session.drive.clone(), 2);
    session.caught_up().await;
    let file = blocking(move || {
        let mut byte = [0];
        assert_eq!(file.read_at(&mut byte, 1 << 20).unwrap(), 1);
        assert_eq!(
            byte,
            [b'b'],
            "an existing open follows remote current content"
        );
        file
    })
    .await;
    assert_eq!(env.stub.downloads_started.load(Ordering::SeqCst), 2);
    assert_eq!(descriptors(&blob), 0, "the obsolete source is released");
    // A second open writes; the original open follows the new Session source.
    let other_path = point.join("remote");
    let file = blocking(move || {
        let other = OpenOptions::new().write(true).open(&other_path).unwrap();
        other.write_all_at(b"z", 1 << 20).unwrap();
        let mut byte = [0];
        file.read_at(&mut byte, 1 << 20).unwrap();
        assert_eq!(byte, [b'z']);
        std::fs::remove_file(other_path).unwrap();
        other.write_all_at(b"q", 0).unwrap();
        file.read_at(&mut byte, 0).unwrap();
        assert_eq!(byte, [b'q'], "open descriptors share writes after unlink");
        file.set_len(7).unwrap();
        assert_eq!(other.metadata().unwrap().len(), 7);
        assert_eq!(file.metadata().unwrap().len(), 7);
        file.sync_all().unwrap();
        file
    })
    .await;
    assert_eq!(
        env.stub.downloads_started.load(Ordering::SeqCst),
        2,
        "partial writes reuse the evicted current blob"
    );
    let inode = file.metadata().unwrap().ino();
    let local_content = session
        .directory
        .join("content")
        .join(support::DRIVE)
        .join(inode.to_string());
    assert!(!local_content.exists());
    assert!(descriptors(&local_content) > 0);
    blocking(move || drop(file)).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while descriptors(&local_content) != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("last release reclaims unlinked Session content");
    session.sync.detach_all();
    session.sync.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn temporary_files_and_replaced_inodes_keep_independent_read_write_and_stat_lifetimes() {
    let mut env = Env::new();
    let (session, _unmount, point) = mounted(&mut env).await;
    blocking(move || {
        let mut temporary = tempfile::tempfile_in(&point).unwrap();
        temporary.write_all(b"temporary").unwrap();
        temporary.seek(SeekFrom::Start(0)).unwrap();
        let mut text = String::new();
        temporary.read_to_string(&mut text).unwrap();
        assert_eq!(text, "temporary");
        assert_eq!(temporary.metadata().unwrap().len(), 9);
        assert_eq!(temporary.metadata().unwrap().nlink(), 0);
        temporary.set_len(4).unwrap();
        assert_eq!(temporary.metadata().unwrap().len(), 4);
        temporary.sync_all().unwrap();
        drop(temporary);
        let path = point.join("target");
        let mut original = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        original.write_all(b"old inode").unwrap();
        std::fs::write(point.join("replacement"), b"new inode").unwrap();
        std::fs::rename(point.join("replacement"), &path).unwrap();
        original.write_all_at(b"OLD", 0).unwrap();
        let mut old = [0; 9];
        original.read_at(&mut old, 0).unwrap();
        assert_eq!(&old, b"OLD inode");
        assert_eq!(std::fs::read(&path).unwrap(), b"new inode");
        assert_ne!(
            original.metadata().unwrap().ino(),
            File::open(path).unwrap().metadata().unwrap().ino()
        );
    })
    .await;
    let tables = session.sync.tables().await;
    assert!(tables.files.keys().all(|(_, path)| !path.contains("tmp")));
    assert!(
        tables
            .pending_changes
            .keys()
            .all(|(_, path)| !path.contains("tmp")),
        "unlinked temporary writes do not recreate a pending path"
    );
    session.sync.detach_all();
    session.sync.close().await;
}
