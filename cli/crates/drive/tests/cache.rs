//! Cache integrity, sharing, eviction and FIFO capacity through real files.
mod support;
use drive::cache::{DOWNLOADS_MAX, FREE_SPACE_MIN};
use protocol::field::Sha256;
use protocol::id::SafeId;
use std::fs;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use support::cache::*;
#[tokio::test]
async fn bytes_of_another_hash_fail_the_read_with_eio_and_store_nothing() {
    let root = tempfile::tempdir().unwrap();
    let stub = Arc::new(Stub::default());
    let wanted = sha256(b"the content");
    stub.serve(&wanted, b"other bytes");
    let cache = cache(root.path(), stub, &TestClock::default(), plenty());
    let error = cache.read(needed("sess_a", &wanted, 11)).await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(5));
    assert!(names(&root.path().join("cache/blobs")).is_empty());
    assert!(names(&root.path().join("cache/tmp")).is_empty());
}

#[tokio::test]
async fn two_drive_syncs_reading_one_hash_leave_one_blob() {
    let root = tempfile::tempdir().unwrap();
    let stub = Arc::new(Stub::default());
    let content = b"shared content";
    let hash = sha256(content);
    stub.serve(&hash, content);
    let cache = cache(root.path(), stub.clone(), &TestClock::default(), plenty());
    let (first, second) = tokio::join!(
        cache.read(needed("sess_a", &hash, content.len())),
        cache.read(needed("sess_b", &hash, content.len())),
    );
    for mut file in [first.unwrap(), second.unwrap()] {
        let mut read = Vec::new();
        file.read_to_end(&mut read).unwrap();
        assert_eq!(read, content);
    }
    assert_eq!(names(&root.path().join("cache/blobs")), [hash.as_str()]);
    assert!(names(&root.path().join("cache/tmp")).is_empty());
    assert_eq!(stub.started().len(), 2);
}

#[tokio::test]
async fn low_free_space_evicts_the_blobs_read_longest_ago_until_five_gib_are_free() {
    let root = tempfile::tempdir().unwrap();
    let blobs = root.path().join("cache/blobs");
    let stub = Arc::new(Stub::default());
    // Below 5 GiB by two blobs' worth once the test lowers it, rising by
    // one step with each blob deleted.
    let step = 1_000;
    let lowered = Arc::new(AtomicU64::new(0));
    let reading = {
        let (lowered, blobs) = (lowered.clone(), blobs.clone());
        Arc::new(move || {
            let held = fs::read_dir(&blobs).map(Iterator::count).unwrap_or(0) as u64;
            Ok(if lowered.load(Ordering::SeqCst) == 0 {
                u64::MAX
            } else {
                FREE_SPACE_MIN - (held.saturating_sub(2)) * step
            })
        })
    };
    let session_file = root.path().join("sessions/sess_a/content/drive_a/7");
    fs::create_dir_all(session_file.parent().unwrap()).unwrap();
    fs::write(&session_file, b"uncommitted").unwrap();
    let clock = TestClock::default();
    let cache = cache(root.path(), stub.clone(), &clock, reading);
    let contents: Vec<Vec<u8>> = (0..4)
        .map(|index| format!("blob {index}").into_bytes())
        .collect();
    let hashes: Vec<Sha256> = contents.iter().map(|bytes| sha256(bytes)).collect();
    for (hash, bytes) in hashes.iter().zip(&contents) {
        stub.serve(hash, bytes);
        cache
            .read(needed("sess_a", hash, bytes.len()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Reading the first blob again makes the second the one read longest ago.
    cache
        .read(needed("sess_a", &hashes[0], contents[0].len()))
        .await
        .unwrap();
    lowered.store(1, Ordering::SeqCst);
    clock.advance(Duration::from_secs(10));
    let mut kept: Vec<String> = [&hashes[0], &hashes[3]]
        .map(|hash| hash.as_str().to_owned())
        .to_vec();
    kept.sort();
    within("two blobs evicted", || names(&blobs).len() == 2).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(names(&blobs), kept);
    assert_eq!(fs::read(&session_file).unwrap(), b"uncommitted");
}

#[tokio::test]
async fn twenty_downloads_run_eight_at_a_time_in_the_order_their_reads_came() {
    let root = tempfile::tempdir().unwrap();
    let stub = Arc::new(Stub::held());
    let cache = cache(root.path(), stub.clone(), &TestClock::default(), plenty());
    let mut order = Vec::new();
    let mut reads = Vec::new();
    for index in 0..20 {
        let bytes = format!("content {index}").into_bytes();
        let hash = sha256(&bytes);
        stub.serve(&hash, &bytes);
        // Turns between Sessions would grant sess_b's first read before
        // sess_a's last four.
        let session = if index < 12 { "sess_a" } else { "sess_b" };
        reads.push(tokio::spawn(cache.read(needed(
            session,
            &hash,
            bytes.len(),
        ))));
        order.push(hash);
    }
    within("eight downloads started", || {
        stub.started().len() == DOWNLOADS_MAX
    })
    .await;
    for released in 1..=20 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            stub.started(),
            order[..(DOWNLOADS_MAX + released - 1).min(20)],
        );
        stub.permits.as_ref().unwrap().add_permits(1);
        let expected = (DOWNLOADS_MAX + released).min(20);
        within("the next download started", || {
            stub.started().len() == expected
        })
        .await;
    }
    for read in reads {
        read.await.unwrap().unwrap();
    }
    assert_eq!(stub.started(), order);
    assert_eq!(
        stub.most_in_progress.load(Ordering::SeqCst),
        DOWNLOADS_MAX as u64
    );
}

#[tokio::test]
async fn concurrent_readers_open_verified_content_before_low_space_eviction() {
    let root = tempfile::tempdir().unwrap();
    let stub = Arc::new(Stub::held());
    let hash = sha256(b"shared bytes");
    stub.serve(&hash, b"shared bytes");
    let cache = cache(
        root.path(),
        stub.clone(),
        &TestClock::default(),
        Arc::new(|| Ok(0)),
    );
    let mut reads = Vec::new();
    for index in 0..16 {
        reads.push(tokio::spawn(cache.read(needed(
            &format!("sess_{index}"),
            &hash,
            12,
        ))));
    }
    within("first eight downloads", || stub.started().len() == 8).await;
    stub.permits.as_ref().unwrap().add_permits(16);
    for read in reads {
        let mut file = read.await.unwrap().unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"shared bytes");
    }
    // The query is an owner barrier after download publication and eviction.
    assert!(
        cache
            .downloading(&SafeId::new("sess_0").unwrap())
            .await
            .is_empty()
    );
    assert!(names(&root.path().join("cache/blobs")).is_empty());
}
