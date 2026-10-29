//! The Content cache owner against a stub `DriveTransfer` whose downloads
//! the test holds and releases: the digest check, one blob per hash across
//! drive syncs, eviction by last read time under a free-space reading the
//! test gives, and the download slots.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use drive::cache::{CacheConfig, ContentCache, DOWNLOADS_MAX, FREE_SPACE_MIN, FreeSpace, Needed};
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::field::Sha256;
use protocol::id::{Id, SafeId};
use sha2::Digest;
use state::clock::Clock;
use tokio::sync::{Semaphore, watch};

fn sha256(bytes: &[u8]) -> Sha256 {
    let digest = sha2::Sha256::digest(bytes);
    Sha256::new(
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
    .unwrap()
}

/// A stub that serves `contents` by hash, each download held until the test
/// releases a permit, and counts the downloads in progress.
#[derive(Default)]
struct Stub {
    contents: Mutex<HashMap<Sha256, Vec<u8>>>,
    /// The hashes in the order their downloads started.
    started: Mutex<Vec<Sha256>>,
    in_progress: AtomicU64,
    most_in_progress: AtomicU64,
    permits: Option<Semaphore>,
}

impl Stub {
    fn held() -> Stub {
        Stub {
            permits: Some(Semaphore::new(0)),
            ..Stub::default()
        }
    }

    fn serve(&self, sha256: &Sha256, bytes: &[u8]) {
        self.contents
            .lock()
            .unwrap()
            .insert(sha256.clone(), bytes.to_vec());
    }

    fn started(&self) -> Vec<Sha256> {
        self.started.lock().unwrap().clone()
    }
}

impl DriveTransfer for Stub {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        unimplemented!()
    }

    fn list_changes(&self, _: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        unimplemented!()
    }

    fn upload_content(&self, _: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        unimplemented!()
    }

    fn commit(&self, _: Request<Commit>) -> Transfer<'_, CommitResponse> {
        unimplemented!()
    }

    fn download_content<'a>(
        &'a self,
        request: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        Box::pin(async move {
            let sha256 = request.body.sha256;
            self.started.lock().unwrap().push(sha256.clone());
            let now = self.in_progress.fetch_add(1, Ordering::SeqCst) + 1;
            self.most_in_progress.fetch_max(now, Ordering::SeqCst);
            if let Some(permits) = &self.permits {
                permits.acquire().await.unwrap().forget();
            }
            let bytes = self.contents.lock().unwrap().get(&sha256).cloned();
            self.in_progress.fetch_sub(1, Ordering::SeqCst);
            let Some(bytes) = bytes else {
                return Err(Ended::Permanent {
                    status: 404,
                    code: Some("content_not_found"),
                });
            };
            sink.restart().map_err(Ended::Sink)?;
            sink.append(&bytes).map_err(Ended::Sink)
        })
    }
}

/// A clock the test moves.
#[derive(Clone, Default)]
struct TestClock(Arc<AtomicU64>);

impl TestClock {
    fn clock(&self) -> Clock {
        let nanos = self.0.clone();
        Clock::new(
            move || Duration::from_nanos(nanos.load(Ordering::SeqCst)),
            Duration::from_millis(1),
        )
    }

    fn advance(&self, step: Duration) {
        self.0
            .fetch_add(u64::try_from(step.as_nanos()).unwrap(), Ordering::SeqCst);
    }
}

fn cache(root: &Path, stub: Arc<Stub>, clock: &TestClock, free_space: FreeSpace) -> ContentCache {
    ContentCache::spawn(CacheConfig {
        root: root.to_path_buf(),
        transfer: stub,
        clock: clock.clock(),
        free_space,
    })
    .unwrap()
}

fn plenty() -> FreeSpace {
    Arc::new(|| Ok(u64::MAX))
}

fn needed(session: &str, sha256: &Sha256, size: usize) -> Needed {
    Needed {
        session_id: SafeId::new(session).unwrap(),
        drive_id: Id::new("drive_a").unwrap(),
        sha256: sha256.clone(),
        size: size as u64,
        binding: watch::channel(Some(Binding {
            connection_id: Id::new("conn_a").unwrap(),
            binding_id: Id::new("bind_a").unwrap(),
        }))
        .1,
    }
}

fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

async fn within(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

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
        let session = if index % 2 == 0 { "sess_a" } else { "sess_b" };
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
