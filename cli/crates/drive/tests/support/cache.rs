//! Shared cache owner fixture with controlled downloads and clock.
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use drive::cache::{CacheConfig, ContentCache, FreeSpace, Limits, Needed};
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

pub fn sha256(bytes: &[u8]) -> Sha256 {
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
pub struct Stub {
    pub contents: Mutex<HashMap<Sha256, Vec<u8>>>,
    /// The hashes in the order their downloads started.
    pub started: Mutex<Vec<Sha256>>,
    pub in_progress: AtomicU64,
    pub most_in_progress: AtomicU64,
    pub permits: Option<Semaphore>,
}

impl Stub {
    pub fn held() -> Stub {
        Stub {
            permits: Some(Semaphore::new(0)),
            ..Stub::default()
        }
    }

    pub fn serve(&self, sha256: &Sha256, bytes: &[u8]) {
        self.contents
            .lock()
            .unwrap()
            .insert(sha256.clone(), bytes.to_vec());
    }

    pub fn started(&self) -> Vec<Sha256> {
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
pub struct TestClock(Arc<AtomicU64>);

impl TestClock {
    pub fn clock(&self) -> Clock {
        let nanos = self.0.clone();
        Clock::new(
            move || Duration::from_nanos(nanos.load(Ordering::SeqCst)),
            Duration::from_millis(1),
        )
    }

    pub fn advance(&self, step: Duration) {
        self.0
            .fetch_add(u64::try_from(step.as_nanos()).unwrap(), Ordering::SeqCst);
    }
}

pub fn cache(
    root: &Path,
    stub: Arc<Stub>,
    clock: &TestClock,
    free_space: FreeSpace,
) -> ContentCache {
    cache_with_limits(root, stub, clock, free_space, Limits::default())
}

pub fn cache_with_limits(
    root: &Path,
    stub: Arc<Stub>,
    clock: &TestClock,
    free_space: FreeSpace,
    limits: Limits,
) -> ContentCache {
    ContentCache::spawn_with_limits(
        CacheConfig {
            root: root.to_path_buf(),
            transfer: Arc::new(super::Checked(stub)),
            clock: clock.clock(),
            free_space,
        },
        limits,
    )
    .unwrap()
}

pub fn plenty() -> FreeSpace {
    Arc::new(|| Ok(u64::MAX))
}

pub fn needed(session: &str, sha256: &Sha256, size: usize) -> Needed {
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

pub fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

pub async fn within(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
