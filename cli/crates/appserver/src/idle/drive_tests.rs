//! IdleExit composed with real Connection and DriveSync owners and SQLite.
//! Only the transfer service, collection delay and monotonic clock are controlled.
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use connection::ReadyFields;
use drive::cache::{CacheConfig, ContentCache};
use drive::state::SyncState;
use drive::transfer::*;
use drive::{DriveSync, SyncConfig};
use protocol::drive_transfer::*;
use protocol::id::SafeId;
use protocol::ports::*;
use serde_json::json;
use state::clock::Clock;
use tokio::sync::oneshot;

const CONTENT: &[u8] = b"pending content";
const HASH: &str = "c9767394e677bcab0b7fc34372541dff50fac0501d7f268866bdcfb092736e75";

struct NoSessions;
impl SessionFactory for NoSessions {
    fn create(
        &self,
        _: SafeId,
        _: Arc<dyn ConnectionPort>,
        _: Arc<dyn ReportSink>,
    ) -> Arc<dyn SessionHandle> {
        panic!("no preparation in this owner fixture")
    }
    fn unbound_tool(&self, _: ToolCall, _: Arc<dyn ReportSink>) {
        panic!("no tools")
    }
}
struct Ports;
impl TaskOwnership for Ports {
    fn task_of(&self, _: u32, reply: TaskOwnerReply) {
        reply(Some(id("task_writer")));
    }
}
impl ReportSink for Ports {
    fn submit(&self, _: Submission) -> ReportId {
        panic!("no failures expected")
    }
    fn cancel(&self, _: ReportId) {}
    fn add_final(&self, _: ReportId, _: Id) {}
}
impl drive::mount::Registry for Ports {
    fn add(&self, _: SafeId, _: DriveSync) {}
    fn remove(&self, _: SafeId) {}
    fn mounted(&self, _: Id, _: SafeId) {}
    fn detached(&self, _: Id, _: SafeId) {}
}

enum Call {
    Upload(oneshot::Sender<Result<UploadContentResponse, Ended>>),
    Commit(oneshot::Sender<Result<CommitResponse, Ended>>),
}
impl Call {
    fn accept(self) {
        match self {
            Call::Upload(answer) => answer
                .send(Ok(serde_json::from_value(json!({
                    "content_sha256":HASH, "size_bytes":CONTENT.len()
                }))
                .unwrap()))
                .unwrap(),
            Call::Commit(answer) => answer
                .send(Ok(serde_json::from_value(json!({
                    "seq":2, "results":[{"status":"accepted", "version_id":"version_new"}]
                }))
                .unwrap()))
                .unwrap(),
        }
    }
    fn cancelled(&self) -> bool {
        match self {
            Call::Upload(answer) => answer.is_closed(),
            Call::Commit(answer) => answer.is_closed(),
        }
    }
}
struct TransferStub(mpsc::UnboundedSender<Call>);
impl DriveTransfer for TransferStub {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        Box::pin(async {
            Ok(
                serde_json::from_value(json!({"files":[], "has_more":false, "start_seq":1}))
                    .unwrap(),
            )
        })
    }
    fn list_changes(&self, _: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        Box::pin(async {
            Ok(serde_json::from_value(json!({"records":[], "has_more":false})).unwrap())
        })
    }
    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        Box::pin(async move {
            assert_eq!(request.body.sha256.as_str(), HASH);
            let bytes = match request.body.content {
                Content::File { path, size } => {
                    assert_eq!(size, CONTENT.len() as u64);
                    std::fs::read(path).unwrap()
                }
                Content::Bytes(bytes) => bytes.to_vec(),
            };
            assert_eq!(bytes, CONTENT);
            let (answer, wait) = oneshot::channel();
            self.0
                .send(Call::Upload(answer))
                .unwrap_or_else(|_| panic!("live fixture"));
            wait.await.unwrap()
        })
    }
    fn commit(&self, request: Request<Commit>) -> Transfer<'_, CommitResponse> {
        Box::pin(async move {
            assert_eq!(request.body.changes.len(), 1);
            let CommitChange::Write {
                path,
                content_sha256,
                ..
            } = &request.body.changes[0]
            else {
                panic!("write")
            };
            assert_eq!(path.as_str(), "file");
            assert_eq!(content_sha256.as_str(), HASH);
            let (answer, wait) = oneshot::channel();
            self.0
                .send(Call::Commit(answer))
                .unwrap_or_else(|_| panic!("live fixture"));
            wait.await.unwrap()
        })
    }
    fn download_content<'a>(
        &'a self,
        _: Request<Download>,
        _: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        panic!("the complete local write requires no download")
    }
}
struct Fixture {
    root: tempfile::TempDir,
    connection: Connection,
    clock: Clock,
    now: Arc<AtomicU64>,
    calls: mpsc::UnboundedReceiver<Call>,
    transfer: Arc<TransferStub>,
    cache: ContentCache,
    syncs: Vec<(String, DriveSync)>,
}
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), f)
        .await
        .expect("owner finishes within 3 real seconds")
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let bootstrap = protocol::bootstrap::Bootstrap::decode(br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_idle","credential_id":"cred_idle","connection_key":"tetral_ck_idle","expires_at":"2000-01-01T00:00:00Z"}"#).unwrap();
        let connection = Connection::spawn(connection::Config {
            server_url: bootstrap.server_url.as_str().into(),
            instance_id: bootstrap.instance_id,
            credential: connection::Credential {
                credential_id: bootstrap.credential_id,
                key: bootstrap.connection_key,
                expires_at: bootstrap.expires_at,
            },
            state_root: state::paths::StateRoot::new(root.path().as_os_str()).unwrap(),
            ready: ReadyFields {
                cli_version: serde_json::from_value(json!("0.1.0")).unwrap(),
                capabilities: serde_json::from_value(
                    json!({"session_preparation":true,"drives":{"mount":false}}),
                )
                .unwrap(),
            },
            started: Duration::ZERO,
            sessions: Arc::new(NoSessions),
            activity: Arc::new(NoActivity),
            times: connection::Times::default(),
        });
        let now = Arc::new(AtomicU64::new(0));
        let read = now.clone();
        let clock = Clock::new(
            move || Duration::from_secs(read.load(Ordering::SeqCst)),
            Duration::from_millis(1),
        );
        let (calls, received) = mpsc::unbounded_channel();
        let transfer = Arc::new(TransferStub(calls));
        let cache = ContentCache::spawn(CacheConfig {
            root: root.path().to_owned(),
            transfer: transfer.clone(),
            clock: clock.clone(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        Self {
            root,
            connection,
            clock,
            now,
            calls: received,
            transfer,
            cache,
            syncs: Vec::new(),
        }
    }
    fn idle(&self) -> IdleExit {
        let mut idle = IdleExit::new(
            Duration::from_secs(60),
            Duration::ZERO,
            self.connection.clone(),
            id("inst_idle"),
        );
        idle.clock = self.clock.clone();
        idle
    }
    async fn session(&mut self, name: &str) -> DriveSync {
        let directory = self.root.path().join("sessions").join(name);
        std::fs::create_dir_all(&directory).unwrap();
        let state = SyncState::create(&directory.join("sync-state.db"))
            .await
            .unwrap();
        let sync = DriveSync::spawn(SyncConfig {
            commit_batch_size: drive::commit::MAX_CHANGES,
            session_id: SafeId::new(name).unwrap(),
            root: self.root.path().to_owned(),
            state,
            transfer: self.transfer.clone(),
            cache: self.cache.clone(),
            tasks: Arc::new(Ports),
            mounting: drive::mount::Mounting::Unavailable("owner test".into()),
            registry: Arc::new(Ports),
            clock: self.clock.clone(),
            reports: Arc::new(Ports),
        });
        sync.binding_current(Binding {
            connection_id: id("conn_idle"),
            binding_id: id("bind_idle"),
        });
        sync.mounted(id("drive_idle"), Vec::new());
        within(sync.check()).await;
        let file = sync
            .create(
                &id("drive_idle"),
                drive::ops::TOP_INODE,
                b"file",
                0o644,
                Some(1),
            )
            .await
            .unwrap();
        sync.write(&id("drive_idle"), file.ino, 0, CONTENT, Some(1))
            .await
            .unwrap();
        assert_eq!(sync.tables().await.pending_changes.len(), 1);
        self.connection
            .add_drive_sync(SafeId::new(name).unwrap(), sync.clone());
        self.syncs.push((name.into(), sync.clone()));
        sync
    }
    async fn call(&mut self) -> Call {
        within(self.calls.recv()).await.unwrap()
    }
    async fn retained(&self) {
        for (name, sync) in &self.syncs {
            let tables = sync.tables().await;
            assert_eq!(tables.pending_changes.len(), 1);
            let row = &tables.files[&("drive_idle".into(), "file".into())];
            let bytes = sync
                .read(&id("drive_idle"), row.file_id as u64, 0, 4096)
                .await
                .unwrap();
            assert_eq!(bytes, CONTENT);
            // Independent SQLite reader and physical content remain after timeout.
            let persisted = SyncState::open(
                &self
                    .root
                    .path()
                    .join("sessions")
                    .join(name)
                    .join("sync-state.db"),
            )
            .await
            .unwrap();
            assert_eq!(persisted.tables().pending_changes.len(), 1);
            assert_eq!(persisted.tables().files, tables.files);
            persisted.close().await;
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for (_, sync) in &self.syncs {
            sync.detach_all();
        }
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
    for at in [10, 20, 29] {
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
    f.now.store(30, Ordering::SeqCst);
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
