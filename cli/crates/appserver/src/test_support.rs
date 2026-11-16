//! Real Connection and DriveSync owners with SQLite, controlled transfer replies
//! and a private monotonic clock for App Server composition tests.
use connection::Connection;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

use connection::ReadyFields;
use drive::cache::{CacheConfig, ContentCache};
use drive::state::SyncState;
use drive::transfer::*;
use drive::{DriveSync, SyncConfig};
use protocol::drive_transfer::*;
use protocol::id::{Id, SafeId};
use protocol::ports::*;
use serde_json::json;
use state::clock::Clock;
use tokio::sync::oneshot;

pub(crate) const CONTENT: &[u8] = b"pending content";
pub(crate) const HASH: &str = "c9767394e677bcab0b7fc34372541dff50fac0501d7f268866bdcfb092736e75";

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

pub(crate) enum Call {
    Upload(oneshot::Sender<Result<UploadContentResponse, Ended>>),
    Commit(oneshot::Sender<Result<CommitResponse, Ended>>),
}
impl Call {
    pub(crate) fn accept(self) {
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
    pub(crate) fn cancelled(&self) -> bool {
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
pub(crate) struct Fixture {
    pub(crate) root: tempfile::TempDir,
    pub(crate) connection: Connection,
    pub(crate) clock: Clock,
    pub(crate) now: Arc<AtomicU64>,
    pub(crate) calls: mpsc::UnboundedReceiver<Call>,
    pub(crate) transfer: Arc<dyn DriveTransfer>,
    pub(crate) cache: ContentCache,
    pub(crate) syncs: Vec<(String, DriveSync)>,
}
pub(crate) fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
pub(crate) async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), f)
        .await
        .expect("owner finishes within 3 real seconds")
}
impl Fixture {
    pub(crate) fn new() -> Self {
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
            mcp: Arc::new(protocol::ports::NoMcp),
            times: connection::Times::default(),
        });
        let now = Arc::new(AtomicU64::new(0));
        let read = now.clone();
        let clock = Clock::new(
            move || Duration::from_secs(read.load(Ordering::SeqCst)),
            Duration::from_millis(1),
        );
        let (calls, received) = mpsc::unbounded_channel();
        let transfer: Arc<dyn DriveTransfer> = Arc::new(Checked(Arc::new(TransferStub(calls))));
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
    pub(crate) async fn session(&mut self, name: &str) -> DriveSync {
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
    pub(crate) async fn call(&mut self) -> Call {
        within(self.calls.recv()).await.unwrap()
    }
    pub(crate) async fn retained(&self) {
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
