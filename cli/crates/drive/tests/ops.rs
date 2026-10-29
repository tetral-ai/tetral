//! The operation table of the mounts against a Drive sync owner with a stub
//! `DriveTransfer` that lists a drive, serves its change records and its
//! content by hash: path and exclusion checks, NFC names, the executable
//! bit, local-only entries and their notices, hard links, bases, renames
//! and reads.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use drive::cache::{CacheConfig, ContentCache};
use drive::local::accepted_in_flight;
use drive::mount::{Mounting, Registry};
use drive::ops::{Notice, SetAttr, Special, TOP_INODE};
use drive::state::{FileRow, InflightChange, SyncState, Tables, Write};
use drive::transfer::{
    Binding, Commit, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles, Request,
    Transfer, Upload,
};
use drive::{DriveSync, SyncConfig};
use protocol::drive_transfer::{
    CommitResponse, ListChangesResponse, ListFilesResponse, UploadContentResponse,
};
use protocol::id::{Id, SafeId};
use protocol::ports::{TaskOwnerReply, TaskOwnership};
use rustix::io::Errno;
use serde_json::{Value, json};
use sha2::Digest;
use state::clock::Clock;
use tokio::sync::Semaphore;

const DRIVE: &str = "drive_a";
const SESSION: &str = "sess_a";
const TASK_PID: u32 = 4_242;

fn sha256(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The drive as the stub serves it.
#[derive(Default)]
struct Stub {
    files: Mutex<Vec<Value>>,
    records: Mutex<Vec<Value>>,
    contents: Mutex<HashMap<String, Vec<u8>>>,
    /// Hashes whose downloads are answered `404 content_not_found`.
    missing: Mutex<HashSet<String>>,
    downloads: AtomicU64,
    /// When set, each download waits for a permit.
    permits: Option<Semaphore>,
}

impl Stub {
    fn content(&self, content: &str) -> String {
        let hash = sha256(content.as_bytes());
        self.contents
            .lock()
            .unwrap()
            .insert(hash.clone(), content.as_bytes().to_vec());
        hash
    }

    fn list(&self, path: &str, content: &str, executable: bool) {
        let hash = self.content(content);
        self.files.lock().unwrap().push(json!({
            "path": path,
            "version_id": format!("v_{}", path.replace('/', "_")),
            "content_sha256": hash,
            "size_bytes": content.len(),
            "executable": executable,
        }));
    }
}

impl DriveTransfer for Stub {
    fn list_files(&self, _: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let page = json!({
            "files": self.files.lock().unwrap().clone(),
            "has_more": false,
            "start_seq": 1,
        });
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
    }

    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let after = request.body.after_seq;
        let records: Vec<Value> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record["seq"].as_i64().unwrap() > after)
            .cloned()
            .collect();
        let page = json!({ "records": records, "has_more": false });
        Box::pin(async move { Ok(serde_json::from_value(page).unwrap()) })
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
            let hash = request.body.sha256.as_str().to_owned();
            if self.missing.lock().unwrap().contains(&hash) {
                return Err(Ended::Permanent {
                    status: 404,
                    code: Some("content_not_found"),
                });
            }
            self.downloads.fetch_add(1, Ordering::SeqCst);
            if let Some(permits) = &self.permits {
                permits.acquire().await.unwrap().forget();
            }
            let bytes = self.contents.lock().unwrap()[&hash].clone();
            sink.append(&bytes).map_err(Ended::Sink)
        })
    }
}

/// `TASK_PID` belongs to `task_a`; every other process to no task.
struct NoRegistry;

impl Registry for NoRegistry {
    fn add(&self, _: SafeId, _: DriveSync) {}
    fn remove(&self, _: SafeId) {}
    fn mounted(&self, _: Id, _: SafeId) {}
    fn detached(&self, _: Id, _: SafeId) {}
}

struct Tasks;

impl TaskOwnership for Tasks {
    fn task_of(&self, pid: u32, reply: TaskOwnerReply) {
        reply((pid == TASK_PID).then(|| Id::new("task_a").unwrap()));
    }
}

struct Harness {
    root: tempfile::TempDir,
    stub: Arc<Stub>,
    cache: ContentCache,
    sync: DriveSync,
    drive: Id,
    exclude: Vec<String>,
}

impl Harness {
    /// A Session with `DRIVE` mounted and its first listing done.
    async fn start(stub: Stub, exclude: &[&str]) -> Harness {
        let root = tempfile::tempdir().unwrap();
        let stub = Arc::new(stub);
        let transfer: Arc<dyn DriveTransfer> = stub.clone();
        let cache = ContentCache::spawn(CacheConfig {
            root: root.path().to_path_buf(),
            transfer,
            clock: Clock::seams(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let session = root.path().join("sessions").join(SESSION);
        std::fs::create_dir_all(&session).unwrap();
        let state = SyncState::create(&session.join("sync-state.db"))
            .await
            .unwrap();
        let exclude = exclude.iter().map(|entry| (*entry).to_owned()).collect();
        let sync = spawn(root.path(), &stub, &cache, state);
        let harness = Harness {
            root,
            stub,
            cache,
            sync,
            drive: Id::new(DRIVE).unwrap(),
            exclude,
        };
        harness.mount().await;
        harness
    }

    async fn mount(&self) {
        self.sync.mounted(self.drive.clone(), self.exclude.clone());
        self.caught_up().await;
    }

    async fn caught_up(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.sync.check())
            .await
            .expect("caught up");
    }

    /// Starts the owner again over its sync state with `extra` written in.
    async fn restart(&mut self, extra: Write) {
        let path = self.session().join("sync-state.db");
        let mut state = SyncState::open(&path).await.unwrap();
        state.apply(extra).await.unwrap();
        self.sync = spawn(self.root.path(), &self.stub, &self.cache, state);
        self.mount().await;
    }

    fn session(&self) -> PathBuf {
        self.root.path().join("sessions").join(SESSION)
    }

    fn machine_path(&self, path: &str) -> PathBuf {
        self.session().join("drives").join(DRIVE).join(path)
    }

    fn content_files(&self) -> usize {
        std::fs::read_dir(self.session().join("content").join(DRIVE))
            .map_or(0, |entries| entries.count())
    }

    async fn tables(&self) -> Tables {
        self.sync.tables().await
    }

    async fn row(&self, path: &str) -> Option<FileRow> {
        self.tables()
            .await
            .files
            .get(&(DRIVE.to_owned(), path.to_owned()))
            .cloned()
    }

    /// The inode of the present entry at `path`.
    async fn ino(&self, path: &str) -> u64 {
        let mut ino = TOP_INODE;
        for name in path.split('/').filter(|name| !name.is_empty()) {
            ino = self
                .sync
                .lookup(&self.drive, ino, name.as_bytes())
                .await
                .unwrap_or_else(|error| panic!("lookup {path}: {error}"))
                .ino;
        }
        ino
    }

    async fn create(&self, folder: &str, name: &[u8]) -> Result<u64, Errno> {
        let parent = self.ino(folder).await;
        let attr = self
            .sync
            .create(&self.drive, parent, name, 0o644, Some(TASK_PID))
            .await?;
        Ok(attr.ino)
    }

    async fn mkdir(&self, folder: &str, name: &str) -> Result<u64, Errno> {
        let parent = self.ino(folder).await;
        let attr = self
            .sync
            .mkdir(&self.drive, parent, name.as_bytes(), Some(TASK_PID))
            .await?;
        Ok(attr.ino)
    }

    async fn write(&self, path: &str, bytes: &str) -> Result<u32, Errno> {
        let ino = self.ino(path).await;
        self.sync
            .write(&self.drive, ino, 0, bytes.as_bytes(), Some(TASK_PID))
            .await
    }

    async fn read(&self, path: &str) -> Result<String, Errno> {
        let ino = self.ino(path).await;
        let bytes = self.sync.read(&self.drive, ino, 0, 4_096).await?;
        Ok(String::from_utf8(bytes).unwrap())
    }

    async fn rename(&self, from: &str, to_folder: &str, to_name: &[u8]) -> Result<(), Errno> {
        let (folder, name) = from.rsplit_once('/').unwrap_or(("", from));
        let parent = self.ino(folder).await;
        let new_parent = self.ino(to_folder).await;
        self.sync
            .rename(
                &self.drive,
                parent,
                name.as_bytes(),
                new_parent,
                to_name,
                Some(TASK_PID),
            )
            .await
    }

    async fn chmod(&self, path: &str, mode: u32) {
        let ino = self.ino(path).await;
        let set = SetAttr {
            mode: Some(mode),
            ..SetAttr::default()
        };
        self.sync
            .setattr(&self.drive, ino, set, Some(TASK_PID))
            .await
            .unwrap();
    }

    /// A remote change of `path` to `content`, applied by a catch-up.
    async fn remote(&self, seq: i64, path: &str, content: &str) {
        let hash = self.stub.content(content);
        self.stub.records.lock().unwrap().push(json!({
            "seq": seq, "index": 0, "path": path, "operation": "modified",
            "version_id": format!("v{seq}"), "content_sha256": hash,
            "size_bytes": content.len(), "executable": false,
        }));
        self.sync.drive_changed(self.drive.clone(), seq);
        self.caught_up().await;
    }
}

fn spawn(root: &Path, stub: &Arc<Stub>, cache: &ContentCache, state: SyncState) -> DriveSync {
    let sync = DriveSync::spawn(SyncConfig {
        session_id: SafeId::new(SESSION).unwrap(),
        root: root.to_path_buf(),
        state,
        transfer: stub.clone(),
        cache: cache.clone(),
        tasks: Arc::new(Tasks),
        mounting: Mounting::Unavailable("no FUSE: test".to_owned()),
        registry: Arc::new(NoRegistry),
    });
    sync.binding_current(Binding {
        connection_id: Id::new("conn_a").unwrap(),
        binding_id: Id::new("bind_a").unwrap(),
    });
    sync
}

fn key(path: &str) -> (String, String) {
    (DRIVE.to_owned(), path.to_owned())
}

#[tokio::test]
async fn names_that_break_the_path_checks_fail_with_einval_and_store_nothing() {
    let stub = Stub::default();
    stub.list("notes.md", "notes", false);
    let harness = Harness::start(stub, &[]).await;
    // Folders of 200-byte segments put a path at 1,024 or 1,025 bytes.
    let long = "f".repeat(200);
    let mut folder = String::new();
    for _ in 0..4 {
        harness.mkdir(&folder, &long).await.unwrap();
        folder = if folder.is_empty() {
            long.clone()
        } else {
            format!("{folder}/{long}")
        };
    }
    let fill = 1_024 - folder.len() - 1;
    let before = harness.tables().await;
    let refused: Vec<(&str, Vec<u8>)> = vec![
        ("byte 0xff", b"a\xffb".to_vec()),
        ("U+0009", "a\tb".as_bytes().to_vec()),
        ("U+200B", "a\u{200b}b".as_bytes().to_vec()),
        ("a segment of 256 bytes", "s".repeat(256).into_bytes()),
        (
            "31 non-starters in a row",
            format!("a{}", "\u{301}".repeat(31)).into_bytes(),
        ),
    ];
    for (row, name) in &refused {
        let created = harness.create("", name).await;
        assert_eq!(created, Err(Errno::INVAL), "{row}");
    }
    let created = harness
        .create(&folder, "n".repeat(fill + 1).as_bytes())
        .await;
    assert_eq!(created, Err(Errno::INVAL), "a path of 1,025 bytes");
    let renamed = harness.rename("notes.md", "", "a\tb".as_bytes()).await;
    assert_eq!(renamed, Err(Errno::INVAL), "a rename to U+0009");
    assert_eq!(harness.tables().await, before);
    assert_eq!(harness.content_files(), 0);

    harness
        .create(&folder, "n".repeat(fill).as_bytes())
        .await
        .expect("a path of 1,024 bytes");
    harness
        .create("", "s".repeat(255).as_bytes())
        .await
        .expect("a segment of 255 bytes");
}

#[tokio::test]
async fn a_name_in_nfd_reaches_its_nfc_entry_and_is_recorded_under_it() {
    let harness = Harness::start(Stub::default(), &[]).await;
    let nfd = "cafe\u{301}.txt";
    let nfc = "caf\u{e9}.txt";
    let ino = harness.create("", nfd.as_bytes()).await.unwrap();
    let found = |name: &'static str| {
        let sync = harness.sync.clone();
        let drive = harness.drive.clone();
        async move { sync.lookup(&drive, TOP_INODE, name.as_bytes()).await }
    };
    assert_eq!(found(nfd).await.unwrap().ino, ino);
    assert_eq!(found(nfc).await.unwrap().ino, ino);
    let listed = harness
        .sync
        .readdir(&harness.drive, TOP_INODE)
        .await
        .unwrap();
    let names: Vec<&str> = listed.iter().map(|entry| entry.name.as_str()).collect();
    assert_eq!(names, [nfc]);
    let pending: Vec<String> = harness
        .tables()
        .await
        .pending_changes
        .keys()
        .map(|(_, path)| path.clone())
        .collect();
    assert_eq!(pending, [nfc]);
}

#[tokio::test]
async fn excluded_paths_fail_with_eacces_and_record_nothing() {
    let stub = Stub::default();
    stub.list("notes.md", "notes", false);
    let mut harness = Harness::start(stub, &["datasets/"]).await;
    // A local folder at the excluded path, as a binding that excludes it
    // later would leave it.
    let top = harness.row("").await.unwrap();
    let folder = FileRow {
        path: "datasets".to_owned(),
        file_id: 90,
        origin: Some("local".to_owned()),
        ..top
    };
    harness
        .restart(Write {
            files: vec![folder],
            ..Write::default()
        })
        .await;
    let before = harness.tables().await;
    let drive = harness.drive.clone();
    let datasets = 90;
    let refused = [
        (
            "create datasets/b.csv",
            harness
                .sync
                .create(&drive, datasets, b"b.csv", 0o644, Some(TASK_PID))
                .await
                .err(),
        ),
        (
            "open datasets/a.csv for append with create",
            harness
                .sync
                .create(&drive, datasets, b"a.csv", 0o644, Some(TASK_PID))
                .await
                .err(),
        ),
        (
            "make the folder datasets",
            harness.mkdir("", "datasets").await.err(),
        ),
        (
            "rename notes.md to datasets/notes.md",
            harness
                .sync
                .rename(&drive, TOP_INODE, b"notes.md", datasets, b"notes.md", None)
                .await
                .err(),
        ),
    ];
    for (row, error) in refused {
        assert_eq!(error, Some(Errno::ACCESS), "{row}");
    }
    assert_eq!(harness.tables().await, before);
    assert_eq!(
        harness.sync.lookup(&drive, TOP_INODE, b"datasets").await,
        Err(Errno::NOENT)
    );
    assert_eq!(harness.read("notes.md").await.unwrap(), "notes");
}

#[tokio::test]
async fn of_the_permissions_only_the_executable_bit_is_recorded() {
    let stub = Stub::default();
    stub.list("run.sh", "echo", false);
    stub.list("data.txt", "data", false);
    let harness = Harness::start(stub, &[]).await;
    let hash = sha256(b"echo");

    harness.chmod("run.sh", 0o755).await;
    let change = harness.tables().await.pending_changes[&key("run.sh")].clone();
    assert_eq!(
        (change.change.as_str(), change.content_sha256.as_deref()),
        ("write", Some(hash.as_str()))
    );
    assert_eq!(change.executable, Some(true));
    assert_eq!(
        harness
            .sync
            .getattr(&harness.drive, harness.ino("run.sh").await)
            .await
            .unwrap()
            .mode,
        0o755
    );

    harness.chmod("run.sh", 0o600).await;
    let change = harness.tables().await.pending_changes[&key("run.sh")].clone();
    assert_eq!(change.executable, Some(false));
    assert_eq!(change.content_sha256.as_deref(), Some(hash.as_str()));

    // `chown` and `touch` reach the table as attribute changes without a
    // size or a mode.
    let ino = harness.ino("data.txt").await;
    for set in [
        SetAttr::default(),
        SetAttr {
            mtime_ns: Some(7),
            ..SetAttr::default()
        },
    ] {
        harness
            .sync
            .setattr(&harness.drive, ino, set, Some(TASK_PID))
            .await
            .unwrap();
    }
    assert!(
        !harness
            .tables()
            .await
            .pending_changes
            .contains_key(&key("data.txt"))
    );
}

#[tokio::test]
async fn local_only_entries_record_nothing_and_leave_one_notice_each() {
    let stub = Stub::default();
    let harness = Harness::start(stub, &[]).await;
    harness.mkdir("", "node_modules").await.unwrap();
    harness.create("node_modules", b"x.js").await.unwrap();
    harness.write("node_modules/x.js", "x").await.unwrap();
    harness.mkdir("", "proj").await.unwrap();
    harness.mkdir("proj", ".git").await.unwrap();
    harness.create("proj/.git", b"HEAD").await.unwrap();
    harness
        .sync
        .symlink(&harness.drive, TOP_INODE, b"link", b"proj", Some(TASK_PID))
        .await
        .unwrap();
    harness
        .sync
        .mknod(
            &harness.drive,
            TOP_INODE,
            b"pipe",
            Special::Fifo,
            Some(TASK_PID),
        )
        .await
        .unwrap();
    // `ln -sf` makes the same link again: still one notice.
    harness
        .sync
        .unlink(&harness.drive, TOP_INODE, b"link", Some(TASK_PID))
        .await
        .unwrap();
    harness
        .sync
        .symlink(&harness.drive, TOP_INODE, b"link", b"proj", Some(TASK_PID))
        .await
        .unwrap();
    // A process outside every task leaves no notice.
    harness
        .sync
        .symlink(&harness.drive, TOP_INODE, b"other", b"proj", Some(1))
        .await
        .unwrap();

    let tables = harness.tables().await;
    assert!(
        tables.pending_changes.is_empty(),
        "{:?}",
        tables.pending_changes
    );
    let reasons: Vec<(&str, Option<&str>)> = [
        "node_modules/x.js",
        "proj",
        "proj/.git/HEAD",
        "link",
        "pipe",
    ]
    .iter()
    .map(|path| (*path, tables.files[&key(path)].local_only.as_deref()))
    .collect();
    assert_eq!(
        reasons,
        [
            ("node_modules/x.js", Some("ignored")),
            ("proj", Some("git_repository")),
            ("proj/.git/HEAD", Some("git_repository")),
            ("link", Some("symlink")),
            ("pipe", Some("special_file")),
        ]
    );
    let task = Id::new("task_a").unwrap();
    let notices = harness.sync.notices(&task).await;
    assert_eq!(
        notices,
        [
            Notice {
                path: harness.machine_path("proj"),
                reason: "git_repository",
            },
            Notice {
                path: harness.machine_path("link"),
                reason: "symlink",
            },
            Notice {
                path: harness.machine_path("pipe"),
                reason: "special_file",
            },
        ]
    );
    assert!(harness.sync.notices(&task).await.is_empty());
}

#[tokio::test]
async fn drive_files_at_local_only_paths_are_shown_and_replaced_only_while_unwritten() {
    let stub = Stub::default();
    stub.list("node_modules/x.js", "v1", false);
    stub.list("proj/a.txt", "a", false);
    let harness = Harness::start(stub, &[]).await;
    assert_eq!(harness.read("node_modules/x.js").await.unwrap(), "v1");
    harness.remote(2, "node_modules/x.js", "v2").await;
    assert_eq!(harness.read("node_modules/x.js").await.unwrap(), "v2");

    harness.write("node_modules/x.js", "lo").await.unwrap();
    assert!(harness.tables().await.pending_changes.is_empty());
    harness.remote(3, "node_modules/x.js", "v3").await;
    let row = harness.row("node_modules/x.js").await.unwrap();
    assert_eq!(row.synced_sha256, Some(sha256(b"v3")));
    assert_eq!(harness.read("node_modules/x.js").await.unwrap(), "lo");

    // A write pending before its folder became a Git repository stays.
    harness.write("proj/a.txt", "b").await.unwrap();
    harness.mkdir("proj", ".git").await.unwrap();
    let pending: Vec<String> = harness
        .tables()
        .await
        .pending_changes
        .keys()
        .map(|(_, path)| path.clone())
        .collect();
    assert_eq!(pending, ["proj/a.txt"]);
}

#[tokio::test]
async fn a_hard_link_fails_with_eperm_and_stores_nothing() {
    let stub = Stub::default();
    stub.list("a.txt", "a", false);
    let harness = Harness::start(stub, &[]).await;
    let before = harness.tables().await;
    let ino = harness.ino("a.txt").await;
    assert_eq!(
        harness.sync.link(&harness.drive, ino, TOP_INODE).await,
        Err(Errno::PERM)
    );
    assert_eq!(harness.tables().await, before);
    assert_eq!(harness.content_files(), 0);
}

#[tokio::test]
async fn every_pending_change_carries_its_base_and_a_rename_uploads_nothing() {
    let stub = Stub::default();
    stub.list("a.txt", "a", false);
    stub.list("b.txt", "b", false);
    stub.list("c.txt", "c", false);
    let mut harness = Harness::start(stub, &[]).await;
    // A commit carrying c.txt with new content is in flight.
    let committed = sha256(b"c2");
    harness
        .restart(Write {
            inflight: vec![InflightChange {
                drive_id: DRIVE.to_owned(),
                path: "c.txt".to_owned(),
                change: "write".to_owned(),
                base_sha256: Some(sha256(b"c")),
                content_sha256: Some(committed.clone()),
                executable: Some(false),
                changed_during_flight: false,
            }],
            ..Write::default()
        })
        .await;

    harness.write("a.txt", "A").await.unwrap();
    let content = harness.session().join("content").join(DRIVE);
    let a = harness.row("a.txt").await.unwrap();
    assert_eq!(
        std::fs::read(content.join(a.file_id.to_string())).unwrap(),
        b"A",
        "the write is in the content file when it returns"
    );
    harness.create("", b"new.txt").await.unwrap();
    harness.write("c.txt", "c3").await.unwrap();
    harness.rename("b.txt", "", b"b2.txt").await.unwrap();

    let tables = harness.tables().await;
    let change = |path: &str| tables.pending_changes[&key(path)].clone();
    let bases: Vec<(&str, &str, Option<String>)> = ["a.txt", "new.txt", "c.txt", "b.txt", "b2.txt"]
        .iter()
        .map(|path| {
            let change = change(path);
            (
                *path,
                if change.change == "write" {
                    "write"
                } else {
                    "delete"
                },
                change.base_sha256,
            )
        })
        .collect();
    assert_eq!(
        bases,
        [
            ("a.txt", "write", Some(sha256(b"a"))),
            ("new.txt", "write", None),
            ("c.txt", "write", None),
            ("b.txt", "delete", Some(sha256(b"b"))),
            ("b2.txt", "write", None),
        ]
    );
    let moved = change("b2.txt");
    assert_eq!(moved.content_sha256, Some(sha256(b"b")));
    assert!(moved.uploaded, "the drive holds the moved content");
    assert!(tables.inflight_commits[&key("c.txt")].changed_during_flight);
    let answered = accepted_in_flight(&tables, DRIVE, "c.txt");
    assert_eq!(answered.pending[0].base_sha256, Some(committed));
}

#[tokio::test]
async fn reads_download_a_missing_blob_once_and_fail_at_once_on_a_permanent_error() {
    let stub = Stub {
        permits: Some(Semaphore::new(0)),
        ..Stub::default()
    };
    stub.list("a.txt", "alpha", false);
    stub.list("gone.txt", "gone", false);
    stub.missing.lock().unwrap().insert(sha256(b"gone"));
    let harness = Harness::start(stub, &[]).await;

    let first = tokio::spawn({
        let (sync, drive, ino) = (
            harness.sync.clone(),
            harness.drive.clone(),
            harness.ino("a.txt").await,
        );
        async move { sync.read(&drive, ino, 0, 64).await }
    });
    let second = tokio::spawn({
        let (sync, drive, ino) = (
            harness.sync.clone(),
            harness.drive.clone(),
            harness.ino("a.txt").await,
        );
        async move { sync.read(&drive, ino, 0, 64).await }
    });
    // While the download waits, other operations are answered and the read
    // is listed as downloading.
    let mut downloading = Vec::new();
    for _ in 0..200 {
        downloading = harness.sync.downloading().await;
        if !downloading.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(downloading.len(), 1);
    assert_eq!(downloading[0].path, harness.machine_path("a.txt"));
    assert_eq!(downloading[0].size_bytes, 5);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), harness.read("gone.txt"))
            .await
            .expect("a permanent failure fails at once"),
        Err(Errno::IO)
    );
    harness.stub.permits.as_ref().unwrap().add_permits(1);
    assert_eq!(first.await.unwrap().unwrap(), b"alpha");
    assert_eq!(second.await.unwrap().unwrap(), b"alpha");
    assert_eq!(harness.stub.downloads.load(Ordering::SeqCst), 1);
    assert!(harness.sync.downloading().await.is_empty());
}

#[tokio::test]
async fn a_renamed_file_keeps_its_id_and_cache_content_and_its_source_keeps_the_drive_version() {
    let stub = Stub::default();
    stub.list("a.md", "alpha", false);
    let harness = Harness::start(stub, &[]).await;
    assert_eq!(harness.read("a.md").await.unwrap(), "alpha");
    assert_eq!(harness.stub.downloads.load(Ordering::SeqCst), 1);
    let before = harness.row("a.md").await.unwrap();
    harness.rename("a.md", "", b"b.md").await.unwrap();

    let source = harness.row("a.md").await.unwrap();
    assert!(!source.present);
    assert_eq!(source.version_id, before.version_id);
    assert_eq!(source.synced_sha256, before.synced_sha256);
    assert_ne!(source.file_id, before.file_id);
    let target = harness.row("b.md").await.unwrap();
    assert!(target.present);
    assert_eq!(target.file_id, before.file_id);
    assert_eq!(target.content.as_deref(), Some("cache"));
    assert_eq!(target.local_sha256, before.local_sha256);
    assert_eq!((target.version_id, target.synced_sha256), (None, None));
    assert_eq!(
        harness
            .sync
            .lookup(&harness.drive, TOP_INODE, b"a.md")
            .await,
        Err(Errno::NOENT)
    );

    // The blob is evicted; the next read downloads the same hash again.
    std::fs::remove_file(
        harness
            .root
            .path()
            .join("cache/blobs")
            .join(sha256(b"alpha")),
    )
    .unwrap();
    assert_eq!(harness.read("b.md").await.unwrap(), "alpha");
    assert_eq!(harness.stub.downloads.load(Ordering::SeqCst), 2);
}
