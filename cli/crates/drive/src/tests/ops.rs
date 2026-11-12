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
use drive::mount::{Mounting, Registry};
use drive::ops::{Notice, SetAttr, Special, TOP_INODE};
use drive::state::{FileRow, SyncState, Tables, Write};
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
    changes_gate: Mutex<Option<Arc<Semaphore>>>,
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
        let mut files = self.files.lock().unwrap().clone();
        files.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
        let page = json!({ "files": files, "has_more": false, "start_seq": 1 });
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
        let gate = self.changes_gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.acquire().await.unwrap().forget();
            }
            Ok(serde_json::from_value(page).unwrap())
        })
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
        // Canonical, as an App Server's root is; on macOS the temporary
        // directory lies behind the `/var` link.
        let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let stub = Arc::new(stub);
        let transfer: Arc<dyn DriveTransfer> = Arc::new(drive::transfer::Checked(stub.clone()));
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
        commit_batch_size: drive::commit::MAX_CHANGES,
        session_id: SafeId::new(SESSION).unwrap(),
        root: root.to_path_buf(),
        state,
        transfer: Arc::new(drive::transfer::Checked(stub.clone())),
        cache: cache.clone(),
        tasks: Arc::new(Tasks),
        mounting: Mounting::Unavailable("no FUSE: test".to_owned()),
        registry: Arc::new(NoRegistry),
        clock: state::clock::Clock::seams(),
        reports: std::sync::Arc::new(NoReports),
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
async fn excluded_paths_fail_with_eacces_at_them_and_enoent_below_and_record_nothing() {
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
            "make the folder datasets",
            harness.mkdir("", "datasets").await.err(),
            Errno::ACCESS,
        ),
        (
            "create the file datasets",
            harness
                .sync
                .create(&drive, TOP_INODE, b"datasets", 0o644, Some(TASK_PID))
                .await
                .err(),
            Errno::ACCESS,
        ),
        (
            "rename notes.md to datasets",
            harness
                .sync
                .rename(&drive, TOP_INODE, b"notes.md", TOP_INODE, b"datasets", None)
                .await
                .err(),
            Errno::ACCESS,
        ),
        (
            "create datasets/b.csv",
            harness
                .sync
                .create(&drive, datasets, b"b.csv", 0o644, Some(TASK_PID))
                .await
                .err(),
            Errno::NOENT,
        ),
        (
            "open datasets/a.csv for append with create",
            harness
                .sync
                .create(&drive, datasets, b"a.csv", 0o644, Some(TASK_PID))
                .await
                .err(),
            Errno::NOENT,
        ),
        (
            "rename notes.md to datasets/notes.md",
            harness
                .sync
                .rename(&drive, TOP_INODE, b"notes.md", datasets, b"notes.md", None)
                .await
                .err(),
            Errno::NOENT,
        ),
    ];
    for (row, error, expected) in refused {
        assert_eq!(error, Some(expected), "{row}");
    }
    assert_eq!(harness.tables().await, before);
    assert_eq!(
        harness.sync.lookup(&drive, TOP_INODE, b"datasets").await,
        Err(Errno::NOENT)
    );
    assert_eq!(harness.read("notes.md").await.unwrap(), "notes");
}

#[tokio::test]
async fn a_folder_moved_so_that_an_entry_lands_at_an_excluded_path_fails_with_eacces() {
    let stub = Stub::default();
    let harness = Harness::start(stub, &["out/datasets/"]).await;
    harness.mkdir("", "box").await.unwrap();
    harness.mkdir("box", "datasets").await.unwrap();
    harness.create("box/datasets", b"a.csv").await.unwrap();
    let before = harness.tables().await;
    let drive = harness.drive.clone();
    assert_eq!(
        harness
            .sync
            .rename(&drive, TOP_INODE, b"box", TOP_INODE, b"out", Some(TASK_PID))
            .await
            .err(),
        Some(Errno::ACCESS)
    );
    assert_eq!(harness.tables().await, before);
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
    for folder in [".venv", "__pycache__"] {
        harness.mkdir("", folder).await.unwrap();
        harness.create(folder, b"local.py").await.unwrap();
        harness
            .write(&format!("{folder}/local.py"), "local")
            .await
            .unwrap();
        assert_eq!(
            harness
                .row(&format!("{folder}/local.py"))
                .await
                .unwrap()
                .local_only
                .as_deref(),
            Some("ignored")
        );
    }
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
    // A taskless notice belongs to the next Session sync point, not task_a.
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
async fn a_link_is_listed_only_where_a_regular_file_would_sync() {
    let harness = Harness::start(Stub::default(), &[]).await;
    for folder in ["node_modules", ".venv", "__pycache__", "proj", "src"] {
        harness.mkdir("", folder).await.unwrap();
    }
    harness.mkdir("proj", ".git").await.unwrap();
    let task = Id::new("task_a").unwrap();
    // The repository's own notice.
    harness.sync.notices(&task).await;
    for (folder, listed) in [
        ("node_modules", false),
        (".venv", false),
        ("__pycache__", false),
        ("proj", false),
        ("proj/.git", false),
        ("src", true),
    ] {
        let parent = harness.ino(folder).await;
        harness
            .sync
            .symlink(&harness.drive, parent, b"link", b"target", Some(TASK_PID))
            .await
            .unwrap();
        let link = Notice {
            path: harness.machine_path(&format!("{folder}/link")),
            reason: "symlink",
        };
        let want = if listed { vec![link] } else { Vec::new() };
        assert_eq!(
            harness.sync.notices(&task).await,
            want,
            "a link in {folder}"
        );
    }
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
async fn every_pending_change_carries_its_base_and_a_rename_uploads_nothing() {
    let stub = Stub::default();
    stub.list("a.txt", "a", false);
    stub.list("b.txt", "b", false);
    let harness = Harness::start(stub, &[]).await;
    harness.write("a.txt", "A").await.unwrap();
    let content = harness.session().join("content").join(DRIVE);
    let a = harness.row("a.txt").await.unwrap();
    assert_eq!(
        std::fs::read(content.join(a.file_id.to_string())).unwrap(),
        b"A",
        "the write is in the content file when it returns"
    );
    harness.create("", b"new.txt").await.unwrap();
    harness.rename("b.txt", "", b"b2.txt").await.unwrap();

    let tables = harness.tables().await;
    let change = |path: &str| tables.pending_changes[&key(path)].clone();
    let bases: Vec<(&str, &str, Option<String>)> = ["a.txt", "new.txt", "b.txt", "b2.txt"]
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
            ("b.txt", "delete", Some(sha256(b"b"))),
            ("b2.txt", "write", None),
        ]
    );
    let moved = change("b2.txt");
    assert_eq!(moved.content_sha256, Some(sha256(b"b")));
    assert!(moved.uploaded, "the drive holds the moved content");
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

/// A report outbox for owners whose tests make no drive sync report.
struct NoReports;

impl protocol::ports::ReportSink for NoReports {
    fn submit(&self, _: protocol::ports::Submission) -> protocol::ports::ReportId {
        panic!("no drive sync report is expected")
    }
    fn cancel(&self, _: protocol::ports::ReportId) {}
    fn add_final(&self, _: protocol::ports::ReportId, _: protocol::id::Id) {}
}

#[tokio::test]
async fn moving_a_repository_keeps_its_files_local_only() {
    let stub = Stub::default();
    stub.list("project/a.txt", "alpha", false);
    stub.list("project/z.txt", "zulu", false);
    let harness = Harness::start(stub, &[]).await;
    harness.mkdir("project", ".git").await.unwrap();
    harness.create("project/.git", b"HEAD").await.unwrap();
    harness.write("project/a.txt", "local").await.unwrap();
    harness.rename("project", "", b"moved").await.unwrap();

    let tables = harness.tables().await;
    assert!(
        tables.pending_changes.is_empty(),
        "{:?}",
        tables.pending_changes
    );
    assert_eq!(harness.read("moved/a.txt").await.unwrap(), "local");
    assert_eq!(harness.read("moved/z.txt").await.unwrap(), "zulu");
    for path in [
        "moved",
        "moved/.git",
        "moved/.git/HEAD",
        "moved/a.txt",
        "moved/z.txt",
    ] {
        assert_eq!(
            tables.files[&key(path)].local_only.as_deref(),
            Some("git_repository")
        );
    }
}

#[tokio::test]
async fn a_directory_move_rejects_an_overlong_descendant_without_changing_state() {
    let stub = Stub::default();
    let path = format!(
        "a/{}/{}/{}/file",
        "b".repeat(255),
        "c".repeat(255),
        "d".repeat(255)
    );
    stub.list(&path, "remote", false);
    let harness = Harness::start(stub, &[]).await;
    harness.write(&path, "local!").await.unwrap();
    let target = "t".repeat(255);
    harness.mkdir("", &target).await.unwrap();
    let before = harness.tables().await;
    assert_eq!(
        harness.rename("a", "", target.as_bytes()).await,
        Err(Errno::INVAL)
    );
    assert_eq!(harness.tables().await, before);
    assert_eq!(harness.read(&path).await.unwrap(), "local!");
}

#[tokio::test]
async fn repository_removal_preserves_pending_bases_and_skips_ineligible_entries() {
    let stub = Stub::default();
    stub.list("pending", "old", false);
    stub.list("mode", "same", false);
    stub.list("untouched", "same", false);
    let harness = Harness::start(stub, &["excluded/"]).await;
    harness.write("pending", "new").await.unwrap();
    let pending = harness.tables().await.pending_changes[&key("pending")].clone();
    harness.mkdir("", ".git").await.unwrap();
    harness.create("", b"fresh").await.unwrap();
    harness.write("fresh", "local").await.unwrap();
    harness
        .sync
        .setattr(
            &harness.drive,
            harness.ino("mode").await,
            SetAttr {
                mode: Some(0o755),
                ..SetAttr::default()
            },
            Some(TASK_PID),
        )
        .await
        .unwrap();
    harness.mkdir("", "nested").await.unwrap();
    harness.mkdir("nested", ".git").await.unwrap();
    harness.create("nested", b"private").await.unwrap();
    harness.mkdir("", "node_modules").await.unwrap();
    harness.create("node_modules", b"private").await.unwrap();
    harness
        .sync
        .symlink(&harness.drive, TOP_INODE, b"link", b"fresh", Some(TASK_PID))
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
    harness
        .sync
        .rmdir(&harness.drive, TOP_INODE, b".git", Some(TASK_PID))
        .await
        .unwrap();
    let tables = harness.tables().await;
    assert_eq!(tables.pending_changes.len(), 3);
    assert_eq!(tables.pending_changes[&key("pending")], pending);
    let mode = &tables.pending_changes[&key("mode")];
    assert_eq!(mode.base_sha256, Some(sha256(b"same")));
    assert_eq!(mode.content_sha256, mode.base_sha256);
    assert!(mode.uploaded);
    assert_eq!(mode.executable, Some(true));
    assert_eq!(
        tables.files[&key("nested/private")].local_only.as_deref(),
        Some("git_repository")
    );
    assert_eq!(
        tables.files[&key("node_modules/private")]
            .local_only
            .as_deref(),
        Some("ignored")
    );
    assert!(tables.pending_changes.contains_key(&key("fresh")));
    assert_eq!(
        harness.stub.downloads.load(Ordering::SeqCst),
        1,
        "only the earlier partial pending write read old bytes"
    );
    let reopened = SyncState::open(&harness.session().join("sync-state.db"))
        .await
        .unwrap();
    assert_eq!(reopened.tables().pending_changes, tables.pending_changes);
}

#[tokio::test]
async fn remote_git_marker_changes_refresh_existing_file_classification() {
    let stub = Stub::default();
    stub.list("file", "remote", false);
    let harness = Harness::start(stub, &[]).await;
    harness.remote(2, ".git", "gitdir: elsewhere").await;
    assert_eq!(
        harness.row("file").await.unwrap().local_only.as_deref(),
        Some("git_repository")
    );
    harness.write("file", "local!").await.unwrap();
    assert!(harness.tables().await.pending_changes.is_empty());
    harness.stub.records.lock().unwrap().push(json!({
        "seq": 3, "index": 0, "path": ".git", "operation": "deleted",
        "version_id": "v3", "content_sha256": null, "size_bytes": 0, "executable": false,
    }));
    harness.sync.drive_changed(harness.drive.clone(), 3);
    harness.caught_up().await;
    assert_eq!(harness.row("file").await.unwrap().local_only, None);
    // Catch-up changes classification; enrollment is specific to local marker removal.
    assert!(harness.tables().await.pending_changes.is_empty());
}

#[tokio::test]
async fn removing_a_local_folder_preserves_a_hidden_remote_versions_base() {
    let harness = Harness::start(Stub::default(), &[]).await;
    harness.mkdir("", "path").await.unwrap();
    harness.remote(2, "path", "remote").await;
    harness
        .sync
        .rmdir(&harness.drive, TOP_INODE, b"path", Some(TASK_PID))
        .await
        .unwrap();
    assert_eq!(
        harness.row("path").await.unwrap().synced_sha256,
        Some(sha256(b"remote"))
    );
    harness.create("", b"path").await.unwrap();
    assert_eq!(
        harness.tables().await.pending_changes[&key("path")].base_sha256,
        Some(sha256(b"remote"))
    );
}

#[tokio::test]
async fn a_first_write_waits_for_catchup_before_requesting_uncached_content() {
    for resize in [false, true] {
        let stub = Stub::default();
        stub.list("a.txt", "old", false);
        stub.list("empty.txt", "old", false);
        let harness = Harness::start(stub, &[]).await;
        let ino = harness.ino("a.txt").await;
        let empty = harness.ino("empty.txt").await;
        harness.stub.missing.lock().unwrap().insert(sha256(b"old"));
        let new_hash = harness.stub.content("newer");
        harness.stub.records.lock().unwrap().push(json!({
            "seq":2, "index":0, "path":"a.txt", "operation":"modified",
            "version_id":"v_new", "content_sha256":new_hash,
            "size_bytes":5, "executable":false,
        }));
        let gate = Arc::new(Semaphore::new(0));
        *harness.stub.changes_gate.lock().unwrap() = Some(gate.clone());
        harness.sync.connected();
        let sync = harness.sync.clone();
        let drive = harness.drive.clone();
        let mut writing = tokio::spawn(async move {
            if resize {
                sync.setattr(
                    &drive,
                    ino,
                    SetAttr {
                        size: Some(4),
                        ..SetAttr::default()
                    },
                    Some(TASK_PID),
                )
                .await
                .map(|_| ())
            } else {
                sync.write(&drive, ino, 0, b"X", Some(TASK_PID))
                    .await
                    .map(|_| ())
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                result = &mut writing => panic!("write requested obsolete content before catch-up: {result:?}"),
                () = async {
                    while harness.sync.inspect().await.waiting_operations == 0 { tokio::task::yield_now().await; }
                } => {},
            }
        }).await.unwrap();
        assert_eq!(harness.stub.downloads.load(Ordering::SeqCst), 0);
        // A complete replacement needs no old bytes and does not wait.
        harness
            .sync
            .setattr(
                &harness.drive,
                empty,
                SetAttr {
                    size: Some(0),
                    ..SetAttr::default()
                },
                Some(TASK_PID),
            )
            .await
            .unwrap();
        assert_eq!(harness.stub.downloads.load(Ordering::SeqCst), 0);
        gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), writing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            harness.read("a.txt").await.unwrap(),
            if resize { "newe" } else { "Xewer" }
        );
        assert_eq!(harness.stub.downloads.load(Ordering::SeqCst), 1);
        assert_eq!(
            harness.tables().await.pending_changes[&key("a.txt")]
                .base_sha256
                .as_deref(),
            Some(new_hash.as_str())
        );
    }
}

#[tokio::test]
async fn a_write_to_an_existing_file_at_an_excluded_path_fails_without_changing_state() {
    let mut harness = Harness::start(Stub::default(), &[]).await;
    harness.create("", b"secret").await.unwrap();
    harness.write("secret", "original").await.unwrap();
    let ino = harness.ino("secret").await;
    harness.exclude = vec!["secret".into()];
    harness.mount().await;
    let before = harness.tables().await;
    assert_eq!(
        harness
            .sync
            .write(&harness.drive, ino, 0, b"changed!", Some(TASK_PID))
            .await,
        Err(Errno::ACCESS)
    );
    assert_eq!(harness.tables().await, before);
}

/// Both platform reasons use the same transitions. Linux and malformed
/// near-prefix controls remain ordinary paths under the same operations.
#[tokio::test]
async fn platform_local_paths_share_one_operation_and_rename_rule() {
    for (name, platform_reason) in [
        (".nfs.20051025.3080", Some("nfs_temporary")),
        (".nfs.ABCD1234.AB09", Some("nfs_temporary")),
        ("._metadata", Some("macos_metadata")),
        (".nfs.notes", None),
        (".nfs.2005102.3080", None),
        (".nfs.20051025.30800", None),
        (".nfs.20051025.3080x", None),
        (".nfs.2005102g.3080", None),
        (".nfs.20051025.308g", None),
        (".nfs.20051025x3080", None),
        ("_nfs.20051025.3080", None),
    ] {
        let reason = if cfg!(target_os = "macos") {
            platform_reason
        } else {
            None
        };
        let h = Harness::start(Stub::default(), &[]).await;
        let ino = h.create("", name.as_bytes()).await.unwrap();
        h.write(name, "local-metadata-bytes").await.unwrap();
        h.chmod(name, 0o755).await;
        h.sync
            .setattr(
                &h.drive,
                ino,
                SetAttr {
                    size: Some(5),
                    ..SetAttr::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(h.read(name).await.unwrap(), "local");
        assert_eq!(
            h.row(name).await.unwrap().local_only.as_deref(),
            reason,
            "{name}"
        );
        assert_eq!(
            h.tables().await.pending_changes.contains_key(&key(name)),
            reason.is_none(),
            "{name}"
        );

        // Local -> local replacement retains source inode and bytes.
        let other = if name.starts_with("._") {
            "._other"
        } else {
            ".nfs.abcdef01.1234"
        };
        h.create("", other.as_bytes()).await.unwrap();
        h.write(other, "replaced").await.unwrap();
        h.rename(name, "", other.as_bytes()).await.unwrap();
        assert_eq!(h.ino(other).await, ino);
        assert_eq!(h.read(other).await.unwrap(), "local");
        // The ordinary target records a write, without a strict-source delete.
        h.rename(other, "", b"ordinary").await.unwrap();
        let tables = h.tables().await;
        assert!(tables.pending_changes.contains_key(&key("ordinary")));
        assert!(!tables.pending_changes.contains_key(&key(other)));
        // An unsynced ordinary source moved to a strict name leaves no
        // pending source or destination. Subsequent writes cannot enroll it.
        h.rename("ordinary", "", name.as_bytes()).await.unwrap();
        h.write(name, "local-again").await.unwrap();
        h.sync
            .unlink(&h.drive, TOP_INODE, name.as_bytes(), None)
            .await
            .unwrap();
        assert!(h.tables().await.pending_changes.is_empty(), "{name}");

        // A subtree crossing the boundary classifies descendants before
        // recording either side; Git marker removal cannot enroll them.
        h.mkdir("", "tree").await.unwrap();
        h.create("tree", b"child").await.unwrap();
        h.write("tree/child", "child").await.unwrap();
        h.rename("tree", "", name.as_bytes()).await.unwrap();
        let child = format!("{name}/child");
        h.create(name, b".git").await.unwrap();
        h.sync
            .unlink(&h.drive, h.ino(name).await, b".git", None)
            .await
            .unwrap();
        h.write(&child, "child changed").await.unwrap();
        assert_eq!(
            h.row(&child).await.unwrap().local_only.as_deref(),
            reason,
            "{name}"
        );
        assert_eq!(
            h.tables().await.pending_changes.contains_key(&key(&child)),
            reason.is_none(),
            "{name}"
        );
        h.rename(name, "", b"ordinary-tree").await.unwrap();
        assert!(
            h.tables()
                .await
                .pending_changes
                .contains_key(&key("ordinary-tree/child"))
        );
        h.sync.close().await;
    }
}

#[tokio::test]
async fn platform_local_cached_entries_refresh_but_local_bytes_survive_catchup_and_a_reloaded_state()
 {
    for (name, expected) in [
        (".nfs.20051025.3080", "nfs_temporary"),
        ("._metadata", "macos_metadata"),
    ] {
        let stub = Stub::default();
        stub.list(name, "remote-one", false);
        let mut h = Harness::start(stub, &[]).await;
        assert_eq!(h.read(name).await.unwrap(), "remote-one");
        h.remote(2, name, "remote-two").await;
        assert_eq!(h.read(name).await.unwrap(), "remote-two");
        h.write(name, "local-edit").await.unwrap();
        h.remote(3, name, "remote-three").await;
        assert_eq!(h.read(name).await.unwrap(), "local-edit");
        let local = if cfg!(target_os = "macos") {
            Some(expected)
        } else {
            None
        };
        assert_eq!(h.row(name).await.unwrap().local_only.as_deref(), local);
        assert_eq!(
            h.tables().await.pending_changes.contains_key(&key(name)),
            local.is_none()
        );
        h.sync.close().await;
        h.restart(Write::default()).await;
        assert_eq!(h.read(name).await.unwrap(), "local-edit");
        h.chmod(name, 0o755).await;
        assert_eq!(
            h.tables().await.pending_changes.contains_key(&key(name)),
            local.is_none()
        );
        h.sync.close().await;
    }
}
