//! A Drive sync owner's catch-up against a stub `DriveTransfer` whose list
//! answers the test holds and gives one by one: first listings over pages,
//! pages that end inside a commit, a relisting after `410`, records against
//! pending changes, local-only and excluded paths, folders, notices, the
//! check before a command, and failed listings.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use drive::cache::{CacheConfig, ContentCache};
use drive::state::{FileRow, PendingChange, SyncState, Tables, Write};
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
use serde_json::{Value, json};
use state::clock::Clock;
use tokio::sync::{mpsc, oneshot};

const DRIVE: &str = "drive_a";

/// A list request the stub got, with the way to answer it.
enum Call {
    Files {
        after_path: Option<String>,
        answer: oneshot::Sender<Result<ListFilesResponse, Ended>>,
    },
    Changes {
        after_seq: i64,
        after_index: Option<i64>,
        answer: oneshot::Sender<Result<ListChangesResponse, Ended>>,
    },
}

struct Stub(mpsc::UnboundedSender<Call>);

struct NoTasks;

impl TaskOwnership for NoTasks {
    fn task_of(&self, _: u32, reply: TaskOwnerReply) {
        reply(None);
    }
}

impl DriveTransfer for Stub {
    fn list_files(&self, request: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        let (answer, answered) = oneshot::channel();
        let _ = self.0.send(Call::Files {
            after_path: request.body.after_path.map(|path| path.as_str().to_owned()),
            answer,
        });
        Box::pin(async move { answered.await.unwrap_or(Err(Ended::CursorExpired)) })
    }

    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        let (answer, answered) = oneshot::channel();
        let _ = self.0.send(Call::Changes {
            after_seq: request.body.after_seq,
            after_index: request.body.after_index,
            answer,
        });
        Box::pin(async move { answered.await.unwrap_or(Err(Ended::ReRead)) })
    }

    fn upload_content(&self, _: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        unimplemented!()
    }

    fn commit(&self, _: Request<Commit>) -> Transfer<'_, CommitResponse> {
        unimplemented!()
    }

    fn download_content<'a>(
        &'a self,
        _: Request<Download>,
        _: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        unimplemented!()
    }
}

fn hash(text: &str) -> String {
    let byte = text.bytes().fold(0u8, u8::wrapping_add);
    format!("{byte:02x}").repeat(32)
}

/// A listed file: version `v_<path>` of content hashed from `content`.
fn file(path: &str, content: &str) -> Value {
    json!({
        "path": path,
        "version_id": format!("v_{}", path.replace('/', "_")),
        "content_sha256": hash(content),
        "size_bytes": content.len(),
        "executable": false,
    })
}

fn files_page(files: Vec<Value>, has_more: bool, start_seq: Option<i64>) -> ListFilesResponse {
    let mut page = json!({ "files": files, "has_more": has_more });
    if let Some(seq) = start_seq {
        page["start_seq"] = json!(seq);
    }
    serde_json::from_value(page).unwrap()
}

/// A change record; `content` `None` deletes.
fn record(seq: i64, index: i64, path: &str, content: Option<&str>) -> Value {
    match content {
        Some(content) => json!({
            "seq": seq, "index": index, "path": path, "operation": "modified",
            "version_id": format!("v{seq}_{index}"), "content_sha256": hash(content),
            "size_bytes": content.len(), "executable": false,
        }),
        None => json!({
            "seq": seq, "index": index, "path": path, "operation": "deleted",
            "version_id": format!("v{seq}_{index}"), "content_sha256": null,
            "size_bytes": 0, "executable": false,
        }),
    }
}

fn changes_page(records: Vec<Value>, has_more: bool) -> ListChangesResponse {
    serde_json::from_value(json!({ "records": records, "has_more": has_more })).unwrap()
}

/// A Drive sync with its stub's calls.
struct Harness {
    sync: DriveSync,
    calls: mpsc::UnboundedReceiver<Call>,
}

impl Harness {
    async fn start(directory: &Path, seed: Write) -> Harness {
        let mut state = SyncState::create(&directory.join("sync-state.db"))
            .await
            .unwrap();
        state.apply(seed).await.unwrap();
        Harness::run(directory, state)
    }

    fn run(directory: &Path, state: SyncState) -> Harness {
        let (calls, received) = mpsc::unbounded_channel();
        let transfer: Arc<dyn DriveTransfer> = Arc::new(Stub(calls));
        let cache = ContentCache::spawn(CacheConfig {
            root: directory.to_path_buf(),
            transfer: transfer.clone(),
            clock: Clock::seams(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let sync = DriveSync::spawn(SyncConfig {
            session_id: SafeId::new("sess_a").unwrap(),
            root: directory.to_path_buf(),
            state,
            transfer,
            cache,
            tasks: Arc::new(NoTasks),
        });
        sync.binding_current(Binding {
            connection_id: Id::new("conn_a").unwrap(),
            binding_id: Id::new("bind_a").unwrap(),
        });
        Harness {
            sync,
            calls: received,
        }
    }

    async fn call(&mut self) -> Call {
        tokio::time::timeout(Duration::from_secs(10), self.calls.recv())
            .await
            .expect("a list request")
            .expect("the stub is alive")
    }

    /// No request comes while the owner handles everything sent so far.
    async fn no_call(&mut self) {
        self.sync.tables().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(self.calls.try_recv().is_err(), "an unexpected request");
    }

    async fn files(&mut self, after: Option<&str>, page: ListFilesResponse) {
        match self.call().await {
            Call::Files { after_path, answer } => {
                assert_eq!(after_path.as_deref(), after);
                answer.send(Ok(page)).ok().unwrap();
            }
            Call::Changes { .. } => panic!("list changes where list files was due"),
        }
    }

    async fn changes(
        &mut self,
        after: (i64, Option<i64>),
        page: Result<ListChangesResponse, Ended>,
    ) {
        match self.call().await {
            Call::Changes {
                after_seq,
                after_index,
                answer,
            } => {
                assert_eq!((after_seq, after_index), after);
                answer.send(page).ok().unwrap();
            }
            Call::Files { .. } => panic!("list files where list changes was due"),
        }
    }

    /// The drive-version columns of the drive's files: path to version and
    /// hash.
    async fn versions(&self) -> BTreeMap<String, (String, String)> {
        self.sync
            .tables()
            .await
            .files
            .values()
            .filter_map(|row| {
                Some((
                    row.path.clone(),
                    (row.version_id.clone()?, row.synced_sha256.clone()?),
                ))
            })
            .collect()
    }

    /// Waits until the owner's tables hold what `ready` looks for, since an
    /// answer reaches the owner after the stub gave it.
    async fn until(&self, what: &str, ready: impl Fn(&Tables) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !ready(&self.sync.tables().await) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn until_applied(&self, seq: i64) {
        self.until(&format!("applied_seq {seq}"), |tables| {
            tables
                .drive_cursor
                .get(DRIVE)
                .is_some_and(|cursor| cursor.applied_seq == seq)
        })
        .await;
    }

    /// The error a failed listing unmounted the drive with.
    async fn unmounted_with(&self) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(error) = self.sync.unmounted().await.get(DRIVE) {
                return error.clone();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for an unmount"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn row(&self, path: &str) -> Option<FileRow> {
        self.sync
            .tables()
            .await
            .files
            .get(&(DRIVE.to_owned(), path.to_owned()))
            .cloned()
    }

    async fn applied_seq(&self) -> i64 {
        self.sync.tables().await.drive_cursor[DRIVE].applied_seq
    }

    /// Mounts the drive and lists it in one page of `files` at `start_seq`.
    async fn listed(&mut self, files: Vec<Value>, start_seq: i64, exclude: Vec<String>) {
        self.sync.mounted(Id::new(DRIVE).unwrap(), exclude);
        self.files(None, files_page(files, false, Some(start_seq)))
            .await;
        self.changes((start_seq, None), Ok(changes_page(Vec::new(), false)))
            .await;
        self.until_applied(start_seq).await;
    }
}

fn expected(files: &[Value]) -> BTreeMap<String, (String, String)> {
    files
        .iter()
        .map(|file| {
            (
                file["path"].as_str().unwrap().to_owned(),
                (
                    file["version_id"].as_str().unwrap().to_owned(),
                    file["content_sha256"].as_str().unwrap().to_owned(),
                ),
            )
        })
        .collect()
}

fn pending(path: &str) -> PendingChange {
    PendingChange {
        drive_id: DRIVE.to_owned(),
        path: path.to_owned(),
        change: "write".to_owned(),
        base_sha256: Some(hash("base")),
        content_sha256: None,
        executable: Some(false),
        uploaded: false,
        last_write_at: 1,
        task_id: None,
        fault_code: None,
    }
}

#[tokio::test]
async fn a_first_listing_over_three_pages_with_commits_meanwhile_ends_equal_to_the_drive() {
    let directory = tempfile::tempdir().unwrap();
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    harness.sync.mounted(Id::new(DRIVE).unwrap(), Vec::new());
    let mut all: Vec<Value> = (0..2_500)
        .map(|index| file(&format!("d{}/f{index:04}", index % 7), "one"))
        .collect();
    all.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    let last = |page: &[Value]| page.last().unwrap()["path"].as_str().unwrap().to_owned();
    harness
        .files(None, files_page(all[..1_000].to_vec(), true, Some(5)))
        .await;
    let after_first = last(&all[..1_000]);
    harness
        .files(
            Some(&after_first),
            files_page(all[1_000..2_000].to_vec(), true, None),
        )
        .await;
    let after_second = last(&all[1_000..2_000]);
    harness
        .files(
            Some(&after_second),
            files_page(all[2_000..].to_vec(), false, None),
        )
        .await;
    // Two commits made while the pages were read.
    let changed = all[10]["path"].as_str().unwrap().to_owned();
    let gone = all[2_100]["path"].as_str().unwrap().to_owned();
    harness
        .changes(
            (5, None),
            Ok(changes_page(
                vec![
                    record(6, 0, &changed, Some("two")),
                    record(7, 0, &gone, None),
                    record(7, 1, "new/file", Some("three")),
                ],
                false,
            )),
        )
        .await;
    harness.until_applied(7).await;
    let mut drive = expected(&all);
    drive.insert(changed.clone(), ("v6_0".to_owned(), hash("two")));
    drive.remove(&gone);
    drive.insert("new/file".to_owned(), ("v7_1".to_owned(), hash("three")));
    assert_eq!(harness.versions().await, drive);
    assert_eq!(harness.applied_seq().await, 7);
    let tables = harness.sync.tables().await;
    assert!(tables.listing.is_empty());
    let folder = harness.row("new").await.unwrap();
    assert_eq!(
        (folder.kind.as_str(), folder.origin.as_deref()),
        ("folder", Some("drive"))
    );
    assert_eq!(harness.row("").await.unwrap().file_id, 1);
}

#[tokio::test]
async fn a_page_that_ends_inside_a_commit_keeps_applied_seq_before_it() {
    let directory = tempfile::tempdir().unwrap();
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    harness.listed(Vec::new(), 5, Vec::new()).await;
    harness.sync.drive_changed(Id::new(DRIVE).unwrap(), 7);
    let mut records: Vec<Value> = (0..400)
        .map(|index| record(6, index, &format!("six/{index}"), Some("6")))
        .collect();
    records.extend((0..600).map(|index| record(7, index, &format!("seven/{index}"), Some("7"))));
    harness
        .changes((5, None), Ok(changes_page(records, true)))
        .await;
    harness
        .changes(
            (7, Some(599)),
            Ok(changes_page(
                vec![record(7, 600, "seven/600", Some("7"))],
                false,
            )),
        )
        .await;
    harness.until_applied(7).await;
    assert_eq!(harness.applied_seq().await, 7);
    assert_eq!(harness.versions().await.len(), 1_001);
}

#[tokio::test]
async fn applied_seq_stays_before_a_commit_the_page_cut() {
    assert_eq!(drive::catchup::page_applied_seq(5, &[6, 6, 7, 7], true), 6);
    assert_eq!(drive::catchup::page_applied_seq(5, &[7, 7], true), 5);
    assert_eq!(drive::catchup::page_applied_seq(5, &[6, 7], false), 7);
    assert_eq!(drive::catchup::page_applied_seq(5, &[], false), 5);
}

#[tokio::test]
async fn an_expired_cursor_lists_the_drive_again_and_keeps_pending_changes_and_files() {
    let directory = tempfile::tempdir().unwrap();
    let content = directory.path().join("content/drive_a");
    let blobs = directory.path().join("cache/blobs");
    std::fs::create_dir_all(&content).unwrap();
    std::fs::create_dir_all(&blobs).unwrap();
    for (name, bytes) in [("7", "local seven"), ("8", "local eight")] {
        std::fs::write(content.join(name), bytes).unwrap();
    }
    for index in 0..3 {
        std::fs::write(blobs.join(hash(&index.to_string())), "blob").unwrap();
    }
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    harness
        .listed(
            vec![file("a", "a"), file("b", "b"), file("c", "c")],
            3,
            Vec::new(),
        )
        .await;
    harness.sync.tables().await;
    // Two pending writes, written behind the owner and seen when it starts again.
    drop(harness);
    let mut state = SyncState::open(&directory.path().join("sync-state.db"))
        .await
        .unwrap();
    state
        .apply(Write {
            pending: vec![pending("a"), pending("new")],
            ..Write::default()
        })
        .await
        .unwrap();
    let before = state.tables().pending_changes.clone();
    let mut harness = Harness::run(directory.path(), state);
    harness.sync.mounted(Id::new(DRIVE).unwrap(), Vec::new());
    harness.changes((3, None), Err(Ended::CursorExpired)).await;
    let now = vec![file("a", "a2"), file("c", "c"), file("d", "d")];
    harness
        .files(None, files_page(now.clone(), false, Some(9)))
        .await;
    harness
        .changes((9, None), Ok(changes_page(Vec::new(), false)))
        .await;
    harness.until_applied(9).await;
    assert_eq!(harness.versions().await, expected(&now));
    let tables = harness.sync.tables().await;
    assert_eq!(tables.pending_changes, before);
    // The pending write keeps its local entry; the others follow the drive.
    assert_eq!(
        harness.row("a").await.unwrap().local_sha256,
        Some(hash("a"))
    );
    assert_eq!(
        harness.row("d").await.unwrap().local_sha256,
        Some(hash("d"))
    );
    assert!(harness.row("b").await.is_none());
    assert_eq!(std::fs::read(content.join("7")).unwrap(), b"local seven");
    assert_eq!(std::fs::read(content.join("8")).unwrap(), b"local eight");
    assert_eq!(std::fs::read_dir(&blobs).unwrap().count(), 3);
}

#[tokio::test]
async fn records_follow_the_rules_of_pending_changes_folders_and_excluded_paths() {
    let directory = tempfile::tempdir().unwrap();
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    harness
        .listed(
            vec![
                file("kept/x", "x"),
                file("remote/only", "o"),
                file("local/only", "o"),
                file("datasets/big", "big"),
            ],
            3,
            vec!["datasets/".to_owned()],
        )
        .await;
    assert!(harness.row("datasets/big").await.is_none());
    assert!(harness.row("datasets").await.is_none());
    // `kept/x` gets a pending write; `local` becomes a folder made here.
    let tables = harness.sync.tables().await;
    let mut local = tables.files[&(DRIVE.to_owned(), "local".to_owned())].clone();
    local.origin = Some("local".to_owned());
    drop(harness);
    let mut state = SyncState::open(&directory.path().join("sync-state.db"))
        .await
        .unwrap();
    state
        .apply(Write {
            files: vec![local],
            pending: vec![pending("kept/x")],
            ..Write::default()
        })
        .await
        .unwrap();
    let mut harness = Harness::run(directory.path(), state);
    harness
        .sync
        .mounted(Id::new(DRIVE).unwrap(), vec!["datasets".to_owned()]);
    let before = harness.row("kept/x").await.unwrap();
    harness
        .changes(
            (3, None),
            Ok(changes_page(
                vec![
                    record(4, 0, "kept/x", Some("remote x")),
                    record(4, 1, "remote/only", None),
                    record(4, 2, "local/only", None),
                    record(4, 3, "datasets/more", Some("more")),
                ],
                false,
            )),
        )
        .await;
    harness.until_applied(4).await;
    let after = harness.row("kept/x").await.unwrap();
    assert_eq!(after.synced_sha256, Some(hash("remote x")));
    assert_eq!(after.version_id.as_deref(), Some("v4_0"));
    assert_eq!(
        (
            after.local_sha256,
            after.content,
            after.size,
            after.mtime_ns
        ),
        (
            before.local_sha256,
            before.content,
            before.size,
            before.mtime_ns
        )
    );
    let tables = harness.sync.tables().await;
    assert_eq!(
        tables.pending_changes[&(DRIVE.to_owned(), "kept/x".to_owned())].base_sha256,
        Some(hash("base"))
    );
    // The last file of a drive-origin folder takes the folder with it; a
    // folder made here stays.
    assert!(harness.row("remote/only").await.is_none());
    assert!(harness.row("remote").await.is_none());
    assert!(harness.row("local/only").await.is_none());
    assert!(harness.row("local").await.is_some());
    assert!(harness.row("datasets/more").await.is_none());
    assert_eq!(harness.applied_seq().await, 4);
}

#[tokio::test]
async fn notices_start_catch_ups_only_above_applied_seq_and_merge_during_one() {
    let directory = tempfile::tempdir().unwrap();
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    harness.listed(Vec::new(), 5, Vec::new()).await;
    let drive = || Id::new(DRIVE).unwrap();
    harness.sync.drive_changed(drive(), 5);
    harness.no_call().await;
    harness.sync.drive_changed(drive(), 6);
    let held = match harness.call().await {
        Call::Changes {
            after_seq: 5,
            answer,
            ..
        } => answer,
        _ => panic!("list changes after 5 was due"),
    };
    harness.sync.drive_changed(drive(), 7);
    harness.sync.drive_changed(drive(), 8);
    harness.no_call().await;
    held.send(Ok(changes_page(vec![record(6, 0, "f", Some("6"))], false)))
        .ok()
        .unwrap();
    harness
        .changes(
            (6, None),
            Ok(changes_page(
                vec![record(7, 0, "f", Some("7")), record(8, 0, "f", Some("8"))],
                false,
            )),
        )
        .await;
    harness.until_applied(8).await;
    harness.no_call().await;
}

#[tokio::test]
async fn a_check_waits_for_the_notified_seq_and_a_catch_up_begun_after_connected() {
    let directory = tempfile::tempdir().unwrap();
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    harness.listed(Vec::new(), 5, Vec::new()).await;
    let ready = |sync: &DriveSync| {
        let sync = sync.clone();
        tokio::spawn(async move { sync.check().await })
    };
    ready(&harness.sync).await.unwrap();
    harness.sync.connected();
    harness.sync.drive_changed(Id::new(DRIVE).unwrap(), 6);
    let check = ready(&harness.sync);
    let held = match harness.call().await {
        Call::Changes {
            after_seq: 5,
            answer,
            ..
        } => answer,
        _ => panic!("list changes after 5 was due"),
    };
    harness.no_call().await;
    assert!(!check.is_finished());
    held.send(Ok(changes_page(Vec::new(), false))).ok().unwrap();
    // The catch-up after `connected` found nothing yet: seq 6 is not applied.
    harness
        .changes(
            (5, None),
            Ok(changes_page(vec![record(6, 0, "f", Some("6"))], false)),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(10), check)
        .await
        .expect("the check ends")
        .unwrap();
    assert_eq!(harness.applied_seq().await, 6);
}

#[tokio::test]
async fn a_permanent_list_answer_unmounts_the_drive_and_a_new_listing_starts_clean() {
    let directory = tempfile::tempdir().unwrap();
    let mut harness = Harness::start(directory.path(), Write::default()).await;
    let drive = || Id::new(DRIVE).unwrap();
    harness.sync.mounted(drive(), Vec::new());
    harness
        .files(
            None,
            files_page(vec![file("a", "a"), file("stale", "s")], true, Some(2)),
        )
        .await;
    match harness.call().await {
        Call::Files { answer, .. } => answer
            .send(Err(Ended::Permanent {
                status: 403,
                code: Some("drive_access_denied"),
            }))
            .ok()
            .unwrap(),
        Call::Changes { .. } => panic!("list files was due"),
    }
    assert_eq!(
        harness.unmounted_with().await,
        "list files: 403 drive_access_denied"
    );
    harness.sync.check().await;

    harness.sync.mounted(drive(), Vec::new());
    harness
        .files(None, files_page(vec![file("a", "a")], false, Some(2)))
        .await;
    harness
        .changes(
            (2, None),
            Err(Ended::Permanent {
                status: 200,
                code: None,
            }),
        )
        .await;
    assert_eq!(
        harness.unmounted_with().await,
        "list changes: invalid answer"
    );
    assert!(harness.row("stale").await.is_none());
    assert_eq!(harness.versions().await, expected(&[file("a", "a")]));
}
