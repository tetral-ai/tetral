//! A Session's sync state, `sessions/<session_id>/sync-state.db`: SQLite
//! through `sqlx`, WAL journaling and `synchronous=FULL`, its schema version
//! in `user_version`. The drive sync is its only reader and writer. Every
//! write is one [`Write`] committed in one SQLite transaction, so a process
//! killed at any instant leaves the state from before or after it; the
//! in-memory copy that the mounts are answered from changes only after the
//! commit.
//!
//! A database that SQLite cannot open or read, or whose `user_version` is
//! not 1, is left untouched and gives [`OpenError`].

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteRow, SqliteSynchronous};
use sqlx::{Connection, Row, SqliteConnection};

/// The schema version this CLI reads and writes.
pub const VERSION: i64 = 1;

/// The database's name under the Session's directory.
pub const FILE_NAME: &str = "sync-state.db";

const SCHEMA: &str = "
CREATE TABLE files (
    drive_id TEXT NOT NULL,
    path TEXT NOT NULL,
    file_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    present INTEGER NOT NULL,
    version_id TEXT,
    synced_sha256 TEXT,
    synced_size INTEGER,
    synced_executable INTEGER,
    content TEXT,
    local_sha256 TEXT,
    size INTEGER,
    executable INTEGER,
    mtime_ns INTEGER,
    ctime_ns INTEGER,
    origin TEXT,
    link_target TEXT,
    local_only TEXT,
    PRIMARY KEY (drive_id, path),
    UNIQUE (drive_id, file_id)
);
CREATE TABLE pending_changes (
    drive_id TEXT NOT NULL,
    path TEXT NOT NULL,
    change TEXT NOT NULL,
    base_sha256 TEXT,
    content_sha256 TEXT,
    executable INTEGER,
    uploaded INTEGER NOT NULL,
    last_write_at INTEGER NOT NULL,
    task_id TEXT,
    fault_code TEXT,
    PRIMARY KEY (drive_id, path)
);
CREATE TABLE drive_cursor (
    drive_id TEXT NOT NULL PRIMARY KEY,
    applied_seq INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    next_file_id INTEGER NOT NULL
);
CREATE TABLE inflight_commits (
    drive_id TEXT NOT NULL,
    path TEXT NOT NULL,
    change TEXT NOT NULL,
    base_sha256 TEXT,
    content_sha256 TEXT,
    executable INTEGER,
    changed_during_flight INTEGER NOT NULL,
    PRIMARY KEY (drive_id, path)
);
CREATE TABLE mount_errors (
    drive_id TEXT NOT NULL PRIMARY KEY,
    error TEXT NOT NULL
);
CREATE TABLE listing (
    drive_id TEXT NOT NULL,
    path TEXT NOT NULL,
    version_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    executable INTEGER NOT NULL,
    PRIMARY KEY (drive_id, path)
);
PRAGMA user_version = 1;
";

/// Why a sync state could not be opened; the mount checks fail each of the
/// Session's drives with its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenError {
    /// SQLite's message, such as `file is not a database`.
    Unreadable(String),
    /// A `user_version` other than [`VERSION`].
    Version(i64),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::Unreadable(message) => write!(f, "sync state: {message}"),
            OpenError::Version(version) => {
                write!(f, "sync state: version {version} is not supported")
            }
        }
    }
}

/// SQLite's own message of `error`.
fn message(error: &sqlx::Error) -> String {
    match error {
        sqlx::Error::Database(database) => database.message().to_owned(),
        other => other.to_string(),
    }
}

/// A key of a row of one drive: `(drive_id, path)`.
pub type Key = (String, String);

/// A row of `files`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRow {
    pub drive_id: String,
    pub path: String,
    pub file_id: i64,
    /// `file`, `folder`, `symlink` or `special`.
    pub kind: String,
    pub present: bool,
    pub version_id: Option<String>,
    pub synced_sha256: Option<String>,
    pub synced_size: Option<i64>,
    pub synced_executable: Option<bool>,
    /// `cache` or `session` for a present file.
    pub content: Option<String>,
    pub local_sha256: Option<String>,
    pub size: Option<i64>,
    pub executable: Option<bool>,
    pub mtime_ns: Option<i64>,
    pub ctime_ns: Option<i64>,
    /// `drive` or `local` for a folder.
    pub origin: Option<String>,
    pub link_target: Option<String>,
    /// `ignored`, `git_repository`, `symlink` or `special_file`.
    pub local_only: Option<String>,
}

/// A row of `pending_changes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingChange {
    pub drive_id: String,
    pub path: String,
    /// `write` or `delete`.
    pub change: String,
    pub base_sha256: Option<String>,
    pub content_sha256: Option<String>,
    pub executable: Option<bool>,
    pub uploaded: bool,
    /// Unix milliseconds.
    pub last_write_at: i64,
    pub task_id: Option<String>,
    pub fault_code: Option<String>,
}

/// A row of `drive_cursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub drive_id: String,
    pub applied_seq: i64,
    pub generation: i64,
    pub next_file_id: i64,
}

/// A row of `inflight_commits`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InflightChange {
    pub drive_id: String,
    pub path: String,
    pub change: String,
    pub base_sha256: Option<String>,
    pub content_sha256: Option<String>,
    pub executable: Option<bool>,
    pub changed_during_flight: bool,
}

/// A row of `listing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListingRow {
    pub drive_id: String,
    pub path: String,
    pub version_id: String,
    pub content_sha256: String,
    pub size_bytes: i64,
    pub executable: bool,
}

/// Every table, in memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tables {
    pub files: BTreeMap<Key, FileRow>,
    pub pending_changes: BTreeMap<Key, PendingChange>,
    pub drive_cursor: BTreeMap<String, Cursor>,
    pub inflight_commits: BTreeMap<Key, InflightChange>,
    /// The error text of each drive.
    pub mount_errors: BTreeMap<String, String>,
    pub listing: BTreeMap<Key, ListingRow>,
}

/// What one transaction writes, applied in the order of its fields: each
/// removal before the rows of its table are put. The writes of the drive
/// sync are these:
///
/// 1. a pending change with the `files` rows it changes and, when a commit
///    carrying the path is in flight, its `changed_during_flight`;
/// 2. a finished first listing or relisting with the `drive_cursor` row,
///    emptying that drive's `listing`;
/// 3. one page of change records with the new `applied_seq`;
/// 4. a commit's changes in `inflight_commits` before its first sending;
/// 5. one commit answer, removing or replacing the mount's in-flight rows;
/// 6. abandoned changes, or the removal of a drive's rows;
/// 7. a `mount_errors` row written or deleted;
/// 8. one page of a first listing or relisting into `listing`, the first
///    page's write removing the drive's earlier `listing` rows.
#[derive(Debug, Clone, Default)]
pub struct Write {
    pub files_removed: Vec<Key>,
    pub files: Vec<FileRow>,
    pub pending_removed: Vec<Key>,
    pub pending: Vec<PendingChange>,
    pub cursor_removed: Vec<String>,
    pub cursor: Vec<Cursor>,
    /// Every in-flight row of these drives.
    pub inflight_removed_drives: Vec<String>,
    pub inflight: Vec<InflightChange>,
    /// The in-flight rows whose path changed after the commit was recorded.
    pub changed_during_flight: Vec<Key>,
    pub mount_errors_removed: Vec<String>,
    /// `(drive_id, error)`.
    pub mount_errors: Vec<(String, String)>,
    /// Every listing row of these drives.
    pub listing_removed_drives: Vec<String>,
    pub listing: Vec<ListingRow>,
}

/// An open sync state and its in-memory copy.
pub struct SyncState {
    connection: SqliteConnection,
    tables: Tables,
}

impl SyncState {
    /// Opens the existing database at `path`: its `user_version` is read
    /// before anything else, then every table is loaded.
    pub async fn open(path: &Path) -> Result<SyncState, OpenError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false);
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(|error| OpenError::Unreadable(message(&error)))?;
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut connection)
            .await
            .map_err(|error| OpenError::Unreadable(message(&error)))?;
        if version != VERSION {
            let _ = connection.close().await;
            return Err(OpenError::Version(version));
        }
        let unreadable = |error: sqlx::Error| OpenError::Unreadable(message(&error));
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut connection)
            .await
            .map_err(unreadable)?;
        sqlx::query("PRAGMA synchronous = FULL")
            .execute(&mut connection)
            .await
            .map_err(unreadable)?;
        let tables = load(&mut connection).await.map_err(unreadable)?;
        Ok(SyncState { connection, tables })
    }

    /// Creates the database at `path`, which must not exist, then opens it.
    /// The tables and `user_version` are made under another name and the
    /// file is renamed into place, so that a process killed meanwhile
    /// leaves no database at `path`.
    pub async fn create(path: &Path) -> Result<SyncState, OpenError> {
        let unreadable = |error: sqlx::Error| OpenError::Unreadable(message(&error));
        let new = new_path(path);
        let _ = std::fs::remove_file(&new);
        let options = SqliteConnectOptions::new()
            .filename(&new)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Delete)
            .synchronous(SqliteSynchronous::Full);
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(unreadable)?;
        let mut transaction = connection.begin().await.map_err(unreadable)?;
        sqlx::raw_sql(SCHEMA)
            .execute(&mut *transaction)
            .await
            .map_err(unreadable)?;
        transaction.commit().await.map_err(unreadable)?;
        connection.close().await.map_err(unreadable)?;
        std::fs::rename(&new, path).map_err(|error| OpenError::Unreadable(error.to_string()))?;
        Self::open(path).await
    }

    /// The tables as the last committed write left them.
    pub fn tables(&self) -> &Tables {
        &self.tables
    }

    /// Commits `write` in one SQLite transaction, then applies it to the
    /// in-memory copy; on an error neither changes.
    pub async fn apply(&mut self, write: Write) -> Result<(), sqlx::Error> {
        let mut transaction = self.connection.begin().await?;
        for (drive, path) in &write.files_removed {
            sqlx::query("DELETE FROM files WHERE drive_id = ? AND path = ?")
                .bind(drive)
                .bind(path)
                .execute(&mut *transaction)
                .await?;
        }
        for row in &write.files {
            sqlx::query(
                "INSERT OR REPLACE INTO files (drive_id, path, file_id, kind, present, version_id, \
                 synced_sha256, synced_size, synced_executable, content, local_sha256, size, \
                 executable, mtime_ns, ctime_ns, origin, link_target, local_only) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.drive_id)
            .bind(&row.path)
            .bind(row.file_id)
            .bind(&row.kind)
            .bind(row.present)
            .bind(&row.version_id)
            .bind(&row.synced_sha256)
            .bind(row.synced_size)
            .bind(row.synced_executable)
            .bind(&row.content)
            .bind(&row.local_sha256)
            .bind(row.size)
            .bind(row.executable)
            .bind(row.mtime_ns)
            .bind(row.ctime_ns)
            .bind(&row.origin)
            .bind(&row.link_target)
            .bind(&row.local_only)
            .execute(&mut *transaction)
            .await?;
        }
        for (drive, path) in &write.pending_removed {
            sqlx::query("DELETE FROM pending_changes WHERE drive_id = ? AND path = ?")
                .bind(drive)
                .bind(path)
                .execute(&mut *transaction)
                .await?;
        }
        for row in &write.pending {
            sqlx::query(
                "INSERT OR REPLACE INTO pending_changes (drive_id, path, change, base_sha256, \
                 content_sha256, executable, uploaded, last_write_at, task_id, fault_code) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.drive_id)
            .bind(&row.path)
            .bind(&row.change)
            .bind(&row.base_sha256)
            .bind(&row.content_sha256)
            .bind(row.executable)
            .bind(row.uploaded)
            .bind(row.last_write_at)
            .bind(&row.task_id)
            .bind(&row.fault_code)
            .execute(&mut *transaction)
            .await?;
        }
        for drive in &write.cursor_removed {
            sqlx::query("DELETE FROM drive_cursor WHERE drive_id = ?")
                .bind(drive)
                .execute(&mut *transaction)
                .await?;
        }
        for row in &write.cursor {
            sqlx::query(
                "INSERT OR REPLACE INTO drive_cursor (drive_id, applied_seq, generation, \
                 next_file_id) VALUES (?, ?, ?, ?)",
            )
            .bind(&row.drive_id)
            .bind(row.applied_seq)
            .bind(row.generation)
            .bind(row.next_file_id)
            .execute(&mut *transaction)
            .await?;
        }
        for drive in &write.inflight_removed_drives {
            sqlx::query("DELETE FROM inflight_commits WHERE drive_id = ?")
                .bind(drive)
                .execute(&mut *transaction)
                .await?;
        }
        for row in &write.inflight {
            sqlx::query(
                "INSERT OR REPLACE INTO inflight_commits (drive_id, path, change, base_sha256, \
                 content_sha256, executable, changed_during_flight) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.drive_id)
            .bind(&row.path)
            .bind(&row.change)
            .bind(&row.base_sha256)
            .bind(&row.content_sha256)
            .bind(row.executable)
            .bind(row.changed_during_flight)
            .execute(&mut *transaction)
            .await?;
        }
        for (drive, path) in &write.changed_during_flight {
            sqlx::query(
                "UPDATE inflight_commits SET changed_during_flight = 1 \
                 WHERE drive_id = ? AND path = ?",
            )
            .bind(drive)
            .bind(path)
            .execute(&mut *transaction)
            .await?;
        }
        for drive in &write.mount_errors_removed {
            sqlx::query("DELETE FROM mount_errors WHERE drive_id = ?")
                .bind(drive)
                .execute(&mut *transaction)
                .await?;
        }
        for (drive, error) in &write.mount_errors {
            sqlx::query("INSERT OR REPLACE INTO mount_errors (drive_id, error) VALUES (?, ?)")
                .bind(drive)
                .bind(error)
                .execute(&mut *transaction)
                .await?;
        }
        for drive in &write.listing_removed_drives {
            sqlx::query("DELETE FROM listing WHERE drive_id = ?")
                .bind(drive)
                .execute(&mut *transaction)
                .await?;
        }
        for row in &write.listing {
            sqlx::query(
                "INSERT OR REPLACE INTO listing (drive_id, path, version_id, content_sha256, \
                 size_bytes, executable) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.drive_id)
            .bind(&row.path)
            .bind(&row.version_id)
            .bind(&row.content_sha256)
            .bind(row.size_bytes)
            .bind(row.executable)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.tables.apply(write);
        Ok(())
    }

    pub async fn close(self) {
        let _ = self.connection.close().await;
    }
}

impl Tables {
    pub(crate) fn apply(&mut self, write: Write) {
        for key in write.files_removed {
            self.files.remove(&key);
        }
        for row in write.files {
            self.files
                .insert((row.drive_id.clone(), row.path.clone()), row);
        }
        for key in write.pending_removed {
            self.pending_changes.remove(&key);
        }
        for row in write.pending {
            self.pending_changes
                .insert((row.drive_id.clone(), row.path.clone()), row);
        }
        for drive in write.cursor_removed {
            self.drive_cursor.remove(&drive);
        }
        for row in write.cursor {
            self.drive_cursor.insert(row.drive_id.clone(), row);
        }
        for drive in write.inflight_removed_drives {
            self.inflight_commits.retain(|(each, _), _| *each != drive);
        }
        for row in write.inflight {
            self.inflight_commits
                .insert((row.drive_id.clone(), row.path.clone()), row);
        }
        for key in write.changed_during_flight {
            if let Some(row) = self.inflight_commits.get_mut(&key) {
                row.changed_during_flight = true;
            }
        }
        for drive in write.mount_errors_removed {
            self.mount_errors.remove(&drive);
        }
        for (drive, error) in write.mount_errors {
            self.mount_errors.insert(drive, error);
        }
        for drive in write.listing_removed_drives {
            self.listing.retain(|(each, _), _| *each != drive);
        }
        for row in write.listing {
            self.listing
                .insert((row.drive_id.clone(), row.path.clone()), row);
        }
    }
}

/// The write that turns `before` into `after` for the rows of `files`,
/// `pending_changes`, `drive_cursor` and `inflight_commits`; a drive whose
/// in-flight rows differ has them all replaced.
pub(crate) fn diff(before: &Tables, after: &Tables) -> Write {
    let mut write = Write::default();
    for key in before.files.keys() {
        if !after.files.contains_key(key) {
            write.files_removed.push(key.clone());
        }
    }
    for (key, row) in &after.files {
        if before.files.get(key) != Some(row) {
            write.files.push(row.clone());
        }
    }
    for key in before.pending_changes.keys() {
        if !after.pending_changes.contains_key(key) {
            write.pending_removed.push(key.clone());
        }
    }
    for (key, row) in &after.pending_changes {
        if before.pending_changes.get(key) != Some(row) {
            write.pending.push(row.clone());
        }
    }
    for drive in before.drive_cursor.keys() {
        if !after.drive_cursor.contains_key(drive) {
            write.cursor_removed.push(drive.clone());
        }
    }
    for (drive, row) in &after.drive_cursor {
        if before.drive_cursor.get(drive) != Some(row) {
            write.cursor.push(row.clone());
        }
    }
    let drives: std::collections::BTreeSet<&String> = before
        .inflight_commits
        .keys()
        .chain(after.inflight_commits.keys())
        .map(|(drive, _)| drive)
        .collect();
    for drive in drives {
        let of = |tables: &Tables| -> Vec<InflightChange> {
            tables
                .inflight_commits
                .iter()
                .filter(|((each, _), _)| each == drive)
                .map(|(_, row)| row.clone())
                .collect()
        };
        let (was, is) = (of(before), of(after));
        if was != is {
            write.inflight_removed_drives.push(drive.clone());
            write.inflight.extend(is);
        }
    }
    write
}

fn new_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".new");
    path.with_file_name(name)
}

async fn load(connection: &mut SqliteConnection) -> Result<Tables, sqlx::Error> {
    let mut tables = Tables::default();
    let rows = |sql: &'static str| sqlx::query(sql);
    for row in rows("SELECT * FROM files")
        .fetch_all(&mut *connection)
        .await?
    {
        let row = file_row(&row)?;
        tables
            .files
            .insert((row.drive_id.clone(), row.path.clone()), row);
    }
    for row in rows("SELECT * FROM pending_changes")
        .fetch_all(&mut *connection)
        .await?
    {
        let row = PendingChange {
            drive_id: row.try_get("drive_id")?,
            path: row.try_get("path")?,
            change: row.try_get("change")?,
            base_sha256: row.try_get("base_sha256")?,
            content_sha256: row.try_get("content_sha256")?,
            executable: row.try_get("executable")?,
            uploaded: row.try_get("uploaded")?,
            last_write_at: row.try_get("last_write_at")?,
            task_id: row.try_get("task_id")?,
            fault_code: row.try_get("fault_code")?,
        };
        tables
            .pending_changes
            .insert((row.drive_id.clone(), row.path.clone()), row);
    }
    for row in rows("SELECT * FROM drive_cursor")
        .fetch_all(&mut *connection)
        .await?
    {
        let row = Cursor {
            drive_id: row.try_get("drive_id")?,
            applied_seq: row.try_get("applied_seq")?,
            generation: row.try_get("generation")?,
            next_file_id: row.try_get("next_file_id")?,
        };
        tables.drive_cursor.insert(row.drive_id.clone(), row);
    }
    for row in rows("SELECT * FROM inflight_commits")
        .fetch_all(&mut *connection)
        .await?
    {
        let row = InflightChange {
            drive_id: row.try_get("drive_id")?,
            path: row.try_get("path")?,
            change: row.try_get("change")?,
            base_sha256: row.try_get("base_sha256")?,
            content_sha256: row.try_get("content_sha256")?,
            executable: row.try_get("executable")?,
            changed_during_flight: row.try_get("changed_during_flight")?,
        };
        tables
            .inflight_commits
            .insert((row.drive_id.clone(), row.path.clone()), row);
    }
    for row in rows("SELECT * FROM mount_errors")
        .fetch_all(&mut *connection)
        .await?
    {
        tables
            .mount_errors
            .insert(row.try_get("drive_id")?, row.try_get("error")?);
    }
    for row in rows("SELECT * FROM listing")
        .fetch_all(&mut *connection)
        .await?
    {
        let row = ListingRow {
            drive_id: row.try_get("drive_id")?,
            path: row.try_get("path")?,
            version_id: row.try_get("version_id")?,
            content_sha256: row.try_get("content_sha256")?,
            size_bytes: row.try_get("size_bytes")?,
            executable: row.try_get("executable")?,
        };
        tables
            .listing
            .insert((row.drive_id.clone(), row.path.clone()), row);
    }
    Ok(tables)
}

fn file_row(row: &SqliteRow) -> Result<FileRow, sqlx::Error> {
    Ok(FileRow {
        drive_id: row.try_get("drive_id")?,
        path: row.try_get("path")?,
        file_id: row.try_get("file_id")?,
        kind: row.try_get("kind")?,
        present: row.try_get("present")?,
        version_id: row.try_get("version_id")?,
        synced_sha256: row.try_get("synced_sha256")?,
        synced_size: row.try_get("synced_size")?,
        synced_executable: row.try_get("synced_executable")?,
        content: row.try_get("content")?,
        local_sha256: row.try_get("local_sha256")?,
        size: row.try_get("size")?,
        executable: row.try_get("executable")?,
        mtime_ns: row.try_get("mtime_ns")?,
        ctime_ns: row.try_get("ctime_ns")?,
        origin: row.try_get("origin")?,
        link_target: row.try_get("link_target")?,
        local_only: row.try_get("local_only")?,
    })
}
