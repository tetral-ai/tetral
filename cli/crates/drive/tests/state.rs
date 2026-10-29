//! A Session's sync state against real SQLite files: an error inside each
//! kind of write leaves every table as it was, in memory and on disk, and a
//! database that SQLite cannot read or of another version is left untouched
//! while a valid one loads.

use std::path::Path;

use drive::state::{
    Cursor, FileRow, InflightChange, ListingRow, OpenError, PendingChange, SyncState, Tables, Write,
};
use sqlx::AssertSqlSafe;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};

const FAIL: &str = "drive_fail";

fn file(drive: &str, path: &str, file_id: i64) -> FileRow {
    FileRow {
        drive_id: drive.to_owned(),
        path: path.to_owned(),
        file_id,
        kind: "file".to_owned(),
        present: true,
        version_id: Some("ver_a".to_owned()),
        synced_sha256: Some("a".repeat(64)),
        synced_size: Some(3),
        synced_executable: Some(false),
        content: Some("cache".to_owned()),
        local_sha256: Some("a".repeat(64)),
        size: Some(3),
        executable: Some(false),
        mtime_ns: Some(1),
        ctime_ns: Some(1),
        origin: None,
        link_target: None,
        local_only: None,
    }
}

fn pending(drive: &str, path: &str) -> PendingChange {
    PendingChange {
        drive_id: drive.to_owned(),
        path: path.to_owned(),
        change: "write".to_owned(),
        base_sha256: Some("a".repeat(64)),
        content_sha256: None,
        executable: Some(false),
        uploaded: false,
        last_write_at: 1_000,
        task_id: Some("task_a".to_owned()),
        fault_code: None,
    }
}

fn cursor(drive: &str, applied_seq: i64) -> Cursor {
    Cursor {
        drive_id: drive.to_owned(),
        applied_seq,
        generation: 7,
        next_file_id: 3,
    }
}

fn inflight(drive: &str, path: &str) -> InflightChange {
    InflightChange {
        drive_id: drive.to_owned(),
        path: path.to_owned(),
        change: "write".to_owned(),
        base_sha256: None,
        content_sha256: Some("b".repeat(64)),
        executable: Some(false),
        changed_during_flight: false,
    }
}

fn listing(drive: &str, path: &str) -> ListingRow {
    ListingRow {
        drive_id: drive.to_owned(),
        path: path.to_owned(),
        version_id: "ver_b".to_owned(),
        content_sha256: "b".repeat(64),
        size_bytes: 4,
        executable: false,
    }
}

fn key(drive: &str, path: &str) -> (String, String) {
    (drive.to_owned(), path.to_owned())
}

/// Makes every insert, update and delete of a row of drive `drive_fail`
/// fail inside its transaction.
async fn inject_failures(path: &Path) {
    let options = SqliteConnectOptions::new().filename(path);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    for table in [
        "files",
        "pending_changes",
        "drive_cursor",
        "inflight_commits",
        "mount_errors",
        "listing",
    ] {
        for (event, row) in [("INSERT", "NEW"), ("UPDATE", "OLD"), ("DELETE", "OLD")] {
            sqlx::raw_sql(AssertSqlSafe(format!(
                "CREATE TRIGGER fail_{table}_{event} BEFORE {event} ON {table} \
                 WHEN {row}.drive_id = '{FAIL}' BEGIN SELECT RAISE(ABORT, 'injected'); END;"
            )))
            .execute(&mut connection)
            .await
            .unwrap();
        }
    }
    connection.close().await.unwrap();
}

#[tokio::test]
async fn an_error_inside_each_kind_of_write_leaves_every_table_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sync-state.db");
    let mut state = SyncState::create(&path).await.unwrap();
    state
        .apply(Write {
            files: vec![file("drive_a", "", 1), file("drive_a", "a.txt", 2)],
            pending: vec![pending("drive_a", "a.txt")],
            cursor: vec![cursor("drive_a", 4), cursor(FAIL, 1)],
            inflight: vec![inflight("drive_a", "a.txt"), inflight(FAIL, "x")],
            mount_errors: vec![("drive_b".to_owned(), "mount: busy".to_owned())],
            listing: vec![listing("drive_a", "b.txt"), listing(FAIL, "x")],
            ..Write::default()
        })
        .await
        .unwrap();
    inject_failures(&path).await;
    let before: Tables = state.tables().clone();
    let kinds: Vec<(&str, Write)> = vec![
        (
            "a pending change with its files rows and an in-flight path",
            Write {
                files: vec![file("drive_a", "a.txt", 2), file(FAIL, "x", 1)],
                pending: vec![pending("drive_a", "a.txt")],
                changed_during_flight: vec![key("drive_a", "a.txt")],
                ..Write::default()
            },
        ),
        (
            "a finished listing with its cursor",
            Write {
                files: vec![file("drive_a", "b.txt", 3)],
                cursor: vec![cursor("drive_a", 9)],
                listing_removed_drives: vec!["drive_a".to_owned(), FAIL.to_owned()],
                ..Write::default()
            },
        ),
        (
            "a page of change records with its applied_seq",
            Write {
                files_removed: vec![key("drive_a", "a.txt")],
                cursor: vec![cursor("drive_a", 5), cursor(FAIL, 2)],
                ..Write::default()
            },
        ),
        (
            "a commit recorded in flight",
            Write {
                inflight: vec![inflight("drive_a", "c.txt"), inflight(FAIL, "c.txt")],
                ..Write::default()
            },
        ),
        (
            "a commit answer",
            Write {
                files: vec![file("drive_a", "a.txt", 2)],
                pending_removed: vec![key("drive_a", "a.txt")],
                inflight_removed_drives: vec!["drive_a".to_owned(), FAIL.to_owned()],
                ..Write::default()
            },
        ),
        (
            "a drive's rows removed",
            Write {
                files_removed: vec![key("drive_a", ""), key("drive_a", "a.txt")],
                pending_removed: vec![key("drive_a", "a.txt")],
                cursor_removed: vec!["drive_a".to_owned(), FAIL.to_owned()],
                ..Write::default()
            },
        ),
        (
            "a mount error",
            Write {
                mount_errors_removed: vec!["drive_b".to_owned()],
                mount_errors: vec![(FAIL.to_owned(), "mount: busy".to_owned())],
                ..Write::default()
            },
        ),
        (
            "a first listing page",
            Write {
                listing_removed_drives: vec!["drive_a".to_owned()],
                listing: vec![listing("drive_a", "c.txt"), listing(FAIL, "y")],
                ..Write::default()
            },
        ),
    ];
    for (kind, write) in kinds {
        let error = state.apply(write).await.unwrap_err();
        assert!(error.to_string().contains("injected"), "{kind}: {error}");
        assert_eq!(state.tables(), &before, "{kind}");
    }
    state.close().await;
    let reopened = SyncState::open(&path).await.unwrap();
    assert_eq!(reopened.tables(), &before);
}

#[tokio::test]
async fn an_unreadable_or_other_version_database_is_left_untouched_and_a_valid_one_loads() {
    let directory = tempfile::tempdir().unwrap();
    let zeros = directory.path().join("sess_a.db");
    std::fs::write(&zeros, vec![0; 4_096]).unwrap();
    let later = directory.path().join("sess_b.db");
    SyncState::create(&later).await.unwrap().close().await;
    let options = SqliteConnectOptions::new().filename(&later);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql("PRAGMA user_version = 2")
        .execute(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let valid = directory.path().join("sess_c.db");
    let mut state = SyncState::create(&valid).await.unwrap();
    state
        .apply(Write {
            pending: vec![pending("drive_a", "a.txt")],
            ..Write::default()
        })
        .await
        .unwrap();
    state.close().await;

    let bytes = |path: &Path| std::fs::read(path).unwrap();
    let (zeros_before, later_before) = (bytes(&zeros), bytes(&later));
    let refused = SyncState::open(&zeros).await.err().unwrap();
    assert_eq!(
        refused,
        OpenError::Unreadable("file is not a database".to_owned())
    );
    assert_eq!(refused.to_string(), "sync state: file is not a database");
    let refused = SyncState::open(&later).await.err().unwrap();
    assert_eq!(refused, OpenError::Version(2));
    assert_eq!(
        refused.to_string(),
        "sync state: version 2 is not supported"
    );
    let loaded = SyncState::open(&valid).await.unwrap();
    assert_eq!(
        loaded
            .tables()
            .pending_changes
            .values()
            .cloned()
            .collect::<Vec<_>>(),
        [pending("drive_a", "a.txt")]
    );
    assert_eq!(bytes(&zeros), zeros_before);
    assert_eq!(bytes(&later), later_before);
}
