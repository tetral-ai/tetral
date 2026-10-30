//! What a catch-up writes: a finished first listing or relisting, and one
//! page of change records, each as one [`Write`] against the tables as they
//! stand. A record sets its path's drive-version columns and, unless the
//! path holds an uncommitted change or the local entry stands in its way,
//! the local entry the mounts show, with the `drive`-origin folders above
//! it. Paths this computer's `drive_exclude` leaves out are dropped.

use std::collections::{BTreeMap, BTreeSet};

use crate::paths::{self, ancestors, parent};
use crate::state::{Cursor, FileRow, ListingRow, Tables, Write};

/// The file id of a drive's top folder, the row of path `""`.
pub const TOP_FOLDER_ID: i64 = 1;

/// A file's state on the drive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub version_id: String,
    pub sha256: String,
    pub size: i64,
    pub executable: bool,
}

/// One change of a path: its new state, or `None` for a deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub path: String,
    pub state: Option<Version>,
}

/// The `applied_seq` after a page of records: the last record's seq when it
/// is the last page, otherwise the highest seq below the last record's,
/// since that commit may go on in the next page; unchanged when there is
/// none.
pub fn page_applied_seq(current: i64, seqs: &[i64], has_more: bool) -> i64 {
    let Some(&last) = seqs.last() else {
        return current;
    };
    if !has_more {
        return last;
    }
    seqs.iter()
        .copied()
        .filter(|seq| *seq < last)
        .max()
        .unwrap_or(current)
        .max(current)
}

/// One drive's `files` rows while records are applied to them.
struct Work<'a> {
    drive_id: &'a str,
    rows: BTreeMap<String, FileRow>,
    pending: BTreeSet<String>,
    next_file_id: i64,
    now_ns: i64,
}

impl Work<'_> {
    fn new_id(&mut self) -> i64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }

    fn row(&self, path: &str) -> Option<&FileRow> {
        self.rows.get(path)
    }

    fn local_only(&self, path: &str, kind: &str) -> Option<&'static str> {
        paths::local_only(path, kind, |each| {
            self.row(each).is_some_and(|row| row.present)
        })
    }

    /// Whether any present entry lies below `folder`.
    fn holds_entry(&self, folder: &str) -> bool {
        let prefix = if folder.is_empty() {
            String::new()
        } else {
            format!("{folder}/")
        };
        self.rows
            .range(prefix.clone()..)
            .take_while(|(path, _)| path.starts_with(&prefix))
            .any(|(path, row)| *path != folder && row.present)
    }

    /// A folder gained or lost an entry.
    fn touch(&mut self, folder: &str) {
        let now = self.now_ns;
        if let Some(row) = self.rows.get_mut(folder) {
            row.mtime_ns = Some(now);
            row.ctime_ns = Some(now);
        }
    }

    fn folder(&mut self, path: &str) -> FileRow {
        FileRow {
            drive_id: self.drive_id.to_owned(),
            path: path.to_owned(),
            file_id: self.new_id(),
            kind: "folder".to_owned(),
            present: true,
            version_id: None,
            synced_sha256: None,
            synced_size: None,
            synced_executable: None,
            content: None,
            local_sha256: None,
            size: None,
            executable: None,
            mtime_ns: Some(self.now_ns),
            ctime_ns: Some(self.now_ns),
            origin: Some("drive".to_owned()),
            link_target: None,
            local_only: self.local_only(path, "folder").map(str::to_owned),
        }
    }

    /// Whether the local entry keeps its state against `record`.
    fn blocked(&self, record: &Record, relisting: bool) -> bool {
        let path = record.path.as_str();
        let below_file = ancestors(path).iter().any(|folder| {
            self.row(folder)
                .is_some_and(|row| row.present && row.kind != "folder")
        });
        let over_folder = record.state.is_some()
            && self
                .row(path)
                .is_some_and(|row| row.present && row.kind == "folder");
        // A local-only path takes the drive's version only where this
        // machine has not changed the entry since the version it applied.
        let replaceable = self.row(path).is_none_or(|row| {
            row.present && row.kind == "file" && row.content.as_deref() == Some("cache")
        });
        let in_local_only = self.local_only(path, "file").is_some() && !replaceable;
        let local_only = relisting
            && self
                .row(path)
                .is_some_and(|row| self.local_only(path, &row.kind).is_some())
            && !replaceable;
        self.pending.contains(path) || below_file || over_folder || in_local_only || local_only
    }

    fn apply(&mut self, record: &Record, relisting: bool) {
        let path = record.path.as_str();
        if let Some(row) = self.rows.get_mut(path) {
            set_version(row, record.state.as_ref());
        }
        if self.blocked(record, relisting) {
            if let Some(version) = &record.state
                && !self.rows.contains_key(path)
            {
                // The drive holds a file the mount does not show.
                let mut row = self.folder(path);
                row.kind = "file".to_owned();
                row.present = false;
                row.origin = None;
                row.local_only = None;
                row.mtime_ns = None;
                row.ctime_ns = None;
                set_version(&mut row, Some(version));
                self.rows.insert(path.to_owned(), row);
            }
            return;
        }
        match &record.state {
            Some(version) => self.write(path, version),
            None => self.delete(path),
        }
    }

    fn write(&mut self, path: &str, version: &Version) {
        for folder in ancestors(path) {
            if !self.rows.contains_key(folder) {
                let row = self.folder(folder);
                self.rows.insert(folder.to_owned(), row);
                if let Some(above) = parent(folder) {
                    self.touch(above);
                }
            }
        }
        let now = self.now_ns;
        let existing = self.rows.get(path).cloned();
        let shown = existing.as_ref().is_some_and(|row| row.present);
        let same = existing.as_ref().is_some_and(|row| {
            row.present
                && row.kind == "file"
                && row.local_sha256.as_deref() == Some(version.sha256.as_str())
        });
        let file_id = match &existing {
            Some(row) => row.file_id,
            None => self.new_id(),
        };
        let mut row = existing.unwrap_or_else(|| {
            let mut row = self.folder(path);
            row.file_id = file_id;
            row
        });
        row.file_id = file_id;
        row.kind = "file".to_owned();
        row.present = true;
        row.content = Some("cache".to_owned());
        row.local_sha256 = Some(version.sha256.clone());
        row.size = Some(version.size);
        row.executable = Some(version.executable);
        row.origin = None;
        row.link_target = None;
        row.local_only = self.local_only(path, "file").map(str::to_owned);
        set_version(&mut row, Some(version));
        if !same {
            row.mtime_ns = Some(now);
            row.ctime_ns = Some(now);
        }
        self.rows.insert(path.to_owned(), row);
        if !shown && let Some(folder) = parent(path) {
            self.touch(folder);
        }
    }

    fn delete(&mut self, path: &str) {
        let Some(row) = self.rows.get(path) else {
            return;
        };
        if row.kind == "folder" {
            return;
        }
        let shown = row.present;
        self.rows.remove(path);
        if !shown {
            return;
        }
        let mut at = parent(path);
        while let Some(folder) = at {
            self.touch(folder);
            let empty_drive_folder = !folder.is_empty()
                && self.row(folder).is_some_and(|row| {
                    row.kind == "folder" && row.origin.as_deref() == Some("drive")
                })
                && !self.holds_entry(folder)
                && !self.pending.contains(folder);
            if !empty_drive_folder {
                return;
            }
            self.rows.remove(folder);
            at = parent(folder);
        }
    }

    /// The write that turns `before` into the rows as they are now.
    fn into_write(self, before: &BTreeMap<String, FileRow>) -> (Write, i64) {
        let mut write = Write::default();
        for path in before.keys() {
            if !self.rows.contains_key(path) {
                write
                    .files_removed
                    .push((self.drive_id.to_owned(), path.clone()));
            }
        }
        for (path, row) in &self.rows {
            if before.get(path) != Some(row) {
                write.files.push(row.clone());
            }
        }
        (write, self.next_file_id)
    }
}

fn set_version(row: &mut FileRow, version: Option<&Version>) {
    row.version_id = version.map(|each| each.version_id.clone());
    row.synced_sha256 = version.map(|each| each.sha256.clone());
    row.synced_size = version.map(|each| each.size);
    row.synced_executable = version.map(|each| each.executable);
}

fn drive_rows(tables: &Tables, drive_id: &str) -> BTreeMap<String, FileRow> {
    tables
        .files
        .iter()
        .filter(|((drive, _), _)| drive == drive_id)
        .map(|((_, path), row)| (path.clone(), row.clone()))
        .collect()
}

fn work<'a>(tables: &Tables, drive_id: &'a str, now_ns: i64) -> Work<'a> {
    Work {
        drive_id,
        rows: drive_rows(tables, drive_id),
        pending: tables
            .pending_changes
            .keys()
            .filter(|(drive, _)| drive == drive_id)
            .map(|(_, path)| path.clone())
            .collect(),
        next_file_id: tables
            .drive_cursor
            .get(drive_id)
            .map_or(TOP_FOLDER_ID + 1, |cursor| cursor.next_file_id),
        now_ns,
    }
}

fn listed(row: &ListingRow) -> Record {
    Record {
        path: row.path.clone(),
        state: Some(Version {
            version_id: row.version_id.clone(),
            sha256: row.content_sha256.clone(),
            size: row.size_bytes,
            executable: row.executable,
        }),
    }
}

/// The final transaction of a listing of `drive_id` whose rows are in
/// `listing`: a first listing when the drive has no `drive_cursor` row,
/// with `generation` for the new row, otherwise a relisting. The cursor's
/// `applied_seq` becomes `start_seq` and the drive's `listing` rows go.
pub fn finish_listing(
    tables: &Tables,
    drive_id: &str,
    start_seq: i64,
    generation: i64,
    now_ns: i64,
) -> Write {
    let cursor = tables.drive_cursor.get(drive_id);
    let mut work = work(tables, drive_id, now_ns);
    let before = work.rows.clone();
    let rows: Vec<Record> = tables
        .listing
        .iter()
        .filter(|((drive, _), _)| drive == drive_id)
        .map(|(_, row)| listed(row))
        .collect();
    let relisting = cursor.is_some();
    if !relisting && !work.rows.contains_key("") {
        let mut top = work.folder("");
        top.file_id = TOP_FOLDER_ID;
        work.rows.insert(String::new(), top);
    }
    if relisting {
        let listed_paths: BTreeSet<&str> = rows.iter().map(|row| row.path.as_str()).collect();
        let gone: Vec<Record> = work
            .rows
            .iter()
            .filter(|(path, row)| row.version_id.is_some() && !listed_paths.contains(path.as_str()))
            .map(|(path, _)| Record {
                path: path.clone(),
                state: None,
            })
            .collect();
        for record in gone {
            work.apply(&record, true);
        }
    }
    for record in &rows {
        work.apply(record, relisting);
    }
    let (mut write, next_file_id) = work.into_write(&before);
    write.cursor.push(Cursor {
        drive_id: drive_id.to_owned(),
        applied_seq: start_seq,
        generation: cursor.map_or(generation, |cursor| cursor.generation),
        next_file_id,
    });
    write.listing_removed_drives.push(drive_id.to_owned());
    write
}

/// The transaction of one page of change records of `drive_id`, in
/// `(seq, index)` order, with the new `applied_seq`.
pub fn apply_page(
    tables: &Tables,
    drive_id: &str,
    records: &[Record],
    applied_seq: i64,
    now_ns: i64,
) -> Write {
    let cursor = tables
        .drive_cursor
        .get(drive_id)
        .expect("records are listed after a listing")
        .clone();
    let mut work = work(tables, drive_id, now_ns);
    let before = work.rows.clone();
    for record in records {
        work.apply(record, false);
    }
    let (mut write, next_file_id) = work.into_write(&before);
    write.cursor.push(Cursor {
        applied_seq,
        next_file_id,
        ..cursor
    });
    write
}

/// The `file_id`s of the present files whose content `write` replaces.
pub fn replaced(tables: &Tables, write: &Write) -> Vec<i64> {
    write
        .files
        .iter()
        .filter(|row| row.present && row.kind == "file")
        .filter(|row| {
            tables
                .files
                .get(&(row.drive_id.clone(), row.path.clone()))
                .is_some_and(|before| {
                    before.present
                        && before.kind == "file"
                        && before.file_id == row.file_id
                        && (&before.content, &before.local_sha256)
                            != (&row.content, &row.local_sha256)
                })
        })
        .map(|row| row.file_id)
        .collect()
}

/// The write that brings the local entry at each of `paths` of `drive_id`
/// to the drive version its row holds, as a record of that version would,
/// unless something blocks it; `applied_seq` stays.
pub(crate) fn restore(tables: &Tables, drive_id: &str, paths: &[String], now_ns: i64) -> Write {
    let cursor = tables.drive_cursor.get(drive_id).cloned();
    let mut work = work(tables, drive_id, now_ns);
    let before = work.rows.clone();
    for path in paths {
        let state = work.row(path).and_then(|row| {
            Some(Version {
                version_id: row.version_id.clone()?,
                sha256: row.synced_sha256.clone()?,
                size: row.synced_size?,
                executable: row.synced_executable?,
            })
        });
        work.apply(
            &Record {
                path: path.clone(),
                state,
            },
            false,
        );
    }
    let (mut write, next_file_id) = work.into_write(&before);
    if let Some(cursor) = cursor
        && cursor.next_file_id != next_file_id
    {
        write.cursor.push(Cursor {
            next_file_id,
            ..cursor
        });
    }
    write
}
