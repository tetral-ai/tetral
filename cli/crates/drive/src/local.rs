//! Local changes: the rows one operation through a mount changes, built as
//! one [`Write`], and the pending changes it records with their bases. A
//! change made while a commit that carries its path is in flight gets its
//! base from that commit's answer ([`accepted_in_flight`]).

use std::collections::BTreeMap;

use crate::paths;
use crate::state::{FileRow, PendingChange, Tables, Write};

/// The content a recorded write carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Written<'a> {
    /// New bytes, still to be uploaded.
    Bytes,
    /// Only the executable bit changed.
    Mode,
    /// A renamed entry's content: the cache blob of this hash, which the
    /// drive holds, or `None` for a content file.
    Moved(Option<&'a str>),
}

/// The rows of one drive that one operation changes, over the tables as
/// they stand.
pub(crate) struct Edit<'a> {
    tables: &'a Tables,
    drive_id: &'a str,
    files: BTreeMap<String, Option<FileRow>>,
    pending: BTreeMap<String, Option<PendingChange>>,
    changed_during_flight: Vec<String>,
    next_file_id: i64,
    pub(crate) now_ns: i64,
}

impl<'a> Edit<'a> {
    /// `None` while the drive has no `drive_cursor` row.
    pub(crate) fn new(tables: &'a Tables, drive_id: &'a str, now_ns: i64) -> Option<Edit<'a>> {
        let cursor = tables.drive_cursor.get(drive_id)?;
        Some(Edit {
            tables,
            drive_id,
            files: BTreeMap::new(),
            pending: BTreeMap::new(),
            changed_during_flight: Vec::new(),
            next_file_id: cursor.next_file_id,
            now_ns,
        })
    }

    pub(crate) fn drive_id(&self) -> &str {
        self.drive_id
    }

    fn key(&self, path: &str) -> (String, String) {
        (self.drive_id.to_owned(), path.to_owned())
    }

    pub(crate) fn new_id(&mut self) -> i64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }

    /// The row at `path`, present or not.
    pub(crate) fn row(&self, path: &str) -> Option<&FileRow> {
        match self.files.get(path) {
            Some(row) => row.as_ref(),
            None => self.tables.files.get(&self.key(path)),
        }
    }

    /// The present entry at `path`.
    pub(crate) fn entry(&self, path: &str) -> Option<&FileRow> {
        self.row(path).filter(|row| row.present)
    }

    pub(crate) fn set_row(&mut self, row: FileRow) {
        self.files.insert(row.path.clone(), Some(row));
    }

    pub(crate) fn remove_row(&mut self, path: &str) {
        self.files.insert(path.to_owned(), None);
    }

    /// The paths of the rows at and below `folder`, present or not, in
    /// UTF-8 byte order.
    pub(crate) fn paths_at_or_below(&self, folder: &str) -> Vec<String> {
        let mut paths: Vec<String> = self
            .tables
            .files
            .range(self.key(folder)..)
            .take_while(|((drive, path), _)| drive == self.drive_id && path.starts_with(folder))
            .map(|((_, path), _)| path.clone())
            .chain(self.files.keys().cloned())
            .filter(|path| paths::at_or_below(path, folder))
            .collect();
        paths.sort();
        paths.dedup();
        paths.retain(|path| self.row(path).is_some());
        paths
    }

    /// Whether any present entry lies below `folder`.
    pub(crate) fn holds_entry(&self, folder: &str) -> bool {
        self.paths_at_or_below(folder)
            .iter()
            .any(|path| path != folder && self.entry(path).is_some())
    }

    pub(crate) fn local_only(&self, path: &str, kind: &str) -> Option<&'static str> {
        paths::local_only(path, kind, |each| self.entry(each).is_some())
    }

    /// Sets the `local_only` column of every row at and below `folder`
    /// after an entry named `.git` came or went, or a subtree moved.
    pub(crate) fn refresh_local_only(&mut self, folder: &str) {
        for path in self.paths_at_or_below(folder) {
            let Some(row) = self.row(&path) else {
                continue;
            };
            let reason = row
                .present
                .then(|| self.local_only(&path, &row.kind))
                .flatten()
                .map(str::to_owned);
            if row.local_only != reason {
                let mut row = row.clone();
                row.local_only = reason;
                self.set_row(row);
            }
        }
    }

    /// A folder gained, lost or renamed an entry.
    pub(crate) fn touch(&mut self, folder: &str) {
        if let Some(row) = self.entry(folder) {
            let mut row = row.clone();
            row.mtime_ns = Some(self.now_ns);
            row.ctime_ns = Some(self.now_ns);
            self.set_row(row);
        }
    }

    pub(crate) fn pending(&self, path: &str) -> Option<&PendingChange> {
        match self.pending.get(path) {
            Some(change) => change.as_ref(),
            None => self.tables.pending_changes.get(&self.key(path)),
        }
    }

    fn in_flight(&self, path: &str) -> bool {
        self.tables.inflight_commits.contains_key(&self.key(path))
    }

    /// The base of a change of `path`: the pending change's, or for the
    /// first change since its last commit, rejection or abandonment the
    /// drive version this machine applied, none when the drive holds no
    /// file there. While a commit carrying the path is in flight, the
    /// change is marked so that its answer sets the base.
    fn base(&mut self, path: &str) -> Option<String> {
        if self.in_flight(path) && !self.changed_during_flight.iter().any(|each| each == path) {
            self.changed_during_flight.push(path.to_owned());
        }
        if let Some(change) = self.pending(path) {
            return change.base_sha256.clone();
        }
        if self.in_flight(path) {
            return None;
        }
        self.row(path).and_then(|row| row.synced_sha256.clone())
    }

    /// Records a write of the present file at `path`.
    pub(crate) fn record_write(&mut self, path: &str, written: Written<'_>, task_id: Option<&str>) {
        let base_sha256 = self.base(path);
        let row = self.entry(path).expect("a written file is present").clone();
        let earlier = self.pending(path).cloned();
        let (content_sha256, uploaded) = match written {
            Written::Bytes | Written::Moved(None) => (None, false),
            Written::Moved(Some(sha256)) => (Some(sha256.to_owned()), true),
            Written::Mode => match &earlier {
                Some(change) if change.change == "write" => {
                    (change.content_sha256.clone(), change.uploaded)
                }
                _ if row.content.as_deref() == Some("cache") => (row.local_sha256.clone(), true),
                _ => (None, false),
            },
        };
        let change = PendingChange {
            drive_id: self.drive_id.to_owned(),
            path: path.to_owned(),
            change: "write".to_owned(),
            base_sha256,
            content_sha256,
            executable: row.executable,
            uploaded,
            last_write_at: self.now_ns / 1_000_000,
            task_id: task_id.map(str::to_owned),
            fault_code: earlier.and_then(|change| change.fault_code),
        };
        self.pending.insert(path.to_owned(), Some(change));
    }

    /// Records the deletion of the file that was at `path`. With no base
    /// and no commit of the path in flight the drive holds nothing to
    /// delete, so the path keeps no pending change.
    pub(crate) fn record_delete(&mut self, path: &str, task_id: Option<&str>) {
        let base_sha256 = self.base(path);
        if base_sha256.is_none() && !self.in_flight(path) {
            self.pending.insert(path.to_owned(), None);
            return;
        }
        let earlier = self.pending(path).cloned();
        let change = PendingChange {
            drive_id: self.drive_id.to_owned(),
            path: path.to_owned(),
            change: "delete".to_owned(),
            base_sha256,
            content_sha256: None,
            executable: None,
            uploaded: false,
            last_write_at: self.now_ns / 1_000_000,
            task_id: task_id.map(str::to_owned),
            fault_code: earlier.and_then(|change| change.fault_code),
        };
        self.pending.insert(path.to_owned(), Some(change));
    }

    /// The write that turns the tables into the rows as edited.
    pub(crate) fn into_write(self) -> Write {
        let mut write = Write::default();
        for (path, row) in self.files {
            match row {
                Some(row) => {
                    if self
                        .tables
                        .files
                        .get(&(self.drive_id.to_owned(), path.clone()))
                        != Some(&row)
                    {
                        write.files.push(row);
                    }
                }
                None => {
                    if self
                        .tables
                        .files
                        .contains_key(&(self.drive_id.to_owned(), path.clone()))
                    {
                        write.files_removed.push((self.drive_id.to_owned(), path));
                    }
                }
            }
        }
        for (path, change) in self.pending {
            match change {
                Some(change) => write.pending.push(change),
                None => {
                    let key = (self.drive_id.to_owned(), path);
                    if self.tables.pending_changes.contains_key(&key) {
                        write.pending_removed.push(key);
                    }
                }
            }
        }
        for path in self.changed_during_flight {
            write
                .changed_during_flight
                .push((self.drive_id.to_owned(), path));
        }
        let cursor = &self.tables.drive_cursor[self.drive_id];
        if cursor.next_file_id != self.next_file_id {
            write.cursor.push(crate::state::Cursor {
                next_file_id: self.next_file_id,
                ..cursor.clone()
            });
        }
        write
    }
}

/// The write that gives the change of `path` made while an accepted commit
/// carrying the path was in flight its base: the content that commit
/// committed, or none after a deletion, in which case a deletion keeps no
/// pending change. Nothing when the path was not changed during the flight.
pub fn accepted_in_flight(tables: &Tables, drive_id: &str, path: &str) -> Write {
    let key = (drive_id.to_owned(), path.to_owned());
    let mut write = Write::default();
    let (Some(inflight), Some(change)) = (
        tables.inflight_commits.get(&key),
        tables.pending_changes.get(&key),
    ) else {
        return write;
    };
    if !inflight.changed_during_flight {
        return write;
    }
    let base_sha256 = match inflight.change.as_str() {
        "write" => inflight.content_sha256.clone(),
        _ => None,
    };
    if base_sha256.is_none() && change.change == "delete" {
        write.pending_removed.push(key);
        return write;
    }
    write.pending.push(PendingChange {
        base_sha256,
        ..change.clone()
    });
    write
}
