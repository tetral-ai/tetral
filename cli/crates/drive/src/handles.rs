//! Open regular files belong to the Drive sync owner. The namespace may move
//! or unlink an inode without ending its open lifetime. A cached descriptor
//! is reused only while its hash is the inode's current content source.

use std::fs::{File, OpenOptions};
use std::sync::Arc;

use rustix::io::Errno;

use crate::state::FileRow;
use crate::sync::Owner;

pub(crate) struct OpenFile {
    pub references: usize,
    pub row: FileRow,
    pub file: Option<Arc<File>>,
    pub unlinked: bool,
}

impl Owner {
    pub(crate) fn file_row(&self, drive: &str, ino: u64) -> Result<&FileRow, Errno> {
        let key = (
            drive.to_owned(),
            i64::try_from(ino).map_err(|_| Errno::NOENT)?,
        );
        if let Some(path) = self.inodes.get(&key)
            && let Some(row) = self
                .state
                .tables()
                .files
                .get(&(drive.to_owned(), path.clone()))
            && row.present
            && row.file_id == key.1
        {
            return Ok(row);
        }
        self.open_files
            .get(&key)
            .map(|open| &open.row)
            .ok_or(Errno::NOENT)
    }

    pub(crate) fn open_file(&mut self, drive: &str, ino: u64) -> Result<(), Errno> {
        let row = self.file_row(drive, ino)?.clone();
        if row.kind != "file" {
            return Err(if row.kind == "folder" {
                Errno::ISDIR
            } else {
                Errno::INVAL
            });
        }
        let key = (drive.to_owned(), row.file_id);
        if let Some(open) = self.open_files.get_mut(&key) {
            open.references += 1;
            return Ok(());
        }
        // Pin a local descriptor before unlink can remove its directory entry.
        // A missing cache blob stays lazy until a read or partial write needs it.
        let file = if row.content.as_deref() == Some("session") {
            Some(Arc::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(self.content_path(drive, row.file_id))
                    .map_err(|_| Errno::IO)?,
            ))
        } else {
            row.local_sha256
                .as_ref()
                .and_then(|hash| File::open(self.blobs.join(hash)).ok())
                .map(Arc::new)
        };
        self.open_files.insert(
            key,
            OpenFile {
                references: 1,
                row,
                file,
                unlinked: false,
            },
        );
        Ok(())
    }

    pub(crate) fn release_file(&mut self, drive: &str, ino: u64) {
        let key = (drive.to_owned(), ino as i64);
        if let Some(open) = self.open_files.get_mut(&key) {
            open.references -= 1;
            if open.references == 0 {
                self.open_files.remove(&key);
            }
        }
    }

    pub(crate) fn open_descriptor(&self, drive: &str, ino: u64) -> Option<Arc<File>> {
        self.open_files
            .get(&(drive.to_owned(), ino as i64))
            .and_then(|open| open.file.clone())
    }

    pub(crate) fn unlinked(&self, drive: &str, ino: u64) -> bool {
        self.open_files
            .get(&(drive.to_owned(), ino as i64))
            .is_some_and(|open| open.unlinked)
    }

    /// Updates descriptor selection only after the corresponding database
    /// transaction succeeds. Removed/replaced inodes retain their previous row.
    pub(crate) fn refresh_open_files(&mut self) {
        for (key, open) in &mut self.open_files {
            if open.unlinked {
                continue;
            }
            let row = self
                .inodes
                .get(key)
                .and_then(|path| {
                    self.state
                        .tables()
                        .files
                        .get(&(key.0.clone(), path.clone()))
                })
                .filter(|row| row.present && row.file_id == key.1);
            let Some(row) = row else {
                open.unlinked = true;
                continue;
            };
            if row.content.as_deref() == Some("session") {
                let path = self
                    .session_dir
                    .join("content")
                    .join(&key.0)
                    .join(key.1.to_string());
                open.file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .ok()
                    .map(Arc::new);
            } else if row.content != open.row.content || row.local_sha256 != open.row.local_sha256 {
                open.file = None;
            }
            open.row = row.clone();
        }
    }

    /// A cache result is reusable only for the same current inode content.
    pub(crate) fn read_opened(
        &mut self,
        drive: &str,
        ino: u64,
        hash: &str,
        file: Arc<File>,
    ) -> bool {
        let Ok(row) = self.file_row(drive, ino) else {
            return false;
        };
        if row.content.as_deref() != Some("cache") || row.local_sha256.as_deref() != Some(hash) {
            return false;
        }
        if let Some(open) = self.open_files.get_mut(&(drive.to_owned(), ino as i64)) {
            open.file = Some(file);
        }
        true
    }
}
