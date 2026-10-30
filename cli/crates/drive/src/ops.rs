//! The operation table both mount adapters serve, answered by the Session's
//! Drive sync owner from the present rows of `files`. Inode numbers are
//! `file_id`s, the top folder's 1. Names are converted to NFC before use.
//!
//! An operation that changes an entry commits its rows and pending changes
//! in one transaction before it replies, and writes its bytes into the
//! content file `sessions/<session_id>/content/<drive_id>/<file_id>` before
//! that. A read is answered with where its bytes are; waiting for a blob's
//! download happens outside the owner, so it blocks no other operation.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use bytes::Bytes;
use protocol::field::Sha256;
use protocol::id::Id;
use rustix::io::Errno;
use tokio::sync::oneshot;

use crate::cache::Needed;
use crate::local::{Edit, Written};
use crate::paths;
use crate::state::{FileRow, Key, Write};
use crate::sync::{DriveSync, Message, Owner, Reply, now_ns};

/// The inode number of a drive's top folder.
pub const TOP_INODE: u64 = 1;

/// The kind of a special file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Special {
    Fifo,
    Socket,
    CharDevice(u64),
    BlockDevice(u64),
}

impl Special {
    /// The form kept in the row's `link_target`.
    fn stored(self) -> String {
        match self {
            Special::Fifo => "fifo".to_owned(),
            Special::Socket => "socket".to_owned(),
            Special::CharDevice(rdev) => format!("char {rdev}"),
            Special::BlockDevice(rdev) => format!("block {rdev}"),
        }
    }

    fn parse(stored: &str) -> Special {
        let device = |rest: &str| rest.parse().unwrap_or(0);
        match stored.split_once(' ') {
            Some(("char", rdev)) => Special::CharDevice(device(rdev)),
            Some(("block", rdev)) => Special::BlockDevice(device(rdev)),
            _ if stored == "socket" => Special::Socket,
            _ => Special::Fifo,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Folder,
    Symlink,
    Special(Special),
}

/// An entry's attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attr {
    pub ino: u64,
    pub kind: Kind,
    pub size: u64,
    /// The permission bits: `0644` or `0755` for a file, `0755` for a
    /// folder, `0777` for a symbolic link.
    pub mode: u32,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub uid: u32,
    pub gid: u32,
}

/// An entry of a folder listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub attr: Attr,
}

/// The attribute changes of one request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetAttr {
    pub size: Option<u64>,
    pub mode: Option<u32>,
    pub mtime_ns: Option<i64>,
}

/// A file of the Session whose read waits for its content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Downloading {
    /// `R/sessions/<session_id>/drives/<drive_id>/<drive path>`.
    pub path: PathBuf,
    pub downloaded_bytes: u64,
    pub size_bytes: u64,
}

/// A local-only entry a task created, for its next sync point's
/// `not_synced`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// The machine path of the link, the special file, or the folder
    /// holding `.git`.
    pub path: PathBuf,
    /// `symlink`, `special_file` or `git_repository`.
    pub reason: &'static str,
}

pub(crate) enum Op {
    Lookup {
        parent: u64,
        name: Vec<u8>,
    },
    GetAttr(u64),
    ReadDir(u64),
    Read(u64),
    ReadLink(u64),
    Create {
        parent: u64,
        name: Vec<u8>,
        mode: u32,
        task: Option<String>,
    },
    Write {
        ino: u64,
        offset: u64,
        data: Bytes,
        blob: Option<(String, File)>,
        task: Option<String>,
    },
    SetAttr {
        ino: u64,
        set: SetAttr,
        blob: Option<(String, File)>,
        task: Option<String>,
    },
    MakeFolder {
        parent: u64,
        name: Vec<u8>,
        task: Option<String>,
    },
    Remove {
        parent: u64,
        name: Vec<u8>,
        task: Option<String>,
    },
    RemoveFolder {
        parent: u64,
        name: Vec<u8>,
        task: Option<String>,
    },
    Rename {
        parent: u64,
        name: Vec<u8>,
        new_parent: u64,
        new_name: Vec<u8>,
        task: Option<String>,
    },
    Symlink {
        parent: u64,
        name: Vec<u8>,
        target: Vec<u8>,
        task: Option<String>,
    },
    MakeSpecial {
        parent: u64,
        name: Vec<u8>,
        special: Special,
        task: Option<String>,
    },
    Sync(u64),
}

pub(crate) enum Done {
    Attr(Attr),
    Entries(Vec<DirEntry>),
    Source(Source),
    Link(Vec<u8>),
    Written(u32),
    /// The entry's blob is needed first: its hash and the download.
    NeedBlob(String, Needed),
    Unit,
}

/// Where a read's bytes are.
pub(crate) enum Source {
    Session(PathBuf),
    /// The blob of `needed`; `token` ends the read's place among the
    /// downloads in progress.
    Cache {
        needed: Needed,
        token: u64,
    },
}

fn hash_of(text: &str) -> Result<Sha256, Errno> {
    Sha256::new(text.to_owned()).map_err(|_| Errno::IO)
}

impl DriveSync {
    async fn op(&self, drive_id: &Id, op: Op) -> Result<Done, Errno> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Op {
            drive_id: drive_id.as_str().to_owned(),
            op,
            reply,
        });
        answer.await.unwrap_or(Err(Errno::IO))
    }

    /// The task that the process `pid` belongs to; none without a process
    /// ID, as on macOS.
    async fn task(&self, pid: Option<u32>) -> Option<String> {
        let pid = pid?;
        let (reply, answer) = oneshot::channel();
        self.tasks.task_of(
            pid,
            Box::new(move |task| {
                let _ = reply.send(task);
            }),
        );
        answer
            .await
            .ok()
            .flatten()
            .map(|task| task.as_str().to_owned())
    }

    /// Gets the entry's blob and repeats `op` with it while the owner asks
    /// for one.
    async fn with_blob(
        &self,
        drive_id: &Id,
        mut op: impl FnMut(Option<(String, File)>) -> Op,
    ) -> Result<Done, Errno> {
        let mut blob = None;
        loop {
            match self.op(drive_id, op(blob.take())).await? {
                Done::NeedBlob(sha256, needed) => {
                    let file = self.cache.read(needed).await.map_err(|_| Errno::IO)?;
                    blob = Some((sha256, file));
                }
                done => return Ok(done),
            }
        }
    }

    pub async fn lookup(&self, drive_id: &Id, parent: u64, name: &[u8]) -> Result<Attr, Errno> {
        let op = Op::Lookup {
            parent,
            name: name.to_vec(),
        };
        attr(self.op(drive_id, op).await?)
    }

    pub async fn getattr(&self, drive_id: &Id, ino: u64) -> Result<Attr, Errno> {
        attr(self.op(drive_id, Op::GetAttr(ino)).await?)
    }

    /// The folder's entries in UTF-8 byte order of name.
    pub async fn readdir(&self, drive_id: &Id, ino: u64) -> Result<Vec<DirEntry>, Errno> {
        match self.op(drive_id, Op::ReadDir(ino)).await? {
            Done::Entries(entries) => Ok(entries),
            _ => Err(Errno::IO),
        }
    }

    /// Up to `size` bytes of the file's content at `offset`, waiting for
    /// its download with no time limit; `EIO` when the download fails
    /// permanently.
    pub async fn read(
        &self,
        drive_id: &Id,
        ino: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, Errno> {
        let source = match self.op(drive_id, Op::Read(ino)).await? {
            Done::Source(source) => source,
            _ => return Err(Errno::IO),
        };
        let read_at = |file: &File| -> Result<Vec<u8>, Errno> {
            let mut bytes = vec![0; size as usize];
            let mut filled = 0;
            while filled < bytes.len() {
                match file.read_at(&mut bytes[filled..], offset + filled as u64) {
                    Ok(0) => break,
                    Ok(count) => filled += count,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(errno(&error)),
                }
            }
            bytes.truncate(filled);
            Ok(bytes)
        };
        match source {
            Source::Session(path) => read_at(&File::open(path).map_err(|error| errno(&error))?),
            Source::Cache { needed, token } => {
                let file = self.cache.read(needed).await;
                self.send(Message::ReadEnded(token));
                read_at(&file.map_err(|_| Errno::IO)?)
            }
        }
    }

    pub async fn readlink(&self, drive_id: &Id, ino: u64) -> Result<Vec<u8>, Errno> {
        match self.op(drive_id, Op::ReadLink(ino)).await? {
            Done::Link(target) => Ok(target),
            _ => Err(Errno::IO),
        }
    }

    /// Creates an empty file, executable when `mode` has an execute bit;
    /// `pid` is the requesting process.
    pub async fn create(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        mode: u32,
        pid: Option<u32>,
    ) -> Result<Attr, Errno> {
        let task = self.task(pid).await;
        let op = Op::Create {
            parent,
            name: name.to_vec(),
            mode,
            task,
        };
        attr(self.op(drive_id, op).await?)
    }

    /// Writes `data` at `offset` into the file's content file; the change
    /// is stored when this returns.
    pub async fn write(
        &self,
        drive_id: &Id,
        ino: u64,
        offset: u64,
        data: &[u8],
        pid: Option<u32>,
    ) -> Result<u32, Errno> {
        let task = self.task(pid).await;
        let data = Bytes::copy_from_slice(data);
        let done = self
            .with_blob(drive_id, |blob| Op::Write {
                ino,
                offset,
                data: data.clone(),
                blob,
                task: task.clone(),
            })
            .await?;
        match done {
            Done::Written(count) => Ok(count),
            _ => Err(Errno::IO),
        }
    }

    /// Changes the size, the executable bit or the modification time.
    pub async fn setattr(
        &self,
        drive_id: &Id,
        ino: u64,
        set: SetAttr,
        pid: Option<u32>,
    ) -> Result<Attr, Errno> {
        let task = self.task(pid).await;
        attr(
            self.with_blob(drive_id, |blob| Op::SetAttr {
                ino,
                set: set.clone(),
                blob,
                task: task.clone(),
            })
            .await?,
        )
    }

    pub async fn mkdir(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        pid: Option<u32>,
    ) -> Result<Attr, Errno> {
        let task = self.task(pid).await;
        let op = Op::MakeFolder {
            parent,
            name: name.to_vec(),
            task,
        };
        attr(self.op(drive_id, op).await?)
    }

    pub async fn unlink(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        pid: Option<u32>,
    ) -> Result<(), Errno> {
        let task = self.task(pid).await;
        let op = Op::Remove {
            parent,
            name: name.to_vec(),
            task,
        };
        self.op(drive_id, op).await.map(|_| ())
    }

    pub async fn rmdir(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        pid: Option<u32>,
    ) -> Result<(), Errno> {
        let task = self.task(pid).await;
        let op = Op::RemoveFolder {
            parent,
            name: name.to_vec(),
            task,
        };
        self.op(drive_id, op).await.map(|_| ())
    }

    pub async fn rename(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        new_parent: u64,
        new_name: &[u8],
        pid: Option<u32>,
    ) -> Result<(), Errno> {
        let task = self.task(pid).await;
        let op = Op::Rename {
            parent,
            name: name.to_vec(),
            new_parent,
            new_name: new_name.to_vec(),
            task,
        };
        self.op(drive_id, op).await.map(|_| ())
    }

    pub async fn symlink(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        target: &[u8],
        pid: Option<u32>,
    ) -> Result<Attr, Errno> {
        let task = self.task(pid).await;
        let op = Op::Symlink {
            parent,
            name: name.to_vec(),
            target: target.to_vec(),
            task,
        };
        attr(self.op(drive_id, op).await?)
    }

    /// Makes a FIFO, a socket or a device node.
    pub async fn mknod(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        special: Special,
        pid: Option<u32>,
    ) -> Result<Attr, Errno> {
        let task = self.task(pid).await;
        let op = Op::MakeSpecial {
            parent,
            name: name.to_vec(),
            special,
            task,
        };
        attr(self.op(drive_id, op).await?)
    }

    /// Hard links are never made: nothing is stored or recorded.
    pub async fn link(&self, _drive_id: &Id, _ino: u64, _new_parent: u64) -> Result<Attr, Errno> {
        Err(Errno::PERM)
    }

    pub fn setxattr(&self) -> Result<(), Errno> {
        Err(Errno::NOTSUP)
    }

    /// Entries hold no extended attributes.
    pub fn getxattr(&self) -> Result<Vec<u8>, Errno> {
        Err(Errno::NODATA)
    }

    pub fn listxattr(&self) -> Vec<u8> {
        Vec::new()
    }

    /// A flush or an `fsync`: the content file reaches the disk.
    pub async fn fsync(&self, drive_id: &Id, ino: u64) -> Result<(), Errno> {
        self.op(drive_id, Op::Sync(ino)).await.map(|_| ())
    }

    /// The Session's files whose reads wait for a download, with the bytes
    /// received so far.
    pub async fn downloading(&self) -> Vec<Downloading> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Reads(reply));
        let reads = answer.await.unwrap_or_default();
        let received: std::collections::HashMap<String, u64> = self
            .cache
            .downloading(&self.session_id)
            .into_iter()
            .map(|(sha256, bytes)| (sha256.as_str().to_owned(), bytes))
            .collect();
        let mut each: Vec<Downloading> = reads
            .into_iter()
            .filter_map(|(path, sha256, size)| {
                received.get(&sha256).map(|bytes| Downloading {
                    path,
                    downloaded_bytes: *bytes,
                    size_bytes: size,
                })
            })
            .collect();
        each.sort_by(|a, b| a.path.cmp(&b.path));
        each.dedup_by(|a, b| a.path == b.path);
        each
    }

    /// Takes the local-only notices kept for `task_id`.
    pub async fn notices(&self, task_id: &Id) -> Vec<Notice> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Notices {
            task_id: task_id.as_str().to_owned(),
            reply,
        });
        answer.await.unwrap_or_default()
    }
}

fn attr(done: Done) -> Result<Attr, Errno> {
    match done {
        Done::Attr(attr) => Ok(attr),
        _ => Err(Errno::IO),
    }
}

fn errno(error: &io::Error) -> Errno {
    error
        .raw_os_error()
        .map_or(Errno::IO, Errno::from_raw_os_error)
}

fn attr_of(row: &FileRow) -> Attr {
    let kind = match row.kind.as_str() {
        "folder" => Kind::Folder,
        "symlink" => Kind::Symlink,
        "special" => Kind::Special(Special::parse(row.link_target.as_deref().unwrap_or(""))),
        _ => Kind::File,
    };
    let mode = match kind {
        Kind::Folder => 0o755,
        Kind::Symlink => 0o777,
        Kind::File if row.executable == Some(true) => 0o755,
        _ => 0o644,
    };
    let size = match kind {
        Kind::File => row.size.unwrap_or(0),
        Kind::Symlink => row.link_target.as_deref().map_or(0, str::len) as i64,
        _ => 0,
    };
    Attr {
        ino: row.file_id as u64,
        kind,
        size: size.max(0) as u64,
        mode,
        mtime_ns: row.mtime_ns.unwrap_or(0),
        ctime_ns: row.ctime_ns.unwrap_or(0),
        uid: rustix::process::getuid().as_raw(),
        gid: rustix::process::getgid().as_raw(),
    }
}

/// A new present row of `kind` at `path`, which keeps the drive-version
/// columns of any row already there.
fn new_row(edit: &mut Edit<'_>, path: &str, kind: &str) -> FileRow {
    let earlier = edit.row(path).cloned();
    let now = edit.now_ns;
    FileRow {
        drive_id: edit.drive_id().to_owned(),
        path: path.to_owned(),
        file_id: edit.new_id(),
        kind: kind.to_owned(),
        present: true,
        version_id: earlier.as_ref().and_then(|row| row.version_id.clone()),
        synced_sha256: earlier.as_ref().and_then(|row| row.synced_sha256.clone()),
        synced_size: earlier.as_ref().and_then(|row| row.synced_size),
        synced_executable: earlier.as_ref().and_then(|row| row.synced_executable),
        content: None,
        local_sha256: None,
        size: None,
        executable: None,
        mtime_ns: Some(now),
        ctime_ns: Some(now),
        origin: None,
        link_target: None,
        local_only: None,
    }
}

/// The row a path keeps once its entry is gone: `present` 0 with its
/// drive-version columns and a new `file_id` while the drive holds a file
/// there, otherwise none.
fn vacate(edit: &mut Edit<'_>, path: &str) {
    let row = edit.row(path).expect("a vacated path has a row").clone();
    if row.version_id.is_none() {
        edit.remove_row(path);
        return;
    }
    let file_id = edit.new_id();
    edit.set_row(FileRow {
        file_id,
        kind: "file".to_owned(),
        present: false,
        content: None,
        local_sha256: None,
        size: None,
        executable: None,
        mtime_ns: None,
        ctime_ns: None,
        origin: None,
        link_target: None,
        local_only: None,
        ..row
    });
}

/// What an operation changes besides its rows, done once they commit.
#[derive(Default)]
struct After {
    remove_files: Vec<PathBuf>,
    notices: Vec<(Option<String>, Notice)>,
}

impl Owner {
    pub(crate) fn machine_path(&self, drive: &str, path: &str) -> PathBuf {
        let top = self.session_dir.join("drives").join(drive);
        if path.is_empty() { top } else { top.join(path) }
    }

    pub(crate) fn content_path(&self, drive: &str, file_id: i64) -> PathBuf {
        self.session_dir
            .join("content")
            .join(drive)
            .join(file_id.to_string())
    }

    /// The path of the present entry `ino` of `drive`.
    fn path_of(&self, drive: &str, ino: u64) -> Result<String, Errno> {
        let file_id = i64::try_from(ino).map_err(|_| Errno::NOENT)?;
        let path = self
            .inodes
            .get(&(drive.to_owned(), file_id))
            .ok_or(Errno::NOENT)?;
        let row = self
            .state
            .tables()
            .files
            .get(&(drive.to_owned(), path.clone()))
            .ok_or(Errno::NOENT)?;
        if !row.present || row.file_id != file_id {
            return Err(Errno::NOENT);
        }
        Ok(path.clone())
    }

    fn row(&self, drive: &str, path: &str) -> Option<&FileRow> {
        self.state
            .tables()
            .files
            .get(&(drive.to_owned(), path.to_owned()))
            .filter(|row| row.present)
    }

    /// The path of `name` in the folder `parent`, in NFC.
    fn child(&self, drive: &str, parent: u64, name: &[u8]) -> Result<String, Errno> {
        let folder = self.path_of(drive, parent)?;
        if self
            .row(drive, &folder)
            .is_some_and(|row| row.kind != "folder")
        {
            return Err(Errno::NOTDIR);
        }
        Ok(paths::join(&folder, &paths::nfc_name(name)?))
    }

    fn exclude_of(&self, drive: &str) -> &[String] {
        self.mounts
            .get(drive)
            .map_or(&[], |mount| mount.exclude.as_slice())
    }

    /// The path checks, then the exclusion checks, on a path a process
    /// creates or renames to.
    fn target(&self, drive: &str, parent: u64, name: &[u8]) -> Result<String, Errno> {
        let path = self.child(drive, parent, name)?;
        paths::check(&path)?;
        if let Some(errno) = paths::exclusion_error(&path, self.exclude_of(drive)) {
            return Err(errno);
        }
        Ok(path)
    }

    pub(crate) fn waiting_reads(&self) -> Vec<(PathBuf, String, u64)> {
        self.reads
            .values()
            .map(|(drive, path, sha256, size)| {
                (self.machine_path(drive, path), sha256.clone(), *size)
            })
            .collect()
    }

    pub(crate) async fn op(&mut self, drive: String, op: Op, reply: Reply) {
        let listed = self.state.tables().drive_cursor.contains_key(&drive);
        let missing = matches!(&op, Op::Read(ino) if self.blob_missing(&drive, *ino));
        let Some(mount) = self.mounts.get_mut(&drive) else {
            let _ = reply.send(Err(Errno::IO));
            return;
        };
        if !listed {
            mount.until_listed.push((op, reply));
            return;
        }
        if missing && mount.catching_up() {
            mount.until_caught_up.push((op, reply));
            return;
        }
        let result = self.answer(&drive, op).await;
        let _ = reply.send(result);
    }

    fn blob_missing(&self, drive: &str, ino: u64) -> bool {
        self.path_of(drive, ino)
            .ok()
            .and_then(|path| self.row(drive, &path).cloned())
            .is_some_and(|row| {
                row.content.as_deref() == Some("cache")
                    && row
                        .local_sha256
                        .as_ref()
                        .is_some_and(|sha256| !self.blobs.join(sha256).exists())
            })
    }

    async fn answer(&mut self, drive: &str, op: Op) -> Result<Done, Errno> {
        match op {
            Op::Lookup { parent, name } => {
                let Ok(path) = self.child(drive, parent, &name) else {
                    return Err(Errno::NOENT);
                };
                if paths::excluded(&path, self.exclude_of(drive)) {
                    return Err(Errno::NOENT);
                }
                let row = self.row(drive, &path).ok_or(Errno::NOENT)?;
                Ok(Done::Attr(attr_of(row)))
            }
            Op::GetAttr(ino) => {
                let path = self.path_of(drive, ino)?;
                Ok(Done::Attr(attr_of(
                    self.row(drive, &path).ok_or(Errno::NOENT)?,
                )))
            }
            Op::ReadDir(ino) => self.readdir(drive, ino),
            Op::Read(ino) => self.read(drive, ino),
            Op::ReadLink(ino) => {
                let path = self.path_of(drive, ino)?;
                let row = self.row(drive, &path).ok_or(Errno::NOENT)?;
                match (row.kind.as_str(), &row.link_target) {
                    ("symlink", Some(target)) => Ok(Done::Link(target.clone().into_bytes())),
                    _ => Err(Errno::INVAL),
                }
            }
            Op::Sync(ino) => {
                let path = self.path_of(drive, ino)?;
                let row = self.row(drive, &path).ok_or(Errno::NOENT)?;
                if row.content.as_deref() == Some("session") {
                    let content = self.content_path(drive, row.file_id);
                    File::open(content)
                        .and_then(|file| file.sync_all())
                        .map_err(|error| errno(&error))?;
                }
                Ok(Done::Unit)
            }
            Op::Create {
                parent,
                name,
                mode,
                task,
            } => self.create(drive, parent, &name, mode, task).await,
            Op::Write {
                ino,
                offset,
                data,
                blob,
                task,
            } => {
                self.write_bytes(drive, ino, offset, &data, blob, task)
                    .await
            }
            Op::SetAttr {
                ino,
                set,
                blob,
                task,
            } => self.setattr(drive, ino, set, blob, task).await,
            Op::MakeFolder { parent, name, task } => {
                let path = self.target(drive, parent, &name)?;
                self.make(drive, &path, "folder", None, task).await
            }
            Op::Symlink {
                parent,
                name,
                target,
                task,
            } => {
                let path = self.target(drive, parent, &name)?;
                let target = String::from_utf8(target).map_err(|_| Errno::INVAL)?;
                self.make(drive, &path, "symlink", Some(target), task).await
            }
            Op::MakeSpecial {
                parent,
                name,
                special,
                task,
            } => {
                let path = self.target(drive, parent, &name)?;
                self.make(drive, &path, "special", Some(special.stored()), task)
                    .await
            }
            Op::Remove { parent, name, task } => self.remove(drive, parent, &name, task).await,
            Op::RemoveFolder { parent, name, task } => {
                self.remove_folder(drive, parent, &name, task).await
            }
            Op::Rename {
                parent,
                name,
                new_parent,
                new_name,
                task,
            } => {
                self.rename(drive, parent, &name, new_parent, &new_name, task)
                    .await
            }
        }
    }

    fn readdir(&self, drive: &str, ino: u64) -> Result<Done, Errno> {
        let folder = self.path_of(drive, ino)?;
        let row = self.row(drive, &folder).ok_or(Errno::NOENT)?;
        if row.kind != "folder" {
            return Err(Errno::NOTDIR);
        }
        let prefix = paths::join(&folder, "");
        let entries = self
            .state
            .tables()
            .files
            .range((drive.to_owned(), prefix.clone())..)
            .take_while(|((each, path), _)| each == drive && path.starts_with(&prefix))
            .filter(|((_, path), row)| {
                row.present && !path.is_empty() && !path[prefix.len()..].contains('/')
            })
            .map(|((_, path), row)| DirEntry {
                name: path[prefix.len()..].to_owned(),
                attr: attr_of(row),
            })
            .collect();
        Ok(Done::Entries(entries))
    }

    fn read(&mut self, drive: &str, ino: u64) -> Result<Done, Errno> {
        let path = self.path_of(drive, ino)?;
        let row = self.row(drive, &path).ok_or(Errno::NOENT)?;
        match (row.kind.as_str(), row.content.as_deref()) {
            ("folder", _) => Err(Errno::ISDIR),
            ("file", Some("session")) => Ok(Done::Source(Source::Session(
                self.content_path(drive, row.file_id),
            ))),
            ("file", Some("cache")) => {
                let sha256 = row.local_sha256.clone().ok_or(Errno::IO)?;
                let size = row.size.unwrap_or(0).max(0) as u64;
                let needed = self.needed(drive, &sha256, size)?;
                self.read_tokens += 1;
                let token = self.read_tokens;
                self.reads
                    .insert(token, (drive.to_owned(), path, sha256, size));
                Ok(Done::Source(Source::Cache { needed, token }))
            }
            _ => Err(Errno::INVAL),
        }
    }

    fn needed(&self, drive: &str, sha256: &str, size: u64) -> Result<Needed, Errno> {
        Ok(Needed {
            session_id: self.session_id.clone(),
            drive_id: Id::new(drive).map_err(|_| Errno::IO)?,
            sha256: hash_of(sha256)?,
            size,
            binding: self.binding.subscribe(),
        })
    }

    /// Commits the edit's rows, then carries out what follows them.
    async fn commit(&mut self, edit: Write, after: After) -> Result<(), Errno> {
        let changed: Vec<Key> = edit
            .pending
            .iter()
            .map(|change| (change.drive_id.clone(), change.path.clone()))
            .chain(edit.pending_removed.iter().cloned())
            .collect();
        self.write(edit).await.map_err(|_| Errno::IO)?;
        for key in &changed {
            self.local_change(key);
        }
        if !changed.is_empty() {
            // A covered write the operation settled leaves nothing to wait
            // for.
            self.sync_points_progress().await;
            self.changes_ready().await;
        }
        for path in after.remove_files {
            let _ = fs::remove_file(path);
        }
        for (task, notice) in after.notices {
            let Some(task) = task else { continue };
            let kept = self.notices.entry(task).or_default();
            if !kept.contains(&notice) {
                kept.push(notice);
            }
        }
        Ok(())
    }

    fn git_notice(&self, drive: &str, path: &str, task: &Option<String>, after: &mut After) {
        if path.rsplit('/').next() == Some(".git") {
            let folder = paths::parent(path).unwrap_or("");
            after.notices.push((
                task.clone(),
                Notice {
                    path: self.machine_path(drive, folder),
                    reason: "git_repository",
                },
            ));
        }
    }

    /// Makes a folder, a symbolic link or a special file at `path`.
    async fn make(
        &mut self,
        drive: &str,
        path: &str,
        kind: &str,
        link_target: Option<String>,
        task: Option<String>,
    ) -> Result<Done, Errno> {
        let mut after = After::default();
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        if edit.entry(path).is_some() {
            return Err(Errno::EXIST);
        }
        let mut row = new_row(&mut edit, path, kind);
        if kind == "folder" {
            row.origin = Some("local".to_owned());
        }
        row.link_target = link_target;
        row.local_only = edit.local_only(path, kind).map(str::to_owned);
        let attr = attr_of(&row);
        edit.set_row(row);
        let folder = paths::parent(path).unwrap_or("");
        edit.touch(folder);
        match kind {
            "symlink" | "special" => after.notices.push((
                task.clone(),
                Notice {
                    path: self.machine_path(drive, path),
                    reason: if kind == "symlink" {
                        "symlink"
                    } else {
                        "special_file"
                    },
                },
            )),
            _ => {}
        }
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder);
            self.git_notice(drive, path, &task, &mut after);
        }
        let write = edit.into_write();
        self.commit(write, after).await?;
        Ok(Done::Attr(attr))
    }

    async fn create(
        &mut self,
        drive: &str,
        parent: u64,
        name: &[u8],
        mode: u32,
        task: Option<String>,
    ) -> Result<Done, Errno> {
        let path = self.target(drive, parent, name)?;
        let mut after = After::default();
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        if edit.entry(&path).is_some() {
            return Err(Errno::EXIST);
        }
        let mut row = new_row(&mut edit, &path, "file");
        row.content = Some("session".to_owned());
        row.size = Some(0);
        row.executable = Some(mode & 0o111 != 0);
        row.local_only = edit.local_only(&path, "file").map(str::to_owned);
        let content = self.content_path(drive, row.file_id);
        let attr = attr_of(&row);
        let local_only = row.local_only.is_some();
        edit.set_row(row);
        let folder = paths::parent(&path).unwrap_or("");
        edit.touch(folder);
        if !local_only {
            edit.record_write(&path, Written::Bytes, task.as_deref());
        }
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder);
            self.git_notice(drive, &path, &task, &mut after);
        }
        let write = edit.into_write();
        create_content(&content, None)?;
        if let Err(error) = self.commit(write, after).await {
            let _ = fs::remove_file(&content);
            return Err(error);
        }
        Ok(Done::Attr(attr))
    }

    /// Turns a `cache` file into a `session` one: the content file is a
    /// copy of `blob`, or empty when `empty`. `None` asks for the blob.
    fn to_session(
        &self,
        drive: &str,
        row: &mut FileRow,
        blob: Option<(String, File)>,
        empty: bool,
    ) -> Result<Option<Done>, Errno> {
        if row.content.as_deref() != Some("cache") {
            return Ok(None);
        }
        let sha256 = row.local_sha256.clone().ok_or(Errno::IO)?;
        let content = self.content_path(drive, row.file_id);
        if empty || row.size == Some(0) {
            create_content(&content, None)?;
        } else {
            match blob {
                Some((held, mut file)) if held == sha256 => {
                    create_content(&content, Some(&mut file))?;
                }
                _ => {
                    let size = row.size.unwrap_or(0).max(0) as u64;
                    return Ok(Some(Done::NeedBlob(
                        sha256.clone(),
                        self.needed(drive, &sha256, size)?,
                    )));
                }
            }
        }
        row.content = Some("session".to_owned());
        row.local_sha256 = None;
        Ok(None)
    }

    fn written_file(&self, drive: &str, ino: u64) -> Result<(String, FileRow), Errno> {
        let path = self.path_of(drive, ino)?;
        let row = self.row(drive, &path).ok_or(Errno::NOENT)?.clone();
        match row.kind.as_str() {
            "file" => {}
            "folder" => return Err(Errno::ISDIR),
            _ => return Err(Errno::INVAL),
        }
        if let Some(errno) = paths::exclusion_error(&path, self.exclude_of(drive)) {
            return Err(errno);
        }
        Ok((path, row))
    }

    async fn write_bytes(
        &mut self,
        drive: &str,
        ino: u64,
        offset: u64,
        data: &[u8],
        blob: Option<(String, File)>,
        task: Option<String>,
    ) -> Result<Done, Errno> {
        let (path, mut row) = self.written_file(drive, ino)?;
        if let Some(done) = self.to_session(drive, &mut row, blob, false)? {
            return Ok(done);
        }
        let content = self.content_path(drive, row.file_id);
        let file = OpenOptions::new()
            .write(true)
            .open(&content)
            .map_err(|error| errno(&error))?;
        file.write_all_at(data, offset)
            .map_err(|error| errno(&error))?;
        let end = i64::try_from(offset + data.len() as u64).map_err(|_| Errno::FBIG)?;
        row.size = Some(row.size.unwrap_or(0).max(end));
        self.changed(drive, &path, row, Written::Bytes, task)
            .await?;
        Ok(Done::Written(data.len() as u32))
    }

    /// Commits the changed file `row` and records its write unless the
    /// path is local-only.
    async fn changed(
        &mut self,
        drive: &str,
        path: &str,
        mut row: FileRow,
        written: Written<'_>,
        task: Option<String>,
    ) -> Result<(), Errno> {
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        if written == Written::Bytes {
            row.mtime_ns = Some(edit.now_ns);
            row.ctime_ns = Some(edit.now_ns);
        }
        // A change recorded before the path became local-only keeps
        // recording until a sync point settles it.
        let records = row.local_only.is_none() || edit.pending(path).is_some();
        edit.set_row(row);
        if records {
            edit.record_write(path, written, task.as_deref());
        }
        let write = edit.into_write();
        self.commit(write, After::default()).await
    }

    async fn setattr(
        &mut self,
        drive: &str,
        ino: u64,
        set: SetAttr,
        blob: Option<(String, File)>,
        task: Option<String>,
    ) -> Result<Done, Errno> {
        let path = self.path_of(drive, ino)?;
        let mut row = self.row(drive, &path).ok_or(Errno::NOENT)?.clone();
        if let Some(mtime_ns) = set.mtime_ns {
            row.mtime_ns = Some(mtime_ns);
        }
        let mut written = None;
        if let Some(size) = set.size {
            let (_, mut file) = self.written_file(drive, ino)?;
            if let Some(done) = self.to_session(drive, &mut file, blob, size == 0)? {
                return Ok(done);
            }
            let content = self.content_path(drive, file.file_id);
            OpenOptions::new()
                .write(true)
                .open(&content)
                .and_then(|handle| handle.set_len(size))
                .map_err(|error| errno(&error))?;
            row.content = file.content;
            row.local_sha256 = None;
            row.size = Some(i64::try_from(size).map_err(|_| Errno::FBIG)?);
            written = Some(Written::Bytes);
        }
        if let Some(mode) = set.mode
            && row.kind == "file"
        {
            let executable = mode & 0o111 != 0;
            if row.executable != Some(executable) {
                row.executable = Some(executable);
                written = written.or(Some(Written::Mode));
            }
        }
        let attr = match written {
            Some(written) => {
                let path = path.clone();
                self.changed(drive, &path, row, written, task).await?;
                attr_of(self.row(drive, &path).ok_or(Errno::IO)?)
            }
            None => {
                let tables = self.state.tables();
                let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
                let attr = attr_of(&row);
                edit.set_row(row);
                let write = edit.into_write();
                self.commit(write, After::default()).await?;
                attr
            }
        };
        Ok(Done::Attr(attr))
    }

    async fn remove(
        &mut self,
        drive: &str,
        parent: u64,
        name: &[u8],
        task: Option<String>,
    ) -> Result<Done, Errno> {
        let path = self.child(drive, parent, name)?;
        let row = self.row(drive, &path).ok_or(Errno::NOENT)?.clone();
        if row.kind == "folder" {
            return Err(Errno::ISDIR);
        }
        let mut after = After::default();
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        let synced = row.kind == "file"
            && (edit.local_only(&path, "file").is_none() || edit.pending(&path).is_some());
        vacate(&mut edit, &path);
        if synced {
            edit.record_delete(&path, task.as_deref());
        }
        let folder = paths::parent(&path).unwrap_or("");
        edit.touch(folder);
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder);
        }
        if row.content.as_deref() == Some("session") {
            after
                .remove_files
                .push(self.content_path(drive, row.file_id));
        }
        let write = edit.into_write();
        self.commit(write, after).await?;
        Ok(Done::Unit)
    }

    async fn remove_folder(
        &mut self,
        drive: &str,
        parent: u64,
        name: &[u8],
        _task: Option<String>,
    ) -> Result<Done, Errno> {
        let path = self.child(drive, parent, name)?;
        let row = self.row(drive, &path).ok_or(Errno::NOENT)?;
        if row.kind != "folder" {
            return Err(Errno::NOTDIR);
        }
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        if edit.holds_entry(&path) {
            return Err(Errno::NOTEMPTY);
        }
        edit.remove_row(&path);
        let folder = paths::parent(&path).unwrap_or("");
        edit.touch(folder);
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder);
        }
        let write = edit.into_write();
        self.commit(write, After::default()).await?;
        Ok(Done::Unit)
    }

    async fn rename(
        &mut self,
        drive: &str,
        parent: u64,
        name: &[u8],
        new_parent: u64,
        new_name: &[u8],
        task: Option<String>,
    ) -> Result<Done, Errno> {
        let source = self.child(drive, parent, name)?;
        let source_row = self.row(drive, &source).ok_or(Errno::NOENT)?.clone();
        let target = self.target(drive, new_parent, new_name)?;
        if source == target {
            return Ok(Done::Unit);
        }
        let is_folder = source_row.kind == "folder";
        if is_folder && paths::at_or_below(&target, &source) {
            return Err(Errno::INVAL);
        }
        let mut after = After::default();
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        if let Some(existing) = edit.entry(&target).cloned() {
            match (existing.kind == "folder", is_folder) {
                (true, false) => return Err(Errno::ISDIR),
                (false, true) => return Err(Errno::NOTDIR),
                (true, true) if edit.holds_entry(&target) => return Err(Errno::NOTEMPTY),
                (true, true) => edit.remove_row(&target),
                (false, false) => {
                    if existing.content.as_deref() == Some("session") {
                        after
                            .remove_files
                            .push(self.content_path(drive, existing.file_id));
                    }
                }
            }
        }
        let moved: Vec<String> = edit
            .paths_at_or_below(&source)
            .into_iter()
            .filter(|path| edit.entry(path).is_some())
            .collect();
        // A moved folder's entry may land at an excluded path below the
        // target.
        let exclude = self.exclude_of(drive);
        if moved
            .iter()
            .any(|from| paths::excluded(&format!("{target}{}", &from[source.len()..]), exclude))
        {
            return Err(Errno::ACCESS);
        }
        let mut files = Vec::new();
        for from in &moved {
            let to = format!("{target}{}", &from[source.len()..]);
            let row = edit.entry(from).expect("a moved entry").clone();
            let synced_from = row.kind == "file"
                && (edit.local_only(from, "file").is_none() || edit.pending(from).is_some());
            let there = edit.row(&to).cloned();
            vacate(&mut edit, from);
            edit.set_row(FileRow {
                path: to.clone(),
                version_id: there.as_ref().and_then(|row| row.version_id.clone()),
                synced_sha256: there.as_ref().and_then(|row| row.synced_sha256.clone()),
                synced_size: there.as_ref().and_then(|row| row.synced_size),
                synced_executable: there.as_ref().and_then(|row| row.synced_executable),
                ..row.clone()
            });
            if row.kind == "file" {
                let cache = (row.content.as_deref() == Some("cache"))
                    .then(|| row.local_sha256.clone())
                    .flatten();
                files.push((from.clone(), synced_from, to, cache));
            }
        }
        let source_folder = paths::parent(&source).unwrap_or("").to_owned();
        let target_folder = paths::parent(&target).unwrap_or("").to_owned();
        edit.refresh_local_only(&target);
        for (name, folder) in [(&source, &source_folder), (&target, &target_folder)] {
            if name.rsplit('/').next() == Some(".git") {
                edit.refresh_local_only(folder);
            }
        }
        for (from, synced_from, to, cache) in &files {
            if *synced_from {
                edit.record_delete(from, task.as_deref());
            }
            if edit.local_only(to, "file").is_none() || edit.pending(to).is_some() {
                edit.record_write(to, Written::Moved(cache.as_deref()), task.as_deref());
            }
        }
        edit.touch(&source_folder);
        edit.touch(&target_folder);
        self.git_notice(drive, &target, &task, &mut after);
        let write = edit.into_write();
        self.commit(write, after).await?;
        Ok(Done::Unit)
    }
}

/// Creates the content file at `path`, empty or holding a copy of `blob`.
fn create_content(path: &std::path::Path, blob: Option<&mut File>) -> Result<(), Errno> {
    let folder = path.parent().expect("a content file has a folder");
    state::files::create_private_dirs(folder).map_err(|error| errno(&error.source))?;
    let mut file = File::create(path).map_err(|error| errno(&error))?;
    if let Some(blob) = blob {
        io::copy(blob, &mut file).map_err(|error| errno(&error))?;
    }
    Ok(())
}
