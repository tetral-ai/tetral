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
use std::io::{self, Seek};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    pub nlink: u32,
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

/// A local-only entry for its task's next sync point, or the next Session
/// sync point when the operation had no task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// The machine path of the link, the special file, or the folder
    /// holding `.git`.
    pub path: PathBuf,
    /// `symlink`, `special_file` or `git_repository`.
    pub reason: &'static str,
}

pub(crate) enum Op {
    Mounted {
        detached: Arc<AtomicBool>,
        op: Box<Op>,
    },
    Open(u64),
    Release(u64),
    ReadOpened {
        ino: u64,
        hash: String,
        file: Arc<File>,
    },
    Lookup {
        parent: u64,
        name: Vec<u8>,
    },
    GetAttr(u64),
    ReadDir(u64),
    Read(u64),
    ReadLink(u64),
    Create {
        open: bool,
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
    Retry,
    Unit,
}

/// Where a read's bytes are.
pub(crate) enum Source {
    File(Arc<File>),
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

impl Op {
    fn requested(&self) -> &Op {
        match self {
            Op::Mounted { op, .. } => op.requested(),
            op => op,
        }
    }

    fn admitted(&self) -> bool {
        match self {
            Op::Mounted { detached, op } => !detached.load(Ordering::Acquire) && op.admitted(),
            _ => true,
        }
    }
}

impl DriveSync {
    async fn op(&self, drive_id: &Id, op: Op) -> Result<Done, Errno> {
        let op = [&self.write_guard, &self.mount_guard]
            .into_iter()
            .flatten()
            .fold(op, |op, detached| Op::Mounted {
                detached: detached.clone(),
                op: Box::new(op),
            });
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

    /// Opens an inode independently of its namespace lifetime.
    pub async fn open_file(&self, drive_id: &Id, ino: u64) -> Result<(), Errno> {
        self.op(drive_id, Op::Open(ino)).await.map(|_| ())
    }

    /// Releases one open reference; the last release drops its descriptor.
    pub async fn release_file(&self, drive_id: &Id, ino: u64) -> Result<(), Errno> {
        self.op(drive_id, Op::Release(ino)).await.map(|_| ())
    }

    /// Reads the inode's current content. Missing content waits outside the
    /// owner; binding loss and permanent download failures return EIO.
    pub async fn read(
        &self,
        drive_id: &Id,
        ino: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, Errno> {
        loop {
            let source = match self.op(drive_id, Op::Read(ino)).await? {
                Done::Source(source) => source,
                _ => return Err(Errno::IO),
            };
            let file = match source {
                Source::File(file) => file,
                Source::Cache { needed, token } => {
                    let hash = needed.sha256.as_str().to_owned();
                    let file = self.cache.read(needed).await;
                    self.send(Message::ReadEnded(token));
                    let file = Arc::new(file.map_err(|_| Errno::IO)?);
                    match self
                        .op(
                            drive_id,
                            Op::ReadOpened {
                                ino,
                                hash,
                                file: file.clone(),
                            },
                        )
                        .await?
                    {
                        Done::Unit => file,
                        _ => continue,
                    }
                }
            };
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
            return Ok(bytes);
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
            open: false,
            parent,
            name: name.to_vec(),
            mode,
            task,
        };
        attr(self.op(drive_id, op).await?)
    }

    /// Creates and opens a file in one owner operation, before another
    /// namespace operation can unlink or replace it.
    pub async fn create_open(
        &self,
        drive_id: &Id,
        parent: u64,
        name: &[u8],
        mode: u32,
        pid: Option<u32>,
    ) -> Result<Attr, Errno> {
        let task = self.task(pid).await;
        attr(
            self.op(
                drive_id,
                Op::Create {
                    open: true,
                    parent,
                    name: name.to_vec(),
                    mode,
                    task,
                },
            )
            .await?,
        )
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
            .await
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
        nlink: if kind == Kind::Folder { 2 } else { 1 },
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
    /// The `session` content files to remove, by drive and file ID.
    remove_contents: Vec<(String, i64)>,
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

    /// Removes a `session` content file and forgets its write generations.
    pub(crate) fn remove_content(&mut self, drive: &str, file_id: i64) {
        let _ = fs::remove_file(self.content_path(drive, file_id));
        self.content_syncs.forget(drive, file_id);
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

    /// The top folder's attributes; before the first listing writes its
    /// row, those of an empty folder with no times.
    #[cfg(target_os = "macos")]
    pub(crate) fn top_attr(&self, drive: &str) -> Attr {
        self.row(drive, "").map_or_else(
            || Attr {
                ino: TOP_INODE,
                nlink: 2,
                kind: Kind::Folder,
                size: 0,
                mode: 0o755,
                mtime_ns: 0,
                ctime_ns: 0,
                uid: rustix::process::getuid().as_raw(),
                gid: rustix::process::getgid().as_raw(),
            },
            attr_of,
        )
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
        if !op.admitted() {
            let _ = reply.send(Err(Errno::IO));
            return;
        }
        let listed = self.state.tables().drive_cursor.contains_key(&drive);
        let missing = match op.requested() {
            Op::Read(ino)
            | Op::Write {
                ino, blob: None, ..
            } => self.blob_missing(&drive, *ino),
            Op::SetAttr {
                ino,
                set,
                blob: None,
                ..
            } if set.size.is_some_and(|size| size > 0) => self.blob_missing(&drive, *ino),
            _ => false,
        };
        let Some(mount) = self.mounts.get_mut(&drive) else {
            let _ = reply.send(Err(Errno::IO));
            return;
        };
        if (!listed || missing && mount.catching_up()) && self.binding.borrow().is_none() {
            let _ = reply.send(Err(Errno::IO));
            return;
        }
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
        if self.open_descriptor(drive, ino).is_some() {
            return false;
        }
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

    async fn answer(&mut self, drive: &str, mut op: Op) -> Result<Done, Errno> {
        while let Op::Mounted {
            detached,
            op: inner,
        } = op
        {
            if detached.load(Ordering::Acquire) {
                return Err(Errno::IO);
            }
            op = *inner;
        }
        match op {
            Op::Mounted { .. } => unreachable!("mount guards were checked"),
            Op::Open(ino) => {
                self.open_file(drive, ino)?;
                Ok(Done::Unit)
            }
            Op::Release(ino) => {
                self.release_file(drive, ino);
                Ok(Done::Unit)
            }
            Op::ReadOpened { ino, hash, file } => {
                Ok(if self.read_opened(drive, ino, &hash, file) {
                    Done::Unit
                } else {
                    Done::Retry
                })
            }
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
                let mut attr = attr_of(self.file_row(drive, ino)?);
                if self.unlinked(drive, ino) {
                    attr.nlink = 0;
                }
                Ok(Done::Attr(attr))
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
                let row = self.file_row(drive, ino)?;
                // Cache blobs are immutable and synced before publication.
                // FLUSH on every close must not fsync their last-read mtime.
                if row.content.as_deref() != Some("session") {
                    return Ok(Done::Unit);
                }
                let file_id = row.file_id;
                // A file reached only through an unlinked descriptor is
                // synced every time; its syncs are not recorded.
                let unlinked = self.unlinked(drive, ino);
                let generation = self.content_syncs.unsynced(drive, file_id);
                if generation.is_none() && !unlinked {
                    return Ok(Done::Unit);
                }
                let file = match self.open_descriptor(drive, ino) {
                    Some(file) => file,
                    None => Arc::new(
                        File::open(self.content_path(drive, file_id))
                            .map_err(|error| errno(&error))?,
                    ),
                };
                (self.syncer)(&file).map_err(|error| errno(&error))?;
                state::seams::count("drive.content_synced");
                if let Some(generation) = generation
                    && !unlinked
                {
                    self.content_syncs.synced(drive, file_id, generation);
                }
                Ok(Done::Unit)
            }
            Op::Create {
                open,
                parent,
                name,
                mode,
                task,
            } => {
                let made = self.create(drive, parent, &name, mode, task).await?;
                if open && let Done::Attr(attr) = &made {
                    self.open_file(drive, attr.ino)?;
                }
                Ok(made)
            }
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
        let row = self.file_row(drive, ino)?;
        let path = row.path.clone();
        if let Some(file) = self.open_descriptor(drive, ino) {
            if row.content.as_deref() == Some("cache")
                && let Some(hash) = &row.local_sha256
            {
                self.cache.accessed(hash_of(hash)?);
            }
            return Ok(Done::Source(Source::File(file)));
        }
        match (row.kind.as_str(), row.content.as_deref()) {
            ("folder", _) => Err(Errno::ISDIR),
            ("file", Some("session")) => Ok(Done::Source(Source::File(Arc::new(
                File::open(self.content_path(drive, row.file_id)).map_err(|error| errno(&error))?,
            )))),
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
        // Keep notices before progress can complete a covered result. A
        // failed transaction publishes none.
        for (task, notice) in after.notices {
            let kept = match task {
                Some(task) => self.notices.entry(task).or_default(),
                None => &mut self.taskless_notices,
            };
            if !kept.contains(&notice) {
                kept.push(notice);
            }
        }
        for key in &changed {
            self.local_change(key);
        }
        if !changed.is_empty() {
            // A covered write the operation settled leaves nothing to wait
            // for.
            self.sync_points_progress().await;
            self.changes_ready().await;
        }
        for (drive, file_id) in after.remove_contents {
            self.remove_content(&drive, file_id);
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
        // A link or special file is listed only where a regular file would
        // sync, so nothing inside an ignored folder or a Git repository.
        if matches!(kind, "symlink" | "special") && edit.local_only(path, "file").is_none() {
            after.notices.push((
                task.clone(),
                Notice {
                    path: self.machine_path(drive, path),
                    reason: if kind == "symlink" {
                        "symlink"
                    } else {
                        "special_file"
                    },
                },
            ));
        }
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder, task.as_deref(), self.exclude_of(drive));
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
        edit.set_row(row);
        let folder = paths::parent(&path).unwrap_or("");
        edit.touch(folder);
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder, task.as_deref(), self.exclude_of(drive));
            self.git_notice(drive, &path, &task, &mut after);
        }
        if edit.records(&path) {
            edit.record_write(&path, Written::Bytes, task.as_deref());
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
    fn materialize_session(
        &mut self,
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
            let blob = blob.or_else(|| {
                self.open_descriptor(drive, row.file_id as u64)
                    .and_then(|file| file.try_clone().ok())
                    .map(|file| (sha256.clone(), file))
            });
            match blob {
                Some((held, mut file)) if held == sha256 => {
                    file.rewind().map_err(|error| errno(&error))?;
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
        if self.unlinked(drive, row.file_id as u64) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&content)
                .map_err(|error| errno(&error))?;
            fs::remove_file(&content).map_err(|error| errno(&error))?;
            self.open_files
                .get_mut(&(drive.to_owned(), row.file_id))
                .ok_or(Errno::IO)?
                .file = Some(Arc::new(file));
        }
        Ok(None)
    }

    fn written_file(&self, drive: &str, ino: u64) -> Result<(String, FileRow), Errno> {
        let row = self.file_row(drive, ino)?.clone();
        let path = row.path.clone();
        match row.kind.as_str() {
            "file" => {}
            "folder" => return Err(Errno::ISDIR),
            _ => return Err(Errno::INVAL),
        }
        if !self.unlinked(drive, ino)
            && let Some(errno) = paths::exclusion_error(&path, self.exclude_of(drive))
        {
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
        self.content_syncs.wrote(drive, row.file_id);
        if let Some(done) = self.materialize_session(drive, &mut row, blob, false)? {
            return Ok(done);
        }
        let content = self.content_path(drive, row.file_id);
        let file = if self.unlinked(drive, ino) {
            self.open_descriptor(drive, ino).ok_or(Errno::IO)?
        } else {
            Arc::new(
                OpenOptions::new()
                    .write(true)
                    .open(&content)
                    .map_err(|error| errno(&error))?,
            )
        };
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
        if self.unlinked(drive, row.file_id as u64) {
            if written == Written::Bytes {
                row.mtime_ns = Some(now_ns());
                row.ctime_ns = row.mtime_ns;
            }
            let key = (drive.to_owned(), row.file_id);
            self.open_files.get_mut(&key).ok_or(Errno::IO)?.row = row;
            return Ok(());
        }
        let tables = self.state.tables();
        let mut edit = Edit::new(tables, drive, now_ns()).ok_or(Errno::IO)?;
        if written == Written::Bytes {
            row.mtime_ns = Some(edit.now_ns);
            row.ctime_ns = Some(edit.now_ns);
        }
        let records = edit.records(path);
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
        let mut row = self.file_row(drive, ino)?.clone();
        let path = row.path.clone();
        if let Some(mtime_ns) = set.mtime_ns {
            row.mtime_ns = Some(mtime_ns);
        }
        let mut written = None;
        if let Some(size) = set.size {
            let (_, mut file) = self.written_file(drive, ino)?;
            self.content_syncs.wrote(drive, file.file_id);
            if let Some(done) = self.materialize_session(drive, &mut file, blob, size == 0)? {
                return Ok(done);
            }
            let content = self.content_path(drive, file.file_id);
            if self.unlinked(drive, ino) {
                self.open_descriptor(drive, ino)
                    .ok_or(Errno::IO)?
                    .set_len(size)
                    .map_err(|error| errno(&error))?;
            } else {
                OpenOptions::new()
                    .write(true)
                    .open(&content)
                    .and_then(|handle| handle.set_len(size))
                    .map_err(|error| errno(&error))?;
            }
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
        let mut attr = match written {
            Some(written) => {
                let path = path.clone();
                self.changed(drive, &path, row, written, task).await?;
                attr_of(self.file_row(drive, ino)?)
            }
            None if self.unlinked(drive, ino) => {
                let attr = attr_of(&row);
                let key = (drive.to_owned(), row.file_id);
                self.open_files.get_mut(&key).ok_or(Errno::IO)?.row = row;
                attr
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
        if self.unlinked(drive, ino) {
            attr.nlink = 0;
        }
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
        let synced = row.kind == "file" && edit.records(&path);
        vacate(&mut edit, &path);
        if synced {
            edit.record_delete(&path, task.as_deref());
        }
        let folder = paths::parent(&path).unwrap_or("");
        edit.touch(folder);
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder, task.as_deref(), self.exclude_of(drive));
        }
        if row.content.as_deref() == Some("session") {
            after.remove_contents.push((drive.to_owned(), row.file_id));
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
        task: Option<String>,
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
        vacate(&mut edit, &path);
        let folder = paths::parent(&path).unwrap_or("");
        edit.touch(folder);
        if path.rsplit('/').next() == Some(".git") {
            edit.refresh_local_only(folder, task.as_deref(), self.exclude_of(drive));
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
        // Freeze classification in the original namespace: moving a .git
        // marker must not change whether a later source file was synced.
        // Validate every descendant before applying any namespace changes.
        let mut moved = Vec::new();
        for from in edit.paths_at_or_below(&source) {
            let Some(row) = edit.entry(&from).cloned() else {
                continue;
            };
            let to = format!("{target}{}", &from[source.len()..]);
            paths::check(&to)?;
            if paths::excluded(&to, self.exclude_of(drive)) {
                return Err(Errno::ACCESS);
            }
            let synced_from = row.kind == "file" && edit.records(&from);
            moved.push((from, to, row, synced_from));
        }
        if let Some(existing) = edit.entry(&target).cloned() {
            match (existing.kind == "folder", is_folder) {
                (true, false) => return Err(Errno::ISDIR),
                (false, true) => return Err(Errno::NOTDIR),
                (true, true) if edit.holds_entry(&target) => return Err(Errno::NOTEMPTY),
                (true, true) => edit.remove_row(&target),
                (false, false) => {
                    if existing.content.as_deref() == Some("session") {
                        after
                            .remove_contents
                            .push((drive.to_owned(), existing.file_id));
                    }
                }
            }
        }
        let mut files = Vec::new();
        for (from, to, row, synced_from) in moved {
            let there = edit.row(&to).cloned();
            vacate(&mut edit, &from);
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
        edit.refresh_local_only(&target, task.as_deref(), self.exclude_of(drive));
        for (name, folder) in [(&source, &source_folder), (&target, &target_folder)] {
            if name.rsplit('/').next() == Some(".git") {
                edit.refresh_local_only(folder, task.as_deref(), self.exclude_of(drive));
            }
        }
        for (from, synced_from, to, cache) in &files {
            if *synced_from {
                edit.record_delete(from, task.as_deref());
            }
            if edit.records(to) {
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
