//! The `fuser` adapter: one FUSE session per Session and drive, serving the
//! operation table ([`crate::ops`]) with inode numbers equal to `file_id`.
//! Each request runs as a task that replies when its operation completes,
//! so a read waiting for a download blocks no other request. Entries and
//! attributes are valid for 0 seconds and opens never keep cached pages;
//! the owner has an inode's pages invalidated, off its own task, when a
//! record replaces its content. Once detached, every request answers `EIO`.

use std::ffi::OsStr;
use std::future::Future;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner,
    OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use protocol::id::Id;
use tokio::runtime::Handle;

use crate::DriveSync;
use crate::ops::{Attr, Kind, SetAttr, Special};

const TTL: Duration = Duration::ZERO;

/// The FUSE filesystem of one drive of one Session.
pub struct Adapter {
    sync: DriveSync,
    drive_id: Id,
    runtime: Handle,
    detached: Arc<AtomicBool>,
}

impl Adapter {
    /// An adapter whose requests run on `runtime`; setting `detached` makes
    /// every later request answer `EIO`.
    pub fn new(
        sync: DriveSync,
        drive_id: Id,
        runtime: Handle,
        detached: Arc<AtomicBool>,
    ) -> Adapter {
        Adapter {
            sync,
            drive_id,
            runtime,
            detached,
        }
    }

    /// Runs `operation` for a request as a task, or answers `EIO` once
    /// detached.
    fn run<R, F>(&self, reply: R, operation: impl FnOnce(DriveSync, Id, R) -> F + Send + 'static)
    where
        R: Fails + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        if self.detached.load(Ordering::Acquire) {
            reply.fail(Errno::EIO);
            return;
        }
        let (sync, drive_id) = (self.sync.clone(), self.drive_id.clone());
        let detached = self.detached.clone();
        self.runtime.spawn(async move {
            if detached.load(Ordering::Acquire) {
                reply.fail(Errno::EIO);
            } else {
                operation(sync, drive_id, reply).await;
            }
        });
    }
}

/// A reply that can carry an error.
trait Fails {
    fn fail(self, error: Errno);
}

macro_rules! fails {
    ($($reply:ty),*) => {
        $(impl Fails for $reply {
            fn fail(self, error: Errno) {
                self.error(error);
            }
        })*
    };
}

fails!(
    ReplyAttr,
    ReplyCreate,
    ReplyData,
    ReplyDirectory,
    ReplyEmpty,
    ReplyEntry,
    ReplyWrite
);

fn errno(error: rustix::io::Errno) -> Errno {
    Errno::from_i32(error.raw_os_error())
}

fn time(ns: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns.max(0) as u64)
}

fn time_ns(time: TimeOrNow) -> i64 {
    let at = match time {
        TimeOrNow::SpecificTime(at) => at,
        TimeOrNow::Now => SystemTime::now(),
    };
    at.duration_since(UNIX_EPOCH).map_or(0, |since| {
        i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)
    })
}

fn file_attr(attr: &Attr) -> FileAttr {
    let (kind, rdev) = match attr.kind {
        Kind::File => (FileType::RegularFile, 0),
        Kind::Folder => (FileType::Directory, 0),
        Kind::Symlink => (FileType::Symlink, 0),
        Kind::Special(Special::Fifo) => (FileType::NamedPipe, 0),
        Kind::Special(Special::Socket) => (FileType::Socket, 0),
        Kind::Special(Special::CharDevice(rdev)) => (FileType::CharDevice, rdev),
        Kind::Special(Special::BlockDevice(rdev)) => (FileType::BlockDevice, rdev),
    };
    FileAttr {
        ino: INodeNo(attr.ino),
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: time(attr.mtime_ns),
        mtime: time(attr.mtime_ns),
        ctime: time(attr.ctime_ns),
        crtime: time(attr.ctime_ns),
        kind,
        perm: attr.mode as u16,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: attr.uid,
        gid: attr.gid,
        rdev: rdev as u32,
        blksize: 4_096,
        flags: 0,
    }
}

fn file_type(kind: Kind) -> FileType {
    file_attr(&Attr {
        ino: 0,
        kind,
        size: 0,
        mode: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        uid: 0,
        gid: 0,
    })
    .kind
}

fn entry(reply: ReplyEntry, result: Result<Attr, rustix::io::Errno>) {
    match result {
        Ok(attr) => reply.entry(&TTL, &file_attr(&attr), Generation(0)),
        Err(error) => reply.error(errno(error)),
    }
}

fn empty(reply: ReplyEmpty, result: Result<(), rustix::io::Errno>) {
    match result {
        Ok(()) => reply.ok(),
        Err(error) => reply.error(errno(error)),
    }
}

impl Filesystem for Adapter {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name = name.as_bytes().to_vec();
        self.run(reply, move |sync, drive, reply| async move {
            entry(reply, sync.lookup(&drive, parent.0, &name).await)
        });
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        self.run(reply, move |sync, drive, reply| async move {
            match sync.getattr(&drive, ino.0).await {
                Ok(attr) => reply.attr(&TTL, &file_attr(&attr)),
                Err(error) => reply.error(errno(error)),
            }
        });
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let pid = req.pid();
        let set = SetAttr {
            size,
            mode,
            mtime_ns: mtime.map(time_ns),
        };
        self.run(reply, move |sync, drive, reply| async move {
            match sync.setattr(&drive, ino.0, set, Some(pid)).await {
                Ok(attr) => reply.attr(&TTL, &file_attr(&attr)),
                Err(error) => reply.error(errno(error)),
            }
        });
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        self.run(reply, move |sync, drive, reply| async move {
            match sync.readlink(&drive, ino.0).await {
                Ok(target) => reply.data(&target),
                Err(error) => reply.error(errno(error)),
            }
        });
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let (pid, name) = (req.pid(), name.as_bytes().to_vec());
        let special = match rustix::fs::FileType::from_raw_mode(mode) {
            rustix::fs::FileType::RegularFile => None,
            rustix::fs::FileType::Fifo => Some(Special::Fifo),
            rustix::fs::FileType::Socket => Some(Special::Socket),
            rustix::fs::FileType::CharacterDevice => Some(Special::CharDevice(u64::from(rdev))),
            rustix::fs::FileType::BlockDevice => Some(Special::BlockDevice(u64::from(rdev))),
            _ => {
                reply.error(Errno::EINVAL);
                return;
            }
        };
        self.run(reply, move |sync, drive, reply| async move {
            let made = match special {
                Some(special) => {
                    sync.mknod(&drive, parent.0, &name, special, Some(pid))
                        .await
                }
                None => sync.create(&drive, parent.0, &name, mode, Some(pid)).await,
            };
            entry(reply, made);
        });
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let (pid, name) = (req.pid(), name.as_bytes().to_vec());
        self.run(reply, move |sync, drive, reply| async move {
            entry(reply, sync.mkdir(&drive, parent.0, &name, Some(pid)).await);
        });
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let (pid, name) = (req.pid(), name.as_bytes().to_vec());
        self.run(reply, move |sync, drive, reply| async move {
            empty(reply, sync.unlink(&drive, parent.0, &name, Some(pid)).await);
        });
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let (pid, name) = (req.pid(), name.as_bytes().to_vec());
        self.run(reply, move |sync, drive, reply| async move {
            empty(reply, sync.rmdir(&drive, parent.0, &name, Some(pid)).await);
        });
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let pid = req.pid();
        let name = link_name.as_bytes().to_vec();
        let target = target.as_os_str().as_bytes().to_vec();
        self.run(reply, move |sync, drive, reply| async move {
            entry(
                reply,
                sync.symlink(&drive, parent.0, &name, &target, Some(pid))
                    .await,
            );
        });
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if !flags.is_empty() {
            reply.error(Errno::EINVAL);
            return;
        }
        let pid = req.pid();
        let (name, newname) = (name.as_bytes().to_vec(), newname.as_bytes().to_vec());
        self.run(reply, move |sync, drive, reply| async move {
            let renamed = sync
                .rename(&drive, parent.0, &name, newparent.0, &newname, Some(pid))
                .await;
            empty(reply, renamed);
        });
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(if self.detached.load(Ordering::Acquire) {
            Errno::EIO
        } else {
            Errno::EPERM
        });
    }

    fn open(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        if self.detached.load(Ordering::Acquire) {
            reply.error(Errno::EIO);
            return;
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        self.run(reply, move |sync, drive, reply| async move {
            match sync.read(&drive, ino.0, offset, size).await {
                Ok(bytes) => reply.data(&bytes),
                Err(error) => reply.error(errno(error)),
            }
        });
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let (pid, data) = (req.pid(), data.to_vec());
        self.run(reply, move |sync, drive, reply| async move {
            match sync.write(&drive, ino.0, offset, &data, Some(pid)).await {
                Ok(count) => reply.written(count),
                Err(error) => reply.error(errno(error)),
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        self.run(reply, move |sync, drive, reply| async move {
            empty(reply, sync.fsync(&drive, ino.0).await)
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.run(reply, move |sync, drive, reply| async move {
            empty(reply, sync.fsync(&drive, ino.0).await)
        });
    }

    fn opendir(&self, _req: &Request, _ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        if self.detached.load(Ordering::Acquire) {
            reply.error(Errno::EIO);
            return;
        }
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        self.run(reply, move |sync, drive, mut reply| async move {
            let entries = match sync.readdir(&drive, ino.0).await {
                Ok(entries) => entries,
                Err(error) => return reply.error(errno(error)),
            };
            let dots = [
                (ino.0, FileType::Directory, b".".to_vec()),
                (ino.0, FileType::Directory, b"..".to_vec()),
            ];
            let all = dots.into_iter().chain(entries.into_iter().map(|each| {
                (
                    each.attr.ino,
                    file_type(each.attr.kind),
                    each.name.into_bytes(),
                )
            }));
            for (index, (ino, kind, name)) in all.enumerate().skip(offset as usize) {
                if reply.add(
                    INodeNo(ino),
                    index as u64 + 1,
                    kind,
                    OsStr::from_bytes(&name),
                ) {
                    break;
                }
            }
            reply.ok();
        });
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::from_i32(rustix::io::Errno::NOTSUP.raw_os_error()));
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::from_i32(rustix::io::Errno::NODATA.raw_os_error()));
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, size: u32, reply: ReplyXattr) {
        if size == 0 {
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::from_i32(rustix::io::Errno::NODATA.raw_os_error()));
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let (pid, name) = (req.pid(), name.as_bytes().to_vec());
        self.run(reply, move |sync, drive, reply| async move {
            match sync.create(&drive, parent.0, &name, mode, Some(pid)).await {
                Ok(attr) => reply.created(
                    &TTL,
                    &file_attr(&attr),
                    Generation(0),
                    FileHandle(0),
                    FopenFlags::empty(),
                ),
                Err(error) => reply.error(errno(error)),
            }
        });
    }
}
