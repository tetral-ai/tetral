//! The App Server's NFSv3 server on macOS: one `nfsserve` server on
//! `127.0.0.1` that serves the operation table ([`crate::ops`]) of every
//! mounted drive of every Session, and the test drive of
//! `tetral drives enable`.
//!
//! A drive mount's export is `/<port>/<session_id>/<drive_id>`. Its file
//! handles hold the `generation` of the drive's `drive_cursor` row and the
//! entry's `file_id`, so they stay valid across App Server restarts. One
//! server serves many drives whose `file_id`s overlap, so the id `nfsserve`
//! works with, which is also each entry's `fileid`, is the mount's slot
//! shifted left by [`FILE_ID_BITS`] plus the `file_id`.
//!
//! Each request's operation runs as a task on the App Server's runtime and
//! is answered when it completes, so a read waiting for a download blocks
//! no other request. A READ not completed within [`READ_WAIT`] is answered
//! `NFS3ERR_JUKEBOX` while its download goes on, and the client's retry
//! joins that download. Other requests retain their operation's wait and
//! error behavior; the bounded READ retry does not apply to mutations.
//!
//! Each mount keeps the WRITEs it has received and not yet answered. The
//! client sends some writes without waiting for their answer, so one can
//! still wait for its file's download after the writing process exited: a
//! flush waits for those received before it began ([`Server::received`]),
//! and a final upload in abandon mode refuses those of its drives
//! ([`Server::refuse_unanswered`]), which then answer `NFS3ERR_IO` and
//! change nothing.
//!
//! The server itself runs on its own thread: the Drive sync owner runs
//! `/sbin/mount`, which waits for GETATTR of the top folder, and the server
//! answers that from the attributes the owner took before the command, so
//! neither waits for the other.
//!
//! On the export of a `read_only` binding every request that would change
//! the drive is answered `NFS3ERR_ROFS`, whatever the mount's options, so a
//! second mount of the export cannot write it. LINK never reaches the
//! table: `nfsserve` answers it `PROC_UNAVAIL` itself.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfs_fh3, nfspath3, nfsstat3, nfstime3, sattr3, set_mode3,
    set_mtime, set_size3, specdata3,
};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use protocol::id::Id;
use rustix::io::Errno;
use tokio::runtime::Handle;

use crate::DriveSync;
use crate::ops::{Attr, Kind, SetAttr, Special, TOP_INODE};

/// The bits of an `nfsserve` id that hold the `file_id`.
pub const FILE_ID_BITS: u32 = 48;
const FILE_ID_MASK: u64 = (1 << FILE_ID_BITS) - 1;

/// The length of a file handle: the generation and the `file_id`, each
/// eight bytes, little-endian.
const HANDLE_BYTES: usize = 16;

/// How long a READ waits for its operation, a download included, before it
/// is answered `NFS3ERR_JUKEBOX`. A Mac asks again only 4, 8, 16 and then
/// every 30 seconds after that answer, and a download that ends between two
/// of its requests is read only at the next one: the longer wait leaves
/// fewer such gaps, none in a download's first 15 seconds. macOS shows its
/// dialog of unresponsive servers for a request unanswered for about 25
/// seconds.
const READ_WAIT: Duration = Duration::from_secs(15);

/// The NFS server of an App Server.
#[derive(Clone)]
pub struct Server {
    port: u16,
    exports: Arc<Exports>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").field("port", &self.port).finish()
    }
}

impl PartialEq for Server {
    fn eq(&self, other: &Server) -> bool {
        Arc::ptr_eq(&self.exports, &other.exports)
    }
}

impl Eq for Server {}

impl Server {
    /// Starts the server on `127.0.0.1` at a port the system assigns.
    pub fn start() -> io::Result<Server> {
        let exports = Arc::new(Exports::default());
        let port = serve(Table(exports.clone()), "tetral-nfs")?;
        Ok(Server { port, exports })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// The source a mount of `drive_id` of `session_id` names.
    pub fn source(&self, session_id: &str, drive_id: &str) -> String {
        format!("127.0.0.1:/{}/{session_id}/{drive_id}", self.port)
    }

    /// Serves a drive mount, replacing an earlier export of the same drive
    /// of the same Session. Until [`Export::mounted`], GETATTR of the top
    /// folder is answered with `top`.
    pub(crate) fn export(&self, export: NewExport) -> Arc<Export> {
        let mut slots = self.exports.slots.lock().expect("the exports");
        slots.retain(|_, each| {
            each.session_id != export.session_id || each.drive_id != export.drive_id
        });
        let slot = (1..)
            .find(|slot| !slots.contains_key(slot))
            .expect("a free slot");
        let mut sync = export.sync;
        sync.mount_guard = Some(export.detached.clone());
        let export = Arc::new(Export {
            slot,
            session_id: export.session_id,
            drive_id: export.drive_id,
            generation: export.generation,
            read_only: export.read_only,
            sync,
            runtime: export.runtime,
            detached: export.detached,
            mounting: Mutex::new(Some(export.top)),
            writes: Arc::default(),
        });
        slots.insert(slot, export.clone());
        export
    }

    /// Stops serving the drives of `session_id`, whose drive sync ended:
    /// their handles are stale from now on.
    pub(crate) fn forget(&self, session_id: &str) {
        let mut slots = self.exports.slots.lock().expect("the exports");
        slots.retain(|_, export| export.session_id != session_id);
    }

    /// The mounts of `drives` of `session_id`, every drive of the Session
    /// for none.
    fn mounts(&self, session_id: &str, drives: Option<&BTreeSet<String>>) -> Vec<Arc<Export>> {
        let slots = self.exports.slots.lock().expect("the exports");
        slots
            .values()
            .filter(|export| {
                export.session_id == session_id
                    && drives.is_none_or(|drives| drives.contains(export.drive_id.as_str()))
            })
            .cloned()
            .collect()
    }

    /// The WRITEs to the mounts of `drives` of `session_id`, every drive of
    /// the Session for none, received by now.
    pub(crate) fn received(&self, session_id: &str, drives: Option<&BTreeSet<String>>) -> Received {
        Received(
            self.mounts(session_id, drives)
                .into_iter()
                .map(|export| {
                    let mark = export.writes.lock().next;
                    (export.writes.clone(), mark)
                })
                .collect(),
        )
    }

    /// Refuses the unanswered WRITEs to the mounts of `drives` of
    /// `session_id`: each answers `NFS3ERR_IO` and changes nothing. The
    /// drive and `file_id` of each.
    pub(crate) fn refuse_unanswered(
        &self,
        session_id: &str,
        drives: &BTreeSet<String>,
    ) -> Vec<(String, u64)> {
        let mut refused = Vec::new();
        for export in self.mounts(session_id, Some(drives)) {
            for (file_id, flag) in export.writes.lock().unanswered.values() {
                flag.store(true, Ordering::Release);
                refused.push((export.drive_id.as_str().to_owned(), *file_id));
            }
        }
        refused
    }
}

/// WRITEs that [`Server::received`] found.
pub(crate) struct Received(Vec<(Arc<Writes>, u64)>);

impl Received {
    /// Ends once each of them is answered.
    pub(crate) async fn answered(self) {
        for (writes, mark) in self.0 {
            loop {
                let answered = writes.answered.notified();
                if writes.lock().unanswered.range(..mark).next().is_none() {
                    break;
                }
                answered.await;
            }
        }
    }
}

/// The WRITEs a mount has received and not yet answered.
#[derive(Default)]
pub(crate) struct Writes {
    state: Mutex<Unanswered>,
    answered: tokio::sync::Notify,
}

#[derive(Default)]
struct Unanswered {
    next: u64,
    /// By arrival: the `file_id` and whether the WRITE is refused.
    unanswered: BTreeMap<u64, (u64, Arc<AtomicBool>)>,
}

impl Writes {
    fn lock(&self) -> std::sync::MutexGuard<'_, Unanswered> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A WRITE to `file_id` arrived; it is unanswered until the guard drops.
    fn received(self: &Arc<Writes>, file_id: u64) -> HeldWrite {
        let refused = Arc::new(AtomicBool::new(false));
        let mut state = self.lock();
        let arrival = state.next;
        state.next += 1;
        state.unanswered.insert(arrival, (file_id, refused.clone()));
        HeldWrite {
            writes: self.clone(),
            arrival,
            refused,
        }
    }
}

/// An unanswered WRITE.
struct HeldWrite {
    writes: Arc<Writes>,
    arrival: u64,
    refused: Arc<AtomicBool>,
}

impl Drop for HeldWrite {
    fn drop(&mut self) {
        self.writes.lock().unanswered.remove(&self.arrival);
        self.writes.answered.notify_waiters();
    }
}

/// What [`Server::export`] serves.
pub(crate) struct NewExport {
    pub session_id: String,
    pub drive_id: Id,
    pub generation: i64,
    pub read_only: bool,
    pub sync: DriveSync,
    pub runtime: Handle,
    pub detached: Arc<AtomicBool>,
    pub top: Attr,
}

/// A drive mount the server serves.
pub(crate) struct Export {
    slot: u64,
    session_id: String,
    drive_id: Id,
    generation: i64,
    read_only: bool,
    sync: DriveSync,
    runtime: Handle,
    detached: Arc<AtomicBool>,
    /// The top folder's attributes while the mount command runs.
    mounting: Mutex<Option<Attr>>,
    writes: Arc<Writes>,
}

impl Export {
    /// The mount command returned: GETATTR of the top folder goes to the
    /// Drive sync owner from now on.
    pub(crate) fn mounted(&self) {
        *self.mounting.lock().expect("the top folder's attributes") = None;
    }

    fn id(&self, file_id: u64) -> fileid3 {
        self.slot << FILE_ID_BITS | file_id
    }

    /// Runs `operation` as a task on the App Server's runtime; `NFS3ERR_IO`
    /// once the mount is detached.
    async fn run<T, F>(
        &self,
        operation: impl FnOnce(DriveSync, Id) -> F + Send + 'static,
    ) -> Result<T, nfsstat3>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, nfsstat3>> + Send + 'static,
    {
        if self.detached.load(Ordering::Acquire) {
            return Err(nfsstat3::NFS3ERR_IO);
        }
        let (sync, drive_id) = (self.sync.clone(), self.drive_id.clone());
        self.runtime
            .spawn(operation(sync, drive_id))
            .await
            .unwrap_or(Err(nfsstat3::NFS3ERR_IO))
    }

    fn fattr(&self, attr: &Attr) -> fattr3 {
        let (ftype, rdev) = match attr.kind {
            Kind::File => (ftype3::NF3REG, 0),
            Kind::Folder => (ftype3::NF3DIR, 0),
            Kind::Symlink => (ftype3::NF3LNK, 0),
            Kind::Special(Special::Fifo) => (ftype3::NF3FIFO, 0),
            Kind::Special(Special::Socket) => (ftype3::NF3SOCK, 0),
            Kind::Special(Special::CharDevice(rdev)) => (ftype3::NF3CHR, rdev),
            Kind::Special(Special::BlockDevice(rdev)) => (ftype3::NF3BLK, rdev),
        };
        let rdev = rustix::fs::Dev::try_from(rdev).unwrap_or(0);
        fattr3 {
            ftype,
            mode: attr.mode,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            size: attr.size,
            used: attr.size,
            rdev: specdata3 {
                specdata1: rustix::fs::major(rdev),
                specdata2: rustix::fs::minor(rdev),
            },
            fsid: self.generation as u64,
            fileid: self.id(attr.ino),
            atime: time(attr.mtime_ns),
            mtime: time(attr.mtime_ns),
            ctime: time(attr.ctime_ns),
        }
    }
}

#[derive(Default)]
struct Exports {
    slots: Mutex<HashMap<u64, Arc<Export>>>,
}

impl Exports {
    fn by_slot(&self, slot: u64) -> Option<Arc<Export>> {
        self.slots.lock().expect("the exports").get(&slot).cloned()
    }
}

/// Serves `fs` on `127.0.0.1` at a port the system assigns, with the
/// export `/<port>`, from a thread and runtime of its own; the port once it
/// listens.
fn serve<T: NFSFileSystem + Send + Sync + 'static>(fs: T, thread: &str) -> io::Result<u16> {
    let (listening, port) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(thread.to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = listening.send(Err(error));
                    return;
                }
            };
            runtime.block_on(async move {
                let mut listener = match NFSTcpListener::bind("127.0.0.1:0", fs).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = listening.send(Err(error));
                        return;
                    }
                };
                let port = listener.get_listen_port();
                listener.with_export_name(port.to_string());
                let _ = listening.send(Ok(port));
                let _ = listener.handle_forever().await;
            });
        })?;
    port.recv()
        .unwrap_or_else(|_| Err(io::Error::other("the server's thread ended")))
}

fn time(ns: i64) -> nfstime3 {
    let ns = ns.max(0);
    nfstime3 {
        seconds: u32::try_from(ns / 1_000_000_000).unwrap_or(u32::MAX),
        nseconds: (ns % 1_000_000_000) as u32,
    }
}

fn time_ns(time: nfstime3) -> i64 {
    i64::from(time.seconds) * 1_000_000_000 + i64::from(time.nseconds)
}

fn status(error: Errno) -> nfsstat3 {
    match error {
        Errno::PERM => nfsstat3::NFS3ERR_PERM,
        Errno::NOENT => nfsstat3::NFS3ERR_NOENT,
        Errno::ACCESS => nfsstat3::NFS3ERR_ACCES,
        Errno::EXIST => nfsstat3::NFS3ERR_EXIST,
        Errno::XDEV => nfsstat3::NFS3ERR_XDEV,
        Errno::NOTDIR => nfsstat3::NFS3ERR_NOTDIR,
        Errno::ISDIR => nfsstat3::NFS3ERR_ISDIR,
        Errno::INVAL => nfsstat3::NFS3ERR_INVAL,
        Errno::FBIG => nfsstat3::NFS3ERR_FBIG,
        Errno::NOSPC => nfsstat3::NFS3ERR_NOSPC,
        Errno::ROFS => nfsstat3::NFS3ERR_ROFS,
        Errno::NAMETOOLONG => nfsstat3::NFS3ERR_NAMETOOLONG,
        Errno::NOTEMPTY => nfsstat3::NFS3ERR_NOTEMPTY,
        Errno::DQUOT => nfsstat3::NFS3ERR_DQUOT,
        Errno::NOTSUP => nfsstat3::NFS3ERR_NOTSUPP,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

/// [`status`] for an operation on the entry a handle names: an entry that
/// is gone makes the handle stale.
fn object_status(error: Errno) -> nfsstat3 {
    match error {
        Errno::NOENT => nfsstat3::NFS3ERR_STALE,
        error => status(error),
    }
}

/// The changes of `set` the operation table makes; owner and group stay.
fn set_attr(set: &sattr3) -> SetAttr {
    SetAttr {
        size: match set.size {
            set_size3::size(size) => Some(size),
            set_size3::Void => None,
        },
        mode: match set.mode {
            set_mode3::mode(mode) => Some(mode),
            set_mode3::Void => None,
        },
        mtime_ns: match set.mtime {
            set_mtime::SET_TO_CLIENT_TIME(time) => Some(time_ns(time)),
            set_mtime::SET_TO_SERVER_TIME => Some(crate::sync::now_ns()),
            set_mtime::DONT_CHANGE => None,
        },
    }
}

/// The table of drive mounts as `nfsserve` sees it.
struct Table(Arc<Exports>);

impl Table {
    /// The drive mount and `file_id` of an id.
    fn entry(&self, id: fileid3) -> Result<(Arc<Export>, u64), nfsstat3> {
        let export = self
            .0
            .by_slot(id >> FILE_ID_BITS)
            .ok_or(nfsstat3::NFS3ERR_STALE)?;
        if export.detached.load(Ordering::Acquire) {
            return Err(nfsstat3::NFS3ERR_IO);
        }
        Ok((export, id & FILE_ID_MASK))
    }

    /// [`Table::entry`] of a request that would change the drive.
    fn writable(&self, id: fileid3) -> Result<(Arc<Export>, u64), nfsstat3> {
        let (export, file_id) = self.entry(id)?;
        if export.read_only {
            return Err(nfsstat3::NFS3ERR_ROFS);
        }
        Ok((export, file_id))
    }
}

#[async_trait]
impl NFSFileSystem for Table {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    /// Unused: MOUNT goes through [`Table::path_to_id`].
    fn root_dir(&self) -> fileid3 {
        0
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let (export, parent) = self.entry(dirid)?;
        let name = filename.to_vec();
        let attr = export
            .run(move |sync, drive| async move {
                sync.lookup(&drive, parent, &name).await.map_err(status)
            })
            .await?;
        Ok(export.id(attr.ino))
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let (export, file_id) = self.entry(id)?;
        if file_id == TOP_INODE {
            let top = export
                .mounting
                .lock()
                .expect("the top folder's attributes")
                .clone();
            if let Some(top) = top {
                return Ok(export.fattr(&top));
            }
        }
        let attr = export
            .run(move |sync, drive| async move {
                sync.getattr(&drive, file_id).await.map_err(object_status)
            })
            .await?;
        Ok(export.fattr(&attr))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let (export, file_id) = self.writable(id)?;
        let set = set_attr(&setattr);
        let attr = export
            .run(move |sync, drive| async move {
                sync.setattr(&drive, file_id, set, None)
                    .await
                    .map_err(object_status)
            })
            .await?;
        Ok(export.fattr(&attr))
    }

    /// Answered `NFS3ERR_JUKEBOX` after `READ_WAIT`; the operation's task
    /// goes on.
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let (export, file_id) = self.entry(id)?;
        let deadline = export.sync.clock.now() + READ_WAIT;
        let reading = export.run(move |sync, drive| async move {
            sync.read(&drive, file_id, offset, count)
                .await
                .map_err(object_status)
        });
        let bytes = tokio::select! {
            biased;
            bytes = reading => bytes?,
            () = export.sync.clock.sleep_until(deadline) => {
                return Err(nfsstat3::NFS3ERR_JUKEBOX);
            }
        };
        let end = bytes.len() < count as usize;
        Ok((bytes, end))
    }

    /// `nfsserve` replies `FILE_SYNC` only after the content owner has
    /// stored and synchronized these bytes, including local-only files.
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let (export, file_id) = self.writable(id)?;
        let data = data.to_vec();
        let held = export.writes.received(file_id);
        let refused = held.refused.clone();
        let attr = export
            .run(move |mut sync, drive| async move {
                sync.write_guard = Some(refused);
                sync.write(&drive, file_id, offset, &data, None)
                    .await
                    .map_err(object_status)?;
                sync.fsync(&drive, file_id).await.map_err(object_status)?;
                sync.getattr(&drive, file_id).await.map_err(object_status)
            })
            .await?;
        Ok(export.fattr(&attr))
    }

    /// An UNCHECKED or GUARDED create; GUARDED has checked the name first,
    /// so a file already there is the UNCHECKED case, which keeps it.
    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let (export, parent) = self.writable(dirid)?;
        let name = filename.to_vec();
        let mut set = set_attr(&attr);
        let mode = set.mode.take().unwrap_or(0o644);
        let made = export
            .run(move |sync, drive| async move {
                let made = match sync.create(&drive, parent, &name, mode, None).await {
                    Err(Errno::EXIST) => {
                        let found = sync.lookup(&drive, parent, &name).await.map_err(status)?;
                        if found.kind != Kind::File {
                            return Err(nfsstat3::NFS3ERR_EXIST);
                        }
                        set.mode = Some(mode);
                        found
                    }
                    made => made.map_err(status)?,
                };
                if set == SetAttr::default() {
                    return Ok(made);
                }
                sync.setattr(&drive, made.ino, set, None)
                    .await
                    .map_err(status)
            })
            .await?;
        Ok((export.id(made.ino), export.fattr(&made)))
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let (export, parent) = self.writable(dirid)?;
        let name = filename.to_vec();
        let made = export
            .run(move |sync, drive| async move {
                sync.create(&drive, parent, &name, 0o644, None)
                    .await
                    .map_err(status)
            })
            .await?;
        Ok(export.id(made.ino))
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let (export, parent) = self.writable(dirid)?;
        let name = dirname.to_vec();
        let made = export
            .run(move |sync, drive| async move {
                sync.mkdir(&drive, parent, &name, None)
                    .await
                    .map_err(status)
            })
            .await?;
        Ok((export.id(made.ino), export.fattr(&made)))
    }

    /// REMOVE and RMDIR both arrive here; the entry's kind decides.
    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let (export, parent) = self.writable(dirid)?;
        let name = filename.to_vec();
        export
            .run(move |sync, drive| async move {
                let found = sync.lookup(&drive, parent, &name).await.map_err(status)?;
                let removed = if found.kind == Kind::Folder {
                    sync.rmdir(&drive, parent, &name, None).await
                } else {
                    sync.unlink(&drive, parent, &name, None).await
                };
                removed.map_err(status)
            })
            .await
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let (export, parent) = self.writable(from_dirid)?;
        let (to_export, new_parent) = self.entry(to_dirid)?;
        if !Arc::ptr_eq(&export, &to_export) {
            return Err(nfsstat3::NFS3ERR_XDEV);
        }
        let (name, new_name) = (from_filename.to_vec(), to_filename.to_vec());
        export
            .run(move |sync, drive| async move {
                sync.rename(&drive, parent, &name, new_parent, &new_name, None)
                    .await
                    .map_err(status)
            })
            .await
    }

    /// The entries whose file ID is above `start_after`'s (`0`: from the
    /// first), in file-ID order.
    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let (export, folder) = self.entry(dirid)?;
        let entries = export
            .run(move |sync, drive| async move {
                sync.readdir(&drive, folder).await.map_err(object_status)
            })
            .await?;
        let mut entries = entries;
        // File IDs survive renames and are never reused, so a cookie keeps
        // its place after its entry is renamed or removed.
        entries.sort_unstable_by_key(|entry| entry.attr.ino);
        let after = start_after & FILE_ID_MASK;
        let first = entries.partition_point(|entry| entry.attr.ino <= after);
        let rest = &entries[first..];
        let taken = rest.len().min(max_entries);
        Ok(ReadDirResult {
            entries: rest[..taken]
                .iter()
                .map(|entry| DirEntry {
                    fileid: export.id(entry.attr.ino),
                    name: entry.name.as_bytes().into(),
                    attr: export.fattr(&entry.attr),
                })
                .collect(),
            end: taken == rest.len(),
        })
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let (export, parent) = self.writable(dirid)?;
        let (name, target) = (linkname.to_vec(), symlink.to_vec());
        let made = export
            .run(move |sync, drive| async move {
                sync.symlink(&drive, parent, &name, &target, None)
                    .await
                    .map_err(status)
            })
            .await?;
        Ok((export.id(made.ino), export.fattr(&made)))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let (export, file_id) = self.entry(id)?;
        let target = export
            .run(move |sync, drive| async move {
                sync.readlink(&drive, file_id).await.map_err(object_status)
            })
            .await?;
        Ok(target.into())
    }

    /// `path` is the export path after `/<port>`: `/<session_id>/<drive_id>`
    /// of a drive mount being served.
    async fn path_to_id(&self, path: &[u8]) -> Result<fileid3, nfsstat3> {
        let path = std::str::from_utf8(path).map_err(|_| nfsstat3::NFS3ERR_NOENT)?;
        let (session_id, drive_id) = path
            .strip_prefix('/')
            .and_then(|path| path.split_once('/'))
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let slots = self.0.slots.lock().expect("the exports");
        slots
            .values()
            .find(|export| {
                export.session_id == session_id
                    && export.drive_id.as_str() == drive_id
                    && !export.detached.load(Ordering::Acquire)
            })
            .map(|export| export.id(TOP_INODE))
            .ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    fn id_to_fh(&self, id: fileid3) -> nfs_fh3 {
        let generation = self
            .0
            .by_slot(id >> FILE_ID_BITS)
            .map_or(0, |export| export.generation);
        let mut data = Vec::with_capacity(HANDLE_BYTES);
        data.extend_from_slice(&generation.to_le_bytes());
        data.extend_from_slice(&(id & FILE_ID_MASK).to_le_bytes());
        nfs_fh3 { data }
    }

    /// A handle whose generation no drive mount holds, or whose `file_id`
    /// cannot be one, is stale; one of a detached mount answers
    /// `NFS3ERR_IO`.
    fn fh_to_id(&self, handle: &nfs_fh3) -> Result<fileid3, nfsstat3> {
        let data: &[u8; HANDLE_BYTES] = handle
            .data
            .as_slice()
            .try_into()
            .map_err(|_| nfsstat3::NFS3ERR_BADHANDLE)?;
        let (generation, file_id) = data.split_at(8);
        let generation = i64::from_le_bytes(generation.try_into().expect("eight bytes"));
        let file_id = u64::from_le_bytes(file_id.try_into().expect("eight bytes"));
        if file_id == 0 || file_id > FILE_ID_MASK {
            return Err(nfsstat3::NFS3ERR_STALE);
        }
        let slots = self.0.slots.lock().expect("the exports");
        let export = slots
            .values()
            .find(|export| export.generation == generation)
            .ok_or(nfsstat3::NFS3ERR_STALE)?;
        if export.detached.load(Ordering::Acquire) {
            return Err(nfsstat3::NFS3ERR_IO);
        }
        Ok(export.id(file_id))
    }
}

/// The content of the test drive's one file, `check.txt`.
pub const CHECK_CONTENT: &[u8] = b"ok\n";

/// The test build's answer to the READ of `check.txt`: `denied` answers
/// `NFS3ERR_ACCES`, `never` answers nothing.
#[cfg(feature = "test-seams")]
pub const TEST_CHECK_READ_VARIABLE: &str = "TETRAL_TEST_CHECK_READ";

/// File written when the test drive enters a held READ, before waiting.
#[cfg(feature = "test-seams")]
pub const TEST_CHECK_READ_ENTERED_VARIABLE: &str = "TETRAL_TEST_CHECK_READ_ENTERED";

/// The test drive of `tetral drives enable`, exported as `/<port>`: a top
/// folder holding `check.txt`.
pub struct TestDrive {
    port: u16,
}

impl TestDrive {
    /// Starts serving the test drive on `127.0.0.1` at a port the system
    /// assigns.
    pub fn start() -> io::Result<TestDrive> {
        let port = serve(CheckFiles, "tetral-nfs-test-drive")?;
        Ok(TestDrive { port })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn source(&self) -> String {
        format!("127.0.0.1:/{}", self.port)
    }
}

const CHECK_TOP: fileid3 = 1;
const CHECK_FILE: fileid3 = 2;

struct CheckFiles;

impl CheckFiles {
    fn attr(id: fileid3) -> fattr3 {
        let top = id == CHECK_TOP;
        fattr3 {
            ftype: if top { ftype3::NF3DIR } else { ftype3::NF3REG },
            mode: if top { 0o755 } else { 0o644 },
            nlink: if top { 2 } else { 1 },
            uid: rustix::process::getuid().as_raw(),
            gid: rustix::process::getgid().as_raw(),
            size: if top { 0 } else { CHECK_CONTENT.len() as u64 },
            used: 0,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: id,
            atime: nfstime3::default(),
            mtime: nfstime3::default(),
            ctime: nfstime3::default(),
        }
    }
}

#[async_trait]
impl NFSFileSystem for CheckFiles {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    fn root_dir(&self) -> fileid3 {
        CHECK_TOP
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        match (dirid, filename.as_slice()) {
            (CHECK_TOP, b"check.txt") => Ok(CHECK_FILE),
            (CHECK_TOP, _) => Err(nfsstat3::NFS3ERR_NOENT),
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        match id {
            CHECK_TOP | CHECK_FILE => Ok(CheckFiles::attr(id)),
            _ => Err(nfsstat3::NFS3ERR_STALE),
        }
    }

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        if id != CHECK_FILE {
            return Err(nfsstat3::NFS3ERR_ISDIR);
        }
        #[cfg(feature = "test-seams")]
        match std::env::var(TEST_CHECK_READ_VARIABLE).as_deref() {
            Ok("denied") => return Err(nfsstat3::NFS3ERR_ACCES),
            Ok("never") => {
                if let Some(path) = std::env::var_os(TEST_CHECK_READ_ENTERED_VARIABLE) {
                    std::fs::write(path, b"read entered\n").map_err(|_| nfsstat3::NFS3ERR_IO)?;
                }
                std::future::pending::<()>().await;
            }
            _ => {}
        }
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(CHECK_CONTENT.len());
        let end = start
            .saturating_add(count as usize)
            .min(CHECK_CONTENT.len());
        Ok((
            CHECK_CONTENT[start..end].to_vec(),
            end == CHECK_CONTENT.len(),
        ))
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        _max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        if dirid != CHECK_TOP {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }
        let entries = if start_after == 0 {
            vec![DirEntry {
                fileid: CHECK_FILE,
                name: b"check.txt"[..].into(),
                attr: CheckFiles::attr(CHECK_FILE),
            }]
        } else {
            Vec::new()
        };
        Ok(ReadDirResult { entries, end: true })
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_INVAL)
    }

    async fn path_to_id(&self, path: &[u8]) -> Result<fileid3, nfsstat3> {
        if path == b"/" {
            Ok(CHECK_TOP)
        } else {
            Err(nfsstat3::NFS3ERR_NOENT)
        }
    }
}
