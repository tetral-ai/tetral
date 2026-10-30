//! Mounts of a Session's drives: the capability probe, the mount check of
//! each bound drive at `R/sessions/<session_id>/drives/<drive_id>`, the
//! failure texts, the new-failure rule against the `mount_errors` rows,
//! `TETRAL_DRIVES` and the lazy detach.
//!
//! The check reads the mount point's line in `/proc/self/mountinfo` before
//! anything touches the path, so a mount whose server does not answer is
//! force-unmounted with `fusermount3 -u -z` instead of hanging the check.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use protocol::id::{Id, SafeId};

use crate::DriveSync;

/// Whether this App Server can mount drives, decided once at its start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mounting {
    Fuse,
    /// The reason every bound drive fails with, such as `no FUSE: <error>`.
    Unavailable(String),
}

/// The Connection owner's registry of drive syncs, which forwards each
/// `drive.changed` to the drive syncs that have its drive mounted.
pub trait Registry: Send + Sync {
    fn add(&self, session_id: SafeId, sync: DriveSync);
    fn remove(&self, session_id: SafeId);
    fn mounted(&self, drive_id: Id, session_id: SafeId);
    fn detached(&self, drive_id: Id, session_id: SafeId);
}

/// A bound drive as the mount check takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bound {
    pub drive_id: SafeId,
    pub read_only: bool,
    /// This computer's `drive_exclude` entries for the drive.
    pub exclude: Vec<String>,
}

/// A mount failure of a drive with its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub drive_id: String,
    pub error: String,
}

/// The `TETRAL_DRIVES` value of a command of the Session whose directory is
/// `session_dir`: set while it binds a drive on a machine that can mount.
pub fn drives_variable(
    session_dir: &Path,
    binds_drives: bool,
    mounting: &Mounting,
) -> Option<PathBuf> {
    (binds_drives && *mounting == Mounting::Fuse).then(|| session_dir.join("drives"))
}

/// The capability probe: mounts an empty FUSE filesystem at
/// `R/run/fuse-probe` through `fusermount3`, as drive mounts do, and
/// unmounts it.
#[cfg(target_os = "linux")]
pub fn probe(root: &Path) -> Mounting {
    let point = root.join("run").join("fuse-probe");
    let mounted = state::files::create_private_dirs(&point)
        .map_err(|error| error.source.to_string())
        .and_then(|()| fusermount(&point, true))
        .and_then(|device| {
            let unmounted = lazy_unmount(&point);
            drop(device);
            unmounted
        });
    match mounted {
        Ok(()) => Mounting::Fuse,
        Err(error) => Mounting::Unavailable(format!("no FUSE: {error}")),
    }
}

/// Drives mount only on Linux.
#[cfg(not(target_os = "linux"))]
pub fn probe(_root: &Path) -> Mounting {
    Mounting::Unavailable("drives not enabled".to_owned())
}

/// `<error>` of a failure text: the I/O error as Rust displays it, or
/// `exit status <n>` for a command that exits with status `n`.
fn command_error(output: std::io::Result<std::process::ExitStatus>) -> Result<(), String> {
    match output {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(match status.code() {
            Some(code) => format!("exit status {code}"),
            None => status.to_string(),
        }),
        Err(error) => Err(error.to_string()),
    }
}

/// `fusermount3 -u -z <point>`, which returns at once even when the
/// mount's server does not answer.
pub(crate) fn lazy_unmount(point: &Path) -> Result<(), String> {
    command_error(
        Command::new("fusermount3")
            .arg("-u")
            .arg("-z")
            .arg(point)
            .status(),
    )
}

/// A drive this App Server mounted.
pub(crate) struct Mounted {
    /// The `major:minor` of the mount, from `/proc/self/mountinfo`.
    pub device: String,
    pub detached: Arc<AtomicBool>,
    #[cfg(target_os = "linux")]
    pub notifier: fuser::Notifier,
}

impl Mounted {
    /// Invalidates the kernel's cached pages of `ino` off the drive sync
    /// owner: the kernel locks those pages for the notice, and a READ or
    /// WRITE holding one waits for the owner's answer.
    pub(crate) fn invalidate(&self, ino: u64) {
        #[cfg(target_os = "linux")]
        {
            let notifier = self.notifier.clone();
            tokio::task::spawn_blocking(move || {
                let _ = notifier.inval_inode(fuser::INodeNo(ino), 0, 0);
            });
        }
        #[cfg(not(target_os = "linux"))]
        let _ = ino;
    }
}

/// Mounts `sync`'s drive `drive_id` at `point`, which the check found not
/// listed: a missing point is created with mode `0700`, a folder is mounted
/// over and anything else fails.
#[cfg(target_os = "linux")]
pub(crate) fn mount(
    sync: DriveSync,
    drive_id: &Id,
    point: &Path,
    read_only: bool,
    runtime: tokio::runtime::Handle,
) -> Result<Mounted, String> {
    match std::fs::symlink_metadata(point) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            state::files::create_private_dirs(point)
                .map_err(|error| format!("mount point: {}", error.source))?;
        }
        Err(error) => return Err(format!("mount point: {error}")),
        Ok(metadata) if !metadata.is_dir() => {
            let error = std::io::Error::from_raw_os_error(rustix::io::Errno::NOTDIR.raw_os_error());
            return Err(format!("mount point: {error}"));
        }
        Ok(_) => {}
    }
    let detached = Arc::new(AtomicBool::new(false));
    let adapter = crate::fuse::Adapter::new(sync, drive_id.clone(), runtime, detached.clone());
    let device = fusermount(point, read_only)?;
    let session = fuser::Session::from_fd(
        adapter,
        device,
        fuser::SessionACL::Owner,
        fuser::Config::default(),
    )
    .map_err(|error| format!("mount: {error}"))?;
    let notifier = session.notifier();
    // The session ends when the kernel ends the mount. It owns no mount, so
    // its end never unmounts a later mount at the same point.
    std::thread::Builder::new()
        .name("tetral-fuse".to_owned())
        .spawn(move || session.run())
        .map_err(|error| format!("mount: {error}"))?;
    let device = crate::mountinfo::current_device(point)
        .ok()
        .flatten()
        .ok_or_else(|| "mount: the mount is not listed".to_owned())?;
    Ok(Mounted {
        device,
        detached,
        notifier,
    })
}

/// Mounts a FUSE filesystem at `point` through `fusermount3`, which hands
/// back the device's file descriptor over `_FUSE_COMMFD`.
#[cfg(target_os = "linux")]
fn fusermount(point: &Path, read_only: bool) -> Result<std::os::fd::OwnedFd, String> {
    use std::os::fd::AsRawFd;

    use rustix::net::{
        AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SocketFlags,
        SocketType,
    };
    let failed = |error: std::io::Error| format!("mount: {error}");
    let (ours, theirs) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::CLOEXEC,
        None,
    )
    .map_err(|error| failed(error.into()))?;
    // `fusermount3` inherits its end of the pair.
    rustix::io::fcntl_setfd(&theirs, rustix::io::FdFlags::empty())
        .map_err(|error| failed(error.into()))?;
    let options = format!(
        "fsname=tetral,subtype=tetral,default_permissions,{}",
        if read_only { "ro" } else { "rw" }
    );
    let status = Command::new("fusermount3")
        .arg("-o")
        .arg(options)
        .arg("--")
        .arg(point)
        .env("_FUSE_COMMFD", theirs.as_raw_fd().to_string())
        .stdin(std::process::Stdio::null())
        .status();
    drop(theirs);
    command_error(status).map_err(|error| format!("mount: {error}"))?;
    let mut space = [std::mem::MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
    let mut ancillary = RecvAncillaryBuffer::new(&mut space);
    let mut byte = [0u8; 1];
    rustix::net::recvmsg(
        &ours,
        &mut [std::io::IoSliceMut::new(&mut byte)],
        &mut ancillary,
        RecvFlags::CMSG_CLOEXEC,
    )
    .map_err(|error| failed(error.into()))?;
    for message in ancillary.drain() {
        if let RecvAncillaryMessage::ScmRights(mut descriptors) = message
            && let Some(descriptor) = descriptors.next()
        {
            return Ok(descriptor);
        }
    }
    Err("mount: fusermount3 sent no device".to_owned())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn mount(
    _sync: DriveSync,
    _drive_id: &Id,
    _point: &Path,
    _read_only: bool,
    _runtime: tokio::runtime::Handle,
) -> Result<Mounted, String> {
    Err("mount: not supported".to_owned())
}

/// Detaches a mount: later requests of its session answer `EIO`, and the
/// mount point is lazily unmounted.
pub(crate) fn detach(mounted: &Mounted, point: &Path) {
    mounted.detached.store(true, Ordering::Release);
    let _ = lazy_unmount(point);
}

/// Judges `failures` in order against the drives' rows: a failure is new
/// unless the row, as the failures judged before it would leave it, holds
/// its text.
pub(crate) fn new_failures(
    rows: &std::collections::BTreeMap<String, String>,
    failures: Vec<Failure>,
) -> Vec<Failure> {
    let mut rows = rows.clone();
    failures
        .into_iter()
        .filter(|failure| {
            let new = rows.get(&failure.drive_id) != Some(&failure.error);
            rows.insert(failure.drive_id.clone(), failure.error.clone());
            new
        })
        .collect()
}

impl crate::sync::Owner {
    /// The mount check of each drive of `bound`; the failures, in order.
    pub(crate) fn check_mounts(&mut self, bound: Vec<Bound>) -> Vec<Failure> {
        bound
            .into_iter()
            .filter_map(|bound| {
                let drive = bound.drive_id.as_str().to_owned();
                self.check_mount(&bound).err().map(|error| Failure {
                    drive_id: drive,
                    error,
                })
            })
            .collect()
    }

    fn check_mount(&mut self, bound: &Bound) -> Result<(), String> {
        if let Mounting::Unavailable(reason) = &self.mounting {
            return Err(reason.clone());
        }
        let drive = bound.drive_id.as_str();
        let point = self.machine_path(drive, "");
        let listed =
            crate::mountinfo::current_device(&point).map_err(|error| format!("mount: {error}"))?;
        let ours = self
            .mounted
            .get(drive)
            .is_some_and(|mounted| Some(&mounted.device) == listed.as_ref())
            && self.mounts.contains_key(drive);
        if ours {
            return Ok(());
        }
        if listed.is_some() {
            lazy_unmount(&point).map_err(|error| format!("force unmount: {error}"))?;
        }
        self.detach(drive);
        let handle = self.handle.clone().expect("the owner's handle");
        let drive_id = bound.drive_id.to_id();
        let mounted = mount(
            handle,
            &drive_id,
            &point,
            bound.read_only,
            tokio::runtime::Handle::current(),
        )?;
        self.mounted.insert(drive.to_owned(), mounted);
        self.registry
            .mounted(drive_id.clone(), self.session_id.clone());
        self.mount_catch_up(drive_id, bound.exclude.clone());
        Ok(())
    }

    /// Detaches `drive`'s mount, if this App Server holds one, and ends its
    /// catch-up.
    pub(crate) fn detach(&mut self, drive: &str) {
        self.mounts.remove(drive);
        self.clean.remove(drive);
        if let Some(mounted) = self.mounted.remove(drive) {
            let point = self.machine_path(drive, "");
            self.registry.detached(
                Id::new(drive).expect("a mounted drive's id"),
                self.session_id.clone(),
            );
            detach(&mounted, &point);
        }
    }

    /// A failed listing of `drive` ended: it goes to every check in
    /// progress, or is held when it is new.
    pub(crate) fn listing_failed(&mut self, drive: &str, error: &str) {
        let failure = Failure {
            drive_id: drive.to_owned(),
            error: error.to_owned(),
        };
        if !self.checking.is_empty() {
            for failures in self.checking.values_mut() {
                failures.push(failure.clone());
            }
            return;
        }
        let rows = &self.state.tables().mount_errors;
        let last = self
            .held
            .iter()
            .rev()
            .find(|held| held.drive_id == drive)
            .map(|held| &held.error)
            .or_else(|| rows.get(drive));
        if last != Some(&failure.error) {
            self.held.push(failure);
        }
    }

    pub(crate) fn judge(&self, failures: Vec<Failure>) -> Vec<Failure> {
        new_failures(&self.state.tables().mount_errors, failures)
    }

    /// The first catch-up since `drive`'s mount finished: its row goes.
    pub(crate) async fn caught_up_first(&mut self, drive: &str) -> Result<(), String> {
        self.clean.insert(drive.to_owned());
        if !self.state.tables().mount_errors.contains_key(drive) {
            return Ok(());
        }
        self.write(crate::state::Write {
            mount_errors_removed: vec![drive.to_owned()],
            ..crate::state::Write::default()
        })
        .await
    }

    pub(crate) async fn reported(&mut self, failures: Vec<Failure>) {
        let mut write = crate::state::Write::default();
        for failure in failures {
            if self.clean.contains(&failure.drive_id) {
                continue;
            }
            write
                .mount_errors
                .retain(|(drive, _)| *drive != failure.drive_id);
            write.mount_errors.push((failure.drive_id, failure.error));
        }
        if !write.mount_errors.is_empty() {
            // A failed write leaves the failure to be reported again.
            let _ = self.write(write).await;
        }
    }
}
