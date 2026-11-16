//! Drive mounts on macOS: the capability, the mount table read with
//! `getfsstat` without waiting on any file system, `/sbin/mount -t nfs`
//! against the App Server's NFS server ([`crate::nfs`]), the flush of the
//! client's cache, `umount -f` and the unmount of a mount whose server is
//! gone.
//!
//! A mount is hard, with no `soft` and no `deadtimeout`, so a read waiting
//! minutes for a download succeeds; `mutejukebox` keeps a mount whose READs
//! are answered `NFS3ERR_JUKEBOX` out of macOS's dialog of unresponsive
//! servers, which macOS still shows for a server that answers nothing for
//! 25 seconds or more. `locallocks` keeps `fcntl` and `flock` locks on the Mac, since
//! the server has no lock manager. `nobrowse` keeps the mount out of the
//! Finder and `noac` asks the server for every attribute.

use std::collections::BTreeSet;
use std::ffi::{CStr, CString, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::mount::{Mounted, Mounting, command_error};
use crate::nfs::{NewExport, Server};

/// The `mount` that mounts a drive.
const MOUNT: &str = "/sbin/mount";

/// The test build's replacement for [`MOUNT`].
#[cfg(feature = "test-seams")]
pub const TEST_MOUNT_VARIABLE: &str = "TETRAL_TEST_MOUNT";

/// The capability: drives mount once `drives_enabled` is recorded and the
/// NFS server listens.
pub fn probe(drives_enabled: bool) -> Mounting {
    if !drives_enabled {
        return Mounting::Unavailable("drives not enabled".to_owned());
    }
    match Server::start() {
        Ok(server) => Mounting::Nfs(server),
        Err(error) => Mounting::Unavailable(format!("NFS server: {error}")),
    }
}

/// The mounts of the mount table, as mount point and source, read without
/// waiting on any file system.
fn mount_table() -> io::Result<Vec<(PathBuf, String)>> {
    let entry = std::mem::size_of::<libc::statfs>();
    let mut room = 16;
    loop {
        let mut table: Vec<libc::statfs> = Vec::with_capacity(room);
        let bytes = libc::c_int::try_from(room * entry).map_err(|_| io::ErrorKind::OutOfMemory)?;
        // SAFETY: the buffer holds `room` entries of `bytes` bytes in all,
        // which the call does not exceed.
        let count = unsafe { libc::getfsstat(table.as_mut_ptr(), bytes, libc::MNT_NOWAIT) };
        let count = usize::try_from(count).map_err(|_| io::Error::last_os_error())?;
        if count == room {
            room *= 2;
            continue;
        }
        // SAFETY: the call wrote `count` entries.
        unsafe { table.set_len(count) };
        return Ok(table
            .iter()
            .map(|each| {
                // SAFETY: both names are NUL-terminated within their arrays.
                let (point, source) = unsafe {
                    (
                        CStr::from_ptr(each.f_mntonname.as_ptr()),
                        CStr::from_ptr(each.f_mntfromname.as_ptr()),
                    )
                };
                (
                    PathBuf::from(OsStr::from_bytes(point.to_bytes())),
                    source.to_string_lossy().into_owned(),
                )
            })
            .collect());
    }
}

/// The source of the last mount at `point`, if one is listed.
pub fn listed(point: &Path) -> io::Result<Option<String>> {
    Ok(mount_table()?
        .into_iter()
        .rev()
        .find(|(each, _)| each == point)
        .map(|(_, source)| source))
}

/// `sync_volume_np`'s flag that waits for the writes it starts.
const SYNC_VOLUME_WAIT: libc::c_int = 2;

unsafe extern "C" {
    /// Libc's volume sync: 0, or the error number.
    fn sync_volume_np(path: *const libc::c_char, flags: libc::c_int) -> libc::c_int;
}

/// Flushes, one after another, the client cache of each mount in the
/// Session's reserved drive directory that the mount table lists as
/// `server`'s, of `drives` only when given. The client keeps written data
/// until the open file's last close, and a close is not the last while a
/// child still holds the descriptor; the flush sends that data and waits
/// for the replies, which the drive sync owner gives, so it never runs in
/// the owner's turn. A failure is recorded as `drive.flush_failed`.
pub(crate) fn flush(
    server: &Server,
    session_id: &str,
    session_directory: &Path,
    drives: Option<&BTreeSet<String>>,
) {
    state::seams::barrier_blocking("drive.flush");
    let table = match mount_table() {
        Ok(table) => table,
        Err(error) => return flush_failed(session_id, None, &error),
    };
    let reserved = session_directory.join("drives");
    for (point, source) in table {
        let Some(drive) = point
            .strip_prefix(&reserved)
            .ok()
            .and_then(|rest| rest.to_str())
            .filter(|rest| !rest.is_empty() && !rest.contains('/'))
        else {
            continue;
        };
        if drives.is_some_and(|drives| !drives.contains(drive))
            || source != server.source(session_id, drive)
        {
            continue;
        }
        let Ok(path) = CString::new(point.as_os_str().as_bytes()) else {
            continue;
        };
        // SAFETY: `path` is NUL-terminated.
        let error = unsafe { sync_volume_np(path.as_ptr(), SYNC_VOLUME_WAIT) };
        if error != 0 {
            let error = std::io::Error::from_raw_os_error(error);
            flush_failed(session_id, Some(drive), &error);
        }
    }
}

fn flush_failed(session_id: &str, drive: Option<&str>, error: &std::io::Error) {
    let mut identities = vec![(observe::Identity::Session, session_id)];
    identities.extend(drive.map(|drive| (observe::Identity::Drive, drive)));
    observe::correlation_span(&identities, None).in_scope(|| {
        observe::warn!(
            "drive.flush_failed",
            error.os_code = error.raw_os_error().unwrap_or(0),
            error.code = observe::Code("drive_flush_failed"),
            error.message = observe::Cause::system(&error.to_string())
        );
    });
}

/// `umount -f <point>`, for a mount whose server answers: the command syncs
/// the mount before it unmounts, and waits ten seconds for that sync when
/// the server does not answer ([`unmount_stale`]). Like the mount command's,
/// its output stays off the record stream; the failure text is the exit
/// status.
pub fn force_unmount(point: &Path) -> Result<(), String> {
    command_error(
        Command::new("/sbin/umount")
            .arg("-f")
            .arg(point)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status(),
    )
}

/// Unmounts by force a mount whose server is gone, as a killed App Server
/// leaves it: `unmount(2)` with `MNT_FORCE` and without the sync of
/// `umount -f`, which such a server never answers. It returns in about two
/// seconds, where `umount -f` takes ten. The failure text is the OS error.
pub fn unmount_stale(point: &Path) -> Result<(), String> {
    let path = CString::new(point.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
    // SAFETY: `path` is NUL-terminated and outlives the call.
    if unsafe { libc::unmount(path.as_ptr(), libc::MNT_FORCE) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().to_string())
    }
}

fn mount_program() -> PathBuf {
    #[cfg(feature = "test-seams")]
    if let Some(program) = std::env::var_os(TEST_MOUNT_VARIABLE) {
        return PathBuf::from(program);
    }
    PathBuf::from(MOUNT)
}

/// Mounts `source`, served on `port`, at `point`; the failure text is
/// `mount: <error>`.
pub fn mount_nfs(port: u16, source: &str, point: &Path, read_only: bool) -> Result<(), String> {
    let mut options =
        format!("vers=3,tcp,port={port},mountport={port},nobrowse,noac,mutejukebox,locallocks");
    if read_only {
        options.push_str(",rdonly");
    }
    command_error(
        Command::new(mount_program())
            .args(["-t", "nfs", "-o", &options, source])
            .arg(point)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status(),
    )
    .map_err(|error| format!("mount: {error}"))
}

/// Serves `export` and mounts it at `point`, which the check found not
/// listed and prepared.
pub(crate) fn mount(server: &Server, export: NewExport, point: &Path) -> Result<Mounted, String> {
    let source = server.source(&export.session_id, export.drive_id.as_str());
    let (detached, read_only) = (export.detached.clone(), export.read_only);
    let served = server.export(export);
    let mounted = mount_nfs(server.port(), &source, point, read_only);
    served.mounted();
    if let Err(error) = mounted {
        detached.store(true, std::sync::atomic::Ordering::Release);
        return Err(error);
    }
    Ok(Mounted {
        device: source,
        detached,
    })
}

/// Force-unmounts the mounts an earlier App Server left in the Session's
/// reserved drive directory, reading the mount table before touching any
/// mount point.
pub fn detach_remaining(session_directory: &Path) -> Result<(), String> {
    let entries = match std::fs::read_dir(session_directory.join("drives")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    let table = mount_table().map_err(|error| error.to_string())?;
    for entry in entries {
        let point = entry.map_err(|error| error.to_string())?.path();
        if table.iter().any(|(each, _)| *each == point) {
            unmount_stale(&point)?;
        }
    }
    Ok(())
}
