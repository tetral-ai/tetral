//! Drive mounts on macOS: the capability, the mount table read with
//! `getfsstat` without waiting on any file system, `/sbin/mount -t nfs`
//! against the App Server's NFS server ([`crate::nfs`]) and `umount -f`.
//!
//! A mount is hard, with no `soft` and no `deadtimeout`, so a read waiting
//! minutes for a download succeeds and macOS shows no "server not
//! responding" dialog; `nobrowse` keeps it out of the Finder and `noac`
//! asks the server for every attribute.

use std::ffi::{CStr, OsStr};
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

/// `umount -f <point>`, which returns at once even when the mount's server
/// does not answer. Like the mount command's, its output stays off the
/// record stream; the failure text is the exit status.
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
    let mut options = format!("vers=3,tcp,port={port},mountport={port},nobrowse,noac");
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

/// Force-unmounts the mounts left in the Session's reserved drive
/// directory, reading the mount table before touching any mount point.
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
            force_unmount(&point)?;
        }
    }
    Ok(())
}
