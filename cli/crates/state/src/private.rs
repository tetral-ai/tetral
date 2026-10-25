//! The checks that the state root's control paths, and the control socket's
//! directory and socket, belong to the current user alone.

use std::fmt;
use std::fs::{FileType, Metadata};
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};

use crate::atomic::IoFailure;
use crate::paths::{self, StateRoot};

/// Why a control path is not private to the current user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsafe {
    SymbolicLink,
    OtherOwner,
    NotDirectory,
    NotRegularFile,
    NotSocket,
    OpenToOthers,
    Mode { expected: u32, found: u32 },
}

impl fmt::Display for Unsafe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unsafe::SymbolicLink => f.write_str("is a symbolic link"),
            Unsafe::OtherOwner => f.write_str("is owned by another user"),
            Unsafe::NotDirectory => f.write_str("is not a directory"),
            Unsafe::NotRegularFile => f.write_str("is not a regular file"),
            Unsafe::NotSocket => f.write_str("is not a socket"),
            Unsafe::OpenToOthers => f.write_str("has permission bits for group or others"),
            Unsafe::Mode { expected, found } => {
                write!(f, "has mode {found:04o}, not {expected:04o}")
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("{} {reason}", path.display())]
    Unsafe { path: PathBuf, reason: Unsafe },
    #[error(transparent)]
    Io(#[from] IoFailure),
}

#[derive(Clone, Copy)]
enum Kind {
    Directory,
    RegularFile,
    Socket,
}

#[derive(Clone, Copy)]
enum Permissions {
    /// No permission bit for group or others.
    Private,
    Exactly(u32),
}

/// Checks, without creating anything and without following a symbolic link,
/// each control path that exists: the root, `run/`, the lock,
/// `config.toml` and `credentials.json` are not symbolic links, belong to the
/// current user and give group and others no permission, the root and `run/`
/// being directories and the others regular files. When the root exists, `S`
/// must also be a directory of the current user with mode `0700`, and the
/// root's socket a socket of the current user with mode `0600`. Ancestors of
/// the root may be symbolic links.
pub fn check_root(root: &StateRoot) -> Result<(), CheckError> {
    if !check(root.path(), Kind::Directory, Permissions::Private)? {
        return Ok(());
    }
    check(
        &root.join(paths::RUN),
        Kind::Directory,
        Permissions::Private,
    )?;
    for file in [paths::LOCK, paths::CONFIG, paths::CREDENTIALS] {
        check(&root.join(file), Kind::RegularFile, Permissions::Private)?;
    }
    check_socket(root)?;
    Ok(())
}

/// Checks `S` and the root's socket as [`check_root`] does, and gives the
/// socket's path; `None` when the root does not exist.
pub fn check_socket(root: &StateRoot) -> Result<Option<PathBuf>, CheckError> {
    let socket = paths::socket_path(root).map_err(|error| IoFailure::new(root.path(), error))?;
    let directory = paths::socket_dir();
    if check(&directory, Kind::Directory, Permissions::Exactly(0o700))?
        && let Some(socket) = &socket
    {
        check(socket, Kind::Socket, Permissions::Exactly(0o600))?;
    }
    Ok(socket)
}

/// Creates a missing `S` with mode `0700`, then checks `S` as
/// [`check_root`] does: an existing `S` keeps its mode, so one with another
/// mode or owner, or a symbolic link, is refused and left as it is.
pub fn create_socket_dir() -> Result<(), CheckError> {
    let directory = paths::socket_dir();
    match rustix::fs::mkdir(&directory, Mode::from_raw_mode(0o700)) {
        Ok(()) => {
            // The umask may have cleared bits of the mode that mkdir got.
            let created = rustix::fs::open(
                &directory,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| IoFailure::new(&directory, error))?;
            rustix::fs::fchmod(&created, Mode::from_raw_mode(0o700))
                .map_err(|error| IoFailure::new(&directory, error))?;
        }
        Err(rustix::io::Errno::EXIST) => {}
        Err(error) => return Err(IoFailure::new(&directory, error).into()),
    }
    check(&directory, Kind::Directory, Permissions::Exactly(0o700))?;
    Ok(())
}

/// Checks one path; `false` when it does not exist.
fn check(path: &Path, kind: Kind, permissions: Permissions) -> Result<bool, CheckError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(IoFailure::new(path, error).into()),
    };
    match reason(&metadata, kind, permissions) {
        None => Ok(true),
        Some(reason) => Err(CheckError::Unsafe {
            path: path.to_path_buf(),
            reason,
        }),
    }
}

fn reason(metadata: &Metadata, kind: Kind, permissions: Permissions) -> Option<Unsafe> {
    let file_type: FileType = metadata.file_type();
    if file_type.is_symlink() {
        return Some(Unsafe::SymbolicLink);
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Some(Unsafe::OtherOwner);
    }
    match kind {
        Kind::Directory if !file_type.is_dir() => return Some(Unsafe::NotDirectory),
        Kind::RegularFile if !file_type.is_file() => return Some(Unsafe::NotRegularFile),
        Kind::Socket if !file_type.is_socket() => return Some(Unsafe::NotSocket),
        _ => {}
    }
    let mode = metadata.mode() & 0o7777;
    match permissions {
        Permissions::Private if mode & 0o077 != 0 => Some(Unsafe::OpenToOthers),
        Permissions::Exactly(expected) if mode != expected => Some(Unsafe::Mode {
            expected,
            found: mode,
        }),
        _ => None,
    }
}
