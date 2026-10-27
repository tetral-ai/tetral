//! The checks that the state root's control paths, and the control socket's
//! directory and socket, belong to the current user alone.

use std::fmt;
use std::fs::{FileType, Metadata};
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};

use crate::files::IoFailure;
use crate::paths::{self, StateRoot};

/// Why a control path is not private to the current user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unsafe {
    NotUtf8,
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
            Unsafe::NotUtf8 => f.write_str("is not UTF-8"),
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
    #[error("{}", unsafe_path_message(path, reason))]
    Unsafe { path: PathBuf, reason: Unsafe },
    #[error(transparent)]
    Io(#[from] IoFailure),
}

fn unsafe_path_message(path: &Path, reason: &Unsafe) -> String {
    if *reason == Unsafe::NotUtf8 {
        "the state root path is not UTF-8".to_owned()
    } else {
        format!("{} {reason}", path.display())
    }
}

/// Checks the root's given and resolved forms before configure can create
/// anything or a command reads configuration. Missing directories are only
/// projected; existing components keep their filesystem resolution rules.
pub fn check_root_utf8(root: &StateRoot) -> Result<(), CheckError> {
    check_canonical_root_utf8(root.path())?;
    let resolved = root
        .resolved_for_creation()
        .map_err(|error| IoFailure::new(root.path(), error))?;
    check_canonical_root_utf8(&resolved)
}

/// Checks the canonical root computed while startup owns its lock. The
/// same refusal is used before command dispatch and during startup.
pub fn check_canonical_root_utf8(root: &Path) -> Result<(), CheckError> {
    root.to_str().map(|_| ()).ok_or_else(|| CheckError::Unsafe {
        path: root.to_path_buf(),
        reason: Unsafe::NotUtf8,
    })
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
    check_canonical_root_utf8(root.path())?;
    let exists = check(root.path(), Kind::Directory, Permissions::Private)?;
    check_root_utf8(root)?;
    if !exists {
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
    check_socket_paths(socket, &directory)
}

#[cfg(feature = "test-seams")]
pub fn check_socket_in(
    root: &StateRoot,
    context: &paths::TestSocketContext,
) -> Result<Option<PathBuf>, CheckError> {
    let socket = context
        .socket_path(root)
        .map_err(|error| IoFailure::new(root.path(), error))?;
    check_socket_paths(socket, &context.socket_dir())
}

fn check_socket_paths(
    socket: Option<PathBuf>,
    directory: &Path,
) -> Result<Option<PathBuf>, CheckError> {
    if check(directory, Kind::Directory, Permissions::Exactly(0o700))?
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

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;

    use super::*;

    #[test]
    fn root_encoding_is_checked_before_any_missing_directory_is_created() {
        let base = tempfile::tempdir().unwrap();
        let non_utf8 = base.path().join(OsString::from_vec(b"root-\xff".to_vec()));
        fs::create_dir(&non_utf8).unwrap();
        let link = base.path().join("alias");
        symlink(&non_utf8, &link).unwrap();
        let rows = [
            non_utf8.clone(),
            non_utf8.join("missing"),
            link.clone(),
            link.join("missing/child"),
        ];
        for path in rows {
            let root = StateRoot::new(path.as_os_str()).unwrap();
            let error = check_root_utf8(&root).unwrap_err();
            assert!(matches!(
                error,
                CheckError::Unsafe {
                    reason: Unsafe::NotUtf8,
                    ..
                }
            ));
            assert_eq!(error.to_string(), "the state root path is not UTF-8");
        }
        assert!(!non_utf8.join("missing").exists());
    }

    #[test]
    fn missing_root_resolution_keeps_filesystem_symlinks_and_parent_components() {
        let base = tempfile::tempdir().unwrap();
        let canonical_base = base.path().canonicalize().unwrap();
        let non_utf8 = base.path().join(OsString::from_vec(b"root-\xff".to_vec()));
        fs::create_dir_all(non_utf8.join("child")).unwrap();
        let alias = base.path().join("alias");
        symlink(non_utf8.join("child"), &alias).unwrap();

        let forbidden = StateRoot::new(alias.join("../missing").as_os_str()).unwrap();
        assert!(matches!(
            check_root_utf8(&forbidden),
            Err(CheckError::Unsafe {
                reason: Unsafe::NotUtf8,
                ..
            })
        ));
        // Leaving both resolved ancestors reaches the UTF-8 base. A lexical
        // normalization of the original alias would choose a different path.
        let outside = StateRoot::new(alias.join("../../missing").as_os_str()).unwrap();
        check_root_utf8(&outside).unwrap();
        assert_eq!(
            outside.resolved_for_creation().unwrap(),
            canonical_base.join("missing")
        );

        // Once a prospective directory is left, existing links are resolved
        // again rather than treated as part of a purely lexical suffix.
        let reentered =
            StateRoot::new(base.path().join("missing/../alias/../new").as_os_str()).unwrap();
        assert!(matches!(
            check_root_utf8(&reentered),
            Err(CheckError::Unsafe {
                reason: Unsafe::NotUtf8,
                ..
            })
        ));
        assert!(!base.path().join("missing").exists());
        assert!(!non_utf8.join("new").exists());
    }

    #[test]
    fn a_dangling_root_link_keeps_its_existing_unsafe_path_refusal() {
        let base = tempfile::tempdir().unwrap();
        let link = base.path().join("root");
        symlink(base.path().join("missing"), &link).unwrap();
        let root = StateRoot::new(link.as_os_str()).unwrap();
        assert!(matches!(
            check_root(&root),
            Err(CheckError::Unsafe {
                reason: Unsafe::SymbolicLink,
                ..
            })
        ));
        assert!(!base.path().join("missing").exists());
    }
}
