//! Private creation, bounded reads and atomic replacement of the files under
//! the state root, each failure naming its path.

use std::fs::{self, DirBuilder, File};
use std::io::{self, Read, Write};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};

/// An operating-system error on a path under the state root.
#[derive(Debug, thiserror::Error)]
#[error("{}: {source}", path.display())]
pub struct IoFailure {
    pub path: PathBuf,
    pub source: io::Error,
}

impl IoFailure {
    pub fn new(path: &Path, source: impl Into<io::Error>) -> IoFailure {
        IoFailure {
            path: path.to_path_buf(),
            source: source.into(),
        }
    }
}

/// Creates `path` and its missing ancestors with mode `0700`, one directory
/// at a time, so that a failure names the directory that could not be made.
pub fn create_private_dirs(path: &Path) -> Result<(), IoFailure> {
    let mut missing = Vec::new();
    let mut at = Some(path);
    while let Some(directory) = at {
        match fs::symlink_metadata(directory) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => missing.push(directory),
            _ => break,
        }
        at = directory.parent();
    }
    for directory in missing.into_iter().rev() {
        match DirBuilder::new().mode(0o700).create(directory) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => {
                return Err(IoFailure::new(directory, error));
            }
            _ => {}
        }
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(IoFailure::new(
            path,
            io::Error::from(io::ErrorKind::AlreadyExists),
        )),
        Err(error) => Err(IoFailure::new(path, error)),
    }
}

/// Opens `path` for reading and writing, creating it with mode `0600` when it
/// is missing, never following a symbolic link.
pub fn open_private(path: &Path) -> Result<File, IoFailure> {
    rustix::fs::open(
        path,
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o600),
    )
    .map(File::from)
    .map_err(|error| IoFailure::new(path, error))
}

/// Reads `path`, never following a symbolic link: `None` when it does not
/// exist, and at most `limit + 1` bytes, so that the caller can refuse a file
/// over its limit without reading it whole.
pub fn read_bounded(path: &Path, limit: usize) -> Result<Option<Vec<u8>>, IoFailure> {
    let file = match rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(file) => File::from(file),
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(IoFailure::new(path, error)),
    };
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| IoFailure::new(path, error))?;
    Ok(Some(bytes))
}

/// Marks a replacement's temporary file, so that the next lock holder can
/// remove those that an interrupted replacement left
/// ([`remove_interrupted_replacements`]).
pub const REPLACEMENT_PREFIX: &str = ".replace-";

/// Replaces `path` with `bytes`: a temporary file of mode `0600` in the same
/// directory is written and synced, renamed over `path`, and the directory is
/// synced, so a reader opening the path sees complete old or new bytes.
/// Failures before rename leave the target unchanged; a failure opening or
/// syncing the parent after rename leaves the new target visible without
/// confirmed durability. There is no rollback. A symbolic link at `path` is
/// replaced, never followed.
pub fn replace(path: &Path, bytes: &[u8]) -> Result<(), IoFailure> {
    let directory = path.parent().unwrap_or(Path::new("/"));
    let mut temporary = tempfile::Builder::new()
        .prefix(REPLACEMENT_PREFIX)
        .tempfile_in(directory)
        .map_err(|error| IoFailure::new(directory, error))?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| IoFailure::new(temporary.path(), error))?;
    temporary
        .persist(path)
        .map_err(|error| IoFailure::new(path, error.error))?;
    rustix::fs::open(
        directory,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .and_then(|directory| rustix::fs::fsync(&directory))
    .map_err(|error| IoFailure::new(directory, error))
}

/// Removes the temporary files that replacements interrupted before their
/// rename left in `directory`. Every replacement under the state root runs
/// under its lock, so a caller that holds the lock removes no file in use.
pub fn remove_interrupted_replacements(directory: &Path) -> Result<(), IoFailure> {
    let entries = fs::read_dir(directory).map_err(|error| IoFailure::new(directory, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| IoFailure::new(directory, error))?;
        let path = entry.path();
        let leftover = entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(REPLACEMENT_PREFIX.as_bytes());
        // `file_type` does not follow a symbolic link, which is unlinked as such.
        let is_dir = entry
            .file_type()
            .map_err(|error| IoFailure::new(&path, error))?
            .is_dir();
        if leftover && !is_dir {
            fs::remove_file(&path).map_err(|error| IoFailure::new(&path, error))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn replacement_keeps_open_readers_on_the_complete_old_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("configuration");
        fs::write(&path, b"old complete contents").unwrap();
        let mut old = fs::File::open(&path).unwrap();
        replace(&path, b"new contents").unwrap();
        let mut contents = Vec::new();
        old.read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"old complete contents");
        assert_eq!(fs::read(&path).unwrap(), b"new contents");
    }
}
