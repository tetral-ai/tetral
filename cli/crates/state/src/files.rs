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

/// Creates `path` and its missing ancestors with mode `0700`.
pub fn create_private_dirs(path: &Path) -> Result<(), IoFailure> {
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|error| IoFailure::new(path, error))
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

/// The name prefix of a replacement's temporary file.
pub const REPLACEMENT_PREFIX: &str = ".replace-";

/// Replaces `path` with `bytes`: a temporary file of mode `0600` in the same
/// directory is written and synced, renamed over `path`, and the directory is
/// synced, so that a reader sees the old or the new bytes and a failure leaves
/// `path` as it was; a symbolic link at `path` is replaced, never followed.
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
