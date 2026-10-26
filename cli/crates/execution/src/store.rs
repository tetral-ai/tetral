//! Where a task's files live on this machine, and when they exist. Each task
//! is named during its admission, before its policy is translated, so that
//! its profile already names its private temporary directory; that directory
//! lies in `T`, the parent of every task's temporary directory, kept short so
//! that a Unix socket below `TMPDIR` fits the platforms' socket path limits.
//! Before its launch a task gets its directory under its Session, holding its
//! two logs, and then its temporary directory (`TaskFiles::create`); both go
//! once the task has ended and its reports are acknowledged
//! (`TaskFiles::remove`). What an earlier App Server's tasks left goes too:
//! the entries of `T` at the start (`clear_task_parent`), and a Session's
//! task directories once its earlier records are confirmed
//! (`clear_session_tasks`, from `cleanup`). Nothing else is created or
//! deleted here.

use std::fs::{self, DirBuilder, File};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};
use sha2::{Digest, Sha256};
use state::files::{self, IoFailure};

/// The random bytes of a task's name, written as twice as many lowercase
/// hexadecimal characters.
const TASK_NAME_BYTES: usize = 16;

/// The characters of a task's name that name its temporary directory.
const TEMPORARY_NAME_CHARS: usize = 16;

/// The bytes of the state root's SHA-256 that name `T`, written as twice as
/// many lowercase hexadecimal characters.
const PARENT_DIGEST_BYTES: usize = 4;

#[cfg(target_os = "linux")]
const TEMPORARY_ROOT: &str = "/tmp";
#[cfg(target_os = "macos")]
const TEMPORARY_ROOT: &str = "/private/tmp";

/// The directory of a Session's task directories.
const TASKS: &str = "tasks";

const STDOUT_LOG: &str = "stdout.log";
const STDERR_LOG: &str = "stderr.log";

/// The mode of `T`, of a task's directory and of its temporary directory.
const DIRECTORY_MODE: u32 = 0o700;

/// The mode of a task's logs.
const LOG_MODE: u32 = 0o600;

/// The bytes of each stream that its log receives as they arrive.
pub const LOG_HEAD_BYTES: usize = crate::capture::STORED_HALF_BYTES;

/// A new task's name.
pub fn task_name() -> String {
    hex(&rand::random::<[u8; TASK_NAME_BYTES]>())
}

/// `T` for the state root whose canonical path is `state_root`:
/// `tetral-<h>` in the canonical temporary root, `<h>` naming the root.
pub fn task_parent(state_root: &Path) -> PathBuf {
    let digest = Sha256::digest(state_root.as_os_str().as_bytes());
    let root = std::fs::canonicalize(TEMPORARY_ROOT).unwrap_or_else(|_| TEMPORARY_ROOT.into());
    root.join(format!("tetral-{}", hex(&digest[..PARENT_DIGEST_BYTES])))
}

/// The private temporary directory in `parent` of the task named `name`.
pub fn task_temporary_directory(parent: &Path, name: &str) -> PathBuf {
    parent.join(&name[..TEMPORARY_NAME_CHARS.min(name.len())])
}

/// The directory of the task named `name` of the Session `session_id`,
/// `sessions/<session_id>/tasks/<name>/` under the canonical state root.
pub fn task_directory(state_root: &Path, session_id: &str, name: &str) -> PathBuf {
    state::paths::session_directory(state_root, session_id)
        .join(TASKS)
        .join(name)
}

/// Why a task's files could not be created, naming the path.
#[derive(Debug, thiserror::Error)]
pub enum StorageFailure {
    #[error("the task's files could not be created: {0}")]
    Io(#[from] IoFailure),
    #[error(
        "the task's files could not be created: {} is not a directory of this user with no permission for group or others",
        .0.display()
    )]
    UnsafeParent(PathBuf),
}

/// A task's directory, with its logs, and its temporary directory.
pub struct TaskFiles {
    directory: PathBuf,
    temporary: PathBuf,
    pub stdout: Log,
    pub stderr: Log,
}

impl TaskFiles {
    /// The files of a task whose directory is `directory` and whose
    /// temporary directory is `temporary`, in `T`; each log receives the
    /// first `log_head_bytes` bytes of its stream as they arrive.
    pub fn new(directory: PathBuf, temporary: PathBuf, log_head_bytes: usize) -> TaskFiles {
        let log = |name: &str| Log {
            path: directory.join(name),
            room: log_head_bytes,
        };
        TaskFiles {
            stdout: log(STDOUT_LOG),
            stderr: log(STDERR_LOG),
            directory,
            temporary,
        }
    }

    pub fn temporary(&self) -> &Path {
        &self.temporary
    }

    /// Creates the task's directory and its missing ancestors, its two empty
    /// logs, a missing `T`, and then the temporary directory, after checking
    /// without following a symbolic link that `T` is a directory of this user
    /// with no permission bit for group or others. What a failure leaves
    /// goes with [`TaskFiles::remove`].
    pub fn create(&self) -> Result<(), StorageFailure> {
        files::create_private_dirs(self.directory.parent().unwrap_or(&self.directory))?;
        new_directory(&self.directory)?;
        for log in [&self.stdout, &self.stderr] {
            rustix::fs::open(
                &log.path,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(LOG_MODE),
            )
            .map_err(|error| IoFailure::new(&log.path, error))?;
        }
        let parent = self.temporary.parent().unwrap_or(&self.temporary);
        match DirBuilder::new().mode(DIRECTORY_MODE).create(parent) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => {
                return Err(IoFailure::new(parent, error).into());
            }
            _ => {}
        }
        if !is_private_directory(parent) {
            return Err(StorageFailure::UnsafeParent(parent.to_path_buf()));
        }
        new_directory(&self.temporary)?;
        Ok(())
    }

    /// Deletes the temporary directory and then the task's directory, each
    /// where it exists, never following a symbolic link out of them.
    pub fn remove(&self) -> Result<(), IoFailure> {
        for directory in [&self.temporary, &self.directory] {
            match fs::remove_dir_all(directory) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => {
                    return Err(IoFailure::new(directory, error));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// One stream's log: the stream's first bytes as they arrive, then, once the
/// task has ended, the capture's stored text. It is opened for each write and
/// closed after it, never created again and never through a symbolic link,
/// so a task holds no descriptor of its logs.
pub struct Log {
    path: PathBuf,
    room: usize,
}

impl Log {
    /// Appends what of `bytes` lies within the stream's first bytes.
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), IoFailure> {
        let part = &bytes[..bytes.len().min(self.room)];
        if part.is_empty() {
            return Ok(());
        }
        self.write(OFlags::APPEND, part)?;
        self.room -= part.len();
        Ok(())
    }

    /// Replaces the log's bytes with `text`.
    pub fn finish(&self, text: &str) -> Result<(), IoFailure> {
        self.write(OFlags::TRUNC, text.as_bytes())
    }

    fn write(&self, mode: OFlags, bytes: &[u8]) -> Result<(), IoFailure> {
        let failure = |error: io::Error| IoFailure::new(&self.path, error);
        let file = rustix::fs::open(
            &self.path,
            OFlags::WRONLY | mode | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| failure(error.into()))?;
        File::from(file).write_all(bytes).map_err(failure)
    }
}

/// Deletes every entry of `parent`, `T`, never following a symbolic link out
/// of it, when `T` is a directory of this user with no permission bit for
/// group or others; leaves any other `T` untouched. At an App Server's start
/// its entries are the temporary directories of an earlier process's tasks,
/// which no task names any more.
pub fn clear_task_parent(parent: &Path) {
    if is_private_directory(parent) {
        remove_entries(parent);
    }
}

/// Deletes every entry of `sessions/<session_id>/tasks/` under the canonical
/// state root, never following a symbolic link out of it: the directories an
/// earlier App Server's tasks of that Session left, once their processes are
/// confirmed gone.
pub fn clear_session_tasks(state_root: &Path, session_id: &str) {
    remove_entries(&state::paths::session_directory(state_root, session_id).join(TASKS));
}

fn remove_entries(directory: &Path) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let _ = match entry.file_type() {
            Ok(kind) if kind.is_dir() => fs::remove_dir_all(&path),
            _ => fs::remove_file(&path),
        };
    }
}

fn new_directory(path: &Path) -> Result<(), IoFailure> {
    DirBuilder::new()
        .mode(DIRECTORY_MODE)
        .create(path)
        .map_err(|error| IoFailure::new(path, error))
}

fn is_private_directory(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| {
        metadata.is_dir()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o077 == 0
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
