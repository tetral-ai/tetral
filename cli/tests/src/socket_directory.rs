//! The directory of control sockets, `S`, that every App Server of this user
//! on the machine shares. A test either uses `S` as it is or replaces it for
//! one case; tests in every process take a lock on a file beside `S`, shared
//! to use it and exclusive to replace it, so no test sees another's
//! replacement.

use std::ffi::OsStr;
use std::fs::{self, File};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;

use rustix::fs::FlockOperation;

/// `S` as the protocol of the local API derives it, independently of the
/// code under test: `/tmp/tetral-sock-<uid>` on Linux and
/// `/private/tmp/tetral-sock-<uid>` on macOS.
pub fn socket_dir() -> PathBuf {
    let parent = if cfg!(target_os = "macos") {
        "/private/tmp"
    } else {
        "/tmp"
    };
    PathBuf::from(format!(
        "{parent}/tetral-sock-{}",
        rustix::process::getuid().as_raw()
    ))
}

fn lock(operation: FlockOperation) -> File {
    let mut path = socket_dir().into_os_string();
    path.push(".tests.lock");
    let file = File::options()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|error| panic!("open {path:?}: {error}"));
    rustix::fs::flock(&file, operation).expect("flock on the socket directory's test lock");
    file
}

/// Permission to use `S` as it is, while held.
pub struct SharedSocketDir {
    _lock: File,
}

pub fn shared() -> SharedSocketDir {
    SharedSocketDir {
        _lock: lock(FlockOperation::LockShared),
    }
}

/// `S` moved aside while this is held, so that the test can put something
/// else at its path; dropping it removes that and puts `S` back.
pub struct ReplacedSocketDir {
    _lock: File,
    saved: Option<PathBuf>,
    owned_by_root: bool,
}

pub fn replaced() -> ReplacedSocketDir {
    let held = lock(FlockOperation::LockExclusive);
    let path = socket_dir();
    let saved = fs::symlink_metadata(&path).is_ok().then(|| {
        let mut saved = path.clone().into_os_string();
        saved.push(format!(".saved-{}", std::process::id()));
        let saved = PathBuf::from(saved);
        fs::rename(&path, &saved).unwrap_or_else(|error| panic!("move {path:?} aside: {error}"));
        saved
    });
    ReplacedSocketDir {
        _lock: held,
        saved,
        owned_by_root: false,
    }
}

impl ReplacedSocketDir {
    pub fn path(&self) -> PathBuf {
        socket_dir()
    }

    /// `S` as a directory of this user with `mode`.
    pub fn directory_with_mode(&self, mode: u32) {
        let path = self.path();
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    }

    /// `S` as a symbolic link to `target`.
    pub fn link_to(&self, target: &Path) {
        symlink(target, self.path()).unwrap();
    }

    /// `S` as a directory of mode `0700` owned by root, made with
    /// `sudo -n` (`other_user::sudo`); the reason when that is not possible
    /// here.
    pub fn directory_owned_by_root(&mut self) -> Result<(), String> {
        let path = self.path();
        crate::other_user::sudo(
            ["install", "-d", "-m", "0700", "-o", "0", "-g", "0"]
                .map(OsStr::new)
                .into_iter()
                .chain([path.as_os_str()]),
        )?;
        self.owned_by_root = true;
        Ok(())
    }
}

impl Drop for ReplacedSocketDir {
    fn drop(&mut self) {
        let path = self.path();
        if self.owned_by_root {
            let _ = Command::new("sudo")
                .args(["-n", "rmdir"])
                .arg(&path)
                .status();
        } else if let Ok(metadata) = fs::symlink_metadata(&path) {
            if metadata.is_dir() {
                let _ = fs::remove_dir_all(&path);
            } else {
                let _ = fs::remove_file(&path);
            }
        }
        if let Some(saved) = &self.saved {
            let _ = fs::rename(saved, &path);
        }
    }
}
