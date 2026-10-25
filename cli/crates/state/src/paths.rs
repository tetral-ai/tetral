//! The state root, the names in it, and the control socket's path.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The environment variable that names the state root.
pub const ROOT_VARIABLE: &str = "TETRAL_HOME";

/// The configuration: the app, Engine's URL, the instance and local options.
pub const CONFIG: &str = "config.toml";
/// The credential: the bootstrap object, connection key included.
pub const CREDENTIALS: &str = "credentials.json";
/// The directory of the lock.
pub const RUN: &str = "run";
/// The lock that the one process owning the root holds with `flock`; it is
/// never unlinked or replaced, so every process locks the same file.
pub const LOCK: &str = "run/app-server.lock";
/// The directory of `server run`'s log files.
pub const LOGS: &str = "logs";
/// The directory where the Report outbox freezes its reports (`outbox`).
pub const OUTBOX: &str = "outbox";

/// The state root as `TETRAL_HOME` names it. The CLI reaches the root's
/// control files through this form; the paths it builds for others start from
/// the canonical form (`canonical`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot(PathBuf);

impl StateRoot {
    /// The root that `value` names, or `None` unless it is an absolute path.
    /// It is rebuilt from its components, which drops a trailing `/` and
    /// every `.`: a check of a path that ends in either resolves a symbolic
    /// link at its last component.
    pub fn new(value: &OsStr) -> Option<StateRoot> {
        value
            .as_bytes()
            .starts_with(b"/")
            .then(|| StateRoot(Path::new(value).components().collect()))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    /// The path of `name`, one of this module's names, in the root.
    pub fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    /// The canonical path of the root, resolved now, or `None` when the root
    /// does not exist.
    pub fn canonical(&self) -> io::Result<Option<PathBuf>> {
        match self.0.canonicalize() {
            Ok(path) => Ok(Some(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(target_os = "linux")]
const SOCKET_PARENT: &str = "/tmp";
#[cfg(target_os = "macos")]
const SOCKET_PARENT: &str = "/private/tmp";

/// `S`, the directory of this user's control sockets:
/// `/tmp/tetral-sock-<uid>` on Linux and `/private/tmp/tetral-sock-<uid>` on
/// macOS, `<uid>` being the process's real user ID.
pub fn socket_dir() -> PathBuf {
    socket_dir_of(rustix::process::getuid().as_raw())
}

fn socket_dir_of(uid: u32) -> PathBuf {
    PathBuf::from(format!("{SOCKET_PARENT}/tetral-sock-{uid}"))
}

/// The bytes of the root's SHA-256 that name its socket, written as 16
/// hexadecimal characters.
const SOCKET_NAME_DIGEST_BYTES: usize = 8;

/// The App Server's socket for `root`, `S/<h>.sock`, where `<h>` is the first
/// 16 lowercase hexadecimal characters of the SHA-256 of the bytes of the
/// root's canonical path, resolved now; `None` when the root does not exist.
/// The path is at most 57 bytes, within every platform's socket path limit.
pub fn socket_path(root: &StateRoot) -> io::Result<Option<PathBuf>> {
    let Some(canonical) = root.canonical()? else {
        return Ok(None);
    };
    let uid = rustix::process::getuid().as_raw();
    Ok(Some(socket_path_of(uid, canonical.as_os_str().as_bytes())))
}

fn socket_path_of(uid: u32, canonical: &[u8]) -> PathBuf {
    let digest = Sha256::digest(canonical);
    let name: String = digest[..SOCKET_NAME_DIGEST_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    socket_dir_of(uid).join(format!("{name}.sock"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_socket_path_does_not_grow_with_the_root_or_the_user_id() {
        // The longest user ID and a 4,000-byte root; the name is the first 16
        // hexadecimal characters of the SHA-256 of the root's bytes.
        let root = format!("/{}", "r".repeat(3_999));
        assert_eq!(root.len(), 4_000);
        let path = socket_path_of(4_294_967_294, root.as_bytes());
        assert_eq!(
            path,
            PathBuf::from(format!(
                "{SOCKET_PARENT}/tetral-sock-4294967294/3182798be1f1fbe1.sock"
            ))
        );
        let length = path.as_os_str().len();
        assert_eq!(length, if cfg!(target_os = "linux") { 49 } else { 57 });
        assert!(length <= 57);
    }
}
