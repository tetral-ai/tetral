//! The state root, the names in it, and the control socket's path.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

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
/// The directory that holds one directory of files per Session.
pub const SESSIONS: &str = "sessions";

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

    /// Resolves existing components and projects only the directories that
    /// `configure` would create. Existing symbolic links and `..` are
    /// resolved by the filesystem; a `..` below a missing component leaves
    /// that prospective directory before resolution resumes. Nothing is
    /// created, including when an existing link has a missing target.
    pub fn resolved_for_creation(&self) -> io::Result<PathBuf> {
        let mut resolved = PathBuf::new();
        let mut missing = 0_usize;
        for component in self.0.components() {
            if missing > 0 {
                match component {
                    Component::ParentDir => {
                        resolved.pop();
                        missing -= 1;
                    }
                    Component::Normal(name) => {
                        resolved.push(name);
                        missing += 1;
                    }
                    _ => {}
                }
                continue;
            }
            resolved.push(component.as_os_str());
            match resolved.canonicalize() {
                Ok(path) => resolved = path,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    // An existing dangling link cannot be a directory that
                    // configure creates. Preserve its resolution failure.
                    match std::fs::symlink_metadata(&resolved) {
                        Err(missing_error) if missing_error.kind() == io::ErrorKind::NotFound => {
                            missing = 1;
                        }
                        _ => return Err(error),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(resolved)
    }
}

/// The directory of the Session `session_id` under the root's canonical path
/// `root`, `sessions/<session_id>/`. `session_id` is a `SafeId`, which holds
/// no `/` and is neither `.` nor `..`; no other identity names a directory.
pub fn session_directory(root: &Path, session_id: &str) -> PathBuf {
    root.join(SESSIONS).join(session_id)
}

#[cfg(target_os = "linux")]
const SOCKET_PARENT: &str = "/tmp";
#[cfg(target_os = "macos")]
const SOCKET_PARENT: &str = "/private/tmp";

/// `S`, the directory of this user's control sockets:
/// `/tmp/tetral-sock-<uid>` on Linux and `/private/tmp/tetral-sock-<uid>` on
/// macOS, `<uid>` being the process's real user ID.
pub fn socket_dir() -> PathBuf {
    #[cfg(feature = "test-seams")]
    if let Some(parent) = std::env::var_os(TEST_SOCKET_PARENT_VARIABLE) {
        return TestSocketContext::new(Path::new(&parent))
            .expect("the test socket parent must be an existing absolute directory")
            .socket_dir();
    }
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
    socket_path_in(root, &socket_dir())
}

fn socket_path_in(root: &StateRoot, directory: &Path) -> io::Result<Option<PathBuf>> {
    let Some(canonical) = root.canonical()? else {
        return Ok(None);
    };
    Ok(Some(socket_path_under(
        directory,
        canonical.as_os_str().as_bytes(),
    )))
}

#[cfg(test)]
fn socket_path_of(uid: u32, canonical: &[u8]) -> PathBuf {
    socket_path_under(&socket_dir_of(uid), canonical)
}

fn socket_path_under(directory: &Path, canonical: &[u8]) -> PathBuf {
    let digest = Sha256::digest(canonical);
    let name: String = digest[..SOCKET_NAME_DIGEST_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    directory.join(format!("{name}.sock"))
}

/// The test build's socket-parent override, supplied only to child processes.
#[cfg(feature = "test-seams")]
pub const TEST_SOCKET_PARENT_VARIABLE: &str = "TETRAL_TEST_SOCKET_PARENT";

/// An explicit test context for in-process clients. It changes only the
/// parent of `S`; the uid directory, root digest and permission checks stay
/// the same. Production builds expose neither this type nor the override.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone)]
pub struct TestSocketContext {
    parent: PathBuf,
}

#[cfg(feature = "test-seams")]
impl TestSocketContext {
    pub fn new(parent: &Path) -> io::Result<Self> {
        if !parent.is_absolute() || !parent.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the test socket parent must be an existing absolute directory",
            ));
        }
        let parent = parent.canonicalize()?;
        // The longest uid and socket name must fit macOS's sockaddr_un too.
        if parent.as_os_str().len() + "/tetral-sock-4294967295/0000000000000000.sock".len() >= 104 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the test socket parent is too long",
            ));
        }
        Ok(Self { parent })
    }

    pub fn parent(&self) -> &Path {
        &self.parent
    }

    pub fn socket_dir(&self) -> PathBuf {
        self.parent.join(format!(
            "tetral-sock-{}",
            rustix::process::getuid().as_raw()
        ))
    }

    pub fn socket_path(&self, root: &StateRoot) -> io::Result<Option<PathBuf>> {
        socket_path_in(root, &self.socket_dir())
    }
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
