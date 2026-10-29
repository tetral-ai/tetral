//! Each test owns a short temporary parent for its control sockets. Related
//! child processes capture the same context at construction; in-process SDK
//! clients receive it explicitly. No test moves or removes the user's `S`.

use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs;
use std::marker::PhantomData;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use state::paths::TestSocketContext;

struct OwnedParent {
    paths: TestSocketContext,
    _directory: tempfile::TempDir,
}

/// A shared lifetime for one test's socket parent, captured by its children.
#[derive(Clone)]
pub struct SocketContext(Arc<OwnedParent>);

impl SocketContext {
    fn new() -> Self {
        let parent = if cfg!(target_os = "macos") {
            "/private/tmp"
        } else {
            "/tmp"
        };
        let directory = tempfile::Builder::new()
            .prefix("tt-")
            .tempdir_in(parent)
            .unwrap();
        let paths = TestSocketContext::new(directory.path()).unwrap();
        assert!(paths.socket_dir().starts_with(paths.parent()));
        Self(Arc::new(OwnedParent {
            paths,
            _directory: directory,
        }))
    }

    pub fn paths(&self) -> &TestSocketContext {
        &self.0.paths
    }

    pub fn socket_dir(&self) -> PathBuf {
        // Derive the expected uid directory independently of production code.
        self.paths().parent().join(format!(
            "tetral-sock-{}",
            rustix::process::getuid().as_raw()
        ))
    }
}

thread_local! {
    static CURRENT: RefCell<Weak<OwnedParent>> = const { RefCell::new(Weak::new()) };
}

/// The current test's context. A command without an explicit test scope gets
/// its own parent too, so it can never fall back to the user's socket path.
pub fn context() -> SocketContext {
    CURRENT.with(|current| {
        if let Some(parent) = current.borrow().upgrade() {
            return SocketContext(parent);
        }
        let context = SocketContext::new();
        *current.borrow_mut() = Arc::downgrade(&context.0);
        context
    })
}

pub fn socket_dir() -> PathBuf {
    context().socket_dir()
}

/// An isolated scope shared by all commands and clients of this test thread.
/// The guard stays on that thread; child processes own cloned contexts.
pub struct IsolatedSocketDir {
    context: SocketContext,
    previous: Weak<OwnedParent>,
    owned_by_root: bool,
    _thread: PhantomData<Rc<()>>,
}

pub fn isolated() -> IsolatedSocketDir {
    let context = SocketContext::new();
    let previous = CURRENT.with(|current| current.replace(Arc::downgrade(&context.0)));
    IsolatedSocketDir {
        context,
        previous,
        owned_by_root: false,
        _thread: PhantomData,
    }
}

impl IsolatedSocketDir {
    pub fn path(&self) -> PathBuf {
        let path = self.context.socket_dir();
        assert_eq!(path.parent(), Some(self.context.paths().parent()));
        path
    }

    /// The test's `S`, owned by the current user with `mode`.
    pub fn directory_with_mode(&self, mode: u32) {
        let path = self.path();
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    }

    pub fn link_to(&self, target: &Path) {
        symlink(target, self.path()).unwrap();
    }

    /// Creates only this scope's directory as root, for the owner-rejection
    /// case. The tested process itself continues to run as the invoking user.
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

impl Drop for IsolatedSocketDir {
    fn drop(&mut self) {
        if self.owned_by_root {
            let _ = Command::new("sudo")
                .args(["-n", "rmdir"])
                .arg(self.path())
                .status();
        }
        CURRENT.with(|current| *current.borrow_mut() = self.previous.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_and_parallel_scopes_have_distinct_owned_parents() {
        let first = isolated();
        let first_context = context();
        assert_eq!(first.path(), first_context.paths().socket_dir());
        {
            let second = isolated();
            assert_ne!(second.path(), first.path());
            assert_eq!(socket_dir(), second.path());
        }
        assert_eq!(socket_dir(), first.path());
        let other = std::thread::spawn(|| {
            let scope = isolated();
            (scope.path(), context())
        })
        .join()
        .unwrap();
        assert_ne!(first.path(), other.0);
        assert!(other.1.paths().parent().is_dir());
    }
}
