//! Paths that another user owns, for the cases that need one and no
//! privilege.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// A directory that root already owns and the current user may look up,
/// `/root` on Linux and `/var/root` on macOS, to stand as a state root owned
/// by another user; `None` when the test runs as root, which owns it.
pub fn directory() -> Option<&'static Path> {
    let path = Path::new(if cfg!(target_os = "macos") {
        "/var/root"
    } else {
        "/root"
    });
    let metadata = fs::symlink_metadata(path).ok()?;
    (metadata.is_dir() && metadata.uid() != rustix::process::geteuid().as_raw()).then_some(path)
}
