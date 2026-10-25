//! Paths that another user owns, for the cases that need one and no
//! privilege.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// A directory that root already owns and the current user may look up,
/// `/root` on Linux and `/var/root` on macOS, to stand as a state root owned
/// by another user; the reason when there is none, such as a test that runs
/// as root, which owns it.
pub fn directory() -> Result<&'static Path, String> {
    let path = Path::new(if cfg!(target_os = "macos") {
        "/var/root"
    } else {
        "/root"
    });
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.is_dir() {
        return Err(format!("{} is not a directory", path.display()));
    }
    if metadata.uid() == rustix::process::geteuid().as_raw() {
        return Err(format!(
            "the test runs as the owner of {}, so it is no other user's",
            path.display()
        ));
    }
    Ok(path)
}
