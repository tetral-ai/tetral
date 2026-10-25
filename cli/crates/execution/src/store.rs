//! Where a task's files live on this machine. Each task is named during its
//! admission, before its policy is translated, so that its profile already
//! names its private temporary directory; that directory lies in `T`, the
//! parent of every task's temporary directory, kept short so that a Unix
//! socket below `TMPDIR` fits the platforms' socket path limits. The paths
//! are only computed here: nothing is created or checked.

use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
