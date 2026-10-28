//! A snapshot of a directory tree: every path with its type, owner, mode,
//! and its bytes or link target, so a test can show that a command changed
//! nothing.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub kind: &'static str,
    pub uid: u32,
    pub mode: u32,
    pub content: Option<Vec<u8>>,
}

pub type Snapshot = BTreeMap<PathBuf, Entry>;

/// The tree at `root`, not following symbolic links; an absent root gives
/// an empty snapshot, and a directory this user cannot read is recorded
/// without its entries.
pub fn snapshot(root: &Path) -> Snapshot {
    let mut entries = Snapshot::new();
    walk(root, &mut entries);
    entries
}

fn walk(path: &Path, entries: &mut Snapshot) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    let file_type = metadata.file_type();
    let (kind, content) = if file_type.is_symlink() {
        (
            "link",
            fs::read_link(path)
                .ok()
                .map(|target| target.into_os_string().into_encoded_bytes()),
        )
    } else if file_type.is_dir() {
        ("directory", None)
    } else if file_type.is_file() {
        ("file", fs::read(path).ok())
    } else {
        ("other", None)
    };
    entries.insert(
        path.to_path_buf(),
        Entry {
            kind,
            uid: metadata.uid(),
            mode: metadata.mode() & 0o7777,
            content,
        },
    );
    if file_type.is_dir()
        && let Ok(children) = fs::read_dir(path)
    {
        for child in children.flatten() {
            walk(&child.path(), entries);
        }
    }
}
