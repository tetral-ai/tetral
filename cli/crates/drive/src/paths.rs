//! Drive paths as the mounts name them: names converted to NFC, the checks
//! on a path a process creates or renames to, excluded paths and the
//! local-only rules.

use rustix::io::Errno;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;

/// The folder names whose entries are never synced; the list is fixed.
pub const IGNORED: [&str; 3] = ["node_modules", ".venv", "__pycache__"];

/// The most non-starters in a row that the Stream-Safe Text Format allows.
const NON_STARTERS_MAX: usize = 30;

/// `name` in NFC; `EINVAL` when it is not valid UTF-8.
pub fn nfc_name(name: &[u8]) -> Result<String, Errno> {
    let name = std::str::from_utf8(name).map_err(|_| Errno::INVAL)?;
    Ok(name.nfc().collect())
}

/// The path of `name` in the folder `parent`.
pub fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

/// The parent folder of `path`; the top folder's parent is none.
pub fn parent(path: &str) -> Option<&str> {
    if path.is_empty() {
        return None;
    }
    Some(path.rsplit_once('/').map_or("", |(parent, _)| parent))
}

/// The folders above `path`, from the top folder down.
pub fn ancestors(path: &str) -> Vec<&str> {
    let mut each = Vec::new();
    let mut at = parent(path);
    while let Some(folder) = at {
        each.push(folder);
        at = parent(folder);
    }
    each.reverse();
    each
}

/// Whether `path` is `folder` or lies below it.
pub fn at_or_below(path: &str, folder: &str) -> bool {
    folder.is_empty()
        || path == folder
        || path
            .strip_prefix(folder)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// The path checks on a path in NFC that a process creates or renames to:
/// `EINVAL` when it breaks the drive path rules or is not in the
/// Stream-Safe Text Format. Case folding and files above or below other
/// files are left to Engine.
pub fn check(path: &str) -> Result<(), Errno> {
    protocol::path::path_rule(path).map_err(|_| Errno::INVAL)?;
    if !stream_safe(path) {
        return Err(Errno::INVAL);
    }
    Ok(())
}

/// No run of more than 30 non-starters in the NFKD form of `text`.
fn stream_safe(text: &str) -> bool {
    let mut run = 0;
    for character in text.nfkd() {
        if canonical_combining_class(character) == 0 {
            run = 0;
        } else {
            run += 1;
            if run > NON_STARTERS_MAX {
                return false;
            }
        }
    }
    true
}

/// Whether `path` equals an entry of `exclude`, written without its
/// trailing `/`, or lies below it.
pub fn excluded(path: &str, exclude: &[String]) -> bool {
    exclude.iter().any(|entry| {
        let entry = entry.trim_end_matches('/');
        !entry.is_empty() && at_or_below(path, entry)
    })
}

/// The error of creating at or renaming to an excluded `path`: `EACCES` at
/// an `exclude` entry itself, and `ENOENT` below one, since the excluded
/// folder does not exist; `None` for a path no entry excludes.
pub fn exclusion_error(path: &str, exclude: &[String]) -> Option<Errno> {
    let mut below = false;
    for entry in exclude {
        let entry = entry.trim_end_matches('/');
        if entry.is_empty() || !at_or_below(path, entry) {
            continue;
        }
        if path == entry {
            return Some(Errno::ACCESS);
        }
        below = true;
    }
    below.then_some(Errno::NOENT)
}

/// Why the entry of `kind` at `path` is local-only, or `None` for a synced
/// entry; `holds` tells whether a path holds a present entry.
pub fn local_only(path: &str, kind: &str, holds: impl Fn(&str) -> bool) -> Option<&'static str> {
    match kind {
        "symlink" => return Some("symlink"),
        "special" => return Some("special_file"),
        _ => {}
    }
    let mut folders = ancestors(path);
    if kind == "folder" {
        folders.push(path);
    }
    let ignored = folders.iter().any(|folder| {
        folder
            .rsplit('/')
            .next()
            .is_some_and(|name| IGNORED.contains(&name))
    });
    if ignored {
        return Some("ignored");
    }
    if folders.iter().any(|folder| holds(&join(folder, ".git"))) {
        return Some("git_repository");
    }
    None
}
