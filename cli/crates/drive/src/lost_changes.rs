//! `lost_changes` entries: one per drive with an abandoned change, in UTF-8
//! byte order of `drive_id`, each naming the first 100 drive-relative paths
//! in UTF-8 byte order and counting all of them; and their fitting into the
//! 65,536 bytes of the report or ready that carries them.

use std::collections::{BTreeMap, BTreeSet};

use protocol::connection::{LOST_CHANGE_PATHS_MAX, LostChange, LostChanges};
use protocol::field::{List, PositiveInt, Unique};
use protocol::id::Id;
use protocol::path::DrivePath;

/// The most bytes of the compact JSON body that carries `lost_changes`.
pub const BODY_MAX_BYTES: usize = protocol::connection::MESSAGE_MAX_BYTES;

/// The entries of the changes abandoned for `request_message_id`, given as
/// `(drive_id, path)`.
pub fn entries(
    request_message_id: &Id,
    abandoned: impl IntoIterator<Item = (String, String)>,
) -> Vec<LostChange> {
    let mut drives: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (drive, path) in abandoned {
        drives.entry(drive).or_default().insert(path);
    }
    // `BTreeMap` and `BTreeSet` of `String` order by UTF-8 bytes.
    drives
        .into_iter()
        .filter_map(|(drive, paths)| {
            Some(LostChange {
                request_message_id: request_message_id.clone(),
                drive_id: Id::new(drive).ok()?,
                paths_total: PositiveInt::new(paths.len() as i64)?,
                paths: Unique(
                    paths
                        .into_iter()
                        .filter_map(|path| DrivePath::new(path).ok())
                        .take(LOST_CHANGE_PATHS_MAX)
                        .collect(),
                ),
            })
        })
        .collect()
}

/// The `lost_changes` of `entries` that keeps the body within
/// [`BODY_MAX_BYTES`]; `size` gives the body's compact JSON bytes with a
/// candidate list. Every entry with empty `paths` is added in entry order
/// while the body fits, an entry that does not fit leaving out every later
/// one; then the paths of each kept entry, in entry order and path order,
/// until the first that does not fit. `None` when no entry fits.
pub fn fit(
    entries: &[LostChange],
    size: impl Fn(Option<&LostChanges>) -> usize,
) -> Option<LostChanges> {
    let fits = |list: &LostChanges| size(Some(list)) <= BODY_MAX_BYTES;
    let mut kept = List(Vec::new());
    for entry in entries {
        kept.0.push(LostChange {
            paths: Unique(Vec::new()),
            ..entry.clone()
        });
        if !fits(&kept) {
            kept.0.pop();
            break;
        }
    }
    'paths: for (index, entry) in entries.iter().enumerate().take(kept.0.len()) {
        for path in entry.paths.iter() {
            kept.0[index].paths.0.push(path.clone());
            if !fits(&kept) {
                kept.0[index].paths.0.pop();
                break 'paths;
            }
        }
    }
    (!kept.0.is_empty()).then_some(kept)
}
