//! `$TETRAL_HOME/lost-changes.json`: the `lost_changes` entries of final
//! uploads that Engine has not acknowledged, a JSON array in the order they
//! were kept. The Connection owner alone reads and writes it, each change
//! through [`state::files::replace`]. An entry goes when the Report outbox
//! confirms its report final, or when Engine acknowledges a ready built
//! while it was kept; until then every ready lists it.
//!
//! A failed replacement is recorded and the list in memory still counts, so
//! only a restart before the acknowledgement loses those entries. A file
//! unreadable at start is recorded and the list starts empty.

use std::path::PathBuf;

use protocol::connection::LostChange;
use protocol::id::Id;
use state::paths::StateRoot;

/// The name of the file in the state root.
pub const FILE: &str = "lost-changes.json";

/// The largest file read: far above any number of entries kept in practice.
const READ_MAX_BYTES: usize = 64 << 20;

pub(crate) struct KeptLostChanges {
    path: PathBuf,
    entries: Vec<LostChange>,
}

impl KeptLostChanges {
    /// The entries the root's file keeps; none when it does not exist.
    pub(crate) fn load(root: &StateRoot) -> KeptLostChanges {
        let path = root.join(FILE);
        let entries = match state::files::read_bounded(&path, READ_MAX_BYTES) {
            Ok(None) => Vec::new(),
            Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                unreadable(0);
                Vec::new()
            }),
            Err(failure) => {
                unreadable(failure.source.raw_os_error().unwrap_or(0));
                Vec::new()
            }
        };
        KeptLostChanges { path, entries }
    }

    /// Every kept entry, in kept order.
    pub(crate) fn entries(&self) -> &[LostChange] {
        &self.entries
    }

    /// Appends a final upload's entries.
    pub(crate) fn keep(&mut self, entries: Vec<LostChange>) {
        self.entries.extend(entries);
        self.store();
    }

    /// The report answering `request_message_id` is final.
    pub(crate) fn report_acked(&mut self, request_message_id: &Id) {
        let before = self.entries.len();
        self.entries
            .retain(|entry| entry.request_message_id != *request_message_id);
        if self.entries.len() != before {
            self.store();
        }
    }

    /// A ready built with `listed`, the entries kept then, was
    /// acknowledged: each of them goes, those its fitting left out included.
    pub(crate) fn ready_acked(&mut self, listed: &[LostChange]) {
        if listed.is_empty() {
            return;
        }
        for entry in listed {
            if let Some(index) = self.entries.iter().position(|kept| kept == entry) {
                self.entries.remove(index);
            }
        }
        self.store();
    }

    fn store(&self) {
        let bytes = serde_json::to_vec(&self.entries).expect("entries serialize");
        if let Err(failure) = state::files::replace(&self.path, &bytes) {
            observe::error!(
                "lost_changes.not_saved",
                error.os_code = failure.source.raw_os_error().unwrap_or(0),
                error.code = observe::Code("lost_changes_not_saved")
            );
        }
    }
}

/// The file at start is unreadable or holds no entry array; the next
/// replacement overwrites it.
fn unreadable(os_code: i32) {
    observe::error!(
        "lost_changes.unreadable",
        error.os_code = os_code,
        error.code = observe::Code("lost_changes_unreadable")
    );
}
