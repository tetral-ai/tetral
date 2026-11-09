//! Which `session` content files a sync would make more durable. A write or
//! a truncation bumps the file's write generation before it changes the
//! file, and a successful flush or `fsync` records the generation its sync
//! began at. A sync is skipped only when the current generation is the one
//! last recorded: a file this owner has not synced, as every file is after a
//! start and every created file is, is synced, and a write that lands while
//! a sync runs makes the next one run. A file's generations are forgotten
//! when its content file is removed, so that the record holds only files
//! that have one, and when its drive leaves the Session, so that a file ID
//! used again starts unsynced.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::sync::Arc;

/// Makes a content file's data and metadata durable: [`File::sync_all`], or
/// a test's stand-in.
pub(crate) type Syncer = Arc<dyn Fn(&File) -> io::Result<()> + Send + Sync>;

pub(crate) fn disk() -> Syncer {
    Arc::new(File::sync_all)
}

/// The write generations of the content files, by drive and file ID.
#[derive(Default)]
pub(crate) struct ContentSyncs(HashMap<(String, i64), Generations>);

#[derive(Default, Clone, Copy)]
struct Generations {
    written: u64,
    synced: Option<u64>,
}

impl ContentSyncs {
    /// Called by a write or a truncation before it changes the content file.
    pub(crate) fn wrote(&mut self, drive: &str, file_id: i64) {
        self.0
            .entry((drive.to_owned(), file_id))
            .or_default()
            .written += 1;
    }

    /// The generation that a sync begun now makes durable, or `None` when
    /// the last successful sync already made it durable.
    pub(crate) fn unsynced(&self, drive: &str, file_id: i64) -> Option<u64> {
        let generations = self
            .0
            .get(&(drive.to_owned(), file_id))
            .copied()
            .unwrap_or_default();
        (generations.synced != Some(generations.written)).then_some(generations.written)
    }

    /// A sync begun at `generation` succeeded.
    pub(crate) fn synced(&mut self, drive: &str, file_id: i64, generation: u64) {
        self.0
            .entry((drive.to_owned(), file_id))
            .or_default()
            .synced = Some(generation);
    }

    /// The file's content file was removed.
    pub(crate) fn forget(&mut self, drive: &str, file_id: i64) {
        self.0.remove(&(drive.to_owned(), file_id));
    }

    /// The drive left the Session.
    pub(crate) fn forget_drive(&mut self, drive: &str) {
        self.0.retain(|(each, _), _| each != drive);
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::ContentSyncs;

    // The owner runs a sync inline, so no write of its own lands during one;
    // the generation a sync began at is what it may record.
    #[test]
    fn a_write_during_a_sync_leaves_the_file_unsynced() {
        let mut syncs = ContentSyncs::default();
        syncs.wrote("drive_a", 1);
        let begun = syncs.unsynced("drive_a", 1).unwrap();
        syncs.wrote("drive_a", 1);
        syncs.synced("drive_a", 1, begun);
        assert!(syncs.unsynced("drive_a", 1).is_some());
    }
}
