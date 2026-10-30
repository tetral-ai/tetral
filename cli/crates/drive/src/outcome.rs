//! The `drive_sync` of a result: which facts it carries, and its fitting
//! into 65,536 bytes of compact JSON with at most 100 entries per list,
//! entries added in the order `unmounted`, `rejected`, `not_synced`,
//! `downloading`, each list in UTF-8 byte order, and a total for each list
//! with entries left out. Drive sync reports are fitted with it too.

use protocol::field::{Count, List, PositiveInt};
use protocol::tool::{
    DRIVE_SYNC_LISTED_MAX, DriveDownload, DriveMountFailure, DriveNotSynced, DriveRejection,
    DriveSyncOutcome,
};

/// The most bytes of a `drive_sync`'s compact JSON.
pub const MAX_BYTES: usize = 65_536;

/// Everything one `drive_sync` may report.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    pub committed: u64,
    pub deleted: u64,
    pub rejected: Vec<DriveRejection>,
    pub not_synced: Vec<DriveNotSynced>,
    /// Only for a `running` result.
    pub downloading: Vec<DriveDownload>,
    /// Only for the first result of an `exec_command` or `view_image`.
    pub unmounted: Vec<DriveMountFailure>,
}

/// A fitted `drive_sync`, and the `unmounted` entries it holds, which are
/// then reported.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fitted {
    pub drive_sync: Option<DriveSyncOutcome>,
    pub placed: Vec<DriveMountFailure>,
}

/// The `drive_sync` of `facts`, or none when they hold nothing to report.
pub fn fit(mut facts: Facts) -> Fitted {
    let present = facts.committed > 0
        || facts.deleted > 0
        || !facts.rejected.is_empty()
        || !facts.not_synced.is_empty()
        || !facts.downloading.is_empty()
        || !facts.unmounted.is_empty();
    if !present {
        return Fitted::default();
    }
    facts
        .unmounted
        .sort_by(|a, b| a.drive_id.as_str().cmp(b.drive_id.as_str()));
    facts
        .rejected
        .sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
    facts
        .not_synced
        .sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
    facts
        .downloading
        .sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
    let lens = [
        facts.unmounted.len(),
        facts.rejected.len(),
        facts.not_synced.len(),
        facts.downloading.len(),
    ];
    let mut kept = [0usize; 4];
    'adding: for list in 0..4 {
        while kept[list] < lens[list].min(DRIVE_SYNC_LISTED_MAX) {
            kept[list] += 1;
            if size(&build(&facts, kept, lens)) > MAX_BYTES {
                kept[list] -= 1;
                break 'adding;
            }
        }
    }
    Fitted {
        drive_sync: Some(build(&facts, kept, lens)),
        placed: facts.unmounted[..kept[0]].to_vec(),
    }
}

/// The outcome with the first `kept` entries of each list, and the total of
/// each list with entries left out.
fn build(facts: &Facts, kept: [usize; 4], lens: [usize; 4]) -> DriveSyncOutcome {
    let total = |list: usize| {
        (kept[list] < lens[list])
            .then(|| PositiveInt::new(lens[list] as i64))
            .flatten()
    };
    DriveSyncOutcome {
        committed: Count::saturating(facts.committed),
        deleted: Count::saturating(facts.deleted),
        rejected: List(facts.rejected[..kept[1]].to_vec()),
        rejected_total: total(1),
        not_synced: List(facts.not_synced[..kept[2]].to_vec()),
        not_synced_total: total(2),
        downloading: (kept[3] > 0).then(|| List(facts.downloading[..kept[3]].to_vec())),
        downloading_total: total(3),
        unmounted: (kept[0] > 0).then(|| facts.unmounted[..kept[0]].to_vec()),
    }
}

fn size(outcome: &DriveSyncOutcome) -> usize {
    serde_json::to_vec(outcome).map_or(usize::MAX, |json| json.len())
}
