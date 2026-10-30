//! The fitting of `drive_sync` against the contract's boundary cases of
//! `protocol/tool-execution-v1.fixtures.json`'s `terminal_with_drive_sync`,
//! and the placement of mount failures and downloads in the answers of a
//! drive sync's `DriveSyncPort`: each output is validated as a result's
//! `drive_sync` by the protocol types.

mod support;

use drive::outcome::{Facts, MAX_BYTES, fit};
use protocol::tool::{
    DriveMountFailure, DriveNotSynced, DriveRejection, DriveSyncOutcome, ToolStatus,
};
use serde_json::Value;

fn fixture() -> Value {
    named("terminal_with_drive_sync")
}

fn named(name: &str) -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../protocol/tool-execution-v1.fixtures.json"
    );
    let fixtures: Vec<Value> = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    fixtures
        .into_iter()
        .find(|fixture| fixture["name"] == name)
        .unwrap()["value"]
        .clone()
}

/// The fixture's `drive_sync` as facts.
fn facts() -> Facts {
    let outcome: DriveSyncOutcome =
        serde_json::from_value(fixture()["result"]["drive_sync"].clone()).unwrap();
    Facts {
        committed: outcome.committed.get() as u64,
        deleted: outcome.deleted.get() as u64,
        rejected: outcome.rejected.0,
        not_synced: outcome.not_synced.0,
        ..Facts::default()
    }
}

fn rejection(path: String) -> DriveRejection {
    DriveRejection {
        path: protocol::field::AbsolutePath::new(path).unwrap(),
        ..facts().rejected[0].clone()
    }
}

/// `outcome` is valid as the fixture result's `drive_sync`; its size.
fn valid(outcome: &DriveSyncOutcome) -> usize {
    valid_in("terminal_with_drive_sync", outcome)
}

/// `outcome` is valid as the `drive_sync` of the fixture `name`'s result.
fn valid_in(name: &str, outcome: &DriveSyncOutcome) -> usize {
    let mut value = named(name);
    value["result"]["drive_sync"] = serde_json::to_value(outcome).unwrap();
    protocol::decode::<ToolStatus>(&serde_json::to_vec(&value).unwrap())
        .unwrap_or_else(|error| panic!("invalid drive_sync: {error}"));
    serde_json::to_vec(outcome).unwrap().len()
}

const PREFIX: &str = "/tetral/sessions/id_session_id/drives/id_drive_id/";

#[test]
fn rejections_past_one_hundred_are_counted_in_the_total() {
    let mut input = facts();
    // In reverse, so the first 100 in path order are the ones kept.
    input.rejected = (0..150)
        .rev()
        .map(|index| rejection(format!("{PREFIX}r{index:03}")))
        .collect();
    let outcome = fit(input).drive_sync.unwrap();
    valid(&outcome);
    assert_eq!(outcome.rejected.len(), 100);
    assert_eq!(outcome.rejected_total.map(|total| total.get()), Some(150));
    assert_eq!(outcome.rejected[99].path.as_str(), format!("{PREFIX}r099"));
}

#[test]
fn an_outcome_of_exactly_64_kib_is_kept_whole_and_one_byte_more_is_cut_with_totals() {
    let mut input = facts();
    let whole = fit(input.clone()).drive_sync.unwrap();
    let pad = MAX_BYTES - serde_json::to_vec(&whole).unwrap().len();
    let path = format!("{}{}", input.rejected[0].path.as_str(), "x".repeat(pad));
    input.rejected[0] = rejection(path.clone());
    let exact = fit(input.clone()).drive_sync.unwrap();
    assert_eq!(valid(&exact), MAX_BYTES);
    assert_eq!(exact.rejected_total, None);
    assert_eq!(exact.not_synced.len(), 1);

    input.rejected[0] = rejection(format!("{path}x"));
    let cut = fit(input).drive_sync.unwrap();
    assert!(valid(&cut) <= MAX_BYTES);
    // The prefix keeps the rejection; the `not_synced` entry gives way to
    // its total.
    assert_eq!(cut.rejected.len(), 1);
    assert_eq!(cut.rejected_total, None);
    assert!(cut.not_synced.is_empty());
    assert_eq!(cut.not_synced_total.map(|total| total.get()), Some(1));
}

#[test]
fn one_hundred_long_paths_keep_the_longest_prefix_that_fits() {
    let long_path = |index: usize| {
        let path = format!("{PREFIX}{}/{index:03}", vec!["a".repeat(255); 3].join("/"));
        format!("{path}{}", "b".repeat(1024 - path.len()))
    };
    let mut input = facts();
    input.rejected = (0..100).map(|index| rejection(long_path(index))).collect();
    let outcome = fit(input.clone()).drive_sync.unwrap();
    assert!(valid(&outcome) <= MAX_BYTES);
    // `drive_sync_long_paths_cut_to_fit`: the most entries whose outcome,
    // with its total, fits.
    let kept = outcome.rejected.len();
    assert!(kept < 100);
    assert_eq!(outcome.rejected_total.map(|total| total.get()), Some(100));
    let mut one_more = outcome.clone();
    one_more.rejected.0.push(input.rejected[kept].clone());
    one_more.not_synced.0.clear();
    assert!(serde_json::to_vec(&one_more).unwrap().len() > MAX_BYTES);
    assert!(outcome.not_synced.is_empty());
}

#[test]
fn a_path_longer_than_64_kib_leaves_totals_only_and_an_unplaced_mount_failure() {
    let mut input = facts();
    input.rejected = vec![rejection(format!("{PREFIX}{}", "x".repeat(MAX_BYTES)))];
    input.not_synced.clear();
    input.unmounted = vec![DriveMountFailure {
        drive_id: protocol::id::Id::new("id_drive_id").unwrap(),
        error: protocol::field::NonEmptyString::new("e".repeat(MAX_BYTES)).unwrap(),
    }];
    let fitted = fit(input);
    let outcome = fitted.drive_sync.unwrap();
    valid(&outcome);
    assert!(outcome.rejected.is_empty() && outcome.unmounted.is_none());
    assert_eq!(outcome.rejected_total.map(|total| total.get()), Some(1));
    assert!(
        fitted.placed.is_empty(),
        "a failure that does not fit stays new"
    );
}

#[test]
fn an_empty_outcome_gives_no_drive_sync_and_lists_follow_their_order() {
    assert_eq!(fit(Facts::default()).drive_sync, None);
    let input = Facts {
        not_synced: ["b", "a"]
            .map(|name| DriveNotSynced {
                path: protocol::field::AbsolutePath::new(format!("{PREFIX}{name}")).unwrap(),
                reason: protocol::tool::NotSyncedReason::Uploading,
            })
            .to_vec(),
        ..Facts::default()
    };
    let outcome = fit(input).drive_sync.unwrap();
    valid(&outcome);
    let paths: Vec<&str> = outcome
        .not_synced
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(paths, [format!("{PREFIX}a"), format!("{PREFIX}b")]);
}

fn point(first: bool, running: bool) -> protocol::ports::DriveSyncPoint {
    protocol::ports::DriveSyncPoint {
        task_id: protocol::id::Id::new("task_1").unwrap(),
        first,
        running,
        wait: std::time::Duration::ZERO,
        traceparent: None,
    }
}

/// A Session whose drive's mount check fails; with `listed`, the drive is
/// also listed and served, as by an earlier mount.
async fn failing_session(env: &support::Env, session: &str, listed: bool) -> support::Session {
    let session = env
        .session(session, drive::state::Write::default(), listed)
        .await;
    let bound = vec![drive::mount::Bound {
        drive_id: protocol::id::SafeId::new(support::DRIVE).unwrap(),
        read_only: false,
        exclude: Vec::new(),
    }];
    session.sync.mount_drives(bound).await;
    session
}

#[tokio::test]
async fn mount_failures_reach_only_the_first_result_and_downloads_only_running_ones() {
    use protocol::ports::{DriveCheck, DriveSyncPort};
    let env = support::Env::new();
    env.stub.list("big", "large content");
    env.stub
        .held_downloads
        .lock()
        .unwrap()
        .insert(support::sha256(b"large content"));
    let session = failing_session(&env, "sess_a", true).await;
    let reader = session.sync.clone();
    let drive = session.drive.clone();
    let ino = session
        .sync
        .lookup(&drive, drive::ops::TOP_INODE, b"big")
        .await
        .unwrap()
        .ino;
    tokio::spawn(async move { reader.read(&drive, ino, 0, 16).await });
    let port: &dyn DriveSyncPort = &session.sync;
    let task = protocol::id::Id::new("task_1").unwrap();

    let checked = port
        .check(DriveCheck {
            task_id: task,
            view_image: false,
        })
        .await;
    assert_eq!(checked.drive_sync, None);
    let unmounted: Vec<&str> = checked
        .unmounted
        .iter()
        .map(|drive| drive.as_str())
        .collect();
    assert_eq!(
        unmounted,
        [support::DRIVE],
        "the drive its check left unmounted"
    );
    // A stdin reply before the first report gets none of its failures.
    let reply = port
        .sync_point(point(false, true))
        .await
        .expect("a running stdin reply");
    valid_in("stdin_read_reply", &reply);
    assert!(reply.unmounted.is_none() && reply.downloading.is_some());
    let first = port
        .sync_point(point(true, true))
        .await
        .expect("the first running report");
    valid_in("running", &first);
    let unmounted = first.unmounted.as_ref().expect("the check's failure");
    assert_eq!(unmounted[0].drive_id.as_str(), support::DRIVE);
    assert_eq!(unmounted[0].error.as_str(), "no FUSE: test");
    let downloading = first.downloading.as_ref().expect("the held download");
    assert_eq!(downloading[0].path.as_str(), session.machine_path("big"));

    assert_eq!(
        port.sync_point(point(false, false)).await,
        None,
        "a finished result"
    );
}

#[tokio::test]
async fn a_view_image_result_carries_its_check_s_mount_failure_alone() {
    use protocol::ports::{DriveCheck, DriveSyncPort};
    let env = support::Env::new();
    let session = failing_session(&env, "sess_b", false).await;
    let port: &dyn DriveSyncPort = &session.sync;
    let check = DriveCheck {
        task_id: protocol::id::Id::new("task_2").unwrap(),
        view_image: true,
    };
    let outcome = port
        .check(check.clone())
        .await
        .drive_sync
        .expect("the view_image result's drive_sync");
    valid(&outcome);
    assert_eq!(outcome.unmounted.as_ref().map(Vec::len), Some(1));
    assert!(outcome.rejected.is_empty() && outcome.not_synced.is_empty());
    assert!(outcome.downloading.is_none());
    assert_eq!(port.check(check).await.drive_sync, None, "reported once");
}
