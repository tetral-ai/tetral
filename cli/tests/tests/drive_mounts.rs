//! Drive mounts seen from the reference control server: an App Server whose
//! `/dev` has no `fuse` advertises no mounting and reports every bound
//! drive as a mount failure, and a Session whose sync state is of another
//! version reports its drives once per App Server process while another
//! Session mounts its drives.
#![cfg(target_os = "linux")]

use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, wait_for_received,
};
use tetral_tests::sessions::{new_connection, online_machine, prepare, reports_for};
use tetral_tests::socket_directory;

fn payload(session: &str, binding: &str, cwd: &Path, drives: &[&str]) -> Value {
    let drives: Vec<Value> = drives
        .iter()
        .map(|drive| json!({ "drive_id": drive, "access": "read_write" }))
        .collect();
    json!({
        "session_id": session,
        "binding_id": binding,
        "cwd": cwd,
        "drives": drives,
        "drive_exclude": [],
        "repositories": [],
    })
}

/// The `drive_mount_failures` of the report answering `message_id`, as
/// `(drive_id, error)` pairs; none when the report has no such member.
fn mount_failures(
    engine: &tetral_tests::reference::ReferenceServer,
    id: &str,
) -> Vec<(String, String)> {
    let mut reports = Vec::new();
    tetral_tests::app_server::wait_until(&format!("the report answering {id}"), || {
        reports = reports_for(engine, id);
        !reports.is_empty()
    });
    assert_eq!(reports[0]["status"], "prepared", "{}", reports[0]);
    reports[0]["drive_mount_failures"]
        .as_array()
        .map(|failures| {
            failures
                .iter()
                .map(|failure| {
                    (
                        failure["drive_id"].as_str().unwrap().to_owned(),
                        failure["error"].as_str().unwrap().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Lazily unmounts what the App Server left mounted under a directory once
/// it is killed.
struct Unmount(std::path::PathBuf);

impl Drop for Unmount {
    fn drop(&mut self) {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for point in mountinfo.lines().filter_map(|line| line.split(' ').nth(4)) {
            if Path::new(point).starts_with(&self.0) {
                let _ = std::process::Command::new("fusermount3")
                    .args(["-u", "-z", point])
                    .status();
            }
        }
    }
}

#[test]
fn an_app_server_without_fuse_reports_every_bound_drive_with_the_reason() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("an App Server started under a /dev without fuse");
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let arguments = ["--die-with-parent", "--dev-bind", "/", "/", "--dev", "/dev"].map(Into::into);
    let machine = Machine::start_under(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        "bwrap",
        &arguments,
    );
    machine.wait_for("online");
    let ready = wait_for_received(&engine, "ready", 1);
    assert_eq!(
        ready[0]["body"]["capabilities"]["drives"],
        json!({ "mount": false })
    );
    case.observe("ready's capabilities.drives is {\"mount\": false}");

    let cwd = directory.path().join("work");
    std::fs::create_dir(&cwd).unwrap();
    let id = prepare(
        &engine,
        &payload("sess_a", "bind_a", &cwd, &["drive_a", "drive_b"]),
    );
    let failures = mount_failures(&engine, &id);
    let drives: Vec<&str> = failures.iter().map(|(drive, _)| drive.as_str()).collect();
    assert_eq!(drives, ["drive_a", "drive_b"]);
    for (_, error) in &failures {
        assert!(error.starts_with("no FUSE: "), "{error}");
    }
    assert!(!machine.root.join("sessions/sess_a/drives").exists());
    case.observe("the report lists drive_a and drive_b with errors starting 'no FUSE: '");
    case.pass();
}

#[test]
fn a_sync_state_of_another_version_is_reported_once_and_other_sessions_mount() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a Session whose sync-state.db holds user_version 2, prepared twice, and another Session",
    );
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let _unmount = Unmount(directory.path().canonicalize().unwrap());
    let machine = online_machine(&mut case, &engine, directory.path());
    let session = machine.root.join("sessions/sess_v2");
    state::files::create_private_dirs(&session).unwrap();
    let database = session.join("sync-state.db");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        drive::state::SyncState::create(&database)
            .await
            .unwrap()
            .close()
            .await;
    });
    // `user_version` is the big-endian integer at byte 60 of the header.
    let mut bytes = std::fs::read(&database).unwrap();
    bytes[60..64].copy_from_slice(&2u32.to_be_bytes());
    std::fs::write(&database, &bytes).unwrap();
    let cwd = directory.path().join("work");
    std::fs::create_dir(&cwd).unwrap();

    let first = prepare(
        &engine,
        &payload("sess_v2", "bind_a", &cwd, &["drive_a", "drive_b"]),
    );
    let refused = "sync state: version 2 is not supported".to_owned();
    assert_eq!(
        mount_failures(&engine, &first),
        [
            ("drive_a".to_owned(), refused.clone()),
            ("drive_b".to_owned(), refused)
        ]
    );
    assert!(!session.join("drives").exists(), "nothing is mounted");
    case.observe("the first preparation lists both drives with the version error");

    new_connection(&engine, &machine);
    let second = prepare(
        &engine,
        &payload("sess_v2", "bind_b", &cwd, &["drive_a", "drive_b"]),
    );
    assert!(mount_failures(&engine, &second).is_empty());
    assert_eq!(
        std::fs::read(&database).unwrap(),
        bytes,
        "the database is untouched"
    );
    case.observe("the second preparation lists none; the database is unchanged");

    let other = prepare(&engine, &payload("sess_ok", "bind_c", &cwd, &["drive_a"]));
    assert!(mount_failures(&engine, &other).is_empty());
    case.observe("another Session's preparation lists no mount failure");
    case.pass();
}
