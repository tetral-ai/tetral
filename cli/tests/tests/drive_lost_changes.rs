//! Layer 2. The changes of a Session's drives that the real App Server
//! gives up, against the reference control server and through real mounts:
//! what its reports list as lost, and what it does with the Session's data.

use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{self, DRIVE, DriveMachine, READ_WRITE};
use tetral_tests::machine::INSTANCE;
use tetral_tests::tasks::{BINDING, SESSION};

const OTHER: &str = "drive_b";

/// Runs `write` on the mount of each of a Session's two drives, outside any
/// command, so that the App Server, whose clock stands, uploads nothing;
/// kills the App Server, starts it again and sends the Session's close on
/// the new connection, which holds no binding of the Session. Gives the
/// machine, the accepted `closed` report and the size of its request.
fn late_closed(
    case: &mut tetral_tests::record::Case,
    write: impl Fn(&Path),
) -> (DriveMachine, Value, u64) {
    let mut machine = DriveMachine::start(case, &[(DRIVE, READ_WRITE), (OTHER, READ_WRITE)]);
    write(&machine.mount(DRIVE));
    write(&machine.mount(OTHER));
    machine.kill_and_restart(|_| {});
    let issued = machine.engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": SESSION, "binding_id": BINDING },
        }),
    );
    let close = issued["message_id"].as_str().unwrap().to_owned();
    let report = machine.session_report(&close);
    assert_eq!(report["status"], "closed", "{report}");
    let sent: Vec<Value> = machine
        .engine
        .requests("session-status")
        .into_iter()
        .filter(|request| {
            request["message_id"] == report["message_id"]
                && request["connection_id"] == report["connection_id"]
        })
        .collect();
    assert_eq!(sent.len(), 1, "{sent:?}");
    let session = machine.root.join("sessions").join(SESSION);
    wait_until("the Session's data deleted", || !session.exists());
    for drive in [DRIVE, OTHER] {
        assert_eq!(
            drives::requests(&machine.engine, "upload_content", drive),
            Vec::<Value>::new()
        );
        assert!(drives::files(&machine.engine, drive).is_empty());
    }
    let bytes = sent[0]["body_bytes"].as_u64().expect("the report's size");
    (machine, report, bytes)
}

/// The report's `lost_changes` entry of `drive`, which must be its only one
/// and name the close.
fn lost<'a>(report: &'a Value, drive: &str) -> &'a Value {
    let entries: Vec<&Value> = report["lost_changes"]
        .as_array()
        .expect("lost_changes")
        .iter()
        .filter(|entry| entry["drive_id"] == drive)
        .collect();
    assert_eq!(entries.len(), 1, "{report}");
    assert_eq!(
        entries[0]["request_message_id"], report["request_message_id"],
        "{report}"
    );
    entries[0]
}

#[test]
fn a_late_close_lists_each_drives_lost_changes_uploads_nothing_and_deletes_the_sessions_data() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a close that reaches a restarted App Server whose Session holds 2 changes not uploaded in one drive and 101 in another",
    );
    let many: Vec<String> = (0..100).map(|index| format!("n{index:02}")).collect();
    let (_machine, report, _) = late_closed(&mut case, |mount| {
        if mount.ends_with(DRIVE) {
            for name in ["a.txt", "Z.txt"] {
                std::fs::write(mount.join(name), "x").unwrap();
            }
        } else {
            for name in many.iter().map(String::as_str).chain(["ж.txt"]) {
                std::fs::write(mount.join(name), "x").unwrap();
            }
        }
    });
    assert_eq!(
        report["lost_changes"].as_array().map(Vec::len),
        Some(2),
        "{report}"
    );
    let few = lost(&report, DRIVE);
    assert_eq!(
        (&few["paths"], &few["paths_total"]),
        (&json!(["Z.txt", "a.txt"]), &json!(2)),
        "{report}"
    );
    let most = lost(&report, OTHER);
    assert_eq!(
        (&most["paths"], &most["paths_total"]),
        (&json!(many), &json!(101)),
        "{report}"
    );
    case.observe(
        "the closed report listed Z.txt and a.txt with paths_total 2 for one drive and n00 to n99, without ж.txt, with paths_total 101 for the other, each under the close; the App Server sent no upload and deleted the Session's data",
    );
    case.pass();
}

#[test]
fn a_late_closes_report_of_long_lost_paths_fits_in_64_kib() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a close that reaches a restarted App Server whose Session holds 100 changes with paths of 1,000 bytes in each of two drives",
    );
    let directory = format!("{}/", "x".repeat(240)).repeat(4);
    let name = "y".repeat(1_000 - directory.len() - 2);
    let paths: Vec<String> = (0..100)
        .map(|index| format!("{directory}{index:02}{name}"))
        .collect();
    let (_machine, report, bytes) = late_closed(&mut case, |mount| {
        // A Mac takes paths of at most 1,024 bytes: the files are made
        // from inside their directory.
        std::fs::create_dir_all(mount.join(&directory)).unwrap();
        let made = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "cd '{}' && cd '{directory}' && for i in $(seq -w 0 99); do printf x > \"${{i}}{name}\"; done",
                mount.display()
            ))
            .output()
            .unwrap();
        assert!(
            made.status.success(),
            "{}",
            String::from_utf8_lossy(&made.stderr)
        );
    });
    let entries = report["lost_changes"].as_array().expect("lost_changes");
    assert_eq!(entries.len(), 2, "{report}");
    let listed = entries[0]["paths"].as_array().expect("paths");
    assert!(
        !listed.is_empty()
            && listed.as_slice() == &json!(paths[..listed.len()]).as_array().unwrap()[..],
        "the first entry lists {} paths that are not the drive's first",
        listed.len()
    );
    assert_eq!(
        (
            &entries[0]["paths_total"],
            &entries[1]["paths"],
            &entries[1]["paths_total"]
        ),
        (&json!(100), &json!([]), &json!(100)),
        "{report}"
    );
    assert_ne!(entries[0]["drive_id"], entries[1]["drive_id"]);
    // One more path would add its quoted text and a comma.
    let next = u64::try_from(paths[listed.len()].len()).unwrap() + 3;
    assert!(
        bytes <= 65_536 && bytes + next > 65_536,
        "a closed report of {bytes} bytes with {} paths",
        listed.len()
    );
    case.observe(format!(
        "the closed report of {bytes} bytes listed the first {} of 100 paths for one drive and none for the other, both with paths_total 100",
        listed.len()
    ));
    case.pass();
}
