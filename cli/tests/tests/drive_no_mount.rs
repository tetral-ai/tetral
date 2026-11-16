//! Layer 2. The real App Server on a machine that cannot mount drives,
//! against the reference control server: what it tells the server and a
//! Session's commands, and what a start on which it can mount changes.

use serde_json::{Value, json};
use tetral_tests::drives::{DRIVE, DriveMachine, READ_WRITE, commit, in_drive, outcome, write};
use tetral_tests::machine::wait_for_received;

const OTHER: &str = "drive_b";

/// The Session that binds the two drives.
const SESSION: &str = "sess_drives";

/// The `(drive_id, error)` of each of a `prepared` report's mount failures.
fn mount_failures(report: &Value) -> Vec<(String, String)> {
    assert_eq!(report["status"], "prepared", "{report}");
    report["drive_mount_failures"]
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

#[test]
fn a_machine_that_cannot_mount_prepares_without_drives_until_a_start_on_which_it_can() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a Session with two drives on a machine that cannot mount: a Mac without tetral drives enable, or Linux whose fusermount3 fails as it does without /dev/fuse",
    );
    let mut machine = DriveMachine::start_unable_to_mount(&mut case);
    commit(&machine.engine, DRIVE, &[write("a.txt", "one")]);
    commit(&machine.engine, OTHER, &[write("b.txt", "two")]);
    let ready = wait_for_received(&machine.engine, "ready", 1);
    assert_eq!(
        ready[0]["body"]["capabilities"]["drives"],
        json!({ "mount": false })
    );
    let drives = [(DRIVE, READ_WRITE), (OTHER, READ_WRITE)];
    let prepare = |machine: &DriveMachine, binding: &str| {
        let issued = machine.prepare_with(SESSION, binding, &machine.project, &drives, &[]);
        let report = machine.session_report(&issued);
        machine.answers_settled();
        report
    };
    let failures = mount_failures(&prepare(&machine, "bind_1"));
    let reason = if cfg!(target_os = "macos") {
        "drives not enabled"
    } else {
        "no FUSE: "
    };
    assert!(
        failures.len() == 2
            && failures[0].0 == DRIVE
            && failures[1].0 == OTHER
            && failures.iter().all(|(_, error)| error.starts_with(reason)),
        "{failures:?}"
    );
    let variable = machine.run_in(
        SESSION,
        "bind_1",
        "variable",
        "echo \"${TETRAL_DRIVES-unset}\"",
    );
    assert_eq!(outcome(&variable).1, "unset\n", "{variable}");
    case.observe(format!(
        "ready's capabilities.drives is {{\"mount\": false}}; the preparation succeeded and lists both drives as mount failures: {failures:?}; TETRAL_DRIVES is unset in the Session's command"
    ));
    case.pass();

    let mut case = record.case(
        "the App Server started again on the machine once it can mount, and the Session prepared again",
    );
    machine.kill_and_restart(|machine| {
        if cfg!(target_os = "macos") {
            let run = tetral_tests::run::Tetral::new([
                "drives",
                "enable",
                "--app",
                tetral_tests::machine::APP,
            ])
            .home(&machine.root)
            .run();
            assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        }
    });
    let ready = wait_for_received(&machine.engine, "ready", 2);
    assert_eq!(
        ready[1]["body"]["capabilities"]["drives"],
        json!({ "mount": true })
    );
    let failures = mount_failures(&prepare(&machine, "bind_2"));
    assert!(failures.is_empty(), "{failures:?}");
    let read = machine.run_in(
        SESSION,
        "bind_2",
        "read",
        &format!(
            "cat {} {}",
            in_drive(DRIVE, "a.txt"),
            in_drive(OTHER, "b.txt")
        ),
    );
    let (_, stdout, sync) = outcome(&read);
    assert_eq!(stdout, "onetwo", "{read}");
    assert!(
        sync.is_null()
            || sync
                .get("unmounted")
                .is_none_or(|unmounted| *unmounted == json!([])),
        "{read}"
    );
    case.observe("the new ready's capabilities.drives is {\"mount\": true}; the preparation lists no mount failure and the Session's command read a file of each drive");
    case.pass();
}
