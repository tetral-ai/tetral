//! Layer 2. The mount points of a Session's drives on the real App Server
//! against the reference control server: what a start finds where a killed
//! App Server left them, what a command gets of a drive unmounted outside
//! the CLI, how often a mount point that cannot be used is reported, and
//! which drives a Session's commands see.

use std::path::Path;

use serde_json::Value;
use tetral_tests::drives::{DRIVE, DriveMachine, READ_ONLY, commit, in_drive, outcome, write};
use tetral_tests::tasks::SESSION;

const OTHER: &str = "drive_b";
const THIRD: &str = "drive_c";

/// Whether something is mounted at `point`.
fn mounted(point: &Path) -> bool {
    let (table, field): (String, fn(&str) -> Option<&str>) = if cfg!(target_os = "linux") {
        (
            std::fs::read_to_string("/proc/self/mountinfo").unwrap(),
            |line| line.split(' ').nth(4),
        )
    } else {
        let listed = std::process::Command::new("/sbin/mount").output().unwrap();
        (
            String::from_utf8_lossy(&listed.stdout).into_owned(),
            |line| {
                line.split(" on ")
                    .nth(1)
                    .and_then(|rest| rest.rsplit_once(" ("))
                    .map(|(point, _)| point)
            },
        )
    };
    table
        .lines()
        .filter_map(field)
        .any(|listed| Path::new(listed) == point)
}

/// Unmounts `point` as a user of the machine would.
fn unmount_outside(point: &Path) {
    let (program, arguments): (&str, &[&str]) = if cfg!(target_os = "linux") {
        ("fusermount3", &["-u", "-z"])
    } else {
        ("/sbin/umount", &["-f"])
    };
    let run = std::process::Command::new(program)
        .args(arguments)
        .arg(point)
        .output()
        .unwrap();
    assert!(
        run.status.success() && !mounted(point),
        "unmounting {}: {}",
        point.display(),
        String::from_utf8_lossy(&run.stderr)
    );
}

/// Unmounts `point` and puts a file where the directory was, so that
/// nothing can be mounted there.
fn block(point: &Path) {
    unmount_outside(point);
    std::fs::remove_dir(point).unwrap();
    std::fs::write(point, "").unwrap();
}

/// The `drive_mount_failures` of a Session report, as the drives it names.
fn mount_failures(report: &Value) -> Vec<String> {
    report["drive_mount_failures"]
        .as_array()
        .map(|failures| {
            failures
                .iter()
                .map(|failure| failure["drive_id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// The drives a result's `drive_sync` lists as unmounted.
fn unmounted(terminal: &Value) -> Vec<String> {
    outcome(terminal).2["unmounted"]
        .as_array()
        .map(|failures| {
            failures
                .iter()
                .map(|failure| failure["drive_id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Prepares the Session again as `binding` with `drives` read-only, as the
/// server does on a new connection, and gives the `prepared` report.
fn prepared_again(machine: &DriveMachine, binding: &str, drives: &[&str]) -> Value {
    let bound: Vec<(&str, &str)> = drives.iter().map(|drive| (*drive, READ_ONLY)).collect();
    let issued = machine.prepare_with(SESSION, binding, &machine.project, &bound, &[]);
    let report = machine.session_report(&issued);
    assert_eq!(report["status"], "prepared", "{report}");
    machine.answers_settled();
    report
}

#[test]
fn a_start_mounts_over_what_a_killed_app_server_left_and_reports_a_blocked_mount_point_once() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "an App Server killed with three drives mounted; before the next start one mount is left as it is, one mount point holds a file and one is replaced by a file",
    );
    let (covered, stale, blocked) = (DRIVE, OTHER, THIRD);
    let drives = [covered, stale, blocked];
    let mut machine = DriveMachine::start_seeded(
        &mut case,
        &drives.map(|drive| (drive, READ_ONLY)),
        |engine| {
            for drive in drives {
                commit(engine, drive, &[write("f.txt", drive)]);
            }
        },
    );
    let reads = format!(
        "cat {}; echo; ls {}; cat {}; echo; cat {} 2>/dev/null || echo unavailable",
        in_drive(covered, "f.txt"),
        in_drive(covered, ""),
        in_drive(stale, "f.txt"),
        in_drive(blocked, "f.txt")
    );
    assert_eq!(
        machine.output(&reads),
        format!("{covered}\nf.txt\n{stale}\n{blocked}")
    );
    let point = |drive: &str| machine.mount(drive);
    let (covered_point, stale_point, blocked_point) =
        (point(covered), point(stale), point(blocked));
    machine.kill_and_restart(|_| {
        assert!(mounted(&stale_point), "the killed App Server left no mount");
        unmount_outside(&covered_point);
        std::fs::write(covered_point.join("earlier.txt"), "earlier").unwrap();
        block(&blocked_point);
    });

    let report = prepared_again(&machine, "bind_2", &drives);
    assert_eq!(mount_failures(&report), [blocked], "{report}");
    for task in ["again_1", "again_2"] {
        let terminal = machine.run_in(SESSION, "bind_2", task, &reads);
        assert_eq!(
            outcome(&terminal).1,
            format!("{covered}\nf.txt\n{stale}\nunavailable\n"),
            "{terminal}"
        );
        assert_eq!(unmounted(&terminal), Vec::<String>::new(), "{terminal}");
    }
    case.observe(format!(
        "the next start's preparation reported one mount failure, of {blocked}; two commands read {covered} without the file put into its mount point and {stale} over its stale mount, found {blocked} unavailable and listed no drive unmounted"
    ));

    // The failure counts as reported once the drive sync has stored it,
    // which it has by the time a later command's result has come.
    machine.kill_and_restart(|_| {});
    let report = prepared_again(&machine, "bind_3", &drives);
    assert_eq!(mount_failures(&report), Vec::<String>::new(), "{report}");
    let terminal = machine.run_in(SESSION, "bind_3", "further", &reads);
    assert_eq!(
        outcome(&terminal).1,
        format!("{covered}\nf.txt\n{stale}\nunavailable\n"),
        "{terminal}"
    );
    assert_eq!(unmounted(&terminal), Vec::<String>::new(), "{terminal}");
    case.observe(format!(
        "a further start, with the mount point still blocked, reported no mount failure in its preparation or its command's result, and {blocked} stayed unavailable"
    ));
    case.pass();
}

#[test]
fn a_drive_unmounted_outside_the_cli_is_mounted_again_and_a_blocked_one_is_listed_once() {
    let record = tetral_tests::record!();
    let mut case = record
        .case("a Session of two drives, one unmounted from outside the CLI between two commands");
    let machine = DriveMachine::start_seeded(
        &mut case,
        &[(DRIVE, READ_ONLY), (OTHER, READ_ONLY)],
        |engine| {
            commit(engine, DRIVE, &[write("f.txt", "v1")]);
            commit(engine, OTHER, &[write("g.txt", "other")]);
        },
    );
    let read = format!("cat {}", in_drive(DRIVE, "f.txt"));
    assert_eq!(machine.output(&read), "v1");
    let point = machine.mount(DRIVE);
    unmount_outside(&point);
    let next = machine.command(&read);
    assert_eq!(outcome(&next).1, "v1", "{next}");
    assert_eq!(unmounted(&next), Vec::<String>::new(), "{next}");
    assert!(mounted(&point));
    case.observe(
        "the next command read the drive, which is mounted again, and listed no drive unmounted",
    );
    case.pass();

    let mut case =
        record.case("the same drive unmounted again and a file put where its mount point was");
    block(&point);
    let other = format!("cat {}", in_drive(OTHER, "g.txt"));
    let next = machine.command(&other);
    assert_eq!(outcome(&next).1, "other", "{next}");
    assert_eq!(unmounted(&next), [DRIVE], "{next}");
    let after = machine.command(&other);
    assert_eq!(outcome(&after).1, "other", "{after}");
    assert_eq!(unmounted(&after), Vec::<String>::new(), "{after}");
    case.observe(format!(
        "the next command read the other drive and listed {} unmounted; the command after it listed no drive",
        outcome(&next).2["unmounted"][0]
    ));
    case.pass();
}

#[test]
fn a_sessions_commands_see_its_own_drives_only() {
    let record = tetral_tests::record!();
    let mut case = record.case("two Sessions on one machine, each with a drive of its own");
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("f.txt", "one")]);
        commit(engine, OTHER, &[write("g.txt", "two")]);
    });
    let issued = machine.prepare_with(
        "sess_2",
        "bind_other",
        &machine.project,
        &[(OTHER, READ_ONLY)],
        &[],
    );
    let report = machine.session_report(&issued);
    assert_eq!(report["status"], "prepared", "{report}");
    machine.answers_settled();
    let lists = "ls \"$TETRAL_DRIVES\"; cat \"$TETRAL_DRIVES\"/*/*";
    let own = machine.command(lists);
    let others = machine.run_in("sess_2", "bind_other", "other_lists", lists);
    assert_eq!(
        (outcome(&own).1.as_str(), outcome(&others).1.as_str()),
        ("drive_a\none", "drive_b\ntwo"),
        "{own} {others}"
    );
    case.observe("ls \"$TETRAL_DRIVES\" listed drive_a alone in one Session and drive_b alone in the other, and each read its own file");
    case.pass();
}
