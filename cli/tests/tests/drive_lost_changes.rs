//! Layer 2. The changes of a Session's drives that the real App Server
//! gives up, against the reference control server and through real mounts:
//! what its reports list as lost, and what it does with the Session's data.

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, FINAL_UPLOAD, READ_WRITE, commit, sha256, write,
};
use tetral_tests::machine::{INSTANCE, answer_fault};
use tetral_tests::sessions::state_of;
use tetral_tests::tasks::{BINDING, ENDING_CHECKED, GONE_CHECKED, SESSION, WAIT_CHECKED};

const OTHER: &str = "drive_b";

/// How long the pauses are in which the App Server could do what a test
/// then finds it has not done.
const PAUSE: Duration = Duration::from_millis(300);

const SECOND: Duration = Duration::from_secs(1);

/// Issues the close of the prepared binding and gives its `message_id`.
fn close(machine: &DriveMachine) -> String {
    let issued = machine.engine.control(
        "close",
        json!({
            "instance_id": INSTANCE,
            "payload": { "session_id": SESSION, "binding_id": BINDING },
        }),
    );
    issued["message_id"].as_str().unwrap().to_owned()
}

/// How many requests of `endpoint` the server holds.
fn held(machine: &DriveMachine, endpoint: &str) -> usize {
    machine.engine.state()["held"].as_array().map_or(0, |held| {
        held.iter()
            .filter(|one| one["endpoint"] == endpoint)
            .count()
    })
}

/// The `lost_changes` of the latest ready the server accepted.
fn ready_lost_changes(machine: &DriveMachine) -> Value {
    let readies = machine.engine.received("ready");
    readies.last().expect("a ready")["body"]
        .get("lost_changes")
        .cloned()
        .unwrap_or(Value::Null)
}

/// A machine whose Session binds `drives`, the first with a file `f` that
/// the machine has rewritten outside any command while the server holds the
/// upload of the new content and, with `report_held`, the next Session
/// report.
fn with_a_held_change(
    case: &mut tetral_tests::record::Case,
    drives: &[&str],
    report_held: bool,
) -> DriveMachine {
    let bound: Vec<(&str, &str)> = drives.iter().map(|drive| (*drive, READ_WRITE)).collect();
    let machine = DriveMachine::start_seeded(case, &bound, |engine| {
        commit(engine, drives[0], &[write("f", "old")]);
    });
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "upload_content", "content_sha256": sha256(b"new"),
            "action": "hold_request", "count": 100,
        }),
    );
    if report_held {
        answer_fault(
            &machine.engine,
            json!({"endpoint": "session-status", "action": "hold_request", "count": 1}),
        );
    }
    std::fs::write(machine.mount(drives[0]).join("f"), "new").unwrap();
    machine
}

/// Lets the grant of a connection the App Server has given up end and moves
/// the App Server's clock until its next connection is online. The delay
/// before an attempt doubles from one second while no heartbeat is
/// confirmed, and one that begins after a move of the clock counts from the
/// moved clock: the clock moves by the first delay at a time.
fn online_again(machine: &DriveMachine) {
    machine.engine.advance(Duration::from_secs(46));
    for _ in 0..10 {
        machine
            .machine()
            .seams
            .advance(Duration::from_millis(1_200));
        std::thread::sleep(PAUSE);
        if machine.machine().status()["connection_state"] == "online" {
            return;
        }
    }
    panic!("no connection within 12 seconds of the App Server's clock");
}

/// What a report or a ready lists for the lost change of `f` under
/// `message_id`.
fn lost_f(message_id: &str) -> Value {
    json!([{
        "request_message_id": message_id, "drive_id": DRIVE,
        "paths": ["f"], "paths_total": 1,
    }])
}

/// Sends the Session's close on the current connection, which holds no
/// binding of it, and gives the `closed` report once the Session's data is
/// deleted. The drive must still hold its one version of `f`.
fn closed_late(machine: &DriveMachine) -> Value {
    let late = close(machine);
    let report = machine.session_report(&late);
    assert_eq!(report["status"], "closed", "{report}");
    let session = machine.root.join("sessions").join(SESSION);
    wait_until("the Session's data deleted", || !session.exists());
    let state = drives::state(&machine.engine, DRIVE);
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("f".to_owned(), sha256(b"old"))]
    );
    assert_eq!(
        state["versions"].as_array().map(Vec::len),
        Some(1),
        "{state}"
    );
    report
}

/// Moves the App Server's clock through the 30 seconds of a final upload
/// that has begun, a second at a time.
fn pass_the_final_upload(machine: &DriveMachine) {
    for _ in 0..30 {
        machine.step(SECOND);
    }
}

/// The Session reports the server accepted in answer to `message_id`.
fn answers(machine: &DriveMachine, message_id: &str) -> Vec<Value> {
    machine
        .engine
        .received("session-status")
        .into_iter()
        .filter(|report| report["body"]["request_message_id"] == message_id)
        .map(|report| report["body"].clone())
        .collect()
}

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
    let closing = close(&machine);
    let report = machine.session_report(&closing);
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
        // A Mac takes paths of at most 1,024 bytes: the directory and the
        // files are made by paths relative to the mount and to it.
        let made = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "cd '{}' && mkdir -p '{directory}' && cd '{directory}' && for i in $(seq -w 0 99); do printf x > \"${{i}}{name}\"; done",
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

#[test]
fn a_close_reports_thirty_seconds_after_it_with_the_change_whose_upload_never_ended() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a close of a Session with a background command that ignores its end signal, a change whose upload the server holds and a change to a path another writer committed meanwhile",
    );
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_WRITE)], |engine| {
        commit(
            engine,
            DRIVE,
            &[write("held.txt", "old"), write("taken.txt", "base")],
        );
    });
    let seams = &machine.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    let ignoring = machine.start_background("trap '' TERM; while :; do sleep 1; done", 250);
    machine.wait_for_start(checked);
    seams.advance(Duration::from_millis(250));
    machine.report(&ignoring, "running");

    // Written outside any command, with the clock short of the two seconds
    // after which the App Server uploads such a write.
    let mount = machine.mount(DRIVE);
    std::fs::write(mount.join("held.txt"), "new").unwrap();
    std::fs::write(mount.join("taken.txt"), "mine").unwrap();
    commit(&machine.engine, DRIVE, &[write("taken.txt", "theirs")]);
    let versions = || drives::state(&machine.engine, DRIVE)["versions"].clone();
    let before = versions();
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "upload_content", "drive_id": DRIVE, "content_sha256": sha256(b"new"),
            "action": "hold_request", "count": 100,
        }),
    );

    let final_uploads = seams.count(FINAL_UPLOAD);
    let closing = close(&machine);
    wait_until("the close admitted", || {
        state_of(machine.machine(), SESSION) == "closing"
    });
    // The command ignores its signal and is killed two seconds later; the
    // final upload begins once it is gone. Each step waits until the
    // termination's timer or the check of the ended task has read the time.
    let mut passed = 0;
    while seams.count(FINAL_UPLOAD) == final_uploads {
        assert!(
            passed < 10,
            "no final upload {passed} seconds after the close"
        );
        let (ending, gone) = (seams.count(ENDING_CHECKED), seams.count(GONE_CHECKED));
        machine.step(SECOND);
        passed += 1;
        wait_until("the App Server read the new time", || {
            seams.count(ENDING_CHECKED) >= ending + 2
                || seams.count(GONE_CHECKED) > gone
                || seams.count(FINAL_UPLOAD) > final_uploads
        });
    }
    let killed = machine.terminal(&ignoring);
    wait_until("the held upload and the commit of the other change", || {
        drives::held(&machine.engine) == 1
            && !drives::requests(&machine.engine, "commit", DRIVE).is_empty()
    });
    case.observe(format!(
        "{passed} seconds after the close the command was gone, with the terminal report {}, and the final upload began: the upload of held.txt was held and taken.txt's commit sent",
        killed["result"]["result"]
    ));

    for _ in passed..29 {
        machine.step(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert_eq!(answers(&machine, &closing), Vec::<Value>::new());
    assert_eq!(state_of(machine.machine(), SESSION), "closing");
    machine.step(SECOND);
    let report = machine.session_report(&closing);
    assert_eq!(report["status"], "closed", "{report}");
    assert_eq!(
        report["lost_changes"],
        json!([{
            "request_message_id": closing, "drive_id": DRIVE,
            "paths": ["held.txt"], "paths_total": 1,
        }]),
        "{report}"
    );
    let sent = drives::commits(&machine.engine, DRIVE);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(
        sent[0],
        json!([{
            "path": "taken.txt", "change": "write", "base_sha256": sha256(b"base"),
            "content_sha256": sha256(b"mine"), "executable": false,
        }])
    );
    assert_eq!(versions(), before);
    let session = machine.root.join("sessions").join(SESSION);
    wait_until("the Session's data deleted", || !session.exists());
    answer_fault(&machine.engine, json!({"action": "clear"}));
    answer_fault(&machine.engine, json!({"action": "release"}));
    case.observe(
        "the Session stayed closing and the closed report came with the 30th second after the close, listing held.txt alone as lost; the one commit sent held taken.txt on the base another writer had replaced, the drive kept its versions, and the Session's data was deleted",
    );
    case.pass();
}

#[test]
fn a_removed_drives_lost_change_comes_with_the_ready_after_a_kill_and_with_no_later_report() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a drive removed from an idle Session while its one change's upload is held, and the App Server killed before the removal's report is accepted",
    );
    let mut machine = with_a_held_change(&mut case, &[DRIVE, OTHER], true);
    let final_uploads = machine.machine().seams.count(FINAL_UPLOAD);
    let removal = machine.change_drives(SESSION, BINDING, &[(OTHER, READ_WRITE)]);
    machine
        .machine()
        .seams
        .wait_for_count(FINAL_UPLOAD, final_uploads + 1);
    pass_the_final_upload(&machine);
    wait_until("the removal's report held by the server", || {
        held(&machine, "session-status") == 1
    });
    assert_eq!(answers(&machine, &removal), Vec::<Value>::new());

    machine.kill_and_restart(|machine| {
        answer_fault(&machine.engine, json!({"action": "clear"}));
        answer_fault(&machine.engine, json!({"action": "release"}));
    });
    let listed = ready_lost_changes(&machine);
    assert_eq!(listed, lost_f(&removal));
    case.observe(format!(
        "the removal's report was never accepted; the ready of the App Server started again listed lost_changes {listed}"
    ));

    let again = machine.prepare_with(
        SESSION,
        "bind_2",
        &machine.project,
        &[(OTHER, READ_WRITE)],
        &[],
    );
    let prepared = machine.session_report(&again);
    assert_eq!(
        (&prepared["status"], prepared.get("lost_changes")),
        (&json!("prepared"), None),
        "{prepared}"
    );
    machine.answers_settled();
    let other = machine.change_drives(SESSION, "bind_2", &[]);
    let applied = machine.session_report(&other);
    assert_eq!(
        (&applied["status"], applied.get("lost_changes")),
        (&json!("applied"), None),
        "{applied}"
    );
    machine.end_connection();
    machine.reconnect(Duration::from_millis(1_200));
    assert_eq!(ready_lost_changes(&machine), Value::Null);
    let state = drives::state(&machine.engine, DRIVE);
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("f".to_owned(), sha256(b"old"))]
    );
    assert_eq!(
        state["versions"].as_array().map(Vec::len),
        Some(1),
        "{state}"
    );
    case.observe(
        "the Session's new preparation, the removal of its other drive and the next connection's ready listed no lost change, and the drive kept its one version of f",
    );
    case.pass();
}

#[test]
fn a_closes_lost_change_is_listed_by_each_ready_until_one_is_acknowledged() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a close whose final upload ends with a change's upload held, whose closed report is never accepted and whose connection then ends; the next connection's ready is not acknowledged",
    );
    let machine = with_a_held_change(&mut case, &[DRIVE], true);
    let seams = &machine.machine().seams;
    let final_uploads = seams.count(FINAL_UPLOAD);
    let closing = close(&machine);
    seams.wait_for_count(FINAL_UPLOAD, final_uploads + 1);
    pass_the_final_upload(&machine);
    wait_until("the closed report held by the server", || {
        held(&machine, "session-status") == 1
    });
    assert_eq!(answers(&machine, &closing), Vec::<Value>::new());

    answer_fault(&machine.engine, json!({"action": "clear"}));
    answer_fault(
        &machine.engine,
        json!({"endpoint": "ready", "action": "withhold", "count": 1}),
    );
    let readies = machine.engine.received("ready").len();
    machine.end_connection();
    machine.engine.advance(Duration::from_secs(46));
    seams.advance(Duration::from_millis(1_200));
    tetral_tests::machine::wait_for_received(&machine.engine, "ready", readies + 1);
    assert_eq!(ready_lost_changes(&machine), lost_f(&closing));
    assert_eq!(answers(&machine, &closing), Vec::<Value>::new());
    case.observe(format!(
        "the closed report was never accepted; the next connection's ready listed lost_changes {}",
        lost_f(&closing)
    ));

    // The ready's ACK has 45 seconds from the connection's start; without
    // it the App Server gives the connection up and makes another once the
    // server's grant of this one has ended.
    machine.machine().wait_for("registering");
    seams.advance(Duration::from_secs(45));
    machine.machine().wait_for("disconnected");
    online_again(&machine);
    assert_eq!(machine.engine.received("ready").len(), readies + 2);
    assert_eq!(ready_lost_changes(&machine), lost_f(&closing));
    answer_fault(&machine.engine, json!({"action": "release"}));
    case.observe(
        "45 seconds without the ready's ACK the App Server gave the connection up; the following connection's ready listed the same entry and was acknowledged",
    );

    let late = closed_late(&machine);
    assert_eq!(late.get("lost_changes"), None, "{late}");
    machine.end_connection();
    online_again(&machine);
    assert_eq!(ready_lost_changes(&machine), Value::Null);
    case.observe(
        "a close sent on that connection was answered closed without lost_changes, the Session's data was deleted, and the next connection's ready listed no lost change",
    );
    case.pass();
}

#[test]
fn a_close_whose_connection_ends_in_its_final_upload_leaves_the_change_to_the_late_close() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a close whose connection ends ten seconds into its final upload, with a change's upload held",
    );
    let machine = with_a_held_change(&mut case, &[DRIVE], false);
    let seams = &machine.machine().seams;
    let final_uploads = seams.count(FINAL_UPLOAD);
    let closing = close(&machine);
    seams.wait_for_count(FINAL_UPLOAD, final_uploads + 1);
    for _ in 0..10 {
        machine.step(SECOND);
    }
    machine.end_connection();
    machine.reconnect(Duration::from_millis(1_200));
    assert_eq!(ready_lost_changes(&machine), Value::Null);
    assert_eq!(answers(&machine, &closing), Vec::<Value>::new());

    let late = close(&machine);
    let report = machine.session_report(&late);
    assert_eq!(
        (&report["status"], &report["lost_changes"]),
        (&json!("closed"), &lost_f(&late)),
        "{report}"
    );
    let session = machine.root.join("sessions").join(SESSION);
    wait_until("the Session's data deleted", || !session.exists());
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("f".to_owned(), sha256(b"old"))]
    );
    answer_fault(&machine.engine, json!({"action": "clear"}));
    answer_fault(&machine.engine, json!({"action": "release"}));
    case.observe(
        "the close was never answered and the next connection's ready listed no lost change; a close sent on that connection was answered closed with f in lost_changes under it, and the Session's data was deleted",
    );
    case.pass();
}
