//! Layer 2. Downloads of the real App Server against the reference control server,
//! through real mounts: what a command's read gets when the content's
//! download stalls, is answered `503` or is refused, and what its running
//! result says of a download it waits for.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::drives::{
    self, DRIVE, DriveMachine, READ_ONLY, commit, in_drive, outcome, sha256, write,
};
use tetral_tests::machine::answer_fault;
use tetral_tests::tasks::WAIT_CHECKED;

/// The statuses of the downloads of `content`, in arrival order.
fn downloads(machine: &DriveMachine, content: &str) -> Vec<Value> {
    drives::requests(&machine.engine, "download_content", DRIVE)
        .into_iter()
        .filter(|request| request["content_sha256"] == sha256(content.as_bytes()))
        .map(|request| request["status"].clone())
        .collect()
}

#[test]
fn a_download_that_stalls_is_listed_in_the_running_result_and_tried_again_after_ten_seconds() {
    let record = tetral_tests::record!();
    let mut case = record
        .case("a command reading a file whose first download stops after its first 4,096 bytes");
    let content = "0123456789abcdef".repeat(16_384);
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("big.txt", &content)]);
    });
    // The server writes a throttled download at the pace of its own clock:
    // 4,096 bytes for the one second it moves, and no more while it stands.
    answer_fault(
        &machine.engine,
        json!({"endpoint": "download_content", "action": "throttle", "bytes_per_second": 4096}),
    );
    let seams = &machine.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    let expected = machine.project.join("expected");
    std::fs::write(&expected, &content).unwrap();
    let reading = machine.start_background(
        &format!(
            "cmp {} '{}' && echo same",
            in_drive(DRIVE, "big.txt"),
            expected.display()
        ),
        2_000,
    );
    machine.wait_for_start(checked);
    wait_until("the download's request", || {
        downloads(&machine, &content).len() == 1
    });
    machine.engine.advance(Duration::from_secs(1));
    // The App Server's stall time counts from the bytes it has, so it must
    // hold them before its clock moves.
    let partial = machine.root.join("cache/tmp");
    wait_until("the App Server holds the first 4,096 bytes", || {
        std::fs::read_dir(&partial).is_ok_and(|entries| {
            entries
                .flatten()
                .any(|entry| entry.metadata().is_ok_and(|file| file.len() == 4_096))
        })
    });

    seams.advance(Duration::from_secs(2));
    let running = machine.report(&reading, "running");
    let waited = &running["result"]["drive_sync"]["downloading"];
    assert_eq!(waited.as_array().map(Vec::len), Some(1), "{running}");
    assert!(
        waited[0]["path"]
            .as_str()
            .is_some_and(|path| path.ends_with(&format!("/drives/{DRIVE}/big.txt"))),
        "{running}"
    );
    assert_eq!(
        (&waited[0]["downloaded_bytes"], &waited[0]["size_bytes"]),
        (&json!(4_096), &json!(content.len())),
        "{running}"
    );
    case.observe(format!(
        "after the command's 2-second wait its running result listed {}",
        waited[0]
    ));

    // Ten seconds without a byte end the attempt, and the next one starts
    // after the client's first retry delay of at most 1.2 seconds. Up to
    // nine seconds the clock moves a second at a time, each followed by a
    // pause: an attempt given up sooner would be followed by a second one
    // here.
    for second in 3..=9 {
        seams.advance(Duration::from_secs(1));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            downloads(&machine, &content).len(),
            1,
            "a second download {second} seconds after the last byte"
        );
    }
    machine.advance_until(
        "the second download",
        Duration::from_millis(1_200),
        Duration::from_secs(12),
        || downloads(&machine, &content).len() == 2,
    );
    let (exit, stdout, _) = outcome(&machine.terminal(&reading));
    assert_eq!((exit, stdout.as_str()), (0, "same\n"));
    // The second download asks for the bytes the first did not bring.
    assert_eq!(downloads(&machine, &content), [json!(200), json!(206)]);
    case.observe(format!(
        "ten seconds of the App Server's clock after the last byte it asked for the rest in a second download, and the read returned all {} bytes",
        content.len()
    ));
    case.pass();
}

#[test]
fn a_download_answered_503_is_tried_again_until_it_passes() {
    let record = tetral_tests::record!();
    let mut case =
        record.case("a command reading a file whose first two downloads are answered 503");
    let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
        commit(engine, DRIVE, &[write("waits.txt", "waited")]);
    });
    answer_fault(
        &machine.engine,
        json!({
            "endpoint": "download_content", "action": "error",
            "code": "unavailable", "status": 503, "count": 2,
        }),
    );
    // The read's wait covers the clock's steps below.
    let reading =
        machine.start_command_waiting(&format!("cat {}", in_drive(DRIVE, "waits.txt")), 50_000);
    // The client's retry delays are at most 1.2 and 2.4 seconds.
    machine.advance_until(
        "the third download",
        Duration::from_millis(2_400),
        Duration::from_secs(24),
        || downloads(&machine, "waited").len() == 3,
    );
    let (exit, stdout, _) = outcome(&machine.terminal(&reading));
    assert_eq!((exit, stdout.as_str()), (0, "waited"));
    assert_eq!(
        downloads(&machine, "waited"),
        [json!(503), json!(503), json!(200)]
    );
    case.observe("two downloads were answered 503, the third 200, and the read returned the file");
    case.pass();
}

#[test]
fn a_download_refused_as_missing_or_denied_fails_its_read_at_once() {
    let record = tetral_tests::record!();
    let machine = {
        let mut case = record.case("a drive with two files, mounted read-only");
        let machine = DriveMachine::start_seeded(&mut case, &[(DRIVE, READ_ONLY)], |engine| {
            commit(
                engine,
                DRIVE,
                &[
                    write("missing.txt", "missing"),
                    write("refused.txt", "refused"),
                ],
            );
        });
        case.pass();
        machine
    };
    for (file, content, code, status) in [
        ("missing.txt", "missing", "content_not_found", 404),
        ("refused.txt", "refused", "drive_access_denied", 403),
    ] {
        let mut case = record.case(&format!(
            "a read of a file whose downloads are answered {code}"
        ));
        answer_fault(
            &machine.engine,
            json!({"endpoint": "download_content", "action": "error", "code": code, "count": 100}),
        );
        // No clock moves here: a download tried again after a delay would
        // wait for one, and the result would not come.
        let terminal = machine.command(&format!(
            "cat {} 2>&1; echo \"exit=$?\"",
            in_drive(DRIVE, file)
        ));
        answer_fault(&machine.engine, json!({"action": "clear"}));
        let (_, stdout, _) = outcome(&terminal);
        assert!(
            stdout.contains("Input/output error") && stdout.ends_with("exit=1\n"),
            "{terminal}"
        );
        let answered = downloads(&machine, content);
        assert!(
            !answered.is_empty() && answered.iter().all(|answer| answer == status),
            "{answered:?}"
        );
        case.observe(format!(
            "its {} downloads were answered {status} {code} and the read wrote {stdout:?} with the App Server's clock standing",
            answered.len()
        ));
        case.pass();
    }
}
