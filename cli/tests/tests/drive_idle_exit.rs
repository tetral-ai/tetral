//! Layer 2. `server run --idle-exit` with a drive, on the real App Server
//! against the reference control server and through a real mount: what the
//! exit does with a file written after the last command, with its upload
//! passing and held.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::{PAUSE, wait_until};
use tetral_tests::drives::{self, DRIVE, DriveMachine, FINAL_UPLOAD, READ_WRITE, sha256};
use tetral_tests::machine::answer_fault;
use tetral_tests::tasks::SESSION;

const SECOND: Duration = Duration::from_secs(1);

/// A machine started with `--idle-exit 1m` whose last command has ended and
/// into whose drive a process outside the CLI then wrote `name`.
fn idle_with_a_file(case: &mut tetral_tests::record::Case, name: &str, text: &str) -> DriveMachine {
    let machine =
        DriveMachine::start_with_arguments(case, &["--idle-exit", "1m"], &[(DRIVE, READ_WRITE)]);
    assert_eq!(machine.output("echo last"), "last");
    std::fs::write(machine.mount(DRIVE).join(name), text).unwrap();
    machine
}

/// Moves the App Server's clock to the 59th second after its last command,
/// finds it running with no final upload begun, and moves it to the 60th,
/// at which the exit's final upload begins.
fn pass_the_idle_minute(machine: &DriveMachine) {
    let seams = &machine.machine().seams;
    let final_uploads = seams.count(FINAL_UPLOAD);
    for _ in 0..59 {
        machine.step(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert_eq!(seams.count(FINAL_UPLOAD), final_uploads);
    assert_eq!(machine.machine().status()["server_state"], "running");
    machine.step(SECOND);
    seams.wait_for_count(FINAL_UPLOAD, final_uploads + 1);
}

#[test]
fn an_idle_exit_commits_a_file_written_after_the_last_command_before_it_exits() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "server run --idle-exit 1m; a process outside the CLI writes a file into the drive after the last command",
    );
    let mut machine = idle_with_a_file(&mut case, "idle.txt", "idle");
    pass_the_idle_minute(&machine);
    let (run, _) = machine.exited();
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(
        drives::commits(&machine.engine, DRIVE),
        [json!([{
            "path": "idle.txt", "change": "write", "base_sha256": null,
            "content_sha256": sha256(b"idle"), "executable": false,
        }])]
    );
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("idle.txt".to_owned(), sha256(b"idle"))]
    );
    case.observe("the App Server ran through the 59th second after its last command and exited 0 after the 60th, with idle.txt committed in one commit before the exit");
    case.pass();
}

#[test]
fn an_idle_exit_leaves_a_file_whose_upload_is_held_to_the_next_start() {
    let record = tetral_tests::record!();
    let mut case = record.case(
        "server run --idle-exit 1m; the upload of a file written after the last command is held",
    );
    let mut machine = {
        let machine = DriveMachine::start_with_arguments(
            &mut case,
            &["--idle-exit", "1m"],
            &[(DRIVE, READ_WRITE)],
        );
        answer_fault(
            &machine.engine,
            json!({
                "endpoint": "upload_content", "content_sha256": sha256(b"held"),
                "action": "hold_request", "count": 100,
            }),
        );
        assert_eq!(machine.output("echo last"), "last");
        std::fs::write(machine.mount(DRIVE).join("held.txt"), "held").unwrap();
        machine
    };
    pass_the_idle_minute(&machine);
    wait_until("the held upload", || drives::held(&machine.engine) == 1);
    for _ in 0..29 {
        machine.step(SECOND);
    }
    std::thread::sleep(PAUSE);
    assert_eq!(machine.machine().status()["server_state"], "running");
    machine.step(SECOND);
    let (run, _) = machine.exited();
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(drives::commits(&machine.engine, DRIVE), Vec::<Value>::new());
    assert_eq!(
        drives::requests(&machine.engine, "upload_content", DRIVE),
        Vec::<Value>::new()
    );
    case.observe("the final upload began with the 60th second after the last command and the App Server exited 0 with the 30th second of it; the upload of held.txt was held and nothing was committed");

    answer_fault(&machine.engine, json!({"action": "clear"}));
    answer_fault(&machine.engine, json!({"action": "release"}));
    machine.start_again();
    let issued = machine.prepare_with(
        SESSION,
        "bind_2",
        &machine.project,
        &[(DRIVE, READ_WRITE)],
        &[],
    );
    let report = machine.session_report(&issued);
    assert_eq!(report["status"], "prepared", "{report}");
    // The change was kept, not lost: nothing reports it.
    for ready in machine.engine.received("ready") {
        assert!(ready["body"].get("lost_changes").is_none(), "{ready}");
    }
    assert!(report.get("lost_changes").is_none(), "{report}");
    machine.advance_until(
        "the commit of held.txt",
        SECOND,
        Duration::from_secs(10),
        || drives::commits(&machine.engine, DRIVE).len() == 1,
    );
    assert_eq!(
        drives::files(&machine.engine, DRIVE),
        [("held.txt".to_owned(), sha256(b"held"))]
    );
    case.observe("started again and with the Session prepared again, the App Server committed held.txt with no tool call; no ready and no report listed a lost change");
    case.pass();
}
