//! `server run --idle-exit` against the reference control server, as
//! separate processes on the controlled clock: the exit a duration after
//! the last activity, held by a running command, moved by a control and
//! ended by a call during the upload before it; no exit without the option;
//! and the durations it refuses.

use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    APP, CLI_START, CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, seconds,
};
use tetral_tests::run::Tetral;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{TaskMachine, WAIT_CHECKED, policy};

const UPLOADING: &str = "idle.uploading";
const CHECKED: &str = "idle.checked";
const EXIT_OBSERVED: &str = "task.exit_observed";

/// Moves the clock to `elapsed` after the App Server's start, keeping the
/// connection online, and waits until the idle timer has read the new time
/// or begun the exit, after which it reads the clock no more.
fn to(tasks: &TaskMachine, elapsed: Duration) {
    let seams = &tasks.machine().seams;
    let now = seams.now();
    tasks.advance((CLI_START + elapsed).saturating_sub(now));
    let checked = seams.count(CHECKED);
    wait_until("the idle timer's reading of the new time", || {
        seams.count(CHECKED) >= checked + 2 || seams.waiting(UPLOADING) == 1
    });
}

/// Whether the exit has begun: its upload waits at the held barrier.
fn exiting(tasks: &TaskMachine) -> bool {
    tasks.machine().seams.waiting(UPLOADING) == 1
}

fn idle_machine(case: &mut tetral_tests::record::Case, directory: &std::path::Path) -> TaskMachine {
    let tasks = TaskMachine::start_with(case, directory, &["--idle-exit", "1m"]);
    tasks.machine().seams.hold(UPLOADING);
    tasks
}

/// Ends the exit's held upload and checks that `server run` exits 0 within
/// the shutdown's 5 seconds, printing its status.
fn exits(case: &mut tetral_tests::record::Case, mut tasks: TaskMachine) {
    tasks.machine().seams.release(UPLOADING);
    let (run, took) = tasks.exited();
    case.command(
        "tetral server run --app app_one --idle-exit 1m",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert!(run.object().get("server_state").is_some(), "{}", run.exit());
    assert!(took < seconds(5.0), "{took:?}");
}

#[test]
fn an_idle_app_server_begins_its_exit_a_minute_after_its_start() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let mut case = record.case("--idle-exit 1m with no activity");
    let tasks = idle_machine(&mut case, directory.path());
    to(&tasks, seconds(59.0));
    assert!(!exiting(&tasks));
    to(&tasks, seconds(60.0));
    tasks.machine().seams.wait_until_waiting(UPLOADING, 1);
    exits(&mut case, tasks);
    case.observe("no exit at 59 seconds; the exit began at 60 seconds and server run exited 0 with its status within 5 seconds");
    case.pass();
}

#[test]
fn a_running_background_command_holds_the_exit_until_a_minute_after_its_exit() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let mut case = record.case("a background exec_command running from 0 to 70 seconds");
    let tasks = idle_machine(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let flag = tasks.project.join("flag");
    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_background",
        json!({
            "cmd": format!("while [ ! -e {} ]; do sleep 0.05; done", flag.display()),
            "wait_ms": 250, "on_wait_expiry": "detach",
        }),
        policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    seams.advance(Duration::from_millis(250));
    tasks.report("task_background", "running");
    tasks.advance(seconds(69.75));
    assert!(!exiting(&tasks));
    // The command exits at 70 seconds; its exit is read at that time.
    seams.hold(EXIT_OBSERVED);
    std::fs::write(&flag, "").unwrap();
    seams.wait_until_waiting(EXIT_OBSERVED, 1);
    let checked = seams.count(CHECKED);
    seams.release(EXIT_OBSERVED);
    seams.wait_for_count(CHECKED, checked + 1);
    to(&tasks, seconds(129.0));
    assert!(!exiting(&tasks));
    to(&tasks, seconds(130.0));
    seams.wait_until_waiting(UPLOADING, 1);
    exits(&mut case, tasks);
    case.observe("no exit at 60 or 129 seconds while the command ran and after its exit at 70; the exit began at 130 seconds");
    case.pass();
}

#[test]
fn a_control_moves_the_exit_and_a_call_during_its_upload_ends_it() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();

    let mut case = record.case("a tool.stdin at 50 seconds");
    let tasks = idle_machine(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    to(&tasks, seconds(50.0));
    let checked = seams.count(CHECKED);
    tasks.stdin("task_unknown", json!({ "chars": "x", "write_seq": 1 }));
    seams.wait_for_count(CHECKED, checked + 1);
    to(&tasks, seconds(109.0));
    assert!(!exiting(&tasks));
    to(&tasks, seconds(110.0));
    seams.wait_until_waiting(UPLOADING, 1);
    case.observe("the exit, due at 60 seconds, began at 110 seconds");
    case.pass();

    let mut case = record.case("a tool.execute read while the upload before the exit is held");
    let checked = seams.count(CHECKED);
    tasks.execute(
        "task_late",
        json!({ "cmd": "true" }),
        policy(&tasks.project, "disabled"),
    );
    let terminal = tasks.terminal("task_late");
    assert_eq!(terminal["result"]["result"]["exit_code"], 0, "{terminal:#}");
    seams.wait_for_count(CHECKED, checked + 1);
    // The released upload's end comes after the call: the exit stays ended.
    let checked = seams.count(CHECKED);
    seams.release_one(UPLOADING);
    seams.wait_for_count(CHECKED, checked + 1);
    to(&tasks, seconds(169.0));
    assert!(!exiting(&tasks));
    assert_eq!(tasks.machine().status()["server_state"], "running");
    to(&tasks, seconds(170.0));
    seams.wait_until_waiting(UPLOADING, 1);
    exits(&mut case, tasks);
    case.observe("the call was executed and reported; the process still ran 59 seconds later and began the exit 60 seconds after the call");
    case.pass();
}

#[test]
fn without_the_option_the_app_server_still_runs_after_24_hours() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let engine = one_credential_engine();
    let mut case = record.case("server run without --idle-exit, the clock advanced 24 hours");
    let machine = Machine::start(
        &mut case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    machine.advance(seconds(24.0 * 60.0 * 60.0));
    machine.settle();
    assert_eq!(machine.status()["server_state"], "running");
    case.observe(
        "server run still answered its status as running after the clock advanced 24 hours",
    );
    case.pass();
}

#[test]
fn an_idle_exit_that_is_not_a_positive_count_of_s_m_or_h_is_refused() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let directory = tempfile::tempdir().unwrap();
    let mut case = record.case("server run --idle-exit 0m, 15, 1d and -1s");
    for duration in ["0m", "15", "1d", "-1s"] {
        let run = Tetral::new(["server", "run", "--app", APP, "--idle-exit", duration])
            .home(&directory.path().join("root"))
            .run();
        case.command(
            &format!("tetral server run --app app_one --idle-exit {duration}"),
            &run.exit(),
        );
        let error: Value = run.object()["error"].clone();
        assert_eq!(
            error["code"],
            "invalid_arguments",
            "{duration}: {}",
            run.exit()
        );
        assert!(!directory.path().join("root").exists());
    }
    case.observe("each was refused with invalid_arguments before the state root was created");
    case.pass();
}
