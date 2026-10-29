//! `exec_command` between `tetral server run`, whose ready advertises
//! `tool_execution`, and the reference control server as separate
//! processes, each on its own controlled clock, with real restricted
//! processes and real directories: the reports and their sequence, the
//! initial wait under `kill` and `detach`, its clamp and a start that never
//! comes within it, a detached command's lifetime, the working directory and
//! its errors, the environment, the exit facts, the end of a task's
//! processes, the metrics and the trace context of a task. The test seams
//! these tests use are those `tetral_tests::tasks` and
//! `tetral_tests::machine` name.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{INSTANCE, answer_fault, seconds, wait_for_received};
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{
    BINDING, EXIT_OBSERVED, LAUNCH, LAUNCH_STATUS, OUTPUT_ENDED, SESSION, TaskMachine,
    WAIT_CHECKED, millis, policy, process_running,
};

/// What a report says of its task.
fn summary(report: &Value) -> Value {
    let envelope = &report["result"];
    let result = &envelope["result"];
    json!({
        "phase": report["phase"],
        "report_seq": report["report_seq"],
        "status": envelope["status"],
        "error": envelope["error"]["kind"],
        "exit_code": result["exit_code"],
        "signal": result["signal"],
        "timed_out": result["timed_out"],
        "stdout": result["stdout"]["text"],
        "stderr": result["stderr"]["text"],
    })
}

/// The summary of a report whose command never ran.
fn not_run(error: &str) -> Value {
    json!({
        "phase": "terminal", "report_seq": 1, "status": "error", "error": error,
        "exit_code": null, "signal": null, "timed_out": false, "stdout": "", "stderr": "",
    })
}

/// The summary of a command that exited 0 with `stdout`.
fn exited(stdout: &str) -> Value {
    json!({
        "phase": "terminal", "report_seq": 1, "status": "success", "error": null,
        "exit_code": 0, "signal": null, "timed_out": false, "stdout": stdout, "stderr": "",
    })
}

/// The `message_id` of the `running` report of `task_id`.
fn running_message_id(engine: &ReferenceServer, task_id: &str) -> Value {
    engine
        .received("tool-status")
        .into_iter()
        .find(|received| {
            received["body"]["task_id"] == task_id && received["body"]["phase"] == "running"
        })
        .map(|received| received["message_id"].clone())
        .unwrap()
}

/// Every instruction the reference control server issued, on any
/// connection.
fn issued(engine: &ReferenceServer) -> Vec<Value> {
    engine.state()["connections"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|connection| connection["issued"].as_array().cloned().unwrap_or_default())
        .collect()
}

#[test]
fn a_command_that_exits_within_its_wait_reports_its_output_once_as_terminal() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("printf hello with kill and wait_ms 1000, the clock never moved");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    tasks.execute(
        "task_hello",
        json!({"cmd": "printf hello", "on_wait_expiry": "kill", "wait_ms": 1000}),
        policy(&tasks.project, "disabled"),
    );
    let terminal = tasks.terminal("task_hello");
    assert_eq!(summary(&terminal), exited("hello"), "{terminal:#}");
    assert_eq!(tasks.reports("task_hello").len(), 1);
    case.observe(
        "one execution report: terminal, report_seq 1, success, exit_code 0, stdout hello",
    );
    case.pass();
}

#[test]
fn a_command_runs_in_its_directory_with_the_cleaned_environment_and_separate_streams() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record
        .case("a relative cwd and an env that sets FOO, PAGER, TERM, NO_COLOR, TETRAL_HOME and another TETRAL_ name");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    fs::create_dir(tasks.project.join("work")).unwrap();
    tasks.execute(
        "task_env",
        json!({
            "cmd": r#"printf '%s:%s:%s:%s:%s:[%s]:%s' "$FOO" "$TERM" "$NO_COLOR" "$PAGER" "$GIT_PAGER" "$(env | sed -n 's/^\(TETRAL_[^=]*\)=.*/\1/p' | tr '\n' ' ')" "$(pwd)"; printf err >&2"#,
            "cwd": "work",
            "env": {"FOO": "bar", "PAGER": "less", "TERM": "xterm", "NO_COLOR": "0", "TETRAL_HOME": "/elsewhere", "TETRAL_EXTRA": "x"},
        }),
        policy(&tasks.project, "disabled"),
    );
    let terminal = tasks.terminal("task_env");
    let mut expected = exited(&format!(
        "bar:dumb:1:cat:cat:[]:{}",
        tasks.project.join("work").display()
    ));
    expected["stderr"] = json!("err");
    assert_eq!(summary(&terminal), expected, "{terminal:#}");
    case.observe("FOO from env; TERM, NO_COLOR, PAGER and GIT_PAGER fixed; no name beginning with TETRAL_ in the command's environment, neither the App Server's nor the call's TETRAL_HOME and TETRAL_EXTRA; pwd the joined directory; stderr apart");
    case.pass();
}

#[test]
fn a_detach_wait_is_clamped_reports_running_and_the_lifetime_then_ends_the_task() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "sleep 120 under detach with wait_ms 60,000 and lifetime 60,000, then wait_ms 1 and lifetime 1,000",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    for (task, wait_ms, running_at, lifetime) in [
        ("task_long", 60_000, 50_000, 60_000),
        ("task_short", 1, 250, 1_000),
    ] {
        let checked = seams.count(WAIT_CHECKED);
        tasks.execute(
            task,
            json!({"cmd": "sleep 120", "wait_ms": wait_ms, "on_wait_expiry": "detach", "task_lifetime_ms": lifetime}),
            policy(&tasks.project, "disabled"),
        );
        tasks.wait_for_start(checked);
        tasks.advance_wait(millis(running_at - 1));
        assert_eq!(
            tasks.reports(task),
            Vec::<Value>::new(),
            "{task} before {running_at} ms"
        );
        seams.advance(millis(1));
        let running = tasks.report(task, "running");
        assert_eq!(
            summary(&running),
            json!({
                "phase": "running", "report_seq": 1, "status": "running", "error": null,
                "exit_code": null, "signal": null, "timed_out": false, "stdout": "", "stderr": "",
            }),
            "{running:#}"
        );
        assert_eq!(
            running["result"]["result"]["duration_ms"],
            json!(running_at)
        );
        tasks.advance(millis(lifetime - running_at));
        let terminal = tasks.terminal(task);
        assert_eq!(
            summary(&terminal),
            json!({
                "phase": "terminal", "report_seq": 2, "status": "success", "error": null,
                "exit_code": null, "signal": "terminated", "timed_out": true, "stdout": "", "stderr": "",
            }),
            "{terminal:#}"
        );
        assert_eq!(terminal["result"]["result"]["duration_ms"], json!(lifetime));
        case.observe(format!(
            "{task}: nothing at {} ms, running at {running_at} ms, terminal report_seq 2 at {lifetime} ms, timed_out, signal terminated",
            running_at - 1
        ));
    }
    case.pass();
}

#[test]
fn a_kill_wait_ends_every_process_of_the_command_with_the_timeout_error_and_its_partial_output() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("printf partial with two sleeps, one in the background, kill at wait_ms 1,000");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_kill",
        json!({"cmd": "printf partial; sleep 1204 & sleep 1205", "wait_ms": 1_000, "on_wait_expiry": "kill"}),
        policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    wait_until("both sleeps running", || {
        process_running(&["sleep", "1204"]) && process_running(&["sleep", "1205"])
    });
    seams.advance(millis(1_000));
    let terminal = tasks.terminal("task_kill");
    assert_eq!(
        summary(&terminal),
        json!({
            "phase": "terminal", "report_seq": 1, "status": "error", "error": "timeout",
            "exit_code": null, "signal": "terminated", "timed_out": true,
            "stdout": "partial", "stderr": "",
        }),
        "{terminal:#}"
    );
    assert_eq!(terminal["result"]["error"]["message"], "command timed out");
    assert!(!process_running(&["sleep", "1204"]) && !process_running(&["sleep", "1205"]));
    case.observe("terminal timeout, timed_out, signal terminated, stdout partial; neither sleep runs when the report arrives");
    case.pass();
}

#[test]
fn a_command_that_does_not_start_within_its_wait_is_ended_without_a_running_report() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("sleep 1 under detach at wait_ms 250 while the launcher waits at task.launch");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    seams.hold(LAUNCH);
    tasks.execute(
        "task_unstarted",
        json!({"cmd": "sleep 1", "wait_ms": 250, "on_wait_expiry": "detach"}),
        policy(&tasks.project, "direct"),
    );
    seams.wait_until_waiting(LAUNCH, 1);
    seams.advance(millis(250));
    let terminal = tasks.terminal("task_unstarted");
    seams.release(LAUNCH);
    assert_eq!(
        summary(&terminal),
        not_run("exec_spawn_failed"),
        "{terminal:#}"
    );
    assert_eq!(
        terminal["result"]["error"]["message"],
        "process did not start within the wait"
    );
    assert_eq!(tasks.reports("task_unstarted").len(), 1);
    case.observe("no running report; terminal exec_spawn_failed \"process did not start within the wait\" after the wait of 250 ms ended the held launcher");
    case.pass();
}

#[test]
fn a_detached_command_reports_terminal_at_its_exit_without_a_read_or_the_running_ack() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "sleep 2; echo done under detach at wait_ms 250, the running report's answer delayed 30 seconds",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "tool-status", "action": "delay", "delay_ms": 30_000}),
    );
    let answered = seams.count("outbox.answered");
    let checked = seams.count(WAIT_CHECKED);
    let issued_at = Instant::now();
    let message_id = tasks.execute(
        "task_detach",
        json!({"cmd": "sleep 2; echo done", "wait_ms": 250, "on_wait_expiry": "detach"}),
        policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    seams.advance(millis(250));
    let running = tasks.report("task_detach", "running");
    let terminal = tasks.terminal("task_detach");
    let elapsed = issued_at.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "the terminal report after {elapsed:?}"
    );
    assert_eq!(
        (&running["report_seq"], &running["result"]["status"]),
        (&json!(1), &json!("running"))
    );
    let mut expected = exited("done\n");
    expected["report_seq"] = json!(2);
    assert_eq!(summary(&terminal), expected, "{terminal:#}");
    // The running report's answer is still delayed: its attempt ends at
    // the stall limit and it is sent again, after the terminal report.
    seams.wait_for_count("outbox.answered", answered + 1);
    seams.advance(seconds(10.0));
    seams.wait_for_count("outbox.answered", answered + 2);
    seams.advance(seconds(1.5));
    let running_id = running_message_id(&tasks.engine, "task_detach");
    let terminal_id = terminal["message_id"].clone();
    let sent = || -> Vec<Value> {
        tasks
            .engine
            .requests("tool-status")
            .into_iter()
            .map(|request| request["message_id"].clone())
            .filter(|id| *id == running_id || *id == terminal_id)
            .collect()
    };
    wait_until("the running report sent again", || sent().len() == 3);
    assert_eq!(sent(), [running_id.clone(), terminal_id, running_id]);
    let instructions = issued(&tasks.engine);
    let execute = instructions
        .iter()
        .find(|instruction| instruction["message_id"] == message_id.as_str())
        .unwrap();
    assert_eq!(execute["reported"], true, "{execute:#}");
    assert!(
        instructions
            .iter()
            .all(|instruction| instruction["type"] != "tool.stdin")
    );
    tasks.engine.advance(seconds(30.0));
    case.observe(format!("running at 250 ms, terminal report_seq 2 {elapsed:?} after the instruction with no tool.stdin issued; the running report sent again after the terminal one left the reference server's task reported"));
    case.pass();
}

#[test]
fn a_missing_working_directory_is_cwd_not_found_without_a_fallback_or_a_rerun() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "the binding's directory removed, then removed while the launcher waits at task.launch",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let other = directory.path().join("other");
    fs::create_dir(&other).unwrap();
    let other = other.canonicalize().unwrap();
    let prepared = tasks.prepare("sess_2", "bind_2", &other, None);
    tasks.wait_prepared(&prepared);
    let disabled = policy(&tasks.project, "disabled");
    let path = |report: &Value| report["result"]["error"]["path"].clone();

    fs::remove_dir(&tasks.project).unwrap();
    tasks.execute(
        "task_missing",
        json!({"cmd": "touch marker"}),
        disabled.clone(),
    );
    let missing = tasks.terminal("task_missing");
    assert_eq!(summary(&missing), not_run("cwd_not_found"), "{missing:#}");
    assert_eq!(path(&missing), json!(tasks.project));
    assert!(!tasks.project.exists());

    tasks.execute(
        "task_absolute",
        json!({"cmd": "pwd", "cwd": other}),
        disabled.clone(),
    );
    let pwd = format!("{}\n", other.display());
    assert_eq!(summary(&tasks.terminal("task_absolute")), exited(&pwd));
    tasks.execute_with(
        "sess_2",
        "bind_2",
        "task_other",
        json!({"cmd": "pwd"}),
        policy(&other, "disabled"),
        None,
    );
    assert_eq!(summary(&tasks.terminal("task_other")), exited(&pwd));

    fs::create_dir(&tasks.project).unwrap();
    seams.hold(LAUNCH);
    tasks.execute(
        "task_removed",
        json!({"cmd": "echo ran >> ran.log"}),
        policy(&tasks.project, "direct"),
    );
    seams.wait_until_waiting(LAUNCH, 1);
    // The launch has mounted an empty `.git` in the directory.
    fs::remove_dir_all(&tasks.project).unwrap();
    seams.release(LAUNCH);
    let removed = tasks.terminal("task_removed");
    assert_eq!(summary(&removed), not_run("cwd_not_found"), "{removed:#}");
    assert_eq!(path(&removed), json!(tasks.project));

    fs::create_dir(&tasks.project).unwrap();
    tasks.execute("task_after", json!({"cmd": "echo ok"}), disabled.clone());
    assert_eq!(summary(&tasks.terminal("task_after")), exited("ok\n"));
    assert!(!tasks.project.join("ran.log").exists());

    // The failure reaches the supervisor only after it has seen the end of
    // both output pipes and the sandbox's exit.
    seams.hold(LAUNCH_STATUS);
    seams.hold(EXIT_OBSERVED);
    let ended = seams.count(OUTPUT_ENDED);
    let absent = tasks.project.join("absent");
    tasks.execute(
        "task_late",
        json!({"cmd": "echo ran > ran.log", "cwd": absent}),
        disabled,
    );
    seams.wait_for_count(OUTPUT_ENDED, ended + 2);
    seams.wait_until_waiting(EXIT_OBSERVED, 1);
    let reading = seams.waiting(LAUNCH_STATUS);
    seams.release(EXIT_OBSERVED);
    // Having seen the exit, the supervisor reads the launch-status pipe again.
    seams.wait_until_waiting(LAUNCH_STATUS, reading + 1);
    seams.release(LAUNCH_STATUS);
    let late = tasks.terminal("task_late");
    assert_eq!(summary(&late), not_run("cwd_not_found"), "{late:#}");
    assert_eq!(path(&late), json!(absent));
    assert!(!tasks.project.join("ran.log").exists());
    case.observe("cwd_not_found naming the binding's directory, which stays missing; an absolute cwd and another Session's call ran; the removal at task.launch is cwd_not_found, and after the directory came back a new call ran and the failed one did not; a failure read after the exit and both pipes' ends is cwd_not_found");
    case.pass();
}

#[test]
fn a_working_directory_the_user_cannot_enter_is_cwd_inaccessible_and_the_command_does_not_run() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a cwd of mode 0000");
    if !case.runs_as_non_root("a directory of mode 0000 keeps out only a user who is not root") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let locked = tasks.project.join("locked");
    fs::create_dir(&locked).unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    tasks.execute(
        "task_locked",
        json!({"cmd": "touch ../ran", "cwd": "locked"}),
        policy(&tasks.project, "disabled"),
    );
    let terminal = tasks.terminal("task_locked");
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        summary(&terminal),
        not_run("cwd_inaccessible"),
        "{terminal:#}"
    );
    assert_eq!(terminal["result"]["error"]["path"], json!(locked));
    assert!(!tasks.project.join("ran").exists());
    case.observe("cwd_inaccessible naming the directory; no ran file");
    case.pass();
}

#[test]
fn a_file_as_working_directory_is_cwd_not_directory() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("a cwd that is a regular file");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    fs::write(tasks.project.join("file.txt"), "x").unwrap();
    tasks.execute(
        "task_file",
        json!({"cmd": "pwd", "cwd": "file.txt"}),
        policy(&tasks.project, "disabled"),
    );
    let terminal = tasks.terminal("task_file");
    assert_eq!(
        summary(&terminal),
        not_run("cwd_not_directory"),
        "{terminal:#}"
    );
    assert_eq!(
        terminal["result"]["error"]["path"],
        json!(tasks.project.join("file.txt"))
    );
    case.observe("cwd_not_directory naming the file");
    case.pass();
}

/// A command's last output and a background process that keeps its pipes:
/// cases that only an operating system where a process can outlive the
/// shell decides. On Linux the end of the task's PID namespace closes every
/// holder of the pipes before the exit is observed, so the drain after the
/// exit has nothing left to read there.
#[cfg(target_os = "macos")]
mod pipes_past_the_exit {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::json;
    use tetral_tests::socket_directory;
    use tetral_tests::tasks::{TaskMachine, policy};

    use super::{exited, summary};

    /// Threads that keep every CPU busy until dropped, so that the App Server
    /// is often descheduled between a command's last write and its exit.
    struct CpuSpinners {
        stop: Arc<AtomicBool>,
        threads: Vec<thread::JoinHandle<()>>,
    }

    impl CpuSpinners {
        fn start() -> CpuSpinners {
            let stop = Arc::new(AtomicBool::new(false));
            let count = 2 * thread::available_parallelism().map_or(1, usize::from);
            let threads = (0..count)
                .map(|_| {
                    let stop = stop.clone();
                    thread::spawn(move || {
                        let mut spins = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            spins += 1;
                            if spins.is_multiple_of(1_024) {
                                thread::yield_now();
                            }
                        }
                    })
                })
                .collect();
            CpuSpinners { stop, threads }
        }
    }

    impl Drop for CpuSpinners {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            for thread in self.threads.drain(..) {
                let _ = thread.join();
            }
        }
    }

    #[test]
    fn the_last_bytes_a_command_writes_before_it_exits_are_in_its_terminal_report() {
        let _socket_dir = socket_directory::isolated();
        let record = tetral_tests::record!();
        let mut case = record.case(
        "fifty commands that fill the pipe's 65,536 bytes, then print a sentinel and exit, with every CPU busy",
    );
        let directory = tempfile::tempdir().unwrap();
        let tasks = TaskMachine::start(&mut case, directory.path());
        let _spinners = CpuSpinners::start();
        for index in 0..50 {
            let task = format!("task_drain_{index}");
            tasks.execute(
            &task,
            json!({"cmd": format!(r"head -c 65536 /dev/zero | tr '\0' x; printf 'final-sentinel-{index:02}\n'")}),
            policy(&tasks.project, "disabled"),
        );
            let terminal = tasks.terminal(&task);
            let text = terminal["result"]["result"]["stdout"]["text"]
                .as_str()
                .unwrap();
            assert!(
                text.ends_with(&format!("final-sentinel-{index:02}\n")),
                "{task}: …{}",
                &text[text.len().saturating_sub(64)..]
            );
        }
        case.observe("each terminal stdout ends with its sentinel");
        case.pass();
    }

    #[test]
    fn a_background_process_that_holds_the_pipes_does_not_delay_the_terminal_report() {
        let _socket_dir = socket_directory::isolated();
        let record = tetral_tests::record!();
        let mut case = record.case("printf a sentinel, then sleep 5 in the background, and exit");
        let directory = tempfile::tempdir().unwrap();
        let tasks = TaskMachine::start(&mut case, directory.path());
        let issued_at = Instant::now();
        tasks.execute(
            "task_holder",
            json!({"cmd": r"printf 'foreground-sentinel\n'; sleep 5 &"}),
            policy(&tasks.project, "disabled"),
        );
        let terminal = tasks.terminal("task_holder");
        let elapsed = issued_at.elapsed();
        assert_eq!(
            summary(&terminal),
            exited("foreground-sentinel\n"),
            "{terminal:#}"
        );
        assert!(
            elapsed < Duration::from_secs(4),
            "the terminal report after {elapsed:?}"
        );
        case.observe(format!(
            "terminal success with the sentinel {elapsed:?} after the instruction"
        ));
        case.pass();
    }
}

#[test]
fn the_app_servers_death_ends_its_running_tasks() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("the App Server killed with SIGKILL while sleep 1203 runs");
    let directory = tempfile::tempdir().unwrap();
    let mut tasks = TaskMachine::start(&mut case, directory.path());
    tasks.execute(
        "task_orphan",
        json!({"cmd": "sleep 1203"}),
        policy(&tasks.project, "disabled"),
    );
    wait_until("sleep 1203 running", || process_running(&["sleep", "1203"]));
    let mut gone_after = None;
    tasks.kill_and_restart(|_| {
        let killed = Instant::now();
        wait_until("sleep 1203 gone", || !process_running(&["sleep", "1203"]));
        gone_after = Some(killed.elapsed());
    });
    let gone_after = gone_after.unwrap();
    assert!(gone_after < Duration::from_secs(2), "{gone_after:?}");
    case.observe(format!(
        "sleep 1203 gone {gone_after:?} after the App Server's SIGKILL"
    ));
    case.pass();
}

#[test]
fn a_terminal_reports_metrics_measured_between_its_phases_on_the_controlled_clock() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "sleep 1 while the clock moves 1,000 ms before its exit is observed, and true staged behind a prepared ACK withheld for 500 ms",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let machine = tasks.machine();
    let metrics = |report: &Value| report["result"]["metrics"].clone();
    let within_wall = |metrics: &Value| {
        let phases: i64 = ["queued_ms", "drive_wait_ms", "run_ms", "sync_ms"]
            .iter()
            .map(|phase| metrics[phase].as_i64().unwrap())
            .sum();
        metrics["wall_time_ms"].as_i64().unwrap() >= phases
    };

    // The exit is observed only after the clock has moved, however long
    // the real second of the sleep takes.
    machine.seams.hold(EXIT_OBSERVED);
    let checked = machine.seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_run",
        json!({"cmd": "sleep 1"}),
        policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    machine.seams.advance(millis(1_000));
    machine.seams.release(EXIT_OBSERVED);
    let run = metrics(&tasks.terminal("task_run"));
    assert!(run["run_ms"].as_i64().unwrap() >= 1_000, "{run}");
    assert!(within_wall(&run), "{run}");

    answer_fault(
        &tasks.engine,
        json!({"endpoint": "session-status", "action": "withhold"}),
    );
    let prepared = tasks.prepare("sess_q", "bind_q", &tasks.project, None);
    tasks.wait_prepared(&prepared);
    let routed = machine.routed();
    tasks.execute_with(
        "sess_q",
        "bind_q",
        "task_queued",
        json!({"cmd": "true"}),
        policy(&tasks.project, "disabled"),
        None,
    );
    wait_until("the instruction routed to its Session", || {
        machine.routed() > routed
    });
    machine.seams.advance(millis(500));
    tasks.engine.control(
        "answer",
        json!({"endpoint": "session-status", "action": "release"}),
    );
    let queued = metrics(&tasks.terminal("task_queued"));
    assert!(queued["queued_ms"].as_i64().unwrap() >= 500, "{queued}");
    assert!(within_wall(&queued), "{queued}");
    case.observe(format!("sleep 1: {run}; staged true: {queued}"));
    case.pass();
}

#[test]
fn a_tasks_reports_and_records_carry_its_instructions_trace_context_and_an_unusable_one_none() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a prepare and an execute with a valid traceparent, an execute resent after a stream resume, and executes with an all-zero trace ID and a 10-character traceparent",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let machine = tasks.machine();
    let seams = &machine.seams;
    let request_traceparents = |endpoint: &str, report_ids: &[Value]| -> Vec<Value> {
        tasks
            .engine
            .requests(endpoint)
            .into_iter()
            .filter(|request| report_ids.contains(&request["message_id"]))
            .map(|request| request["traceparent"].clone())
            .collect()
    };

    let prepare_trace = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let prepared = tasks.prepare("sess_t", "bind_t", &tasks.project, Some(prepare_trace));
    tasks.wait_prepared(&prepared);
    let prepared_reports: Vec<Value> = tasks
        .engine
        .received("session-status")
        .into_iter()
        .filter(|received| received["body"]["request_message_id"] == prepared.as_str())
        .map(|received| received["message_id"].clone())
        .collect();
    let sent = request_traceparents("session-status", &prepared_reports);
    assert!(
        !sent.is_empty() && sent.iter().all(|value| value == prepare_trace),
        "{sent:?}"
    );

    for (task, traceparent, trace_id) in [
        (
            "task_traced",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            json!("4bf92f3577b34da6a3ce929d0e0e4736"),
        ),
        (
            "task_zero",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            Value::Null,
        ),
        ("task_short", "0123456789", Value::Null),
    ] {
        // A log that cannot be written gives the task a record of its own.
        seams.hold(LAUNCH);
        let message_id = tasks.execute_with(
            SESSION,
            BINDING,
            task,
            json!({"cmd": "echo run >> runs; printf complete"}),
            policy(&tasks.project, "direct"),
            Some(traceparent),
        );
        seams.wait_until_waiting(LAUNCH, 1);
        let [task_directory] = &tasks.task_directories(SESSION)[..] else {
            panic!("one task directory")
        };
        fs::set_permissions(
            task_directory.join("stdout.log"),
            fs::Permissions::from_mode(0o400),
        )
        .unwrap();
        if task == "task_traced" {
            resend_after_resume(&tasks, &message_id);
            case.observe("the open execute resent after the stream's resume was not routed again");
        }
        seams.release(LAUNCH);
        let terminal = tasks.terminal(task);
        assert_eq!(summary(&terminal), exited("complete"), "{terminal:#}");
        tasks.wait_files_removed(SESSION);
        let report_ids: Vec<Value> = tasks
            .engine
            .received("tool-status")
            .into_iter()
            .filter(|received| received["body"]["task_id"] == task)
            .map(|received| received["message_id"].clone())
            .collect();
        let expected_header = if trace_id.is_null() {
            Value::Null
        } else {
            json!(traceparent)
        };
        let sent = request_traceparents("tool-status", &report_ids);
        assert!(
            !sent.is_empty() && sent.iter().all(|value| *value == expected_header),
            "{task}: {sent:?}"
        );
        let records: Vec<Value> = tasks
            .records("task.output_write_failed")
            .into_iter()
            .filter(|record| record["task.id"] == task)
            .collect();
        assert_eq!(records.len(), 1, "{task}: {records:?}");
        assert_eq!(records[0]["trace_id"], trace_id, "{task}: {:?}", records[0]);
        case.observe(format!(
            "{task}: reports sent with traceparent {expected_header}, its record's trace_id {trace_id}"
        ));
    }
    assert_eq!(
        fs::read_to_string(tasks.project.join("runs")).unwrap(),
        "run\nrun\nrun\n"
    );
    case.pass();
}

/// Breaks the machine's stream, waits for its resume, and writes the
/// instruction `message_id` again on the resumed stream, which the CLI must
/// take as the instruction it already has.
fn resend_after_resume(tasks: &TaskMachine, message_id: &str) {
    let machine = tasks.machine();
    let streams = tasks.engine.state()["connections"][0]["streams"].clone();
    tasks
        .engine
        .control("break_stream", json!({"instance_id": INSTANCE}));
    machine.wait_for("resuming");
    machine.seams.advance(seconds(1.2));
    wait_until("the stream resumed", || {
        let connection = &tasks.engine.state()["connections"][0];
        connection["streams"] != streams && connection["stream_open"] == true
    });
    machine.wait_for("online");
    let routed = machine.routed();
    tasks.engine.control(
        "send",
        json!({"instance_id": INSTANCE, "resend": message_id}),
    );
    // Events are handled in stream order: once the event after the resent
    // instruction has its protocol error, the instruction has been taken.
    tasks
        .engine
        .control("send", json!({"instance_id": INSTANCE, "data": "not json"}));
    wait_for_received(&tasks.engine, "protocol-error", 1);
    assert_eq!(
        machine.routed(),
        routed,
        "the resent instruction routed again"
    );
}
