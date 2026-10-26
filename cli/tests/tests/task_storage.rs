//! A task's output and files between `tetral server run`, whose ready
//! advertises `tool_execution`, and the reference control server as
//! separate processes, with real restricted processes: output far beyond
//! what a stream keeps, cut on character boundaries; a task whose files
//! cannot be created and a log that cannot be written; task files that no
//! other task reaches and a `TMPDIR` that holds files and sockets; the
//! files' removal once the reports are acknowledged, with no descriptor of
//! the task's PID namespace left in the App Server; and `T` after the App
//! Server's death. The test seams these tests use are those
//! `tetral_tests::tasks` names.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::answer_fault;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{LAUNCH, SESSION, TaskMachine, WAIT_CHECKED, millis, policy};

fn stdout(report: &Value) -> &Value {
    &report["result"]["result"]["stdout"]
}

/// The links of the App Server's descriptors that name a PID namespace.
#[cfg(target_os = "linux")]
fn namespace_descriptors(app_server: u32) -> Vec<String> {
    fs::read_dir(format!("/proc/{app_server}/fd"))
        .unwrap()
        .filter_map(|entry| fs::read_link(entry.ok()?.path()).ok())
        .map(|link| link.to_string_lossy().into_owned())
        .filter(|link| link.starts_with("pid:["))
        .collect()
}

fn only(paths: Vec<PathBuf>) -> PathBuf {
    let [path] = &paths[..] else {
        panic!("one entry: {paths:?}")
    };
    path.clone()
}

#[test]
fn output_beyond_what_a_stream_keeps_completes_with_its_head_and_tail_cut_on_character_boundaries()
{
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("50,000,000 bytes of a, then 30,000 three-byte characters, with no limits");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    tasks.execute(
        "task_large",
        json!({"cmd": r"head -c 50000000 /dev/zero | tr '\0' a"}),
        policy(&tasks.project, "disabled"),
    );
    let large = tasks.terminal("task_large");
    let half = "a".repeat(25_600);
    assert_eq!(
        stdout(&large),
        &json!({
            "text": format!("{half}\n[... 49948800 bytes truncated ...]\n{half}"),
            "total_bytes": 50_000_000, "total_lines": 0,
            "returned_bytes": 51_200, "truncated": true,
        })
    );
    assert_eq!(large["result"]["result"]["exit_code"], 0);
    assert_eq!(large["result"]["truncated"], true);

    tasks.execute(
        "task_characters",
        json!({"cmd": "printf '€%.0s' $(seq 1 30000)"}),
        policy(&tasks.project, "disabled"),
    );
    let characters = tasks.terminal("task_characters");
    // 25,600 bytes end inside the 8,534th character: each part keeps 8,533.
    let half = "€".repeat(8_533);
    assert_eq!(
        stdout(&characters),
        &json!({
            "text": format!("{half}\n[... 38802 bytes truncated ...]\n{half}"),
            "total_bytes": 90_000, "total_lines": 0,
            "returned_bytes": 51_198, "truncated": true,
        })
    );
    assert_eq!(characters["result"]["truncated"], true);

    tasks.execute(
        "task_small",
        json!({"cmd": "echo ok"}),
        policy(&tasks.project, "disabled"),
    );
    let small = tasks.terminal("task_small");
    assert_eq!(
        json!([stdout(&small)["truncated"], small["result"]["truncated"]]),
        json!([false, false])
    );
    case.observe("the 50,000,000 bytes completed with 25,600 bytes, the marker and 25,600 bytes; the characters were cut to 8,533 whole characters on each side; both results were truncated, and the result of a short output was not");
}

#[test]
fn a_task_whose_files_cannot_be_created_never_starts_and_a_failing_log_leaves_its_report_complete()
{
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a read-only sessions/<session_id>/tasks/, then a stdout.log made read-only at task.launch",
    );
    if !case.runs_as_non_root("a read-only directory stops only a user who is not root") {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let parent = tasks.root.join("sessions").join(SESSION).join("tasks");
    fs::create_dir_all(&parent).unwrap();
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o500)).unwrap();
    tasks.execute(
        "task_unstored",
        json!({"cmd": "touch ran"}),
        policy(&tasks.project, "disabled"),
    );
    let unstored = tasks.terminal("task_unstored");
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
    let error = &unstored["result"]["error"];
    assert_eq!(
        (&unstored["result"]["status"], &error["kind"]),
        (&json!("error"), &json!("exec_spawn_failed")),
        "{unstored:#}"
    );
    let message = error["message"].as_str().unwrap();
    assert!(message.contains(parent.to_str().unwrap()), "{message}");
    assert_eq!(seams.count("process.spawn"), 0);
    assert!(!tasks.project.join("ran").exists());

    seams.hold(LAUNCH);
    // More output than one read of the pipe takes.
    tasks.execute(
        "task_unlogged",
        json!({"cmd": r"head -c 200000 /dev/zero | tr '\0' a; printf complete-output"}),
        policy(&tasks.project, "direct"),
    );
    seams.wait_until_waiting(LAUNCH, 1);
    let log = only(tasks.task_directories(SESSION)).join("stdout.log");
    fs::set_permissions(&log, fs::Permissions::from_mode(0o400)).unwrap();
    seams.release(LAUNCH);
    let unlogged = tasks.terminal("task_unlogged");
    let output = stdout(&unlogged);
    assert_eq!(output["total_bytes"], 200_015, "{unlogged:#}");
    assert!(
        output["text"]
            .as_str()
            .is_some_and(|text| text.ends_with("complete-output")),
        "{unlogged:#}"
    );
    let records: Vec<Value> = tasks
        .records("task.output_write_failed")
        .into_iter()
        .filter(|record| record["task.id"] == "task_unlogged")
        .collect();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(
        (&records[0]["level"], &records[0]["error.code"]),
        (&json!("WARN"), &json!("output_write_failed"))
    );
    case.observe(format!("the start failed exec_spawn_failed ({message}) with process.spawn 0; the unwritable log left stdout complete, 200,015 bytes ending in complete-output, and one WARN task.output_write_failed of the task"));
}

#[test]
fn task_files_are_reachable_from_no_other_task_and_tmpdir_holds_files_and_unix_sockets() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a running task of S1 that writes $TMPDIR/x, talks over $TMPDIR/s and reads its own stdout.log, read by another task of S1 and one of S2",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let other = directory.path().join("other");
    fs::create_dir(&other).unwrap();
    let other = other.canonicalize().unwrap();
    let prepared = tasks.prepare("sess_2", "bind_2", &other, None);
    tasks.wait_prepared(&prepared);

    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_owner",
        json!({
            "cmd": r#"printf secret; echo kept > "$TMPDIR/x" && python3 -c 'import os, socket
path = os.environ["TMPDIR"] + "/s"
listener = socket.socket(socket.AF_UNIX); listener.bind(path); listener.listen(1)
client = socket.socket(socket.AF_UNIX); client.connect(path)
accepted, _ = listener.accept(); client.sendall(b"ping"); print(" socket", accepted.recv(4).decode(), flush=True)'; while [ ! -s own ]; do sleep 0.05; done; cat "$(cat own)" || echo own-denied; sleep 120"#,
            "wait_ms": 250, "on_wait_expiry": "detach", "task_lifetime_ms": 1_000,
        }),
        policy(&tasks.project, "direct"),
    );
    tasks.wait_for_start(checked);
    let log = only(tasks.task_directories(SESSION)).join("stdout.log");
    let temporary = only(tasks.temporary_directories());
    wait_until("the owner's output in its log", || {
        fs::read_to_string(&log).is_ok_and(|text| text == "secret socket ping\n")
    });
    // The owner reads the log's path from `own`, renamed into place whole,
    // and then the log itself.
    fs::write(tasks.project.join("own.part"), log.to_str().unwrap()).unwrap();
    fs::rename(tasks.project.join("own.part"), tasks.project.join("own")).unwrap();
    wait_until("the owner's read of its own log", || {
        fs::read_to_string(&log).is_ok_and(|text| text.len() > "secret socket ping\n".len())
    });
    assert_eq!(fs::read_to_string(temporary.join("x")).unwrap(), "kept\n");

    let readers = format!(
        "cat '{}' || echo log-denied; cat '{}' || echo tmp-denied",
        log.display(),
        temporary.join("x").display()
    );
    tasks.execute(
        "task_same_session",
        json!({"cmd": readers}),
        policy(&tasks.project, "disabled"),
    );
    tasks.execute_with(
        "sess_2",
        "bind_2",
        "task_other_session",
        json!({"cmd": readers}),
        policy(&other, "disabled"),
        None,
    );
    for task in ["task_same_session", "task_other_session"] {
        let terminal = tasks.terminal(task);
        assert_eq!(
            (
                &stdout(&terminal)["text"],
                &terminal["result"]["result"]["exit_code"]
            ),
            (&json!("log-denied\ntmp-denied\n"), &json!(0)),
            "{task}: {terminal:#}"
        );
    }
    tasks.advance(millis(1_000));
    let owner = tasks.terminal("task_owner");
    assert_eq!(stdout(&owner)["text"], "secret socket ping\nown-denied\n");
    case.observe("the owner wrote $TMPDIR/x, exchanged ping over $TMPDIR/s and could not read its own stdout.log; another task of its Session and a task of another Session could read neither its stdout.log nor $TMPDIR/x");
}

#[test]
fn a_tasks_files_go_once_its_reports_are_acknowledged_and_its_project_files_stay() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("touch kept, its terminal report's ACK withheld, then released");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "tool-status", "action": "withhold"}),
    );
    tasks.execute(
        "task_cleaned",
        json!({"cmd": "touch kept; printf done"}),
        policy(&tasks.project, "disabled"),
    );
    tasks.terminal("task_cleaned");
    assert_eq!(tasks.task_directories(SESSION).len(), 1);
    assert_eq!(tasks.temporary_directories().len(), 1);
    tasks.engine.control(
        "answer",
        json!({"endpoint": "tool-status", "action": "release"}),
    );
    tasks.wait_files_removed(SESSION);
    assert!(tasks.project.join("kept").exists());
    #[cfg(target_os = "linux")]
    assert_eq!(
        namespace_descriptors(tasks.machine().app_server.pid()),
        Vec::<String>::new()
    );
    case.observe("both directories stayed while the ACK was withheld and went after it; kept stayed in the project; the App Server kept no descriptor of the task's PID namespace");
}

#[test]
fn after_the_app_server_dies_during_a_task_its_next_start_empties_t() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("the App Server killed with SIGKILL during sleep 120, then started again");
    let directory = tempfile::tempdir().unwrap();
    let mut tasks = TaskMachine::start(&mut case, directory.path());
    let checked = tasks.machine().seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_left",
        json!({"cmd": "sleep 120"}),
        policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    let mut left = Vec::new();
    tasks.kill_and_restart(|tasks| left = tasks.temporary_directories());
    assert_eq!(left.len(), 1, "{left:?}");
    assert_eq!(tasks.temporary_directories(), Vec::<PathBuf>::new());
    case.observe("T held the task's temporary directory after the kill and was empty once the App Server was online again");
}
