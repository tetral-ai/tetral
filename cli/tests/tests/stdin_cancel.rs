//! `tool.stdin` and `tool.cancel` between `tetral server run`, whose ready
//! advertises `tool_execution`, and the reference control server as
//! separate processes, each on its own controlled clock, with real
//! restricted processes: reads and their waits, writes recorded by
//! `write_seq` and never repeated, writes to a full pipe, U+0003, the
//! control errors, cancels, and replies and `terminal` reports delivered
//! and acknowledged apart. The test seams these tests use are those
//! `tetral_tests::tasks` and `tetral_tests::machine` name.

use std::fs;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{answer_fault, seconds};
use tetral_tests::socket_directory;
use tetral_tests::tasks::{
    OUTPUT_READ, REPLY_CHECKED, REPLY_WAITING, TaskMachine, WAIT_CHECKED, WRITE_STARTED, millis,
    policy,
};

/// What a control reply says.
fn summary(reply: &Value) -> Value {
    let envelope = &reply["result"];
    let result = &envelope["result"];
    json!({
        "error": reply["error"]["code"],
        "tool": envelope["tool"],
        "status": envelope["status"],
        "exit_code": result["exit_code"],
        "signal": result["signal"],
        "written_bytes": result["written_bytes"],
        "stdout": result["stdout"]["text"],
    })
}

/// A reply of a task still running, with `stdout` and, for a write,
/// `written_bytes`.
fn running(stdout: &str, written_bytes: Value) -> Value {
    json!({
        "error": null, "tool": "stdin", "status": "running", "exit_code": null,
        "signal": null, "written_bytes": written_bytes, "stdout": stdout,
    })
}

/// Starts `cmd` under `detach` with a wait of 250 ms, moves the clock to
/// its `running` report and gives that report.
fn detached(tasks: &TaskMachine, task: &str, cmd: &str, lifetime_ms: u64) -> Value {
    let seams = &tasks.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        task,
        json!({"cmd": cmd, "wait_ms": 250, "on_wait_expiry": "detach", "task_lifetime_ms": lifetime_ms}),
        policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    seams.advance(millis(250));
    tasks.report(task, "running")
}

/// Issues a `tool.stdin`, waits until its reply waits, and gives its
/// `message_id`.
fn waiting(tasks: &TaskMachine, task: &str, members: Value) -> String {
    let seams = &tasks.machine().seams;
    let waiting = seams.count(REPLY_WAITING);
    let message_id = tasks.stdin(task, members);
    seams.wait_for_count(REPLY_WAITING, waiting + 1);
    message_id
}

/// Moves the clock by `step` and waits until the timer of the task's
/// waiting replies has read the new time.
fn advance_reply_timer(tasks: &TaskMachine, step: Duration) {
    let seams = &tasks.machine().seams;
    seams.advance(step);
    let checked = seams.count(REPLY_CHECKED);
    seams.wait_for_count(REPLY_CHECKED, checked + 2);
}

/// A write of `chars` as `write_seq`, its 250-millisecond wait moved past,
/// and its reply.
fn write(tasks: &TaskMachine, task: &str, write_seq: i64, chars: &str) -> Value {
    let message_id = waiting(tasks, task, json!({"chars": chars, "write_seq": write_seq}));
    tasks.machine().seams.advance(millis(250));
    tasks.reply(&message_id)
}

/// The SHA-256 of each `tool-status` request that carried `message_id`.
fn sent_bodies(tasks: &TaskMachine, message_id: &Value) -> Vec<Value> {
    tasks
        .engine
        .requests("tool-status")
        .into_iter()
        .filter(|request| request["message_id"] == *message_id)
        .map(|request| request["body_sha256"].clone())
        .collect()
}

/// Once the outbox has applied the answer to the attempt of the report
/// `message_id` that followed `answered` answers, moves the clock past the
/// first wait of its retry backoff (1 second with at most 20% of jitter)
/// and gives the SHA-256 of each send of the report.
fn sent_again(tasks: &TaskMachine, answered: u64, message_id: &Value) -> Vec<Value> {
    let seams = &tasks.machine().seams;
    seams.wait_for_count("outbox.answered", answered + 1);
    seams.advance(seconds(1.5));
    wait_until(&format!("{message_id} sent again"), || {
        sent_bodies(tasks, message_id).len() >= 2
    });
    sent_bodies(tasks, message_id)
}

#[test]
fn a_write_is_recorded_by_its_write_seq_and_its_lost_reply_is_sent_again_unchanged() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "cat > f: write 1 with its reply's answer dropped, a read, write 2, write 1 again, write 4, then write 3",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    detached(&tasks, "task_cat", "cat > f", 1_800_000);
    let file = || fs::read_to_string(tasks.project.join("f")).unwrap_or_default();

    let answered = tasks.answers_settled();
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "tool-status", "action": "drop"}),
    );
    let first = waiting(
        &tasks,
        "task_cat",
        json!({"chars": "one\n", "write_seq": 1}),
    );
    seams.advance(millis(250));
    let reply = tasks.reply(&first);
    assert_eq!(summary(&reply), running("", json!(4)), "{reply:#}");
    let bodies = sent_again(&tasks, answered, &reply["message_id"]);
    assert!(bodies.iter().all(|body| *body == bodies[0]), "{bodies:?}");
    wait_until("one in f", || file() == "one\n");

    let read = tasks.stdin("task_cat", json!({"chars": "", "wait_ms": 0}));
    assert_eq!(summary(&tasks.reply(&read)), running("", Value::Null));
    assert_eq!(
        summary(&write(&tasks, "task_cat", 2, "two\n")),
        running("", json!(4))
    );
    let repeated = tasks.stdin("task_cat", json!({"chars": "one\n", "write_seq": 1}));
    let repeated = tasks.reply(&repeated);
    assert_eq!(summary(&repeated), running("", json!(4)), "{repeated:#}");
    assert_eq!(
        summary(&write(&tasks, "task_cat", 4, "four\n")),
        running("", json!(5))
    );
    let stale = tasks.stdin("task_cat", json!({"chars": "three\n", "write_seq": 3}));
    let stale = tasks.reply(&stale);
    assert_eq!(
        (&stale["error"]["code"], &stale["result"]),
        (&json!("invalid_input"), &Value::Null),
        "{stale:#}"
    );
    wait_until("f complete", || file() == "one\ntwo\nfour\n");
    assert_eq!(file(), "one\ntwo\nfour\n");
    case.observe(format!(
        "write 1's reply sent {} times with one body; f holds one, two and four once each; the repeated write 1 reused written_bytes 4; write 3 below 4 was invalid_input",
        bodies.len()
    ));
}

#[test]
fn failed_controls_leave_the_task_and_its_reports_unchanged() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "cat > f after write 1: a conflicting write 1, an unknown task, stdin to an edit task and a read whose ACK names another report, then write 2",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    detached(&tasks, "task_cat", "cat > f", 1_800_000);
    assert_eq!(
        summary(&write(&tasks, "task_cat", 1, "x\n")),
        running("", json!(2))
    );

    let conflict = tasks.stdin("task_cat", json!({"chars": "y\n", "write_seq": 1}));
    let conflict = tasks.reply(&conflict);
    assert_eq!(
        (&conflict["error"]["code"], &conflict["result"]),
        (&json!("write_conflict"), &Value::Null)
    );
    let unknown = tasks.stdin("task_unknown", json!({"chars": "", "wait_ms": 0}));
    assert_eq!(tasks.reply(&unknown)["error"]["code"], "task_not_found");
    tasks.engine.control(
        "execute",
        json!({"instance_id": tetral_tests::machine::INSTANCE, "payload": {
            "session_id": tetral_tests::tasks::SESSION, "binding_id": tetral_tests::tasks::BINDING,
            "tool_use_event_id": "call_task_edit", "task_id": "task_edit", "tool": "edit",
            "input": {"path": "e.txt", "old_string": "a", "new_string": "b"},
            "sandbox_policy": policy(&tasks.project, "disabled"),
        }}),
    );
    let unsupported = tasks.stdin("task_edit", json!({"chars": "", "wait_ms": 0}));
    assert_eq!(
        tasks.reply(&unsupported)["error"]["code"],
        "unsupported_control"
    );
    // The edit's own report, so that the fault below meets the read's reply.
    tasks.terminal("task_edit");

    let answered = tasks.answers_settled();
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "tool-status", "action": "wrong_identity"}),
    );
    let read = tasks.stdin("task_cat", json!({"chars": "", "wait_ms": 0}));
    let read = tasks.reply(&read);
    assert_eq!(summary(&read), running("", Value::Null), "{read:#}");
    let bodies = sent_again(&tasks, answered, &read["message_id"]);
    assert!(bodies.iter().all(|body| *body == bodies[0]), "{bodies:?}");

    // Writes reach f in order, so f holding z after x alone shows that the
    // conflicting write wrote nothing.
    assert_eq!(
        summary(&write(&tasks, "task_cat", 2, "z\n")),
        running("", json!(2))
    );
    let in_f = || fs::read_to_string(tasks.project.join("f")).unwrap_or_default();
    wait_until("z in f", || in_f().ends_with("z\n"));
    assert_eq!(in_f(), "x\nz\n");
    let reports: Vec<Value> = tasks
        .reports("task_cat")
        .iter()
        .map(|report| report["phase"].clone())
        .collect();
    assert_eq!(reports, ["running"]);
    case.observe(format!(
        "write_conflict, task_not_found and unsupported_control with no result; the read reply under a wrong-identity ACK sent {} times unchanged; f holds x and z; the task's one report is running",
        bodies.len()
    ));
}

#[test]
fn a_cancel_replies_with_the_cumulative_output_and_repeated_cancels_join_one_termination() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "trap '' TERM; printf abcdef; cat > f with its window read: two cancels 1 second apart, the first with visible_bytes 4, and one after the exit",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let output = seams.count(OUTPUT_READ);
    detached(
        &tasks,
        "task_trap",
        "trap '' TERM; printf abcdef; cat > f",
        1_800_000,
    );
    seams.wait_for_count(OUTPUT_READ, output + 1);
    let read = tasks.stdin("task_trap", json!({"chars": "", "wait_ms": 0}));
    tasks.reply(&read);

    let cancel = |members: Value| {
        let message_id = tasks.cancel("task_trap", members);
        tasks.reply(&message_id)
    };
    let cancelled = |reply: &Value| {
        let result = &reply["result"]["result"];
        json!([
            reply["result"]["tool"],
            result["cancelled"],
            result["was_running"],
            result["signal"]
        ])
    };
    let first = cancel(json!({"limits": {"visible_bytes": 4}}));
    assert_eq!(cancelled(&first), json!(["cancel", true, true, null]));
    assert_eq!(
        first["result"]["result"]["stdout"],
        json!({"text": "ab\n[... 2 bytes truncated ...]\nef", "total_bytes": 6, "total_lines": 0, "returned_bytes": 4, "truncated": true}),
        "{first:#}"
    );
    // The second cancel joins the termination that the first began, whose
    // SIGKILL comes 2 seconds after the first.
    tasks.advance(seconds(1.0));
    assert_eq!(
        cancelled(&cancel(json!({}))),
        json!(["cancel", true, true, null])
    );
    tasks.advance(seconds(1.0));
    let terminal = tasks.terminal("task_trap");
    let result = &terminal["result"]["result"];
    assert_eq!(
        json!([
            terminal["report_seq"],
            result["cancelled"],
            result["signal"],
            result["exit_code"]
        ]),
        json!([2, true, "killed", null]),
        "{terminal:#}"
    );
    let after = cancel(json!({}));
    assert_eq!(cancelled(&after), json!(["cancel", false, false, "killed"]));
    let reports: Vec<Value> = tasks
        .reports("task_trap")
        .iter()
        .map(|report| report["phase"].clone())
        .collect();
    assert_eq!(reports, ["running", "terminal"]);
    case.observe("the first cancel cancelled and running, with 4 of the 6 bytes already read; the second, 1 second later while the TERM was ignored, cancelled and running too; one terminal report killed 2 seconds after the first; a cancel after it cancelled false with signal killed");
}

#[test]
fn reads_and_writes_wait_as_long_as_their_bounds_and_never_extend_the_task() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a read with wait_ms 0, one that waits for output, one whose wait ends, a write's wait, and reads repeated over a lifetime of 1,000 ms",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    detached(
        &tasks,
        "task_out",
        "while [ ! -e go ]; do sleep 0.05; done; echo out; sleep 120",
        1_800_000,
    );

    // The best of three, since the time includes both servers' scheduling.
    let mut elapsed = Duration::MAX;
    for _ in 0..3 {
        let issued = Instant::now();
        let immediate = tasks.stdin("task_out", json!({"chars": "", "wait_ms": 0}));
        let immediate = tasks.reply(&immediate);
        elapsed = elapsed.min(issued.elapsed());
        assert_eq!(summary(&immediate), running("", Value::Null));
    }
    assert!(
        elapsed < Duration::from_millis(100),
        "the reply after {elapsed:?}"
    );

    let first_output = waiting(&tasks, "task_out", json!({"chars": ""}));
    fs::write(tasks.project.join("go"), "").unwrap();
    assert_eq!(
        summary(&tasks.reply(&first_output)),
        running("out\n", Value::Null)
    );

    // The task started 250 ms before these waits, so a reply's duration_ms
    // tells the time it was made.
    let ended = waiting(&tasks, "task_out", json!({"chars": ""}));
    advance_reply_timer(&tasks, millis(4_999));
    seams.advance(millis(1));
    let ended = tasks.reply(&ended);
    assert_eq!(summary(&ended), running("", Value::Null));
    assert_eq!(ended["result"]["result"]["duration_ms"], 5_250);

    let written = waiting(&tasks, "task_out", json!({"chars": "x", "write_seq": 1}));
    advance_reply_timer(&tasks, millis(249));
    seams.advance(millis(1));
    let written = tasks.reply(&written);
    assert_eq!(summary(&written), running("", json!(1)));
    assert_eq!(written["result"]["result"]["duration_ms"], 5_500);

    // The running report comes 250 ms after the start; three reads of 250 ms
    // reach the lifetime's end, where the third one's reply races the
    // termination.
    detached(&tasks, "task_short", "sleep 120", 1_000);
    for read in 0..3 {
        let message_id = waiting(&tasks, "task_short", json!({"chars": "", "wait_ms": 250}));
        seams.advance(millis(250));
        let reply = tasks.reply(&message_id);
        if read < 2 {
            assert_eq!(summary(&reply), running("", Value::Null));
        }
    }
    let terminal = tasks.terminal("task_short");
    assert_eq!(
        (
            &terminal["result"]["result"]["duration_ms"],
            &terminal["result"]["result"]["timed_out"]
        ),
        (&json!(1_000), &json!(true)),
        "{terminal:#}"
    );
    case.observe(format!("the immediate read after {elapsed:?}; the waiting read at the first output; the waiting read at 5,000 ms and not at 4,999 (duration_ms 5,250); the write's reply at 250 ms and not at 249 (duration_ms 5,500); three reads of 250 ms, then the terminal report at 1,000 ms with timed_out"));
}

#[test]
fn a_lost_read_reply_returns_the_same_output_and_the_next_read_the_next_window() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "A before the running report, B read with its reply's answer dropped, C read next, then the exit",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let running_report = detached(
        &tasks,
        "task_windows",
        "printf A; while [ ! -e b ]; do sleep 0.05; done; printf B; while [ ! -e c ]; do sleep 0.05; done; printf C; while [ ! -e end ]; do sleep 0.05; done",
        1_800_000,
    );
    assert_eq!(running_report["result"]["result"]["stdout"]["text"], "A");

    let answered = tasks.answers_settled();
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "tool-status", "action": "drop"}),
    );
    let lost = waiting(&tasks, "task_windows", json!({"chars": ""}));
    fs::write(tasks.project.join("b"), "").unwrap();
    let lost = tasks.reply(&lost);
    assert_eq!(summary(&lost), running("B", Value::Null), "{lost:#}");
    let bodies = sent_again(&tasks, answered, &lost["message_id"]);
    assert!(bodies.iter().all(|body| *body == bodies[0]), "{bodies:?}");

    // C is in the window before the read: the read's 5,000 ms wait, which
    // the clock never reaches, does not delay its reply.
    let output = seams.count(OUTPUT_READ);
    fs::write(tasks.project.join("c"), "").unwrap();
    seams.wait_for_count(OUTPUT_READ, output + 1);
    let next = tasks.stdin("task_windows", json!({"chars": ""}));
    assert_eq!(summary(&tasks.reply(&next)), running("C", Value::Null));
    fs::write(tasks.project.join("end"), "").unwrap();
    let terminal = tasks.terminal("task_windows");
    assert_eq!(terminal["result"]["result"]["stdout"]["text"], "ABC");
    case.observe(format!("the running report held A; the read reply holding B was sent {} times unchanged; the next read, with C already read, replied at once with only C; the terminal report held ABC", bodies.len()));
}

#[test]
fn a_reply_and_the_terminal_report_of_an_exit_during_its_wait_are_acknowledged_apart() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a read waiting when the command exits, its reply's ACK withheld, then a write to the ended task",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    detached(
        &tasks,
        "task_exit",
        "while [ ! -e go ]; do sleep 0.05; done; exit 3",
        1_800_000,
    );
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "tool-status", "action": "withhold"}),
    );
    let read = waiting(&tasks, "task_exit", json!({"chars": ""}));
    fs::write(tasks.project.join("go"), "").unwrap();
    let terminal = tasks.terminal("task_exit");
    assert_eq!(terminal["result"]["result"]["exit_code"], 3);
    // The server accepted the reply and holds its ACK; the CLI has none.
    let reply = tasks.reply(&read);
    assert_eq!(
        json!([
            reply["result"]["status"],
            reply["result"]["result"]["exit_code"]
        ]),
        json!(["success", 3]),
        "{reply:#}"
    );
    let closed = tasks.stdin("task_exit", json!({"chars": "late", "write_seq": 1}));
    let closed = tasks.reply(&closed);
    assert_eq!(
        summary(&closed),
        json!({
            "error": "stdin_closed", "tool": "stdin", "status": "error", "exit_code": 3,
            "signal": null, "written_bytes": 0, "stdout": "",
        }),
        "{closed:#}"
    );
    for _ in 0..2 {
        let read = tasks.stdin("task_exit", json!({"chars": ""}));
        let read = tasks.reply(&read);
        assert_eq!(
            json!([
                read["result"]["status"],
                read["result"]["result"]["exit_code"]
            ]),
            json!(["success", 3]),
            "{read:#}"
        );
    }
    assert_eq!(
        tasks.task_directories(tetral_tests::tasks::SESSION).len(),
        1
    );
    tasks.engine.control(
        "answer",
        json!({"endpoint": "tool-status", "action": "release"}),
    );
    wait_until("the task's files removed", || {
        tasks
            .task_directories(tetral_tests::tasks::SESSION)
            .is_empty()
    });
    case.observe("the terminal report arrived while the read reply's ACK was held; the reply said success with exit_code 3; a write to the ended task was stdin_closed with the exit facts; two reads of it replied at once with success and exit_code 3; the task's directory stayed until the held ACK was released");
}

#[test]
fn write_replies_take_the_window_of_their_wait_within_their_limits() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that prints got: for each line, then the line once a file of its name exists: write 1, write 2 with visible_bytes 4, and write 1 again while write 2's reply waits",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    detached(
        &tasks,
        "task_echo",
        "while IFS= read -r line; do printf got:; while [ ! -e \"$line\" ]; do sleep 0.05; done; printf '%s\\n' \"$line\"; done",
        1_800_000,
    );
    // A write of `line` whose output, `got:` and then `line` once the file
    // `line` exists, has been read in two parts while its reply waits.
    let written = |write_seq: i64, line: &str, limits: Value| {
        let output = seams.count(OUTPUT_READ);
        let message_id = waiting(
            &tasks,
            "task_echo",
            json!({"chars": format!("{line}\n"), "write_seq": write_seq, "limits": limits}),
        );
        seams.wait_for_count(OUTPUT_READ, output + 1);
        fs::write(tasks.project.join(line), "").unwrap();
        seams.wait_for_count(OUTPUT_READ, output + 2);
        message_id
    };
    let stdout = |reply: &Value| reply["result"]["result"]["stdout"].clone();

    let first = written(1, "one", json!({}));
    seams.advance(millis(250));
    let first = tasks.reply(&first);
    assert_eq!(summary(&first), running("got:one\n", json!(4)), "{first:#}");
    let second = written(2, "two", json!({"visible_bytes": 4}));
    let repeated = tasks.stdin("task_echo", json!({"chars": "one\n", "write_seq": 1}));
    let repeated = tasks.reply(&repeated);
    assert_eq!(summary(&repeated), running("", json!(4)), "{repeated:#}");
    assert_eq!(
        stdout(&repeated),
        json!({"text": "", "total_bytes": 16, "total_lines": 2, "returned_bytes": 0, "truncated": false})
    );
    seams.advance(millis(250));
    let second = tasks.reply(&second);
    assert_eq!(
        stdout(&second),
        json!({"text": "go\n[... 4 bytes truncated ...]\no\n", "total_bytes": 16, "total_lines": 2, "returned_bytes": 4, "truncated": true}),
        "{second:#}"
    );

    case.observe("write 1's reply held got:one, read in two parts during its wait; write 1 repeated while got:two waited in the window reported the counters 16 bytes and 2 lines with no text; write 2's reply then held 4 of got:two's 8 bytes");
}

#[test]
fn a_write_to_a_full_pipe_ends_stdin_closed_with_the_bytes_written_and_is_never_repeated() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "writes of 32,768, 16,384 and 32,768 bytes to a command that does not read, write 3 again, then the command counts its stdin",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    detached(
        &tasks,
        "task_full",
        "while [ ! -e go ]; do sleep 0.05; done; wc -c",
        1_800_000,
    );
    let waiting_count = seams.count(REPLY_WAITING);
    let first = tasks.stdin(
        "task_full",
        json!({"chars": "a".repeat(32_768), "write_seq": 1}),
    );
    let second = tasks.stdin(
        "task_full",
        json!({"chars": "b".repeat(16_384), "write_seq": 2}),
    );
    seams.wait_for_count(REPLY_WAITING, waiting_count + 2);
    let started = seams.count(WRITE_STARTED);
    let third = tasks.stdin(
        "task_full",
        json!({"chars": "c".repeat(32_768), "write_seq": 3}),
    );
    seams.wait_for_count(WRITE_STARTED, started + 1);
    seams.advance(millis(250));
    for (message_id, written) in [(&first, 32_768), (&second, 16_384)] {
        assert_eq!(
            summary(&tasks.reply(message_id)),
            running("", json!(written))
        );
    }
    let third = tasks.reply(&third);
    let partial = third["result"]["result"]["written_bytes"]
        .as_u64()
        .unwrap_or(0);
    assert_eq!(third["error"]["code"], "stdin_closed", "{third:#}");
    assert!(partial < 32_768, "{third:#}");
    let again = tasks.stdin(
        "task_full",
        json!({"chars": "c".repeat(32_768), "write_seq": 3}),
    );
    let again = tasks.reply(&again);
    assert_eq!(again["error"]["code"], "stdin_closed");
    if partial > 0 {
        assert_eq!(again["result"]["result"]["written_bytes"], partial);
    }
    assert_eq!(
        tasks.reports("task_full").len(),
        1,
        "no terminal report before the exit"
    );

    fs::write(tasks.project.join("go"), "").unwrap();
    let terminal = tasks.terminal("task_full");
    assert_eq!(
        terminal["result"]["result"]["stdout"]["text"],
        format!("{}\n", 32_768 + 16_384 + partial)
    );
    case.observe(format!("writes 1 and 2 written whole; write 3 ended stdin_closed after {partial} bytes, and its repeat wrote nothing; the command counted {} bytes; no terminal report before its exit", 32_768 + 16_384 + partial));
}

#[test]
fn writes_sent_together_are_written_in_write_seq_order() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a command that reads stdin into f once go exists: two writes that fill its pipe, then writes 3, 4 and 5 sent together and go",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    detached(
        &tasks,
        "task_cat",
        "while [ ! -e go ]; do sleep 0.05; done; cat > f",
        1_800_000,
    );
    let waiting_count = seams.count(REPLY_WAITING);
    for write_seq in 1..=2 {
        tasks.stdin(
            "task_cat",
            json!({"chars": "a".repeat(32_768), "write_seq": write_seq}),
        );
    }
    seams.wait_for_count(REPLY_WAITING, waiting_count + 2);
    // The pipe is full: write 3 blocks, and 4 and 5 wait behind it, as the
    // read handled after them shows.
    let started = seams.count(WRITE_STARTED);
    let queued: Vec<String> = (3..=5)
        .map(|write_seq| {
            tasks.stdin(
                "task_cat",
                json!({"chars": write_seq.to_string(), "write_seq": write_seq}),
            )
        })
        .collect();
    seams.wait_for_count(WRITE_STARTED, started + 1);
    let read = tasks.stdin("task_cat", json!({"chars": "", "wait_ms": 0}));
    tasks.reply(&read);
    fs::write(tasks.project.join("go"), "").unwrap();
    let in_f = || fs::read(tasks.project.join("f")).unwrap_or_default();
    wait_until("every write in f", || in_f().len() == 65_539);
    assert_eq!(&in_f()[65_536..], b"345");
    seams.wait_for_count(REPLY_WAITING, waiting_count + 5);
    seams.advance(millis(250));
    for message_id in &queued {
        assert_eq!(summary(&tasks.reply(message_id)), running("", json!(1)));
    }

    case.observe("f ended with 345 after the 65,536 bytes that filled the pipe");
}

#[test]
fn a_cancel_is_answered_during_a_blocked_write_which_ends_stdin_closed() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("sleep 120: two writes that fill its pipe, a third that blocks, then a cancel");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    detached(&tasks, "task_blocked", "sleep 120", 1_800_000);
    let waiting_count = seams.count(REPLY_WAITING);
    for write_seq in 1..=2 {
        tasks.stdin(
            "task_blocked",
            json!({"chars": "a".repeat(32_768), "write_seq": write_seq}),
        );
    }
    seams.wait_for_count(REPLY_WAITING, waiting_count + 2);
    let started = seams.count(WRITE_STARTED);
    let blocked = tasks.stdin(
        "task_blocked",
        json!({"chars": "b".repeat(32_768), "write_seq": 3}),
    );
    seams.wait_for_count(WRITE_STARTED, started + 1);
    let cancel = tasks.cancel("task_blocked", json!({}));
    let cancel = tasks.reply(&cancel);
    assert_eq!(
        json!([
            cancel["result"]["result"]["cancelled"],
            cancel["result"]["result"]["was_running"]
        ]),
        json!([true, true])
    );
    let blocked = tasks.reply(&blocked);
    assert_eq!(blocked["error"]["code"], "stdin_closed", "{blocked:#}");
    let terminal = tasks.terminal("task_blocked");
    assert_eq!(
        json!([
            terminal["result"]["result"]["cancelled"],
            terminal["result"]["result"]["signal"]
        ]),
        json!([true, "terminated"])
    );
    case.observe("the cancel was answered while write 3 was blocked, with the clock never moved; write 3 ended stdin_closed; the terminal report said cancelled, terminated");
}

#[test]
fn chars_needing_six_byte_escapes_reach_the_command_byte_for_byte() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("32,768 U+0001 characters, each escaped as \\u0001 in the instruction");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    detached(
        &tasks,
        "task_head",
        "head -c 32768 > f; echo done",
        1_800_000,
    );
    let chars = "\u{1}".repeat(32_768);
    let message_id = tasks.stdin("task_head", json!({"chars": chars, "write_seq": 1}));
    let terminal = tasks.terminal("task_head");
    assert_eq!(terminal["result"]["result"]["stdout"]["text"], "done\n");
    assert_eq!(fs::read(tasks.project.join("f")).unwrap(), chars.as_bytes());
    let reply = tasks.reply(&message_id);
    assert_eq!(reply["result"]["result"]["written_bytes"], 32_768);
    let sent = tasks.engine.state()["connections"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|connection| connection["issued"].as_array().cloned().unwrap_or_default())
        .find(|issued| issued["message_id"] == message_id.as_str())
        .unwrap();
    case.observe(format!(
        "f holds the 32,768 bytes; the instruction was issued as {sent}"
    ));
}

#[test]
fn u_0003_interrupts_the_commands_process_group_and_the_shell_goes_on() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("trap 'echo i >> m' INT; sleep 100; echo after, then U+0003");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    detached(
        &tasks,
        "task_trap",
        "trap 'echo i >> m' INT; sleep 100; echo after",
        1_800_000,
    );
    let sent = Instant::now();
    let message_id = tasks.stdin("task_trap", json!({"chars": "\u{3}", "write_seq": 1}));
    let terminal = tasks.terminal("task_trap");
    let elapsed = sent.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "the terminal report after {elapsed:?}"
    );
    // The sandbox's first process passes SIGINT on to the command's process
    // group and to the shell itself, so the trap runs once or twice.
    let marks = fs::read_to_string(tasks.project.join("m")).unwrap();
    assert!(
        !marks.is_empty() && marks.lines().all(|line| line == "i"),
        "{marks:?}"
    );
    assert_eq!(
        json!([
            terminal["result"]["result"]["stdout"]["text"],
            terminal["result"]["result"]["exit_code"]
        ]),
        json!(["after\n", 0]),
        "{terminal:#}"
    );
    assert_eq!(
        tasks.reply(&message_id)["result"]["result"]["written_bytes"],
        0
    );
    case.observe(format!(
        "m held i and stdout after {elapsed:?} after U+0003; the reply's written_bytes 0"
    ));
}
