//! The reading and admission of tool instructions by `tetral server run`,
//! whose ready advertises `tool_execution`, against the reference control
//! server as separate processes over real HTTP and SSE: one event of up to
//! 4,259,840 bytes is read, and the byte budgets of each type are applied to
//! it, 65,536 bytes for every type without a larger one. With a prepared
//! Session running `exec_command` tasks: an instruction delivered again or
//! its payload issued again, a `task_id` issued again with other content,
//! tasks staged behind a withheld `prepared` ACK, the replay ledger's
//! entries of tasks, kept until a resume or freed by each ACK, and, with
//! only the ledger's reserve free, new tasks refused while a running task's
//! controls, its binding's close and its report are recorded. The seam
//! counters these tests read are those `tetral_tests::machine` and
//! `tetral_tests::tasks` name.

use std::fs;
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap,
    one_credential_engine, send, wait_for_received, wait_for_requests,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{
    BINDING, HEARTBEAT_INTERVAL, LAUNCH, SESSION, TaskMachine, WAIT_CHECKED, millis,
    policy as project_policy,
};

/// The Session these instructions name, which the machine never prepared:
/// an instruction that passes its budgets finds no binding and no task.
fn identities(task_id: &str) -> Value {
    json!({
        "session_id": "sess_unprepared",
        "binding_id": "bind_unprepared",
        "tool_use_event_id": format!("call_{task_id}"),
        "task_id": task_id,
    })
}

fn with(mut base: Value, members: Value) -> Value {
    if let (Some(base), Value::Object(members)) = (base.as_object_mut(), members) {
        base.extend(members);
    }
    base
}

fn policy() -> Value {
    json!({"schema_version": 1, "filesystem": {"rules": []}, "network": {"mode": "disabled"}})
}

/// An `edit` whose `input` is exactly `size` bytes of compact JSON.
fn edit_with_input_of(task_id: &str, size: usize) -> Value {
    let input = |new_string: String| json!({"path": "/project/f", "old_string": "a", "new_string": new_string});
    let empty = input(String::new()).to_string().len();
    let input = input("x".repeat(size - empty));
    assert_eq!(input.to_string().len(), size);
    with(
        identities(task_id),
        json!({"tool": "edit", "input": input, "sandbox_policy": policy()}),
    )
}

/// A `tool.execute` event of exactly `size` bytes, its `cmd` filling it.
fn execute_event_of(message_id: &str, size: usize) -> String {
    let event = |cmd: String| {
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": message_id,
            "type": "tool.execute",
            "payload": with(identities(message_id), json!({
                "tool": "exec_command", "input": {"cmd": cmd}, "sandbox_policy": policy(),
            })),
        })
        .to_string()
    };
    let empty = event(String::new()).len();
    let event = event("x".repeat(size - empty));
    assert_eq!(event.len(), size);
    event
}

/// Issues the instruction with `control` and gives its report's body.
fn report_of(engine: &ReferenceServer, control: &str, input: Value, reports: usize) -> Value {
    let issued = engine.control(control, with(json!({"instance_id": INSTANCE}), input));
    let received = wait_for_received(engine, "tool-status", reports);
    let report = received
        .iter()
        .find(|report| report["body"]["request_message_id"] == issued["message_id"])
        .expect("the instruction's report");
    report["body"].clone()
}

/// Issues `control`, an instruction of type `kind` with `payload`, as an
/// event of exactly `size` bytes, an unknown envelope member filling it.
fn issue_of_size(
    engine: &ReferenceServer,
    control: &str,
    kind: &str,
    message_id: &str,
    payload: Value,
    size: usize,
) {
    let event = |extension: String| {
        json!({
            "protocol_version": 1,
            "connection_id": FIRST_CONNECTION,
            "message_id": message_id,
            "type": kind,
            "payload": payload,
            "extension": extension,
        })
        .to_string()
        .len()
    };
    let extension = "x".repeat(size - event(String::new()));
    assert_eq!(event(extension.clone()), size);
    engine.control(
        control,
        json!({
            "instance_id": INSTANCE,
            "message_id": message_id,
            "payload": payload,
            "extra": {"extension": extension},
        }),
    );
}

fn online_with_tools(case: &mut Case, directory: &Path) -> (ReferenceServer, Machine) {
    let engine = one_credential_engine();
    let machine = Machine::start_with_tools(
        case,
        directory,
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
    );
    machine.wait_for("online");
    (engine, machine)
}

/// The seam counter of each process the isolation launches.
const SPAWN: &str = "process.spawn";

/// The replay ledger's 4,096 entries less the 64 it keeps in reserve: a
/// ledger that holds this many refuses a new operation.
const LEDGER_ROOM: usize = 4_032;

/// The age at which the reference control server ends a stream with a
/// stream-only `disconnect`.
const MAXIMUM_STREAM_AGE: Duration = Duration::from_secs(30 * 60);

/// The accepted report that answers `message_id`, once it has come.
fn answer_to(tasks: &TaskMachine, message_id: &str) -> Value {
    let mut found = Value::Null;
    wait_until(
        &format!("the report answering {message_id}"),
        || match tasks
            .engine
            .received("tool-status")
            .into_iter()
            .find(|report| report["body"]["request_message_id"] == message_id)
        {
            Some(report) => {
                found = report["body"].clone();
                true
            }
            None => false,
        },
    );
    found
}

/// Waits for the reply to a read of a task that `binding` of `session`
/// never had, its control error `task_not_found`: the Session's owner has
/// then handled every instruction routed to it before.
fn handled(tasks: &TaskMachine, session: &str, binding: &str) {
    let issued = tasks.engine.control(
        "stdin",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": session, "binding_id": binding,
            "tool_use_event_id": "call_absent", "task_id": "task_absent", "chars": "",
        }}),
    );
    let reply = answer_to(tasks, issued["message_id"].as_str().unwrap());
    assert_eq!(reply["error"]["code"], "task_not_found");
}

/// Prepares `binding` of `session` at the machine's project with the ACK of
/// its `prepared` report withheld, so that it stays `prepared_pending_ack`
/// until the withheld answers are released.
fn prepare_pending(tasks: &TaskMachine, session: &str, binding: &str) {
    answer_fault(
        &tasks.engine,
        json!({"endpoint": "session-status", "action": "withhold"}),
    );
    let issued = tasks.prepare(session, binding, &tasks.project, None);
    tasks.wait_prepared(&issued);
}

fn release_answers(tasks: &TaskMachine) {
    answer_fault(&tasks.engine, json!({"action": "release"}));
}

/// Records `closes` closes of Sessions without a record, which nothing
/// reports, so that each keeps its ledger entry.
fn fill_ledger(tasks: &TaskMachine, closes: usize) {
    let seams = &tasks.machine().seams;
    let connection_id = tasks.machine().status()["connection_id"].clone();
    let routed = seams.count("route.session");
    for index in 0..closes {
        send(
            &tasks.engine,
            json!({
                "protocol_version": 1,
                "connection_id": connection_id,
                "message_id": format!("msg_fill_{index}"),
                "type": "session.close",
                "payload": { "session_id": format!("sess_fill_{index}"), "binding_id": "bind_1" },
            }),
        );
    }
    seams.wait_for_count("route.session", routed + closes as u64);
}

/// Ends the current stream with a stream-only `disconnect` and waits for
/// the App Server's resume of the connection.
fn resume(tasks: &TaskMachine) {
    let streams = tasks.engine.requests("events").len();
    send(
        &tasks.engine,
        json!({
            "protocol_version": 1,
            "connection_id": tasks.machine().status()["connection_id"],
            "message_id": format!("msg_disconnect_{streams}"),
            "type": "disconnect",
            "payload": { "scope": "stream" },
        }),
    );
    let events = wait_for_requests(&tasks.engine, "events", streams + 1);
    assert_eq!(events[streams]["query"]["resume"], "true");
}

/// Moves both clocks to the current stream's maximum age, at which the
/// reference control server ends it with a stream-only `disconnect`, and
/// waits for the App Server's resume of the connection.
fn resume_at_maximum_age(tasks: &TaskMachine) {
    let streams = tasks.engine.requests("events").len();
    tasks.advance(MAXIMUM_STREAM_AGE - HEARTBEAT_INTERVAL);
    tasks.engine.advance(HEARTBEAT_INTERVAL);
    let events = wait_for_requests(&tasks.engine, "events", streams + 1);
    assert_eq!(events[streams]["query"]["resume"], "true");
}

#[test]
fn an_event_of_up_to_4_259_840_bytes_is_read_and_a_larger_one_closes_the_stream_unread() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("the event limit of a connection that advertises tool_execution");
    let directory = tempfile::tempdir().unwrap();
    let (engine, machine) = online_with_tools(&mut case, directory.path());

    let routed = machine.routed();
    engine.control(
        "send",
        json!({"instance_id": INSTANCE, "data": execute_event_of("msg_at_limit", 4_259_840)}),
    );
    machine.seams.wait_for_count("route.session", routed + 1);
    assert_eq!(machine.state(), "online");
    case.observe("an event of 4,259,840 bytes was read and routed; the stream stayed open");

    engine.control(
        "send",
        json!({"instance_id": INSTANCE, "data": execute_event_of("msg_over_limit", 4_259_841)}),
    );
    machine.wait_for("resuming");
    machine.settle();
    assert_eq!(machine.routed(), routed + 1);
    assert!(engine.requests("protocol-error").is_empty());
    case.observe("an event of 4,259,841 bytes closed the stream unread: nothing routed and no protocol error, and the connection is resuming");
}

#[test]
fn each_tool_instruction_is_judged_by_its_type_budgets() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case =
        record.case("tool.execute and control budgets, each instruction with its own report");
    let directory = tempfile::tempdir().unwrap();
    let (engine, _machine) = online_with_tools(&mut case, directory.path());

    let execute = report_of(
        &engine,
        "execute",
        json!({"payload": edit_with_input_of("input_over", 4_194_305)}),
        1,
    );
    assert_eq!(
        (&execute["phase"], &execute["error"]["code"]),
        (&json!("rejected"), &json!("invalid_input"))
    );
    case.observe("an edit whose input is 4,194,305 bytes: rejected invalid_input");

    let stdin = |task_id: &str, chars: String| {
        with(identities(task_id), json!({"chars": chars, "write_seq": 1}))
    };
    let rows = [
        (
            "chars of 32,768 U+0000, escaped to six bytes each",
            json!({"payload": stdin("worst_case", "\u{0}".repeat(32_768))}),
            "task_not_found",
        ),
        (
            "chars of 32,769 bytes",
            json!({"payload": stdin("one_over", "x".repeat(32_769))}),
            "invalid_input",
        ),
        (
            "chars of 11,264 three-byte characters, 33,792 bytes",
            json!({"payload": stdin("characters", "中".repeat(11_264))}),
            "invalid_input",
        ),
        (
            "a read whose envelope carries 65,536 bytes of an unknown member",
            json!({
                "payload": with(identities("metadata"), json!({"chars": "", "wait_ms": 5000})),
                "extra": {"extension": "x".repeat(65_536)},
            }),
            "invalid_input",
        ),
    ];
    for (index, (name, input, code)) in rows.into_iter().enumerate() {
        let report = report_of(&engine, "stdin", input, index + 2);
        assert_eq!(
            (&report["kind"], &report["error"]["code"]),
            (&json!("control"), &json!(code)),
            "{name}"
        );
        case.observe(format!("tool.stdin with {name}: the control error {code}"));
    }

    issue_of_size(
        &engine,
        "cancel",
        "tool.cancel",
        "msg_cancel_over",
        identities("cancel_over"),
        65_537,
    );
    let received = wait_for_received(&engine, "tool-status", 6);
    let cancel = received
        .iter()
        .find(|report| report["body"]["request_message_id"] == "msg_cancel_over")
        .expect("the cancel's report");
    assert_eq!(
        (&cancel["body"]["kind"], &cancel["body"]["error"]["code"]),
        (&json!("control"), &json!("invalid_input"))
    );
    case.observe("a tool.cancel of 65,537 bytes: the control error invalid_input");

    issue_of_size(
        &engine,
        "close",
        "session.close",
        "msg_close_over",
        json!({"session_id": "sess_unprepared", "binding_id": "bind_unprepared"}),
        65_537,
    );
    let errors = wait_for_received(&engine, "protocol-error", 1);
    assert_eq!(
        (
            &errors[0]["body"]["request_message_id"],
            &errors[0]["body"]["error"]["code"]
        ),
        (&json!("msg_close_over"), &json!("invalid_message"))
    );
    case.observe("a session.close of 65,537 bytes: the protocol error invalid_message");
}

#[test]
fn a_tool_execute_delivered_again_or_issued_again_with_its_payload_runs_once() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "printf x >> f held at its launch: its instruction delivered again, then its payload issued again under another message_id",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let spawns = seams.count(SPAWN);
    seams.hold(LAUNCH);
    let input = json!({"cmd": "printf x >> f"});
    let original = tasks.execute(
        "task_once",
        input.clone(),
        project_policy(&tasks.project, "direct"),
    );
    seams.wait_until_waiting(LAUNCH, 1);
    tasks
        .engine
        .control("send", json!({"instance_id": INSTANCE, "resend": original}));
    let again = tasks.execute("task_once", input, project_policy(&tasks.project, "direct"));
    handled(&tasks, SESSION, BINDING);
    assert_eq!(seams.count(SPAWN), spawns + 1);
    seams.release(LAUNCH);
    tasks.terminal("task_once");
    handled(&tasks, SESSION, BINDING);
    let answered: Vec<Value> = tasks
        .reports("task_once")
        .iter()
        .map(|report| json!([report["request_message_id"], report["phase"]]))
        .collect();
    assert_eq!(answered, [json!([original, "terminal"])]);
    assert_eq!(seams.count(SPAWN), spawns + 1);
    assert_eq!(fs::read_to_string(tasks.project.join("f")).unwrap(), "x");
    case.observe(format!(
        "one process launched; one terminal report, answering {original}; none answering {again}; f holds x once"
    ));
}

#[test]
fn a_task_id_issued_again_with_another_command_or_policy_is_a_conflict_that_leaves_the_task_alone()
{
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a detached task held at its launch, then running: its task_id with another cmd and with network disabled, before and after its start",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    seams.hold(LAUNCH);
    let input = json!({"cmd": "while [ ! -e go ]; do sleep 0.05; done", "wait_ms": 250, "on_wait_expiry": "detach"});
    let original = tasks.execute(
        "task_kept",
        input.clone(),
        project_policy(&tasks.project, "direct"),
    );
    seams.wait_until_waiting(LAUNCH, 1);
    let conflicts = || -> Vec<Value> {
        let other_cmd = tasks.execute(
            "task_kept",
            with(input.clone(), json!({"cmd": "touch go"})),
            project_policy(&tasks.project, "direct"),
        );
        let other_policy = tasks.execute(
            "task_kept",
            input.clone(),
            project_policy(&tasks.project, "disabled"),
        );
        [other_cmd, other_policy]
            .iter()
            .map(|message_id| {
                let report = answer_to(&tasks, message_id);
                json!([report["phase"], report["error"]["code"]])
            })
            .collect()
    };
    let conflict = json!(["rejected", "task_conflict"]);
    assert_eq!(conflicts(), [conflict.clone(), conflict.clone()]);
    let checked = seams.count(WAIT_CHECKED);
    seams.release(LAUNCH);
    tasks.wait_for_start(checked);
    seams.advance(millis(250));
    tasks.report("task_kept", "running");
    assert_eq!(conflicts(), [conflict.clone(), conflict]);
    assert!(!tasks.project.join("go").exists());
    fs::write(tasks.project.join("go"), "").unwrap();
    tasks.terminal("task_kept");
    let kept: Vec<Value> = tasks
        .reports("task_kept")
        .iter()
        .filter(|report| report["request_message_id"] == original)
        .map(|report| json!([report["phase"], report["report_seq"]]))
        .collect();
    assert_eq!(kept, [json!(["running", 1]), json!(["terminal", 2])]);
    case.observe("each of the four task_conflict; the task ran its own command and reported running 1 and terminal 2");
}

#[test]
fn a_task_staged_behind_a_withheld_prepared_ack_starts_only_after_it_and_never_once_cancelled_or_closed()
 {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "three bindings whose prepared ACK is withheld, each with a staged exec_command: the ACK released; a cancel, then the ACK; a close, then the ACK",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let policy = project_policy(&tasks.project, "disabled");
    let command = json!({"cmd": "true"});
    let spawns = seams.count(SPAWN);

    prepare_pending(&tasks, "sess_started", "bind_started");
    tasks.execute_with(
        "sess_started",
        "bind_started",
        "task_started",
        command.clone(),
        policy.clone(),
        None,
    );
    handled(&tasks, "sess_started", "bind_started");
    assert_eq!(seams.count(SPAWN), spawns);
    release_answers(&tasks);
    tasks.terminal("task_started");
    assert_eq!(seams.count(SPAWN), spawns + 1);

    // A later task of the binding starts only once the ACK has drained its
    // staging queue.
    prepare_pending(&tasks, "sess_cancelled", "bind_cancelled");
    tasks.execute_with(
        "sess_cancelled",
        "bind_cancelled",
        "task_cancelled",
        command.clone(),
        policy.clone(),
        None,
    );
    tasks.engine.control(
        "cancel",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": "sess_cancelled", "binding_id": "bind_cancelled",
            "tool_use_event_id": "call_cancel", "task_id": "task_cancelled",
        }}),
    );
    handled(&tasks, "sess_cancelled", "bind_cancelled");
    assert_eq!(seams.count(SPAWN), spawns + 1);
    release_answers(&tasks);
    tasks.execute_with(
        "sess_cancelled",
        "bind_cancelled",
        "task_later",
        command.clone(),
        policy.clone(),
        None,
    );
    tasks.terminal("task_later");
    assert_eq!(seams.count(SPAWN), spawns + 2);

    prepare_pending(&tasks, "sess_closed", "bind_closed");
    tasks.execute_with(
        "sess_closed",
        "bind_closed",
        "task_closed",
        command,
        policy,
        None,
    );
    tasks.engine.control(
        "close",
        json!({"instance_id": INSTANCE, "payload": {"session_id": "sess_closed", "binding_id": "bind_closed"}}),
    );
    handled(&tasks, "sess_closed", "bind_closed");
    release_answers(&tasks);
    handled(&tasks, "sess_closed", "bind_closed");
    assert_eq!(seams.count(SPAWN), spawns + 2);
    case.observe("no process before the released ACK, then one; the cancelled task never launched, a later task of its binding did; the closed binding's task never launched");
}

#[test]
fn an_instruction_whose_final_ack_comes_after_a_resume_keeps_its_ledger_entry_until_the_next_resume()
 {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    for (name, next_resume) in [
        ("a stream-only disconnect", resume as fn(&TaskMachine)),
        ("the stream's maximum age", resume_at_maximum_age),
    ] {
        let mut case = record.case(&format!(
            "4,031 closes and a task whose terminal report's ACK is held until a resume: the task's instruction resent and a new task, then a new task after the next resume, at {name}"
        ));
        let directory = tempfile::tempdir().unwrap();
        let tasks = TaskMachine::start(&mut case, directory.path());
        let seams = &tasks.machine().seams;
        let policy = project_policy(&tasks.project, "disabled");
        fill_ledger(&tasks, LEDGER_ROOM - 1);
        answer_fault(
            &tasks.engine,
            json!({"endpoint": "tool-status", "action": "withhold"}),
        );
        let held = tasks.execute("task_held", json!({"cmd": "printf x >> f"}), policy.clone());
        tasks.terminal("task_held");
        resume(&tasks);
        let answered = seams.count("outbox.answered");
        release_answers(&tasks);
        seams.wait_for_count("outbox.answered", answered + 1);

        let spawns = seams.count(SPAWN);
        tasks
            .engine
            .control("send", json!({"instance_id": INSTANCE, "resend": held}));
        let refused = tasks.execute("task_refused", json!({"cmd": "true"}), policy.clone());
        let refusal = answer_to(&tasks, &refused);
        assert_eq!(
            json!([refusal["phase"], refusal["error"]["code"]]),
            json!(["rejected", "resource_exhausted"])
        );
        assert_eq!(seams.count(SPAWN), spawns);

        next_resume(&tasks);
        tasks.execute("task_admitted", json!({"cmd": "true"}), policy);
        let admitted = tasks.terminal("task_admitted");
        assert_eq!(admitted["result"]["status"], "success");
        assert_eq!(fs::read_to_string(tasks.project.join("f")).unwrap(), "x");
        case.observe(format!(
            "the resent instruction launched nothing and f holds x once; the new task before the next resume was resource_exhausted; after the resume at {name} a new task ran"
        ));
    }
}

#[test]
fn five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted()
{
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "5,000 exec_command true, each issued once the App Server has applied the ACK of the one before's terminal report",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let policy = project_policy(&tasks.project, "disabled");
    let answered = seams.count("outbox.answered");
    for index in 0..5_000u64 {
        tasks.execute(
            &format!("task_{index}"),
            json!({"cmd": "true"}),
            policy.clone(),
        );
        seams.wait_for_count("outbox.answered", answered + index + 1);
    }
    let outcomes: Vec<Value> = tasks
        .engine
        .received("tool-status")
        .into_iter()
        .map(|report| report["body"].clone())
        .filter(|body| body["kind"] == "execution")
        .map(|body| json!([body["phase"], body["result"]["status"]]))
        .collect();
    assert_eq!(outcomes.len(), 5_000);
    assert!(
        outcomes
            .iter()
            .all(|outcome| *outcome == json!(["terminal", "success"]))
    );
    assert_eq!(tasks.engine.requests("events").len(), 1);
    case.observe("5,000 terminal reports with status success on the connection's one stream");
}

#[test]
fn with_only_the_ledgers_reserve_free_new_tasks_are_refused_and_a_running_tasks_controls_and_report_answered()
 {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a running task, a binding whose prepared ACK is withheld, and closes until 64 entries are free: a staged and a new tool.execute, a read, a cancel and a close of the task's binding, then the ACK",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let policy = project_policy(&tasks.project, "disabled");
    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_running",
        json!({"cmd": "while [ ! -e go ]; do sleep 0.05; done", "wait_ms": 250, "on_wait_expiry": "detach"}),
        policy.clone(),
    );
    tasks.wait_for_start(checked);
    seams.advance(millis(250));
    tasks.report("task_running", "running");
    prepare_pending(&tasks, "sess_pending", "bind_pending");
    // The running task's instruction and the pending preparation hold two
    // entries.
    fill_ledger(&tasks, LEDGER_ROOM - 2);
    let spawns = seams.count(SPAWN);

    let staged = tasks.execute_with(
        "sess_pending",
        "bind_pending",
        "task_staged",
        json!({"cmd": "true"}),
        policy.clone(),
        None,
    );
    let refused = tasks.execute("task_refused", json!({"cmd": "true"}), policy.clone());
    let exhausted = json!(["rejected", "resource_exhausted"]);
    for message_id in [&staged, &refused] {
        let report = answer_to(&tasks, message_id);
        assert_eq!(json!([report["phase"], report["error"]["code"]]), exhausted);
    }
    let read = tasks.stdin("task_running", json!({"chars": "", "wait_ms": 0}));
    assert_eq!(tasks.reply(&read)["result"]["status"], "running");
    let cancel = tasks.cancel("task_running", json!({}));
    assert_eq!(tasks.reply(&cancel)["result"]["result"]["cancelled"], true);
    tasks.engine.control(
        "close",
        json!({"instance_id": INSTANCE, "payload": {"session_id": SESSION, "binding_id": BINDING}}),
    );
    let terminal = tasks.terminal("task_running");
    assert_eq!(
        json!([
            terminal["result"]["result"]["cancelled"],
            terminal["result"]["result"]["signal"]
        ]),
        json!([true, "terminated"])
    );
    release_answers(&tasks);
    handled(&tasks, "sess_pending", "bind_pending");
    // Once every report's ACK has freed its instruction's entry, the close
    // shows itself recorded: a new task of its binding finds the binding
    // closing.
    let reports = ["tool-status", "session-status"]
        .iter()
        .map(|endpoint| tasks.engine.requests(endpoint).len() as u64)
        .sum();
    seams.wait_for_count("outbox.answered", reports);
    let after_close = tasks.execute("task_after_close", json!({"cmd": "true"}), policy);
    let report = answer_to(&tasks, &after_close);
    assert_eq!(
        json!([report["phase"], report["error"]["code"]]),
        json!(["rejected", "binding_unavailable"])
    );
    assert_eq!(seams.count(SPAWN), spawns);
    case.observe("the staged and the new tool.execute each resource_exhausted; the read replied running, the cancel cancelled, and the terminal report said cancelled, terminated; nothing launched after the ACK; once the ACKs were in, a new task of the closed binding was binding_unavailable");
}
