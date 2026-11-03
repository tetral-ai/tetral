//! The reading and admission of tool instructions by `tetral server run`,
//! whose ready advertises `tool_execution`, against the reference control
//! server as separate processes over real HTTP and SSE: one event of up to
//! 4,259,840 bytes is read, and the byte budgets of each type are applied to
//! it, 65,536 bytes for every type without a larger one. With a prepared
//! Session running `exec_command` tasks: an instruction delivered again or
//! its payload issued again, a `task_id` issued again with other content,
//! tasks staged behind a withheld `prepared` ACK and the controls they held
//! when their start fails or is rejected, a cancel and a close while a task's availability
//! check is held, the replay ledger's
//! entries of tasks, kept until a resume or freed by each ACK, also of an
//! instruction that joined a task, and, with
//! only the ledger's reserve free, new tasks refused while a running task's
//! controls, its binding's close and its report are recorded. The seam
//! counters these tests read are those `tetral_tests::machine` and
//! `tetral_tests::tasks` name, and `connection.final_acked`, counted after
//! the Connection owner applies an instruction's final ACK to its ledger.

use std::fs;
use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, answer_fault, bootstrap,
    one_credential_engine, send, wait_for_received, wait_for_requests,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::tasks::{
    AVAILABILITY_CHECK, BINDING, HEARTBEAT_INTERVAL, LAUNCH, MAXIMUM_STREAM_AGE, SESSION,
    TaskMachine, UNBOUND_ADMISSION, WAIT_CHECKED, millis, policy as project_policy,
};
use tetral_tests::{binaries, socket_directory};

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

/// The entries the ledger keeps in reserve for the controls, closes and
/// reports of what it holds.
const LEDGER_RESERVE: usize = 64;

/// What the `terminal` report of a task that never started says: its
/// sequence, status, `cancelled`, exit facts, `duration_ms` and streams.
fn unstarted(report: &Value) -> Value {
    let result = &report["result"]["result"];
    json!([
        report["report_seq"],
        report["result"]["status"],
        result["cancelled"],
        result["exit_code"],
        result["signal"],
        result["duration_ms"],
        result["stdout"]["text"],
        result["stderr"]["text"],
    ])
}

/// The report of a task cancelled before it started.
fn cancelled_unstarted() -> Value {
    json!([1, "success", true, null, null, 0, "", ""])
}

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

/// Records `closes` closes of Sessions without a record. The reference
/// server issued none of them, so it refuses each `closed` report
/// `unknown_instruction`; a refused report acknowledges nothing, so each
/// close keeps its ledger entry. Each chunk is routed before the next is
/// sent, so that every wait covers a bounded number.
fn fill_ledger(tasks: &TaskMachine, closes: usize) {
    const CHUNK: usize = 500;
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
        if (index + 1) % CHUNK == 0 || index + 1 == closes {
            seams.wait_for_count("route.session", routed + index as u64 + 1);
        }
    }
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
fn a_resumed_stream_keeps_the_tool_limit_for_a_valid_large_command() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("a ready tool connection resumes, then executes a command larger than 64 KiB");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let connection = tasks.machine().status()["connection_id"].clone();
    resume(&tasks);
    tasks.machine().wait_for("online");
    assert_eq!(tasks.machine().status()["connection_id"], connection);
    let streams = tasks.engine.requests("events").len();
    // The padding is a shell comment, so the legal large input has a small,
    // exact result.
    let input = json!({"cmd": format!("printf resumed\n#{}", "x".repeat(70_000))});
    assert!(input.to_string().len() > 65_536);
    let instruction = tasks.execute(
        "task_after_resume",
        input,
        project_policy(&tasks.project, "disabled"),
    );
    let report = tasks.terminal("task_after_resume");
    assert_eq!(report["request_message_id"], instruction);
    assert_eq!(report["result"]["status"], "success");
    assert_eq!(report["result"]["result"]["stdout"]["text"], "resumed");
    assert_eq!(tasks.machine().status()["connection_state"], "online");
    assert_eq!(tasks.engine.requests("events").len(), streams);
    case.observe("the same logical connection resumed and accepted a legal command over 64 KiB, returning resumed without another stream loss");
    case.pass();
}

#[test]
fn an_event_of_up_to_4_259_840_bytes_is_read_and_a_larger_one_closes_the_stream_unread() {
    let _socket_dir = socket_directory::isolated();
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
    case.pass();
}

#[test]
fn each_tool_instruction_is_judged_by_its_type_budgets() {
    let _socket_dir = socket_directory::isolated();
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
    case.pass();
}

#[test]
fn a_tool_execute_delivered_again_or_issued_again_with_its_payload_runs_once() {
    let _socket_dir = socket_directory::isolated();
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
    case.pass();
}

#[test]
fn a_task_id_issued_again_with_another_command_or_policy_is_a_conflict_that_leaves_the_task_alone()
{
    let _socket_dir = socket_directory::isolated();
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
    case.pass();
}

#[test]
fn a_task_staged_behind_a_withheld_prepared_ack_starts_only_after_it_and_never_once_cancelled_or_closed()
 {
    let _socket_dir = socket_directory::isolated();
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
    // The close prevents the start at once, before the ACK.
    let closed = tasks.terminal("task_closed");
    assert_eq!(unstarted(&closed), cancelled_unstarted(), "{closed:#}");
    release_answers(&tasks);
    handled(&tasks, "sess_closed", "bind_closed");
    assert_eq!(seams.count(SPAWN), spawns + 2);
    case.observe("no process before the released ACK, then one; the cancelled task never launched, a later task of its binding did; the closed binding's task never launched and reported cancelled at the close");
    case.pass();
}

#[test]
fn a_read_and_a_write_held_by_a_staged_task_are_answered_when_its_start_fails() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a staged exec_command whose env holds U+0000, with a read and a write sent while it is staged, then the prepared ACK",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    prepare_pending(&tasks, "sess_held", "bind_held");
    tasks.execute_with(
        "sess_held",
        "bind_held",
        "task_held",
        json!({"cmd": "true", "env": {"BROKEN": "a\u{0}b"}}),
        project_policy(&tasks.project, "disabled"),
        None,
    );
    let control = |members: Value| {
        let mut payload = json!({
            "session_id": "sess_held", "binding_id": "bind_held",
            "tool_use_event_id": format!("call_{}", members["write_seq"]), "task_id": "task_held",
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(members.as_object().cloned().unwrap());
        let issued = tasks.engine.control(
            "stdin",
            json!({"instance_id": INSTANCE, "payload": payload}),
        );
        issued["message_id"].as_str().unwrap().to_owned()
    };
    let read = control(json!({"chars": "", "wait_ms": 5_000}));
    let write = control(json!({"chars": "x", "write_seq": 1}));
    handled(&tasks, "sess_held", "bind_held");
    release_answers(&tasks);
    let failed = tasks.terminal("task_held");
    assert_eq!(failed["result"]["error"]["kind"], "exec_spawn_failed");
    let read = tasks.reply(&read);
    assert_eq!(
        json!([
            read["error"],
            read["result"]["status"],
            read["result"]["result"]["exit_code"]
        ]),
        json!([null, "success", null]),
        "{read:#}"
    );
    let write = tasks.reply(&write);
    assert_eq!(write["error"]["code"], "stdin_closed", "{write:#}");
    case.observe("the start failed exec_spawn_failed; the held read was answered as of the ended task and the held write stdin_closed");
    case.pass();
}

#[test]
fn a_task_whose_start_is_rejected_leaves_the_registry_so_its_controls_find_no_task() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "an App Server whose PATH holds no bwrap: a staged exec_command with a read sent while it is staged, then the ACK; a cancel after its rejection; the same instruction issued again",
    );
    let directory = tempfile::tempdir().unwrap();
    let empty = directory.path().join("empty");
    fs::create_dir(&empty).unwrap();
    let tasks = TaskMachine::start_from(
        &mut case,
        directory.path(),
        binaries::tetral(),
        empty.to_str().unwrap(),
    );
    let policy = project_policy(&tasks.project, "disabled");
    let execute = || {
        tasks.execute_with(
            "sess_held",
            "bind_held",
            "task_rejected",
            json!({"cmd": "touch ran"}),
            policy.clone(),
            None,
        )
    };
    let control = |kind: &str, members: Value| {
        let mut payload = json!({
            "session_id": "sess_held", "binding_id": "bind_held",
            "tool_use_event_id": format!("call_{kind}"), "task_id": "task_rejected",
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(members.as_object().cloned().unwrap());
        let issued = tasks
            .engine
            .control(kind, json!({"instance_id": INSTANCE, "payload": payload}));
        issued["message_id"].as_str().unwrap().to_owned()
    };
    let rejection = |report: &Value| json!([report["phase"], report["error"]["code"]]);
    let unavailable = json!(["rejected", "sandbox_unavailable"]);

    prepare_pending(&tasks, "sess_held", "bind_held");
    let first = execute();
    let read = control("stdin", json!({"chars": ""}));
    handled(&tasks, "sess_held", "bind_held");
    release_answers(&tasks);
    assert_eq!(rejection(&answer_to(&tasks, &first)), unavailable);
    assert_eq!(tasks.reply(&read)["error"]["code"], "task_not_found");
    let cancel = control("cancel", json!({}));
    assert_eq!(tasks.reply(&cancel)["error"]["code"], "task_not_found");
    // Registered no more, the same instruction is judged again, not joined.
    let second = execute();
    assert_eq!(rejection(&answer_to(&tasks, &second)), unavailable);
    assert!(!tasks.project.join("ran").exists());
    case.observe("the start was rejected sandbox_unavailable; the held read and a later cancel found no task; the instruction issued again was rejected again; nothing ran");
    case.pass();
}

#[test]
fn the_admission_of_a_session_without_an_owner_never_holds_the_connection() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "a tool.execute of a Session never prepared held at its admission, then a preparation of another Session",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    seams.hold(UNBOUND_ADMISSION);
    tasks.execute_with(
        "sess_unprepared",
        "bind_unprepared",
        "task_unbound",
        json!({"cmd": "true"}),
        project_policy(&tasks.project, "disabled"),
        None,
    );
    seams.wait_until_waiting(UNBOUND_ADMISSION, 1);
    let issued = tasks.prepare("sess_other", "bind_other", &tasks.project, None);
    tasks.wait_prepared(&issued);
    seams.release(UNBOUND_ADMISSION);
    let rejected = tasks.report("task_unbound", "rejected");
    assert_eq!(
        rejected["error"]["code"], "binding_unavailable",
        "{rejected:#}"
    );
    case.observe("another Session was prepared while the admission was held; once released, the instruction was rejected binding_unavailable");
    case.pass();
}

#[test]
fn a_cancel_or_a_close_while_the_availability_check_is_held_answers_at_once_and_nothing_starts() {
    let _socket_dir = socket_directory::isolated();
    let mut case = tetral_tests::record!().case(
        "two tasks held before their availability checks run: a cancel of one, a close of the other's binding, then the checks released",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let policy = project_policy(&tasks.project, "disabled");
    let spawns = seams.count(SPAWN);
    seams.hold(AVAILABILITY_CHECK);

    tasks.execute(
        "task_cancelled",
        json!({"cmd": "touch cancelled"}),
        policy.clone(),
    );
    seams.wait_until_waiting(AVAILABILITY_CHECK, 1);
    let cancel = tasks.cancel("task_cancelled", json!({}));
    let reply = tasks.reply(&cancel);
    let result = &reply["result"]["result"];
    assert_eq!(
        json!([result["cancelled"], result["was_running"]]),
        json!([true, false]),
        "{reply:#}"
    );
    let cancelled = tasks.terminal("task_cancelled");
    assert_eq!(
        unstarted(&cancelled),
        cancelled_unstarted(),
        "{cancelled:#}"
    );

    let issued = tasks.prepare("sess_closed", "bind_closed", &tasks.project, None);
    tasks.wait_prepared(&issued);
    tasks.execute_with(
        "sess_closed",
        "bind_closed",
        "task_closed",
        json!({"cmd": "touch closed"}),
        policy.clone(),
        None,
    );
    seams.wait_until_waiting(AVAILABILITY_CHECK, 2);
    tasks.engine.control(
        "close",
        json!({"instance_id": INSTANCE, "payload": {"session_id": "sess_closed", "binding_id": "bind_closed"}}),
    );
    let closed = tasks.terminal("task_closed");
    assert_eq!(unstarted(&closed), cancelled_unstarted(), "{closed:#}");

    seams.release(AVAILABILITY_CHECK);
    tasks.execute("task_after", json!({"cmd": "true"}), policy);
    tasks.terminal("task_after");
    assert_eq!(seams.count(SPAWN), spawns + 1);
    assert!(!tasks.project.join("cancelled").exists());
    assert!(!tasks.project.join("closed").exists());
    case.observe("the cancel replied cancelled, not running, while the check was held, and both tasks reported cancelled before starting; after the release only a later task launched");
    case.pass();
}

#[test]
fn an_instruction_whose_final_ack_comes_after_a_resume_keeps_its_ledger_entry_until_the_next_resume()
 {
    let _socket_dir = socket_directory::isolated();
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
        // Startup issued only one preparation. Settle its ledger ACK before
        // filling the ledger, so only the held task can supply ACK number 2.
        seams.wait_for_count("connection.final_acked", 1);
        fill_ledger(&tasks, LEDGER_ROOM - 1);
        answer_fault(
            &tasks.engine,
            json!({"endpoint": "tool-status", "action": "withhold"}),
        );
        let held = tasks.execute("task_held", json!({"cmd": "printf x >> f"}), policy.clone());
        tasks.terminal("task_held");
        resume(&tasks);
        release_answers(&tasks);
        // The closes of absent Sessions get only refused reports, which
        // acknowledge nothing. The held task is therefore the only remaining
        // final ACK. Wait for the Connection
        // owner to apply it, not just for the outbox to enqueue it: a resend
        // handled first would change the entry's last-delivery stream and
        // allow that ACK to remove it immediately.
        seams.wait_for_count("connection.final_acked", 2);

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
        assert_eq!(fs::read_to_string(tasks.project.join("f")).unwrap(), "x");

        next_resume(&tasks);
        tasks.execute("task_admitted", json!({"cmd": "true"}), policy);
        let admitted = tasks.terminal("task_admitted");
        assert_eq!(admitted["result"]["status"], "success");
        assert_eq!(seams.count(SPAWN), spawns + 1);
        assert_eq!(fs::read_to_string(tasks.project.join("f")).unwrap(), "x");
        case.observe(format!(
            "after the Connection owner applied the held final ACK, the ledger stayed full: the resent instruction launched nothing and f held x once, and the new task was resource_exhausted; after the resume at {name} a new task ran and f still held x once"
        ));
        case.pass();
    }
}

#[test]
fn a_tool_execute_that_joins_a_task_takes_its_final_report_and_frees_its_ledger_entry_with_it() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    for (name, after_terminal) in [
        ("while the task runs", false),
        ("after the task's terminal report, before its ACK", true),
    ] {
        let mut case = record.case(&format!(
            "4,030 closes and a task joined by the same instruction {name}: after the terminal report's ACK, a running task and a new task"
        ));
        let directory = tempfile::tempdir().unwrap();
        let tasks = TaskMachine::start(&mut case, directory.path());
        let policy = project_policy(&tasks.project, "disabled");
        // The task and the instruction that joins it take the last two
        // entries before the reserve.
        fill_ledger(&tasks, LEDGER_ROOM - 2);
        let input = json!({"cmd": "while [ ! -e go ]; do sleep 0.05; done"});
        let go = || fs::write(tasks.project.join("go"), "").unwrap();
        if after_terminal {
            answer_fault(
                &tasks.engine,
                json!({"endpoint": "tool-status", "action": "withhold"}),
            );
        }
        let first = tasks.execute("task_joined", input.clone(), policy.clone());
        if after_terminal {
            go();
            tasks.terminal("task_joined");
        }
        let joined = tasks.execute("task_joined", input, policy.clone());
        handled(&tasks, SESSION, BINDING);
        if after_terminal {
            release_answers(&tasks);
        } else {
            go();
        }
        let terminal = tasks.terminal("task_joined");
        assert_eq!(terminal["request_message_id"], first.as_str());
        tasks.answers_settled();
        assert!(
            tasks
                .engine
                .received("tool-status")
                .iter()
                .all(|report| report["body"]["request_message_id"] != joined.as_str()),
            "the joined instruction got a report of its own"
        );

        // With both entries freed, a task that keeps running and a new one
        // fit before the reserve.
        tasks.execute(
            "task_kept",
            json!({"cmd": "while [ ! -e stop ]; do sleep 0.05; done"}),
            policy.clone(),
        );
        let admitted = tasks.execute("task_admitted", json!({"cmd": "true"}), policy);
        let report = answer_to(&tasks, &admitted);
        assert_eq!(report["phase"], "terminal", "{report:#}");
        fs::write(tasks.project.join("stop"), "").unwrap();
        case.observe(format!(
            "joined {name}, the task's terminal report answered both instructions; after its ACK a running task and a new task were both admitted"
        ));
        case.pass();
    }
}

#[test]
fn five_thousand_tasks_on_one_stream_each_issued_after_the_previous_terminal_ack_are_all_admitted()
{
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "5,000 exec_command true, each issued once the App Server has applied the ACK of the one before's terminal report",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let policy = project_policy(&tasks.project, "disabled");
    let answered = tasks.answers_settled();
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
    case.pass();
}

#[test]
fn with_the_ledger_full_a_tasks_controls_are_resource_exhausted_and_leave_it_unchanged() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a running task waiting for go, and closes until no entry is free, reserve included: a write and a cancel, then go, after which the task says whether its stdin holds input",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    let checked = seams.count(WAIT_CHECKED);
    tasks.execute(
        "task_waiting",
        json!({
            "cmd": "while [ ! -e go ]; do sleep 0.05; done; if read -t 0; then echo pending; else echo empty; fi",
            "wait_ms": 250, "on_wait_expiry": "detach",
        }),
        project_policy(&tasks.project, "disabled"),
    );
    tasks.wait_for_start(checked);
    seams.advance(millis(250));
    tasks.report("task_waiting", "running");
    // The task's instruction holds the last entry.
    fill_ledger(&tasks, LEDGER_ROOM + LEDGER_RESERVE - 1);

    let write = tasks.stdin("task_waiting", json!({"chars": "x", "write_seq": 1}));
    let cancel = tasks.cancel("task_waiting", json!({}));
    for (name, message_id) in [("write", &write), ("cancel", &cancel)] {
        let reply = tasks.reply(message_id);
        assert_eq!(
            reply["error"]["code"], "resource_exhausted",
            "{name}: {reply:#}"
        );
    }
    fs::write(tasks.project.join("go"), "").unwrap();
    let terminal = tasks.terminal("task_waiting");
    let result = &terminal["result"]["result"];
    assert_eq!(
        json!([
            result["exit_code"],
            result["cancelled"],
            result["stdout"]["text"]
        ]),
        json!([0, null, "empty\n"]),
        "{terminal:#}"
    );
    case.observe("the write and the cancel each got resource_exhausted; after go the task found no input on its stdin and exited 0, not cancelled");
    case.pass();
}

#[test]
fn with_only_the_ledgers_reserve_free_new_tasks_are_refused_and_a_running_tasks_controls_and_report_answered()
 {
    let _socket_dir = socket_directory::isolated();
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
    // Both rejections come at one time of the controlled clock, so they
    // open one episode of the task pressure point.
    let rejecting = || -> Vec<Value> {
        tasks
            .records("pressure.rejecting_started")
            .into_iter()
            .filter(|record| record["pressure.point"] == "tetral_tasks")
            .collect()
    };
    wait_until(
        "a pressure.rejecting_started record of tetral_tasks",
        || !rejecting().is_empty(),
    );
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
    let started = rejecting();
    assert_eq!(started.len(), 1, "{started:?}");
    assert_eq!(started[0]["pressure.cause"], "resource_exhausted");
    case.observe("the staged and the new tool.execute each resource_exhausted, recorded as one pressure.rejecting_started of tetral_tasks with cause resource_exhausted; the read replied running, the cancel cancelled, and the terminal report said cancelled, terminated; nothing launched after the ACK; once the ACKs were in, a new task of the closed binding was binding_unavailable");
    case.pass();
}
