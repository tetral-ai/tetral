//! The reading and admission of tool instructions by `tetral server run`,
//! whose ready advertises `tool_execution`, against the reference control
//! server as separate processes over real HTTP and SSE: one event of up to
//! 4,259,840 bytes is read, and the byte budgets of each type are applied to
//! it, 65,536 bytes for every type without a larger one. The seam counters
//! these tests read are those `tetral_tests::machine` names.

use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, bootstrap, one_credential_engine,
    wait_for_received,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;

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
