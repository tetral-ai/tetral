//! The reading of tool instructions by `tetral server run`, whose ready
//! advertises `tool_execution`, against the reference control server as
//! separate processes over real HTTP and SSE: one event of up to 4,259,840
//! bytes is read. The seam counters these tests read are those
//! `tetral_tests::machine` names.

use std::path::Path;

use serde_json::{Value, json};
use tetral_tests::machine::{
    CREDENTIAL, FIRST_CONNECTION, INSTANCE, KEY, Machine, bootstrap, one_credential_engine,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;

/// The Session these instructions name, which the machine never prepared.
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
