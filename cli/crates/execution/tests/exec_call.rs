//! An `exec_command` call's defaults and clamps, and the exit facts of a
//! launched process's wait status.

use std::path::Path;
use std::time::Duration;

use execution::exec::{ExecCall, ExitFacts, OnExpiry};
use protocol::tool::ExecInput;
use serde_json::{Value, json};

fn call(members: Value) -> ExecCall {
    let mut input = json!({"cmd": "true"});
    input
        .as_object_mut()
        .unwrap()
        .extend(members.as_object().unwrap().clone());
    let input: ExecInput = serde_json::from_value(input).unwrap();
    ExecCall::new(&input, Path::new("/project"))
}

#[test]
fn an_exec_input_takes_the_contracts_defaults_and_clamps() {
    let millis = Duration::from_millis;
    assert_eq!(call(json!({})).wait, millis(10_000), "no wait_ms");
    for (members, lifetime) in [
        (json!({}), 1_800_000),
        (json!({"task_lifetime_ms": 100}), 250),
        (json!({"task_lifetime_ms": 3_600_001}), 3_600_000),
    ] {
        assert_eq!(
            call(members.clone()).lifetime,
            millis(lifetime),
            "{members}"
        );
    }
    for members in [
        json!({}),
        json!({"on_wait_expiry": ""}),
        json!({"on_wait_expiry": null}),
    ] {
        assert_eq!(call(members.clone()).on_expiry, OnExpiry::Kill, "{members}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn an_exit_status_from_129_to_192_is_a_death_by_signal_and_any_other_an_exit_code() {
    use std::os::unix::process::ExitStatusExt;
    let facts = |status: i32| ExitFacts::observed(std::process::ExitStatus::from_raw(status << 8));
    for status in [0, 1, 128, 193] {
        assert_eq!(
            facts(status),
            ExitFacts {
                exit_code: Some(i64::from(status)),
                signal: None
            },
            "status {status}"
        );
    }
    for (status, signal) in [
        (129, "hangup"),
        (130, "interrupt"),
        (137, "killed"),
        (143, "terminated"),
        (192, "signal 64"),
    ] {
        assert_eq!(
            facts(status),
            ExitFacts {
                exit_code: None,
                signal: Some(signal.to_owned())
            },
            "status {status}"
        );
    }
}
