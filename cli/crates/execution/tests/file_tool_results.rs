//! The result that a file tool's supervisor reads from its child's stdout,
//! with real child processes: a valid result envelope of the call's tool,
//! also one padded to exactly 14,680,064 bytes, is the call's result, and a
//! child that writes nothing, writes no result, or writes one byte more gives
//! `helper_failure`.

use std::path::Path;
use std::process::Stdio;

use execution::file_tools::{FileCall, NO_VALID_RESULT, Pipes, RESULT_MAX_BYTES};
use protocol::tool::ToolInput;
use serde_json::{Value, json};

fn edit_call() -> FileCall {
    let input: ToolInput = serde_json::from_value(json!({
        "tool": "edit",
        "input": {"path": "f.txt", "old_string": "a", "new_string": "b"},
    }))
    .unwrap();
    FileCall::of(&input).unwrap()
}

/// The outcome of a child that runs `script` in `sh` with `file` as `$0`,
/// once the supervisor's side has read its stdout to the end.
fn outcome(call: &FileCall, script: &str, file: &Path) -> Value {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(script)
            .arg(file)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut pipes = Pipes::new(
            child.stdin.take(),
            call.request(Path::new("/")),
            child.stdout.take(),
            child.stderr.take(),
        );
        while !pipes.received() {
            pipes.step().await;
        }
        let envelope = call.outcome(&pipes);
        pipes.close();
        child.wait().await.unwrap();
        serde_json::to_value(envelope).unwrap()
    })
}

#[test]
fn a_child_without_a_valid_result_within_its_bound_gives_helper_failure() {
    let call = edit_call();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("stdout");
    let result = json!({
        "schema_version": 1, "tool": "edit", "status": "success", "truncated": false,
        "error": null, "result": {"replacements": 1, "bytes_written": 3},
    });
    let padded = |size: usize| {
        let mut bytes = serde_json::to_vec(&result).unwrap();
        bytes.resize(size, b' ');
        std::fs::write(&file, bytes).unwrap();
    };
    let failure = json!({
        "schema_version": 1, "tool": "edit", "status": "error", "truncated": false,
        "error": {"kind": "helper_failure", "message": NO_VALID_RESULT}, "result": {},
    });

    padded(RESULT_MAX_BYTES);
    assert_eq!(outcome(&call, r#"cat "$0""#, &file), result);
    padded(RESULT_MAX_BYTES + 1);
    assert_eq!(outcome(&call, r#"cat "$0""#, &file), failure);
    assert_eq!(outcome(&call, ":", &file), failure);
    assert_eq!(outcome(&call, "echo no result", &file), failure);
}
