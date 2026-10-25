//! `edit` between `tetral server run`, whose ready advertises
//! `tool_execution`, and the reference control server as separate
//! processes, each on its own controlled clock, with the tool's restricted
//! child and real files: each step of an edit that decides its result, the
//! curly-quote fallback that keeps every other byte, the write that keeps
//! the file's mode, a file the policy denies, a missing working directory,
//! the input budget and a cancel while the child runs. Every report is read
//! back with the protocol's types. The test seams these tests use are those
//! `tetral_tests::tasks` names and the barrier `file_tool.started`, at which
//! a file tool's child waits once it has started.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use protocol::tool::ToolStatus;
use serde_json::{Value, json};
use tetral_tests::machine::INSTANCE;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, SESSION, TaskMachine, policy};

/// The barrier at which a file tool's child waits after its start.
const FILE_TOOL_STARTED: &str = "file_tool.started";

/// Issues `tool` with `input` as `task_id` of the prepared binding under
/// `policy` and gives its `message_id`.
fn call(tasks: &TaskMachine, task_id: &str, tool: &str, input: Value, policy: Value) -> String {
    let issued = tasks.engine.control(
        "execute",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": SESSION, "binding_id": BINDING,
            "tool_use_event_id": format!("call_{task_id}"), "task_id": task_id,
            "tool": tool, "input": input, "sandbox_policy": policy,
        }}),
    );
    issued["message_id"].as_str().unwrap().to_owned()
}

/// The error of the `rejected` report of `task_id`, which decodes with the
/// protocol's types.
fn rejection(tasks: &TaskMachine, task_id: &str) -> Value {
    let report = tasks.report(task_id, "rejected");
    decoded(&report);
    report["error"].clone()
}

/// Checks that `report` decodes with the protocol's types.
fn decoded(report: &Value) {
    let bytes = serde_json::to_vec(report).unwrap();
    if let Err(error) = protocol::decode::<ToolStatus>(&bytes) {
        panic!("{error}: {report:#}");
    }
}

/// What the only report of `task_id`, a `terminal` one with `report_seq`
/// 1 that decodes with the protocol's types, says: its status, error and
/// inner result.
fn outcome(tasks: &TaskMachine, task_id: &str) -> Value {
    let report = tasks.terminal(task_id);
    decoded(&report);
    assert_eq!(tasks.reports(task_id).len(), 1, "{task_id}");
    assert_eq!(
        (&report["report_seq"], &report["result"]["tool"]),
        (&json!(1), &json!("edit")),
        "{report:#}"
    );
    let envelope = &report["result"];
    json!({"status": envelope["status"], "error": envelope["error"], "result": envelope["result"]})
}

fn success(replacements: u64, bytes_written: u64) -> Value {
    json!({
        "status": "success", "error": null,
        "result": {"replacements": replacements, "bytes_written": bytes_written},
    })
}

fn failure(kind: &str, message: &str, path: Option<&Path>) -> Value {
    let mut error = json!({"kind": kind, "message": message});
    if let Some(path) = path {
        error["path"] = json!(path);
    }
    json!({"status": "error", "error": error, "result": {}})
}

/// The names of the entries of `directory`.
fn entries(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn each_step_of_an_edit_decides_its_result_and_only_a_success_writes() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "edits of real files in the binding's directory: a unique match, identical strings, two \
         and three spans, replace_all, no whitespace fallback, a file over the cap and a \
         replacement over it, special mode bits, a file under a read rule, then the directory \
         removed",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let project = tasks.project.clone();
    let path = |name: &str| project.join(name);
    let edit = |task: &str, input: Value| {
        call(&tasks, task, "edit", input, policy(&project, "disabled"));
        outcome(&tasks, task)
    };

    fs::write(path("unique.txt"), "alpha beta gamma").unwrap();
    assert_eq!(
        edit(
            "task_unique",
            json!({"path": "unique.txt", "old_string": "beta", "new_string": "BETA"}),
        ),
        success(1, 16)
    );
    assert_eq!(
        fs::read_to_string(path("unique.txt")).unwrap(),
        "alpha BETA gamma"
    );

    fs::write(path("same.txt"), "old").unwrap();
    assert_eq!(
        edit(
            "task_same",
            json!({"path": "same.txt", "old_string": "old", "new_string": "old"}),
        ),
        failure(
            "invalid_input",
            "old_string and new_string must differ",
            None
        )
    );

    fs::write(path("two.txt"), "a b a").unwrap();
    let mut ambiguous = failure(
        "match_ambiguous",
        "old_string matched more than once",
        Some(&path("two.txt")),
    );
    ambiguous["error"]["detail"] = json!({"matches": 2});
    assert_eq!(
        edit(
            "task_two",
            json!({"path": "two.txt", "old_string": "a", "new_string": "c"}),
        ),
        ambiguous
    );
    assert_eq!(fs::read_to_string(path("two.txt")).unwrap(), "a b a");

    fs::write(path("three.txt"), "x x x").unwrap();
    let mut ambiguous = failure(
        "match_ambiguous",
        "old_string matched more than once",
        Some(&path("three.txt")),
    );
    ambiguous["error"]["detail"] = json!({"matches": 3});
    assert_eq!(
        edit(
            "task_three",
            json!({"path": "three.txt", "old_string": "x", "new_string": "y"}),
        ),
        ambiguous
    );
    assert_eq!(
        edit(
            "task_all",
            json!({"path": "three.txt", "old_string": "x", "new_string": "y", "replace_all": true}),
        ),
        success(3, 5)
    );
    assert_eq!(fs::read_to_string(path("three.txt")).unwrap(), "y y y");

    fs::write(path("spaces.txt"), "hello  world").unwrap();
    assert_eq!(
        edit(
            "task_spaces",
            json!({"path": "spaces.txt", "old_string": "hello world", "new_string": "hi world"}),
        ),
        failure(
            "match_not_found",
            "old_string was not found",
            Some(&path("spaces.txt"))
        )
    );

    let large = fs::File::create(path("large.txt")).unwrap();
    large.set_len(33_554_433).unwrap();
    assert_eq!(
        edit(
            "task_large",
            json!({"path": "large.txt", "old_string": "x", "new_string": "y"}),
        ),
        failure(
            "too_large",
            "file exceeds 32 MiB edit cap",
            Some(&path("large.txt"))
        )
    );

    let original = [vec![b'b'; 30 * 1_048_576], b"a".to_vec()].concat();
    fs::write(path("grows.txt"), &original).unwrap();
    assert_eq!(
        edit(
            "task_grows",
            json!({
                "path": "grows.txt", "old_string": "a", "new_string": "c".repeat(3 * 1_048_576),
                "replace_all": true,
            }),
        ),
        failure(
            "too_large",
            "replacement exceeds 32 MiB edit cap",
            Some(&path("grows.txt"))
        )
    );
    assert!(fs::read(path("grows.txt")).unwrap() == original);

    fs::write(path("special.txt"), "old").unwrap();
    fs::set_permissions(path("special.txt"), fs::Permissions::from_mode(0o7640)).unwrap();
    assert_eq!(
        edit(
            "task_special",
            json!({"path": "special.txt", "old_string": "old", "new_string": "new"}),
        ),
        success(1, 3)
    );
    let mode = fs::metadata(path("special.txt"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o7777, 0o7640, "{mode:o}");
    assert_eq!(fs::read_to_string(path("special.txt")).unwrap(), "new");

    fs::create_dir(path("data")).unwrap();
    fs::write(path("data/report.csv"), "old").unwrap();
    let mut read_only = policy(&project, "disabled");
    read_only["filesystem"]["rules"]
        .as_array_mut()
        .unwrap()
        .push(json!({"path": path("data/report.csv"), "access": "read"}));
    call(
        &tasks,
        "task_read_only",
        "edit",
        json!({"path": "data/report.csv", "old_string": "old", "new_string": "new"}),
        read_only,
    );
    assert_eq!(
        outcome(&tasks, "task_read_only"),
        failure(
            "write_failed",
            "write failed",
            Some(&path("data/report.csv"))
        )
    );
    assert_eq!(fs::read_to_string(path("data/report.csv")).unwrap(), "old");
    assert_eq!(entries(&path("data")), ["report.csv"]);

    fs::remove_dir_all(&project).unwrap();
    assert_eq!(
        edit(
            "task_no_directory",
            json!({"path": "marker.txt", "old_string": "old", "new_string": "new"}),
        ),
        failure(
            "cwd_not_found",
            "working directory does not exist",
            Some(&project)
        )
    );
    assert!(!project.exists());
    case.observe(
        "each edit's only report is terminal with report_seq 1 and decodes; only the successes \
         changed their files; the read-only file kept its content and no temporary file stayed \
         beside it; the special mode bits 07640 stayed; the removed directory was not created",
    );
}

#[test]
fn the_curly_quote_fallback_replaces_only_the_matched_span() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "old_string don't against a file with U+2019 in don’t and other curly quotes around it",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let file = tasks.project.join("note.txt");
    fs::write(
        &file,
        "\u{2018}a\u{2019} please don\u{2019}t \u{201c}b\u{201d}",
    )
    .unwrap();
    call(
        &tasks,
        "task_quote",
        "edit",
        json!({"path": "note.txt", "old_string": "don't", "new_string": "do not"}),
        policy(&tasks.project, "disabled"),
    );
    let written = "\u{2018}a\u{2019} please do not \u{201c}b\u{201d}";
    assert_eq!(
        outcome(&tasks, "task_quote"),
        success(1, written.len() as u64)
    );
    assert_eq!(fs::read_to_string(&file).unwrap(), written);
    case.observe("one replacement; every curly quote outside the span kept");
}

#[test]
fn an_edit_that_fails_its_schema_or_budget_is_rejected_and_one_within_its_budget_runs() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "an empty old_string, file_path without path, and an input of exactly 4,194,304 bytes",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let file = tasks.project.join("file");
    fs::write(&file, "before").unwrap();
    let invalid = json!({
        "code": "invalid_input",
        "message": "the instruction does not match its schema or exceeds a size budget",
    });
    for (task, input) in [
        (
            "task_empty",
            json!({"path": "file", "old_string": "", "new_string": ""}),
        ),
        (
            "task_alias",
            json!({"file_path": "file", "old_string": "before", "new_string": "after"}),
        ),
    ] {
        call(
            &tasks,
            task,
            "edit",
            input,
            policy(&tasks.project, "disabled"),
        );
        assert_eq!(rejection(&tasks, task), invalid, "{task}");
    }

    let mut input = json!({"path": "file", "old_string": "before", "new_string": ""});
    let filler = 4_194_304 - serde_json::to_vec(&input).unwrap().len();
    input["new_string"] = json!("x".repeat(filler));
    assert_eq!(serde_json::to_vec(&input).unwrap().len(), 4_194_304);
    call(
        &tasks,
        "task_exact",
        "edit",
        input,
        policy(&tasks.project, "disabled"),
    );
    assert_eq!(outcome(&tasks, "task_exact"), success(1, filler as u64));
    assert_eq!(fs::read(&file).unwrap(), vec![b'x'; filler]);
    case.observe("both rejected invalid_input with no change; the 4 MiB input ran to success");
}

#[test]
fn a_file_the_policy_denies_cannot_be_edited_although_the_app_server_could_read_it() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("an edit of a file under a deny rule inside the writable binding");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let secret = tasks.project.join("secret.txt");
    fs::write(&secret, "old").unwrap();
    let mut denied = policy(&tasks.project, "disabled");
    denied["filesystem"]["rules"]
        .as_array_mut()
        .unwrap()
        .push(json!({"path": secret, "access": "deny"}));
    call(
        &tasks,
        "task_denied",
        "edit",
        json!({"path": "secret.txt", "old_string": "old", "new_string": "new"}),
        denied,
    );
    let outcome = outcome(&tasks, "task_denied");
    assert_eq!(
        (&outcome["status"], &outcome["result"]),
        (&json!("error"), &json!({})),
        "{outcome:#}"
    );
    assert_eq!(fs::read_to_string(&secret).unwrap(), "old");
    case.observe(format!("{}; the file kept its content", outcome["error"]));
}

#[test]
fn a_cancel_of_an_edit_whose_child_has_started_ends_it_cancelled_after_the_start() {
    let _socket_dir = socket_directory::shared();
    let record = tetral_tests::record!();
    let mut case = record.case("tool.cancel while the edit's child waits at file_tool.started");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let seams = &tasks.machine().seams;
    fs::write(tasks.project.join("file"), "old").unwrap();
    seams.hold(FILE_TOOL_STARTED);
    call(
        &tasks,
        "task_held",
        "edit",
        json!({"path": "file", "old_string": "old", "new_string": "new"}),
        policy(&tasks.project, "direct"),
    );
    seams.wait_until_waiting(FILE_TOOL_STARTED, 1);
    let cancel = tasks.cancel("task_held", json!({}));
    let reply = tasks.reply(&cancel);
    let receipt = &reply["result"]["result"];
    assert_eq!(
        (&receipt["cancelled"], &receipt["was_running"]),
        (&json!(true), &json!(true)),
        "{reply:#}"
    );
    assert_eq!(
        outcome(&tasks, "task_held"),
        failure("cancelled", "cancelled after the call started", None)
    );
    seams.release(FILE_TOOL_STARTED);
    assert_eq!(
        fs::read_to_string(tasks.project.join("file")).unwrap(),
        "old"
    );
    case.observe("the reply cancelled and was_running; terminal cancelled after the call started");
}
