//! `edit` and `view_image` between `tetral server run`, whose ready
//! advertises `tool_execution`, and the reference control server as
//! separate processes, each on its own controlled clock, with the tools'
//! restricted children and real files: each step of an edit that decides
//! its result, the curly-quote fallback that keeps every other byte, the
//! write that keeps the file's mode, the images that image acceptance takes
//! and refuses at its limits, files that are no image, files the policy
//! denies, a missing file, a directory and a path longer than any file's,
//! each failure of the working directory, the input budget and a cancel
//! while the child runs. Every report is read back with the protocol's types. The
//! test seams these tests use are those `tetral_tests::tasks` names and the
//! barrier `file_tool.started`, at which a file tool's child waits once it
//! has started.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use base64::Engine;
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

/// What the only report of `task_id`, a `terminal` one of `tool` with
/// `report_seq` 1 that decodes with the protocol's types, says: its status,
/// error and inner result.
fn outcome(tasks: &TaskMachine, task_id: &str, tool: &str) -> Value {
    let report = tasks.terminal(task_id);
    decoded(&report);
    assert_eq!(tasks.reports(task_id).len(), 1, "{task_id}");
    assert_eq!(
        (&report["report_seq"], &report["result"]["tool"]),
        (&json!(1), &json!(tool)),
        "{report:#}"
    );
    let envelope = &report["result"];
    json!({"status": envelope["status"], "error": envelope["error"], "result": envelope["result"]})
}

fn edited(tasks: &TaskMachine, task_id: &str) -> Value {
    outcome(tasks, task_id, "edit")
}

fn viewed(tasks: &TaskMachine, task_id: &str) -> Value {
    outcome(tasks, task_id, "view_image")
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
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "edits of real files in the binding's directory: a unique match, identical strings, two \
         and three spans, replace_all, no whitespace fallback, a file over the cap and a \
         replacement over it, special mode bits and a file under a read rule",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let project = tasks.project.clone();
    let path = |name: &str| project.join(name);
    let edit = |task: &str, input: Value| {
        call(&tasks, task, "edit", input, policy(&project, "disabled"));
        edited(&tasks, task)
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
    assert_eq!(mode & 0o777, 0o640, "{mode:o}");
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
        edited(&tasks, "task_read_only"),
        failure(
            "write_failed",
            "write failed",
            Some(&path("data/report.csv"))
        )
    );
    assert_eq!(fs::read_to_string(path("data/report.csv")).unwrap(), "old");
    assert_eq!(entries(&path("data")), ["report.csv"]);
    case.observe(
        "each edit's only report is terminal with report_seq 1 and decodes; only the successes \
         changed their files; the read-only file kept its content and no temporary file stayed \
         beside it; the ordinary permission bits 0640 stayed",
    );
    case.pass();
}

#[test]
fn the_curly_quote_fallback_replaces_only_the_matched_span() {
    let _socket_dir = socket_directory::isolated();
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
        edited(&tasks, "task_quote"),
        success(1, written.len() as u64)
    );
    assert_eq!(fs::read_to_string(&file).unwrap(), written);
    case.observe("one replacement; every curly quote outside the span kept");
    case.pass();
}

#[test]
fn an_edit_that_fails_its_schema_or_budget_is_rejected_and_one_within_its_budget_runs() {
    let _socket_dir = socket_directory::isolated();
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
    assert_eq!(edited(&tasks, "task_exact"), success(1, filler as u64));
    assert_eq!(fs::read(&file).unwrap(), vec![b'x'; filler]);
    case.observe("both rejected invalid_input with no change; the 4 MiB input ran to success");
    case.pass();
}

#[test]
fn files_the_policy_denies_or_the_cli_protects_are_neither_edited_nor_viewed() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "an edit and a view_image under a deny rule inside the writable binding, and a \
         view_image of an image in the state root, by its path and through a symbolic link",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let secret = tasks.project.join("secret");
    fs::create_dir(&secret).unwrap();
    fs::write(secret.join("note.txt"), "old").unwrap();
    fs::write(secret.join("picture.png"), PNG).unwrap();
    let protected = tasks.root.join("picture.png");
    fs::write(&protected, PNG).unwrap();
    std::os::unix::fs::symlink(&protected, tasks.project.join("link.png")).unwrap();
    let mut denied = policy(&tasks.project, "disabled");
    denied["filesystem"]["rules"]
        .as_array_mut()
        .unwrap()
        .push(json!({"path": secret, "access": "deny"}));
    call(
        &tasks,
        "task_edit",
        "edit",
        json!({"path": "secret/note.txt", "old_string": "old", "new_string": "new"}),
        denied.clone(),
    );
    let mut outcomes = vec![edited(&tasks, "task_edit")];
    for (task, path) in [
        ("task_denied", json!("secret/picture.png")),
        ("task_root", json!(protected)),
        ("task_link", json!("link.png")),
    ] {
        call(
            &tasks,
            task,
            "view_image",
            json!({"path": path}),
            denied.clone(),
        );
        outcomes.push(viewed(&tasks, task));
    }
    for outcome in &outcomes {
        assert_eq!(
            (&outcome["status"], &outcome["result"]),
            (&json!("error"), &json!({})),
            "{outcome:#}"
        );
    }
    assert_eq!(fs::read_to_string(secret.join("note.txt")).unwrap(), "old");
    let errors: Vec<String> = outcomes
        .iter()
        .map(|outcome| outcome["error"].to_string())
        .collect();
    case.observe(format!("{}; the note kept its content", errors.join("; ")));
    case.pass();
}

/// The input of `tool` for `path`: an `edit` of `old` to `new`.
fn file_input(tool: &str, path: Value) -> Value {
    match tool {
        "edit" => json!({"path": path, "old_string": "old", "new_string": "new"}),
        _ => json!({"path": path}),
    }
}

#[test]
fn a_missing_or_overlong_path_and_each_failure_of_the_working_directory_give_their_kinds() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record
        .case("edits of a missing file, of a directory and of an absolute path of 400,000 bytes");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let project = tasks.project.clone();
    let run = |task: &str, tool: &str, path: Value| {
        call(
            &tasks,
            task,
            tool,
            file_input(tool, path),
            policy(&project, "disabled"),
        );
        outcome(&tasks, task, tool)
    };
    assert_eq!(
        run("task_missing", "edit", json!("missing.txt")),
        failure(
            "not_found",
            "path does not exist",
            Some(&project.join("missing.txt"))
        )
    );
    assert!(!project.join("missing.txt").exists());
    fs::create_dir(project.join("folder")).unwrap();
    assert_eq!(
        run("task_folder", "edit", json!("folder")),
        failure(
            "is_directory",
            "path is a directory",
            Some(&project.join("folder"))
        )
    );
    let overlong = format!("/{}", "a".repeat(399_999));
    let refused = run("task_overlong", "edit", json!(overlong));
    let path = refused["error"]["path"].as_str().unwrap();
    assert_eq!(
        (&refused["status"], &refused["error"]["kind"]),
        (&json!("error"), &json!("not_found")),
        "{refused:#}"
    );
    assert!(path.len() <= 4_096, "{} bytes", path.len());
    assert!(
        path.starts_with("/a") && path.ends_with('a') && path.contains("\n[... truncated ...]\n")
    );
    case.observe(
        "not_found \"path does not exist\" and no file created; is_directory \"path is a \
         directory\"; the overlong path's not_found report accepted with an error.path of at \
         most 4,096 bytes around the seam",
    );

    let failures = |state: &str, kind: &str, message: &str| {
        for tool in ["edit", "view_image"] {
            let task = format!("task_{state}_{tool}");
            assert_eq!(
                run(&task, tool, json!("note.txt")),
                failure(kind, message, Some(&project)),
                "{tool} with the working directory {state}"
            );
        }
    };
    case.pass();

    let mut case = record.case(
        "an edit and a view_image by relative path with the binding's directory removed, then \
         replaced by a file",
    );
    fs::remove_dir_all(&project).unwrap();
    failures("removed", "cwd_not_found", "cwd does not exist");
    assert!(!project.exists());
    fs::write(&project, "x").unwrap();
    failures("file", "cwd_not_directory", "cwd is not a directory");
    case.observe(
        "cwd_not_found naming the directory, which was not created, then cwd_not_directory, for \
         both tools",
    );
    case.pass();

    let mut case = record.case(
        "an edit and a view_image by relative path with the binding's directory of mode 0000",
    );
    if !case.runs_as_non_root("a directory of mode 0000 keeps out only a user who is not root") {
        return;
    }
    fs::remove_file(&project).unwrap();
    fs::create_dir(&project).unwrap();
    fs::set_permissions(&project, fs::Permissions::from_mode(0o000)).unwrap();
    let outcomes = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        failures("locked", "cwd_inaccessible", "cwd is not accessible")
    }));
    fs::set_permissions(&project, fs::Permissions::from_mode(0o700)).unwrap();
    if let Err(panic) = outcomes {
        std::panic::resume_unwind(panic);
    }
    case.observe("cwd_inaccessible naming the directory for both tools");
    case.pass();
}

#[test]
fn a_cancel_of_an_edit_whose_child_has_started_ends_it_cancelled_after_the_start() {
    let _socket_dir = socket_directory::isolated();
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
        edited(&tasks, "task_held"),
        failure("cancelled", "cancelled after the call started", None)
    );
    seams.release(FILE_TOOL_STARTED);
    assert_eq!(
        fs::read_to_string(tasks.project.join("file")).unwrap(),
        "old"
    );
    case.observe("the reply cancelled and was_running; terminal cancelled after the call started");
    case.pass();
}

/// A PNG that an encoder wrote, of 33×17 pixels.
const PNG: &[u8] = include_bytes!("../../crates/protocol/tests/images/image.png");

/// The CRC-32 that a PNG chunk ends with.
fn crc32(bytes: &[u8]) -> u32 {
    let table: Vec<u32> = (0..256u32)
        .map(|byte| {
            (0..8).fold(byte, |crc, _| {
                if crc & 1 == 1 {
                    (crc >> 1) ^ 0xedb8_8320
                } else {
                    crc >> 1
                }
            })
        })
        .collect();
    !bytes.iter().fold(!0u32, |crc, &byte| {
        table[((crc ^ u32::from(byte)) & 0xff) as usize] ^ (crc >> 8)
    })
}

fn chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let length = u32::try_from(data.len()).unwrap().to_be_bytes();
    let body = [&kind[..], data].concat();
    [&length[..], &body, &crc32(&body).to_be_bytes()].concat()
}

/// The signature and `IHDR` of a PNG of `width`×`height` 8-bit gray pixels.
fn png_header(width: u32, height: u32) -> Vec<u8> {
    let mut header = width.to_be_bytes().to_vec();
    header.extend_from_slice(&height.to_be_bytes());
    header.extend_from_slice(&[8, 0, 0, 0, 0]);
    [&b"\x89PNG\r\n\x1a\n"[..], &chunk(b"IHDR", &header)].concat()
}

/// The `IDAT` of black pixels, `height` rows of `width`, in a zlib stream of
/// stored blocks.
fn png_pixels(width: u32, height: u32) -> Vec<u8> {
    let raw = vec![0u8; height as usize * (1 + width as usize)];
    let mut stream = vec![0x78, 0x01];
    let blocks: Vec<&[u8]> = raw.chunks(65_535).collect();
    for (index, block) in blocks.iter().enumerate() {
        let length = u16::try_from(block.len()).unwrap();
        stream.push(u8::from(index + 1 == blocks.len()));
        stream.extend_from_slice(&length.to_le_bytes());
        stream.extend_from_slice(&(!length).to_le_bytes());
        stream.extend_from_slice(block);
    }
    let (a, b) = raw.iter().fold((1u32, 0u32), |(a, b), &byte| {
        let a = (a + u32::from(byte)) % 65_521;
        (a, (b + a) % 65_521)
    });
    stream.extend_from_slice(&((b << 16) | a).to_be_bytes());
    chunk(b"IDAT", &stream)
}

/// A whole PNG of black pixels, padded to `size` bytes with a private
/// ancillary chunk when `size` is given.
fn png(width: u32, height: u32, size: Option<usize>) -> Vec<u8> {
    let header = png_header(width, height);
    let pixels = png_pixels(width, height);
    let end = chunk(b"IEND", &[]);
    let filler = size.map_or_else(Vec::new, |size| {
        let room = size - header.len() - pixels.len() - end.len() - 12;
        chunk(b"prVt", &vec![0; room])
    });
    [header, filler, pixels, end].concat()
}

fn media(mime: &str, bytes: &[u8]) -> Value {
    json!({
        "status": "success", "error": null,
        "result": {
            "mime": mime, "size_bytes": bytes.len(),
            "data_base64": base64::engine::general_purpose::STANDARD.encode(bytes),
        },
    })
}

fn refusal(bytes: &[u8], width: u32, height: u32, path: &Path) -> Value {
    let message = format!(
        "image is {} bytes and {width}x{height} pixels; the limit is 7500000 bytes, 8000 pixels \
         per side and 30000 patches of 32x32 pixels. Shrink or compress the image, then view it \
         again.",
        bytes.len()
    );
    failure("too_large", &message, Some(path))
}

#[test]
fn an_image_at_the_acceptance_limits_is_viewed_whole_and_one_past_any_limit_is_too_large() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "PNGs of 7,500,000 bytes at 2000x2000, of 7,500,001 bytes, of 8001x10, of 6000x6000, and \
         one whose IHDR is cut off",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let path = |name: &str| tasks.project.join(name);
    let view = |task: &str, name: &str, bytes: &[u8]| {
        fs::write(path(name), bytes).unwrap();
        call(
            &tasks,
            task,
            "view_image",
            json!({"path": name}),
            policy(&tasks.project, "disabled"),
        );
        viewed(&tasks, task)
    };

    let exact = png(2_000, 2_000, Some(7_500_000));
    assert_eq!(exact.len(), 7_500_000);
    assert!(view("task_exact", "exact.png", &exact) == media("image/png", &exact));

    let over = png(2_000, 2_000, Some(7_500_001));
    assert_eq!(
        view("task_over", "over.png", &over),
        refusal(&over, 2_000, 2_000, &path("over.png"))
    );
    let wide = png(8_001, 10, None);
    assert_eq!(
        view("task_wide", "wide.png", &wide),
        refusal(&wide, 8_001, 10, &path("wide.png"))
    );
    // Refused from its header, it needs no pixels: 188 × 188 = 35,344
    // patches.
    let patches = [png_header(6_000, 6_000), chunk(b"IEND", &[])].concat();
    assert_eq!(
        view("task_patches", "patches.png", &patches),
        refusal(&patches, 6_000, 6_000, &path("patches.png"))
    );
    let cut = &png_header(10, 10)[..20];
    assert_eq!(
        view("task_cut", "cut.png", cut),
        failure(
            "unsupported_format",
            "image dimensions could not be read",
            Some(&path("cut.png"))
        )
    );
    case.observe(
        "the 7,500,000-byte image accepted by the reference server whole in standard Base64; the \
         three over a limit too_large with their size and dimensions; the cut IHDR \
         unsupported_format",
    );
    case.pass();
}

#[test]
fn each_image_format_is_recognized_by_its_content_and_anything_else_is_refused() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a PNG, a JPEG, a GIF87a, a GIF89a and a WebP that encoders wrote; text named .png, an \
         empty file, a named pipe, a PDF, a directory and a missing path",
    );
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    let path = |name: &str| tasks.project.join(name);
    let view = |task: &str, name: &str| {
        call(
            &tasks,
            task,
            "view_image",
            json!({"path": name}),
            policy(&tasks.project, "disabled"),
        );
        viewed(&tasks, task)
    };

    let gif = include_bytes!("../../crates/protocol/tests/images/image.gif");
    let gif89a = [&b"GIF89a"[..], &gif[6..]].concat();
    for (name, mime, bytes) in [
        ("image.dat", "image/png", PNG),
        (
            "baseline.jpg",
            "image/jpeg",
            &include_bytes!("../../crates/protocol/tests/images/baseline.jpg")[..],
        ),
        ("gif87a.gif", "image/gif", gif),
        ("gif89a.gif", "image/gif", &gif89a),
        (
            "lossy.webp",
            "image/webp",
            &include_bytes!("../../crates/protocol/tests/images/lossy.webp")[..],
        ),
    ] {
        fs::write(path(name), bytes).unwrap();
        assert!(
            view(&format!("task_{name}"), name) == media(mime, bytes),
            "{name}"
        );
    }

    let unsupported = |name: &str| {
        failure(
            "unsupported_format",
            "unsupported image format; supported formats are PNG, JPEG, GIF, and WebP",
            Some(&path(name)),
        )
    };
    fs::write(path("foreign.png"), "not actually an image").unwrap();
    assert_eq!(
        view("task_foreign", "foreign.png"),
        unsupported("foreign.png")
    );
    fs::write(
        path("document.pdf"),
        "%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF\n",
    )
    .unwrap();
    assert_eq!(
        view("task_pdf", "document.pdf"),
        unsupported("document.pdf")
    );

    fs::write(path("empty.gif"), "").unwrap();
    assert_eq!(
        view("task_empty", "empty.gif"),
        failure("unsupported_format", "empty file", Some(&path("empty.gif")))
    );
    rustix::fs::mknodat(
        rustix::fs::CWD,
        path("pipe.png"),
        rustix::fs::FileType::Fifo,
        rustix::fs::Mode::from(0o600),
        0,
    )
    .unwrap();
    assert_eq!(
        view("task_pipe", "pipe.png"),
        failure("unsupported_format", "empty file", Some(&path("pipe.png")))
    );

    fs::create_dir(path("images")).unwrap();
    assert_eq!(
        view("task_directory", "images"),
        failure("is_directory", "path is a directory", Some(&path("images")))
    );
    assert_eq!(
        view("task_missing", "missing.png"),
        failure(
            "not_found",
            "path does not exist",
            Some(&path("missing.png"))
        )
    );
    case.observe(
        "each image whole with its format's mime; text, the PDF unsupported_format; the empty \
         file and the named pipe empty file, without blocking; the directory is_directory; the \
         missing path not_found",
    );
    case.pass();
}
