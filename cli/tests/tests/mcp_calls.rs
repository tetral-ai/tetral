//! Local MCP calls across processes: `tetral` against the reference control
//! server, with the test MCP server as Computer Use's driver enabled on the
//! running App Server, under the controlled clock: admission, the one
//! `tools/call` and the one final report of each call, results and their
//! bounds, cancellation and the call timeout, and the ends of bindings,
//! Engine connections, MCP connections and the server.
#![cfg(target_os = "linux")]

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tetral_tests::app_server::{request, socket_path, wait_until};
use tetral_tests::machine::{INSTANCE, seconds};
use tetral_tests::mcp::{self, TestDriver, alive, group_alive, page, tool, wait_for_reports};
use tetral_tests::record::Case;
use tetral_tests::sessions::new_connection;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, LEDGER_ROOM, SESSION, TaskMachine};

const TEST_SERVER: &str = env!("CARGO_BIN_EXE_mcp-test-server");

/// The test server's scenario with one listed tool and `calls`.
fn scenario(calls: Value) -> Value {
    json!({ "pages": [page(None, vec![tool("click")], None)], "calls": calls })
}

/// A machine with a prepared binding whose Computer Use is enabled on the
/// running App Server, with the test server answering `calls` as its
/// driver, and whose first `cua` tool list has been reported.
struct Calls {
    tasks: TaskMachine,
    driver: TestDriver,
    _directory: tempfile::TempDir,
}

impl Calls {
    fn start(case: &mut Case, calls: Value) -> Calls {
        Calls::start_with(case, calls, TestDriver::new)
    }

    /// [`Calls::start`] with a test server that ignores SIGTERM, so that a
    /// stopped connection's process reads what was written to it until the
    /// SIGKILL 2 seconds later on the controlled clock.
    fn start_ignoring_sigterm(case: &mut Case, calls: Value) -> Calls {
        Calls::start_with(case, calls, TestDriver::ignoring_sigterm)
    }

    fn start_with(
        case: &mut Case,
        calls: Value,
        driver: fn(&Path, &Path, Value) -> TestDriver,
    ) -> Calls {
        let directory = tempfile::tempdir().unwrap();
        let driver = driver(
            &directory.path().join("driver"),
            Path::new(TEST_SERVER),
            scenario(calls),
        );
        let tasks =
            TaskMachine::start_with_environment(case, directory.path(), &[driver.environment()]);
        let run = mcp::enable(tasks.machine(), driver.driver());
        case.command(
            "tetral computer-use enable --app app_one --driver <test server>",
            &run.exit(),
        );
        assert_eq!(run.status.code(), Some(0), "{}", run.exit());
        wait_for_reports(&tasks.engine, 1);
        Calls {
            tasks,
            driver,
            _directory: directory,
        }
    }

    /// The `index`th process the driver started, once started: the server
    /// connection's is 0, the binding connections' follow.
    fn process(&self, index: usize) -> u32 {
        self.driver.wait_for_processes(index + 1)[index]
    }

    /// Waits until the process `pid` has received `count` calls and gives
    /// the last one.
    fn call_received(&self, pid: u32, count: usize) -> Value {
        self.driver.wait_for_received(pid, "tools/call", count)[count - 1].clone()
    }

    /// Every `tools/call` any process received.
    fn every_call(&self) -> usize {
        self.driver
            .processes()
            .into_iter()
            .map(|pid| self.driver.received(pid, "tools/call").len())
            .sum()
    }
}

/// A terminal report's status and error.
fn ended(report: &Value) -> Value {
    json!([report["result"]["status"], report["result"]["error"]])
}

/// The error of a `cancelled` terminal report.
fn cancelled(sent: bool) -> Value {
    let message = if sent {
        "cancelled after the call was sent"
    } else {
        "cancelled before the call was sent"
    };
    json!(["error", {"kind": "cancelled", "message": message, "detail": {"sent": sent}}])
}

/// A rejected report's code and message.
fn rejected(report: &Value) -> Value {
    json!([
        report["phase"],
        report["error"]["code"],
        report["error"]["message"]
    ])
}

/// A `cancel` reply's `cancelled` and `was_running`.
fn cancel_reply(reply: &Value) -> Value {
    let result = &reply["result"]["result"];
    json!([
        reply["result"]["tool"],
        result["cancelled"],
        result["was_running"]
    ])
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Issues `tool.cancel` of `task_id` of `session`'s binding `binding` and
/// gives its `message_id`.
fn cancel_in(tasks: &TaskMachine, session: &str, binding: &str, task_id: &str) -> String {
    let issued = tasks.engine.control(
        "cancel",
        json!({"instance_id": INSTANCE, "payload": {
            "session_id": session, "binding_id": binding,
            "tool_use_event_id": format!("call_cancel_{task_id}"), "task_id": task_id,
        }}),
    );
    issued["message_id"].as_str().unwrap().to_owned()
}

/// Waits for the accepted `session-status` report `status` of `binding`.
fn session_report(tasks: &TaskMachine, binding: &str, status: &str) -> Value {
    let mut found = Value::Null;
    wait_until(&format!("the {status} report of {binding}"), || match tasks
        .engine
        .received("session-status")
        .into_iter()
        .map(|received| received["body"].clone())
        .find(|body| body["binding_id"] == binding && body["status"] == status)
    {
        Some(body) => {
            found = body;
            true
        }
        None => false,
    });
    found
}

#[test]
fn a_call_staged_behind_its_prepared_ack_is_written_once_as_received_in_its_bindings_directory() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "Computer Use enabled on the running App Server; a binding whose prepared ACK is withheld, with one mcp.call staged and one cancelled before the ACK; then the ACK",
    );
    let result = json!({
        "content": [{"type": "text", "text": "done", "annotations": {"audience": ["user"]}, "extra": 1}],
        "structuredContent": {"clicked": true},
        "_meta": {"trace": "t1"},
        "note": {"later": "a member MCP may add"},
    });
    let calls = Calls::start(&mut case, json!({"click": {"result": result}}));
    let tasks = &calls.tasks;
    let connection = tasks.machine().status()["connection_id"].clone();
    let arguments = json!({"x": 1.5, "path": "shot.png", "keys": [null, {"é": "\u{1F600}"}]});

    tasks.prepare_pending("sess_staged", "bind_staged");
    tasks.mcp_call_with(
        "sess_staged",
        "bind_staged",
        "task_staged",
        json!({"server": "cua", "tool": "click", "arguments": arguments}),
    );
    tasks.mcp_call_with(
        "sess_staged",
        "bind_staged",
        "task_withdrawn",
        json!({"server": "cua", "tool": "click", "arguments": {}}),
    );
    let cancel = cancel_in(tasks, "sess_staged", "bind_staged", "task_withdrawn");
    tasks.handled("sess_staged", "bind_staged");
    assert_eq!(
        calls.driver.processes().len(),
        1,
        "a binding connection before the ACK"
    );
    tasks.release_answers();

    let report = tasks.terminal("task_staged");
    let pid = calls.process(1);
    let written = calls.driver.received(pid, "tools/call");
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!(
        written[0]["params"],
        json!({"name": "click", "arguments": arguments})
    );
    assert_eq!(
        calls.driver.started(pid)["cwd"],
        json!(tasks.project),
        "the binding connection's directory"
    );
    assert_eq!(
        json!([
            report["report_seq"],
            report["result"]["tool"],
            report["result"]["status"],
            report["result"]["truncated"],
            report["result"]["error"],
            report["result"].get("drive_sync").is_some(),
        ]),
        json!([1, "mcp", "success", false, null, false]),
        "{report:#}"
    );
    let mut expected = result.clone();
    expected.as_object_mut().unwrap().remove("_meta");
    assert_eq!(report["result"]["result"], expected);
    assert_eq!(tasks.reports("task_staged").len(), 1);

    assert_eq!(
        cancel_reply(&tasks.reply(&cancel)),
        json!(["cancel", true, false])
    );
    assert_eq!(ended(&tasks.terminal("task_withdrawn")), cancelled(false));
    tasks.answers_settled();
    assert_eq!(calls.every_call(), 1);
    assert_eq!(tasks.machine().status()["connection_id"], connection);
    case.observe("no binding connection before the ACK; after it one tools/call with params {name, arguments} equal to the instruction's, from a process in the binding's directory; one terminal report, report_seq 1, success, not truncated, no drive_sync, the result without _meta, with its other top-level member and the block's extra member; the cancelled call replied cancelled and not running and ended cancelled with sent false, never written; the Engine connection unchanged since the enable");
    case.pass();

    let mut case = record.case(
        "notifications/tools/list_changed sent on the open binding connection, the server's list now [click, added]",
    );
    let server = calls.process(0);
    let listed = calls.driver.received(server, "tools/list").len();
    let changed = json!([page(None, vec![tool("click"), tool("added")], None)]);
    calls.driver.control(server, json!({"pages": changed}));
    calls.driver.control(
        pid,
        json!({"send": {"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}}),
    );
    calls
        .driver
        .wait_for_received(server, "tools/list", listed + 1);
    let report = wait_for_reports(&tasks.engine, 2)[1].clone();
    assert_eq!(
        json!([mcp::names(&report), report["seq"]]),
        json!([["click", "added"], 2])
    );
    assert!(calls.driver.received(pid, "tools/list").is_empty());
    case.observe("one tools/list on the server connection, none on the binding connection; one report with seq 2 and tools [click, added]");
    case.pass();
}

#[test]
fn admission_refuses_bad_arguments_a_server_not_running_and_a_full_ledger_before_any_tools_call() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "arguments of 65,536 and 65,537 bytes, an array and a payload with sandbox_policy; a call for a binding not prepared on this connection; then a call with a full replay ledger",
    );
    let calls = Calls::start(&mut case, json!({"click": {"result": {"content": []}}}));
    let tasks = &calls.tasks;
    // `{"a":"<n letters>"}` is n + 8 bytes of compact JSON.
    let of_bytes = |bytes: usize| json!({"a": "x".repeat(bytes - 8)});
    tasks.mcp_call("task_at_limit", "click", of_bytes(65_536));
    assert_eq!(
        tasks.terminal("task_at_limit")["result"]["status"],
        "success"
    );
    let arguments = "arguments must be a JSON object of at most 65536 bytes";
    for (task, members, message) in [
        (
            "task_over",
            json!({"server": "cua", "tool": "click", "arguments": of_bytes(65_537)}),
            Some(arguments),
        ),
        (
            "task_array",
            json!({"server": "cua", "tool": "click", "arguments": [1]}),
            Some(arguments),
        ),
        (
            "task_policy",
            json!({"server": "cua", "tool": "click", "arguments": {},
                "sandbox_policy": {"schema_version": 1, "filesystem": {"rules": []}, "network": {"mode": "disabled"}}}),
            None,
        ),
    ] {
        let issued = tasks.mcp_call_with(SESSION, BINDING, task, members);
        let report = tasks.answer_to(&issued);
        assert_eq!(
            json!([report["phase"], report["error"]["code"]]),
            json!(["rejected", "invalid_input"]),
            "{task}: {report:#}"
        );
        let text = report["error"]["message"].as_str().unwrap();
        match message {
            Some(message) => assert_eq!(text, message, "{task}"),
            None => assert!(text.starts_with("invalid mcp.call: "), "{task}: {text}"),
        }
    }

    let issued = tasks.mcp_call_with(
        SESSION,
        "bind_unprepared",
        "task_unprepared",
        json!({"server": "cua", "tool": "click", "arguments": {}}),
    );
    let report = tasks.answer_to(&issued);
    assert_eq!(
        json!([report["phase"], report["error"]["code"]]),
        json!(["rejected", "binding_unavailable"]),
        "{report:#}"
    );

    // Every earlier instruction's entry is freed by its final ACK.
    tasks.answers_settled();
    tasks.fill_ledger(LEDGER_ROOM);
    let issued = tasks.mcp_call("task_exhausted", "click", json!({}));
    let report = tasks.answer_to(&issued);
    assert_eq!(
        json!([report["phase"], report["error"]["code"]]),
        json!(["rejected", "resource_exhausted"])
    );
    assert_eq!(calls.every_call(), 1);
    case.observe("65,536 bytes admitted and answered; 65,537 bytes and an array rejected invalid_input with the arguments message, sandbox_policy with an invalid mcp.call message; a call for a binding never prepared rejected binding_unavailable; with 4,032 ledger entries held the call was rejected resource_exhausted; one tools/call in all");
    case.pass();
    drop(calls);

    let mut case = record.case("a call on an App Server whose Computer Use was never enabled");
    let directory = tempfile::tempdir().unwrap();
    let tasks = TaskMachine::start(&mut case, directory.path());
    tasks.mcp_call("task_unavailable", "click", json!({}));
    assert_eq!(
        rejected(&tasks.report("task_unavailable", "rejected")),
        json!([
            "rejected",
            "mcp_server_unavailable",
            "server cua is not running"
        ])
    );
    case.observe("rejected mcp_server_unavailable: server cua is not running");
    case.pass();
    drop(tasks);

    let mut case = record.case("a call while cua's start waits for its initialize result");
    let directory = tempfile::tempdir().unwrap();
    let mut starting = scenario(json!({"click": {"result": {"content": []}}}));
    starting["initialize"] = json!("never");
    let driver = TestDriver::new(
        &directory.path().join("driver"),
        Path::new(TEST_SERVER),
        starting,
    );
    let tasks = TaskMachine::start_prepared_with_environment(
        &mut case,
        directory.path(),
        &[driver.environment()],
        |root| mcp::record_before_start(root, &["enable", "--driver", driver.driver()]),
    );
    let server = driver.wait_for_processes(1)[0];
    driver.wait_for_received(server, "initialize", 1);
    tasks.mcp_call("task_starting", "click", json!({}));
    assert_eq!(
        rejected(&tasks.report("task_starting", "rejected")),
        json!([
            "rejected",
            "mcp_server_unavailable",
            "server cua is not running"
        ])
    );
    assert_eq!(driver.processes(), [server]);
    case.observe("while the server connection's initialize was unanswered, the call was rejected mcp_server_unavailable: server cua is not running, and no binding connection was spawned");
    case.pass();
}

#[test]
fn a_cancel_reaches_the_server_at_once_a_late_answer_is_dropped_and_an_unanswered_call_times_out_at_300_seconds()
 {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a call held by the server: tool.stdin, tool.cancel, a second cancel, then the late answer; a cancel after an answer; a call never answered",
    );
    let calls = Calls::start(
        &mut case,
        json!({
            "slow": {"result": {"content": [{"type": "text", "text": "late"}]}, "hold": "late"},
            "quick": {"result": {"content": []}},
            "silent": {"never": true},
        }),
    );
    let tasks = &calls.tasks;
    let seams = &tasks.machine().seams;

    tasks.mcp_call("task_slow", "slow", json!({}));
    let pid = calls.process(1);
    let slow = calls.call_received(pid, 1);
    let read = tasks.stdin("task_slow", json!({"chars": ""}));
    assert_eq!(tasks.reply(&read)["error"]["code"], "unsupported_control");

    let before = unix_ms();
    let cancel = tasks.cancel("task_slow", json!({}));
    let (at, notification) = {
        calls
            .driver
            .wait_for_received(pid, "notifications/cancelled", 1);
        calls.driver.received_at(pid, "notifications/cancelled")[0].clone()
    };
    assert_eq!(
        notification["params"],
        json!({"requestId": slow["id"], "reason": "cancelled by Engine"})
    );
    assert!(
        at <= before + 1_000,
        "the notification came {} ms after the cancel",
        at - before
    );
    assert_eq!(
        cancel_reply(&tasks.reply(&cancel)),
        json!(["cancel", true, true])
    );
    assert_eq!(ended(&tasks.terminal("task_slow")), cancelled(true));
    let again = tasks.cancel("task_slow", json!({}));
    assert_eq!(
        cancel_reply(&tasks.reply(&again)),
        json!(["cancel", true, true])
    );
    assert_eq!(
        calls.driver.received(pid, "notifications/cancelled").len(),
        1
    );

    calls.driver.control(pid, json!({"release": "late"}));
    wait_until("the late answer written", || {
        calls.driver.answered(pid).contains(&slow["id"])
    });
    // The connection's answers are read in order: once the next call's
    // report is in, the late answer has been read.
    tasks.mcp_call("task_quick", "quick", json!({}));
    assert_eq!(tasks.terminal("task_quick")["result"]["status"], "success");
    let after = tasks.cancel("task_quick", json!({}));
    assert_eq!(
        cancel_reply(&tasks.reply(&after)),
        json!(["cancel", false, false])
    );
    tasks.answers_settled();
    assert_eq!(tasks.reports("task_slow").len(), 1);
    assert_eq!(tasks.reports("task_quick").len(), 1);
    case.observe("tool.stdin got unsupported_control; notifications/cancelled with the call's requestId and reason cancelled by Engine reached the server within 1 second; the reply cancelled and running, the report cancelled with sent true; a second cancel replied the same and sent nothing; the late answer made no report; a cancel after an answer replied not cancelled and not running");
    case.pass();

    let mut case = record.case("a call the server never answers, at 299.999 and 300 seconds");
    tasks.mcp_call("task_silent", "silent", json!({}));
    let silent = calls.call_received(pid, 3);
    tasks.advance(seconds(299.0));
    seams.advance_within(seconds(0.999), "mcp.call_checked");
    assert!(tasks.reports("task_silent").is_empty());
    assert_eq!(
        calls.driver.received(pid, "notifications/cancelled").len(),
        1
    );
    seams.advance(seconds(0.001));
    assert_eq!(
        ended(&tasks.terminal("task_silent")),
        json!(["error", {"kind": "mcp_timeout", "message": "no answer within 300 seconds"}])
    );
    let notifications = calls
        .driver
        .wait_for_received(pid, "notifications/cancelled", 2);
    assert_eq!(
        notifications[1]["params"],
        json!({"requestId": silent["id"], "reason": "request timeout"})
    );
    case.observe("nothing at 299.999 seconds after the write; at 300 seconds notifications/cancelled with reason request timeout and the report mcp_timeout");
    case.pass();
}

#[test]
fn errors_and_results_keep_what_the_server_answered_within_the_error_bounds() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a JSON-RPC error -32602, a result with isError true, and an error with a 10,000-byte message and 100 KiB of data",
    );
    let failing = json!({"content": [{"type": "text", "text": "no such window"}], "isError": true});
    let calls = Calls::start(
        &mut case,
        json!({
            "failing": {"result": failing},
            "loud": {"error": {"code": -32000, "message": "$text:10000", "data": "$text:102400"}},
        }),
    );
    let tasks = &calls.tasks;

    tasks.mcp_call("task_unknown", "absent", json!({}));
    assert_eq!(
        ended(&tasks.terminal("task_unknown")),
        json!(["error", {"kind": "mcp_error", "message": "unknown tool",
            "detail": {"code": -32602, "message": "unknown tool"}}])
    );
    tasks.mcp_call("task_failing", "failing", json!({}));
    let report = tasks.terminal("task_failing");
    assert_eq!(
        json!([report["result"]["status"], report["result"]["result"]]),
        json!(["success", failing])
    );

    tasks.mcp_call("task_loud", "loud", json!({}));
    let error = tasks.terminal("task_loud")["result"]["error"].clone();
    let detail = &error["detail"];
    let detail_bytes = serde_json::to_string(detail).unwrap().len();
    assert_eq!(
        json!([
            error["kind"],
            error["message"].as_str().unwrap().len(),
            detail["code"],
            detail["truncated"]
        ]),
        json!(["mcp_error", 4_096, -32000, true]),
        "{error:#}"
    );
    assert!(detail_bytes <= 32_768, "a detail of {detail_bytes} bytes");
    case.observe(format!(
        "-32602 gave mcp_error with detail.code -32602; isError true gave success with isError kept; the large error gave mcp_error with a 4,096-byte message and a {detail_bytes}-byte detail with code -32000 and truncated true, and its report was accepted"
    ));
    case.pass();
}

/// The text block that replaces a refused image of `bytes` bytes and
/// `width` by `height` pixels.
fn omitted(bytes: u64, width: u32, height: u32) -> Value {
    json!({"type": "text", "text": format!(
        "Image omitted: it is {bytes} bytes and {width}x{height} pixels, larger than the supported models accept (at most 7500000 bytes, 8000 pixels per side and 30000 patches of 32x32 pixels)."
    )})
}

fn image(data: &str) -> Value {
    json!({"type": "image", "data": data, "mimeType": "image/png"})
}

/// The compact JSON size of a success envelope whose content is one text
/// block of no letters, as an App Server whose clock stands still makes it.
fn empty_text_envelope_bytes() -> usize {
    serde_json::to_string(&json!({
        "schema_version": 1, "tool": "mcp", "status": "success", "truncated": false, "error": null,
        "result": {"content": [{"type": "text", "text": ""}]},
        "metrics": {"wall_time_ms": 0, "queued_ms": 0, "drive_wait_ms": 0, "run_ms": 0, "sync_ms": 0},
    }))
    .unwrap()
    .len()
}

/// The text block that, after `images` image blocks as [`image`] makes
/// them, brings a success envelope without its media to `bytes`.
fn filling(images: usize, bytes: usize) -> Value {
    let image_bytes = serde_json::to_string(&image("")).unwrap().len() + 1;
    let letters = bytes - empty_text_envelope_bytes() - images * image_bytes;
    json!({"type": "text", "text": format!("$text:{letters}")})
}

#[test]
fn media_are_measured_decoded_refused_images_are_replaced_and_invalid_base64_is_refused() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "10,485,760 decoded media bytes with a 262,144-byte envelope without them, then one byte over either bound; refused images beside a text block, invalid base64, a resource blob and a result without content",
    );
    let text = json!({"type": "text", "text": "beside"});
    let blob = json!({"type": "resource", "resource": {
        "uri": "file:///data.bin", "mimeType": "application/octet-stream", "blob": "$bytes:10"}});
    let result = |content: Vec<Value>| json!({"result": {"content": content}});
    let calls = Calls::start(
        &mut case,
        json!({
            "within": result(vec![image("$png:100x100:5242880"), image("$png:100x100:5242880"), filling(2, 262_144)]),
            "over": result(vec![image("$png:100x100:5242880"), image("$png:100x100:5242881"), filling(2, 262_144)]),
            "envelope_over": result(vec![image("$png:100x100:5242880"), image("$png:100x100:5242880"), filling(2, 262_145)]),
            "heavy": result(vec![image("$png:100x100:7500001"), text.clone()]),
            "wide": result(vec![text.clone(), image("$png:9000x100:1000")]),
            "patches": result(vec![json!({"type": "resource", "resource": {
                "uri": "file:///shot.png", "mimeType": "image/png", "blob": "$png:6000x6000:1000"}}), text.clone()]),
            "unpadded": result(vec![image("QQ")]),
            "broken": result(vec![image("QQ\n==")]),
            "url_safe": result(vec![image("-_8=")]),
            "blob": result(vec![blob]),
            "bare": {"result": {"structuredContent": {}}},
        }),
    );
    let tasks = &calls.tasks;
    let call = |tool: &str| -> Value {
        let task = format!("task_{tool}");
        tasks.mcp_call(&task, tool, json!({}));
        tasks.terminal(&task)["result"].clone()
    };

    let within = call("within");
    let content = within["result"]["content"].as_array().unwrap();
    assert_eq!(within["status"], "success");
    let filled = filling(2, 262_144)["text"].as_str().unwrap()["$text:".len()..]
        .parse::<usize>()
        .unwrap();
    assert_eq!(
        [
            content[0]["data"].as_str().unwrap().len(),
            content[1]["data"].as_str().unwrap().len(),
            content[2]["text"].as_str().unwrap().len()
        ],
        [6_990_508, 6_990_508, filled]
    );

    let over = call("over");
    let detail = &over["error"]["detail"];
    let (envelope, report) = (
        detail["envelope_bytes"].as_u64().unwrap(),
        detail["report_bytes"].as_u64().unwrap(),
    );
    assert_eq!(
        json!([over["error"]["kind"], detail["media_bytes"]]),
        json!(["mcp_result_too_large", 10_485_761])
    );
    assert_eq!(
        over["error"]["message"],
        format!(
            "result over a size limit: 10485761 media bytes (limit 10485760), {envelope} envelope bytes (limit 262144), {report} report bytes (limit 14680064)"
        )
    );
    // The report is its envelope, its media strings of 13,981,016
    // characters and its other members.
    assert_eq!(envelope, 262_144, "{detail}");
    assert!(
        report > 13_981_016 + envelope && report < 13_981_016 + envelope + 2_000,
        "{detail}"
    );

    assert_eq!(
        call("heavy")["result"]["content"],
        json!([omitted(7_500_001, 100, 100), text])
    );
    assert_eq!(
        call("wide")["result"]["content"],
        json!([text, omitted(1_000, 9_000, 100)])
    );
    assert_eq!(
        call("patches")["result"]["content"],
        json!([omitted(1_000, 6_000, 6_000), text])
    );
    let over = call("envelope_over");
    assert_eq!(
        json!([
            over["error"]["kind"],
            over["error"]["detail"]["media_bytes"],
            over["error"]["detail"]["envelope_bytes"]
        ]),
        json!(["mcp_result_too_large", 10_485_760, 262_145])
    );
    for tool in ["unpadded", "broken", "url_safe"] {
        assert_eq!(
            call(tool)["error"],
            json!({"kind": "mcp_invalid_result", "message": "content[0] holds media that is not valid base64"}),
            "{tool}"
        );
    }
    let blob = call("blob");
    assert_eq!(
        json!([
            blob["status"],
            blob["result"]["content"][0]["resource"]["blob"]
        ]),
        json!(["success", "AAAAAAAAAAAAAA=="])
    );
    assert_eq!(
        call("bare")["error"],
        json!({"kind": "mcp_invalid_result", "message": "result is not a valid CallToolResult: content is not an array"})
    );
    assert_eq!(calls.every_call(), 11);
    case.observe(format!("10,485,760 decoded media bytes with a 262,144-byte envelope delivered whole; 10,485,761 media bytes gave mcp_result_too_large with {envelope} envelope and {report} report bytes, and a 262,145-byte envelope beside 10,485,760 media bytes mcp_result_too_large; the 7,500,001-byte image, the 9,000x100 PNG and the 6,000x6,000 resource blob each became the omission text at their index, the text block unchanged; unpadded, line-broken and URL-safe data mcp_invalid_result; a resource blob delivered; no content mcp_invalid_result; one tools/call per call"));
    case.pass();
}

#[test]
fn a_resume_a_resent_call_and_a_changed_call_never_repeat_it_and_an_ended_connection_reports_nothing()
 {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a call held by the server through a stream resume, the same mcp.call resent and the task_id called with other arguments; then the answer",
    );
    let calls = Calls::start_ignoring_sigterm(
        &mut case,
        json!({
            "slow": {"result": {"content": [{"type": "text", "text": "done"}]}, "hold": "go"},
            "stuck": {"never": true},
        }),
    );
    let tasks = &calls.tasks;
    let issued = tasks.mcp_call("task_held", "slow", json!({"n": 1}));
    let pid = calls.process(1);
    calls.call_received(pid, 1);
    tasks.resume();
    tasks
        .engine
        .control("send", json!({"instance_id": INSTANCE, "resend": issued}));
    let conflict = tasks.answer_to(&tasks.mcp_call("task_held", "slow", json!({"n": 2})));
    assert_eq!(
        json!([conflict["phase"], conflict["error"]["code"]]),
        json!(["rejected", "task_conflict"])
    );
    tasks.handled(SESSION, BINDING);
    calls.driver.control(pid, json!({"release": "go"}));
    let terminal = tasks.terminal("task_held");
    assert_eq!(
        json!([terminal["request_message_id"], terminal["result"]["status"]]),
        json!([issued, "success"])
    );
    tasks.answers_settled();
    assert_eq!(calls.every_call(), 1);
    // Besides the conflict's rejection, which names the same task.
    let answering: Vec<Value> = tasks
        .reports("task_held")
        .into_iter()
        .filter(|report| report["request_message_id"] == issued.as_str())
        .collect();
    assert_eq!(answering.len(), 1, "{answering:#?}");
    case.observe("one tools/call and one success report through the resume and the resend; the changed call rejected task_conflict while the first ran");
    case.pass();

    let mut case = record.case("a call held by the server when the Engine connection ends");
    tasks.mcp_call("task_lost", "stuck", json!({}));
    let lost = calls.call_received(pid, 2);
    let seams = &tasks.machine().seams;
    let checked = seams.count("mcp.stop_checked");
    new_connection(&tasks.engine, tasks.machine());
    let notification = calls
        .driver
        .wait_for_received(pid, "notifications/cancelled", 1)[0]
        .clone();
    assert_eq!(
        notification["params"],
        json!({"requestId": lost["id"], "reason": "cancelled by Engine"})
    );
    seams.wait_for_count("mcp.stop_checked", checked + 1);
    seams.advance(seconds(2.0));
    wait_until("the binding connection's process gone", || {
        !group_alive(pid)
    });
    tasks.answers_settled();
    assert!(tasks.reports("task_lost").is_empty());
    assert!(alive(calls.process(0)), "the server connection's process");
    case.observe("notifications/cancelled with reason cancelled by Engine, the binding connection's process, which ignores SIGTERM, gone 2 seconds after its stop, no report on any connection, the server connection still running");
    case.pass();
}

#[test]
fn closing_one_binding_cancels_its_call_and_stops_its_connection_while_another_bindings_call_completes()
 {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "two bindings each with a call held by the server, whose processes ignore SIGTERM; binding A closed, a tool.cancel of A's call during the close, then B's call answered",
    );
    let calls = Calls::start_ignoring_sigterm(
        &mut case,
        json!({
            "wait_a": {"never": true},
            "wait_b": {"result": {"content": [{"type": "text", "text": "b"}]}, "hold": "b"},
        }),
    );
    let tasks = &calls.tasks;
    let prepared = tasks.prepare("sess_b", "bind_b", &tasks.project, None);
    tasks.wait_prepared(&prepared);
    tasks.mcp_call("task_a", "wait_a", json!({}));
    let pid_a = calls.process(1);
    calls.call_received(pid_a, 1);
    tasks.mcp_call_with(
        "sess_b",
        "bind_b",
        "task_b",
        json!({"server": "cua", "tool": "wait_b", "arguments": {}}),
    );
    let pid_b = calls.process(2);
    calls.call_received(pid_b, 1);
    let seams = &tasks.machine().seams;
    let checked = seams.count("mcp.stop_checked");

    tasks.engine.control(
        "close",
        json!({"instance_id": INSTANCE, "payload": {"session_id": SESSION, "binding_id": BINDING}}),
    );
    assert_eq!(ended(&tasks.terminal("task_a")), cancelled(true));
    seams.wait_for_count("mcp.stop_checked", checked + 1);
    let cancel = tasks.cancel("task_a", json!({}));
    assert_eq!(
        cancel_reply(&tasks.reply(&cancel)),
        json!(["cancel", true, true])
    );
    let closed = |tasks: &TaskMachine| {
        tasks
            .engine
            .received("session-status")
            .into_iter()
            .any(|received| {
                received["body"]["binding_id"] == BINDING && received["body"]["status"] == "closed"
            })
    };
    assert!(
        group_alive(pid_a) && !closed(tasks),
        "closed before A's process ended"
    );
    seams.advance(seconds(2.0));
    session_report(tasks, BINDING, "closed");
    assert!(
        !group_alive(pid_a),
        "binding A's process outlived its closed report"
    );
    calls.driver.control(pid_b, json!({"release": "b"}));
    assert_eq!(tasks.terminal("task_b")["result"]["status"], "success");
    assert!(alive(pid_b) && alive(calls.process(0)));
    case.observe("A's call ended cancelled with sent true; a tool.cancel of it during the close got cancelled true and was_running true; no closed report while A's process ran out the 2 seconds after its SIGTERM; after them the closed report, with A's process gone; B's call completed and B's and the server connection's processes kept running");
    case.pass();
}

#[test]
fn a_disable_an_enable_again_and_a_shutdown_during_a_call_cancel_it() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record
        .case("a call held by the server after writing its effect file; a disable; another call");
    let calls = Calls::start(&mut case, json!({"move": {"never": true, "effect": true}}));
    let tasks = &calls.tasks;
    tasks.mcp_call("task_running", "move", json!({"to": [1, 2]}));
    calls.call_received(calls.process(1), 1);
    let effects = calls.driver.effects();
    assert_eq!(effects.len(), 1);

    let run = mcp::disable(tasks.machine());
    case.command("tetral computer-use disable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(ended(&tasks.terminal("task_running")), cancelled(true));
    let disabled = wait_for_reports(&tasks.engine, 2)[1].clone();
    assert_eq!(
        json!([disabled["tools"], disabled["error"]]),
        json!([[], null])
    );
    let processes = calls.driver.processes().len();
    tasks.mcp_call("task_after", "move", json!({}));
    assert_eq!(
        rejected(&tasks.report("task_after", "rejected")),
        json!([
            "rejected",
            "mcp_server_unavailable",
            "server cua is not running"
        ])
    );
    assert_eq!(calls.every_call(), 1);
    assert_eq!(calls.driver.processes().len(), processes);
    assert_eq!(calls.driver.effects(), effects);
    case.observe("the running call ended cancelled with sent true; a cua report with tools [] and error null; the next call rejected mcp_server_unavailable with no tools/call and no new process; the first call's effect file stayed");
    case.pass();
    drop(calls);

    let mut case = record.case("a call held by the server; Computer Use enabled again");
    let calls = Calls::start(&mut case, json!({"move": {"never": true}}));
    let tasks = &calls.tasks;
    tasks.mcp_call("task_running", "move", json!({}));
    calls.call_received(calls.process(1), 1);
    let run = mcp::enable(tasks.machine(), calls.driver.driver());
    case.command(
        "tetral computer-use enable --app app_one --driver <test server>",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(ended(&tasks.terminal("task_running")), cancelled(true));
    case.observe("the running call ended cancelled with sent true");
    case.pass();
    drop(calls);

    let mut case =
        record.case("a call held by the server, whose processes ignore SIGTERM; POST /v1/shutdown");
    let mut calls = Calls::start_ignoring_sigterm(&mut case, json!({"move": {"never": true}}));
    let tasks = &calls.tasks;
    tasks.mcp_call("task_running", "move", json!({}));
    let pid = calls.process(1);
    let sent = calls.call_received(pid, 1);
    let seams = &tasks.machine().seams;
    let checked = seams.count("mcp.stop_checked");
    let accepted = request(&socket_path(&tasks.root), "POST", "/v1/shutdown").expect("an answer");
    assert_eq!(accepted.status, 202);
    let notification = calls
        .driver
        .wait_for_received(pid, "notifications/cancelled", 1)[0]
        .clone();
    assert_eq!(
        notification["params"],
        json!({"requestId": sent["id"], "reason": "cancelled by Engine"})
    );
    seams.wait_for_count("mcp.stop_checked", checked + 2);
    seams.advance(seconds(2.0));
    let _ = calls.tasks.exited();
    assert!(!group_alive(pid) && !group_alive(calls.process(0)));
    case.observe("notifications/cancelled with the call's requestId reached the binding connection's process; both processes were gone once the App Server exited, 2 seconds after the SIGTERM they ignored");
    case.pass();
}

#[test]
fn a_server_exit_ends_sent_calls_rejects_waiting_ones_and_is_reported_once() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a call held on binding A's connection and a call waiting for binding B's, whose initialize is never answered; A's process exits",
    );
    let calls_of = json!({"hang": {"never": true}});
    let calls = Calls::start(&mut case, calls_of.clone());
    let tasks = &calls.tasks;
    tasks.mcp_call("task_sent", "hang", json!({}));
    let pid_a = calls.process(1);
    calls.call_received(pid_a, 1);

    let mut unopened = scenario(calls_of);
    unopened["initialize"] = json!("never");
    calls.driver.set_scenario(unopened);
    let prepared = tasks.prepare("sess_w", "bind_w", &tasks.project, None);
    tasks.wait_prepared(&prepared);
    tasks.mcp_call_with(
        "sess_w",
        "bind_w",
        "task_waiting",
        json!({"server": "cua", "tool": "hang", "arguments": {}}),
    );
    let pid_w = calls.process(2);
    calls.driver.wait_for_received(pid_w, "initialize", 1);

    calls.driver.control(pid_a, json!({"exit": 3}));
    assert_eq!(
        ended(&tasks.terminal("task_sent")),
        json!(["error", {"kind": "mcp_server_exited", "message": "the MCP connection ended before the answer"}])
    );
    assert_eq!(
        rejected(&tasks.report("task_waiting", "rejected")),
        json!([
            "rejected",
            "mcp_server_unavailable",
            "server cua is not running"
        ])
    );
    let exited = wait_for_reports(&tasks.engine, 2)[1].clone();
    assert_eq!(
        json!([exited["tools"], exited["error"]["code"]]),
        json!([[], "mcp_server_exited"])
    );
    let pids = calls.driver.processes();
    for pid in &pids {
        wait_until("every process of the server gone", || !group_alive(*pid));
    }
    tasks.answers_settled();
    assert_eq!(mcp::reports(&tasks.engine).len(), 2);
    assert_eq!(pids.len(), 3);
    case.observe(format!(
        "the sent call ended mcp_server_exited, the waiting one was rejected mcp_server_unavailable, one tool-list report with tools [] and mcp_server_exited ({}), and every process of the server ended with none started again",
        exited["error"]["message"]
    ));
    case.pass();
}

#[test]
fn a_binding_connection_that_cannot_open_rejects_its_call_at_30_seconds_or_at_its_spawn() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "two calls of one binding waiting for its connection, whose initialize is never answered, at 29.999 and 30 seconds after its spawn; then the binding's next call",
    );
    let calls_of = json!({"click": {"result": {"content": []}}});
    let calls = Calls::start(&mut case, calls_of.clone());
    let tasks = &calls.tasks;
    let seams = &tasks.machine().seams;
    let mut unopened = scenario(calls_of.clone());
    unopened["initialize"] = json!("never");
    calls.driver.set_scenario(unopened);
    tasks.mcp_call("task_unopened", "click", json!({}));
    let pid = calls.process(1);
    calls.driver.wait_for_received(pid, "initialize", 1);
    tasks.mcp_call("task_also_waiting", "click", json!({}));
    tasks.handled(SESSION, BINDING);
    seams.advance_within(seconds(29.999), "mcp.start_checked");
    assert!(tasks.reports("task_unopened").is_empty());
    assert!(tasks.reports("task_also_waiting").is_empty());
    seams.advance(seconds(0.001));
    for task in ["task_unopened", "task_also_waiting"] {
        assert_eq!(
            rejected(&tasks.report(task, "rejected")),
            json!([
                "rejected",
                "mcp_server_unavailable",
                "could not open a connection to server cua: no initialize result within 30 seconds"
            ]),
            "{task}"
        );
    }
    wait_until("the unopened connection's process gone", || {
        !group_alive(pid)
    });
    calls.driver.set_scenario(scenario(calls_of));
    tasks.mcp_call("task_next", "click", json!({}));
    assert_eq!(tasks.terminal("task_next")["result"]["status"], "success");
    assert_eq!(calls.driver.processes().len(), 3);
    assert_eq!(calls.every_call(), 1);
    case.observe("two calls of the binding waited for its one opening; no report at 29.999 seconds; at 30 seconds both rejected mcp_server_unavailable with the opening's reason and the process gone; the next call opened a new connection and succeeded");
    case.pass();
    drop(calls);

    let mut case = record.case("a binding whose directory was removed before its first call");
    let calls = Calls::start(&mut case, json!({"click": {"result": {"content": []}}}));
    let tasks = &calls.tasks;
    std::fs::remove_dir_all(&tasks.project).unwrap();
    tasks.mcp_call("task_removed", "click", json!({}));
    let report = tasks.report("task_removed", "rejected");
    let message = report["error"]["message"].as_str().unwrap();
    assert_eq!(report["error"]["code"], "mcp_server_unavailable");
    assert!(
        message.starts_with("could not open a connection to server cua: ")
            && message.ends_with("(os error 2)"),
        "{message}"
    );
    assert_eq!(calls.driver.processes().len(), 1);
    case.observe(format!(
        "rejected mcp_server_unavailable: {message}; no process started"
    ));
    case.pass();
}

#[test]
fn a_stdout_line_over_its_bound_closes_only_the_binding_connection_and_the_next_call_opens_another()
{
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a 14,680,064-byte line during a held call; a 14,680,065-byte line during another; then a third call",
    );
    let answer = json!({"content": [{"type": "text", "text": "ok"}]});
    let calls = Calls::start(
        &mut case,
        json!({
            "first": {"result": answer, "hold": "first"},
            "second": {"result": answer, "hold": "second"},
            "third": {"result": answer},
        }),
    );
    let tasks = &calls.tasks;
    tasks.mcp_call("task_first", "first", json!({}));
    let pid = calls.process(1);
    calls.call_received(pid, 1);
    calls.driver.control(pid, json!({"line_bytes": 14_680_064}));
    calls.driver.control(pid, json!({"release": "first"}));
    assert_eq!(tasks.terminal("task_first")["result"]["status"], "success");

    tasks.mcp_call("task_second", "second", json!({}));
    calls.call_received(pid, 2);
    calls.driver.control(pid, json!({"line_bytes": 14_680_065}));
    assert_eq!(
        ended(&tasks.terminal("task_second")),
        json!(["error", {"kind": "mcp_server_exited", "message": "the MCP connection ended before the answer"}])
    );
    wait_until("the closed connection's process gone", || !group_alive(pid));
    tasks.mcp_call("task_third", "third", json!({}));
    assert_eq!(tasks.terminal("task_third")["result"]["status"], "success");
    assert_eq!(calls.driver.processes().len(), 3);
    tasks.answers_settled();
    assert_eq!(mcp::reports(&tasks.engine).len(), 1);
    case.observe("the 14,680,064-byte line was read and the call completed; the 14,680,065-byte line closed the binding connection, its call mcp_server_exited and its process gone; the third call opened a new connection and succeeded; no further tool-list report");
    case.pass();
}
