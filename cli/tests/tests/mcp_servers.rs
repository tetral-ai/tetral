//! Local MCP servers across processes: `tetral` against the reference
//! control server, with the test MCP server as Computer Use's driver, under
//! the controlled clock: the tool-list reports, their listing, start and
//! limits, the ready capability, the server's messages and its processes.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::{Machine, answer_fault, seconds, send, wait_for_received};
use tetral_tests::mcp::{
    self, Setup, TestDriver, alive, enable, group_alive, names, page, record_before_start, reports,
    tool, wait_for_reports,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;
use tetral_tests::tasks::HEARTBEAT_INTERVAL;

const TEST_SERVER: &str = env!("CARGO_BIN_EXE_mcp-test-server");

/// An App Server online with `scenario` as its test driver's, Computer Use
/// not yet recorded; `prepare` runs on the configured root before it starts.
fn online_with(
    case: &mut Case,
    scenario: Value,
    prepare: impl FnOnce(&Path, &TestDriver),
) -> Setup {
    mcp::online_with(case, Path::new(TEST_SERVER), scenario, prepare)
}

/// [`online_with`] and then `tetral computer-use enable` on the running
/// App Server.
fn enabled(case: &mut Case, scenario: Value) -> Setup {
    let setup = online_with(case, scenario, |_, _| {});
    let run = enable(&setup.machine, setup.driver.driver());
    case.command(
        "tetral computer-use enable --app app_one --driver <test server>",
        &run.exit(),
    );
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    assert_eq!(run.object(), json!({}));
    setup
}

fn test_server_info() -> Value {
    json!({ "name": "mcp-test-server", "title": null, "version": "1.0.0" })
}

/// Ends the current connection with a connection-ending `disconnect` and
/// waits for the next one to be online.
fn new_connection(engine: &ReferenceServer, machine: &Machine) {
    let connection = machine.status()["connection_id"].clone();
    send(
        engine,
        json!({
            "protocol_version": 1,
            "connection_id": connection,
            "message_id": format!("msg_end_{}", connection.as_str().unwrap()),
            "type": "disconnect",
            "payload": { "scope": "connection", "retryable": true, "code": "stale_connection" },
        }),
    );
    machine.wait_for("disconnected");
    engine.advance(Duration::from_secs(46));
    machine.seams.advance(Duration::from_secs(2));
    machine.wait_for("online");
}

#[test]
fn tool_list_reports_follow_list_changes_an_exit_and_new_connections() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a list, a list change, a stream-only disconnect, the server's exit and a new connection",
    );
    let Setup {
        engine,
        machine,
        driver,
        directory: _directory,
    } = enabled(
        &mut case,
        json!({ "pages": [page(None, vec![tool("a"), tool("b")], None)] }),
    );

    let first = &wait_for_reports(&engine, 1)[0];
    assert_eq!(first["seq"], 1);
    assert_eq!(names(first), ["a", "b"]);
    assert_eq!(first["server_info"], test_server_info());
    assert_eq!(first["error"], Value::Null);
    case.observe("the first report has seq 1 and the full list");

    let pid = driver.wait_for_processes(1)[0];
    driver.control(pid, json!({ "pages": [page(None, vec![tool("c")], None)] }));
    driver.control(
        pid,
        json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
    );
    let second = &wait_for_reports(&engine, 2)[1];
    assert_eq!(second["seq"], 2);
    assert_eq!(names(second), ["c"]);
    case.observe("after notifications/tools/list_changed, one report with seq 2 and the new list");

    let connection = machine.status()["connection_id"].clone();
    let heartbeats = engine.received("heartbeat").len();
    send(
        &engine,
        json!({
            "protocol_version": 1,
            "connection_id": connection,
            "message_id": "msg_stream_disconnect",
            "type": "disconnect",
            "payload": { "scope": "stream" },
        }),
    );
    wait_until("the resumed stream's heartbeat", || {
        engine.received("heartbeat").len() > heartbeats
    });
    assert_eq!(machine.status()["connection_id"], connection);
    assert_eq!(reports(&engine).len(), 2);
    case.observe("a stream-only disconnect resumes the connection and reports nothing");

    driver.control(pid, json!({ "exit": 3 }));
    let third = &wait_for_reports(&engine, 3)[2];
    assert_eq!(third["seq"], 3);
    assert_eq!(third["tools"], json!([]));
    assert_eq!(third["server_info"], Value::Null);
    assert_eq!(
        third["error"],
        json!({ "code": "mcp_server_exited", "message": "server process exited: exit code 3" })
    );
    assert!(!group_alive(pid));
    case.observe("after the server's exit, one report with seq 3 and mcp_server_exited");

    new_connection(&engine, &machine);
    let fourth = &wait_for_reports(&engine, 4)[3];
    assert_ne!(fourth["connection_id"], connection);
    assert_eq!(fourth["seq"], 1);
    assert_eq!(fourth["error"], third["error"]);
    assert_eq!(driver.processes(), [pid]);
    case.observe(
        "the new connection's first report has seq 1 and the exit's error; no new driver process started",
    );
    case.pass();
}

/// The first report of a server whose `tools/list` answers are `pages`.
fn first_report(case: &mut Case, pages: Vec<Value>) -> (Value, Setup) {
    let setup = enabled(case, json!({ "pages": pages }));
    let report = wait_for_reports(&setup.engine, 1)[0].clone();
    (report, setup)
}

fn list_failed(message: &str) -> Value {
    json!({ "code": "mcp_list_failed", "message": message })
}

/// `count` tools named `t0`, `t1`, ...
fn tools(count: usize) -> Vec<Value> {
    (0..count).map(|index| tool(&format!("t{index}"))).collect()
}

/// One tool whose report, the first on `connection_id`, is exactly `bytes`
/// bytes of compact JSON.
fn tool_of_report_bytes(connection_id: &str, bytes: usize) -> Value {
    let report = |tool: &Value| {
        json!({
            "protocol_version": 1, "connection_id": connection_id, "message_id": "r1",
            "server": "cua", "seq": 1, "server_info": test_server_info(), "tools": [tool],
            "error": null,
        })
    };
    let mut big = json!({ "name": "big", "description": "", "inputSchema": { "type": "object" } });
    let base = serde_json::to_vec(&report(&big)).unwrap().len();
    big["description"] = json!("x".repeat(bytes - base));
    big
}

/// The size of each `mcp-tools` body the reference control server read.
fn body_sizes(engine: &ReferenceServer) -> Vec<u64> {
    engine
        .requests("mcp-tools")
        .iter()
        .map(|request| request["body_bytes"].as_u64().unwrap())
        .collect()
}

#[test]
fn a_listing_reports_every_page_and_fails_at_each_bound() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();

    let mut case = record.case("three pages");
    let (report, _setup) = first_report(
        &mut case,
        vec![
            page(None, vec![tool("a"), tool("b")], Some("p1")),
            page(Some("p1"), vec![tool("c")], Some("p2")),
            page(Some("p2"), vec![tool("d")], None),
        ],
    );
    assert_eq!(names(&report), ["a", "b", "c", "d"]);
    assert_eq!(report["error"], Value::Null);
    case.observe("one report with every tool in page order");
    case.pass();

    let cursor = |bytes: usize| "c".repeat(bytes);
    let failing: Vec<(&str, Vec<Value>, &str)> = vec![
        (
            "a repeated cursor",
            vec![
                page(None, vec![tool("a")], Some("p1")),
                page(Some("p1"), vec![tool("b")], Some("p1")),
            ],
            "nextCursor repeated",
        ),
        (
            "2,049 tools",
            vec![page(None, tools(2_049), None)],
            "more than 2048 tools",
        ),
        (
            "two tools with one name on different pages",
            vec![
                page(None, vec![tool("x")], Some("p1")),
                page(Some("p1"), vec![tool("x")], None),
            ],
            "two tools named x",
        ),
        (
            "a 65,537-byte cursor",
            vec![page(None, vec![tool("a")], Some(&cursor(65_537)))],
            "nextCursor over 65536 bytes",
        ),
        (
            "a tool without inputSchema",
            vec![json!({ "cursor": null, "result": { "tools": [{ "name": "a" }] } })],
            "tools/list answer is not a valid ListToolsResult: tools[0] has no object inputSchema",
        ),
    ];
    for (row, pages, message) in failing {
        let mut case = record.case(row);
        let (report, setup) = first_report(&mut case, pages);
        assert_eq!(report["error"], list_failed(message), "{row}");
        assert_eq!(report["tools"], json!([]), "{row}");
        assert_eq!(report["server_info"], Value::Null, "{row}");
        assert!(
            body_sizes(&setup.engine)
                .iter()
                .all(|bytes| *bytes <= 4_194_304)
        );
        case.observe(format!("tools [] with mcp_list_failed: {message}"));
        case.pass();
    }

    let mut case = record.case("a 100th page carrying nextCursor");
    let mut pages: Vec<Value> = (0..100)
        .map(|index| {
            let cursor = (index > 0).then(|| format!("p{index}"));
            page(
                cursor.as_deref(),
                vec![tool(&format!("t{index}"))],
                Some(&format!("p{}", index + 1)),
            )
        })
        .collect();
    pages.push(page(Some("p100"), vec![tool("t100")], None));
    let (report, setup) = first_report(&mut case, pages);
    assert_eq!(
        report["error"],
        list_failed("tools/list needs more than 100 pages")
    );
    let pid = setup.driver.processes()[0];
    assert_eq!(setup.driver.received(pid, "tools/list").len(), 100);
    case.observe("mcp_list_failed after 100 tools/list requests, no 101st");
    case.pass();

    let accepted: Vec<(&str, Vec<Value>, usize)> = vec![
        ("2,048 tools", vec![page(None, tools(2_048), None)], 2_048),
        (
            "a 65,536-byte cursor",
            vec![
                page(None, vec![tool("a")], Some(&cursor(65_536))),
                page(Some(&cursor(65_536)), vec![tool("b")], None),
            ],
            2,
        ),
    ];
    for (row, pages, count) in accepted {
        let mut case = record.case(row);
        let (report, _setup) = first_report(&mut case, pages);
        assert_eq!(report["error"], Value::Null, "{row}");
        assert_eq!(report["tools"].as_array().unwrap().len(), count, "{row}");
        case.observe(format!("accepted with {count} tools"));
        case.pass();
    }

    let mut case =
        record.case("kept-tools array one byte over its bound, with a valid next page available");
    let mut kept = vec![tool("large")];
    kept[0]["description"] = json!("");
    let empty = serde_json::to_vec(&kept).unwrap().len();
    kept[0]["description"] = json!("x".repeat(4_194_305 - empty));
    assert_eq!(serde_json::to_vec(&kept).unwrap().len(), 4_194_305);
    let (report, setup) = first_report(
        &mut case,
        vec![
            page(None, kept, Some("must-not-request")),
            page(Some("must-not-request"), vec![tool("tiny")], None),
        ],
    );
    assert_eq!(report["error"]["code"], "mcp_list_failed");
    assert_eq!(report["tools"], json!([]));
    let pid = setup.driver.wait_for_processes(1)[0];
    let listed = setup.driver.received(pid, "tools/list");
    assert_eq!(
        listed.len(),
        1,
        "the local bound must stop before requesting the valid next page: {listed:?}"
    );
    assert!(
        listed
            .iter()
            .all(|request| request["params"]["cursor"] != "must-not-request")
    );
    case.observe("4,194,305 independently serialized kept-tool bytes produced mcp_list_failed with no partial tools after exactly one tools/list; the available next cursor was never requested");
    case.pass();

    for (bytes, accepted) in [(4_194_304, true), (4_194_305, false)] {
        let mut case = record.case(&format!("tools whose report is {bytes} bytes"));
        let setup = enabled(&mut case, json!({ "pages": [] }));
        let connection_id = setup.machine.status()["connection_id"].clone();
        let big = tool_of_report_bytes(connection_id.as_str().unwrap(), bytes);
        let pid = setup.driver.wait_for_processes(1)[0];
        setup
            .driver
            .control(pid, json!({ "pages": [page(None, vec![big], None)] }));
        setup.driver.control(
            pid,
            json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
        );
        // The first listing found no page; the second lists the tool, and a
        // refused report's failure goes with the same `seq`.
        let report = wait_for_reports(&setup.engine, 2)[1].clone();
        assert_eq!(report["seq"], 2, "{report:#}");
        let sizes = body_sizes(&setup.engine);
        if accepted {
            assert_eq!(names(&report), ["big"]);
            assert_eq!(sizes.last(), Some(&4_194_304));
        } else {
            assert_eq!(
                report["error"],
                list_failed("tool list report over 4194304 bytes")
            );
        }
        assert!(sizes.iter().all(|size| *size <= 4_194_304), "{sizes:?}");
        case.observe(format!(
            "accepted: {accepted}; body sizes read by the reference control server {sizes:?}"
        ));
        case.pass();
    }

    let mut case = record.case(
        "a first page answered 10 seconds after its request and a second page held past the listing's 30 seconds",
    );
    let mut first = page(None, vec![tool("a")], Some("p1"));
    first["hold"] = json!("first");
    let setup = enabled(
        &mut case,
        json!({ "pages": [
            first,
            { "cursor": "p1", "result": { "tools": [tool("b")] }, "hold": "second" },
        ] }),
    );
    let pid = setup.driver.wait_for_processes(1)[0];
    let seams = &setup.machine.seams;
    setup.driver.wait_for_received(pid, "tools/list", 1);
    seams.advance_within(seconds(10.0), "mcp.listing_checked");
    setup.driver.control(pid, json!({ "release": "first" }));
    setup.driver.wait_for_received(pid, "tools/list", 2);
    seams.advance_within(seconds(19.999), "mcp.listing_checked");
    assert!(reports(&setup.engine).is_empty());
    case.observe("no report at 29.999 seconds after the first tools/list, 19.999 after the second");
    seams.advance(seconds(0.001));
    let report = wait_for_reports(&setup.engine, 1)[0].clone();
    assert_eq!(
        report["error"],
        list_failed("tools/list did not finish within 30 seconds")
    );
    case.observe("mcp_list_failed at 30 seconds");
    case.pass();
}

/// How fast the reference control server reads a throttled tool-list
/// report: 4,194,304 bytes in 60 seconds of its clock.
const THROTTLE_BYTES_PER_SECOND: u64 = 69_906;

/// Lists `tool` on the running test server `pid`.
fn list_changed_to(driver: &TestDriver, pid: u32, tool: Value) {
    driver.control(pid, json!({ "pages": [page(None, vec![tool], None)] }));
    driver.control(
        pid,
        json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
    );
}

#[test]
fn a_large_report_completes_on_progress_and_a_stalled_one_is_sent_again() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a 4,194,304-byte report read in 60 seconds while both clocks move a second at a time, \
         then one whose reading stalls",
    );
    let setup = enabled(&mut case, json!({ "pages": [] }));
    let seams = &setup.machine.seams;
    let engine = &setup.engine;
    // The first listing finds no page and reports a failure.
    wait_for_reports(engine, 1);
    seams.wait_for_count("outbox.answered", 1);
    let connection_id = setup.machine.status()["connection_id"].clone();
    let pid = setup.driver.wait_for_processes(1)[0];
    let size = 4_194_304;
    let big = tool_of_report_bytes(connection_id.as_str().unwrap(), size);

    answer_fault(
        engine,
        json!({
            "endpoint": "mcp-tools", "action": "throttle",
            "bytes_per_second": THROTTLE_BYTES_PER_SECOND,
        }),
    );
    let heartbeats = engine.received("heartbeat").len();
    let answered = seams.count("outbox.answered");
    list_changed_to(&setup.driver, pid, big);
    let read = |index: usize| engine.state()["throttled"][index]["read"].as_u64();
    wait_until("the throttled read", || read(0).is_some());
    let mut elapsed = Duration::ZERO;
    let mut read_at_heartbeats = Vec::new();
    while reports(engine).len() < 2 {
        let grants = seams.count("connection.granted");
        engine.advance(Duration::from_secs(1));
        elapsed += Duration::from_secs(1);
        let allowed = elapsed.as_secs() * THROTTLE_BYTES_PER_SECOND;
        wait_until("the throttled read of the second", || {
            read(0) == Some(allowed) || reports(engine).len() == 2
        });
        if elapsed
            .as_secs()
            .is_multiple_of(HEARTBEAT_INTERVAL.as_secs())
        {
            wait_for_received(
                engine,
                "heartbeat",
                heartbeats + read_at_heartbeats.len() + 1,
            );
            seams.wait_for_count("connection.granted", grants + 1);
            read_at_heartbeats.push(read(0));
        }
        seams.advance(Duration::from_secs(1));
        // The upload reads its acknowledged bytes from its socket apart from
        // the clock, so the clock moves on only once it has read them at
        // this time.
        let checked = seams.count("outbox.upload_checked");
        wait_until("the upload's reading of its socket", || {
            seams.count("outbox.upload_checked") >= checked + 2
                || seams.count("outbox.answered") > answered
        });
        assert!(
            seams.count("outbox.answered") == answered || reports(engine).len() == 2,
            "the throttled upload's attempt ended unaccepted after {elapsed:?}"
        );
    }
    let report = reports(engine)[1].clone();
    assert_eq!(names(&report), ["big"]);
    assert_eq!(elapsed, Duration::from_secs(60));
    assert_eq!(body_sizes(engine), [body_sizes(engine)[0], size as u64]);
    // The heartbeats at 15, 30 and 45 seconds came while the body was read.
    assert_eq!(read_at_heartbeats.len(), 4);
    assert!(
        read_at_heartbeats[..3]
            .iter()
            .all(|read| read.is_some_and(|read| read < size as u64)),
        "{read_at_heartbeats:?}"
    );
    case.observe(format!(
        "the {size}-byte report accepted on its one attempt after {elapsed:?} of both clocks; \
         the heartbeats at 15, 30, 45 and 60 seconds acknowledged with {read_at_heartbeats:?} \
         bytes read"
    ));

    let mut stalled = tool_of_report_bytes(connection_id.as_str().unwrap(), size);
    stalled["name"] = json!("bog");
    answer_fault(
        engine,
        json!({
            "endpoint": "mcp-tools", "action": "throttle",
            "bytes_per_second": THROTTLE_BYTES_PER_SECOND,
        }),
    );
    let answered = seams.count("outbox.answered");
    list_changed_to(&setup.driver, pid, stalled);
    wait_until("the stalled read", || read(1).is_some());
    // The reference control server's clock stays still, so it reads nothing
    // and the upload stops progressing once the socket's buffers are full,
    // well within 20 of its readings.
    seams.wait_for_more("outbox.upload_checked", 20);
    seams.advance(Duration::from_secs(10));
    seams.wait_for_count("outbox.answered", answered + 1);
    assert_eq!(reports(engine).len(), 2);
    seams.advance(seconds(1.5));
    let third = wait_for_reports(engine, 3)[2].clone();
    assert_eq!(read(1), Some(0), "the stalled request was read");
    let requests = engine.requests("mcp-tools");
    let retry = requests.last().unwrap();
    assert_eq!(requests.len(), 3, "{requests:?}");
    assert_eq!(retry["message_id"], "r3");
    assert_eq!(retry["body_bytes"], size);
    assert_eq!(third["seq"], 3);
    assert_eq!(names(&third), ["bog"]);
    assert_eq!(reports(engine).len(), 3);
    case.observe(format!(
        "the stalled attempt, of which the reference control server read nothing, failed after \
         10 seconds without progress; the retry was the report r3 with seq 3 and the stalled \
         list, {size} bytes, accepted once"
    ));
    case.pass();
}

#[test]
fn list_changes_during_a_listing_give_exactly_one_more() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record
        .case("five notifications during a listing held 10 seconds, then one more notification");
    let setup = enabled(
        &mut case,
        json!({ "pages": [{ "cursor": null, "result": { "tools": [tool("first")] }, "hold": "listing" }] }),
    );
    let pid = setup.driver.wait_for_processes(1)[0];
    setup.driver.wait_for_received(pid, "tools/list", 1);
    for _ in 0..5 {
        setup.driver.control(
            pid,
            json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
        );
    }
    setup.machine.seams.advance(Duration::from_secs(10));
    setup.driver.control(
        pid,
        json!({ "pages": [page(None, vec![tool("final")], None)] }),
    );
    setup.driver.control(pid, json!({ "release": "listing" }));
    setup.machine.seams.wait_for_count("mcp.listed", 2);
    let listed = wait_for_reports(&setup.engine, 2);
    assert_eq!(names(&listed[0]), ["first"]);
    assert_eq!(names(&listed[1]), ["final"]);
    assert_eq!(listed[1]["seq"], 2);
    assert_eq!(setup.driver.received(pid, "tools/list").len(), 2);
    case.observe("exactly 2 listings; the last report is the final list");

    setup.driver.control(
        pid,
        json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
    );
    let requests = setup.driver.wait_for_received(pid, "tools/list", 3);
    assert_eq!(requests[2]["params"].get("cursor"), None);
    let third = wait_for_reports(&setup.engine, 3)[2].clone();
    assert_eq!(third["seq"], 3);
    case.observe("one notification gives one tools/list without cursor and a report with seq 3");
    case.pass();
}

#[test]
fn a_start_is_bounded_and_server_requests_get_only_ping_answered() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();

    let mut case = record.case(
        "an App Server started with Computer Use recorded enabled and a server that never answers initialize",
    );
    let setup = online_with(
        &mut case,
        json!({ "initialize": "never" }),
        |root, driver| {
            record_before_start(root, &["enable", "--driver", driver.driver()]);
        },
    );
    let pid = setup.driver.wait_for_processes(1)[0];
    setup.driver.wait_for_received(pid, "initialize", 1);
    setup
        .machine
        .seams
        .advance_within(seconds(29.999), "mcp.start_checked");
    assert!(reports(&setup.engine).is_empty());
    assert!(alive(pid));
    setup.machine.seams.advance(seconds(0.001));
    let report = wait_for_reports(&setup.engine, 1)[0].clone();
    assert_eq!(
        report["error"],
        json!({ "code": "mcp_start_failed", "message": "no initialize result within 30 seconds" })
    );
    wait_until("the test server's group gone", || !group_alive(pid));
    case.observe(
        "no report at 29.999 seconds; mcp_start_failed at 30 seconds and the process group gone",
    );
    case.pass();

    let mut case =
        record.case("a server that sends ping, roots/list, sampling and elicitation requests");
    let setup = enabled(
        &mut case,
        json!({ "pages": [page(None, vec![tool("a")], None)] }),
    );
    wait_for_reports(&setup.engine, 1);
    let pid = setup.driver.processes()[0];
    let requests = [
        ("s1", json!({ "method": "ping" })),
        ("s2", json!({ "method": "roots/list" })),
        (
            "s3",
            json!({ "method": "sampling/createMessage", "params": {
                "messages": [{ "role": "user", "content": { "type": "text", "text": "hi" } }],
                "maxTokens": 10,
            } }),
        ),
        (
            "s4",
            json!({ "method": "elicitation/create", "params": {
                "message": "a value", "requestedSchema": { "type": "object", "properties": {} },
            } }),
        ),
    ];
    for (id, request) in &requests {
        let mut message = request.clone();
        message["jsonrpc"] = json!("2.0");
        message["id"] = json!(id);
        setup.driver.control(pid, json!({ "send": message }));
    }
    let mut answers = Vec::new();
    wait_until("four answers", || {
        answers = setup
            .driver
            .messages(pid)
            .into_iter()
            .filter(|message| message["id"].as_str().is_some_and(|id| id.starts_with('s')))
            .collect();
        answers.len() == 4
    });
    for answer in &answers {
        if answer["id"] == "s1" {
            assert_eq!(answer["result"], json!({}), "{answer}");
        } else {
            assert_eq!(answer["error"]["code"], -32601, "{answer}");
        }
    }
    assert_eq!(reports(&setup.engine).len(), 1);
    case.observe("ping got {}; roots/list, sampling/createMessage and elicitation/create got -32601; no new report");
    case.pass();
}

#[test]
fn initialize_carries_the_fixed_client_and_any_answered_version_starts() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();

    let mut case = record.case("the record of a started server");
    let setup = enabled(
        &mut case,
        json!({ "pages": [page(None, vec![tool("a")], None)] }),
    );
    let pid = setup.driver.wait_for_processes(1)[0];
    wait_for_reports(&setup.engine, 1);
    let messages = setup.driver.messages(pid);
    assert_eq!(messages[0]["method"], "initialize");
    assert_eq!(
        messages[0]["params"],
        json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": { "name": "tetral-cli", "version": env!("CARGO_PKG_VERSION") },
        })
    );
    assert_eq!(messages[1]["method"], "notifications/initialized");
    case.observe("initialize with 2025-06-18, tetral-cli and the workspace version, capabilities {}, then notifications/initialized");
    case.pass();

    let mut case = record.case("a server answering protocolVersion 2025-03-26");
    let (report, _setup) = {
        let setup = enabled(
            &mut case,
            json!({
                "initialize": { "protocol_version": "2025-03-26" },
                "pages": [page(None, vec![tool("a")], None)],
            }),
        );
        (wait_for_reports(&setup.engine, 1)[0].clone(), setup)
    };
    assert_eq!(names(&report), ["a"]);
    case.observe("it starts and reports its tools");
    case.pass();

    let mut case = record.case("a --driver naming no file");
    let setup = online_with(&mut case, json!({}), |_, _| {});
    let run = enable(&setup.machine, "/nonexistent/driver");
    case.command(
        "tetral computer-use enable --app app_one --driver /nonexistent/driver",
        &run.exit(),
    );
    let message = "could not start /nonexistent/driver: No such file or directory (os error 2)";
    assert_eq!(run.status.code(), Some(1));
    assert_eq!(
        run.object(),
        json!({ "error": { "code": "driver_failed", "message": message } })
    );
    let report = wait_for_reports(&setup.engine, 1)[0].clone();
    assert_eq!(
        report["error"],
        json!({ "code": "mcp_start_failed", "message": message })
    );
    case.observe("driver_failed and an mcp_start_failed report with the spawn error");
    case.pass();
}

#[test]
fn a_report_keeps_only_the_six_tool_members_as_received() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case =
        record.case("a tool with an extra member, _meta, icons and an unknown annotation");
    let input_schema = json!({
        "type": "object",
        "properties": { "x": { "type": "string", "x-order": 2.5 } },
        "additionalProperties": false,
    });
    let annotations = json!({ "readOnlyHint": true, "vendorHint": { "a": [1, null] } });
    let given = json!({
        "name": "f", "title": "F", "description": "does f",
        "inputSchema": input_schema, "outputSchema": { "type": "object" },
        "annotations": annotations, "_meta": { "k": 1 }, "icons": [{ "src": "i.png" }],
        "extra": true,
    });
    let (report, _setup) = first_report(
        &mut case,
        vec![json!({ "cursor": null, "result": { "tools": [given] } })],
    );
    assert_eq!(
        report["tools"],
        json!([{
            "name": "f", "title": "F", "description": "does f", "inputSchema": input_schema,
            "outputSchema": { "type": "object" }, "annotations": annotations,
        }])
    );
    case.observe("the tool holds exactly name, title, description, inputSchema, outputSchema and annotations, each as sent");
    case.pass();
}

#[test]
fn only_the_newest_list_waits_behind_a_report_in_flight() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("three list changes while the first report's ACK is withheld");
    let setup = online_with(
        &mut case,
        json!({ "pages": [page(None, vec![tool("v0")], None)] }),
        |_, _| {},
    );
    setup.engine.control(
        "answer",
        json!({ "endpoint": "mcp-tools", "action": "withhold" }),
    );
    let run = enable(&setup.machine, setup.driver.driver());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    wait_for_reports(&setup.engine, 1);
    let pid = setup.driver.processes()[0];
    for version in 1..=3 {
        setup.driver.control(
            pid,
            json!({ "pages": [page(None, vec![tool(&format!("v{version}"))], None)] }),
        );
        setup.driver.control(
            pid,
            json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
        );
        setup
            .machine
            .seams
            .wait_for_count("mcp.listed", version + 1);
    }
    assert_eq!(reports(&setup.engine).len(), 1);
    setup
        .engine
        .control("answer", json!({ "action": "release" }));
    let second = wait_for_reports(&setup.engine, 2)[1].clone();
    assert_eq!(second["seq"], 2);
    assert_eq!(names(&second), ["v3"]);
    // The second report's ACK leaves nothing queued.
    wait_until("the second report's answer", || {
        setup
            .engine
            .requests("mcp-tools")
            .iter()
            .filter(|request| request["status"] == 200)
            .count()
            == 2
    });
    assert_eq!(setup.engine.requests("mcp-tools").len(), 2);
    case.observe("after the ACK, exactly one report with seq 2 and the final list");
    case.pass();
}

#[test]
fn every_ready_declares_local_mcp_with_cancel() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("an App Server started with Computer Use recorded disabled");
    let setup = online_with(&mut case, json!({}), |root, _| {
        record_before_start(root, &["disable"]);
    });
    let ready = setup.engine.received("ready")[0]["body"].clone();
    assert_eq!(
        ready["capabilities"]["local_mcp"],
        json!({ "schema_version": 1 })
    );
    let controls = ready["capabilities"]["tool_execution"]["controls"]
        .as_array()
        .unwrap();
    assert!(controls.contains(&json!("cancel")), "{controls:?}");
    let report = wait_for_reports(&setup.engine, 1)[0].clone();
    assert_eq!(
        (
            &report["seq"],
            &report["server_info"],
            &report["tools"],
            &report["error"]
        ),
        (&json!(1), &Value::Null, &json!([]), &Value::Null)
    );
    assert!(setup.driver.processes().is_empty());
    case.observe("the accepted ready has local_mcp {schema_version: 1} and cancel; cua is reported disabled with seq 1 and no driver runs");
    case.pass();
}

#[test]
fn a_server_runs_in_the_root_directory_and_one_overlong_line_ends_it() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record
        .case("the server connection's process and stdout lines at and over 14,680,064 bytes");
    let setup = enabled(
        &mut case,
        json!({ "pages": [page(None, vec![tool("a")], None)] }),
    );
    wait_for_reports(&setup.engine, 1);
    let pid = setup.driver.processes()[0];
    assert_eq!(
        setup.driver.started(pid),
        json!({ "pid": pid, "cwd": "/", "tetral_home": false })
    );
    case.observe("the server connection's process runs in / without TETRAL_HOME");

    setup
        .driver
        .control(pid, json!({ "line_bytes": 14_680_064 }));
    setup.driver.control(
        pid,
        json!({ "send": { "jsonrpc": "2.0", "method": "notifications/tools/list_changed" } }),
    );
    let second = wait_for_reports(&setup.engine, 2)[1].clone();
    assert_eq!(second["error"], Value::Null);
    case.observe("a 14,680,064-byte line is read and the connection lists again");

    setup
        .driver
        .control(pid, json!({ "line_bytes": 14_680_065 }));
    let third = wait_for_reports(&setup.engine, 3)[2].clone();
    assert_eq!(
        third["error"],
        json!({ "code": "mcp_server_exited", "message": "server output message over 14680064 bytes" })
    );
    wait_until("the test server's group gone", || !group_alive(pid));
    case.observe("a 14,680,065-byte line closes the connection, stops the process and reports mcp_server_exited");
    case.pass();
}
