//! Computer Use with the pinned cua-driver release on Linux: `tetral`
//! against the reference control server with the release as `--driver`.
//! The tests are ignored by default. Run them in `cli/` with
//! `TETRAL_TEST_CUA_DRIVER` set to the release's `cua-driver` executable:
//! `cargo test --test cua_driver -- --ignored`. The desktop tests need a
//! desktop session reachable through `DISPLAY` (an X server such as Xvfb)
//! and `xmessage` as the target application. Without the variable every
//! test fails. The App Server gets the test's desktop variables and any
//! `CUA_DRIVER_*` variable, as a launcher would pass them to
//! `tetral server run`.
#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::INSTANCE;
use tetral_tests::mcp::{self, wait_for_reports};
use tetral_tests::record::Case;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, SESSION, TaskMachine};

const DRIVER_VARIABLE: &str = "TETRAL_TEST_CUA_DRIVER";

/// The variables through which the driver reaches a desktop session.
const DESKTOP_VARIABLES: [&str; 6] = [
    "DISPLAY",
    "XAUTHORITY",
    "WAYLAND_DISPLAY",
    "XDG_RUNTIME_DIR",
    "XDG_SESSION_TYPE",
    "DBUS_SESSION_BUS_ADDRESS",
];

/// How long a direct listing waits for each answer.
const ANSWER_WAIT: Duration = Duration::from_secs(60);

/// The six members a reported tool may have.
const TOOL_MEMBERS: [&str; 6] = [
    "name",
    "title",
    "description",
    "inputSchema",
    "outputSchema",
    "annotations",
];

fn driver() -> String {
    std::env::var(DRIVER_VARIABLE)
        .unwrap_or_else(|_| panic!("{DRIVER_VARIABLE} names the cua-driver executable"))
}

/// The App Server's added environment: the test's desktop variables when
/// `desktop`, and its `CUA_DRIVER_*` variables.
fn environment(desktop: bool) -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(name, _)| {
            (desktop && DESKTOP_VARIABLES.contains(&name.as_str()))
                || name.starts_with("CUA_DRIVER_")
        })
        .collect()
}

fn borrowed(environment: &[(String, String)]) -> Vec<(&str, String)> {
    environment
        .iter()
        .map(|(name, value)| (name.as_str(), value.clone()))
        .collect()
}

/// A machine with a prepared binding whose App Server got `environment`
/// and whose Computer Use was enabled before it started, as an unattended
/// start script does, with no terminal and stdin closed.
fn unattended(case: &mut Case, directory: &Path, environment: &[(String, String)]) -> TaskMachine {
    let driver = driver();
    let tasks = TaskMachine::start_prepared_with_environment(
        case,
        directory,
        &borrowed(environment),
        |root| mcp::record_before_start(root, &["enable", "--driver", &driver]),
    );
    case.command(
        "tetral computer-use enable --app app_one --driver <release>, before tetral server run",
        "0",
    );
    tasks
}

/// The processes running `<driver> mcp`.
fn driver_processes(driver: &str) -> Vec<u32> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        else {
            continue;
        };
        let Ok(command_line) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let arguments: Vec<&[u8]> = command_line.split(|byte| *byte == 0).collect();
        if arguments.len() >= 2 && arguments[0] == driver.as_bytes() && arguments[1] == b"mcp" {
            found.push(pid);
        }
    }
    found.sort_unstable();
    found
}

/// Waits until `count` processes run `<driver> mcp` and gives them.
fn wait_for_driver_processes(driver: &str, count: usize) -> Vec<u32> {
    let mut seen = Vec::new();
    wait_until(&format!("{count} driver processes"), || {
        seen = driver_processes(driver);
        seen.len() == count
    });
    seen
}

/// A JSON-RPC client of `<driver> mcp` started directly by the test, in
/// `/` with `environment` only.
struct Direct {
    child: Child,
    input: ChildStdin,
    lines: Receiver<String>,
    next_id: u64,
}

impl Direct {
    fn start(environment: &[(String, String)]) -> Direct {
        let mut child = Command::new(driver())
            .arg("mcp")
            .current_dir("/")
            .env_clear()
            .envs(environment.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("the driver starts");
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Direct {
            child,
            input,
            lines,
            next_id: 0,
        }
    }

    fn send(&mut self, message: Value) {
        let mut line = serde_json::to_vec(&message).unwrap();
        line.push(b'\n');
        self.input.write_all(&line).unwrap();
    }

    /// The answer to a request of `method`, or `None` when the driver
    /// ends first; a `ping` from the driver is answered on the way.
    fn request(&mut self, method: &str, params: Value) -> Option<Value> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let line = self.lines.recv_timeout(ANSWER_WAIT).ok()?;
            let message: Value = serde_json::from_str(&line).expect("one JSON message per line");
            if message["id"] == id && message.get("method").is_none() {
                return Some(message);
            }
            if message["method"] == "ping" {
                self.send(json!({"jsonrpc": "2.0", "id": message["id"], "result": {}}));
            }
        }
    }

    /// `initialize` then every `tools/list` page: the tools, or the
    /// `initialize` answer when it is not a result.
    fn list(mut self) -> Result<Vec<Value>, Option<Value>> {
        let initialized = self.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "tetral-test", "version": "0"},
            }),
        );
        match &initialized {
            Some(answer) if answer.get("result").is_some() => {}
            _ => return Err(initialized),
        }
        self.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        let mut tools = Vec::new();
        let mut cursor = Value::Null;
        loop {
            let params = match &cursor {
                Value::Null => json!({}),
                cursor => json!({ "cursor": cursor }),
            };
            let page = self
                .request("tools/list", params)
                .expect("a tools/list answer");
            let result = &page["result"];
            tools.extend(
                result["tools"]
                    .as_array()
                    .expect("a tools array")
                    .iter()
                    .cloned(),
            );
            cursor = result["nextCursor"].clone();
            if cursor.is_null() {
                return Ok(tools);
            }
        }
    }
}

impl Drop for Direct {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The structured result of a successful terminal report.
fn structured(report: &Value) -> &Value {
    assert_eq!(report["result"]["status"], "success", "{report:#}");
    &report["result"]["result"]["structuredContent"]
}

/// Whether a terminal report carries an error the driver answered: a
/// JSON-RPC error or a result marked `isError`.
fn drivers_error(report: &Value) -> bool {
    let result = &report["result"];
    (result["status"] == "error" && result["error"]["kind"] == "mcp_error")
        || (result["status"] == "success" && result["result"]["isError"] == true)
}

#[test]
#[ignore = "needs the cua-driver release named by TETRAL_TEST_CUA_DRIVER"]
fn the_releases_tool_list_report_equals_a_direct_listing_after_an_unattended_start() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record
        .case("the release enabled before an unattended tetral server run; its tool list reported");
    let directory = tempfile::tempdir().unwrap();
    let environment = environment(true);
    let tasks = unattended(&mut case, directory.path(), &environment);
    let report = wait_for_reports(&tasks.engine, 1).remove(0);
    let direct = Direct::start(&environment)
        .list()
        .expect("the release lists its tools");

    assert_eq!(report["error"], Value::Null, "{report:#}");
    assert_eq!(report["server_info"]["name"], "cua-driver");
    let tools = report["tools"].as_array().unwrap();
    assert_eq!(tools.len(), direct.len());
    for tool in tools {
        for member in tool.as_object().unwrap().keys() {
            assert!(TOOL_MEMBERS.contains(&member.as_str()), "{member}");
        }
    }
    let sizes: Vec<u64> = tasks
        .engine
        .requests("mcp-tools")
        .iter()
        .map(|request| request["body_bytes"].as_u64().unwrap())
        .collect();
    assert!(sizes.iter().all(|size| *size < 4_194_304), "{sizes:?}");
    case.observe(format!(
        "{} tools reported and listed directly; body sizes {sizes:?}; server_info and tools: {}",
        tools.len(),
        json!({"server_info": report["server_info"], "tools": report["tools"]})
    ));
    case.pass();
}

#[test]
#[ignore = "needs the cua-driver release named by TETRAL_TEST_CUA_DRIVER and a desktop session"]
fn each_binding_has_its_own_driver_and_a_close_ends_only_its_own() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "bindings A and B calling the release; two concurrent type_text calls on A; A closed during a call",
    );
    let driver = driver();
    let directory = tempfile::tempdir().unwrap();
    let environment = environment(true);
    let tasks = unattended(&mut case, directory.path(), &environment);
    wait_for_reports(&tasks.engine, 1);
    let server = wait_for_driver_processes(&driver, 1);

    tasks.mcp_call("state_a", "get_desktop_state", json!({}));
    structured(&tasks.terminal("state_a"));
    let with_a = wait_for_driver_processes(&driver, 2);
    let process_a = *with_a.iter().find(|pid| !server.contains(pid)).unwrap();
    let prepared = tasks.prepare("sess_b", "bind_b", &tasks.project, None);
    tasks.wait_prepared(&prepared);
    let call_b = |task_id: &str, tool: &str, arguments: Value| {
        tasks.mcp_call_with(
            "sess_b",
            "bind_b",
            task_id,
            json!({"server": "cua", "tool": tool, "arguments": arguments}),
        );
        tasks.terminal(task_id)
    };
    structured(&call_b("state_b", "get_desktop_state", json!({})));
    let with_b = wait_for_driver_processes(&driver, 3);
    let process_b = *with_b.iter().find(|pid| !with_a.contains(pid)).unwrap();

    let mut target = Command::new("xmessage")
        .args(["-geometry", "600x200", "target"])
        .env_clear()
        .envs(environment.iter().cloned())
        .spawn()
        .expect("xmessage starts");
    let window = {
        let mut found = Value::Null;
        wait_until("the target's window", || {
            let task_id = format!("windows_{}", next_number());
            tasks.mcp_call(&task_id, "list_windows", json!({"pid": target.id()}));
            let report = tasks.terminal(&task_id);
            found = structured(&report)["windows"][0].clone();
            !found.is_null()
        });
        found
    };
    let typed = |task_id: &str| {
        tasks.mcp_call(
            task_id,
            "type_text",
            json!({
                "target": {"kind": "window", "pid": target.id(), "window_id": window["window_id"]},
                "text": "x".repeat(2_000),
            }),
        );
    };
    typed("type_1");
    typed("type_2");
    let typing = [tasks.terminal("type_1"), tasks.terminal("type_2")];
    assert!(
        typing.iter().any(drivers_error),
        "neither call got the driver's conflict error: {typing:#?}"
    );

    typed("type_long");
    tasks.engine.control(
        "close",
        json!({"instance_id": INSTANCE, "payload": {"session_id": SESSION, "binding_id": BINDING}}),
    );
    let closed = tasks.terminal("type_long");
    wait_for_driver_processes(&driver, 2);
    assert!(!driver_processes(&driver).contains(&process_a));
    assert!(driver_processes(&driver).contains(&process_b));
    assert!(driver_processes(&driver).contains(&server[0]));
    structured(&call_b("state_b_after", "get_desktop_state", json!({})));
    let _ = target.kill();
    let _ = target.wait();
    case.observe(format!(
        "one driver process for the server, one more after A's and after B's first call; the concurrent type_text answers: {}; A's call during the close: {}; after the close A's process gone, B's and the server's running, B's next get_desktop_state succeeded",
        json!([typing[0]["result"], typing[1]["result"]]),
        closed["result"]
    ));
    case.pass();
}

#[test]
#[ignore = "needs the cua-driver release named by TETRAL_TEST_CUA_DRIVER"]
fn without_a_desktop_the_report_and_calls_are_the_drivers_own() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case("the release enabled on an App Server without desktop variables");
    let directory = tempfile::tempdir().unwrap();
    let environment = environment(false);
    let tasks = unattended(&mut case, directory.path(), &environment);
    let report = wait_for_reports(&tasks.engine, 1).remove(0);
    match Direct::start(&environment).list() {
        Ok(tools) => {
            assert_eq!(report["error"], Value::Null, "{report:#}");
            assert_eq!(report["tools"].as_array().unwrap().len(), tools.len());
            tasks.mcp_call("state", "get_desktop_state", json!({}));
            let call = tasks.terminal("state");
            assert!(drivers_error(&call), "{call:#}");
            case.observe(format!(
                "the release's own list of {} tools; get_desktop_state answered {}",
                tools.len(),
                call["result"]
            ));
        }
        Err(answer) => {
            assert_eq!(report["tools"], json!([]));
            assert_eq!(report["error"]["code"], "mcp_start_failed", "{report:#}");
            case.observe(format!(
                "a direct initialize answered {answer:?}; the report: {}",
                report["error"]
            ));
        }
    }
    case.pass();
}

/// A number that keeps each listing's `task_id` new.
fn next_number() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
