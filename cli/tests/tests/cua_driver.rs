//! Computer Use with the pinned cua-driver release: `tetral` against the
//! reference control server with the release as `--driver`. The tests are
//! ignored by default. Run them in `cli/` with `TETRAL_TEST_CUA_DRIVER` set
//! to the release's `cua-driver` executable:
//! `cargo test --test cua_driver -- --ignored --test-threads=1`, one test at
//! a time because they count the driver's processes on the machine and
//! share its desktop. Without the variable every test fails. The App Server
//! gets the test's session variables and any `CUA_DRIVER_*` variable, as a
//! launcher would pass them to `tetral server run`.
//!
//! On Linux the desktop tests need a desktop session reachable through
//! `DISPLAY` (an X server such as Xvfb) and `xmessage` as the target
//! application. On macOS the executable is
//! `/Applications/CuaDriver.app/Contents/MacOS/cua-driver` of the installed
//! release, `CuaDriver.app` holds the Accessibility and Screen Recording
//! grants, and the tests run in the logged-in user's desktop session; the
//! target is a new TextEdit instance with a document of the test's.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tetral_tests::app_server::wait_until;
use tetral_tests::machine::INSTANCE;
use tetral_tests::mcp::{self, wait_for_reports};
use tetral_tests::record::Case;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, SESSION, TaskMachine};

const DRIVER_VARIABLE: &str = "TETRAL_TEST_CUA_DRIVER";

/// The variables through which the driver reaches a desktop session.
#[cfg(target_os = "linux")]
const SESSION_VARIABLES: &[&str] = &[
    "DISPLAY",
    "XAUTHORITY",
    "WAYLAND_DISPLAY",
    "XDG_RUNTIME_DIR",
    "XDG_SESSION_TYPE",
    "DBUS_SESSION_BUS_ADDRESS",
];
/// `<driver> mcp` finds `CuaDriver.app`'s socket under `HOME`.
#[cfg(target_os = "macos")]
const SESSION_VARIABLES: &[&str] = &["HOME"];

/// How long a direct client waits for each answer.
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

/// The App Server's added environment: the test's session variables when
/// `desktop`, and its `CUA_DRIVER_*` variables.
fn environment(desktop: bool) -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(name, _)| {
            (desktop && SESSION_VARIABLES.contains(&name.as_str()))
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
#[cfg(target_os = "linux")]
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

/// The processes running `<driver> mcp`; `CuaDriver.app` itself runs
/// `<driver> serve` and is not one of them.
#[cfg(target_os = "macos")]
fn driver_processes(driver: &str) -> Vec<u32> {
    let listing = Command::new("ps")
        .args(["-axww", "-o", "pid=", "-o", "args="])
        .output()
        .expect("ps runs");
    let wanted = format!("{driver} mcp");
    let mut found: Vec<u32> = String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter_map(|line| {
            let (pid, arguments) = line.trim_start().split_once(' ')?;
            (arguments.trim() == wanted)
                .then(|| pid.parse().ok())
                .flatten()
        })
        .collect();
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

    /// `initialize` and `notifications/initialized`; the `initialize`
    /// answer when it is not a result.
    fn initialize(&mut self) -> Result<(), Option<Value>> {
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
        Ok(())
    }

    /// `initialize` then every `tools/list` page: the tools, or the
    /// `initialize` answer when it is not a result.
    fn list(mut self) -> Result<Vec<Value>, Option<Value>> {
        self.initialize()?;
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

    /// The structured result of a `tools/call` that succeeds.
    #[cfg(target_os = "macos")]
    fn call(&mut self, tool: &str, arguments: Value) -> Value {
        let answer = self
            .request("tools/call", json!({"name": tool, "arguments": arguments}))
            .unwrap_or_else(|| panic!("an answer to {tool}"));
        let result = &answer["result"];
        assert!(
            result.is_object() && result["isError"] != true,
            "{tool}: {answer:#}"
        );
        result["structuredContent"].clone()
    }
}

impl Drop for Direct {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The application the desktop tests type into: `xmessage`, which the test
/// starts, once binding A's `list_windows` shows its window.
#[cfg(target_os = "linux")]
struct Target {
    child: Child,
}

#[cfg(target_os = "linux")]
impl Target {
    fn open(tasks: &TaskMachine, environment: &[(String, String)]) -> Target {
        let child = Command::new("xmessage")
            .args(["-geometry", "600x200", "target"])
            .env_clear()
            .envs(environment.iter().cloned())
            .spawn()
            .expect("xmessage starts");
        wait_until("the target's window", || {
            let task_id = format!("windows_{}", next_number());
            tasks.mcp_call(&task_id, "list_windows", json!({"pid": child.id()}));
            let report = tasks.terminal(&task_id);
            !structured(&report)["windows"][0].is_null()
        });
        Target { child }
    }

    /// `xmessage` holds no text to read back.
    fn contents(&self) -> Option<String> {
        None
    }

    /// The tool and arguments that type `text` into the target's process.
    fn typing(&self, text: &str) -> (&'static str, Value) {
        ("type_text", json!({"pid": self.child.id(), "text": text}))
    }
}

#[cfg(target_os = "linux")]
impl Drop for Target {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The time macOS `type_text` takes per character in `foreground`
/// delivery: 8 ms after the key-down and `delay_ms`, 30, after the key-up.
#[cfg(target_os = "macos")]
const CHARACTER_TIME: Duration = Duration::from_millis(38);

/// The application the desktop tests type into: a new TextEdit instance
/// with a plain-text document of the test's. The test opens and reads it
/// through `<driver> mcp` processes of its own, each ended before its query
/// returns, so that only the bindings' processes stay. `type_text` inserts
/// through Accessibility at once; the calls type in `foreground` delivery,
/// one key per character, and without a `window_id`: with one, the driver
/// brings the window to the front and holds its daemon-wide foreground lock
/// while typing, which would serialize two bindings' concurrent calls.
#[cfg(target_os = "macos")]
struct Target {
    environment: Vec<(String, String)>,
    pid: u64,
    window_id: u64,
    _document: tempfile::TempDir,
}

#[cfg(target_os = "macos")]
impl Target {
    /// The structured result of `tool` through a `<driver> mcp` of the
    /// test's.
    fn query(environment: &[(String, String)], tool: &str, arguments: Value) -> Value {
        let mut direct = Direct::start(environment);
        direct.initialize().expect("the release initializes");
        direct.call(tool, arguments)
    }

    fn open(_tasks: &TaskMachine, environment: &[(String, String)]) -> Target {
        let document = tempfile::tempdir().unwrap();
        let path = document.path().join("target.txt");
        std::fs::write(&path, "").unwrap();
        let launched = Target::query(
            environment,
            "launch_app",
            json!({
                "bundle_id": "com.apple.TextEdit",
                "urls": [path],
                "creates_new_application_instance": true,
            }),
        );
        let pid = launched["pid"].as_u64().expect("TextEdit's pid");
        let mut target = Target {
            environment: environment.to_vec(),
            pid,
            window_id: 0,
            _document: document,
        };
        // The document's window can appear after the launch answers, beside
        // a restored one; the target is the window holding the editor.
        wait_until("TextEdit's editor window", || {
            let listed = Target::query(&target.environment, "list_windows", json!({}));
            let windows: Vec<u64> = listed["windows"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|window| window["pid"].as_u64() == Some(pid))
                .filter_map(|window| window["window_id"].as_u64())
                .collect();
            windows.into_iter().any(|window_id| {
                target.window_id = window_id;
                target.editor().is_some()
            })
        });
        target
    }

    /// The editor's text area in a fresh snapshot of the window.
    fn editor(&self) -> Option<Value> {
        let state = Target::query(
            &self.environment,
            "get_window_state",
            json!({"pid": self.pid, "window_id": self.window_id, "include_screenshot": false}),
        );
        state["elements"]
            .as_array()?
            .iter()
            .find(|element| element["role"] == "AXTextArea")
            .cloned()
    }

    /// The editor's text.
    fn text(&self) -> String {
        let editor = self.editor().expect("the editor is in the window");
        editor["value"].as_str().unwrap_or_default().to_owned()
    }

    fn contents(&self) -> Option<String> {
        Some(self.text())
    }

    /// The tool and arguments that type `text` into the target's process.
    fn typing(&self, text: &str) -> (&'static str, Value) {
        (
            "type_text",
            json!({"pid": self.pid, "text": text, "delivery_mode": "foreground", "delay_ms": 30}),
        )
    }
}

#[cfg(target_os = "macos")]
impl Drop for Target {
    fn drop(&mut self) {
        // Killed, so that TextEdit asks nothing about the unsaved document.
        let _ = Command::new("kill")
            .args(["-KILL", &self.pid.to_string()])
            .status();
    }
}

/// The structured result of a successful terminal report.
fn structured(report: &Value) -> &Value {
    assert_eq!(report["result"]["status"], "success", "{report:#}");
    &report["result"]["result"]["structuredContent"]
}

/// Whether a terminal report carries the driver's success: a result not
/// marked `isError`.
fn drivers_success(report: &Value) -> bool {
    let result = &report["result"];
    result["status"] == "success" && result["result"]["isError"] != true
}

/// Whether a terminal report carries an error the driver answered: a
/// JSON-RPC error or a result marked `isError`.
#[cfg(target_os = "linux")]
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
        "bindings A and B calling the release; their concurrent typing into one process; A closed during a call",
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
    let send_b = |task_id: &str, tool: &str, arguments: Value| {
        tasks.mcp_call_with(
            "sess_b",
            "bind_b",
            task_id,
            json!({"server": "cua", "tool": tool, "arguments": arguments}),
        );
    };
    send_b("state_b", "get_desktop_state", json!({}));
    structured(&tasks.terminal("state_b"));
    let with_b = wait_for_driver_processes(&driver, 3);
    let process_b = *with_b.iter().find(|pid| !with_a.contains(pid)).unwrap();

    let target = Target::open(&tasks, &environment);
    // A types 200 `a` and B 200 `b` into the target's process at once,
    // neither naming a window; Tetral holds neither call back.
    let started = Instant::now();
    let (tool, arguments) = target.typing(&"a".repeat(200));
    tasks.mcp_call("type_a", tool, arguments);
    let (tool, arguments) = target.typing(&"b".repeat(200));
    send_b("type_b", tool, arguments);
    let typing = [tasks.terminal("type_a"), tasks.terminal("type_b")];
    let elapsed = started.elapsed();
    let typed = target.contents();
    assert!(
        typing.iter().all(drivers_success),
        "{typing:#?}; the target holds {typed:?}"
    );
    #[cfg(target_os = "macos")]
    {
        // Typed one after the other, the two calls would take twice this.
        let one_call = CHARACTER_TIME * 200;
        assert!(
            elapsed < one_call * 3 / 2,
            "both answers after {elapsed:?}; one call types for {one_call:?}"
        );
        let typed = typed.as_deref().unwrap_or_default();
        assert_eq!(
            (
                typed.matches('a').count(),
                typed.matches('b').count(),
                typed.chars().count()
            ),
            (200, 200, 400),
            "{typed:?}"
        );
    }

    // At 38 ms per character on macOS, 200 characters keep a call running
    // for 7.6 s.
    let long = if cfg!(target_os = "macos") {
        200
    } else {
        2_000
    };
    let (tool, arguments) = target.typing(&"x".repeat(long));
    tasks.mcp_call("type_long", tool, arguments);
    tasks.engine.control(
        "close",
        json!({"instance_id": INSTANCE, "payload": {"session_id": SESSION, "binding_id": BINDING}}),
    );
    let closed = tasks.terminal("type_long");
    wait_for_driver_processes(&driver, 2);
    assert!(!driver_processes(&driver).contains(&process_a));
    assert!(driver_processes(&driver).contains(&process_b));
    assert!(driver_processes(&driver).contains(&server[0]));
    send_b("state_b_after", "get_desktop_state", json!({}));
    structured(&tasks.terminal("state_b_after"));
    drop(target);
    case.observe(format!(
        "one driver process for the server, one more after A's and after B's first call; the concurrent typing answers after {elapsed:?}: {}, the target then holding {typed:?}; A's call during the close: {}; after the close A's process gone, B's and the server's running, B's next get_desktop_state succeeded",
        json!([typing[0]["result"], typing[1]["result"]]),
        closed["result"]
    ));
    case.pass();
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "needs the cua-driver release named by TETRAL_TEST_CUA_DRIVER and a desktop session"]
fn a_disable_during_typing_cancels_the_call_rejects_the_next_and_keeps_the_typed_text() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let mut case = record.case(
        "a foreground type_text of 2,000 characters into TextEdit; a disable while it types; another call",
    );
    let directory = tempfile::tempdir().unwrap();
    let environment = environment(true);
    let tasks = unattended(&mut case, directory.path(), &environment);
    wait_for_reports(&tasks.engine, 1);
    let target = Target::open(&tasks, &environment);

    let (tool, arguments) = target.typing(&"x".repeat(2_000));
    tasks.mcp_call("task_running", tool, arguments);
    let mut before = String::new();
    wait_until("the first 20 characters in the editor", || {
        before = target.text();
        before.len() >= 20
    });
    let run = mcp::disable(tasks.machine());
    case.command("tetral computer-use disable --app app_one", &run.exit());
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
    let ended = tasks.terminal("task_running");
    assert_eq!(
        json!([ended["result"]["status"], ended["result"]["error"]]),
        json!(["error", {"kind": "cancelled", "message": "cancelled after the call was sent", "detail": {"sent": true}}])
    );
    let disabled = wait_for_reports(&tasks.engine, 2)[1].clone();
    assert_eq!(
        json!([disabled["tools"], disabled["error"]]),
        json!([[], null])
    );
    tasks.mcp_call("task_after", "get_desktop_state", json!({}));
    let after = tasks.report("task_after", "rejected");
    assert_eq!(
        json!([
            after["phase"],
            after["error"]["code"],
            after["error"]["message"]
        ]),
        json!([
            "rejected",
            "mcp_server_unavailable",
            "server cua is not running"
        ])
    );
    let kept = target.text();
    assert!(kept.starts_with(&before), "{before:?} then {kept:?}");
    drop(target);
    case.observe(format!(
        "{} characters typed when the disable ran; the running call ended cancelled with sent true; a cua report with tools [] and error null; the next call rejected mcp_server_unavailable; the editor then held {} characters",
        before.len(),
        kept.len()
    ));
    case.pass();
}

#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
fn next_number() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
