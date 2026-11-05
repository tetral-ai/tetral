//! A scriptable stdio MCP server for the cross-process tests, run as a
//! driver's `<driver> mcp`. It reads its scenario, one JSON object, from the
//! file that `MCP_TEST_SCENARIO` names; the directory of that file holds
//! `processes`, each process's ID on a line in start order, and for each
//! process `<pid>.jsonl`, the record of what it started with, every message
//! it received with its arrival time and the ID of each `tools/call` it
//! answered once the answer is written, and `<pid>.sock`, its control
//! socket.
//!
//! Run as `<driver> --version` instead, it follows the scenario's
//! `version`, and without one prints nothing and exits 1.
//!
//! Scenario members, each optional:
//! - `version`: `"never"` to never exit; or an object whose `line`, when
//!   given, is printed as one line before the process exits with `exit`, 0
//!   when not given.
//! - `initialize`: `"never"` to leave `initialize` unanswered; or an object
//!   with `result` or `error`, answered as given; or with `protocol_version`
//!   and `server_info` for the default result, which is `protocolVersion`
//!   `2025-06-18`, the `tools` capability with `listChanged` and
//!   `serverInfo` `{name: "mcp-test-server", version: "1.0.0"}`.
//! - `pages`: the answers to `tools/list`, each `{cursor, result | error,
//!   hold}`: the page whose `cursor` equals the request's (`null` for none)
//!   answers it with its `result` or `error`, once the barrier `hold`, when
//!   named, is released. A cursor no page names gets the error `-32602`.
//!
//! - `calls`: by tool name, how a `tools/call` of that tool is answered:
//!   `{result}` or `{error}` answered as given, once the barrier `hold`,
//!   when named, is released; `{never: true}` never answered; `{exit:
//!   <code>}` exits at once. With `effect: true`, the call first creates
//!   the file `effect-<pid>-<n>`, `n` counting this process's calls, beside
//!   the scenario. In a `result` or `error`, a string `"$png:<w>x<h>:<n>"` stands for
//!   the base64 of a PNG header of `w` by `h` pixels padded to `n` bytes,
//!   `"$bytes:<n>"` for the base64 of `n` zero bytes and `"$text:<n>"` for
//!   `n` letters `x`. A tool `calls` does not name gets the error `-32602`.
//!
//! `ping` gets `{}`; any other request gets the error `-32601`.
//!
//! Each control request is one connection carrying one JSON object and
//! answered `ok` once done: `{"send": <message>}` writes the message as one
//! line; `{"line_bytes": n}` writes a `notifications/message` line of
//! exactly `n` bytes before its line ending; `{"pages": [...]}` replaces
//! the pages; `{"release": "<barrier>"}` releases a barrier; `{"exit":
//! <code>}` exits at once with that code.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

struct Server {
    scenario: Value,
    /// Where a call's effect file goes.
    effects: PathBuf,
    out: Mutex<std::io::Stdout>,
    record: Mutex<File>,
    state: Mutex<State>,
    released: Condvar,
}

struct State {
    pages: Vec<Value>,
    released: HashSet<String>,
    calls: u64,
}

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments != ["mcp"] && arguments != ["--version"] {
        std::process::exit(2);
    }
    let scenario_path = PathBuf::from(
        std::env::var_os("MCP_TEST_SCENARIO").expect("MCP_TEST_SCENARIO names the scenario"),
    );
    let scenario: Value =
        serde_json::from_slice(&std::fs::read(&scenario_path).expect("the scenario file"))
            .expect("the scenario is JSON");
    if arguments == ["--version"] {
        let version = &scenario["version"];
        if version == "never" {
            loop {
                std::thread::park();
            }
        }
        if let Some(line) = version["line"].as_str() {
            println!("{line}");
        }
        let code = match version {
            Value::Null => 1,
            version => version["exit"].as_i64().unwrap_or(0),
        };
        std::process::exit(i32::try_from(code).expect("an exit status"));
    }
    let directory = scenario_path
        .parent()
        .expect("the scenario has a directory")
        .to_owned();
    let pid = std::process::id();
    let effects = directory.clone();
    let record = File::create(directory.join(format!("{pid}.jsonl"))).expect("the record");
    let server = Arc::new(Server {
        out: Mutex::new(std::io::stdout()),
        record: Mutex::new(record),
        state: Mutex::new(State {
            pages: scenario["pages"].as_array().cloned().unwrap_or_default(),
            released: HashSet::new(),
            calls: 0,
        }),
        released: Condvar::new(),
        scenario,
        effects,
    });
    server.note(json!({
        "started": {
            "pid": pid,
            "cwd": std::env::current_dir().ok(),
            "tetral_home": std::env::var_os("TETRAL_HOME").is_some(),
        }
    }));
    let control =
        UnixListener::bind(directory.join(format!("{pid}.sock"))).expect("the control socket");
    // Listed once its record and control socket exist.
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join("processes"))
        .and_then(|mut processes| processes.write_all(format!("{pid}\n").as_bytes()))
        .expect("the process list");
    let controlled = server.clone();
    std::thread::spawn(move || {
        for connection in control.incoming().flatten() {
            controlled.control(connection);
        }
    });
    for line in std::io::stdin().lock().split(b'\n') {
        let Ok(line) = line else { break };
        let message: Value = serde_json::from_slice(&line).unwrap_or(Value::Null);
        server.note(json!({ "at_ms": unix_ms(), "message": message }));
        server.take(message);
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis())
        .unwrap_or(0)
}

impl Server {
    fn note(&self, entry: Value) {
        let mut record = self.record.lock().unwrap();
        let mut line = serde_json::to_vec(&entry).unwrap();
        line.push(b'\n');
        record.write_all(&line).unwrap();
        record.flush().unwrap();
    }

    fn write(&self, message: &Value) {
        let mut line = serde_json::to_vec(message).unwrap();
        line.push(b'\n');
        self.write_line(&line);
    }

    fn write_line(&self, line: &[u8]) {
        let mut out = self.out.lock().unwrap();
        // A closed stdout leaves the server to its stdin's end.
        let _ = out.write_all(line).and_then(|()| out.flush());
    }

    fn answer(&self, id: &Value, outcome: Result<Value, Value>) {
        self.write(&match outcome {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
        });
    }

    fn take(self: &Arc<Self>, message: Value) {
        let (Some(method), Some(id)) = (message["method"].as_str(), message.get("id")) else {
            return;
        };
        match method {
            "initialize" => self.initialize(id),
            "tools/list" => self.list(id.clone(), message["params"]["cursor"].clone()),
            "tools/call" => self.call(id.clone(), &message["params"]),
            "ping" => self.answer(id, Ok(json!({}))),
            _ => self.answer(
                id,
                Err(json!({ "code": -32601, "message": "Method not found" })),
            ),
        }
    }

    fn initialize(&self, id: &Value) {
        let given = &self.scenario["initialize"];
        if given == "never" {
            return;
        }
        if let Some(error) = given.get("error") {
            return self.answer(id, Err(error.clone()));
        }
        let result = given.get("result").cloned().unwrap_or_else(|| {
            json!({
                "protocolVersion": given.get("protocol_version").cloned().unwrap_or(json!("2025-06-18")),
                "capabilities": { "tools": { "listChanged": true } },
                "serverInfo": given.get("server_info").cloned()
                    .unwrap_or(json!({ "name": "mcp-test-server", "version": "1.0.0" })),
            })
        });
        self.answer(id, Ok(result));
    }

    fn list(self: &Arc<Self>, id: Value, cursor: Value) {
        let page = {
            let state = self.state.lock().unwrap();
            state
                .pages
                .iter()
                .find(|page| page["cursor"] == cursor)
                .cloned()
        };
        let Some(page) = page else {
            return self.answer(
                &id,
                Err(json!({ "code": -32602, "message": "unknown cursor" })),
            );
        };
        let server = self.clone();
        // A held page waits on its own thread, so the server keeps reading.
        std::thread::spawn(move || {
            if let Some(barrier) = page["hold"].as_str() {
                server.wait(barrier);
            }
            let outcome = match page.get("error") {
                Some(error) => Err(error.clone()),
                None => Ok(page["result"].clone()),
            };
            server.answer(&id, outcome);
        });
    }

    fn call(self: &Arc<Self>, id: Value, params: &Value) {
        let tool = params["name"].as_str().unwrap_or_default();
        let Some(behaviour) = self.scenario["calls"].get(tool).cloned() else {
            return self.answer(
                &id,
                Err(json!({ "code": -32602, "message": "unknown tool" })),
            );
        };
        let call = {
            let mut state = self.state.lock().unwrap();
            state.calls += 1;
            state.calls
        };
        if behaviour["effect"] == true {
            let name = format!("effect-{}-{call}", std::process::id());
            std::fs::write(self.effects.join(name), params["arguments"].to_string())
                .expect("the effect file");
        }
        if let Some(code) = behaviour["exit"].as_i64() {
            std::process::exit(code as i32);
        }
        if behaviour["never"] == true {
            return;
        }
        let server = self.clone();
        // A held answer waits on its own thread, so the server keeps reading.
        std::thread::spawn(move || {
            if let Some(barrier) = behaviour["hold"].as_str() {
                server.wait(barrier);
            }
            let outcome = match behaviour.get("error") {
                Some(error) => Err(expand(error)),
                None => Ok(expand(&behaviour["result"])),
            };
            server.answer(&id, outcome);
            server.note(json!({ "at_ms": unix_ms(), "answered": id }));
        });
    }

    fn wait(&self, barrier: &str) {
        let mut state = self.state.lock().unwrap();
        while !state.released.contains(barrier) {
            state = self.released.wait(state).unwrap();
        }
    }

    fn control(&self, connection: std::os::unix::net::UnixStream) {
        let mut reader = BufReader::new(&connection);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let command: Value = serde_json::from_str(&line).expect("a control command is JSON");
        if let Some(message) = command.get("send") {
            self.write(message);
        } else if let Some(bytes) = command["line_bytes"].as_u64() {
            self.write_line(&message_line(bytes as usize));
        } else if let Some(pages) = command["pages"].as_array() {
            self.state.lock().unwrap().pages = pages.clone();
        } else if let Some(barrier) = command["release"].as_str() {
            self.state
                .lock()
                .unwrap()
                .released
                .insert(barrier.to_owned());
            self.released.notify_all();
        } else if let Some(code) = command["exit"].as_i64() {
            std::process::exit(code as i32);
        } else {
            panic!("an unknown control command: {line}");
        }
        let _ = (&connection).write_all(b"ok\n");
    }
}

/// `value` with each placeholder string replaced by what it stands for.
fn expand(value: &Value) -> Value {
    use base64::Engine;
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    match value {
        Value::String(text) => {
            let mut parts = text.splitn(3, ':');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("$bytes"), Some(size), None) => {
                    Value::String(encode(&vec![0; size.parse().unwrap()]))
                }
                (Some("$text"), Some(size), None) => {
                    Value::String("x".repeat(size.parse().unwrap()))
                }
                (Some("$png"), Some(dimensions), Some(size)) => {
                    let (width, height) = dimensions.split_once('x').unwrap();
                    Value::String(encode(&png(
                        width.parse().unwrap(),
                        height.parse().unwrap(),
                        size.parse().unwrap(),
                    )))
                }
                _ => value.clone(),
            }
        }
        Value::Array(values) => Value::Array(values.iter().map(expand).collect()),
        Value::Object(members) => Value::Object(
            members
                .iter()
                .map(|(name, value)| (name.clone(), expand(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A PNG signature and `IHDR` chunk of `width` by `height` pixels, padded
/// with zeros to `size` bytes.
fn png(width: u32, height: u32, size: usize) -> Vec<u8> {
    let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
    bytes.extend_from_slice(&width.to_be_bytes());
    bytes.extend_from_slice(&height.to_be_bytes());
    bytes.extend_from_slice(&[8, 6, 0, 0, 0, 0, 0, 0, 0]);
    assert!(
        bytes.len() <= size,
        "a PNG of at least {} bytes",
        bytes.len()
    );
    bytes.resize(size, 0);
    bytes
}

/// A `notifications/message` line of exactly `bytes` bytes, and its line
/// ending.
fn message_line(bytes: usize) -> Vec<u8> {
    let empty = br#"{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":""}}"#;
    let padding = bytes
        .checked_sub(empty.len())
        .expect("a line at least as long as an empty message");
    let mut line = Vec::with_capacity(bytes + 1);
    line.extend_from_slice(&empty[..empty.len() - 3]);
    line.resize(line.len() + padding, b'x');
    line.extend_from_slice(br#""}}"#);
    line.push(b'\n');
    line
}
