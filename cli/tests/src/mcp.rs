//! The test MCP server, the binary target `mcp-test-server` of this package,
//! as a test drives it: its scenario, the record each of its processes
//! keeps and their control sockets (`src/bin/mcp-test-server.rs`), and
//! `tetral computer-use` against a machine.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::app_server::wait_until;
use crate::machine::{APP, CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine};
use crate::record::Case;
use crate::reference::ReferenceServer;
use crate::run::{Run, Tetral};

/// The variable that names the test server's scenario file.
pub const SCENARIO_VARIABLE: &str = "MCP_TEST_SCENARIO";

/// A driver whose `<driver> mcp` is the test server with one scenario.
pub struct TestDriver {
    directory: PathBuf,
    /// The path given as `--driver`.
    pub program: PathBuf,
}

impl TestDriver {
    /// The test server `binary` with `scenario`, its records in
    /// `directory`, which it creates.
    pub fn new(directory: &Path, binary: &Path, scenario: Value) -> TestDriver {
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(
            directory.join("scenario.json"),
            serde_json::to_vec(&scenario).unwrap(),
        )
        .unwrap();
        TestDriver {
            directory: directory.to_owned(),
            program: binary.to_owned(),
        }
    }

    /// [`TestDriver::new`] behind a script that ignores SIGTERM, which the
    /// test server inherits.
    pub fn ignoring_sigterm(directory: &Path, binary: &Path, scenario: Value) -> TestDriver {
        let mut driver = TestDriver::new(directory, binary, scenario);
        let script = directory.join("driver-ignoring-sigterm");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\ntrap '' TERM\nexec '{}' \"$@\"\n",
                binary.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        driver.program = script;
        driver
    }

    /// Replaces the scenario, which each process reads when it starts: the
    /// processes started from now on follow `scenario`.
    pub fn set_scenario(&self, scenario: Value) {
        std::fs::write(
            self.directory.join("scenario.json"),
            serde_json::to_vec(&scenario).unwrap(),
        )
        .unwrap();
    }

    /// The effect files the processes' calls created, by name.
    pub fn effects(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.directory)
            .unwrap()
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| name.starts_with("effect-"))
            .collect();
        names.sort();
        names
    }

    /// The environment variable that gives an App Server this driver's
    /// scenario.
    pub fn environment(&self) -> (&'static str, String) {
        (
            SCENARIO_VARIABLE,
            self.directory.join("scenario.json").display().to_string(),
        )
    }

    pub fn driver(&self) -> &str {
        self.program.to_str().expect("a UTF-8 test path")
    }

    /// The process IDs of every process started so far, in start order.
    pub fn processes(&self) -> Vec<u32> {
        std::fs::read_to_string(self.directory.join("processes"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| line.parse().ok())
            .collect()
    }

    /// Waits until a `--version` of the scenario `version: "never"` has
    /// written its process ID and gives it.
    pub fn wait_for_version_pid(&self) -> u32 {
        let path = self.directory.join("version-pid");
        let mut pid = None;
        wait_until("the --version process ID", || {
            pid = std::fs::read_to_string(&path)
                .ok()
                .and_then(|text| text.parse().ok());
            pid.is_some()
        });
        pid.expect("a process ID")
    }

    /// Waits until `count` processes have started and gives their IDs.
    pub fn wait_for_processes(&self, count: usize) -> Vec<u32> {
        let mut seen = Vec::new();
        wait_until(&format!("{count} test server processes"), || {
            seen = self.processes();
            seen.len() >= count
        });
        seen
    }

    /// What the process `pid` started with: `pid`, `cwd` and whether
    /// `TETRAL_HOME` was set.
    pub fn started(&self, pid: u32) -> Value {
        self.record(pid)
            .into_iter()
            .find_map(|entry| entry.get("started").cloned())
            .expect("a started entry")
    }

    /// Every message the process `pid` received, in arrival order.
    pub fn messages(&self, pid: u32) -> Vec<Value> {
        self.record(pid)
            .into_iter()
            .filter_map(|entry| entry.get("message").cloned())
            .collect()
    }

    /// The messages of `method` that the process `pid` received.
    pub fn received(&self, pid: u32, method: &str) -> Vec<Value> {
        self.messages(pid)
            .into_iter()
            .filter(|message| message["method"] == method)
            .collect()
    }

    /// The messages of `method` that the process `pid` received, each with
    /// its arrival time in milliseconds since the Unix epoch.
    pub fn received_at(&self, pid: u32, method: &str) -> Vec<(u64, Value)> {
        self.record(pid)
            .into_iter()
            .filter(|entry| entry["message"]["method"] == method)
            .map(|entry| (entry["at_ms"].as_u64().unwrap(), entry["message"].clone()))
            .collect()
    }

    /// The IDs of the `tools/call` requests whose answers the process `pid`
    /// has written.
    pub fn answered(&self, pid: u32) -> Vec<Value> {
        self.record(pid)
            .into_iter()
            .filter_map(|entry| entry.get("answered").cloned())
            .collect()
    }

    /// Waits until the process `pid` has received `count` messages of
    /// `method` and gives them.
    pub fn wait_for_received(&self, pid: u32, method: &str, count: usize) -> Vec<Value> {
        let mut seen = Vec::new();
        wait_until(&format!("{count} {method} messages"), || {
            seen = self.received(pid, method);
            seen.len() >= count
        });
        seen
    }

    fn record(&self, pid: u32) -> Vec<Value> {
        let text = std::fs::read_to_string(self.directory.join(format!("{pid}.jsonl")))
            .unwrap_or_default();
        text.lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    /// Sends the process `pid` a control command and waits until it is done.
    pub fn control(&self, pid: u32, command: Value) {
        let mut stream = UnixStream::connect(self.directory.join(format!("{pid}.sock")))
            .unwrap_or_else(|error| panic!("the control socket of {pid}: {error}"));
        let mut line = serde_json::to_vec(&command).unwrap();
        line.push(b'\n');
        stream.write_all(&line).unwrap();
        // An exit closes the socket without an answer.
        let mut answer = String::new();
        let _ = BufReader::new(&stream).read_line(&mut answer);
    }
}

/// Whether the process `pid` exists, a zombie included.
pub fn alive(pid: u32) -> bool {
    let pid = rustix::process::Pid::from_raw(pid as i32).expect("a positive process ID");
    rustix::process::test_kill_process(pid).is_ok()
}

/// Whether any process of the group `pid` leads remains.
pub fn group_alive(pid: u32) -> bool {
    let pid = rustix::process::Pid::from_raw(pid as i32).expect("a positive process ID");
    rustix::process::test_kill_process_group(pid).is_ok()
}

/// A `cua` tool of the test server: `name` with an object input schema.
pub fn tool(name: &str) -> Value {
    json!({ "name": name, "inputSchema": { "type": "object" } })
}

/// One page of `tools/list` answers: `tools` after `cursor`, with
/// `next_cursor` when another page follows.
pub fn page(cursor: Option<&str>, tools: Vec<Value>, next_cursor: Option<&str>) -> Value {
    let mut result = json!({ "tools": tools });
    if let Some(next) = next_cursor {
        result["nextCursor"] = json!(next);
    }
    json!({ "cursor": cursor, "result": result })
}

/// A running App Server with Computer Use's test driver: the reference
/// control server, the machine and the driver, whose directory the test
/// keeps.
pub struct Setup {
    pub engine: ReferenceServer,
    pub machine: Machine,
    pub driver: TestDriver,
    pub directory: tempfile::TempDir,
}

/// An App Server online with the test server `test_server` and `scenario`
/// as its driver, Computer Use not yet recorded; `prepare` runs on the
/// configured root before it starts.
pub fn online_with(
    case: &mut Case,
    test_server: &Path,
    scenario: Value,
    prepare: impl FnOnce(&Path, &TestDriver),
) -> Setup {
    let engine = one_credential_engine();
    let directory = tempfile::tempdir().unwrap();
    let driver = TestDriver::new(&directory.path().join("driver"), test_server, scenario);
    let (name, value) = driver.environment();
    let server_run = Tetral::new(["server", "run", "--app", APP])
        .current_dir(directory.path())
        .env(name, value);
    let machine = Machine::start_prepared(
        case,
        directory.path(),
        &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        server_run,
        |root| prepare(root, &driver),
    );
    machine.wait_for("online");
    Setup {
        engine,
        machine,
        driver,
        directory,
    }
}

/// `tetral computer-use enable --app app_one --driver <driver>` on the
/// machine's root, on its seam clock.
pub fn enable(machine: &Machine, driver: &str) -> Run {
    Tetral::new(["computer-use", "enable", "--app", APP, "--driver", driver])
        .home(&machine.root)
        .seams(&machine.seams)
        .run()
}

/// Records Computer Use with `tetral computer-use <arguments> --app
/// app_one` on a root no App Server owns; the command must succeed.
pub fn record_before_start(root: &Path, arguments: &[&str]) {
    let mut command_line = vec!["computer-use"];
    command_line.extend_from_slice(arguments);
    command_line.extend(["--app", APP]);
    let run = Tetral::new(command_line).home(root).run();
    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
}

/// `tetral computer-use disable --app app_one` on the machine's root.
pub fn disable(machine: &Machine) -> Run {
    Tetral::new(["computer-use", "disable", "--app", APP])
        .home(&machine.root)
        .seams(&machine.seams)
        .run()
}

/// The `body` of each accepted tool-list report of `cua`, in arrival order.
pub fn reports(engine: &ReferenceServer) -> Vec<Value> {
    engine
        .received("mcp-tools")
        .into_iter()
        .map(|message| message["body"].clone())
        .filter(|body| body["server"] == "cua")
        .collect()
}

/// Waits until `count` tool-list reports were accepted and gives them.
pub fn wait_for_reports(engine: &ReferenceServer, count: usize) -> Vec<Value> {
    let mut seen = Vec::new();
    wait_until(&format!("{count} tool-list reports"), || {
        seen = reports(engine);
        seen.len() >= count
    });
    seen
}

/// The names of a report's tools.
pub fn names(report: &Value) -> Vec<String> {
    report["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect()
}
