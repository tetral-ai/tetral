//! The reference control server as a separate process, and its controls.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use crate::binaries;

/// A running reference control server. Dropping it stops the process.
pub struct ReferenceServer {
    child: Child,
    /// The base URL of Engine's side of the protocol, for `tetral`'s
    /// configuration.
    pub url: String,
    control: String,
    client: reqwest::blocking::Client,
    /// Set while [`ReferenceServer::outside_contract`] runs.
    outside_contract: AtomicBool,
    /// How far [`ReferenceServer::advance`] has moved the clock, in
    /// milliseconds.
    advanced_ms: AtomicU64,
}

/// The controls whose input carries a message of the protocol, which the
/// server refuses when it is invalid and not marked.
const MESSAGE_CONTROLS: [&str; 8] = [
    "prepare", "close", "drives", "execute", "stdin", "cancel", "mcp_call", "send",
];

impl ReferenceServer {
    /// Starts the server with `input` as its startup input: the listen
    /// addresses, the test instances' credentials, the minimum CLI version,
    /// the maximum stream age, the Git proxy URL and the clock's start.
    pub fn start(input: &Value) -> ReferenceServer {
        let mut child = Command::new(binaries::reference_server())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the reference control server starts");
        let mut stdin = child.stdin.take().expect("its stdin is piped");
        stdin
            .write_all(input.to_string().as_bytes())
            .expect("the server reads its startup input");
        drop(stdin);
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("its stdout is piped"))
            .read_line(&mut line)
            .expect("the server prints its addresses");
        let addresses: Value = serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("the server printed {line:?}: {error}"));
        let address =
            |name: &str| format!("http://{}", addresses[name].as_str().expect("an address"));
        ReferenceServer {
            url: address("listen"),
            control: address("control_listen"),
            child,
            client: reqwest::blocking::Client::new(),
            outside_contract: AtomicBool::new(false),
            advanced_ms: AtomicU64::new(0),
        }
    }

    /// Runs `issue`, whose instructions and events the test makes invalid on
    /// purpose: each is marked `outside_contract`, without which the server
    /// refuses a message that fails the protocol's decoder.
    pub fn outside_contract<T>(&self, issue: impl FnOnce() -> T) -> T {
        self.outside_contract.store(true, Ordering::SeqCst);
        let result = issue();
        self.outside_contract.store(false, Ordering::SeqCst);
        result
    }

    /// Runs the control `name` with `input` and returns its result.
    pub fn control(&self, name: &str, mut input: Value) -> Value {
        if self.outside_contract.load(Ordering::SeqCst) && MESSAGE_CONTROLS.contains(&name) {
            input["outside_contract"] = json!(true);
        }
        let response = self
            .client
            .post(format!("{}/control/{name}", self.control))
            .json(&input)
            .send()
            .unwrap_or_else(|error| panic!("control {name}: {error}"));
        let status = response.status();
        let body = response.text().expect("the control answers");
        assert!(status.is_success(), "control {name}: {status} {body}");
        serde_json::from_str(&body)
            .unwrap_or_else(|error| panic!("control {name} answered {body:?}: {error}"))
    }

    /// The server's leases, bindings, received messages, events and
    /// request log; a test that reads it fails at once when the server lists
    /// `failures`, a message it built that failed its decoder among them.
    pub fn state(&self) -> Value {
        let state = self.control("state", json!({}));
        let failures = &state["failures"];
        assert!(
            failures.as_array().is_none_or(Vec::is_empty),
            "the reference control server lists failures: {failures}"
        );
        state
    }

    /// The request log's entries of `endpoint` (`events`, `ready`, ...),
    /// refused ones included, in arrival order.
    pub fn requests(&self, endpoint: &str) -> Vec<Value> {
        self.entries("requests", endpoint)
    }

    /// The messages of `endpoint` that the server accepted, each once.
    pub fn received(&self, endpoint: &str) -> Vec<Value> {
        self.entries("received", endpoint)
    }

    fn entries(&self, log: &str, endpoint: &str) -> Vec<Value> {
        let state = self.state();
        state[log]
            .as_array()
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| entry["endpoint"] == endpoint)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A reader of the current git tickets, for a server that checks them.
    pub fn current_tickets(&self) -> CurrentTickets {
        CurrentTickets {
            control: self.control.clone(),
            client: self.client.clone(),
        }
    }

    /// Moves the server's clock forward.
    pub fn advance(&self, step: Duration) {
        let advance_ms = u64::try_from(step.as_millis()).expect("a step in milliseconds");
        self.control("clock", json!({ "advance_ms": advance_ms }));
        self.advanced_ms.fetch_add(advance_ms, Ordering::SeqCst);
    }

    /// How far the server's clock has moved since its start.
    pub fn advanced(&self) -> Duration {
        Duration::from_millis(self.advanced_ms.load(Ordering::SeqCst))
    }
}

/// Reads the current git ticket of each cloud computer, the last one the
/// server issued to it.
#[derive(Clone)]
pub struct CurrentTickets {
    control: String,
    client: reqwest::blocking::Client,
}

impl CurrentTickets {
    pub fn read(&self) -> Vec<String> {
        let state: Value = self
            .client
            .post(format!("{}/control/state", self.control))
            .json(&json!({}))
            .send()
            .and_then(|response| response.json())
            .expect("the state control answers");
        state["tickets"]
            .as_object()
            .map(|computers| {
                computers
                    .values()
                    .filter_map(|tickets| tickets.as_array()?.last()?.as_str())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Drop for ReferenceServer {
    /// Fails the test when the server met a request it does not implement
    /// or could not answer within the protocol, which it lists as
    /// `failures`, or when it cannot answer the state read that shows them.
    fn drop(&mut self) {
        // Read without a panic of its own, which would abort a test that
        // already panics.
        let state = self
            .client
            .post(format!("{}/control/state", self.control))
            .json(&json!({}))
            .send()
            .and_then(|response| response.json::<Value>());
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            // The test's own panic is the failure; the server's are its cause.
            if let Ok(state) = state
                && state["failures"]
                    .as_array()
                    .is_some_and(|failures| !failures.is_empty())
            {
                eprintln!(
                    "the reference control server lists failures: {}",
                    state["failures"]
                );
            }
            return;
        }
        let failures =
            state.expect("the reference control server answers its state")["failures"].clone();
        assert!(
            failures.as_array().is_none_or(Vec::is_empty),
            "the reference control server lists failures: {failures}"
        );
    }
}
