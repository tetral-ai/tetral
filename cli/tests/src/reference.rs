//! The reference control server as a separate process, and its controls.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
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
}

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
        }
    }

    /// Runs the control `name` with `input` and returns its result.
    pub fn control(&self, name: &str, input: Value) -> Value {
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
    /// request log.
    pub fn state(&self) -> Value {
        self.control("state", json!({}))
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

    /// Moves the server's clock forward.
    pub fn advance(&self, step: Duration) {
        let advance_ms = u64::try_from(step.as_millis()).expect("a step in milliseconds");
        self.control("clock", json!({ "advance_ms": advance_ms }));
    }
}

impl Drop for ReferenceServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
