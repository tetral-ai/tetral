//! The reference control server as a separate process, and its controls.
//!
//! Each control a test sends, other than the reads, goes on the test's
//! timeline; a failure's records get the server's traffic, each request at
//! its arrival and at its end, and its state.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::{binaries, timeline};

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
    /// The server's name on the timeline.
    label: String,
    _at_failure: timeline::Registered,
}

/// The controls that only read, which the timeline leaves out.
const READS: [&str; 4] = ["state", "traffic", "sent_event", "drive_state"];

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
        let label = timeline::label("reference");
        let control = address("control_listen");
        timeline::note(
            &label,
            format!(
                "starts as process {} with Engine's side at {}",
                child.id(),
                address("listen")
            ),
        );
        let at_failure = {
            let control = control.clone();
            timeline::at_failure(&label, move || at_failure(&control))
        };
        ReferenceServer {
            url: address("listen"),
            control,
            child,
            client: reqwest::blocking::Client::new(),
            outside_contract: AtomicBool::new(false),
            advanced_ms: AtomicU64::new(0),
            label,
            _at_failure: at_failure,
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
        if !READS.contains(&name) {
            timeline::note(
                &self.label,
                format!("the test sends control {name} {input}"),
            );
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
        let failures = state
            .as_ref()
            .ok()
            .map(|state| state["failures"].clone())
            .filter(|failures| failures.as_array().is_some_and(|each| !each.is_empty()));
        if std::thread::panicking() {
            stop(&mut self.child);
            // The test's own panic is the failure; the server's are its cause.
            if let Some(failures) = failures {
                eprintln!("the reference control server lists failures: {failures}");
            }
            return;
        }
        // Stopped after the panic, whose records read the server.
        let _stop = Stop(&mut self.child);
        state.expect("the reference control server answers its state");
        assert!(
            failures.is_none(),
            "the reference control server lists failures: {}",
            failures.unwrap_or_default()
        );
    }
}

fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Stops the server when dropped.
struct Stop<'a>(&'a mut Child);

impl Drop for Stop<'_> {
    fn drop(&mut self) {
        stop(self.0);
    }
}

/// What the server at `control` gives a failure's records: its traffic on
/// the timeline, and its traffic and state as a file. It is read on a thread
/// of its own, outside any runtime of the test's.
fn at_failure(control: &str) -> timeline::AtFailure {
    let control = control.to_owned();
    let read = std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        let read = |name: &str| {
            client
                .post(format!("{control}/control/{name}"))
                .json(&json!({}))
                .send()
                .and_then(|response| response.json::<Value>())
                .map_err(|error| error.to_string())
        };
        Some((read("traffic"), read("state")))
    })
    .join();
    let Ok(Some((traffic, state))) = read else {
        return timeline::AtFailure::default();
    };
    let time = |nanoseconds: &Value| {
        timeline::since_start(UNIX_EPOCH + Duration::from_nanos(nanoseconds.as_u64().unwrap_or(0)))
    };
    let mut lines = Vec::new();
    match &traffic {
        Ok(entries) => {
            for entry in entries.as_array().into_iter().flatten() {
                let request = format!(
                    "#{} {} {}",
                    entry["seq"],
                    entry["method"].as_str().unwrap_or("?"),
                    entry["path"].as_str().unwrap_or("?")
                );
                lines.push((
                    time(&entry["arrived_unix_ns"]),
                    format!("{request} arrives"),
                ));
                if entry["ended_unix_ns"].is_u64() {
                    lines.push((time(&entry["ended_unix_ns"]), ended(&request, entry)));
                } else {
                    lines.push((timeline::now(), format!("{request} is still being served")));
                }
            }
        }
        Err(error) => lines.push((
            timeline::now(),
            format!("the traffic cannot be read: {error}"),
        )),
    }
    let mut both = json!({});
    for (name, read) in [("traffic", traffic), ("state", state)] {
        both[name] = read.unwrap_or_else(|error| json!({ "unreadable": error }));
    }
    timeline::AtFailure {
        lines,
        files: vec![(
            "json".to_owned(),
            serde_json::to_string_pretty(&both).unwrap_or_default(),
        )],
    }
}

/// How the request `entry` of the traffic log ended, in the words of the
/// timeline: only what the reference control server could know.
fn ended(request: &str, entry: &Value) -> String {
    let status = entry["status"].as_u64();
    match (entry["outcome"].as_str(), status) {
        (Some("answered"), Some(status)) => format!("{request} ends with status {status}"),
        (Some("client_gone"), Some(status)) => {
            format!("{request} ends with status {status} after its client had gone")
        }
        (Some("client_gone"), None) => {
            format!("{request} ends unanswered after its client had gone")
        }
        (Some("client_unknown"), Some(status)) => format!(
            "{request} ends with status {status}; its body was not read to the end, so whether its client was still there is not known"
        ),
        (Some("cut"), _) => format!("{request} is cut without an answer"),
        (outcome, _) => format!(
            "{request} ends: {}",
            json!({ "outcome": outcome, "status": status })
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_way_a_request_ended_has_its_own_words() {
        for (entry, words) in [
            (
                json!({"outcome": "answered", "status": 200}),
                "#1 GET /x ends with status 200",
            ),
            (
                json!({"outcome": "client_gone", "status": 400}),
                "#1 GET /x ends with status 400 after its client had gone",
            ),
            (
                json!({"outcome": "client_gone"}),
                "#1 GET /x ends unanswered after its client had gone",
            ),
            (
                json!({"outcome": "client_unknown", "status": 400}),
                "#1 GET /x ends with status 400; its body was not read to the end, so whether its client was still there is not known",
            ),
            (
                json!({"outcome": "cut"}),
                "#1 GET /x is cut without an answer",
            ),
        ] {
            assert_eq!(ended("#1 GET /x", &entry), words, "{entry}");
        }
    }
}
