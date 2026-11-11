//! Layer 2. The CLI-direction cases of `protocol/behaviour-v1.cases.json` against
//! `tetral server run`, with the reference control server as Engine: for
//! each case the instruction that the server sent, and then the requests
//! that `tetral` sent for it, in order, are held to what the case writes.
//! These are the sequences that Engine's tests play as the CLI's behaviour.
//!
//! Each state is brought about with the harness's own machines and each
//! trigger with the server's controls. A case, state, trigger, step or
//! matcher that this runner does not implement fails the test, and the
//! acceptance record lists every case that ran.
//!
//! One state is read wider than the file writes it: a `live_connection` with
//! `mount: false` or without `tools` runs on a machine whose ready may
//! advertise either. `tetral` advertises drive mounting wherever its machine
//! can mount and tool execution always, and neither changes what it does for
//! a binding without drives, the only kind this runner prepares on such a
//! connection.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{Value, json};
use tetral_tests::cases::{self, Bound};
use tetral_tests::drives::DriveMachine;
use tetral_tests::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, wait_for_received,
};
use tetral_tests::record::Case;
use tetral_tests::reference::ReferenceServer;
use tetral_tests::socket_directory;
use tetral_tests::tasks::{BINDING, HEARTBEAT_INTERVAL, SESSION, TaskMachine, policy};

/// The machine a case's states ask for.
enum Subject {
    /// An instance whose App Server has not started: the case's first
    /// trigger is its `connected`.
    Offline {
        engine: ReferenceServer,
        machine: Option<Machine>,
    },
    /// A live connection without a binding.
    Online {
        engine: ReferenceServer,
        _machine: Machine,
    },
    /// A live connection with [`SESSION`]'s binding [`BINDING`] prepared.
    Prepared(TaskMachine),
    /// The same on a machine that mounts drives, with the binding's drives.
    Mounting(DriveMachine),
}

impl Subject {
    fn engine(&self) -> &ReferenceServer {
        match self {
            Subject::Offline { engine, .. } | Subject::Online { engine, .. } => engine,
            Subject::Prepared(tasks) => &tasks.engine,
            Subject::Mounting(machine) => &machine.engine,
        }
    }

    fn tasks(&self) -> Option<&TaskMachine> {
        match self {
            Subject::Prepared(tasks) => Some(tasks),
            Subject::Mounting(machine) => Some(machine),
            _ => None,
        }
    }
}

/// What a case's `given` asks for, read before any machine starts.
#[derive(Default)]
struct Given {
    /// The symbol of the instance.
    instance: String,
    live: bool,
    mount: bool,
    prepared: Option<GivenBinding>,
}

/// A prepared binding as a case names it: its symbols and its drives, each
/// a drive symbol and its access.
struct GivenBinding {
    session: String,
    binding: String,
    drives: Vec<(String, String)>,
}

fn text<'a>(with: &'a Value, member: &str) -> &'a str {
    with[member]
        .as_str()
        .unwrap_or_else(|| panic!("{member} of {with} is no string"))
}

fn given(case: &Value) -> Given {
    let mut given = Given::default();
    for (index, state) in case["given"].as_array().unwrap().iter().enumerate() {
        let with = &state["with"];
        let members: Vec<&str> = with
            .as_object()
            .map(|members| members.keys().map(String::as_str).collect())
            .unwrap_or_default();
        match text(state, "state") {
            // The harness's instance has one credential and no minimum
            // version.
            "instance" if index == 0 && members == ["instance"] => {
                given.instance = text(with, "instance").to_owned();
            }
            "live_connection" if !given.live => {
                assert!(
                    members
                        .iter()
                        .all(|member| ["instance", "mount", "tools"].contains(member))
                        && with["instance"] == given.instance.as_str()
                        && with["mount"].is_boolean()
                        && [Value::Null, Value::Bool(true)].contains(&with["tools"]),
                    "live_connection with {with} is not implemented"
                );
                given.live = true;
                given.mount = with["mount"] == true;
            }
            "prepared_binding" if given.live && given.prepared.is_none() => {
                assert!(
                    members == ["binding", "drives", "session"],
                    "prepared_binding with {with} is not implemented"
                );
                let drives = with["drives"]
                    .as_array()
                    .expect("drives of a prepared binding")
                    .iter()
                    .map(|drive| {
                        (
                            text(drive, "drive").to_owned(),
                            text(drive, "access").to_owned(),
                        )
                    })
                    .collect();
                given.prepared = Some(GivenBinding {
                    session: text(with, "session").to_owned(),
                    binding: text(with, "binding").to_owned(),
                    drives,
                });
            }
            other => panic!("the state {other} with {with} at {index} is not implemented"),
        }
    }
    given
}

/// The identity this harness gives a drive symbol.
fn drive_of(symbol: &str) -> String {
    format!("drive_{}", symbol.to_lowercase())
}

struct Run {
    subject: Subject,
    bound: Bound,
    directory: PathBuf,
    /// The prepared binding's drives as last sent, by identity.
    drives: Vec<(String, String)>,
    /// How many events of the server's log the case has passed.
    events: usize,
    /// For each endpoint, how many of its accepted messages the case has
    /// passed.
    received: BTreeMap<String, usize>,
}

fn start(record: &mut Case, case: &Value, directory: PathBuf) -> Run {
    let given = given(case);
    let mut bound = Bound::default();
    bound.bind("instance", &given.instance, INSTANCE);
    let mut drives = Vec::new();
    let subject = match (given.live, given.prepared) {
        (false, _) => Subject::Offline {
            engine: one_credential_engine(),
            machine: None,
        },
        (true, None) if !given.mount => {
            let engine = one_credential_engine();
            let machine = Machine::start_with_tools(
                record,
                &directory,
                &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            );
            machine.wait_for("online");
            Subject::Online {
                engine,
                _machine: machine,
            }
        }
        (true, None) => panic!("a mounting connection without a binding is not implemented"),
        (true, Some(prepared)) => {
            bound.bind("session", &prepared.session, SESSION);
            bound.bind("binding", &prepared.binding, BINDING);
            for (symbol, access) in prepared.drives {
                bound.bind("drive", &symbol, &drive_of(&symbol));
                drives.push((drive_of(&symbol), access));
            }
            if given.mount {
                let set: Vec<(&str, &str)> = drives
                    .iter()
                    .map(|(drive, access)| (drive.as_str(), access.as_str()))
                    .collect();
                Subject::Mounting(DriveMachine::start(record, &set))
            } else {
                assert!(
                    drives.is_empty(),
                    "drives without mounting are not implemented"
                );
                Subject::Prepared(TaskMachine::start(record, &directory))
            }
        }
    };
    let mut run = Run {
        subject,
        bound,
        directory,
        drives,
        events: 0,
        received: BTreeMap::new(),
    };
    if given.live {
        let state = run.subject.engine().state();
        let live = state["connections"]
            .as_array()
            .and_then(|connections| connections.iter().find(|conn| conn["live"] == true))
            .expect("the given connection is live");
        run.bound.connection = Some(text(live, "connection_id").to_owned());
    }
    run
}

impl Run {
    /// Puts the case past every event the server has written and every
    /// message it has accepted, so that a trigger's event and the requests
    /// that answer it are the next ones.
    fn pass_what_was_sent(&mut self) {
        let state = self.subject.engine().state();
        self.events = state["events"].as_array().map_or(0, Vec::len);
        self.received.clear();
        for message in state["received"].as_array().into_iter().flatten() {
            *self
                .received
                .entry(text(message, "endpoint").to_owned())
                .or_default() += 1;
        }
    }

    /// Makes the server send an event of type `kind`, by the control that
    /// issues it or by what the App Server does.
    fn trigger(&mut self, record: &mut Case, kind: &str) {
        if let Subject::Offline { engine, machine } = &mut self.subject {
            assert!(
                kind == "connected" && machine.is_none(),
                "the trigger {kind} of an instance without a connection is not implemented"
            );
            let started = Machine::start_with_tools(
                record,
                &self.directory,
                &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            );
            started.wait_for("online");
            *machine = Some(started);
            return;
        }
        self.pass_what_was_sent();
        let engine = self.subject.engine();
        match (kind, self.subject.tasks()) {
            ("heartbeat", _) => engine.advance(HEARTBEAT_INTERVAL),
            ("session.prepare", None) => {
                let project = self.directory.join("project");
                std::fs::create_dir_all(&project).unwrap();
                engine.control(
                    "prepare",
                    json!({
                        "instance_id": INSTANCE,
                        "payload": {
                            "session_id": "sess_case", "binding_id": "bind_case",
                            "cwd": project.canonicalize().unwrap(),
                            "drives": [], "drive_exclude": [], "repositories": [],
                        },
                    }),
                );
            }
            ("session.close", Some(_)) => {
                engine.control(
                    "close",
                    json!({
                        "instance_id": INSTANCE,
                        "payload": { "session_id": SESSION, "binding_id": BINDING },
                    }),
                );
            }
            ("session.drives", Some(tasks)) => {
                // A set that differs from the last one in its first drive's
                // access.
                let (_, access) = self
                    .drives
                    .first_mut()
                    .expect("a session.drives changes a binding that has a drive");
                *access = if access == "read_write" {
                    "read_only"
                } else {
                    "read_write"
                }
                .to_owned();
                let set: Vec<(&str, &str)> = self
                    .drives
                    .iter()
                    .map(|(drive, access)| (drive.as_str(), access.as_str()))
                    .collect();
                tasks.change_drives(SESSION, BINDING, &set);
            }
            ("tool.execute", Some(tasks)) => {
                tasks.execute(
                    "task_case",
                    json!({ "cmd": "true" }),
                    policy(&tasks.project, "disabled"),
                );
            }
            _ => panic!("the trigger {kind} in this state is not implemented"),
        }
    }

    /// The first event of type `kind` that the server wrote after those the
    /// case has passed, as written.
    fn sent(&mut self, kind: &str) -> Value {
        let engine = self.subject.engine();
        let state = engine.state();
        let events = state["events"].as_array().expect("the server's events");
        let index = (self.events..events.len())
            .find(|index| events[*index]["type"] == kind)
            .unwrap_or_else(|| panic!("the server wrote no {kind}: {events:?}"));
        self.events = index + 1;
        let sent = engine.control("sent_event", json!({ "index": index }));
        serde_json::from_str(text(&sent, "data")).expect("an event is JSON")
    }

    /// The next message of `path`'s endpoint that the server accepted.
    fn next_request(&mut self, path: &str) -> Value {
        let endpoint = path
            .strip_prefix("/v1/client/")
            .unwrap_or_else(|| panic!("{path} is no upstream endpoint"));
        let passed = self.received.entry(endpoint.to_owned()).or_default();
        let messages = wait_for_received(self.subject.engine(), endpoint, *passed + 1);
        *passed += 1;
        messages[*passed - 1]["body"].clone()
    }
}

#[test]
fn every_cli_direction_case_holds_for_tetral() {
    let _socket_dir = socket_directory::isolated();
    let record = tetral_tests::record!();
    let cli_cases = cases::of_direction("cli");
    assert!(
        !cli_cases.is_empty(),
        "the case file has CLI-direction cases"
    );
    for case in &cli_cases {
        let name = text(case, "name");
        let mut record = record.case(name);
        let directory = tempfile::tempdir().unwrap();
        let mut run = start(&mut record, case, directory.path().to_owned());
        let (mut triggered, mut requests) = (false, 0);
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let at = format!("{name}, step {}", index + 1);
            let expect = step["expect"]
                .as_object()
                .filter(|expect| {
                    step.as_object().is_some_and(|step| step.len() == 1) && expect.len() == 1
                })
                .unwrap_or_else(|| panic!("{at}: {step} is not implemented"));
            if let Some(event) = expect.get("event") {
                // A case is one trigger and its reaction.
                assert!(!triggered, "{at}: a second trigger is not implemented");
                triggered = true;
                let kind = text(event, "type");
                run.trigger(&mut record, kind);
                let sent = run.sent(kind);
                if let Err(difference) = run.bound.matches("event", event, &sent) {
                    panic!("{at}: {difference}\nin {sent}");
                }
                if kind == "connected" {
                    run.bound.connection = Some(text(&sent, "connection_id").to_owned());
                }
                record.observe(format!("the server sent {sent}"));
            } else if let Some(request) = expect.get("request") {
                assert!(
                    triggered,
                    "{at}: a request before the trigger is not implemented"
                );
                // Every upstream request is a POST of a JSON body, so a
                // request is its path and body here.
                assert!(
                    request.as_object().is_some_and(|request| request
                        .keys()
                        .all(|member| { member == "path" || member == "body" })),
                    "{at}: {request} is not implemented"
                );
                requests += 1;
                let path = text(request, "path");
                let body = run.next_request(path);
                if let Err(difference) = run.bound.holds("body", &request["body"], &body) {
                    panic!("{at}: {difference}\nin {body}");
                }
                record.observe(format!("tetral sent to {path}: {body}"));
            } else {
                panic!("{at}: {step} is not implemented");
            }
        }
        assert!(
            requests > 0,
            "{name}: a case without a request is not implemented"
        );
        record.pass();
    }
}
