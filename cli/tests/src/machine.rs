//! One machine of the cross-process tests of the connection to Engine: a
//! state root that `configure` installed and its App Server running on its
//! own seam clock, against a reference control server that knows the test
//! instance's credential, with the helpers that drive and read that server.
//!
//! The Connection owner reports the test seam counters `connection.attempt`
//! at each attempt, `connection.checked` after each check of its times,
//! `connection.granted` for each ACK that sets the connection deadline,
//! `connection.answered` for each answer to ready or a heartbeat it takes,
//! and `route.session` for each Session instruction it hands on; it waits at
//! the barrier `connection.message` between taking a message and acting on
//! the times that have passed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};

use crate::app_server::{AppServer, wait_until};
use crate::record::Case;
use crate::reference::ReferenceServer;
use crate::run::Tetral;
use crate::seams::Seams;

pub const APP: &str = "app_one";
pub const INSTANCE: &str = "inst_one";
pub const CREDENTIAL: &str = "ccred_one";
pub const KEY: &str = "tetral_ck_connection_one";
/// Past every clock of these tests.
pub const EXPIRES_AT: &str = "2099-01-01T00:00:00Z";
/// The reference control server's clock at its start.
pub const ENGINE_START: &str = "2030-01-01T00:00:00Z";
/// The CLI's seam clock at its start.
pub const CLI_START: Duration = Duration::from_secs(1_000);
/// The first connection of a reference control server that just started.
pub const FIRST_CONNECTION: &str = "conn_1";

pub fn seconds(value: f64) -> Duration {
    Duration::from_secs_f64(value)
}

pub fn credential(instance: &str, credential: &str, key: &str, state: &str) -> Value {
    json!({
        "instance_id": instance,
        "credential_id": credential,
        "connection_key": key,
        "expires_at": EXPIRES_AT,
        "state": state,
    })
}

/// A reference control server that knows `credentials`, with `more` of its
/// startup input.
pub fn engine(credentials: &[Value], more: Value) -> ReferenceServer {
    let mut input = json!({ "credentials": credentials, "clock_start": ENGINE_START });
    if let (Some(input), Value::Object(more)) = (input.as_object_mut(), more) {
        input.extend(more);
    }
    ReferenceServer::start(&input)
}

/// A reference control server that knows the one test credential.
pub fn one_credential_engine() -> ReferenceServer {
    engine(
        &[credential(INSTANCE, CREDENTIAL, KEY, "active")],
        json!({}),
    )
}

pub fn bootstrap(server_url: &str, instance: &str, credential: &str, key: &str) -> Value {
    json!({
        "server_url": server_url,
        "instance_id": instance,
        "credential_id": credential,
        "connection_key": key,
        "expires_at": EXPIRES_AT,
    })
}

/// One machine: a state root that `configure` installed and the App Server
/// running on it, on its own seam clock.
pub struct Machine {
    // Killed before its seams stop answering.
    pub app_server: AppServer,
    pub seams: Seams,
    pub root: PathBuf,
}

impl Machine {
    pub fn start(case: &mut Case, directory: &Path, bootstrap: &Value) -> Machine {
        Machine::start_in(
            case,
            directory,
            bootstrap,
            &crate::workspace_dir().join("tests"),
        )
    }

    /// [`Machine::start`] with the App Server's working directory
    /// `working_directory`.
    pub fn start_in(
        case: &mut Case,
        directory: &Path,
        bootstrap: &Value,
        working_directory: &Path,
    ) -> Machine {
        std::fs::create_dir_all(directory).unwrap();
        let root = directory.join("root");
        let run = Tetral::new(["configure", "--app", APP, "--from-stdin"])
            .home(&root)
            .stdin(serde_json::to_vec(bootstrap).unwrap())
            .run();
        case.command("tetral configure --app app_one --from-stdin", &run.exit());
        assert_eq!(run.object(), json!({}));
        let seams = Seams::serve(directory, CLI_START);
        let server_run = Tetral::new(["server", "run", "--app", APP])
            .home(&root)
            .seams(&seams)
            .current_dir(working_directory);
        let app_server = AppServer::start(server_run, &root);
        case.command("tetral server run --app app_one", "running");
        Machine {
            app_server,
            seams,
            root,
        }
    }

    pub fn run(root: &Path, seams: &Seams) -> AppServer {
        let server_run = Tetral::new(["server", "run", "--app", APP])
            .home(root)
            .seams(seams);
        AppServer::start(server_run, root)
    }

    pub fn status(&self) -> Value {
        self.app_server.status()
    }

    /// Waits until the connection is in `state` and gives the status then.
    pub fn wait_for(&self, state: &str) -> Value {
        let mut seen = Value::Null;
        wait_until(&format!("connection_state {state}"), || {
            seen = self.status();
            seen["connection_state"] == state
        });
        seen
    }

    /// Waits until the Connection owner has checked its times at the
    /// clock's current time: of two checks, the second starts after this
    /// call. The owner checks before and after each message it takes, so the
    /// status requests made meanwhile bring checks also when no time is
    /// pending.
    pub fn settle(&self) {
        let start = self.seams.count("connection.checked");
        wait_until("two checks of the Connection owner", || {
            self.status();
            self.seams.count("connection.checked") >= start + 2
        });
    }

    pub fn advance(&self, step: Duration) {
        self.seams.advance(step);
        self.settle();
    }

    pub fn attempts(&self) -> u64 {
        self.seams.count("connection.attempt")
    }

    pub fn routed(&self) -> u64 {
        self.seams.count("route.session")
    }

    pub fn state(&self) -> Value {
        self.status()["connection_state"].clone()
    }
}

/// Waits until the reference control server has `count` requests of
/// `endpoint` and gives them.
pub fn wait_for_requests(engine: &ReferenceServer, endpoint: &str, count: usize) -> Vec<Value> {
    let mut seen = Vec::new();
    wait_until(&format!("{count} {endpoint} requests"), || {
        seen = engine.requests(endpoint);
        seen.len() >= count
    });
    seen
}

/// Waits until the reference control server has accepted `count` messages
/// of `endpoint` and gives them.
pub fn wait_for_received(engine: &ReferenceServer, endpoint: &str, count: usize) -> Vec<Value> {
    let mut seen = Vec::new();
    wait_until(&format!("{count} accepted {endpoint} messages"), || {
        seen = engine.received(endpoint);
        seen.len() >= count
    });
    seen
}

/// Writes `event` on the instance's stream as given.
pub fn send(engine: &ReferenceServer, event: Value) {
    engine.control("send", json!({ "instance_id": INSTANCE, "event": event }));
}

pub fn answer_fault(engine: &ReferenceServer, fault: Value) {
    engine.control("answer", fault);
}
