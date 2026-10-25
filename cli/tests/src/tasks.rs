//! A machine that runs tasks: its App Server online with `tool_execution`
//! against a reference control server, one prepared Session, and the
//! helpers that issue tool instructions and read their reports. Each task's
//! initial wait reports the test seam counter `task.wait_checked` after each
//! reading of the clock, the launcher entry waits at the barrier
//! `task.launch` between its directory's check and entering it (in a task
//! with network), the launch counts `process.spawn`, each control reply
//! that starts to wait counts `task.reply_waiting`, each reading of the
//! clock by the timer of a running task's waiting replies
//! `task.reply_checked`, each read of a task's output `task.output_read`,
//! each write that starts on a task's stdin pipe `task.write_started`, and
//! each check of an ended task's processes `task.gone_checked`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use crate::app_server::wait_until;
use crate::machine::{
    CREDENTIAL, INSTANCE, KEY, Machine, bootstrap, one_credential_engine, task_parent,
    wait_for_received,
};
use crate::record::Case;
use crate::reference::ReferenceServer;
use crate::run::Run;

pub const SESSION: &str = "sess_1";
pub const BINDING: &str = "bind_1";

/// The counter of the initial wait's readings of the clock.
pub const WAIT_CHECKED: &str = "task.wait_checked";

/// The barrier of the launcher entry before it enters its directory.
pub const LAUNCH: &str = "task.launch";

/// The counter of the control replies that start to wait.
pub const REPLY_WAITING: &str = "task.reply_waiting";

/// The counter of the readings of the clock by the timer of a running
/// task's waiting replies.
pub const REPLY_CHECKED: &str = "task.reply_checked";

/// The counter of the reads of a task's output, counted once the bytes are
/// in its capture.
pub const OUTPUT_READ: &str = "task.output_read";

/// The counter of the writes that start on a task's stdin pipe.
pub const WRITE_STARTED: &str = "task.write_started";

/// The counter of the checks of an ended task's processes, counted once
/// each check's result is applied.
pub const GONE_CHECKED: &str = "task.gone_checked";

/// How often the App Server checks an ended task's processes again, on its
/// clock.
pub const GONE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How often the reference control server sends a heartbeat, on its clock.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// How long the reference control server's grant of a connection lasts
/// after its last heartbeat ACK, with a margin.
const PAST_LEASE: Duration = Duration::from_secs(46);

pub struct TaskMachine {
    pub engine: ReferenceServer,
    machine: Option<Machine>,
    /// The canonical state root.
    pub root: PathBuf,
    /// The prepared binding's directory, canonical.
    pub project: PathBuf,
    /// The controls issued, which number their `tool_use_event_id`s.
    controls: AtomicU64,
}

impl TaskMachine {
    /// Starts the machine in `directory` and prepares [`SESSION`]'s binding
    /// [`BINDING`] at `directory/project`.
    pub fn start(case: &mut Case, directory: &Path) -> TaskMachine {
        let engine = one_credential_engine();
        let machine = Machine::start_with_tools(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        );
        machine.wait_for("online");
        let project = directory.join("project");
        std::fs::create_dir(&project).unwrap();
        let tasks = TaskMachine {
            engine,
            root: machine.root.canonicalize().unwrap(),
            machine: Some(machine),
            project: project.canonicalize().unwrap(),
            controls: AtomicU64::new(0),
        };
        let issued = tasks.prepare(SESSION, BINDING, &tasks.project, None);
        tasks.wait_prepared(&issued);
        // The prepared report is accepted before its answer is applied.
        tasks.answers_settled();
        tasks
    }

    pub fn machine(&self) -> &Machine {
        self.machine.as_ref().expect("the machine runs")
    }

    /// Kills the App Server with SIGKILL, runs `between` while none runs,
    /// and starts it again on the same state root and clock; it is online
    /// once this returns.
    pub fn kill_and_restart(&mut self, between: impl FnOnce(&TaskMachine)) {
        let Machine {
            app_server,
            seams,
            root,
        } = self.machine.take().expect("the machine runs");
        app_server.kill();
        between(self);
        // The killed App Server's connection ends at its lease, so that the
        // next one is not refused as a second connection of the instance.
        self.engine.advance(PAST_LEASE);
        let app_server = Machine::run_with_tools(&root, &seams);
        let machine = Machine {
            app_server,
            seams,
            root,
        };
        machine.wait_for("online");
        self.machine = Some(machine);
    }

    /// Waits for the App Server to exit by itself, as after a shutdown, and
    /// gives its run and how long the exit took; the state root stays.
    pub fn exited(&mut self) -> (Run, Duration) {
        let Machine {
            app_server,
            seams,
            root: _,
        } = self.machine.take().expect("the machine runs");
        let exited = app_server.exited();
        // The seams answer until the App Server has exited.
        drop(seams);
        exited
    }

    /// The records of `event` that the App Server has written.
    pub fn records(&self, event: &str) -> Vec<Value> {
        let log = std::fs::read_to_string(self.root.join("logs/tetral.log")).unwrap_or_default();
        log.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|record| record["event"] == event)
            .collect()
    }

    /// Issues `session.prepare` of `session`'s binding `binding` at `cwd`
    /// and gives its `message_id`.
    pub fn prepare(
        &self,
        session: &str,
        binding: &str,
        cwd: &Path,
        traceparent: Option<&str>,
    ) -> String {
        let mut control = json!({
            "instance_id": INSTANCE,
            "payload": {
                "session_id": session, "binding_id": binding, "cwd": cwd,
                "drives": [], "drive_exclude": [], "repositories": [],
            },
        });
        if let Some(traceparent) = traceparent {
            control["traceparent"] = json!(traceparent);
        }
        let issued = self.engine.control("prepare", control);
        issued["message_id"].as_str().unwrap().to_owned()
    }

    /// Waits until the outbox has applied an answer to every report request
    /// that the reference control server has logged, and gives the count of
    /// applied answers: the next answer applied is then a later request's.
    pub fn answers_settled(&self) -> u64 {
        let seams = &self.machine().seams;
        let requests = ["tool-status", "session-status"]
            .iter()
            .map(|endpoint| self.engine.requests(endpoint).len() as u64)
            .sum();
        seams.wait_for_count("outbox.answered", requests);
        seams.count("outbox.answered")
    }

    /// Waits until the `prepared` report that answers `message_id` is
    /// accepted.
    pub fn wait_prepared(&self, message_id: &str) {
        wait_until(&format!("the prepared report of {message_id}"), || {
            self.engine.received("session-status").iter().any(|report| {
                report["body"]["request_message_id"] == message_id
                    && report["body"]["status"] == "prepared"
            })
        });
    }

    /// Issues `exec_command` with `input` as `task_id` of the prepared
    /// binding under `policy` and gives its `message_id`.
    pub fn execute(&self, task_id: &str, input: Value, policy: Value) -> String {
        self.execute_with(SESSION, BINDING, task_id, input, policy, None)
    }

    pub fn execute_with(
        &self,
        session: &str,
        binding: &str,
        task_id: &str,
        input: Value,
        policy: Value,
        traceparent: Option<&str>,
    ) -> String {
        let mut control = json!({
            "instance_id": INSTANCE,
            "payload": {
                "session_id": session, "binding_id": binding,
                "tool_use_event_id": format!("call_{task_id}"), "task_id": task_id,
                "tool": "exec_command", "input": input, "sandbox_policy": policy,
            },
        });
        if let Some(traceparent) = traceparent {
            control["traceparent"] = json!(traceparent);
        }
        let issued = self.engine.control("execute", control);
        issued["message_id"].as_str().unwrap().to_owned()
    }

    /// Issues `tool.stdin` of `task_id` with the payload's `members`
    /// (`chars`, `write_seq`, `wait_ms`, `limits`) and gives its
    /// `message_id`.
    pub fn stdin(&self, task_id: &str, members: Value) -> String {
        self.issue_control("stdin", task_id, members)
    }

    /// Issues `tool.cancel` of `task_id` with the payload's `members` and
    /// gives its `message_id`.
    pub fn cancel(&self, task_id: &str, members: Value) -> String {
        self.issue_control("cancel", task_id, members)
    }

    fn issue_control(&self, kind: &str, task_id: &str, members: Value) -> String {
        let number = self.controls.fetch_add(1, Ordering::SeqCst);
        let mut payload = json!({
            "session_id": SESSION, "binding_id": BINDING,
            "tool_use_event_id": format!("call_{kind}_{number}"), "task_id": task_id,
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(members.as_object().cloned().unwrap_or_default());
        let issued = self
            .engine
            .control(kind, json!({"instance_id": INSTANCE, "payload": payload}));
        issued["message_id"].as_str().unwrap().to_owned()
    }

    /// The accepted control reply that answers `message_id`, if any.
    pub fn accepted_reply(&self, message_id: &str) -> Option<Value> {
        self.engine
            .received("tool-status")
            .into_iter()
            .map(|received| received["body"].clone())
            .find(|body| body["kind"] == "control" && body["request_message_id"] == message_id)
    }

    /// Waits for the accepted control reply that answers `message_id` and
    /// gives it.
    pub fn reply(&self, message_id: &str) -> Value {
        let mut found = Value::Null;
        wait_until(&format!("the reply to {message_id}"), || {
            match self.accepted_reply(message_id) {
                Some(reply) => {
                    found = reply;
                    true
                }
                None => false,
            }
        });
        found
    }

    /// The accepted execution reports of `task_id`, in arrival order.
    pub fn reports(&self, task_id: &str) -> Vec<Value> {
        self.engine
            .received("tool-status")
            .into_iter()
            .map(|received| received["body"].clone())
            .filter(|body| body["task_id"] == task_id && body["kind"] == "execution")
            .collect()
    }

    /// Waits for the first accepted execution report of `task_id` in
    /// `phase` and gives it.
    pub fn report(&self, task_id: &str, phase: &str) -> Value {
        let mut found = Value::Null;
        wait_until(&format!("the {phase} report of {task_id}"), || {
            match self
                .reports(task_id)
                .into_iter()
                .find(|report| report["phase"] == phase)
            {
                Some(report) => {
                    found = report;
                    true
                }
                None => false,
            }
        });
        found
    }

    pub fn terminal(&self, task_id: &str) -> Value {
        self.report(task_id, "terminal")
    }

    /// Waits until the next task's initial wait has read the clock, which
    /// shows that the task has started at the clock's current time.
    pub fn wait_for_start(&self, checked: u64) {
        self.machine()
            .seams
            .wait_for_count(WAIT_CHECKED, checked + 1);
    }

    /// Moves the App Server's clock by `step` with its connection kept
    /// online: for each [`HEARTBEAT_INTERVAL`] of it the reference control
    /// server's clock moves too, and the ACK of its heartbeat moves the
    /// connection's deadline.
    pub fn advance(&self, step: Duration) {
        let seams = &self.machine().seams;
        let mut left = step;
        while left >= HEARTBEAT_INTERVAL {
            let heartbeats = self.engine.received("heartbeat").len();
            let grants = seams.count("connection.granted");
            self.engine.advance(HEARTBEAT_INTERVAL);
            wait_for_received(&self.engine, "heartbeat", heartbeats + 1);
            seams.wait_for_count("connection.granted", grants + 1);
            seams.advance(HEARTBEAT_INTERVAL);
            left -= HEARTBEAT_INTERVAL;
        }
        seams.advance(left);
    }

    /// [`TaskMachine::advance`], then waits until a running initial wait
    /// has read the new time.
    pub fn advance_wait(&self, step: Duration) {
        let seams = &self.machine().seams;
        self.advance(step);
        let checked = seams.count(WAIT_CHECKED);
        seams.wait_for_count(WAIT_CHECKED, checked + 2);
    }

    /// Waits until the Session `session`'s task directories and the entries
    /// of `T` are gone. An ended task's files go only once no process is left
    /// in its PID namespace, which the App Server checks again every
    /// [`GONE_CHECK_INTERVAL`] of its clock and shows by deleting the task's
    /// record under `execution/`; while a record is there, the clock moves
    /// one interval and the next check runs.
    pub fn wait_files_removed(&self, session: &str) {
        let seams = &self.machine().seams;
        let records = self.root.join("execution");
        loop {
            let checked = seams.count(GONE_CHECKED);
            if listing(&records).is_empty() {
                break;
            }
            self.advance(GONE_CHECK_INTERVAL);
            wait_until("the next check of an ended task's processes", || {
                seams.count(GONE_CHECKED) > checked || listing(&records).is_empty()
            });
        }
        wait_until(&format!("the files of {session}'s tasks removed"), || {
            self.task_directories(session).is_empty() && self.temporary_directories().is_empty()
        });
    }

    /// The directories of the Session `session`'s current tasks.
    pub fn task_directories(&self, session: &str) -> Vec<PathBuf> {
        listing(&self.root.join("sessions").join(session).join("tasks"))
    }

    /// The entries of `T`.
    pub fn temporary_directories(&self) -> Vec<PathBuf> {
        listing(&task_parent(&self.root))
    }
}

impl Drop for TaskMachine {
    /// Stops the App Server, then removes `T`, which lies outside the test's
    /// directory.
    fn drop(&mut self) {
        drop(self.machine.take());
        let _ = std::fs::remove_dir_all(task_parent(&self.root));
    }
}

fn listing(directory: &Path) -> Vec<PathBuf> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory)
        .map(|entries| entries.flatten().map(|entry| entry.path()).collect())
        .unwrap_or_default();
    entries.sort();
    entries
}

/// Read access to the whole filesystem, write access to `project` and the
/// network mode `network`.
pub fn policy(project: &Path, network: &str) -> Value {
    json!({
        "schema_version": 1,
        "filesystem": {"rules": [
            {"path": "/", "access": "read"},
            {"path": project, "access": "read_write"},
        ]},
        "network": {"mode": network},
    })
}

pub fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// Whether a process of this machine runs with the argument list `argv`.
pub fn process_running(argv: &[&str]) -> bool {
    let wanted: Vec<u8> = argv
        .iter()
        .flat_map(|argument| [argument.as_bytes(), b"\0"].concat())
        .collect();
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .bytes()
                .all(|byte| byte.is_ascii_digit())
        })
        .any(|entry| {
            std::fs::read(entry.path().join("cmdline")).is_ok_and(|cmdline| cmdline == wanted)
        })
}
