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
//! each write that starts on a task's stdin pipe `task.write_started`, each
//! check of an ended task's processes `task.gone_checked`, and each reading
//! of the clock by the timer of the termination procedure's next step
//! `task.ending_checked`, and each end of a task's output pipe
//! `task.output_ended`. The supervisor's observation of the launched
//! process's exit waits at the barrier `task.exit_observed`, and each of its
//! reads of the launch-status pipe before the command's start at
//! `task.launch_status`; a task's availability check waits at
//! `task.availability_check` before it runs, and the admission of a tool
//! instruction of a Session without an owner at `session.unbound_admission`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::app_server::{wait_until, wait_until_within};
use crate::machine::{
    APP, CREDENTIAL, INSTANCE, KEY, Machine, answer_fault, bootstrap, credential, engine, send,
    task_parent, tool_environment, wait_for_received, wait_for_requests,
};
use crate::record::Case;
use crate::reference::ReferenceServer;
use crate::run::{Run, Tetral};

/// The test build's replacement for the `sandbox-exec` that the macOS
/// availability check runs.
pub const SANDBOX_EXEC_VARIABLE: &str = "TETRAL_TEST_SANDBOX_EXEC";

pub const SESSION: &str = "sess_1";
pub const BINDING: &str = "bind_1";

/// The age at which the reference control server of a [`TaskMachine`] ends
/// a stream with a stream-only `disconnect`, set in its startup input.
pub const MAXIMUM_STREAM_AGE: Duration = Duration::from_secs(30 * 60);

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

/// The counter of the readings of the clock by the timer of the
/// termination procedure's next step.
pub const ENDING_CHECKED: &str = "task.ending_checked";

/// The barrier of the supervisor's observation of the launched process's
/// exit.
pub const EXIT_OBSERVED: &str = "task.exit_observed";

/// The counter of the ends of a task's output pipes.
pub const OUTPUT_ENDED: &str = "task.output_ended";

/// The barrier of each read of the launch-status pipe before the command's
/// start.
pub const LAUNCH_STATUS: &str = "task.launch_status";

/// The barrier of the supervisor between the launcher entry's ready newline
/// and the task's record.
pub const RECORD: &str = "task.record";

/// The barrier of a task's availability check before it runs.
pub const AVAILABILITY_CHECK: &str = "task.availability_check";

/// The barrier of the admission of a tool instruction of a Session without
/// an owner, before it runs.
pub const UNBOUND_ADMISSION: &str = "session.unbound_admission";

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
        let engine = task_engine();
        let machine = Machine::start_with_tools(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
        );
        TaskMachine::prepared(engine, machine, directory)
    }

    /// [`TaskMachine::start`] with `arguments` after `server run`'s own.
    pub fn start_with(case: &mut Case, directory: &Path, arguments: &[&str]) -> TaskMachine {
        let engine = task_engine();
        let machine = Machine::start_with_tools_and(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            arguments,
        );
        TaskMachine::prepared(engine, machine, directory)
    }

    /// [`TaskMachine::start`] with `environment` added to the App Server's.
    pub fn start_with_environment(
        case: &mut Case,
        directory: &Path,
        environment: &[(&str, String)],
    ) -> TaskMachine {
        TaskMachine::start_prepared_with_environment(case, directory, environment, &[], |_| {})
    }

    /// [`TaskMachine::start_with_environment`] with `arguments` after
    /// `server run`'s own and `prepare` run on the configured root before
    /// the App Server starts.
    pub fn start_prepared_with_environment(
        case: &mut Case,
        directory: &Path,
        environment: &[(&str, String)],
        arguments: &[&str],
        prepare: impl FnOnce(&Path),
    ) -> TaskMachine {
        let engine = task_engine();
        let mut command_line = vec!["server", "run", "--app", APP];
        command_line.extend_from_slice(arguments);
        let mut server_run =
            Tetral::new(command_line).current_dir(&crate::workspace_dir().join("tests"));
        for (name, value) in tool_environment().iter().chain(environment) {
            server_run = server_run.env(name, value);
        }
        let machine = Machine::start_prepared(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            server_run,
            prepare,
        );
        TaskMachine::prepared(engine, machine, directory)
    }

    /// [`TaskMachine::start`] with `server run` started at a soft limit of
    /// `soft` open descriptors, set by the shell that then becomes it.
    pub fn start_with_soft_descriptor_limit(
        case: &mut Case,
        directory: &Path,
        soft: u64,
    ) -> TaskMachine {
        let engine = task_engine();
        let script = format!("ulimit -Sn {soft} && exec \"$0\" \"$@\"");
        let tetral = crate::binaries::tetral().to_str().unwrap();
        let mut server_run = Tetral::at(
            Path::new("/bin/sh"),
            ["-c", &script, tetral, "server", "run", "--app", APP],
        )
        .current_dir(&crate::workspace_dir().join("tests"));
        for (name, value) in tool_environment() {
            server_run = server_run.env(name, value);
        }
        let machine = Machine::start_as(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            server_run,
        );
        TaskMachine::prepared(engine, machine, directory)
    }

    /// [`TaskMachine::start`] with the App Server run from the `tetral` at
    /// `tetral` with `path` as its `PATH`
    /// ([`Machine::start_with_tools_from`]); a restart runs the default
    /// build.
    pub fn start_from(case: &mut Case, directory: &Path, tetral: &Path, path: &str) -> TaskMachine {
        let engine = task_engine();
        let machine = Machine::start_with_tools_from(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            tetral,
            path,
        );
        TaskMachine::prepared(engine, machine, directory)
    }

    /// [`TaskMachine::start`] with the platform's isolation unusable, so
    /// that each availability check fails: on Linux the App Server's `PATH`
    /// holds no `bwrap`; on macOS the check runs a `sandbox-exec` that exits
    /// 71.
    pub fn start_without_isolation(case: &mut Case, directory: &Path) -> TaskMachine {
        let unusable = directory.join("unusable");
        std::fs::create_dir(&unusable).unwrap();
        if cfg!(target_os = "macos") {
            use std::os::unix::fs::PermissionsExt;
            let sandbox_exec = unusable.join("sandbox-exec");
            std::fs::write(&sandbox_exec, "#!/bin/sh\nexit 71\n").unwrap();
            std::fs::set_permissions(&sandbox_exec, std::fs::Permissions::from_mode(0o755))
                .unwrap();
            let program = sandbox_exec.to_str().unwrap().to_owned();
            TaskMachine::start_with_environment(
                case,
                directory,
                &[(SANDBOX_EXEC_VARIABLE, program)],
            )
        } else {
            TaskMachine::start_from(
                case,
                directory,
                crate::binaries::tetral(),
                unusable.to_str().unwrap(),
            )
        }
    }

    /// [`TaskMachine::start`] on a machine that mounts drives, with
    /// `drives`, each a drive ID and its access, bound by the prepared
    /// binding without the paths of `exclude`. `seed` runs on the reference
    /// control server before the App Server starts. On a Mac the root gets
    /// `tetral drives enable` first.
    pub fn start_with_drives(
        case: &mut Case,
        directory: &Path,
        drives: &[(&str, &str)],
        exclude: &[(&str, &[&str])],
        seed: impl FnOnce(&ReferenceServer),
    ) -> TaskMachine {
        let engine = task_engine();
        seed(&engine);
        let mut server_run = Tetral::new(["server", "run", "--app", APP])
            .current_dir(&crate::workspace_dir().join("tests"));
        for (name, value) in tool_environment() {
            server_run = server_run.env(name, value);
        }
        let machine = Machine::start_prepared(
            case,
            directory,
            &bootstrap(&engine.url, INSTANCE, CREDENTIAL, KEY),
            server_run,
            |root| {
                if cfg!(target_os = "macos") {
                    let run = Tetral::new(["drives", "enable", "--app", APP])
                        .home(root)
                        .run();
                    assert_eq!(run.status.code(), Some(0), "{}", run.exit());
                }
            },
        );
        TaskMachine::prepared_with(engine, machine, directory, drives, exclude)
    }

    fn prepared(engine: ReferenceServer, machine: Machine, directory: &Path) -> TaskMachine {
        TaskMachine::prepared_with(engine, machine, directory, &[], &[])
    }

    fn prepared_with(
        engine: ReferenceServer,
        machine: Machine,
        directory: &Path,
        drives: &[(&str, &str)],
        exclude: &[(&str, &[&str])],
    ) -> TaskMachine {
        machine.wait_for("online");
        // A test may have put files there before the App Server started.
        let project = directory.join("project");
        std::fs::create_dir_all(&project).unwrap();
        let tasks = TaskMachine {
            engine,
            root: machine.root.canonicalize().unwrap(),
            machine: Some(machine),
            project: project.canonicalize().unwrap(),
            controls: AtomicU64::new(0),
        };
        let issued = tasks.prepare_with(SESSION, BINDING, &tasks.project, drives, exclude);
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

    /// Issues `session.prepare` of `session`'s binding `binding` at `cwd`
    /// with `drives`, each a drive ID and its access, and `exclude`, each a
    /// drive ID and its excluded paths, and gives its `message_id`.
    pub fn prepare_with(
        &self,
        session: &str,
        binding: &str,
        cwd: &Path,
        drives: &[(&str, &str)],
        exclude: &[(&str, &[&str])],
    ) -> String {
        let drives: Vec<Value> = drives
            .iter()
            .map(|(drive, access)| json!({ "drive_id": drive, "access": access }))
            .collect();
        let exclude: Vec<Value> = exclude
            .iter()
            .map(|(drive, paths)| json!({ "drive_id": drive, "paths": paths }))
            .collect();
        let issued = self.engine.control(
            "prepare",
            json!({
                "instance_id": INSTANCE,
                "payload": {
                    "session_id": session, "binding_id": binding, "cwd": cwd,
                    "drives": drives, "drive_exclude": exclude, "repositories": [],
                },
            }),
        );
        issued["message_id"].as_str().unwrap().to_owned()
    }

    /// Issues a `session.drives` that gives `binding` of `session` the
    /// drive set `drives`, each a drive ID and its access, and gives its
    /// `message_id`.
    pub fn change_drives(&self, session: &str, binding: &str, drives: &[(&str, &str)]) -> String {
        let drives: Vec<Value> = drives
            .iter()
            .map(|(drive, access)| json!({ "drive_id": drive, "access": access }))
            .collect();
        let issued = self.engine.control(
            "drives",
            json!({
                "instance_id": INSTANCE,
                "payload": { "session_id": session, "binding_id": binding, "drives": drives },
            }),
        );
        issued["message_id"].as_str().unwrap().to_owned()
    }

    /// The accepted Session report that answers `message_id`, once it has
    /// come.
    pub fn session_report(&self, message_id: &str) -> Value {
        let mut found = Value::Null;
        wait_until(
            &format!("the Session report answering {message_id}"),
            || match self
                .engine
                .received("session-status")
                .into_iter()
                .find(|report| report["body"]["request_message_id"] == message_id)
            {
                Some(report) => {
                    found = report["body"].clone();
                    true
                }
                None => false,
            },
        );
        found
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

    /// Issues `mcp.call` of `task_id` of the prepared binding to the `cua`
    /// tool `tool` with `arguments` and gives its `message_id`.
    pub fn mcp_call(&self, task_id: &str, tool: &str, arguments: Value) -> String {
        self.mcp_call_with(
            SESSION,
            BINDING,
            task_id,
            json!({"server": "cua", "tool": tool, "arguments": arguments}),
        )
    }

    /// Issues `mcp.call` of `task_id` of `session`'s binding `binding` with
    /// the payload's `members` beside its identities and gives its
    /// `message_id`.
    pub fn mcp_call_with(
        &self,
        session: &str,
        binding: &str,
        task_id: &str,
        members: Value,
    ) -> String {
        let mut payload = json!({
            "session_id": session, "binding_id": binding,
            "tool_use_event_id": format!("call_{task_id}"), "task_id": task_id,
        });
        payload
            .as_object_mut()
            .unwrap()
            .extend(members.as_object().cloned().unwrap_or_default());
        let issued = self.engine.control(
            "mcp_call",
            json!({"instance_id": INSTANCE, "payload": payload}),
        );
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
        self.report_within(crate::seams::WAIT_LIMIT, task_id, phase)
    }

    /// [`TaskMachine::report`] that waits up to `limit`.
    pub fn report_within(&self, limit: Duration, task_id: &str, phase: &str) -> Value {
        let mut found = Value::Null;
        wait_until_within(
            limit,
            &format!("the {phase} report of {task_id}"),
            || match self
                .reports(task_id)
                .into_iter()
                .find(|report| report["phase"] == phase)
            {
                Some(report) => {
                    found = report;
                    true
                }
                None => false,
            },
        );
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

    /// Moves the App Server's clock by `step`, which is less than a
    /// [`HEARTBEAT_INTERVAL`], with the connection kept online: once the
    /// clock is a heartbeat interval ahead of the reference control
    /// server's, that one moves by the interval and its heartbeat is
    /// answered first.
    pub fn step(&self, step: Duration) {
        let seams = &self.machine().seams;
        assert!(step < HEARTBEAT_INTERVAL, "a step of {step:?}");
        if seams.advanced() + step >= self.engine.advanced() + HEARTBEAT_INTERVAL {
            let heartbeats = self.engine.received("heartbeat").len();
            let grants = seams.count("connection.granted");
            self.engine.advance(HEARTBEAT_INTERVAL);
            wait_for_received(&self.engine, "heartbeat", heartbeats + 1);
            seams.wait_for_count("connection.granted", grants + 1);
        }
        seams.advance(step);
    }

    /// Moves the reference control server's clock by `step`, which is less
    /// than a [`HEARTBEAT_INTERVAL`], and then the App Server's, for what
    /// the server paces by its clock while the App Server times it by its
    /// own: a throttled transfer, the upload budget. A heartbeat the
    /// server's move makes due is answered before the App Server's clock
    /// moves.
    pub fn step_together(&self, step: Duration) {
        let seams = &self.machine().seams;
        assert!(step < HEARTBEAT_INTERVAL, "a step of {step:?}");
        let sent = || -> i64 {
            self.engine.state()["connections"]
                .as_array()
                .expect("connections")
                .iter()
                .map(|connection| connection["heartbeat_seq"].as_i64().expect("a seq"))
                .sum()
        };
        let before = sent();
        let heartbeats = self.engine.received("heartbeat").len();
        let grants = seams.count("connection.granted");
        self.engine.advance(step);
        let due = usize::try_from(sent() - before).expect("heartbeats sent");
        if due > 0 {
            wait_for_received(&self.engine, "heartbeat", heartbeats + due);
            seams.wait_for_count("connection.granted", grants + due as u64);
        }
        seams.advance(step);
    }

    /// Moves the App Server's clock in steps of `step` until `done` holds,
    /// for at most `limit` of the clock, each as [`TaskMachine::step`]. It
    /// is for what the App Server reaches through waits a test cannot see
    /// start, such as the delays between a request's attempts: a wait that
    /// starts after a step measures from the moved clock and ends with a
    /// later step, so each step is followed by a pause in which the App
    /// Server acts on it.
    pub fn advance_until(
        &self,
        what: &str,
        step: Duration,
        limit: Duration,
        mut done: impl FnMut() -> bool,
    ) {
        let mut moved = Duration::ZERO;
        loop {
            let paused = Instant::now();
            while paused.elapsed() < Duration::from_millis(300) {
                if done() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                moved < limit,
                "{what} did not happen within {limit:?} of the App Server's clock"
            );
            self.step(step);
            moved += step;
        }
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

/// The replay ledger's 4,096 entries less the 64 it keeps in reserve: a
/// ledger that holds this many refuses a new operation.
pub const LEDGER_ROOM: usize = 4_032;

/// The entries the ledger keeps in reserve for the controls, closes and
/// reports of what it holds.
pub const LEDGER_RESERVE: usize = 64;

impl TaskMachine {
    /// The accepted report that answers `message_id`, once it has come.
    pub fn answer_to(&self, message_id: &str) -> Value {
        let mut found = Value::Null;
        wait_until(&format!("the report answering {message_id}"), || match self
            .engine
            .received("tool-status")
            .into_iter()
            .find(|report| report["body"]["request_message_id"] == message_id)
        {
            Some(report) => {
                found = report["body"].clone();
                true
            }
            None => false,
        });
        found
    }

    /// Waits for the reply to a read of a task that `binding` of `session`
    /// never had, its control error `task_not_found`: the Session's owner has
    /// then handled every instruction routed to it before.
    pub fn handled(&self, session: &str, binding: &str) {
        let issued = self.engine.control(
            "stdin",
            json!({"instance_id": INSTANCE, "payload": {
                "session_id": session, "binding_id": binding,
                "tool_use_event_id": "call_absent", "task_id": "task_absent", "chars": "",
            }}),
        );
        let reply = self.answer_to(issued["message_id"].as_str().unwrap());
        assert_eq!(reply["error"]["code"], "task_not_found");
    }

    /// Prepares `binding` of `session` at the machine's project with the ACK of
    /// its `prepared` report withheld, so that it stays `prepared_pending_ack`
    /// until the withheld answers are released.
    pub fn prepare_pending(&self, session: &str, binding: &str) {
        answer_fault(
            &self.engine,
            json!({"endpoint": "session-status", "action": "withhold"}),
        );
        let issued = self.prepare(session, binding, &self.project, None);
        self.wait_prepared(&issued);
    }

    pub fn release_answers(&self) {
        answer_fault(&self.engine, json!({"action": "release"}));
    }

    /// Records `closes` closes of Sessions without a record. The reference
    /// server issued none of them, so it refuses each `closed` report
    /// `unknown_instruction`; a refused report acknowledges nothing, so each
    /// close keeps its ledger entry. Each chunk is routed before the next is
    /// sent, so that every wait covers a bounded number.
    pub fn fill_ledger(&self, closes: usize) {
        const CHUNK: usize = 500;
        let seams = &self.machine().seams;
        let connection_id = self.machine().status()["connection_id"].clone();
        let routed = seams.count("route.session");
        for index in 0..closes {
            send(
                &self.engine,
                json!({
                    "protocol_version": 1,
                    "connection_id": connection_id,
                    "message_id": format!("msg_fill_{index}"),
                    "type": "session.close",
                    "payload": { "session_id": format!("sess_fill_{index}"), "binding_id": "bind_1" },
                }),
            );
            if (index + 1) % CHUNK == 0 || index + 1 == closes {
                seams.wait_for_count("route.session", routed + index as u64 + 1);
            }
        }
    }

    /// Ends the current connection with a connection-ending `disconnect`
    /// and waits until the App Server has taken its end.
    pub fn end_connection(&self) {
        let machine = self.machine();
        let connection = machine.status()["connection_id"].clone();
        send(
            &self.engine,
            json!({
                "protocol_version": 1,
                "connection_id": connection,
                "message_id": format!("msg_end_{}", connection.as_str().unwrap()),
                "type": "disconnect",
                "payload": { "scope": "connection", "retryable": true, "code": "stale_connection" },
            }),
        );
        machine.wait_for("disconnected");
    }

    /// Waits for the next connection to be online once the ended one's
    /// lease has passed and the App Server's clock has moved by `backoff`,
    /// and gives its `connection_id`.
    pub fn reconnect(&self, backoff: Duration) -> Value {
        self.engine.advance(PAST_LEASE);
        self.machine().seams.advance(backoff);
        self.machine().wait_for("online")["connection_id"].clone()
    }

    /// Ends the current stream with a stream-only `disconnect` and waits for
    /// the App Server's resume of the connection.
    pub fn resume(&self) {
        let streams = self.engine.requests("events").len();
        send(
            &self.engine,
            json!({
                "protocol_version": 1,
                "connection_id": self.machine().status()["connection_id"],
                "message_id": format!("msg_disconnect_{streams}"),
                "type": "disconnect",
                "payload": { "scope": "stream" },
            }),
        );
        let events = wait_for_requests(&self.engine, "events", streams + 1);
        assert_eq!(events[streams]["query"]["resume"], "true");
    }
}

/// A reference control server that knows the one test credential and ends a
/// stream at [`MAXIMUM_STREAM_AGE`].
fn task_engine() -> ReferenceServer {
    engine(
        &[credential(INSTANCE, CREDENTIAL, KEY, "active")],
        json!({ "max_stream_age_ms": MAXIMUM_STREAM_AGE.as_millis() as u64 }),
    )
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

/// A shell command that writes its process ID to `pid_file` and runs
/// `sleep <seconds>` as the leader of a new session: through the `setsid`
/// utility on Linux, through Python's `os.setsid` on macOS, which has no
/// such utility.
pub fn setsid_sleep(seconds: u32, pid_file: &str) -> String {
    if cfg!(target_os = "macos") {
        format!(
            "python3 -c 'import os; os.setsid(); open(\"{pid_file}\", \"w\").write(str(os.getpid())); os.execvp(\"sleep\", [\"sleep\", \"{seconds}\"])'"
        )
    } else {
        format!("setsid sh -c 'echo $$ > {pid_file}; exec sleep {seconds}'")
    }
}

/// A process that left its task's processes, which the case ends itself:
/// SIGKILL when dropped, a failed assertion included.
pub struct LeftTheTask(pub u32);

impl Drop for LeftTheTask {
    fn drop(&mut self) {
        if let Some(pid) = i32::try_from(self.0)
            .ok()
            .and_then(rustix::process::Pid::from_raw)
        {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
        }
    }
}

pub fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// Whether a process this user can inspect is among the task's processes
/// that its record names by `recorded`: on Linux the PID namespace of that
/// inode, on macOS the process group of that ID.
#[cfg(target_os = "linux")]
pub fn recorded_processes_remain(recorded: u64) -> bool {
    let wanted = std::path::PathBuf::from(format!("pid:[{recorded}]"));
    processes()
        .any(|process| std::fs::read_link(process.join("ns/pid")).is_ok_and(|link| link == wanted))
}

/// Whether a process this user can inspect is among the task's processes
/// that its record names by `recorded`: on Linux the PID namespace of that
/// inode, on macOS the process group of that ID.
#[cfg(target_os = "macos")]
pub fn recorded_processes_remain(recorded: u64) -> bool {
    ps("pgid=")
        .iter()
        .any(|group| group.trim().parse() == Ok(recorded))
}

/// Whether a process of this machine runs with the argument list `argv`.
#[cfg(target_os = "linux")]
pub fn process_running(argv: &[&str]) -> bool {
    let wanted: Vec<u8> = argv
        .iter()
        .flat_map(|argument| [argument.as_bytes(), b"\0"].concat())
        .collect();
    processes().any(|process| {
        std::fs::read(process.join("cmdline")).is_ok_and(|cmdline| cmdline == wanted)
    })
}

/// Whether a process of this machine runs with the argument list `argv`,
/// whose arguments hold no space. A process that has exited and is not yet
/// reaped shows no arguments.
#[cfg(target_os = "macos")]
pub fn process_running(argv: &[&str]) -> bool {
    let wanted = argv.join(" ");
    ps("args=")
        .iter()
        .any(|arguments| arguments.trim() == wanted)
}

/// Whether the launcher entry of a task whose command is `cmd` runs, as a
/// process whose arguments after its program are `__tetral-task exec
/// <directory> <shell> -c <cmd>`.
#[cfg(target_os = "linux")]
pub fn entry_running(cmd: &str) -> bool {
    processes().any(|process| {
        std::fs::read(process.join("cmdline")).is_ok_and(|cmdline| {
            let arguments: Vec<&[u8]> = cmdline.split(|byte| *byte == 0).collect();
            matches!(
                arguments.as_slice(),
                [_, b"__tetral-task", b"exec", _, _, b"-c", command, b""] if *command == cmd.as_bytes()
            )
        })
    })
}

/// Whether the launcher entry of a task whose command is `cmd` runs, as a
/// process whose arguments after its program are `__tetral-task exec
/// <directory> <shell> -c <cmd>`, where the program and the directory hold
/// no space.
#[cfg(target_os = "macos")]
pub fn entry_running(cmd: &str) -> bool {
    let tail = format!(" -c {cmd}");
    ps("args=").iter().any(|arguments| {
        let arguments = arguments.trim();
        arguments.split(' ').nth(1) == Some("__tetral-task")
            && arguments.split(' ').nth(2) == Some("exec")
            && arguments.ends_with(&tail)
    })
}

/// The `/proc` directories of this machine's processes.
#[cfg(target_os = "linux")]
fn processes() -> impl Iterator<Item = PathBuf> {
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
        .map(|entry| entry.path())
}

/// One line of `ps` per process of this machine with the column `column`,
/// at full width.
#[cfg(target_os = "macos")]
fn ps(column: &str) -> Vec<String> {
    let output = std::process::Command::new("/bin/ps")
        .args(["-A", "-ww", "-o", column])
        .output()
        .unwrap();
    assert!(output.status.success(), "ps -o {column} failed");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}
