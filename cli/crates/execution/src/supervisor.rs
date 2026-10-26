//! The Task supervisor: the one owner of a task and its process tree, from
//! the task's admission until its Session binding owner forgets it. A task
//! is `staged` until the binding owner starts it ([`SupervisorMessage::Start`]);
//! it is then `starting` while its supervisor checks the isolation, creates
//! the task's files and launches the command; `running` from the command's
//! actual start, while the supervisor reads both pipes into the task's
//! capture and writes its stdin; `ended` once the shell's exit has been
//! observed, or given up on, and the `terminal` report has been handed to the
//! outbox; and `cleaned` once every report of the task is acknowledged or
//! dropped and the task's files are gone. The supervisor answers the task's
//! controls in every state but `staged`, whose `tool.stdin` wait for the
//! start or the cancel that prevents it: each waiting reply is a timer and a
//! waiter of the supervisor, which meanwhile keeps reading output and
//! handling the exit and other controls. Every wait reads the seam clock.
//! A file tool's task runs its child instead of a command
//! ([`crate::file_tools`]): `running` from the spawn, its `terminal` report
//! is made once the child's result is received, or once its child is gone
//! without one, and a cancel ends the child through the termination
//! procedure.
//!
//! Once the task's first process inside its sandbox exists, the supervisor
//! writes the task's record ([`cleanup::Record`]) and tells its binding
//! owner the task's PID namespace. After the `terminal` report the task's
//! processes are checked every [`cleanup::CHECK_INTERVAL`] until they are
//! confirmed gone; only then do the record and, once every report has its
//! outcome, the task's files go. A connection's end discards a staged task
//! without a report and ends a running one with SIGKILL at once; the App
//! Server's shutdown ends it through the termination procedure.

use std::collections::{HashSet, VecDeque};
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use observe::{Identity, TraceContext};
use protocol::field::PositiveInt;
use protocol::id::Id;
use protocol::ports::{ReportId, ReportListener, ReportOutcome, ReportSink};
use protocol::tool::{
    CancelPayload, ControlErrorCode, HelperEnvelope, HelperStatus, HelperTool, Metrics,
    ProtocolError, ReportPhase, StdinPayload, StreamSnapshot, ToolError, ToolInput,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::pipe::Receiver;
use tokio::process::ChildStdin;
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::cancel::Receipt;
use crate::capture::StreamCapture;
use crate::cleanup;
use crate::exec::{self, ExecCall, ExitFacts, OnExpiry, Outcome};
use crate::file_tools::{FileCall, Pipes};
use crate::isolation::{Command, Isolation, LaunchError, Process};
use crate::limits::Visible;
use crate::policy::{PolicyRejection, TaskPermissions};
use crate::process::{self, Leader};
use crate::report::{self, Body, Correlation, ErrorBounds, Rendering, Report, StreamOutput};
use crate::stdin::{self, Check, Request, WriteRecords};
use crate::store::{Log, TaskFiles};
use crate::terminate::{self, Termination};

/// How long the pipes are still read after the shell's exit, while a
/// process it left holds them open.
pub const OUTPUT_DRAIN: Duration = Duration::from_secs(2);

/// The most bytes that one read of an output pipe takes.
const READ_CHUNK_BYTES: usize = 65_536;

/// The most bytes of the launch-status pipe that the supervisor keeps: one
/// failure object of the launcher entry.
const LAUNCH_STATUS_MAX_BYTES: usize = 65_536;

/// The test seam counter of each reading of the clock by the initial wait
/// of a command that has started.
const WAIT_CHECKED: &str = "task.wait_checked";

/// The test seam counter of each reply that starts to wait.
const REPLY_WAITING: &str = "task.reply_waiting";

/// The test seam counter of each reading of the clock by the timer of the
/// waiting replies of a running task.
const REPLY_CHECKED: &str = "task.reply_checked";

/// The test seam counter of each read of output, counted once its bytes are
/// in the capture and before any reply they end.
const OUTPUT_READ: &str = "task.output_read";

/// The test seam counter of each write that starts on the pipe, counted
/// once its deadline is set.
const WRITE_STARTED: &str = "task.write_started";

/// The test seam counter of each check of an ended task's processes,
/// counted once its result is applied.
const GONE_CHECKED: &str = "task.gone_checked";

/// The test seam counter of each reading of the clock by the timer of the
/// termination procedure's next step.
const ENDING_CHECKED: &str = "task.ending_checked";

/// The test seam barrier that holds the supervisor's observation of the
/// launched process's exit, so that a test can end a task whose exit is
/// never seen.
const EXIT_OBSERVED: &str = "task.exit_observed";

/// The test seam barrier that holds each read of the launch-status pipe
/// before the command's start, so that a test can have a launch failure
/// arrive after the exit and the end of both output pipes.
const LAUNCH_STATUS: &str = "task.launch_status";

/// The test seam counter of each end of an output pipe.
const OUTPUT_ENDED: &str = "task.output_ended";

/// The message of every failure to start other than a directory's.
const NOT_STARTED: &str = "process could not start";

/// The message of a command whose start did not come within its initial
/// wait.
const NOT_STARTED_IN_WAIT: &str = "process did not start within the wait";

const STDIN_CLOSED: &str = "task stdin is closed or not accepting input";

/// One admitted task, as its supervisor starts it.
pub struct TaskSpec {
    pub connection_id: Id,
    pub correlation: Correlation,
    pub tool: ToolInput,
    pub permissions: TaskPermissions,
    pub limits: Visible,
    /// The binding's canonical working directory.
    pub cwd: PathBuf,
    pub files: TaskFiles,
    pub traceparent: Option<String>,
    /// When the instruction was received, on the seam clock.
    pub received: Duration,
    /// Where the task's record goes ([`cleanup::record_path`]).
    pub record: PathBuf,
    pub listener: Arc<dyn TaskListener>,
}

/// What a supervisor tells its task's binding owner about the task's
/// processes.
pub trait TaskListener: Send + Sync {
    /// The task's first process inside its sandbox exists, in the PID
    /// namespace whose inode is `namespace`.
    fn started(&self, namespace: u64);
    /// The task's processes are confirmed gone, or it never had any.
    fn gone(&self);
}

/// What every supervisor of an App Server shares.
#[derive(Clone)]
pub struct Context {
    /// The running `tetral`, canonical.
    pub tetral: PathBuf,
    pub isolation: Isolation,
    pub reports: Arc<dyn ReportSink>,
    pub bounds: Bounds,
    /// The running system's boot ID, which each record carries; without it
    /// no record is written.
    pub boot_id: Option<String>,
}

/// The waits of a supervisor, each a constant but for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    pub output_drain: Duration,
    pub termination: terminate::Steps,
    pub stdin_write: Duration,
}

impl Default for Bounds {
    fn default() -> Bounds {
        Bounds {
            output_drain: OUTPUT_DRAIN,
            termination: terminate::Steps::default(),
            stdin_write: stdin::WRITE_TIMEOUT,
        }
    }
}

pub enum SupervisorMessage {
    /// The binding owner starts the staged task.
    Start,
    /// An admitted `tool.stdin` of the task.
    Stdin(Box<Control<StdinPayload>>),
    /// An admitted `tool.cancel` of the task.
    Cancel(Box<Control<CancelPayload>>),
    /// The outbox's outcome of one of the task's reports.
    Outcome(ReportId, ReportOutcome),
    /// The task's connection ended.
    ConnectionEnded,
    /// The App Server shuts down.
    Shutdown,
}

/// A control of the task, admitted by its binding owner on the task's
/// connection.
pub struct Control<P> {
    pub connection_id: Id,
    /// The control's own identities, which its reply answers.
    pub correlation: Correlation,
    pub traceparent: Option<String>,
    pub payload: P,
}

pub type SupervisorMailbox = mpsc::UnboundedSender<SupervisorMessage>;

/// Posts report outcomes to a supervisor without keeping its mailbox open:
/// a supervisor ends once its binding owner drops the task's mailbox.
struct Listener(mpsc::WeakUnboundedSender<SupervisorMessage>);

impl ReportListener for Listener {
    fn outcome(&self, report: ReportId, outcome: ReportOutcome) {
        if let Some(mailbox) = self.0.upgrade() {
            let _ = mailbox.send(SupervisorMessage::Outcome(report, outcome));
        }
    }
}

/// Starts the supervisor of the admitted task `spec`, `staged` until its
/// mailbox gets [`SupervisorMessage::Start`]; it runs inside its
/// instruction's span.
pub fn spawn(spec: TaskSpec, context: Context) -> SupervisorMailbox {
    let (mailbox, messages) = mpsc::unbounded_channel();
    let trace = spec.traceparent.as_deref().and_then(TraceContext::parse);
    let correlation = &spec.correlation;
    let span = observe::correlation_span(
        &[
            (Identity::Session, correlation.session_id.as_str()),
            (Identity::Binding, correlation.binding_id.as_str()),
            (Identity::Connection, spec.connection_id.as_str()),
            (Identity::Message, correlation.request_message_id.as_str()),
            (
                Identity::ToolUseEvent,
                correlation.tool_use_event_id.as_str(),
            ),
            (Identity::Task, correlation.task_id.as_str()),
        ],
        trace.as_ref(),
    );
    let supervisor = Supervisor {
        spec,
        context,
        listener: Arc::new(Listener(mailbox.downgrade())),
        messages,
        awaited: HashSet::new(),
        report_seq: 0,
        left_queue: None,
        process: None,
        spawned: Duration::ZERO,
        start: None,
        out: Stream::new(None::<tokio::process::ChildStdout>),
        err: Stream::new(None::<tokio::process::ChildStderr>),
        stdin: None,
        ending: None,
        exit: None,
        ended: None,
        cleaned: false,
        kept: (exec::empty_stream(), exec::empty_stream()),
        cancelled: false,
        records: WriteRecords::default(),
        writes: VecDeque::new(),
        writing: None,
        waiters: Vec::new(),
        stopped: false,
        shutting_down: false,
        leader: None,
        gone: false,
        checked: Duration::ZERO,
    };
    tokio::spawn(supervisor.run().instrument(span));
    mailbox
}

struct Supervisor {
    spec: TaskSpec,
    context: Context,
    listener: Arc<Listener>,
    messages: mpsc::UnboundedReceiver<SupervisorMessage>,
    /// The reports whose outcome the cleanup waits for.
    awaited: HashSet<ReportId>,
    /// The `report_seq` of the task's last execution report.
    report_seq: i64,
    /// When the task left admission and its binding's staging queue.
    left_queue: Option<Duration>,
    /// The launched command, until its supervision ends.
    process: Option<Process>,
    spawned: Duration,
    /// The command's actual start.
    start: Option<Duration>,
    out: Stream,
    err: Stream,
    /// The command's stdin, until it closes for good.
    stdin: Option<ChildStdin>,
    ending: Option<(Termination, Expiry)>,
    /// The shell's exit facts and when they were observed.
    exit: Option<(ExitFacts, Duration)>,
    /// What the `terminal` report said, once it was made.
    ended: Option<State>,
    cleaned: bool,
    /// The streams' counters once the task is cleaned.
    kept: (StreamSnapshot, StreamSnapshot),
    cancelled: bool,
    records: WriteRecords,
    /// The writes that wait for the one being written.
    writes: VecDeque<Control<StdinPayload>>,
    writing: Option<Writing>,
    waiters: Vec<Waiter>,
    /// A connection's end or the shutdown stopped the task's writes.
    stopped: bool,
    /// The App Server's shutdown has begun: the end of the connection that
    /// it brings ends the task through the termination procedure, with its
    /// grace.
    shutting_down: bool,
    /// The task's first process inside its sandbox, once seen.
    leader: Option<Leader>,
    /// Whether the task's processes are confirmed gone.
    gone: bool,
    /// When they were last checked.
    checked: Duration,
}

/// What a task runs.
enum Work {
    Exec(ExecCall),
    File(FileCall),
}

impl Work {
    fn of(tool: &ToolInput, cwd: &std::path::Path) -> Work {
        match (tool, FileCall::of(tool)) {
            (ToolInput::ExecCommand(input), _) => Work::Exec(ExecCall::new(input, cwd)),
            (_, Some(call)) => Work::File(call),
            (_, None) => unreachable!("every tool but exec_command is a file tool"),
        }
    }
}

/// Why a task did not reach `running`.
enum Start {
    /// The check or the transform refused the call; no process started.
    Rejected(PolicyRejection),
    /// The start failed; the `terminal` report carries this error.
    Failed(ToolError),
}

/// Why the supervisor ended a running command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expiry {
    /// The initial wait ended before the command's start.
    NotStarted,
    /// The initial wait ended under `kill`.
    Wait,
    /// A detached command's lifetime passed.
    Lifetime,
    /// A `tool.cancel`.
    Cancel,
    /// The task's connection ended.
    ConnectionEnded,
    /// The App Server shuts down.
    Shutdown,
}

/// A task as a reply reports it.
#[derive(Debug, Clone, Default)]
struct State {
    /// Whether the task has ended; its exit facts are then final.
    ended: bool,
    exit: ExitFacts,
    timed_out: bool,
    duration: Duration,
}

/// The write being written, with the bytes written so far.
struct Writing {
    control: Control<StdinPayload>,
    write_seq: i64,
    digest: [u8; 32],
    wait: Duration,
    offset: usize,
    deadline: Duration,
}

/// A reply that waits.
struct Waiter {
    control: Control<StdinPayload>,
    deadline: Duration,
    /// `None` for a read, which also ends at new output; a write's bytes.
    written: Option<u64>,
}

/// How the staged state ended.
enum Staged {
    Started,
    Cancelled,
    /// The binding owner forgot the task, its connection ended or the App
    /// Server shuts down: it ends without a report.
    Discarded,
}

impl Supervisor {
    async fn run(mut self) {
        match self.staged().await {
            Staged::Discarded => {
                self.confirm_gone();
                return;
            }
            Staged::Cancelled => {}
            Staged::Started => match self.launch(Work::of(&self.spec.tool, &self.spec.cwd)).await {
                Err(Start::Rejected(rejection)) => {
                    self.submit(
                        self.spec.correlation.clone(),
                        &self.spec.connection_id.clone(),
                        self.spec.traceparent.clone(),
                        Body::Rejected(ProtocolError {
                            code: rejection.code,
                            message: rejection.message,
                        }),
                    );
                    self.ended = Some(State {
                        ended: true,
                        ..State::default()
                    });
                }
                Err(Start::Failed(error)) => self.report_start_failure(error),
                Ok((Work::Exec(call), status)) => self.running(call, status).await,
                Ok((Work::File(call), status)) => self.file_running(call, status).await,
            },
        }
        self.settle().await;
    }

    /// `staged`: the task's `tool.stdin` wait, until the start or a cancel
    /// that prevents it.
    async fn staged(&mut self) -> Staged {
        let mut held = Vec::new();
        let staged = loop {
            match self.messages.recv().await {
                None
                | Some(SupervisorMessage::ConnectionEnded)
                | Some(SupervisorMessage::Shutdown) => return Staged::Discarded,
                Some(SupervisorMessage::Start) => {
                    self.left_queue = Some(state::seams::now());
                    break Staged::Started;
                }
                Some(SupervisorMessage::Cancel(control)) => {
                    self.left_queue = Some(state::seams::now());
                    self.cancel(*control);
                    break Staged::Cancelled;
                }
                Some(SupervisorMessage::Stdin(control)) => held.push(*control),
                Some(SupervisorMessage::Outcome(report, _)) => {
                    self.awaited.remove(&report);
                }
            }
        };
        for control in held {
            self.stdin(control);
        }
        staged
    }

    /// `ended` and `cleaned`: controls are answered from what the task
    /// left; the task's processes are checked until they are confirmed
    /// gone, and its files go once they are and every report has its
    /// outcome.
    async fn settle(&mut self) {
        let mut open = true;
        self.check_gone();
        loop {
            if !self.cleaned && self.gone && self.awaited.is_empty() {
                self.clean();
            }
            if !open && self.gone {
                break;
            }
            let next_check = self.checked + cleanup::CHECK_INTERVAL;
            tokio::select! {
                message = self.messages.recv(), if open => match message {
                    None => open = false,
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::Stdin(control)) => self.stdin(*control),
                    Some(SupervisorMessage::Cancel(control)) => self.cancel(*control),
                    Some(
                        SupervisorMessage::Start
                        | SupervisorMessage::ConnectionEnded
                        | SupervisorMessage::Shutdown,
                    ) => {}
                },
                () = state::seams::sleep_until(next_check), if !self.gone => {
                    self.check_gone();
                }
            }
        }
        if !self.cleaned {
            self.clean();
        }
    }

    /// Confirms the task's processes gone when, on Linux, no process is in
    /// its PID namespace; a task whose namespace was never seen is gone
    /// once its launched process is reaped.
    fn check_gone(&mut self) {
        self.checked = state::seams::now();
        let gone = match self.leader {
            Some(leader) => process::namespace_empty(leader.namespace),
            None => self.process.as_mut().is_none_or(|process| {
                process
                    .child
                    .try_wait()
                    .is_ok_and(|status| status.is_some())
            }),
        };
        if gone {
            self.confirm_gone();
        }
        state::seams::count(GONE_CHECKED);
    }

    fn confirm_gone(&mut self) {
        if self.gone {
            return;
        }
        self.gone = true;
        cleanup::remove(&self.spec.record);
        self.spec.listener.gone();
    }

    /// Notes the task's first process inside its sandbox once it exists:
    /// writes the task's record and tells the binding owner its namespace.
    fn find_leader(&mut self, pid: u32) {
        if self.leader.is_some() {
            return;
        }
        let Some(leader) = process::leader(pid) else {
            return;
        };
        self.leader = Some(leader);
        if let Some(boot_id) = &self.context.boot_id {
            let record =
                cleanup::Record::new(self.spec.correlation.session_id.as_str(), boot_id, leader);
            let _ = record.write(&self.spec.record);
        }
        self.spec.listener.started(leader.namespace);
    }

    /// Removes the task's files and lets go of its output, keeping only the
    /// streams' counters for a repeated write's reply.
    fn clean(&mut self) {
        let _ = self.spec.files.remove();
        self.kept = (self.out.capture.counters(), self.err.capture.counters());
        self.out.capture = StreamCapture::new();
        self.err.capture = StreamCapture::new();
        self.cleaned = true;
    }

    /// `starting`: the availability check, the task's files, the
    /// environment and the launch; gives the work and the launch-status
    /// pipe.
    async fn launch(&mut self, work: Work) -> Result<(Work, Receiver), Start> {
        let context = &self.context;
        let spec = &self.spec;
        let failed = |message: &str| Start::Failed(start_error(&work, message));
        let available = context
            .isolation
            .check(&spec.permissions, &spec.cwd, &context.tetral)
            .await
            .map_err(Start::Rejected)?;
        spec.files
            .create()
            .map_err(|failure| failed(&failure.to_string()))?;
        let call_env = match &work {
            Work::Exec(call) => call.env.as_ref(),
            Work::File(_) => None,
        };
        let mut env =
            crate::env::command_environment(std::env::vars_os(), call_env, spec.files.temporary())
                .map_err(|_| failed(NOT_STARTED))?;
        // A launcher entry without network cannot reach the test harness,
        // since Codex's seccomp refuses its `connect`; it gets none.
        if spec
            .permissions
            .profile()
            .network_sandbox_policy()
            .is_enabled()
            && let Some((name, value)) = state::seams::harness_variable()
        {
            env.insert(name.to_owned(), value.to_string_lossy().into_owned());
        }
        let (status, status_writer) = std::io::pipe().map_err(|_| failed(NOT_STARTED))?;
        let mut argv = vec![context.tetral.display().to_string()];
        match &work {
            Work::Exec(call) => argv.extend(
                crate::child::exec::arguments(&call.directory, exec::shell(), &call.cmd)
                    .into_iter()
                    .map(|argument| argument.to_string_lossy().into_owned()),
            ),
            Work::File(call) => argv.extend(call.arguments()),
        }
        let command = Command {
            argv,
            env,
            directory: None,
        };
        let mut process = context
            .isolation
            .launch(
                &available,
                command,
                &spec.permissions,
                &spec.cwd,
                &context.tetral,
                Some(OwnedFd::from(status_writer)),
            )
            .await
            .map_err(|error| match error {
                LaunchError::Rejected(rejection) => Start::Rejected(rejection),
                LaunchError::Spawn(_) => failed(NOT_STARTED),
            })?;
        self.spawned = state::seams::now();
        let status =
            Receiver::from_owned_fd(OwnedFd::from(status)).map_err(|_| failed(NOT_STARTED))?;
        if let Work::Exec(_) = work {
            self.stdin = process.child.stdin.take();
            self.out.reader = process
                .child
                .stdout
                .take()
                .map(|reader| Box::new(reader) as _);
            self.err.reader = process
                .child
                .stderr
                .take()
                .map(|reader| Box::new(reader) as _);
        }
        self.process = Some(process);
        Ok((work, status))
    }

    /// The `terminal` report of a command that never started: `status:
    /// error`, null exit facts and empty streams; of a file tool, its
    /// error.
    fn report_start_failure(&mut self, error: ToolError) {
        if let Some(call) = FileCall::of(&self.spec.tool) {
            self.report_file(crate::child::failed(call.tool(), error), Duration::ZERO);
            return;
        }
        let outcome = self.outcome(HelperStatus::Error, Some(error), ExitFacts::default());
        let empty = StreamCapture::new();
        self.report(ReportPhase::Terminal, outcome, &empty, &empty);
        self.ended = Some(State {
            ended: true,
            ..State::default()
        });
    }

    /// From the spawn, through the command's start on the launch-status
    /// pipe and `running`, until the `terminal` report. A sandbox that ends
    /// with neither the start nor a failure on the pipe ended before the
    /// launcher entry ran, as an error of Codex's sandbox stages ends it:
    /// its exit status and output are the command's result.
    async fn running(&mut self, call: ExecCall, status: Receiver) {
        let bounds = self.context.bounds;
        let pid = self.process.as_ref().map_or(0, |process| process.pid);
        let mut status = Some(status);
        let mut failure = Vec::new();
        let wait_ends = self.spawned + call.wait;
        let mut waited = false;
        let mut forgotten = false;
        self.next_write();
        loop {
            // A launch failure is written before the sandbox exits, but may
            // be read after its exit and the end of both output pipes.
            if self.exit.is_some() && !self.out.is_open() && !self.err.is_open() && status.is_none()
            {
                break;
            }
            let lifetime_ends = self
                .start
                .map_or(Duration::MAX, |start| start + call.lifetime);
            let ending_due = self
                .ending
                .map_or(Duration::MAX, |(termination, _)| termination.due());
            // After SIGKILL the pipes are read only until the wait for the
            // exit ends, so that no report comes later than that.
            let drain_ends = self.exit.as_ref().map_or(Duration::MAX, |(_, at)| {
                let drained = *at + bounds.output_drain;
                match self.ending {
                    Some((termination, _)) if termination.killed() => {
                        drained.min(termination.due())
                    }
                    _ => drained,
                }
            });
            let write_due = self
                .writing
                .as_ref()
                .map_or(Duration::MAX, |writing| writing.deadline);
            let reply_due = self.reply_due();
            let detached = call.on_expiry == OnExpiry::Detach;
            let quiet = self.exit.is_none() && self.ending.is_none();
            let started = self.start.is_some();
            if self.ending.is_some() {
                self.find_leader(pid);
            }
            tokio::select! {
                read = self.out.read(), if self.out.is_open() => {
                    if self.out.take(read, &mut self.spec.files.stdout) {
                        self.output_arrived();
                    }
                }
                read = self.err.read(), if self.err.is_open() => {
                    if self.err.take(read, &mut self.spec.files.stderr) {
                        self.output_arrived();
                    }
                }
                read = read_status(status.as_mut(), started), if status.is_some() => match read {
                    Some(bytes) => {
                        let mut bytes = &bytes[..];
                        if self.start.is_none()
                            && failure.is_empty()
                            && let Some(rest) = bytes.strip_prefix(crate::child::STARTED)
                        {
                            self.start = Some(state::seams::now());
                            self.find_leader(pid);
                            bytes = rest;
                        }
                        let room = LAUNCH_STATUS_MAX_BYTES.saturating_sub(failure.len());
                        failure.extend_from_slice(&bytes[..bytes.len().min(room)]);
                    }
                    None => status = None,
                },
                waited_status = wait_child(self.process.as_mut()), if self.exit.is_none() => {
                    let facts = waited_status.map_or_else(|_| ExitFacts::default(), ExitFacts::observed);
                    self.exit = Some((facts, state::seams::now()));
                    self.exited();
                }
                () = initial_wait(wait_ends, started), if !waited && quiet => {
                    waited = true;
                    match (self.start, call.on_expiry) {
                        (None, _) => {
                            self.ending = Some((Termination::begin(pid, bounds.termination), Expiry::NotStarted));
                        }
                        (Some(_), OnExpiry::Kill) => {
                            self.ending = Some((Termination::begin(pid, bounds.termination), Expiry::Wait));
                        }
                        (Some(start), OnExpiry::Detach) => self.report_running(start),
                    }
                }
                () = state::seams::sleep_until(lifetime_ends), if detached && started && quiet => {
                    self.ending = Some((Termination::begin(pid, bounds.termination), Expiry::Lifetime));
                }
                () = state::seams::sleep_until_counted(ending_due, ENDING_CHECKED), if self.exit.is_none() && self.ending.is_some() => {
                    let Some((termination, expiry)) = self.ending else { break };
                    match termination.step() {
                        Some(next) => self.ending = Some((next, expiry)),
                        None => break,
                    }
                }
                () = state::seams::sleep_until(drain_ends), if self.exit.is_some() => break,
                written = write_some(&mut self.stdin, self.writing.as_ref()), if self.writing.is_some() => {
                    self.wrote(written);
                }
                () = state::seams::sleep_until(write_due), if self.writing.is_some() => {
                    self.write_failed();
                }
                () = state::seams::sleep_until_counted(reply_due, REPLY_CHECKED), if !self.waiters.is_empty() => {
                    self.expire_waiters();
                }
                message = self.messages.recv(), if !forgotten => match message {
                    Some(SupervisorMessage::Stdin(control)) => self.stdin(*control),
                    Some(SupervisorMessage::Cancel(control)) => self.cancel(*control),
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::ConnectionEnded) => self.stop(pid, Expiry::ConnectionEnded),
                    Some(SupervisorMessage::Shutdown) => self.stop(pid, Expiry::Shutdown),
                    Some(SupervisorMessage::Start) => {}
                    None => forgotten = true,
                },
            }
        }
        drop((self.out.reader.take(), self.err.reader.take()));
        if self.exit.is_none() {
            self.exit = Some((ExitFacts::default(), state::seams::now()));
        }
        self.exited();
        if self.writing.is_some() {
            self.write_failed();
        }
        self.stdin = None;
        self.next_write();
        let expiry = self.ending.map(|(_, expiry)| expiry);
        if !failure.is_empty() {
            self.report_start_failure(launch_failure(&failure));
            return;
        }
        if expiry == Some(Expiry::NotStarted) {
            self.report_start_failure(exec::tool_error(
                "exec_spawn_failed",
                NOT_STARTED_IN_WAIT,
                None,
            ));
            return;
        }
        let state = self.state();
        let mut outcome = match expiry {
            Some(Expiry::Wait) => self.outcome(
                HelperStatus::Error,
                Some(exec::tool_error("timeout", "command timed out", None)),
                state.exit.clone(),
            ),
            _ => self.outcome(HelperStatus::Success, None, state.exit.clone()),
        };
        outcome.timed_out = state.timed_out;
        outcome.duration = state.duration;
        outcome.cancelled = self.cancelled.then_some(true);
        outcome.metrics = Some(self.metrics(state.duration));
        let (out, err) = (
            std::mem::take(&mut self.out.capture),
            std::mem::take(&mut self.err.capture),
        );
        self.report(ReportPhase::Terminal, outcome, &out, &err);
        for (log, stream, capture) in [
            (&self.spec.files.stdout, &self.out, &out),
            (&self.spec.files.stderr, &self.err, &err),
        ] {
            if !stream.log_failed && log.finish(&capture.stored_text()).is_err() {
                output_write_failed();
            }
        }
        self.out.capture = out;
        self.err.capture = err;
        self.ended = Some(state);
    }

    /// From the spawn of a file tool's child until its `terminal` report:
    /// the request goes to the child's stdin and its result is read from
    /// its stdout, while cancels and the ends of the connection are
    /// handled. A result that is all there before a cancel is handled
    /// stands; a cancel ends the child through the termination procedure,
    /// and its call then ends `cancelled`.
    async fn file_running(&mut self, call: FileCall, status: Receiver) {
        let bounds = self.context.bounds;
        let (pid, mut pipes) = {
            let process = self.process.as_mut().expect("a launched child");
            let child = &mut process.child;
            let pipes = Pipes::new(
                child.stdin.take(),
                call.request(&self.spec.cwd),
                child.stdout.take(),
                child.stderr.take(),
            );
            (process.pid, pipes)
        };
        let mut status = Some(status);
        let mut forgotten = false;
        while !pipes.received() {
            let ending_due = self
                .ending
                .map_or(Duration::MAX, |(termination, _)| termination.due());
            let drain_ends = self.exit.as_ref().map_or(Duration::MAX, |(_, at)| {
                let drained = *at + bounds.output_drain;
                match self.ending {
                    Some((termination, _)) if termination.killed() => {
                        drained.min(termination.due())
                    }
                    _ => drained,
                }
            });
            if self.ending.is_some() {
                self.find_leader(pid);
            }
            tokio::select! {
                () = pipes.step() => {}
                read = read_some(status.as_mut()), if status.is_some() => match read {
                    Some(bytes) => {
                        if self.start.is_none() && bytes.starts_with(crate::child::STARTED) {
                            self.start = Some(state::seams::now());
                            self.find_leader(pid);
                        }
                    }
                    None => status = None,
                },
                _ = wait_child(self.process.as_mut()), if self.exit.is_none() => {
                    self.exit = Some((ExitFacts::default(), state::seams::now()));
                }
                () = state::seams::sleep_until(drain_ends), if self.exit.is_some() => break,
                () = state::seams::sleep_until_counted(ending_due, ENDING_CHECKED), if self.exit.is_none() && self.ending.is_some() => {
                    let Some((termination, expiry)) = self.ending else { break };
                    match termination.step() {
                        Some(next) => self.ending = Some((next, expiry)),
                        None => break,
                    }
                }
                message = self.messages.recv(), if !forgotten => match message {
                    Some(SupervisorMessage::Cancel(control)) => self.cancel(*control),
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::ConnectionEnded) => self.stop(pid, Expiry::ConnectionEnded),
                    Some(SupervisorMessage::Shutdown) => self.stop(pid, Expiry::Shutdown),
                    // The binding owner refuses a `tool.stdin` of a file
                    // tool's task.
                    Some(SupervisorMessage::Stdin(_) | SupervisorMessage::Start) => {}
                    None => forgotten = true,
                },
            }
        }
        let now = state::seams::now();
        let run = self
            .start
            .map_or(Duration::ZERO, |start| now.saturating_sub(start));
        let envelope = if self.cancelled {
            call.cancelled(self.start.is_some())
        } else {
            call.outcome(&pipes)
        };
        pipes.close();
        self.report_file(envelope, run);
    }

    /// Hands a file tool's `terminal` report, with its `metrics` for work
    /// that ran for `run`, to the outbox; the task has then ended.
    fn report_file(&mut self, mut envelope: HelperEnvelope, run: Duration) {
        envelope.metrics = Some(self.metrics(run));
        self.report_seq += 1;
        let body = Body::Result {
            phase: ReportPhase::Terminal,
            report_seq: PositiveInt::new(self.report_seq).expect("a report_seq from 1"),
            result: Box::new(report::bounded(envelope, ErrorBounds::default())),
        };
        self.submit(
            self.spec.correlation.clone(),
            &self.spec.connection_id.clone(),
            self.spec.traceparent.clone(),
            body,
        );
        self.ended = Some(State {
            ended: true,
            duration: run,
            ..State::default()
        });
    }

    /// The task as a reply reports it now.
    fn state(&self) -> State {
        if let Some(ended) = &self.ended {
            return ended.clone();
        }
        let now = state::seams::now();
        match &self.exit {
            Some((facts, at)) => State {
                ended: true,
                exit: facts.clone(),
                timed_out: matches!(self.ending, Some((_, Expiry::Wait | Expiry::Lifetime))),
                duration: at.saturating_sub(self.start.unwrap_or(*at)),
            },
            None => State {
                ended: false,
                exit: ExitFacts::default(),
                timed_out: false,
                duration: self
                    .start
                    .map_or(Duration::ZERO, |start| now.saturating_sub(start)),
            },
        }
    }

    /// The `running` report at the end of a `detach` initial wait, from both
    /// streams' windows.
    fn report_running(&mut self, start: Duration) {
        let (stdout, stderr) = (
            self.out.capture.take_window(),
            self.err.capture.take_window(),
        );
        let mut outcome = self.outcome(HelperStatus::Running, None, ExitFacts::default());
        outcome.duration = state::seams::now().saturating_sub(start);
        outcome.metrics = None;
        self.report(ReportPhase::Running, outcome, &stdout, &stderr);
    }

    /// An `exec_command` result with empty streams, no duration and the
    /// metrics of a command that never ran.
    fn outcome(&self, status: HelperStatus, error: Option<ToolError>, exit: ExitFacts) -> Outcome {
        Outcome {
            tool: HelperTool::ExecCommand,
            task_id: self.spec.correlation.task_id.clone(),
            status,
            error,
            exit,
            timed_out: false,
            stdout: exec::empty_stream(),
            stderr: exec::empty_stream(),
            duration: Duration::ZERO,
            metrics: Some(self.metrics(Duration::ZERO)),
            cancelled: None,
            was_running: None,
            written_bytes: None,
        }
    }

    /// Renders both streams with the task's limits into `outcome`, fits the
    /// report and hands it to the outbox.
    fn report(
        &mut self,
        phase: ReportPhase,
        mut outcome: Outcome,
        stdout: &dyn StreamOutput,
        stderr: &dyn StreamOutput,
    ) {
        let limits = self.spec.limits;
        outcome.stdout = stdout.snapshot(limits.bytes, limits.lines);
        outcome.stderr = stderr.snapshot(limits.bytes, limits.lines);
        self.report_seq += 1;
        let mut report = Report {
            correlation: self.spec.correlation.clone(),
            body: Body::Result {
                phase,
                report_seq: PositiveInt::new(self.report_seq).expect("a report_seq from 1"),
                result: Box::new(report::bounded(outcome.envelope(), ErrorBounds::default())),
            },
        };
        report::fit(
            &mut report,
            &self.spec.connection_id,
            &Rendering {
                stdout,
                stderr,
                visible_bytes: limits.bytes,
                visible_lines: limits.lines,
            },
        );
        self.submit(
            report.correlation,
            &self.spec.connection_id.clone(),
            self.spec.traceparent.clone(),
            report.body,
        );
    }

    /// Hands a report to the outbox; the cleanup waits for its outcome.
    fn submit(
        &mut self,
        correlation: Correlation,
        connection_id: &Id,
        traceparent: Option<String>,
        body: Body,
    ) {
        let report = Report { correlation, body };
        let mut submission = report.submission(connection_id.clone());
        submission.traceparent = traceparent;
        submission.listener = Some(self.listener.clone());
        let id = self.context.reports.submit(submission);
        self.awaited.insert(id);
    }

    /// The metrics of a report built now, for a command that ran for `run`.
    fn metrics(&self, run: Duration) -> Metrics {
        let now = state::seams::now();
        let left_queue = self.left_queue.unwrap_or(now);
        Metrics {
            wall_time_ms: exec::millis(now.saturating_sub(self.spec.received)),
            queued_ms: exec::millis(left_queue.saturating_sub(self.spec.received)),
            drive_wait_ms: exec::millis(Duration::ZERO),
            run_ms: exec::millis(run),
            sync_ms: exec::millis(Duration::ZERO),
        }
    }

    /// The shell's exit has been observed: every waiting reply is made.
    fn exited(&mut self) {
        for waiter in std::mem::take(&mut self.waiters) {
            self.reply_waiter(waiter);
        }
    }

    /// New output ends every waiting read.
    fn output_arrived(&mut self) {
        state::seams::count(OUTPUT_READ);
        let (reads, writes) = std::mem::take(&mut self.waiters)
            .into_iter()
            .partition(|waiter| waiter.written.is_none());
        self.waiters = writes;
        for waiter in reads {
            self.reply_waiter(waiter);
        }
    }

    fn reply_due(&self) -> Duration {
        self.waiters
            .iter()
            .map(|waiter| waiter.deadline)
            .min()
            .unwrap_or(Duration::MAX)
    }

    fn expire_waiters(&mut self) {
        let now = state::seams::now();
        let (due, waiting) = std::mem::take(&mut self.waiters)
            .into_iter()
            .partition(|waiter| waiter.deadline <= now);
        self.waiters = waiting;
        for waiter in due {
            self.reply_waiter(waiter);
        }
    }

    /// A `tool.stdin`: a read replies at once when a window holds output or
    /// the task has ended, else waits; a write joins the task's writes.
    fn stdin(&mut self, control: Control<StdinPayload>) {
        match Request::of(&control.payload) {
            Request::Read { wait } => {
                if self.cleaned {
                    self.control_error(
                        &control,
                        ControlErrorCode::ResultUnavailable,
                        "the task's output is no longer kept",
                    );
                } else if self.state().ended
                    || self.out.capture.window_has_output()
                    || self.err.capture.window_has_output()
                    || wait.is_zero()
                {
                    self.reply_window(&control, None);
                } else {
                    self.wait_reply(control, wait, None);
                }
            }
            Request::Write { .. } => {
                self.writes.push_back(control);
                if self.process.is_some() || self.ended.is_some() {
                    self.next_write();
                }
            }
        }
    }

    fn wait_reply(&mut self, control: Control<StdinPayload>, wait: Duration, written: Option<u64>) {
        self.waiters.push(Waiter {
            control,
            deadline: state::seams::now() + wait,
            written,
        });
        state::seams::count(REPLY_WAITING);
    }

    /// Handles the queued writes in arrival order until one has to be
    /// written to the pipe.
    fn next_write(&mut self) {
        while self.writing.is_none() {
            let Some(control) = self.writes.pop_front() else {
                return;
            };
            let Request::Write {
                write_seq,
                chars,
                digest,
                wait,
            } = Request::of(&control.payload)
            else {
                continue;
            };
            let check = self.records.check(write_seq, &digest);
            if self.cleaned && !matches!(check, Check::Repeat(_)) {
                self.control_error(
                    &control,
                    ControlErrorCode::ResultUnavailable,
                    "the task's output is no longer kept",
                );
                continue;
            }
            match check {
                Check::Conflict => self.control_error(
                    &control,
                    ControlErrorCode::WriteConflict,
                    "write_seq names a recorded write with other chars",
                ),
                Check::Stale => self.control_error(
                    &control,
                    ControlErrorCode::InvalidInput,
                    "write_seq is at or below the task's highest recorded write and matches none",
                ),
                Check::Repeat(record) => self.reply_repeat(&control, record),
                Check::New
                    if self.state().ended
                        || self.stdin.is_none()
                        || self.cancelled
                        || self.stopped =>
                {
                    self.records
                        .record(write_seq, digest, 0, Some(ControlErrorCode::StdinClosed));
                    self.reply_closed(&control, 0);
                }
                Check::New if chars == stdin::INTERRUPT => {
                    if let Some(process) = &self.process {
                        terminate::interrupt(process.pid);
                    }
                    self.records.record(write_seq, digest, 0, None);
                    self.wait_reply(control, wait, Some(0));
                }
                Check::New => {
                    self.writing = Some(Writing {
                        control,
                        write_seq,
                        digest,
                        wait,
                        offset: 0,
                        deadline: state::seams::now() + self.context.bounds.stdin_write,
                    });
                    state::seams::count(WRITE_STARTED);
                }
            }
        }
    }

    /// What one write to the pipe gave.
    fn wrote(&mut self, written: std::io::Result<usize>) {
        let Some(writing) = &mut self.writing else {
            return;
        };
        match written {
            Ok(count) if count > 0 => {
                writing.offset += count;
                if writing.offset < writing.control.payload.chars.len() {
                    return;
                }
                let writing = self.writing.take().expect("a write in progress");
                let written = writing.offset as u64;
                self.records
                    .record(writing.write_seq, writing.digest, written, None);
                self.wait_reply(writing.control, writing.wait, Some(written));
                self.next_write();
            }
            _ => self.write_failed(),
        }
    }

    /// A write that failed or timed out: the task's stdin closes for good
    /// and the write ends `stdin_closed` with the bytes written so far.
    fn write_failed(&mut self) {
        self.stdin = None;
        self.write_interrupted();
    }

    /// Ends the write in progress `stdin_closed` with the bytes written so
    /// far.
    fn write_interrupted(&mut self) {
        let Some(writing) = self.writing.take() else {
            return;
        };
        let written = writing.offset as u64;
        self.records.record(
            writing.write_seq,
            writing.digest,
            written,
            Some(ControlErrorCode::StdinClosed),
        );
        self.reply_closed(&writing.control, written);
        self.next_write();
    }

    /// A `tool.cancel`, recorded and answered at once.
    fn cancel(&mut self, control: Control<CancelPayload>) {
        let state = self.state();
        let receipt = if state.ended {
            Receipt::Ended
        } else if self.process.is_none() {
            Receipt::Staged
        } else if self.ending.is_some() {
            Receipt::Terminating
        } else {
            Receipt::Running
        };
        if receipt.cancelled() {
            self.cancelled = true;
        }
        let limits = Visible::requested(control.payload.limits.as_ref());
        let mut outcome = self.reply_outcome(HelperTool::Cancel, HelperStatus::Success, &state);
        outcome.cancelled = Some(receipt.cancelled());
        outcome.was_running = Some(receipt.was_running());
        let (out, err) = (
            std::mem::take(&mut self.out.capture),
            std::mem::take(&mut self.err.capture),
        );
        self.reply(&control, outcome, &out, &err, limits, None);
        self.out.capture = out;
        self.err.capture = err;
        if receipt == Receipt::Staged {
            self.report_cancelled_staged();
            return;
        }
        // Writes stop, but the command's stdin stays open: its end of file
        // would let the command exit before the termination ends it.
        if receipt.was_running() {
            self.write_interrupted();
        }
        if receipt == Receipt::Running {
            let pid = self.process.as_ref().map_or(0, |process| process.pid);
            self.ending = Some((
                Termination::begin(pid, self.context.bounds.termination),
                Expiry::Cancel,
            ));
        }
    }

    /// The task's connection ended or the App Server shuts down: writes stop
    /// as at a cancel, and the termination procedure ends the task, at a
    /// connection's end with SIGKILL at once, also one already in its grace.
    /// Once the shutdown has begun, the end of the connection that it brings
    /// counts as the shutdown.
    fn stop(&mut self, pid: u32, expiry: Expiry) {
        let expiry = match expiry {
            Expiry::ConnectionEnded if self.shutting_down => Expiry::Shutdown,
            Expiry::Shutdown => {
                self.shutting_down = true;
                expiry
            }
            expiry => expiry,
        };
        self.stopped = true;
        self.write_interrupted();
        if self.exit.is_some() {
            return;
        }
        let steps = self.context.bounds.termination;
        self.ending = match (self.ending, expiry) {
            (Some((termination, _)), _) if termination.killed() => self.ending,
            (Some((_, current)), Expiry::ConnectionEnded) => {
                Some((Termination::kill(pid, steps), current))
            }
            (None, Expiry::ConnectionEnded) => Some((Termination::kill(pid, steps), expiry)),
            (None, _) => Some((Termination::begin(pid, steps), expiry)),
            (ending, _) => ending,
        };
    }

    /// The `terminal` report of a task cancelled before it started.
    fn report_cancelled_staged(&mut self) {
        if let Some(call) = FileCall::of(&self.spec.tool) {
            self.report_file(call.cancelled(false), Duration::ZERO);
            return;
        }
        let mut outcome = self.outcome(HelperStatus::Success, None, ExitFacts::default());
        outcome.cancelled = Some(true);
        let empty = StreamCapture::new();
        self.report(ReportPhase::Terminal, outcome, &empty, &empty);
        self.ended = Some(State {
            ended: true,
            ..State::default()
        });
        self.exited();
        self.next_write();
    }

    /// A reply's result for `tool`, with the task's state and empty streams.
    fn reply_outcome(&self, tool: HelperTool, status: HelperStatus, state: &State) -> Outcome {
        Outcome {
            tool,
            task_id: self.spec.correlation.task_id.clone(),
            status,
            error: None,
            exit: if state.ended {
                state.exit.clone()
            } else {
                ExitFacts::default()
            },
            timed_out: state.ended && state.timed_out,
            stdout: exec::empty_stream(),
            stderr: exec::empty_stream(),
            duration: state.duration,
            metrics: None,
            cancelled: None,
            was_running: None,
            written_bytes: None,
        }
    }

    fn reply_waiter(&mut self, waiter: Waiter) {
        self.reply_window(&waiter.control, waiter.written);
    }

    /// A `stdin` reply with both streams' windows: `running` while the task
    /// runs, `success` with its exit facts once it has ended.
    fn reply_window(&mut self, control: &Control<StdinPayload>, written: Option<u64>) {
        let state = self.state();
        let status = if state.ended {
            HelperStatus::Success
        } else {
            HelperStatus::Running
        };
        let mut outcome = self.reply_outcome(HelperTool::Stdin, status, &state);
        outcome.written_bytes = written;
        let limits = Visible::requested(control.payload.limits.as_ref());
        let (stdout, stderr) = (
            self.out.capture.take_window(),
            self.err.capture.take_window(),
        );
        self.reply(control, outcome, &stdout, &stderr, limits, None);
    }

    /// A write that ended `stdin_closed`: the result, with the bytes
    /// written, and the error when bytes were written or the task has
    /// ended; otherwise only the error.
    fn reply_closed(&mut self, control: &Control<StdinPayload>, written: u64) {
        let state = self.state();
        if written == 0 && !state.ended {
            self.control_error(control, ControlErrorCode::StdinClosed, STDIN_CLOSED);
            return;
        }
        let mut outcome = self.reply_outcome(HelperTool::Stdin, HelperStatus::Error, &state);
        outcome.error = Some(exec::tool_error("stdin_closed", STDIN_CLOSED, None));
        outcome.written_bytes = Some(written);
        let limits = Visible::requested(control.payload.limits.as_ref());
        let (stdout, stderr) = (
            self.out.capture.take_window(),
            self.err.capture.take_window(),
        );
        self.reply(
            control,
            outcome,
            &stdout,
            &stderr,
            limits,
            Some(closed_error()),
        );
    }

    /// A write repeated with its recorded content: a new reply with the
    /// recorded outcome and the task's current state, reading no output.
    fn reply_repeat(&mut self, control: &Control<StdinPayload>, record: stdin::WriteRecord) {
        let state = self.state();
        let closed = record.error == Some(ControlErrorCode::StdinClosed);
        let status = match (closed, state.ended) {
            (true, _) => HelperStatus::Error,
            (false, true) => HelperStatus::Success,
            (false, false) => HelperStatus::Running,
        };
        let mut outcome = self.reply_outcome(HelperTool::Stdin, status, &state);
        outcome.written_bytes = Some(record.written_bytes);
        if closed {
            outcome.error = Some(exec::tool_error("stdin_closed", STDIN_CLOSED, None));
        }
        let limits = Visible::requested(control.payload.limits.as_ref());
        let (stdout, stderr) = if self.cleaned {
            (Counters(self.kept.0.clone()), Counters(self.kept.1.clone()))
        } else {
            (
                Counters(self.out.capture.counters()),
                Counters(self.err.capture.counters()),
            )
        };
        let error = closed.then(closed_error);
        self.reply(control, outcome, &stdout, &stderr, limits, error);
    }

    fn control_error(
        &mut self,
        control: &Control<StdinPayload>,
        code: ControlErrorCode,
        message: &str,
    ) {
        self.submit(
            control.correlation.clone(),
            &control.connection_id,
            control.traceparent.clone(),
            Body::Control {
                result: None,
                error: Some(ProtocolError {
                    code,
                    message: message.to_owned(),
                }),
            },
        );
    }

    /// Renders both streams with the control's limits into `outcome`, fits
    /// the reply and hands it to the outbox.
    fn reply<P>(
        &mut self,
        control: &Control<P>,
        mut outcome: Outcome,
        stdout: &dyn StreamOutput,
        stderr: &dyn StreamOutput,
        limits: Visible,
        error: Option<ProtocolError<ControlErrorCode>>,
    ) {
        outcome.stdout = stdout.snapshot(limits.bytes, limits.lines);
        outcome.stderr = stderr.snapshot(limits.bytes, limits.lines);
        let mut report = Report {
            correlation: control.correlation.clone(),
            body: Body::Control {
                result: Some(Box::new(report::bounded(
                    outcome.envelope(),
                    ErrorBounds::default(),
                ))),
                error,
            },
        };
        report::fit(
            &mut report,
            &control.connection_id,
            &Rendering {
                stdout,
                stderr,
                visible_bytes: limits.bytes,
                visible_lines: limits.lines,
            },
        );
        self.submit(
            report.correlation,
            &control.connection_id,
            control.traceparent.clone(),
            report.body,
        );
    }
}

fn closed_error() -> ProtocolError<ControlErrorCode> {
    ProtocolError {
        code: ControlErrorCode::StdinClosed,
        message: STDIN_CLOSED.to_owned(),
    }
}

/// Streams that a reply reports by their counters alone.
struct Counters(StreamSnapshot);

impl StreamOutput for Counters {
    fn snapshot(&self, _visible_bytes: usize, _visible_lines: usize) -> StreamSnapshot {
        self.0.clone()
    }
}

/// The error of a start of `work` that failed for `message`: a command's
/// `exec_spawn_failed`, a file tool's `helper_failure`.
fn start_error(work: &Work, message: &str) -> ToolError {
    let kind = match work {
        Work::Exec(_) => "exec_spawn_failed",
        Work::File(_) => "helper_failure",
    };
    exec::tool_error(kind, message, None)
}

/// The error of the failure object that the launcher entry wrote on the
/// launch-status pipe: a directory error as given, any other
/// `exec_spawn_failed`.
fn launch_failure(bytes: &[u8]) -> ToolError {
    let reported: serde_json::Value = serde_json::from_slice(bytes).unwrap_or_default();
    match reported["kind"].as_str() {
        Some(kind @ ("cwd_not_found" | "cwd_not_directory" | "cwd_inaccessible")) => {
            exec::tool_error(
                kind,
                reported["message"].as_str().unwrap_or_default(),
                reported["path"].as_str().map(str::to_owned),
            )
        }
        _ => exec::tool_error("exec_spawn_failed", NOT_STARTED, None),
    }
}

/// The initial wait until `ends`, counted on [`WAIT_CHECKED`] once the
/// command has `started`.
async fn initial_wait(ends: Duration, started: bool) {
    if started {
        state::seams::sleep_until_counted(ends, WAIT_CHECKED).await;
    } else {
        state::seams::sleep_until(ends).await;
    }
}

async fn wait_child(process: Option<&mut Process>) -> std::io::Result<std::process::ExitStatus> {
    match process {
        Some(process) => {
            let status = process.child.wait().await;
            state::seams::barrier(EXIT_OBSERVED).await;
            status
        }
        None => std::future::pending().await,
    }
}

/// One write to the task's stdin of what is left of the write in progress.
/// A write that another branch overtakes has written nothing.
async fn write_some(
    stdin: &mut Option<ChildStdin>,
    writing: Option<&Writing>,
) -> std::io::Result<usize> {
    match (stdin, writing) {
        (Some(stdin), Some(writing)) => {
            stdin
                .write(&writing.control.payload.chars.as_bytes()[writing.offset..])
                .await
        }
        _ => std::future::pending().await,
    }
}

/// [`read_some`] of the launch-status pipe, held at [`LAUNCH_STATUS`]
/// until the command has `started`.
async fn read_status(pipe: Option<&mut Receiver>, started: bool) -> Option<Vec<u8>> {
    if pipe.is_some() && !started {
        state::seams::barrier(LAUNCH_STATUS).await;
    }
    read_some(pipe).await
}

/// The next bytes of `pipe`, or `None` at its end.
async fn read_some(pipe: Option<&mut Receiver>) -> Option<Vec<u8>> {
    let Some(pipe) = pipe else {
        return std::future::pending().await;
    };
    let mut buffer = [0u8; 4_096];
    match pipe.read(&mut buffer).await {
        Ok(0) | Err(_) => None,
        Ok(count) => Some(buffer[..count].to_vec()),
    }
}

/// One of the command's output pipes, read into its capture and its log.
struct Stream {
    reader: Option<Box<dyn AsyncRead + Unpin + Send>>,
    capture: StreamCapture,
    buffer: Vec<u8>,
    /// Whether a write of its log failed; the log is then left alone.
    log_failed: bool,
}

impl Stream {
    fn new<R: AsyncRead + Unpin + Send + 'static>(reader: Option<R>) -> Stream {
        Stream {
            reader: reader.map(|reader| Box::new(reader) as _),
            capture: StreamCapture::new(),
            buffer: vec![0; READ_CHUNK_BYTES],
            log_failed: false,
        }
    }

    fn is_open(&self) -> bool {
        self.reader.is_some()
    }

    async fn read(&mut self) -> std::io::Result<usize> {
        match &mut self.reader {
            Some(reader) => reader.read(&mut self.buffer).await,
            None => std::future::pending().await,
        }
    }

    /// Keeps what a read gave, telling whether it gave bytes; the pipe's
    /// end, or an error, closes it.
    fn take(&mut self, read: std::io::Result<usize>, log: &mut Log) -> bool {
        match read {
            Ok(0) | Err(_) => {
                self.reader = None;
                state::seams::count(OUTPUT_ENDED);
                false
            }
            Ok(count) => {
                let bytes = &self.buffer[..count];
                self.capture.write(bytes);
                if !self.log_failed && log.append(bytes).is_err() {
                    self.log_failed = true;
                    output_write_failed();
                }
                true
            }
        }
    }
}

fn output_write_failed() {
    observe::warn!(
        "task.output_write_failed",
        error.code = observe::Code("output_write_failed")
    );
}
