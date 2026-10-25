//! The Task supervisor: the one owner of a task and its process tree, from
//! the task's admission until its Session binding owner forgets it. A task
//! is `staged` until the binding owner starts it
//! ([`SupervisorMessage::Start`]); it is then `starting` while its
//! supervisor checks the isolation, creates the task's files and launches
//! the command;
//! `running` from the command's actual start, while the supervisor reads both
//! pipes into the task's capture; `ended` once the shell's exit has been
//! observed, or given up on, and the `terminal` report has been handed to the
//! outbox; and `cleaned` once that report is acknowledged or dropped and the
//! task's files are gone. Every wait reads the seam clock.

use std::collections::HashSet;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use observe::{Identity, TraceContext};
use protocol::field::PositiveInt;
use protocol::id::Id;
use protocol::ports::{ReportId, ReportListener, ReportOutcome, ReportSink};
use protocol::tool::{ExecInput, HelperStatus, Metrics, ProtocolError, ReportPhase, ToolError};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::unix::pipe::Receiver;
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::capture::StreamCapture;
use crate::exec::{self, ExecCall, ExitFacts, OnExpiry, Outcome};
use crate::isolation::{Command, Isolation, LaunchError, Process};
use crate::limits::Visible;
use crate::policy::{PolicyRejection, TaskPermissions};
use crate::report::{self, Body, Correlation, ErrorBounds, Rendering, Report, StreamOutput};
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

/// The message of every failure to start other than a directory's.
const NOT_STARTED: &str = "process could not start";

/// The message of a command whose start did not come within its initial
/// wait.
const NOT_STARTED_IN_WAIT: &str = "process did not start within the wait";

/// One admitted `exec_command` task, as its supervisor starts it.
pub struct TaskSpec {
    pub connection_id: Id,
    pub correlation: Correlation,
    pub input: ExecInput,
    pub permissions: TaskPermissions,
    pub limits: Visible,
    /// The binding's canonical working directory.
    pub cwd: PathBuf,
    pub files: TaskFiles,
    pub traceparent: Option<String>,
    /// When the instruction was received, on the seam clock.
    pub received: Duration,
}

/// What every supervisor of an App Server shares.
#[derive(Clone)]
pub struct Context {
    /// The running `tetral`, canonical.
    pub tetral: PathBuf,
    pub isolation: Isolation,
    pub reports: Arc<dyn ReportSink>,
    pub bounds: Bounds,
}

/// The waits of a supervisor, each a constant but for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    pub output_drain: Duration,
    pub termination: terminate::Steps,
}

impl Default for Bounds {
    fn default() -> Bounds {
        Bounds {
            output_drain: OUTPUT_DRAIN,
            termination: terminate::Steps::default(),
        }
    }
}

pub enum SupervisorMessage {
    /// The binding owner starts the staged task.
    Start,
    /// The outbox's outcome of one of the task's reports.
    Outcome(ReportId, ReportOutcome),
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
        left_queue: Duration::ZERO,
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
    left_queue: Duration,
}

/// Why a task did not reach `running`.
enum Start {
    /// The check or the transform refused the call; no process started.
    Rejected(PolicyRejection),
    /// The start failed; the `terminal` report carries this error.
    Failed(ToolError),
}

/// A launched command, from its spawn.
struct Launched {
    process: Process,
    call: ExecCall,
    /// The launch-status pipe, on which the launcher entry reports the
    /// command's start or its failure.
    status: Receiver,
    spawned: Duration,
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
}

impl Supervisor {
    async fn run(mut self) {
        // `staged`
        loop {
            match self.messages.recv().await {
                None => return,
                Some(SupervisorMessage::Start) => break,
                Some(SupervisorMessage::Outcome(..)) => {}
            }
        }
        self.left_queue = state::seams::now();
        match self.start().await {
            Err(Start::Rejected(rejection)) => {
                self.submit(Body::Rejected(ProtocolError {
                    code: rejection.code,
                    message: rejection.message,
                }));
                return;
            }
            Err(Start::Failed(error)) => self.report_start_failure(error),
            Ok(launched) => self.supervise(launched).await,
        }
        while !self.awaited.is_empty() {
            match self.messages.recv().await {
                Some(SupervisorMessage::Outcome(report, _)) => {
                    self.awaited.remove(&report);
                }
                Some(SupervisorMessage::Start) => {}
                None => break,
            }
        }
        let _ = self.spec.files.remove();
    }

    /// `starting`: the availability check, the task's files, the
    /// environment and the launch.
    async fn start(&mut self) -> Result<Launched, Start> {
        let context = &self.context;
        let spec = &self.spec;
        let available = context
            .isolation
            .check(&spec.permissions, &spec.cwd, &context.tetral)
            .await
            .map_err(Start::Rejected)?;
        spec.files
            .create()
            .map_err(|failure| spawn_failed(&failure.to_string()))?;
        let call = ExecCall::new(&spec.input, &spec.cwd);
        let mut env = crate::env::command_environment(
            std::env::vars_os(),
            call.env.as_ref(),
            spec.files.temporary(),
        )
        .map_err(|_| spawn_failed(NOT_STARTED))?;
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
        let (status, status_writer) = std::io::pipe().map_err(|_| spawn_failed(NOT_STARTED))?;
        let mut argv = vec![context.tetral.display().to_string()];
        argv.extend(
            crate::child::exec::arguments(&call.directory, exec::shell(), &call.cmd)
                .into_iter()
                .map(|argument| argument.to_string_lossy().into_owned()),
        );
        let command = Command {
            argv,
            env,
            directory: None,
        };
        let process = context
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
                LaunchError::Spawn(_) => spawn_failed(NOT_STARTED),
            })?;
        let spawned = state::seams::now();
        let status = Receiver::from_owned_fd(OwnedFd::from(status))
            .map_err(|_| spawn_failed(NOT_STARTED))?;
        Ok(Launched {
            process,
            call,
            status,
            spawned,
        })
    }

    /// The `terminal` report of a command that never started: `status:
    /// error`, null exit facts and empty streams.
    fn report_start_failure(&mut self, error: ToolError) {
        let outcome = self.outcome(HelperStatus::Error, Some(error), ExitFacts::default());
        let empty = StreamCapture::new();
        self.report(ReportPhase::Terminal, outcome, &empty, &empty);
    }

    /// From the spawn, through the command's start on the launch-status
    /// pipe and `running`, until the `terminal` report. A sandbox that ends
    /// with neither the start nor a failure on the pipe ended before the
    /// launcher entry ran, as an error of Codex's sandbox stages ends it:
    /// its exit status and output are the command's result.
    async fn supervise(&mut self, launched: Launched) {
        let Launched {
            mut process,
            call,
            mut status,
            spawned,
        } = launched;
        let bounds = self.context.bounds;
        // stdin stays open for the task's writes.
        let _stdin = process.child.stdin.take();
        let mut out = Stream::new(process.child.stdout.take());
        let mut err = Stream::new(process.child.stderr.take());
        let mut status_open = true;
        let mut start: Option<Duration> = None;
        let mut failure = Vec::new();
        let wait_ends = spawned + call.wait;
        let mut waited = false;
        let mut forgotten = false;
        let mut ending: Option<(Termination, Expiry)> = None;
        let mut exit: Option<(ExitFacts, Duration)> = None;
        loop {
            if exit.is_some() && !out.is_open() && !err.is_open() {
                break;
            }
            tokio::select! {
                read = out.read(), if out.is_open() => {
                    out.take(read, &mut self.spec.files.stdout);
                }
                read = err.read(), if err.is_open() => {
                    err.take(read, &mut self.spec.files.stderr);
                }
                read = read_some(&mut status), if status_open => match read {
                    Some(bytes) => {
                        let mut bytes = &bytes[..];
                        if start.is_none()
                            && failure.is_empty()
                            && let Some(rest) = bytes.strip_prefix(crate::child::STARTED)
                        {
                            start = Some(state::seams::now());
                            bytes = rest;
                        }
                        let room = LAUNCH_STATUS_MAX_BYTES.saturating_sub(failure.len());
                        failure.extend_from_slice(&bytes[..bytes.len().min(room)]);
                    }
                    None => status_open = false,
                },
                waited_status = process.child.wait(), if exit.is_none() => {
                    let facts = waited_status.map_or_else(|_| ExitFacts::default(), ExitFacts::observed);
                    exit = Some((facts, state::seams::now()));
                }
                () = initial_wait(wait_ends, start.is_some()), if !waited && exit.is_none() && ending.is_none() => {
                    waited = true;
                    match (start, call.on_expiry) {
                        (None, _) => {
                            ending = Some((Termination::begin(process.pid, bounds.termination), Expiry::NotStarted));
                        }
                        (Some(_), OnExpiry::Kill) => {
                            ending = Some((Termination::begin(process.pid, bounds.termination), Expiry::Wait));
                        }
                        (Some(start), OnExpiry::Detach) => self.report_running(start, &mut out.capture, &mut err.capture),
                    }
                }
                () = state::seams::sleep_until(start.map_or(Duration::MAX, |start| start + call.lifetime)), if call.on_expiry == OnExpiry::Detach && start.is_some() && exit.is_none() && ending.is_none() => {
                    ending = Some((Termination::begin(process.pid, bounds.termination), Expiry::Lifetime));
                }
                () = state::seams::sleep_until(ending.map_or(Duration::MAX, |(termination, _)| termination.due())), if exit.is_none() && ending.is_some() => {
                    let Some((termination, expiry)) = ending else { break };
                    match termination.step() {
                        Some(next) => ending = Some((next, expiry)),
                        None => break,
                    }
                }
                () = state::seams::sleep_until(exit.as_ref().map_or(Duration::MAX, |(_, at)| *at + bounds.output_drain)), if exit.is_some() => break,
                message = self.messages.recv(), if !forgotten => match message {
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::Start) => {}
                    None => forgotten = true,
                },
            }
        }
        drop((out.reader.take(), err.reader.take()));
        let expiry = ending.map(|(_, expiry)| expiry);
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
        let (facts, exited) = exit.unwrap_or_else(|| (ExitFacts::default(), state::seams::now()));
        let mut outcome = match expiry {
            Some(Expiry::Wait) => self.outcome(
                HelperStatus::Error,
                Some(exec::tool_error("timeout", "command timed out", None)),
                facts,
            ),
            _ => self.outcome(HelperStatus::Success, None, facts),
        };
        let run = exited.saturating_sub(start.unwrap_or(exited));
        outcome.timed_out = expiry.is_some();
        outcome.duration = run;
        outcome.metrics = Some(self.metrics(run));
        self.report(ReportPhase::Terminal, outcome, &out.capture, &err.capture);
        for (log, stream) in [
            (&self.spec.files.stdout, &out),
            (&self.spec.files.stderr, &err),
        ] {
            if !stream.log_failed && log.finish(&stream.capture.stored_text()).is_err() {
                output_write_failed();
            }
        }
    }

    /// The `running` report at the end of a `detach` initial wait, from both
    /// streams' windows.
    fn report_running(
        &mut self,
        start: Duration,
        stdout: &mut StreamCapture,
        stderr: &mut StreamCapture,
    ) {
        let (stdout, stderr) = (stdout.take_window(), stderr.take_window());
        let mut outcome = self.outcome(HelperStatus::Running, None, ExitFacts::default());
        outcome.duration = state::seams::now().saturating_sub(start);
        outcome.metrics = None;
        self.report(ReportPhase::Running, outcome, &stdout, &stderr);
    }

    /// An `exec_command` result with empty streams, no duration and the
    /// metrics of a command that never ran.
    fn outcome(&self, status: HelperStatus, error: Option<ToolError>, exit: ExitFacts) -> Outcome {
        Outcome {
            task_id: self.spec.correlation.task_id.clone(),
            status,
            error,
            exit,
            timed_out: false,
            stdout: exec::empty_stream(),
            stderr: exec::empty_stream(),
            duration: Duration::ZERO,
            metrics: Some(self.metrics(Duration::ZERO)),
        }
    }

    /// Renders both streams with the task's limits into `outcome`, fits the
    /// report and hands it to the outbox; the cleanup waits for a
    /// `terminal` report's outcome.
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
        let id = self.submit(report.body);
        if phase == ReportPhase::Terminal {
            self.awaited.insert(id);
        }
    }

    fn submit(&self, body: Body) -> ReportId {
        let report = Report {
            correlation: self.spec.correlation.clone(),
            body,
        };
        let mut submission = report.submission(self.spec.connection_id.clone());
        submission.traceparent = self.spec.traceparent.clone();
        submission.listener = Some(self.listener.clone());
        self.context.reports.submit(submission)
    }

    /// The metrics of a report built now, for a command that ran for `run`.
    fn metrics(&self, run: Duration) -> Metrics {
        let now = state::seams::now();
        Metrics {
            wall_time_ms: exec::millis(now.saturating_sub(self.spec.received)),
            queued_ms: exec::millis(self.left_queue.saturating_sub(self.spec.received)),
            drive_wait_ms: exec::millis(Duration::ZERO),
            run_ms: exec::millis(run),
            sync_ms: exec::millis(Duration::ZERO),
        }
    }
}

fn spawn_failed(message: &str) -> Start {
    Start::Failed(exec::tool_error("exec_spawn_failed", message, None))
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

/// The next bytes of `pipe`, or `None` at its end.
async fn read_some(pipe: &mut Receiver) -> Option<Vec<u8>> {
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

    /// Keeps what a read gave; the pipe's end, or an error, closes it.
    fn take(&mut self, read: std::io::Result<usize>, log: &mut Log) {
        match read {
            Ok(0) | Err(_) => self.reader = None,
            Ok(count) => {
                let bytes = &self.buffer[..count];
                self.capture.write(bytes);
                if !self.log_failed && log.append(bytes).is_err() {
                    self.log_failed = true;
                    output_write_failed();
                }
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
