//! The Task supervisor owns a task's execution from admission through process
//! disappearance, report outcomes and file cleanup. It then transfers the
//! existing mailbox to [`FinishedTask`], which retains only counters, exit and
//! write facts for late controls until the Session binding forgets the task. A task
//! is `staged` until the binding owner starts it ([`SupervisorMessage::Start`]);
//! it is then `starting` while its supervisor checks the isolation, creates
//! the task's files and launches the command; `running` from the command's
//! actual start, while the supervisor reads both pipes into the task's
//! capture and writes its stdin; `ended` once the shell's exit has been
//! observed, or given up on, and the `terminal` report has been handed to the
//! outbox. Once the processes are gone and every report is acknowledged or
//! dropped, cleanup is attempted and the full supervisor is released; cleanup
//! failures are logged without retaining execution for a retry. It answers the task's
//! `tool.cancel` at receipt in every state, and its `tool.stdin` in every
//! state but `staged` and the availability check that begins `starting`,
//! during which that `tool.stdin` waits and the task's other messages are
//! still read: a cancel or the binding's close prevents the start and makes
//! the task's `terminal` report at once, and the connection's end discards
//! the task without one. Each waiting reply is a timer and a waiter of the
//! supervisor, which meanwhile keeps reading output and handling the exit
//! and other controls. Every wait reads the seam clock.
//! With a Session drive sync (`DriveSyncPort`), an `exec_command` or
//! `view_image` runs the drive check after the availability check, still
//! `starting`; each execution report and each newly built `tool.stdin`
//! reply runs its sync point before it is frozen, and carries its
//! `drive_sync`.
//! A file tool's task runs its child instead of a command
//! ([`crate::file_tools`]): `running` from the spawn, its `terminal` report
//! is made once the child's result is received, or once its child is gone
//! without one, and a cancel ends the child through the termination
//! procedure.
//!
//! When the task's child reports its start on descriptor 3, or, without
//! that report, while the task is being ended, the supervisor finds the
//! task's first process inside its sandbox, writes the task's record
//! ([`cleanup::Record`]) and tells its binding owner the task's PID
//! namespace. After the `terminal` report the task's
//! processes are checked every [`cleanup::CHECK_INTERVAL`] until they are
//! confirmed gone; only then do the record and, once every report has its
//! outcome, the task's files go. A connection's end discards a staged task
//! without a report and ends a running one with SIGKILL at once; the App
//! Server's shutdown ends it through the termination procedure.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use observe::{Identity, TraceContext};
use protocol::field::PositiveInt;
use protocol::id::Id;
use protocol::ports::{
    ActivitySink, DriveAnswer, DriveCheck, DriveSyncPoint, DriveSyncPort, ReportId, ReportListener,
    ReportOutcome, ReportSink,
};
use protocol::tool::{
    CancelPayload, ControlErrorCode, DriveSyncOutcome, HelperEnvelope, HelperStatus, HelperTool,
    Metrics, ProtocolError, ReportPhase, StdinPayload, StreamSnapshot, ToolError, ToolInput,
    ToolResult,
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
use crate::isolation::{Available, Command, Isolation, LaunchError, Process};
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
pub const LAUNCH_STATUS_MAX_BYTES: usize = 65_536;

/// The most bytes that one read of the launch-status pipe takes.
const LAUNCH_STATUS_READ_BYTES: usize = 4_096;

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

/// The test seam barrier that holds the availability check before it runs,
/// so that a test can control a task while its start is undecided.
const AVAILABILITY_CHECK: &str = "task.availability_check";

/// The message of every failure to start other than a directory's.
const NOT_STARTED: &str = "process could not start";

/// The message of a command whose start did not come within its initial
/// wait.
const NOT_STARTED_IN_WAIT: &str = "process did not start within the wait";

const STDIN_CLOSED: &str = "task stdin is closed or not accepting input";

/// The wait of a sync point for a result whose command has exited and for
/// an `edit` result; a `running` result's sync point waits for no content.
pub const SYNC_WAIT: Duration = Duration::from_secs(30);

/// The message of the control error `task_not_found`, which also answers
/// every control of a task whose start was rejected.
pub const TASK_NOT_FOUND: &str = "task_id names no task of the named binding on this connection";

/// One admitted task, as its supervisor starts it, without its tool's input.
pub struct TaskSpec {
    pub connection_id: Id,
    pub correlation: Correlation,
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
    /// The Session's drive sync, when the Session has one: the check before
    /// an `exec_command` or `view_image` starts, `TETRAL_DRIVES`, and the
    /// sync point before each result.
    pub drive_sync: Option<Arc<dyn DriveSyncPort>>,
}

/// What a supervisor tells its task's binding owner about the task's
/// processes.
pub trait TaskListener: Send + Sync {
    /// The task's first process inside its sandbox exists, in the PID
    /// namespace whose inode is `namespace`.
    fn started(&self, namespace: u64);
    /// The task's processes are confirmed gone, or it never had any.
    fn gone(&self);
    /// The task's start was rejected, so its instruction was not admitted:
    /// the registration is withdrawn.
    fn withdrawn(&self);
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
    /// Told of each `exec_command` command's start and exit.
    pub activity: Arc<dyn ActivitySink>,
}

/// The waits and sizes of a supervisor, each a constant but for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    pub output_drain: Duration,
    pub termination: terminate::Steps,
    pub stdin_write: Duration,
    /// How often an ended task's processes are checked until they are gone.
    pub gone_check: Duration,
    pub launch_status_max_bytes: usize,
}

impl Default for Bounds {
    fn default() -> Bounds {
        Bounds {
            output_drain: OUTPUT_DRAIN,
            termination: terminate::Steps::default(),
            stdin_write: stdin::WRITE_TIMEOUT,
            gone_check: cleanup::CHECK_INTERVAL,
            launch_status_max_bytes: LAUNCH_STATUS_MAX_BYTES,
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
    /// The task's binding closes: a staged task never starts, and a running
    /// one ends as at a cancel.
    Prevent,
    /// An identical `tool.execute` with this `message_id` joined the task:
    /// the task's final report, its `terminal` or `rejected`, ends it too.
    Joined(Id),
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

/// Starts the supervisor of the admitted task `spec` with the input of its
/// tool, `staged` until its mailbox gets [`SupervisorMessage::Start`]; it
/// runs inside its instruction's span.
pub fn spawn(spec: TaskSpec, tool: ToolInput, context: Context) -> SupervisorMailbox {
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
    let (supervisor, mailbox) = Supervisor::new(spec, tool, context);
    tokio::spawn(supervisor.run().instrument(span));
    mailbox
}

struct Supervisor {
    spec: TaskSpec,
    /// The tool's input, until the task is cleaned.
    tool: Option<ToolInput>,
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
    /// The `tool.stdin` that arrived before the start was decided.
    held: Vec<Control<StdinPayload>>,
    /// The instructions that joined the task.
    joined: Vec<Id>,
    /// The task's final report, once submitted.
    final_report: Option<ReportId>,
    report_exhausted: bool,
    /// A compact terminal error, submitted only after all processes are gone.
    pending_failure: Option<Body>,
    /// The start was rejected and the registration withdrawn.
    withdrawn: bool,
    ending: Option<(Termination, Expiry)>,
    /// The shell's exit facts and when they were observed.
    exit: Option<(ExitFacts, Duration)>,
    /// What the `terminal` report said, once it was made.
    ended: Option<State>,
    /// The streams' counters once the task is cleaned.
    kept: (StreamSnapshot, StreamSnapshot),
    cancelled: bool,
    records: WriteRecords,
    /// The writes that wait for the one being written.
    writes: VecDeque<Control<StdinPayload>>,
    writing: Option<Writing>,
    waiters: Vec<Waiter>,
    /// Connection end, shutdown or report-capacity exhaustion stopped writes.
    stopped: bool,
    /// The App Server's shutdown has begun: the end of the connection that
    /// it brings ends the task through the termination procedure, with its
    /// grace.
    shutting_down: bool,
    /// The task's first process inside its sandbox, once seen.
    leader: Option<Leader>,
    /// The leader's PID namespace, held open until the task's processes are
    /// confirmed gone ([`process::hold_namespace`]).
    _namespace: Option<std::fs::File>,
    /// Whether the task's processes are confirmed gone.
    gone: bool,
    /// When they were last checked.
    checked: Duration,
    /// How long the drive check before the start took.
    drive_wait: Duration,
    /// A `view_image`'s `drive_sync`, from its drive check.
    image_drive_sync: Option<DriveSyncOutcome>,
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

/// The availability check while it runs.
type Checking = Pin<Box<dyn Future<Output = Result<Available, PolicyRejection>> + Send>>;

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
    /// A running result could not enter the retained-report budget.
    ReportCapacity,
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

/// How the time before the start ended.
enum Staged {
    /// The availability check ended with this result.
    Started(Result<Available, PolicyRejection>),
    /// A cancel or the binding's close prevented the start.
    Cancelled,
    /// The binding owner forgot the task, its connection ended or the App
    /// Server shuts down: it ends without a report.
    Discarded,
}

impl Supervisor {
    fn new(spec: TaskSpec, tool: ToolInput, context: Context) -> (Self, SupervisorMailbox) {
        let (mailbox, messages) = mpsc::unbounded_channel();
        let supervisor = Supervisor {
            spec,
            tool: Some(tool),
            context,
            listener: Arc::new(Listener(mailbox.downgrade())),
            messages,
            awaited: HashSet::new(),
            report_seq: 0,
            left_queue: None,
            process: None,
            spawned: Duration::ZERO,
            start: None,
            out: Stream::new(),
            err: Stream::new(),
            stdin: None,
            held: Vec::new(),
            joined: Vec::new(),
            final_report: None,
            report_exhausted: false,
            pending_failure: None,
            withdrawn: false,
            ending: None,
            exit: None,
            ended: None,
            kept: (exec::empty_stream(), exec::empty_stream()),
            cancelled: false,
            records: WriteRecords::default(),
            writes: VecDeque::new(),
            writing: None,
            waiters: Vec::new(),
            stopped: false,
            shutting_down: false,
            leader: None,
            _namespace: None,
            gone: false,
            checked: Duration::ZERO,
            drive_wait: Duration::ZERO,
            image_drive_sync: None,
        };
        (supervisor, mailbox)
    }

    async fn run(mut self) {
        let checked = match self.staged().await {
            Staged::Discarded => {
                self.confirm_gone();
                return;
            }
            Staged::Cancelled => None,
            Staged::Started(checked) => Some(checked),
        };
        // The held `tool.stdin` are handled once the start is decided: with
        // the launched task, or as of a task that has ended.
        match checked {
            None => self.release_held(),
            Some(Err(rejection)) => self.reject(rejection),
            Some(Ok(available)) => {
                let work = Work::of(self.input(), &self.spec.cwd);
                match self.launch(&available, work).await {
                    Err(Start::Rejected(rejection)) => self.reject(rejection),
                    Err(Start::Failed(error)) => {
                        self.report_start_failure(error).await;
                        self.release_held();
                    }
                    Ok((Work::Exec(call), status)) => {
                        self.release_held();
                        self.running(call, status).await;
                    }
                    Ok((Work::File(call), status)) => {
                        self.release_held();
                        self.file_running(call, status).await;
                    }
                }
            }
        }
        self.settle().await;
    }

    fn release_held(&mut self) {
        for control in std::mem::take(&mut self.held) {
            self.stdin(control);
        }
    }

    /// `staged`, then the availability check: the task's `tool.stdin` wait
    /// and its messages are read, until the check ends, a cancel or the
    /// binding's close prevents the start, or the task is discarded. A
    /// check that a message overtakes is dropped, its probes with it.
    async fn staged(&mut self) -> Staged {
        enum Step {
            Message(Option<SupervisorMessage>),
            Checked(Result<Available, PolicyRejection>),
            Drive(Option<DriveSyncOutcome>),
        }
        let mut check: Option<Checking> = None;
        let mut drive: Option<(Available, DriveAnswer, Duration)> = None;
        loop {
            let step = match (drive.as_mut(), check.as_mut()) {
                (Some((_, answer, _)), _) => tokio::select! {
                    drive_sync = answer.as_mut() => Step::Drive(drive_sync),
                    message = self.messages.recv() => Step::Message(message),
                },
                (None, Some(checking)) => tokio::select! {
                    checked = checking.as_mut() => Step::Checked(checked),
                    message = self.messages.recv() => Step::Message(message),
                },
                (None, None) => Step::Message(self.messages.recv().await),
            };
            let message = match step {
                Step::Message(message) => message,
                Step::Checked(Ok(available)) => match self.drive_check() {
                    Some(answer) => {
                        check = None;
                        drive = Some((available, answer, state::seams::now()));
                        continue;
                    }
                    None => return Staged::Started(Ok(available)),
                },
                Step::Checked(checked) => return Staged::Started(checked),
                Step::Drive(drive_sync) => {
                    let (available, _, began) = drive.take().expect("a drive check runs");
                    self.drive_wait = state::seams::now().saturating_sub(began);
                    self.image_drive_sync = drive_sync;
                    return Staged::Started(Ok(available));
                }
            };
            match message {
                None
                | Some(SupervisorMessage::ConnectionEnded)
                | Some(SupervisorMessage::Shutdown) => return Staged::Discarded,
                Some(SupervisorMessage::Start) => {
                    if check.is_none() {
                        self.left_queue = Some(state::seams::now());
                        check = Some(self.availability());
                    }
                }
                Some(SupervisorMessage::Cancel(control)) => {
                    self.left_queue.get_or_insert_with(state::seams::now);
                    if self.cancel(*control) {
                        self.report_cancelled_staged().await;
                    }
                    return Staged::Cancelled;
                }
                Some(SupervisorMessage::Prevent) => {
                    self.left_queue.get_or_insert_with(state::seams::now);
                    self.report_cancelled_staged().await;
                    return Staged::Cancelled;
                }
                Some(SupervisorMessage::Stdin(control)) => self.held.push(*control),
                Some(SupervisorMessage::Outcome(report, _)) => {
                    self.awaited.remove(&report);
                }
                Some(SupervisorMessage::Joined(instruction)) => self.join(instruction),
            }
        }
    }

    /// The drive check of an `exec_command` or `view_image` of a Session
    /// with a drive sync; an `edit` has none.
    fn drive_check(&self) -> Option<DriveAnswer> {
        let view_image = match self.input() {
            ToolInput::ExecCommand(_) => false,
            ToolInput::ViewImage(_) => true,
            ToolInput::Edit(_) => return None,
        };
        let port = self.spec.drive_sync.as_ref()?;
        Some(port.check(DriveCheck {
            task_id: self.spec.correlation.task_id.clone(),
            view_image,
        }))
    }

    /// The availability check of the task, borrowing nothing of the
    /// supervisor, which reads its messages while it runs.
    fn availability(&self) -> Checking {
        let isolation = self.context.isolation;
        let permissions = self.spec.permissions.clone();
        let cwd = self.spec.cwd.clone();
        let tetral = self.context.tetral.clone();
        Box::pin(async move {
            state::seams::barrier(AVAILABILITY_CHECK).await;
            isolation.check(&permissions, &cwd, &tetral).await
        })
    }

    /// The `rejected` report of a call that the availability check or the
    /// transform refused; no process of the task started. The registration
    /// is withdrawn, so each control of the task, held or later, gets
    /// `task_not_found` as one of a task never admitted.
    fn reject(&mut self, rejection: PolicyRejection) {
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
        self.withdrawn = true;
        self.spec.listener.withdrawn();
        for control in std::mem::take(&mut self.held) {
            self.control_error(&control, ControlErrorCode::TaskNotFound, TASK_NOT_FOUND);
        }
    }

    /// Checks ended processes until they are confirmed gone, then attempts
    /// file cleanup once every report has its outcome. Ownership of the same
    /// mailbox passes to the minimal finished task; this supervisor ends.
    async fn settle(mut self) {
        let mut open = true;
        self.check_gone();
        loop {
            if self.gone
                && let Some(body) = self.pending_failure.take()
            {
                self.submit_reserved(
                    self.spec.correlation.clone(),
                    self.spec.connection_id.clone(),
                    self.spec.traceparent.clone(),
                    body,
                );
            }
            if self.gone && self.awaited.is_empty() {
                self.clean();
                tokio::spawn(FinishedTask::from_supervisor(self).run());
                return;
            }
            if !open && self.gone {
                break;
            }
            let next_check = self.checked + self.context.bounds.gone_check;
            tokio::select! {
                message = self.messages.recv(), if open => match message {
                    None => open = false,
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::Joined(instruction)) => self.join(instruction),
                    Some(SupervisorMessage::Stdin(control)) if self.withdrawn => {
                        self.control_error(&control, ControlErrorCode::TaskNotFound, TASK_NOT_FOUND);
                    }
                    Some(SupervisorMessage::Cancel(control)) if self.withdrawn => {
                        self.control_error(&control, ControlErrorCode::TaskNotFound, TASK_NOT_FOUND);
                    }
                    Some(SupervisorMessage::Stdin(control)) => self.stdin(*control),
                    Some(SupervisorMessage::Cancel(control)) => {
                        if self.cancel(*control) {
                            self.report_cancelled_staged().await;
                        }
                    }
                    Some(
                        SupervisorMessage::Start
                        | SupervisorMessage::Prevent
                        | SupervisorMessage::ConnectionEnded
                        | SupervisorMessage::Shutdown,
                    ) => {}
                },
                () = state::seams::sleep_until(next_check), if !self.gone => {
                    self.check_gone();
                }
            }
        }
        self.clean();
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
        self._namespace = None;
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
        self._namespace = process::hold_namespace(leader.pid, leader.namespace);
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
        for (part, result) in self.spec.files.remove() {
            if let Err(error) = result {
                observe::warn!(
                    "task.cleanup_failed",
                    task.id = observe::Id(self.spec.correlation.task_id.as_str()),
                    cleanup.part = observe::Id(part),
                    cleanup.directory = observe::Id(
                        error
                            .path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("unknown")
                    ),
                    error.os_code = error.source.raw_os_error().unwrap_or(0),
                    error.code = observe::Code("task_cleanup_failed")
                );
            }
        }
        self.kept = (self.out.capture.counters(), self.err.capture.counters());
        self.out.capture = StreamCapture::new();
        self.err.capture = StreamCapture::new();
        self.tool = None;
    }

    /// The tool's input, which only the start and the reports of a task
    /// that never ran read, all before the cleanup.
    fn input(&self) -> &ToolInput {
        self.tool
            .as_ref()
            .expect("the input is kept until the cleanup")
    }

    /// The rest of `starting`, after the availability check: the task's
    /// files, the environment and the launch; gives the work and the
    /// launch-status pipe.
    async fn launch(
        &mut self,
        available: &Available,
        work: Work,
    ) -> Result<(Work, Receiver), Start> {
        let context = &self.context;
        let spec = &self.spec;
        let failed = |message: &str| Start::Failed(start_error(&work, message));
        spec.files
            .create()
            .map_err(|failure| failed(&failure.to_string()))?;
        let env = match &work {
            Work::Exec(call) => Some(
                crate::env::command_environment(
                    std::env::vars_os(),
                    call.env.as_ref(),
                    spec.files.temporary(),
                    spec.drive_sync
                        .as_ref()
                        .and_then(|port| port.drives_variable())
                        .as_deref(),
                )
                .map_err(|_| failed(NOT_STARTED))?,
            ),
            Work::File(_) => None,
        };
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
                available,
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
            self.out.open(process.child.stdout.take());
            self.err.open(process.child.stderr.take());
        }
        self.process = Some(process);
        Ok((work, status))
    }

    /// The `terminal` report of a command that never started: `status:
    /// error`, null exit facts and empty streams; of a file tool, its
    /// error.
    async fn report_start_failure(&mut self, error: ToolError) {
        if let Some(call) = FileCall::of(self.input()) {
            self.report_file(crate::child::failed(call.tool(), error), Duration::ZERO)
                .await;
            return;
        }
        let outcome = self.outcome(HelperStatus::Error, Some(error), ExitFacts::default());
        let empty = StreamCapture::new();
        self.report(ReportPhase::Terminal, outcome, &empty, &empty)
            .await;
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
                            self.context.activity.exec_started();
                            self.find_leader(pid);
                            bytes = rest;
                        }
                        let room = bounds.launch_status_max_bytes.saturating_sub(failure.len());
                        failure.extend_from_slice(&bytes[..bytes.len().min(room)]);
                    }
                    None => status = None,
                },
                waited_status = wait_child(self.process.as_mut()), if self.exit.is_none() => {
                    let facts = waited_status.map_or_else(|_| ExitFacts::default(), ExitFacts::observed);
                    self.exit = Some((facts, state::seams::now()));
                    self.exec_exited();
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
                        (Some(start), OnExpiry::Detach) => self.report_running(start).await,
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
                    Some(SupervisorMessage::Cancel(control)) => {
                        if self.cancel(*control) {
                            self.report_cancelled_staged().await;
                        }
                    }
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::Joined(instruction)) => self.join(instruction),
                    Some(SupervisorMessage::ConnectionEnded) => self.stop(pid, Expiry::ConnectionEnded),
                    Some(SupervisorMessage::Shutdown) => self.stop(pid, Expiry::Shutdown),
                    Some(SupervisorMessage::Prevent) => self.binding_closed(pid),
                    Some(SupervisorMessage::Start) => {}
                    None => forgotten = true,
                },
            }
        }
        drop((self.out.reader.take(), self.err.reader.take()));
        if self.exit.is_none() {
            self.exit = Some((ExitFacts::default(), state::seams::now()));
            self.exec_exited();
        }
        self.exited();
        if self.writing.is_some() {
            self.write_failed();
        }
        self.stdin = None;
        self.next_write();
        let expiry = self.ending.map(|(_, expiry)| expiry);
        if !failure.is_empty() {
            self.report_start_failure(launch_failure(&failure)).await;
            return;
        }
        if expiry == Some(Expiry::NotStarted) {
            self.report_start_failure(exec::tool_error(
                "exec_spawn_failed",
                NOT_STARTED_IN_WAIT,
                None,
            ))
            .await;
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
        self.report(ReportPhase::Terminal, outcome, &out, &err)
            .await;
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
                    Some(SupervisorMessage::Cancel(control)) => {
                        if self.cancel(*control) {
                            self.report_cancelled_staged().await;
                        }
                    }
                    Some(SupervisorMessage::Outcome(report, _)) => {
                        self.awaited.remove(&report);
                    }
                    Some(SupervisorMessage::Joined(instruction)) => self.join(instruction),
                    Some(SupervisorMessage::ConnectionEnded) => self.stop(pid, Expiry::ConnectionEnded),
                    Some(SupervisorMessage::Shutdown) => self.stop(pid, Expiry::Shutdown),
                    Some(SupervisorMessage::Prevent) => self.binding_closed(pid),
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
        self.report_file(envelope, run).await;
    }

    /// Hands a file tool's `terminal` report, with its `metrics` for work
    /// that ran for `run`, to the outbox; the task has then ended. An
    /// `edit` result's `drive_sync` comes from its sync point, a
    /// `view_image` result's from its drive check.
    async fn report_file(&mut self, mut envelope: HelperEnvelope, run: Duration) {
        let mut metrics = self.metrics(run);
        envelope.drive_sync = match envelope.tool {
            HelperTool::ViewImage => self.image_drive_sync.take(),
            _ => {
                let (drive_sync, took) = self.sync_point(false, ReportPhase::Terminal).await;
                metrics.sync_ms = exec::millis(took);
                metrics.wall_time_ms =
                    exec::millis(state::seams::now().saturating_sub(self.spec.received));
                drive_sync
            }
        };
        envelope.metrics = Some(metrics);
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
    async fn report_running(&mut self, start: Duration) {
        let (stdout, stderr) = (
            self.out.capture.take_window(),
            self.err.capture.take_window(),
        );
        let mut outcome = self.outcome(HelperStatus::Running, None, ExitFacts::default());
        outcome.duration = state::seams::now().saturating_sub(start);
        outcome.metrics = None;
        self.report(ReportPhase::Running, outcome, &stdout, &stderr)
            .await;
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
            drive_sync: None,
        }
    }

    /// The sync point of the task's next execution report, with how long it
    /// took; none without a drive sync.
    fn sync_point(
        &self,
        first: bool,
        phase: ReportPhase,
    ) -> impl Future<Output = (Option<DriveSyncOutcome>, Duration)> + Send + use<> {
        let running = phase == ReportPhase::Running;
        let answer = self.spec.drive_sync.as_ref().map(|port| {
            port.sync_point(DriveSyncPoint {
                task_id: self.spec.correlation.task_id.clone(),
                first,
                running,
                wait: if running { Duration::ZERO } else { SYNC_WAIT },
                traceparent: self.spec.traceparent.clone(),
            })
        });
        let began = state::seams::now();
        async move {
            let Some(answer) = answer else {
                return (None, Duration::ZERO);
            };
            let drive_sync = answer.await;
            (drive_sync, state::seams::now().saturating_sub(began))
        }
    }

    /// Runs the report's sync point, renders both streams with the task's
    /// limits into `outcome`, fits the report and hands it to the outbox.
    async fn report(
        &mut self,
        phase: ReportPhase,
        mut outcome: Outcome,
        stdout: &(dyn StreamOutput + Sync),
        stderr: &(dyn StreamOutput + Sync),
    ) {
        let (drive_sync, took) = self.sync_point(self.report_seq == 0, phase).await;
        outcome.drive_sync = drive_sync;
        if let Some(metrics) = outcome.metrics.as_mut() {
            metrics.sync_ms = exec::millis(took);
            metrics.wall_time_ms =
                exec::millis(state::seams::now().saturating_sub(self.spec.received));
        }
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

    /// Hands a report to the outbox; the cleanup waits for its outcome. The
    /// task's final report also ends the instructions that joined it.
    fn submit(
        &mut self,
        correlation: Correlation,
        connection_id: &Id,
        traceparent: Option<String>,
        body: Body,
    ) {
        // Control errors and rejected admissions are already compact. Ordinary
        // results must not consume the reserve before their owner handles failure.
        if matches!(body, Body::Rejected(_) | Body::Control { result: None, .. }) {
            self.submit_reserved(correlation, connection_id.clone(), traceparent, body);
            return;
        }
        let running = matches!(
            body,
            Body::Result {
                phase: ReportPhase::Running,
                ..
            }
        );
        let terminal = matches!(
            body,
            Body::Result {
                phase: ReportPhase::Terminal,
                ..
            }
        );
        let fallback = capacity_error(&body);
        if terminal && self.report_exhausted {
            drop(body);
            self.tool = None;
            self.pending_failure = Some(fallback);
            return;
        }
        let report = Report {
            correlation: correlation.clone(),
            body,
        };
        let submission = self.submission(report, connection_id.clone(), traceparent.clone());
        match self.context.reports.try_submit(submission) {
            Ok(id) => self.track(id, terminal),
            Err(_) if running => {
                // Admission failed before publication. The eventual terminal
                // error is the first report and must still have sequence 1.
                self.report_seq -= 1;
                self.report_exhausted = true;
                let pid = self.process.as_ref().map_or(0, |process| process.pid);
                self.stop(pid, Expiry::ReportCapacity);
            }
            Err(_) if terminal => {
                self.tool = None;
                self.pending_failure = Some(fallback);
            }
            Err(_) => {
                self.submit_reserved(correlation, connection_id.clone(), traceparent, fallback)
            }
        }
    }

    fn submission(
        &self,
        report: Report,
        connection: Id,
        trace: Option<String>,
    ) -> protocol::ports::Submission {
        let mut submission = report.submission(connection);
        if submission.final_for.first() == Some(&self.spec.correlation.request_message_id) {
            submission.final_for.extend(self.joined.iter().cloned());
        }
        submission.traceparent = trace;
        submission.listener = Some(self.listener.clone());
        submission
    }

    fn submit_reserved(
        &mut self,
        correlation: Correlation,
        connection: Id,
        trace: Option<String>,
        body: Body,
    ) {
        let report = Report { correlation, body };
        let task_final = report.is_final()
            && report.correlation.request_message_id == self.spec.correlation.request_message_id;
        let submission = self.submission(report, connection, trace);
        let id = self.context.reports.submit(submission);
        self.track(id, task_final);
    }

    fn track(&mut self, id: ReportId, task_final: bool) {
        if task_final {
            self.final_report = Some(id);
        }
        self.awaited.insert(id);
    }

    /// An identical `tool.execute` joined the task.
    fn join(&mut self, instruction: Id) {
        match self.final_report {
            Some(report) => self.context.reports.add_final(report, instruction),
            None => self.joined.push(instruction),
        }
    }

    /// The metrics of a report built now, for a command that ran for `run`.
    fn metrics(&self, run: Duration) -> Metrics {
        let now = state::seams::now();
        let left_queue = self.left_queue.unwrap_or(now);
        Metrics {
            wall_time_ms: exec::millis(now.saturating_sub(self.spec.received)),
            queued_ms: exec::millis(left_queue.saturating_sub(self.spec.received)),
            drive_wait_ms: exec::millis(self.drive_wait),
            run_ms: exec::millis(run),
            sync_ms: exec::millis(Duration::ZERO),
        }
    }

    /// The shell's exit has been observed: every waiting reply is made.
    /// An `exec_command` whose command started has exited, at the exit's
    /// time.
    fn exec_exited(&self) {
        if let (Some(_), Some((_, at))) = (self.start, &self.exit) {
            self.context.activity.exec_exited(*at);
        }
    }

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
                if self.state().ended
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

    /// A `tool.cancel`, recorded and answered at once; whether it found the
    /// task staged, whose `terminal` report the caller then makes.
    #[must_use]
    fn cancel(&mut self, control: Control<CancelPayload>) -> bool {
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
            return true;
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
        false
    }

    /// The task's binding closes while it runs: as at a cancel, writes stop
    /// and the termination begins, and its `terminal` report says
    /// `cancelled`.
    fn binding_closed(&mut self, pid: u32) {
        if self.exit.is_some() || self.ending.is_some() {
            return;
        }
        self.cancelled = true;
        self.write_interrupted();
        self.ending = Some((
            Termination::begin(pid, self.context.bounds.termination),
            Expiry::Cancel,
        ));
    }

    /// Connection end, App Server shutdown or report-capacity exhaustion:
    /// writes stop as at a cancel, and termination ends the task, at a
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

    /// The `terminal` report of a task cancelled, or whose binding closed,
    /// before it started.
    async fn report_cancelled_staged(&mut self) {
        if let Some(call) = FileCall::of(self.input()) {
            self.report_file(call.cancelled(false), Duration::ZERO)
                .await;
            return;
        }
        let mut outcome = self.outcome(HelperStatus::Success, None, ExitFacts::default());
        outcome.cancelled = Some(true);
        let empty = StreamCapture::new();
        self.report(ReportPhase::Terminal, outcome, &empty, &empty)
            .await;
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
            drive_sync: None,
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
        self.reply_synced(
            control,
            outcome,
            Box::new(stdout),
            Box::new(stderr),
            limits,
            None,
        );
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
        self.reply_synced(
            control,
            outcome,
            Box::new(stdout),
            Box::new(stderr),
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
        let (stdout, stderr) = (
            Counters(self.out.capture.counters()),
            Counters(self.err.capture.counters()),
        );
        let error = closed.then(closed_error);
        self.reply_synced(
            control,
            outcome,
            Box::new(stdout),
            Box::new(stderr),
            limits,
            error,
        );
    }

    fn control_error<P>(&mut self, control: &Control<P>, code: ControlErrorCode, message: &str) {
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
        outcome: Outcome,
        stdout: &dyn StreamOutput,
        stderr: &dyn StreamOutput,
        limits: Visible,
        error: Option<ProtocolError<ControlErrorCode>>,
    ) {
        let report = control_report(
            control.correlation.clone(),
            &control.connection_id,
            outcome,
            stdout,
            stderr,
            limits,
            error,
        );
        self.submit(
            report.correlation,
            &control.connection_id,
            control.traceparent.clone(),
            report.body,
        );
    }

    /// A newly built `tool.stdin` reply: with a drive sync, it is frozen
    /// once its sync point answers, while the supervisor goes on.
    fn reply_synced(
        &mut self,
        control: &Control<StdinPayload>,
        outcome: Outcome,
        stdout: Box<dyn StreamOutput + Send + Sync>,
        stderr: Box<dyn StreamOutput + Send + Sync>,
        limits: Visible,
        error: Option<ProtocolError<ControlErrorCode>>,
    ) {
        let Some(port) = self.spec.drive_sync.clone() else {
            return self.reply(control, outcome, &*stdout, &*stderr, limits, error);
        };
        let ended = self.state().ended;
        spawn_synced_reply(
            SyncedReply {
                reports: self.context.reports.clone(),
                port,
                ended,
                control: Control {
                    connection_id: control.connection_id.clone(),
                    correlation: control.correlation.clone(),
                    traceparent: control.traceparent.clone(),
                    payload: (),
                },
                task_id: self.spec.correlation.task_id.clone(),
                limits,
                error,
            },
            outcome,
            stdout,
            stderr,
        );
    }
}

/// A control reply of `outcome`, its streams rendered with `limits` and
/// fitted.
fn control_report(
    correlation: Correlation,
    connection_id: &Id,
    mut outcome: Outcome,
    stdout: &dyn StreamOutput,
    stderr: &dyn StreamOutput,
    limits: Visible,
    error: Option<ProtocolError<ControlErrorCode>>,
) -> Report {
    outcome.stdout = stdout.snapshot(limits.bytes, limits.lines);
    outcome.stderr = stderr.snapshot(limits.bytes, limits.lines);
    let mut report = Report {
        correlation,
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
        connection_id,
        &Rendering {
            stdout,
            stderr,
            visible_bytes: limits.bytes,
            visible_lines: limits.lines,
        },
    );
    report
}

/// What a `tool.stdin` reply waiting for its sync point is sent with.
struct SyncedReply {
    reports: Arc<dyn ReportSink>,
    port: Arc<dyn DriveSyncPort>,
    /// Whether the task had ended when the reply was built.
    ended: bool,
    control: Control<()>,
    task_id: Id,
    limits: Visible,
    error: Option<ProtocolError<ControlErrorCode>>,
}

/// Runs the reply's sync point, then fits the reply with its `drive_sync`
/// and hands it to the outbox, as a compact error when it does not fit the
/// report budget.
fn spawn_synced_reply(
    reply: SyncedReply,
    mut outcome: Outcome,
    stdout: Box<dyn StreamOutput + Send + Sync>,
    stderr: Box<dyn StreamOutput + Send + Sync>,
) {
    let point = DriveSyncPoint {
        task_id: reply.task_id.clone(),
        first: false,
        running: outcome.status == HelperStatus::Running,
        wait: if reply.ended {
            SYNC_WAIT
        } else {
            Duration::ZERO
        },
        traceparent: reply.control.traceparent.clone(),
    };
    let answer = reply.port.sync_point(point);
    tokio::spawn(
        async move {
            outcome.drive_sync = answer.await;
            let report = control_report(
                reply.control.correlation.clone(),
                &reply.control.connection_id,
                outcome,
                &*stdout,
                &*stderr,
                reply.limits,
                reply.error,
            );
            send_reply(&*reply.reports, &reply.control, report.body, false);
        }
        .instrument(tracing::Span::current()),
    );
}

/// Hands a control reply to the outbox; one that does not fit the report
/// budget goes as its compact error, which the reserve takes.
fn send_reply<P>(reports: &dyn ReportSink, control: &Control<P>, body: Body, reserved: bool) {
    let fallback = capacity_error(&body);
    let submission = |body| {
        let mut value = Report {
            correlation: control.correlation.clone(),
            body,
        }
        .submission(control.connection_id.clone());
        value.traceparent = control.traceparent.clone();
        value
    };
    if reserved {
        reports.submit(submission(body));
    } else if reports.try_submit(submission(body)).is_err() {
        reports.submit(submission(fallback));
    }
}

const REPORT_CAPACITY_MESSAGE: &str = "the result could not be retained because the local report budget is exhausted; the tool may have performed work";

/// Retains only facts already observed, never the rejected output body.
fn capacity_error(body: &Body) -> Body {
    match body {
        Body::Result {
            phase,
            report_seq,
            result,
        } => Body::Result {
            phase: *phase,
            report_seq: *report_seq,
            result: Box::new(capacity_envelope(result)),
        },
        Body::Control { result, .. } => Body::Control {
            result: result.as_deref().map(capacity_envelope).map(Box::new),
            error: Some(ProtocolError {
                code: ControlErrorCode::ResourceExhausted,
                message: REPORT_CAPACITY_MESSAGE.to_owned(),
            }),
        },
        Body::Rejected(error) => Body::Rejected(error.clone()),
    }
}

fn capacity_envelope(envelope: &HelperEnvelope) -> HelperEnvelope {
    let result = match &envelope.result {
        ToolResult::Exec(result) => ToolResult::Exec(protocol::tool::ExecResult {
            task_id: result.task_id.clone(),
            exit_code: result.exit_code.clone(),
            signal: result.signal.clone(),
            timed_out: result.timed_out,
            cancelled: result.cancelled,
            was_running: result.was_running,
            written_bytes: result.written_bytes,
            stdout: stream_counters(&result.stdout),
            stderr: stream_counters(&result.stderr),
            duration_ms: result.duration_ms,
        }),
        _ => ToolResult::Object(Default::default()),
    };
    HelperEnvelope {
        tool: envelope.tool,
        status: HelperStatus::Error,
        truncated: false,
        error: Some(exec::tool_error(
            "resource_exhausted",
            REPORT_CAPACITY_MESSAGE,
            None,
        )),
        result,
        metrics: envelope.metrics.clone(),
        drive_sync: None,
    }
}

fn stream_counters(stream: &StreamSnapshot) -> StreamSnapshot {
    StreamSnapshot {
        text: String::new(),
        total_bytes: stream.total_bytes,
        total_lines: stream.total_lines,
        returned_bytes: protocol::field::Count::saturating(0),
        truncated: false,
    }
}

/// The protocol record that outlives execution. It owns no process, task
/// specification, sandbox policy, output buffer or execution tracing span.
struct FinishedTask {
    messages: mpsc::UnboundedReceiver<SupervisorMessage>,
    reports: Arc<dyn ReportSink>,
    drive_sync: Option<Arc<dyn DriveSyncPort>>,
    task_id: Id,
    state: State,
    counters: (StreamSnapshot, StreamSnapshot),
    records: WriteRecords,
    withdrawn: bool,
}

impl FinishedTask {
    fn from_supervisor(supervisor: Supervisor) -> Self {
        let state = supervisor.state();
        Self {
            messages: supervisor.messages,
            reports: supervisor.context.reports,
            drive_sync: supervisor.spec.drive_sync,
            task_id: supervisor.spec.correlation.task_id,
            state,
            counters: supervisor.kept,
            records: supervisor.records,
            withdrawn: supervisor.withdrawn,
        }
    }

    async fn run(mut self) {
        state::seams::count("task.execution_released");
        while let Some(message) = self.messages.recv().await {
            match message {
                SupervisorMessage::Stdin(control) => self.stdin(*control),
                SupervisorMessage::Cancel(control) => {
                    if self.withdrawn {
                        self.error(&control, ControlErrorCode::TaskNotFound, TASK_NOT_FOUND);
                    } else {
                        let mut outcome = self.outcome(HelperTool::Cancel, HelperStatus::Success);
                        outcome.cancelled = Some(false);
                        outcome.was_running = Some(false);
                        // Cleared cumulative output has the same empty counters
                        // that the old cleaned supervisor returned for cancel.
                        outcome.stdout = exec::empty_stream();
                        outcome.stderr = exec::empty_stream();
                        self.reply(&control, outcome, None);
                    }
                }
                // Joined final reports are already gone. No terminal message
                // can restart execution or reconstruct an acknowledged body.
                SupervisorMessage::Start
                | SupervisorMessage::Prevent
                | SupervisorMessage::Joined(_)
                | SupervisorMessage::Outcome(_, _)
                | SupervisorMessage::ConnectionEnded
                | SupervisorMessage::Shutdown => {}
            }
        }
    }

    fn stdin(&self, control: Control<StdinPayload>) {
        if self.withdrawn {
            self.error(&control, ControlErrorCode::TaskNotFound, TASK_NOT_FOUND);
            return;
        }
        if let Request::Write {
            write_seq, digest, ..
        } = Request::of(&control.payload)
            && let Check::Repeat(record) = self.records.check(write_seq, &digest)
        {
            let closed = record.error == Some(ControlErrorCode::StdinClosed);
            let mut outcome = self.outcome(
                HelperTool::Stdin,
                if closed {
                    HelperStatus::Error
                } else {
                    HelperStatus::Success
                },
            );
            outcome.written_bytes = Some(record.written_bytes);
            if closed {
                outcome.error = Some(exec::tool_error("stdin_closed", STDIN_CLOSED, None));
            }
            let error = closed.then(closed_error);
            match &self.drive_sync {
                Some(port) => {
                    let (stdout, stderr) = (
                        Counters(outcome.stdout.clone()),
                        Counters(outcome.stderr.clone()),
                    );
                    spawn_synced_reply(
                        SyncedReply {
                            reports: self.reports.clone(),
                            port: port.clone(),
                            ended: true,
                            control: Control {
                                connection_id: control.connection_id.clone(),
                                correlation: control.correlation.clone(),
                                traceparent: control.traceparent.clone(),
                                payload: (),
                            },
                            task_id: self.task_id.clone(),
                            limits: Visible::requested(control.payload.limits.as_ref()),
                            error,
                        },
                        outcome,
                        Box::new(stdout),
                        Box::new(stderr),
                    );
                }
                None => self.reply(&control, outcome, error),
            }
        } else {
            self.error(
                &control,
                ControlErrorCode::ResultUnavailable,
                "the task's output is no longer kept",
            );
        }
    }

    fn outcome(&self, tool: HelperTool, status: HelperStatus) -> Outcome {
        Outcome {
            tool,
            task_id: self.task_id.clone(),
            status,
            error: None,
            exit: self.state.exit.clone(),
            timed_out: self.state.timed_out,
            stdout: self.counters.0.clone(),
            stderr: self.counters.1.clone(),
            duration: self.state.duration,
            metrics: None,
            cancelled: None,
            was_running: None,
            written_bytes: None,
            drive_sync: None,
        }
    }

    fn error<P>(&self, control: &Control<P>, code: ControlErrorCode, message: &str) {
        let body = Body::Control {
            result: None,
            error: Some(ProtocolError {
                code,
                message: message.to_owned(),
            }),
        };
        self.send(control, body, true);
    }

    fn reply<P>(
        &self,
        control: &Control<P>,
        outcome: Outcome,
        error: Option<ProtocolError<ControlErrorCode>>,
    ) {
        self.send(
            control,
            Body::Control {
                result: Some(Box::new(outcome.envelope())),
                error,
            },
            false,
        );
    }

    fn send<P>(&self, control: &Control<P>, body: Body, reserved: bool) {
        send_reply(&*self.reports, control, body, reserved);
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
    let mut buffer = [0u8; LAUNCH_STATUS_READ_BYTES];
    match pipe.read(&mut buffer).await {
        Ok(0) | Err(_) => None,
        Ok(count) => Some(buffer[..count].to_vec()),
    }
}

/// One of the command's output pipes, read into its capture and its log.
struct Stream {
    reader: Option<Box<dyn AsyncRead + Unpin + Send>>,
    capture: StreamCapture,
    /// The read buffer, held only while the pipe is open.
    buffer: Vec<u8>,
    /// Whether a write of its log failed; the log is then left alone.
    log_failed: bool,
}

impl Stream {
    fn new() -> Stream {
        Stream {
            reader: None,
            capture: StreamCapture::new(),
            buffer: Vec::new(),
            log_failed: false,
        }
    }

    /// Reads `reader` from now on, when the command has the pipe.
    fn open<R: AsyncRead + Unpin + Send + 'static>(&mut self, reader: Option<R>) {
        if let Some(reader) = reader {
            self.reader = Some(Box::new(reader));
            self.buffer = vec![0; READ_CHUNK_BYTES];
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
                self.buffer = Vec::new();
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

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::ports::{ReportAdmissionError, Submission};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Recorded {
        id: ReportId,
        body: serde_json::Value,
        listener: Option<Arc<dyn ReportListener>>,
    }

    struct Reports {
        next: AtomicU64,
        full: bool,
        sent: mpsc::UnboundedSender<Recorded>,
    }

    impl ReportSink for Reports {
        fn submit(&self, submission: Submission) -> ReportId {
            let id = ReportId(self.next.fetch_add(1, Ordering::Relaxed));
            let message = Id::new(format!("r{}", id.0)).unwrap();
            let bytes = (submission.build)(&submission.connection_id, &message).unwrap();
            self.sent
                .send(Recorded {
                    id,
                    body: serde_json::from_slice(&bytes).unwrap(),
                    listener: submission.listener,
                })
                .unwrap();
            id
        }
        fn try_submit(&self, submission: Submission) -> Result<ReportId, ReportAdmissionError> {
            if self.full {
                Err(ReportAdmissionError::ResourceExhausted)
            } else {
                Ok(self.submit(submission))
            }
        }
        fn cancel(&self, _: ReportId) {}
        fn add_final(&self, _: ReportId, _: Id) {}
    }

    struct TaskEvents(mpsc::UnboundedSender<()>);
    impl TaskListener for TaskEvents {
        fn started(&self, _: u64) {
            panic!("this fixture never launches a process")
        }
        fn gone(&self) {
            self.0.send(()).unwrap();
        }
        fn withdrawn(&self) {}
    }

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn correlation(request: &str) -> Correlation {
        Correlation {
            session_id: id("session"),
            binding_id: id("binding"),
            tool_use_event_id: id("event"),
            task_id: id("task"),
            request_message_id: id(request),
        }
    }

    fn staged_supervisor(
        root: &std::path::Path,
        full: bool,
    ) -> (
        Supervisor,
        SupervisorMailbox,
        mpsc::UnboundedReceiver<Recorded>,
        mpsc::UnboundedReceiver<()>,
        std::sync::Weak<TaskEvents>,
    ) {
        let (sent, received) = mpsc::unbounded_channel();
        let (gone, gone_received) = mpsc::unbounded_channel();
        let listener = Arc::new(TaskEvents(gone));
        let witness = Arc::downgrade(&listener);
        let policy =
            json!({"schema_version":1,"filesystem":{"rules":[]},"network":{"mode":"disabled"}});
        let inputs = crate::policy::TranslationInputs {
            state_root: root.join("state"),
            socket_directory: root.join("socket"),
            task_directories: root.join("temporary"),
            task_temporary_directory: root.join("temporary/task"),
            drives: None,
            trusted_programs: Vec::new(),
        };
        let spec = TaskSpec {
            connection_id: id("connection"),
            correlation: correlation("execute"),
            permissions: TaskPermissions::admit(Some(&policy), &inputs).unwrap(),
            limits: Visible::requested(None),
            cwd: root.to_owned(),
            files: TaskFiles::new(root.join("task"), root.join("temporary/task"), 1024),
            traceparent: None,
            received: state::seams::now(),
            record: root.join("record"),
            listener,
            drive_sync: None,
        };
        let context = Context {
            tetral: root.join("unused"),
            isolation: Isolation::new(),
            reports: Arc::new(Reports {
                next: AtomicU64::new(1),
                full,
                sent,
            }),
            bounds: Bounds::default(),
            boot_id: None,
            activity: Arc::new(protocol::ports::NoActivity),
        };
        let tool: ToolInput =
            serde_json::from_value(json!({"tool":"exec_command","input":{"cmd":"unused"}}))
                .unwrap();
        let (supervisor, mailbox) = Supervisor::new(spec, tool, context);
        (supervisor, mailbox, received, gone_received, witness)
    }

    fn staged_task(
        root: &std::path::Path,
        full: bool,
    ) -> (
        SupervisorMailbox,
        mpsc::UnboundedReceiver<Recorded>,
        mpsc::UnboundedReceiver<()>,
        std::sync::Weak<TaskEvents>,
    ) {
        let (supervisor, mailbox, reports, gone, witness) = staged_supervisor(root, full);
        tokio::spawn(supervisor.run());
        (mailbox, reports, gone, witness)
    }

    // The shell confirms that it is executing, then blocks on its own stdin.
    // It has no descendants and lives in a dedicated process group. kill_on_drop
    // bounds cleanup even if an assertion or a mutation's watchdog fails.
    async fn live_command(supervisor: &mut Supervisor) -> tokio::process::ChildStdin {
        use std::process::Stdio;
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", "printf ready; read release"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut ready = [0; 5];
        tokio::time::timeout(
            Duration::from_secs(2),
            child.stdout.take().unwrap().read_exact(&mut ready),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&ready, b"ready");
        assert!(child.try_wait().unwrap().is_none());
        let release = child.stdin.take().unwrap();
        let pid = child.id().unwrap();
        supervisor.process = Some(Process { child, pid });
        supervisor.spawned = state::seams::now();
        supervisor.start = Some(supervisor.spawned);
        release
    }

    /// A drive sync that records each sync point and answers with one
    /// committed write, and a download for a `running` result.
    #[derive(Debug, Default)]
    struct Points(std::sync::Mutex<Vec<DriveSyncPoint>>);

    impl DriveSyncPort for Points {
        fn check(&self, _: DriveCheck) -> DriveAnswer {
            Box::pin(async { None })
        }

        fn sync_point(&self, point: DriveSyncPoint) -> DriveAnswer {
            let mut outcome = json!({
                "committed": 1, "deleted": 0, "rejected": [], "not_synced": [],
            });
            if point.running {
                outcome["downloading"] = json!([{
                    "path": "/state/sessions/session/drives/drive/big.log",
                    "downloaded_bytes": 1, "size_bytes": 2,
                }]);
            }
            self.0.lock().unwrap().push(point);
            Box::pin(async move { Some(serde_json::from_value(outcome).unwrap()) })
        }

        fn drives_variable(&self) -> Option<PathBuf> {
            None
        }
    }

    #[tokio::test]
    async fn each_result_and_stdin_reply_carries_the_drive_sync_of_its_own_sync_point() {
        let root = tempfile::tempdir().unwrap();
        let (mut supervisor, _mailbox, mut reports, _gone, _) =
            staged_supervisor(root.path(), false);
        let points = Arc::new(Points::default());
        supervisor.spec.drive_sync = Some(points.clone());
        let _release = live_command(&mut supervisor).await;
        let start = supervisor.start.unwrap();
        supervisor.report_running(start).await;
        let running = reports.recv().await.unwrap();
        let read: Control<StdinPayload> = Control {
            connection_id: id("connection"),
            correlation: correlation("read"),
            traceparent: None,
            payload: serde_json::from_value(json!({
                "session_id": "session", "binding_id": "binding",
                "tool_use_event_id": "event", "task_id": "task", "chars": "",
            }))
            .unwrap(),
        };
        supervisor.reply_window(&read, None);
        let reply = reports.recv().await.unwrap();
        let mut outcome = supervisor.outcome(HelperStatus::Success, None, ExitFacts::default());
        outcome.metrics = Some(supervisor.metrics(Duration::ZERO));
        let empty = StreamCapture::new();
        supervisor
            .report(ReportPhase::Terminal, outcome, &empty, &empty)
            .await;
        let terminal = reports.recv().await.unwrap();

        let seen: Vec<(bool, bool, Duration)> = points
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|point| (point.first, point.running, point.wait))
            .collect();
        assert_eq!(
            seen,
            [
                (true, true, Duration::ZERO),
                (false, true, Duration::ZERO),
                (false, false, SYNC_WAIT),
            ]
        );
        for recorded in [&running, &reply, &terminal] {
            protocol::decode::<protocol::tool::ToolStatus>(
                &serde_json::to_vec(&recorded.body).unwrap(),
            )
            .unwrap_or_else(|error| panic!("{error}: {}", recorded.body));
            assert_eq!(recorded.body["result"]["drive_sync"]["committed"], 1);
        }
        assert!(running.body["result"]["drive_sync"]["downloading"].is_array());
        assert!(reply.body["result"]["drive_sync"]["downloading"].is_array());
        assert!(
            terminal.body["result"]["drive_sync"]
                .get("downloading")
                .is_none()
        );
    }

    #[tokio::test]
    async fn refused_running_report_stops_the_live_command_and_preserves_first_report_sequence() {
        let root = tempfile::tempdir().unwrap();
        let (mut supervisor, mailbox, mut reports, mut gone, _) =
            staged_supervisor(root.path(), true);
        let _release = live_command(&mut supervisor).await;
        let pid = supervisor.process.as_ref().unwrap().pid;
        // The command has started and its initial wait has expired under detach.
        let mut call = match Work::of(supervisor.input(), root.path()) {
            Work::Exec(call) => call,
            _ => unreachable!(),
        };
        call.wait = Duration::ZERO;
        call.on_expiry = OnExpiry::Detach;
        let (writer, status) = tokio::net::unix::pipe::pipe().unwrap();
        drop(writer);
        tokio::time::timeout(Duration::from_secs(3), supervisor.running(call, status))
            .await
            .expect("refusing the running report must end the command");
        let exited = supervisor
            .process
            .as_mut()
            .unwrap()
            .child
            .try_wait()
            .unwrap();
        assert!(
            exited.is_some(),
            "the command must be reaped before settlement"
        );
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        assert!(
            reports.try_recv().is_err(),
            "no running report was published"
        );
        let owner = tokio::spawn(supervisor.settle());
        let terminal = tokio::time::timeout(Duration::from_secs(2), reports.recv())
            .await
            .unwrap()
            .unwrap();
        gone.try_recv()
            .expect("disappearance is confirmed before publication");
        assert_eq!(terminal.body["phase"], "terminal");
        assert_eq!(terminal.body["report_seq"], 1);
        assert_eq!(
            terminal.body["result"]["error"]["kind"],
            "resource_exhausted"
        );
        terminal
            .listener
            .unwrap()
            .outcome(terminal.id, ReportOutcome::Acked);
        tokio::time::timeout(Duration::from_secs(2), owner)
            .await
            .unwrap()
            .unwrap();
        drop(mailbox);
        assert!(reports.recv().await.is_none());
    }

    #[tokio::test]
    async fn capacity_error_waits_for_confirmed_process_disappearance() {
        let root = tempfile::tempdir().unwrap();
        let (mut supervisor, mailbox, mut reports, mut gone, _) =
            staged_supervisor(root.path(), true);
        let mut release = live_command(&mut supervisor).await;
        // Model the running loop having ended its bounded wait for exit with
        // a pending capacity error. The child is deliberately still alive.
        supervisor.report_exhausted = true;
        supervisor
            .report_start_failure(exec::tool_error("resource_exhausted", "full", None))
            .await;
        supervisor.context.bounds.gone_check = Duration::from_millis(1);
        let mut settling = Box::pin(supervisor.settle());
        std::future::poll_fn(|cx| {
            assert!(settling.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // The single poll ran check_gone and the pending-report branch. This
        // is an ordering observation, not a guess about task scheduling.
        assert!(gone.try_recv().is_err());
        assert!(
            reports.try_recv().is_err(),
            "a live command forbids terminal publication"
        );
        release.write_all(b"exit\n").await.unwrap();
        drop(release);
        let owner = tokio::spawn(settling);
        let terminal = tokio::time::timeout(Duration::from_secs(2), reports.recv())
            .await
            .unwrap()
            .unwrap();
        gone.try_recv()
            .expect("disappearance must precede the terminal report");
        assert_eq!(terminal.body["phase"], "terminal");
        assert_eq!(terminal.body["report_seq"], 1);
        assert_eq!(
            terminal.body["result"]["error"]["kind"],
            "resource_exhausted"
        );
        terminal
            .listener
            .unwrap()
            .outcome(terminal.id, ReportOutcome::Acked);
        tokio::time::timeout(Duration::from_secs(2), owner)
            .await
            .unwrap()
            .unwrap();
        drop(mailbox);
        assert!(reports.recv().await.is_none());
    }

    #[tokio::test]
    async fn settled_reports_release_execution_and_the_same_mailbox_answers_late_controls() {
        for outcome in [ReportOutcome::Acked, ReportOutcome::Dropped] {
            let root = tempfile::tempdir().unwrap();
            let (mailbox, mut reports, mut gone, witness) = staged_task(root.path(), false);
            mailbox.send(SupervisorMessage::Prevent).unwrap();
            let terminal = reports.recv().await.unwrap();
            gone.recv().await.unwrap();
            assert!(
                witness.upgrade().is_some(),
                "the report still owns the execution wait"
            );
            terminal.listener.unwrap().outcome(terminal.id, outcome);
            tokio::time::timeout(Duration::from_secs(2), async {
                while witness.upgrade().is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let payload = json!({"session_id":"session","binding_id":"binding","tool_use_event_id":"event","task_id":"task","chars":""});
            mailbox
                .send(SupervisorMessage::Stdin(Box::new(Control {
                    connection_id: id("connection"),
                    correlation: correlation("read"),
                    traceparent: None,
                    payload: serde_json::from_value(payload).unwrap(),
                })))
                .unwrap();
            let reply = reports.recv().await.unwrap();
            assert_eq!(reply.body["error"]["code"], "result_unavailable");
            assert_eq!(reply.body["request_message_id"], "read");
            mailbox.send(SupervisorMessage::Cancel(Box::new(Control {
                connection_id: id("connection"), correlation: correlation("cancel"), traceparent: None,
                payload: serde_json::from_value(json!({"session_id":"session","binding_id":"binding","tool_use_event_id":"event","task_id":"task"})).unwrap(),
            }))).unwrap();
            let reply = reports.recv().await.unwrap();
            assert_eq!(reply.body["result"]["result"]["cancelled"], false);
            assert_eq!(reply.body["result"]["result"]["was_running"], false);
            mailbox
                .send(SupervisorMessage::Joined(id("joined")))
                .unwrap();
            drop(mailbox);
            assert!(
                reports.recv().await.is_none(),
                "no duplicate final reply is reconstructed"
            );
        }
    }

    #[tokio::test]
    async fn an_unadmitted_terminal_result_uses_a_compact_error_and_waits_for_its_outcome() {
        let root = tempfile::tempdir().unwrap();
        let (mailbox, mut reports, mut gone, witness) = staged_task(root.path(), true);
        mailbox.send(SupervisorMessage::Prevent).unwrap();
        gone.recv().await.unwrap();
        let terminal = reports.recv().await.unwrap();
        assert_eq!(terminal.body["phase"], "terminal");
        assert_eq!(
            terminal.body["result"]["error"]["kind"],
            "resource_exhausted"
        );
        assert_eq!(terminal.body["result"]["result"]["cancelled"], true);
        assert!(witness.upgrade().is_some());
        terminal
            .listener
            .unwrap()
            .outcome(terminal.id, ReportOutcome::Dropped);
        tokio::time::timeout(Duration::from_secs(2), async {
            while witness.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(mailbox);
        assert!(reports.recv().await.is_none());
    }

    fn recorded_write(request: &str, sequence: i64, chars: &str) -> Control<StdinPayload> {
        Control {
            connection_id: id("connection"),
            correlation: correlation(request),
            traceparent: None,
            payload: serde_json::from_value(json!({
                "session_id": "session", "binding_id": "binding",
                "tool_use_event_id": "event", "task_id": "task",
                "chars": chars, "write_seq": sequence, "wait_ms": 30_000,
            }))
            .unwrap(),
        }
    }

    fn finished_task(
        withdrawn: bool,
    ) -> (
        FinishedTask,
        SupervisorMailbox,
        mpsc::UnboundedReceiver<Recorded>,
    ) {
        let (mailbox, messages) = mpsc::unbounded_channel();
        let (sent, reports) = mpsc::unbounded_channel();
        let mut output = StreamCapture::new();
        output.write(b"finished output\n");
        let task = FinishedTask {
            messages,
            drive_sync: None,
            reports: Arc::new(Reports {
                next: AtomicU64::new(1),
                full: false,
                sent,
            }),
            task_id: id("task"),
            state: State {
                ended: true,
                exit: ExitFacts {
                    exit_code: Some(7),
                    signal: None,
                },
                timed_out: true,
                duration: Duration::from_millis(425),
            },
            counters: (output.counters(), exec::empty_stream()),
            records: WriteRecords::default(),
            withdrawn,
        };
        (task, mailbox, reports)
    }

    #[tokio::test]
    async fn finished_writes_repeat_recorded_facts_without_output_or_another_write() {
        let (mut task, mailbox, mut reports) = finished_task(false);
        for (sequence, chars, written, error) in [
            (1, "complete", 8, None),
            (2, "partial", 3, Some(ControlErrorCode::StdinClosed)),
        ] {
            let control = recorded_write("seed", sequence, chars);
            let Request::Write { digest, .. } = Request::of(&control.payload) else {
                panic!("the fixture must describe a write");
            };
            task.records.record(sequence, digest, written, error);
        }
        let owner = tokio::spawn(task.run());
        for repetition in 0..2 {
            for (sequence, chars, written, status) in
                [(1, "complete", 8, "success"), (2, "partial", 3, "error")]
            {
                let request = format!("repeat_{sequence}_{repetition}");
                mailbox
                    .send(SupervisorMessage::Stdin(Box::new(recorded_write(
                        &request, sequence, chars,
                    ))))
                    .unwrap();
                let reply = tokio::time::timeout(Duration::from_secs(2), reports.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(reply.body["request_message_id"], request);
                assert_eq!(reply.body["result"]["tool"], "stdin");
                assert_eq!(reply.body["result"]["status"], status);
                let result = &reply.body["result"]["result"];
                assert_eq!(result["written_bytes"], written);
                assert_eq!(result["exit_code"], 7);
                assert_eq!(result["signal"], serde_json::Value::Null);
                assert_eq!(result["timed_out"], true);
                assert_eq!(result["duration_ms"], 425);
                assert_eq!(
                    result["stdout"],
                    json!({
                        "text": "", "total_bytes": 16, "total_lines": 1,
                        "returned_bytes": 0, "truncated": false,
                    })
                );
                assert_eq!(
                    result["stderr"],
                    serde_json::to_value(exec::empty_stream()).unwrap()
                );
                if sequence == 2 {
                    assert_eq!(reply.body["error"]["code"], "stdin_closed");
                    assert_eq!(reply.body["result"]["error"]["kind"], "stdin_closed");
                } else {
                    assert!(reply.body["error"].is_null());
                    assert!(reply.body["result"]["error"].is_null());
                }
            }
        }
        // Cleanup keeps the old precedence: even a conflicting write
        // is unavailable, unless its exact original content was recorded.
        for (request, sequence, chars) in [("conflict", 1, "other"), ("new", 3, "new input")] {
            mailbox
                .send(SupervisorMessage::Stdin(Box::new(recorded_write(
                    request, sequence, chars,
                ))))
                .unwrap();
            let reply = tokio::time::timeout(Duration::from_secs(2), reports.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reply.body["request_message_id"], request);
            assert_eq!(reply.body["error"]["code"], "result_unavailable");
        }
        drop(mailbox);
        tokio::time::timeout(Duration::from_secs(2), owner)
            .await
            .unwrap()
            .unwrap();
        assert!(
            reports.recv().await.is_none(),
            "one reply per control, no duplicate report"
        );
    }

    #[tokio::test]
    async fn a_withdrawn_finished_task_refuses_both_control_kinds() {
        let (task, mailbox, mut reports) = finished_task(true);
        let owner = tokio::spawn(task.run());
        mailbox
            .send(SupervisorMessage::Stdin(Box::new(recorded_write(
                "write", 1, "unused",
            ))))
            .unwrap();
        mailbox
            .send(SupervisorMessage::Cancel(Box::new(Control {
                connection_id: id("connection"),
                correlation: correlation("cancel"),
                traceparent: None,
                payload: serde_json::from_value(json!({
                    "session_id": "session", "binding_id": "binding",
                    "tool_use_event_id": "event", "task_id": "task",
                }))
                .unwrap(),
            })))
            .unwrap();
        for request in ["write", "cancel"] {
            let reply = tokio::time::timeout(Duration::from_secs(2), reports.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reply.body["request_message_id"], request);
            assert_eq!(reply.body["error"]["code"], "task_not_found");
            assert!(reply.body["result"].is_null());
        }
        drop(mailbox);
        tokio::time::timeout(Duration::from_secs(2), owner)
            .await
            .unwrap()
            .unwrap();
        assert!(reports.recv().await.is_none());
    }

    #[tokio::test]
    async fn cleanup_failure_releases_execution_after_the_report_outcome() {
        use std::io::Write;
        use std::sync::Mutex;
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default)]
        struct LogBytes(Arc<Mutex<Vec<u8>>>);
        impl Write for LogBytes {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = LogBytes::default();
        let sinks = Arc::new(observe::Sinks::new(
            observe::Sink::spawn_stderr(Box::new(bytes.clone()), observe::registry()),
            observe::FlushClock {
                now: state::seams::now,
                recheck: Duration::from_millis(1),
            },
        ));
        let subscriber = tracing_subscriber::registry().with(observe::RecordLayer::new(
            observe::Level::Warn,
            sinks.clone(),
        ));
        let _recording = tracing::subscriber::set_default(subscriber);
        let root = tempfile::tempdir().unwrap();
        // An ordinary file in place of the temporary parent makes removal
        // fail with ENOTDIR on every user, without special permissions.
        std::fs::write(root.path().join("temporary"), b"retained obstruction").unwrap();
        std::fs::create_dir(root.path().join("task")).unwrap();
        std::fs::write(root.path().join("task/stdout.log"), b"task output").unwrap();
        let (mailbox, mut reports, mut gone, witness) = staged_task(root.path(), false);
        mailbox.send(SupervisorMessage::Prevent).unwrap();
        let terminal = reports.recv().await.unwrap();
        gone.recv().await.unwrap();
        assert!(witness.upgrade().is_some());
        assert!(root.path().join("task/stdout.log").exists());
        terminal
            .listener
            .unwrap()
            .outcome(terminal.id, ReportOutcome::Acked);
        tokio::time::timeout(Duration::from_secs(2), async {
            while witness.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("temporary")).unwrap(),
            b"retained obstruction"
        );
        assert!(
            !root.path().join("task").exists(),
            "log cleanup is independent"
        );
        assert!(
            reports.try_recv().is_err(),
            "cleanup failure does not rewrite the result"
        );
        sinks.flush(Duration::from_secs(2));
        let recorded = String::from_utf8(bytes.0.lock().unwrap().clone()).unwrap();
        let records: Vec<serde_json::Value> = recorded
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let failure = records
            .iter()
            .find(|record| record["event"] == "task.cleanup_failed")
            .expect("the cleanup failure must be recorded");
        assert_eq!(failure["task.id"], "task");
        assert_eq!(failure["cleanup.part"], "temporary");
        assert_eq!(failure["cleanup.directory"], "task");
        assert_eq!(failure["error.code"], "task_cleanup_failed");
        assert_eq!(failure["error.os_code"], libc::ENOTDIR);
        drop(mailbox);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), reports.recv())
                .await
                .unwrap()
                .is_none()
        );
    }
}
