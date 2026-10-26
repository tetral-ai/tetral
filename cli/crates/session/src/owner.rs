//! The Session binding owner: one task per Session on this machine.
//!
//! The owner sets no timer for a probe's deadline: it reads the seam clock
//! whenever it wakes and, while a probe runs, wakes at least once per
//! recheck interval, as the Connection owner does, and acts on a passed
//! deadline before it handles the message it woke for. Probe tasks and report
//! outcomes come back as messages tagged with their operation or report,
//! and a result whose operation is no longer current is discarded.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use execution::cleanup::Leftovers;
use execution::supervisor::{SupervisorMailbox, SupervisorMessage, TaskListener};
use protocol::id::{Id, SafeId};
use protocol::local::SessionState;
use protocol::ports::{
    Close, ConnectionPort, EntriesReply, Prepare, ProbeRequest, ProbeSlot, ReportId,
    ReportListener, ReportOutcome, ReportSink, SessionFactory, SessionHandle, TaskOwnerReply,
    TaskOwnership, ToolCall,
};
use tokio::sync::{mpsc, oneshot};

use crate::probe::{PROBE_LIMIT, Probe};
use crate::probe_child::Output;
use crate::record::Record;
use crate::tools::{self, TaskEvent, TaskKey, TaskStart, ToolContext};

pub enum SessionMessage {
    /// A tool instruction for one of this Session's bindings.
    Tool(Box<ToolCall>),
    /// A `session.close` of one of this Session's bindings; the reply comes
    /// once the close is admitted.
    Close {
        close: Close,
        reply: oneshot::Sender<()>,
    },
    Prepare(Box<Prepare>),
    ProbeSlot {
        request: ProbeRequest,
        answer: ProbeSlot,
    },
    /// The probe of `operation` is reaped, with its result if it wrote one.
    ProbeDone {
        operation: u64,
        output: Option<Output>,
    },
    Report {
        report: ReportId,
        outcome: ReportOutcome,
    },
    ConnectionEnded(Id),
    Shutdown,
    Entries(EntriesReply),
    /// What a task's supervisor reported about its processes.
    Task {
        task: TaskKey,
        event: TaskEvent,
    },
    /// Which task, if any, the host process `pid` belongs to.
    TaskOwner {
        pid: u32,
        reply: TaskOwnerReply,
    },
}

pub type SessionMailbox = mpsc::UnboundedSender<SessionMessage>;

/// Supervises the tasks of admitted commands.
pub trait TaskStarter: Send + 'static {
    /// The supervisor of an admitted task, `staged` until [`TaskStarter::start`],
    /// whose reports go to `reports` and which tells `listener` about the
    /// task's processes; `None` for a tool that no supervisor runs here.
    fn supervise(
        &mut self,
        task: TaskStart,
        reports: Arc<dyn ReportSink>,
        listener: Arc<dyn TaskListener>,
    ) -> Option<SupervisorMailbox>;

    /// Starts a staged task's supervisor.
    fn start(&mut self, supervisor: &SupervisorMailbox) {
        let _ = supervisor.send(SupervisorMessage::Start);
    }
}

/// The starter of an App Server that executes no tool: its ready advertises
/// no `tool_execution`, so no tool instruction is admitted.
#[derive(Debug, Clone, Copy)]
pub struct NoTasks;

impl TaskStarter for NoTasks {
    fn supervise(
        &mut self,
        _task: TaskStart,
        _reports: Arc<dyn ReportSink>,
        _listener: Arc<dyn TaskListener>,
    ) -> Option<SupervisorMailbox> {
        None
    }
}

/// The paths a preparation checks against and runs.
#[derive(Debug, Clone)]
pub struct RootPaths {
    /// The state root as `TETRAL_HOME` gives it.
    pub given: PathBuf,
    /// The state root's canonical path.
    pub canonical: PathBuf,
    /// The running executable, which runs as the directory probe.
    pub probe_program: PathBuf,
}

/// The durations an owner runs by, each a constant but for tests.
#[derive(Debug, Clone, Copy)]
pub struct Times {
    pub probe_limit: Duration,
}

impl Default for Times {
    fn default() -> Times {
        Times {
            probe_limit: PROBE_LIMIT,
        }
    }
}

/// What every Session binding owner of an App Server shares.
#[derive(Debug, Clone)]
pub struct Shared {
    pub paths: Arc<RootPaths>,
    pub times: Times,
    pub tools: Arc<ToolContext>,
    /// Which Sessions a record an earlier App Server left keeps
    /// `cleanup_pending`.
    pub leftovers: Leftovers,
}

/// Creates the Session binding owners of an App Server.
pub struct Factory<S> {
    shared: Shared,
    starter: S,
}

impl<S> Factory<S> {
    pub fn new(shared: Shared, starter: S) -> Factory<S> {
        Factory { shared, starter }
    }
}

impl<S: TaskStarter + Clone + Sync> SessionFactory for Factory<S> {
    fn create(
        &self,
        session_id: SafeId,
        connection: Arc<dyn ConnectionPort>,
        reports: Arc<dyn ReportSink>,
    ) -> Arc<dyn SessionHandle> {
        let (mailbox, messages) = mpsc::unbounded_channel();
        let preparation = Preparation {
            session_id,
            connection,
            reports,
            shared: self.shared.clone(),
        };
        let owner = SessionOwner::new(Some(preparation), self.starter.clone(), mailbox.clone());
        tokio::spawn(own(owner, messages));
        Arc::new(Handle { mailbox })
    }

    fn unbound_tool(&self, call: ToolCall, reports: Arc<dyn ReportSink>) {
        tools::unbound(&self.shared, call, reports.as_ref());
    }
}

/// The Connection owner's handle on one owner: each call is a message.
struct Handle {
    mailbox: SessionMailbox,
}

impl Handle {
    fn send(&self, message: SessionMessage) {
        let _ = self.mailbox.send(message);
    }
}

impl SessionHandle for Handle {
    fn prepare(&self, prepare: Prepare) {
        self.send(SessionMessage::Prepare(Box::new(prepare)));
    }

    fn close(&self, close: Close) {
        let (reply, _) = oneshot::channel();
        self.send(SessionMessage::Close { close, reply });
    }

    fn tool(&self, call: ToolCall) {
        self.send(SessionMessage::Tool(Box::new(call)));
    }

    fn probe_slot(&self, request: ProbeRequest, answer: ProbeSlot) {
        self.send(SessionMessage::ProbeSlot { request, answer });
    }

    fn connection_ended(&self, connection_id: Id) {
        self.send(SessionMessage::ConnectionEnded(connection_id));
    }

    fn shutdown(&self) {
        self.send(SessionMessage::Shutdown);
    }

    fn entries(&self, reply: EntriesReply) {
        self.send(SessionMessage::Entries(reply));
    }
}

impl TaskOwnership for Handle {
    fn task_of(&self, pid: u32, reply: TaskOwnerReply) {
        self.send(SessionMessage::TaskOwner { pid, reply });
    }
}

/// Gives each report outcome back to the owner as a message.
pub(crate) struct ReportEvents {
    pub mailbox: SessionMailbox,
}

impl ReportListener for ReportEvents {
    fn outcome(&self, report: ReportId, outcome: ReportOutcome) {
        let _ = self
            .mailbox
            .send(SessionMessage::Report { report, outcome });
    }
}

/// What an owner that prepares bindings holds beyond the admission of
/// commands.
pub(crate) struct Preparation {
    pub session_id: SafeId,
    pub connection: Arc<dyn ConnectionPort>,
    pub reports: Arc<dyn ReportSink>,
    pub shared: Shared,
}

/// A spawned probe, until its task reports it reaped.
pub(crate) struct Running {
    pub probe: Probe,
    pub deadline: Duration,
    pub killed: bool,
}

pub(crate) struct SessionOwner<S> {
    pub starter: S,
    pub mailbox: SessionMailbox,
    pub preparation: Option<Preparation>,
    /// Absent until the first `session.prepare` is handled.
    pub record: Option<Record>,
    pub last_operation: u64,
    pub last_request: u64,
    /// Each slot request that may still wait, and its operation.
    pub requests: HashMap<ProbeRequest, u64>,
    /// Each probe not yet reaped, by operation.
    pub probes: HashMap<u64, Running>,
    /// The tasks of ended connections, by connection and `task_id`, whose
    /// processes are not yet confirmed gone.
    pub unconfirmed: HashSet<(Id, Id)>,
    /// The App Server's shutdown has ended every task.
    pub shutting_down: bool,
    /// The Connection owner was told that every task is confirmed gone.
    pub shutdown_answered: bool,
}

async fn own<S: TaskStarter>(
    mut owner: SessionOwner<S>,
    mut messages: mpsc::UnboundedReceiver<SessionMessage>,
) {
    loop {
        let wake = owner.wake_in();
        let message = tokio::select! {
            message = messages.recv() => match message {
                Some(message) => Some(message),
                None => return,
            },
            () = async {
                match wake {
                    Some(wake) => tokio::time::sleep(wake).await,
                    None => std::future::pending().await,
                }
            } => None,
        };
        owner.check_deadlines();
        if let Some(message) = message {
            owner.handle(message);
        }
    }
}

impl<S: TaskStarter> SessionOwner<S> {
    fn new(
        preparation: Option<Preparation>,
        starter: S,
        mailbox: SessionMailbox,
    ) -> SessionOwner<S> {
        SessionOwner {
            starter,
            mailbox,
            preparation,
            record: None,
            last_operation: 0,
            last_request: 0,
            requests: HashMap::new(),
            probes: HashMap::new(),
            unconfirmed: HashSet::new(),
            shutting_down: false,
            shutdown_answered: false,
        }
    }

    fn handle(&mut self, message: SessionMessage) {
        match message {
            SessionMessage::Tool(call) => self.tool(*call),
            SessionMessage::Close { close, reply } => {
                self.close(&close);
                let _ = reply.send(());
            }
            SessionMessage::Prepare(prepare) => self.prepare(*prepare),
            SessionMessage::ProbeSlot { request, answer } => self.probe_slot(request, answer),
            SessionMessage::ProbeDone { operation, output } => self.probe_done(operation, output),
            SessionMessage::Report { report, outcome } => self.report_outcome(report, outcome),
            SessionMessage::ConnectionEnded(connection) => self.connection_ended(&connection),
            SessionMessage::Shutdown => self.shutdown(),
            SessionMessage::Entries(reply) => {
                let entries = match (&self.record, &self.preparation) {
                    (Some(record), Some(preparation)) => record.entries(&preparation.session_id),
                    _ => Vec::new(),
                };
                reply(entries);
            }
            SessionMessage::Task { task, event } => {
                self.task_event(task, event);
                self.answer_shutdown();
            }
            SessionMessage::TaskOwner { pid, reply } => self.task_owner(pid, reply),
        }
    }

    /// The time until the owner must read the clock again, while a probe
    /// runs that has not been killed.
    fn wake_in(&self) -> Option<Duration> {
        let next = self
            .probes
            .values()
            .filter(|running| !running.killed)
            .map(|running| running.deadline)
            .min()?;
        Some(
            next.saturating_sub(state::seams::now())
                .min(state::seams::recheck_interval()),
        )
    }

    /// Kills each probe whose time has passed; its preparation fails with
    /// `cwd_check_timeout`, and its slot stays taken until it is reaped.
    fn check_deadlines(&mut self) {
        let now = state::seams::now();
        let passed: Vec<u64> = self
            .probes
            .iter_mut()
            .filter(|(_, running)| !running.killed && now >= running.deadline)
            .map(|(operation, running)| {
                running.probe.kill();
                running.killed = true;
                *operation
            })
            .collect();
        for operation in passed {
            self.probe_timed_out(operation);
        }
    }

    /// A close of the current binding: it enters `closing`, its preparation
    /// ends, retries of its reports stop and its staged tasks never start.
    fn close(&mut self, close: &Close) {
        let Some(binding) = self
            .record
            .as_mut()
            .and_then(|record| record.current.as_mut())
            .filter(|binding| binding.binding_id == close.binding_id)
        else {
            return;
        };
        binding.state = SessionState::Closing;
        binding.waiting.clear();
        binding.tasks.close();
        let operation = binding.operation;
        let reports = std::mem::take(&mut binding.reports);
        self.end_operation(operation);
        if let Some(preparation) = &self.preparation {
            for report in reports {
                preparation.reports.cancel(report);
            }
        }
    }

    /// The bindings of the ended connection become `unavailable`; a running
    /// preparation of one ends, its staged tasks are discarded and its
    /// running ones end.
    fn connection_ended(&mut self, connection: &Id) {
        let Some(record) = self.record.as_mut() else {
            return;
        };
        let Some(mut binding) = record
            .current
            .take_if(|binding| binding.connection_id == *connection)
        else {
            return;
        };
        binding.state = SessionState::Unavailable;
        binding.waiting.clear();
        binding.reports.clear();
        for task_id in binding.tasks.connection_ended() {
            self.unconfirmed.insert((connection.clone(), task_id));
        }
        let operation = binding.operation;
        record.unavailable = Some(binding);
        self.end_operation(operation);
    }

    fn shutdown(&mut self) {
        self.shutting_down = true;
        if let Some(record) = &self.record {
            for binding in [&record.current, &record.unavailable].into_iter().flatten() {
                binding.tasks.shutdown();
            }
        }
        let operations: Vec<u64> = self
            .requests
            .values()
            .chain(self.probes.keys())
            .copied()
            .collect();
        for operation in operations {
            self.end_operation(operation);
        }
        self.answer_shutdown();
    }

    /// Ends `operation`: a slot request it still has is withdrawn and its
    /// probe killed. Its result, if one comes, is discarded since no binding
    /// holds the operation any more.
    pub(crate) fn end_operation(&mut self, operation: u64) {
        let withdrawn: Vec<ProbeRequest> = self
            .requests
            .iter()
            .filter(|(_, waiting)| **waiting == operation)
            .map(|(request, _)| *request)
            .collect();
        for request in withdrawn {
            self.requests.remove(&request);
            if let Some(preparation) = &self.preparation {
                preparation
                    .connection
                    .probe_slot_withdraw(&preparation.session_id, request);
            }
        }
        if let Some(running) = self.probes.get_mut(&operation)
            && !running.killed
        {
            running.probe.kill();
            running.killed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use protocol::connection::{DownstreamType, PreparationError, PreparationErrorCode};
    use protocol::field::AbsolutePath;
    use protocol::local::SessionEntry;
    use protocol::ports::Submission;
    use protocol::tool::{ExecutionReport, ToolStatus};
    use serde_json::json;
    use tokio::sync::oneshot::error::TryRecvError;
    use tokio::sync::{Barrier, oneshot};

    use super::*;
    use crate::record::Binding;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    /// A record whose current binding is `bind_1` of operation 1.
    fn record_with(state: SessionState) -> Record {
        Record {
            current: Some(Binding {
                binding_id: id("bind_1"),
                connection_id: id("conn_1"),
                requested_cwd: AbsolutePath::new("/project").unwrap(),
                create_cwd: false,
                cwd: (state == SessionState::Prepared)
                    .then(|| AbsolutePath::new("/project").unwrap()),
                state,
                error: None,
                fingerprint: "a".repeat(64),
                operation: 1,
                waiting: Vec::new(),
                reports: Vec::new(),
                tasks: Default::default(),
            }),
            unavailable: None,
        }
    }

    /// What the owners of the tests share: an App Server whose ready lists
    /// `exec_command`.
    fn shared() -> Shared {
        let capability = json!({
            "schema_version": 1,
            "tools": ["exec_command", "edit", "view_image"],
            "controls": ["stdin", "cancel"],
            "sandbox_policy_versions": [1],
            "network_modes": ["disabled", "direct"],
            "limits": {
                "metadata_json_bytes": 65536, "tool_input_json_bytes": 4194304,
                "execute_sse_json_bytes": 4259840, "stdin_utf8_bytes": 32768,
                "stdin_sse_json_bytes": 262144, "default_message_json_bytes": 65536,
                "helper_result_json_bytes": 262144, "drive_sync_json_bytes": 65536,
                "ordinary_tool_status_json_bytes": 327680,
                "drive_sync_report_json_bytes": 131072,
                "view_image_decoded_bytes": 10485760,
                "media_tool_status_json_bytes": 14680064,
            },
        });
        Shared {
            paths: Arc::new(RootPaths {
                given: "/root".into(),
                canonical: "/root".into(),
                probe_program: "/probe".into(),
            }),
            times: Times::default(),
            tools: Arc::new(ToolContext {
                capability: Some(serde_json::from_value(capability).unwrap()),
                socket_directory: "/sockets".into(),
                tetral: "/tetral".into(),
            }),
            leftovers: Leftovers::none(),
        }
    }

    /// The `task_id` of each `rejected` report submitted.
    #[derive(Default)]
    struct Rejections(Mutex<Vec<String>>);

    impl Rejections {
        fn of(&self, task_id: &str) -> bool {
            self.0.lock().unwrap().iter().any(|task| task == task_id)
        }
    }

    impl ReportSink for Rejections {
        fn submit(&self, submission: Submission) -> ReportId {
            let bytes = (submission.build)(&id("conn_1"), &id("r1")).unwrap();
            if let ToolStatus::Execution(ExecutionReport::Rejected { header, .. }) =
                protocol::decode::<ToolStatus>(&bytes).unwrap()
            {
                let task_id = header.task_id.as_str().to_owned();
                self.0.lock().unwrap().push(task_id);
            }
            ReportId(0)
        }
        fn cancel(&self, _report: ReportId) {}
    }

    /// Starts an owner whose Session's current binding is `bind_1`, prepared,
    /// with the rejections it submits.
    fn owner_with_binding<S: TaskStarter>(starter: S) -> (SessionMailbox, Arc<Rejections>) {
        let (mailbox, messages) = mpsc::unbounded_channel();
        let rejections = Arc::new(Rejections::default());
        let preparation = Preparation {
            session_id: SafeId::new("sess_1").unwrap(),
            connection: Arc::new(NoConnection),
            reports: rejections.clone(),
            shared: shared(),
        };
        let mut owner = SessionOwner::new(Some(preparation), starter, mailbox.clone());
        owner.record = Some(record_with(SessionState::Prepared));
        tokio::spawn(own(owner, messages));
        (mailbox, rejections)
    }

    /// Returns once the owner has handled every message sent before.
    async fn handled(mailbox: &SessionMailbox) {
        let (reply, read) = oneshot::channel();
        let entries = SessionMessage::Entries(Box::new(move |_| {
            let _ = reply.send(());
        }));
        assert!(mailbox.send(entries).is_ok());
        read.await.unwrap();
    }

    struct NoConnection;

    impl ConnectionPort for NoConnection {
        fn probe_slot(&self, _session_id: &SafeId, _request: ProbeRequest) {}
        fn probe_slot_withdraw(&self, _session_id: &SafeId, _request: ProbeRequest) {}
        fn probe_slot_free(&self) {}
        fn record_gone(&self, _session_id: &SafeId) {}
        fn tasks_gone(&self, _session_id: &SafeId) {}
    }

    struct NoReports;

    impl ReportSink for NoReports {
        fn submit(&self, _submission: Submission) -> ReportId {
            ReportId(0)
        }
        fn cancel(&self, _report: ReportId) {}
    }

    fn close_of(binding_id: &str) -> Close {
        Close {
            connection_id: id("conn_1"),
            message_id: id("msg_close"),
            binding_id: id(binding_id),
        }
    }

    /// The close's reply, which the owner sends once it admits the close, as
    /// both the closer and the starter see it: the starter checks at each
    /// start whether the close was already admitted, so a start that follows
    /// the admission is seen however soon after it comes.
    #[derive(Clone)]
    struct CloseAdmitted(Arc<Mutex<(oneshot::Receiver<()>, bool)>>);

    impl CloseAdmitted {
        fn new(reply: oneshot::Receiver<()>) -> CloseAdmitted {
            CloseAdmitted(Arc::new(Mutex::new((reply, false))))
        }

        fn check(&self) -> bool {
            let mut state = self.0.lock().unwrap();
            let (reply, admitted) = &mut *state;
            if !*admitted {
                match reply.try_recv() {
                    Ok(()) => *admitted = true,
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Closed) => panic!("the owner dropped the close's reply"),
                }
            }
            *admitted
        }
    }

    struct CountingStarter {
        starts: Arc<AtomicUsize>,
        starts_after_close: Arc<AtomicUsize>,
        close: CloseAdmitted,
        supervisors: Vec<mpsc::UnboundedReceiver<SupervisorMessage>>,
    }

    impl TaskStarter for CountingStarter {
        fn supervise(
            &mut self,
            _task: TaskStart,
            _reports: Arc<dyn ReportSink>,
            _listener: Arc<dyn TaskListener>,
        ) -> Option<SupervisorMailbox> {
            let (mailbox, messages) = mpsc::unbounded_channel();
            self.supervisors.push(messages);
            Some(mailbox)
        }

        fn start(&mut self, _supervisor: &SupervisorMailbox) {
            self.starts.fetch_add(1, Ordering::SeqCst);
            if self.close.check() {
                self.starts_after_close.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// The barrier releases both senders at once but does not decide which
    /// message reaches the mailbox first; the second sender waits until the
    /// first one's message is in the mailbox, so each order is tested.
    struct Turn {
        after: Option<oneshot::Receiver<()>>,
        then: Option<oneshot::Sender<()>>,
    }

    impl Turn {
        fn pair(close_first: bool) -> (Turn, Turn) {
            let (sent, was_sent) = oneshot::channel();
            let first = Turn {
                after: None,
                then: Some(sent),
            };
            let second = Turn {
                after: Some(was_sent),
                then: None,
            };
            if close_first {
                (first, second)
            } else {
                (second, first)
            }
        }

        async fn send(self, mailbox: &SessionMailbox, message: SessionMessage) {
            if let Some(earlier) = self.after {
                earlier.await.unwrap();
            }
            assert!(mailbox.send(message).is_ok());
            if let Some(sent) = self.then {
                sent.send(()).unwrap();
            }
        }
    }

    struct Outcome {
        /// Whether the command was rejected rather than started.
        refused: bool,
        starts_after_close: usize,
        starts: usize,
    }

    /// A `tool.execute` of `bind_1` that every other admission step admits.
    fn tool(task_id: &str) -> SessionMessage {
        let data = json!({
            "protocol_version": 1,
            "connection_id": "conn_1",
            "message_id": format!("msg_{task_id}"),
            "type": "tool.execute",
            "payload": {
                "session_id": "sess_1",
                "binding_id": "bind_1",
                "tool_use_event_id": format!("call_{task_id}"),
                "task_id": task_id,
                "tool": "exec_command",
                "input": {"cmd": "true"},
                "sandbox_policy": {
                    "schema_version": 1,
                    "filesystem": {"rules": [{"path": "/project", "access": "read_write"}]},
                    "network": {"mode": "disabled"},
                },
            },
        });
        SessionMessage::Tool(Box::new(ToolCall {
            connection_id: id("conn_1"),
            message_id: id(&format!("msg_{task_id}")),
            kind: DownstreamType::ToolExecute,
            session_id: id("sess_1"),
            binding_id: id("bind_1"),
            tool_use_event_id: id(&format!("call_{task_id}")),
            task_id: id(task_id),
            fingerprint: protocol::fingerprint::fingerprint(&data["payload"]),
            data: data.to_string(),
            recorded: true,
            traceparent: None,
            received: Duration::ZERO,
        }))
    }

    async fn close_and_command(close_first: bool) -> Outcome {
        let (starts, starts_after_close) =
            (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (reply, admitted) = oneshot::channel();
        let close = CloseAdmitted::new(admitted);
        let (mailbox, rejections) = owner_with_binding(CountingStarter {
            starts: starts.clone(),
            starts_after_close: starts_after_close.clone(),
            close: close.clone(),
            supervisors: Vec::new(),
        });
        let barrier = Arc::new(Barrier::new(2));
        let (close_turn, tool_turn) = Turn::pair(close_first);

        let closer = tokio::spawn({
            let (mailbox, barrier) = (mailbox.clone(), barrier.clone());
            async move {
                barrier.wait().await;
                close_turn
                    .send(
                        &mailbox,
                        SessionMessage::Close {
                            close: close_of("bind_1"),
                            reply,
                        },
                    )
                    .await;
                while !close.check() {
                    tokio::task::yield_now().await;
                }
            }
        });
        let commander = tokio::spawn({
            let (mailbox, barrier) = (mailbox.clone(), barrier.clone());
            async move {
                barrier.wait().await;
                tool_turn.send(&mailbox, tool("task-1")).await;
                handled(&mailbox).await;
            }
        });

        closer.await.unwrap();
        commander.await.unwrap();
        assert!(mailbox.send(tool("task-2")).is_ok());
        handled(&mailbox).await;
        assert!(rejections.of("task-2"));
        Outcome {
            refused: rejections.of("task-1"),
            starts_after_close: starts_after_close.load(Ordering::SeqCst),
            starts: starts.load(Ordering::SeqCst),
        }
    }

    /// Whether the owner finishes the first message before the second arrives
    /// depends on scheduling, so one round can miss an owner that interleaves
    /// its messages; this many rounds make it fail reliably.
    const ROUNDS: usize = 200;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_close_handled_first_refuses_the_command() {
        for round in 0..ROUNDS {
            let outcome = close_and_command(true).await;
            assert!(outcome.refused, "round {round}");
            assert_eq!(outcome.starts_after_close, 0, "round {round}");
            assert_eq!(outcome.starts, 0, "round {round}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_command_handled_first_starts_and_nothing_starts_after_the_close() {
        for round in 0..ROUNDS {
            let outcome = close_and_command(false).await;
            assert!(!outcome.refused, "round {round}");
            assert_eq!(outcome.starts_after_close, 0, "round {round}");
            assert_eq!(outcome.starts, 1, "round {round}");
        }
    }

    #[tokio::test]
    async fn a_close_of_another_binding_leaves_the_current_binding_admitting() {
        let starts = Arc::new(AtomicUsize::new(0));
        // No close of `bind_1` comes, so its reply stays open and unsent.
        let (_open, never) = oneshot::channel();
        let (mailbox, rejections) = owner_with_binding(CountingStarter {
            starts: starts.clone(),
            starts_after_close: Arc::new(AtomicUsize::new(0)),
            close: CloseAdmitted::new(never),
            supervisors: Vec::new(),
        });
        let (reply, admitted) = oneshot::channel();
        let message = SessionMessage::Close {
            close: close_of("bind_replaced"),
            reply,
        };
        assert!(mailbox.send(message).is_ok());
        admitted.await.unwrap();
        assert!(mailbox.send(tool("task-1")).is_ok());
        handled(&mailbox).await;
        assert!(!rejections.of("task-1"));
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    /// The entry of a Session whose running probe of operation 1 has the
    /// given deadline once the owner has taken that probe's `output`.
    async fn entry_after_probe_done(deadline: Duration, output: Option<Output>) -> SessionEntry {
        let (mailbox, messages) = mpsc::unbounded_channel();
        let preparation = Preparation {
            session_id: SafeId::new("sess_1").unwrap(),
            connection: Arc::new(NoConnection),
            reports: Arc::new(NoReports),
            shared: shared(),
        };
        let mut owner = SessionOwner::new(Some(preparation), NoTasks, mailbox.clone());
        owner.record = Some(record_with(SessionState::Preparing));
        owner.probes.insert(
            1,
            Running {
                probe: Probe::reaped(),
                deadline,
                killed: false,
            },
        );
        let result = SessionMessage::ProbeDone {
            operation: 1,
            output,
        };
        assert!(mailbox.send(result).is_ok());
        tokio::spawn(own(owner, messages));

        let (reply, entries) = oneshot::channel();
        let read = SessionMessage::Entries(Box::new(move |entries| {
            let _ = reply.send(entries);
        }));
        assert!(mailbox.send(read).is_ok());
        entries.await.unwrap().remove(0)
    }

    #[tokio::test]
    async fn a_probe_result_taken_after_the_probe_deadline_fails_the_preparation_with_the_timeout()
    {
        let passed = state::seams::now().saturating_sub(Duration::from_millis(1));
        let entered = Output::Entered {
            cwd: "/project".to_owned(),
        };
        let entry = entry_after_probe_done(passed, Some(entered)).await;
        assert_eq!(entry.state, SessionState::Failed);
        assert_eq!(
            entry.error.0.map(|error| error.code),
            Some(PreparationErrorCode::CwdCheckTimeout)
        );
    }

    #[tokio::test]
    async fn a_probe_that_ends_without_a_result_line_fails_the_preparation_as_inaccessible() {
        let ahead = state::seams::now() + Duration::from_secs(60);
        let entry = entry_after_probe_done(ahead, None).await;
        assert_eq!(entry.state, SessionState::Failed);
        assert_eq!(
            entry.error.0,
            Some(PreparationError {
                code: PreparationErrorCode::CwdInaccessible,
                message: "the directory probe ended without a result".to_owned(),
            })
        );
    }
}
