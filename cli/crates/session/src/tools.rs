//! Tool instructions in a Session binding owner. A `tool.execute` passes its
//! admission steps in a fixed order, the first failing one deciding its
//! `rejected` report, and every step precedes any side effect: its task
//! identity against the binding's registry, the replay ledger's room, its
//! schema, budgets and tool, its policy, then its binding. An admitted task
//! enters its binding's registry and starts, or waits in the binding's
//! staging queue while the binding waits for the ACK of its `prepared`
//! report; that ACK starts the queue in arrival order
//! ([`SessionOwner::start_staged`]). Each admitted task has its Task
//! supervisor from its admission on, `staged` until the owner starts it. A
//! control finds its task only in the registry of the binding it names, on
//! its own connection, and goes to that task's supervisor.
//!
//! Each task's supervisor tells the owner the task's PID namespace and when
//! its processes are confirmed gone. A connection's end discards the
//! binding's staged tasks and ends its running ones; until every task of an
//! ended connection is confirmed gone, and every record an earlier App
//! Server left for the Session is, the Session is `cleanup_pending`
//! ([`SessionOwner::cleanup_pending`]). A host process belongs to the task
//! whose PID namespace holds it ([`SessionOwner::task_owner`]).

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use execution::policy::{TaskPermissions, TranslationInputs, validate};
use execution::report::{Body, Correlation, Report};
use execution::supervisor::{
    Control, SupervisorMailbox, SupervisorMessage, TASK_NOT_FOUND, TaskListener,
};
use execution::{cleanup, isolation, process, store};
use protocol::connection::{Downstream, DownstreamPayload, DownstreamType};
use protocol::id::Id;
use protocol::local::SessionState;
use protocol::ports::{ReportSink, TaskOwnerReply, ToolCall};
use protocol::tool::{
    ControlErrorCode, ExecutePayload, ExecutionTool, ProtocolError, RejectionCode,
    ToolExecutionCapability, ToolInput, VisibleLimits,
};
use serde_json::{Value, json};

use crate::owner::{SessionMailbox, SessionMessage, SessionOwner, Shared, TaskStarter};
use crate::record::{Binding, Record};

/// What the admission of tool instructions reads besides a binding, the
/// same for every Session of an App Server.
#[derive(Debug, Clone)]
pub struct ToolContext {
    /// The `tool_execution` that every ready of this App Server advertises;
    /// without it no tool is admitted.
    pub capability: Option<ToolExecutionCapability>,
    /// The control socket's directory `S`, canonical.
    pub socket_directory: PathBuf,
    /// The running `tetral`, canonical.
    pub tetral: PathBuf,
}

/// An admitted task as its task supervisor starts it.
#[derive(Debug, Clone)]
pub struct TaskStart {
    pub connection_id: Id,
    pub correlation: Correlation,
    pub tool: ToolInput,
    pub permissions: TaskPermissions,
    pub limits: Option<VisibleLimits>,
    /// The binding's canonical working directory.
    pub cwd: PathBuf,
    /// The task's directory under its Session ([`store::task_directory`]).
    pub task_directory: PathBuf,
    /// The task's private temporary directory in `T`.
    pub temporary_directory: PathBuf,
    /// The task's record under `R/execution/` ([`cleanup::record_path`]).
    pub record: PathBuf,
    pub traceparent: Option<String>,
    /// When the instruction was received, on the seam clock.
    pub received: Duration,
}

/// One binding's tasks: its registry by `task_id` and its staging queue.
#[derive(Debug, Default)]
pub struct Tasks {
    registry: HashMap<Id, Task>,
    /// The tasks admitted while the binding waits for its ACK, in arrival
    /// order.
    staged: VecDeque<Id>,
}

struct Task {
    /// Its `tool.execute` payload's content fingerprint.
    fingerprint: String,
    tool: ExecutionTool,
    supervisor: Option<SupervisorMailbox>,
    state: TaskState,
    /// The inode of its PID namespace, once its supervisor reported it.
    namespace: Option<u64>,
    /// Whether its processes are confirmed gone.
    gone: bool,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Task")
            .field("tool", &self.tool)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskState {
    Staged,
    Started,
    /// A close or a cancel prevented its start while it was staged.
    Prevented,
}

impl Tasks {
    /// Prevents the start of every staged task: at once, never behind
    /// another binding's ACK.
    fn prevent_staged(&mut self) {
        for task_id in std::mem::take(&mut self.staged) {
            if let Some(task) = self.registry.get_mut(&task_id) {
                task.state = TaskState::Prevented;
            }
        }
    }

    /// The binding closes: staged tasks never start, and each makes the
    /// `terminal` report of a cancelled staged task.
    pub fn close(&mut self) {
        self.prevent_staged();
        self.send_all(|| SupervisorMessage::Prevent);
    }

    /// The connection ended: staged tasks never start, and every task's
    /// supervisor ends its task. Gives the tasks whose processes are not
    /// yet confirmed gone.
    pub fn connection_ended(&mut self) -> Vec<Id> {
        self.prevent_staged();
        self.send_all(|| SupervisorMessage::ConnectionEnded);
        self.unconfirmed().cloned().collect()
    }

    /// The App Server shuts down: every task's supervisor ends its task.
    pub fn shutdown(&self) {
        self.send_all(|| SupervisorMessage::Shutdown);
    }

    /// Whether every started task's processes are confirmed gone.
    pub fn all_gone(&self) -> bool {
        self.unconfirmed().next().is_none()
    }

    /// The started tasks whose processes are not yet confirmed gone.
    fn unconfirmed(&self) -> impl Iterator<Item = &Id> {
        self.registry
            .iter()
            .filter(|(_, task)| task.state == TaskState::Started && !task.gone)
            .map(|(task_id, _)| task_id)
    }

    fn send_all(&self, message: impl Fn() -> SupervisorMessage) {
        for supervisor in self
            .registry
            .values()
            .filter_map(|task| task.supervisor.as_ref())
        {
            let _ = supervisor.send(message());
        }
    }

    /// The supervisors of the staged tasks in arrival order, each task now
    /// started.
    fn start_staged(&mut self) -> Vec<SupervisorMailbox> {
        std::mem::take(&mut self.staged)
            .into_iter()
            .filter_map(|task_id| {
                let task = self.registry.get_mut(&task_id)?;
                task.state = TaskState::Started;
                task.supervisor.clone()
            })
            .collect()
    }
}

/// The policy that stands in for an instruction's own while step 3 decides
/// whether the instruction fails for anything but its policy, which step 4
/// judges.
fn stand_in_policy() -> Value {
    json!({
        "schema_version": 1,
        "filesystem": {"rules": []},
        "network": {"mode": "disabled"},
    })
}

impl<S: TaskStarter> SessionOwner<S> {
    pub(crate) fn tool(&mut self, call: ToolCall) {
        let Some(preparation) = &self.preparation else {
            return;
        };
        let reports = preparation.reports.clone();
        let refusal = match call.kind {
            DownstreamType::ToolExecute => self.execute(&call).err(),
            _ => self.control(&call).err(),
        };
        if let Some(body) = refusal {
            submit(reports.as_ref(), &call, body);
        }
    }

    /// The admission steps of a `tool.execute`; an admitted task registers,
    /// then starts or is staged.
    fn execute(&mut self, call: &ToolCall) -> Result<(), Body> {
        let preparation = self.preparation.as_ref().expect("a preparing owner");
        let shared = &preparation.shared;
        let connection_binding = self
            .record
            .as_mut()
            .and_then(|record| record.current.as_mut())
            .filter(|binding| binding.connection_id == call.connection_id);
        if let Some(task) =
            connection_binding.and_then(|binding| binding.tasks.registry.get_mut(&call.task_id))
        {
            if task.fingerprint != call.fingerprint {
                return Err(rejected(
                    RejectionCode::TaskConflict,
                    "task_id names a task of this connection with another payload",
                ));
            }
            if let Some(supervisor) = &task.supervisor {
                let _ = supervisor.send(SupervisorMessage::Joined(call.message_id.clone()));
            }
            return Ok(());
        }
        if !call.recorded {
            return Err(rejected(
                RejectionCode::ResourceExhausted,
                "the connection has no room to record a new task",
            ));
        }
        let cwd = named_binding(self.record.as_ref(), call)
            .and_then(|binding| binding.cwd.as_ref())
            .map(|cwd| PathBuf::from(cwd.as_str()));
        let task_name = store::task_name();
        let (payload, permissions) = validated(
            shared,
            call,
            cwd.as_deref().unwrap_or(&shared.paths.canonical),
            &task_name,
        )?;
        let prepared = self
            .record
            .as_mut()
            .and_then(|record| record.current.as_mut())
            .filter(|binding| {
                binding.binding_id == call.binding_id
                    && binding.connection_id == call.connection_id
                    && matches!(
                        binding.state,
                        SessionState::Prepared | SessionState::PreparedPendingAck
                    )
            });
        let Some((binding, cwd)) = prepared.zip(cwd) else {
            return Err(binding_unavailable());
        };
        let root = &shared.paths.canonical;
        let start = TaskStart {
            connection_id: call.connection_id.clone(),
            correlation: correlation(call),
            tool: payload.tool,
            permissions,
            limits: payload.limits,
            cwd,
            task_directory: store::task_directory(
                root,
                preparation.session_id.as_str(),
                &task_name,
            ),
            temporary_directory: store::task_temporary_directory(
                &store::task_parent(root),
                &task_name,
            ),
            record: cleanup::record_path(root, &task_name),
            traceparent: call.traceparent.clone(),
            received: call.received,
        };
        let staged = binding.state == SessionState::PreparedPendingAck;
        let tool = tool_of(&start.tool);
        let listener = Arc::new(Events {
            mailbox: self.mailbox.clone(),
            connection_id: call.connection_id.clone(),
            binding_id: call.binding_id.clone(),
            task_id: call.task_id.clone(),
        });
        let supervisor = self
            .starter
            .supervise(start, preparation.reports.clone(), listener);
        if !staged && let Some(supervisor) = &supervisor {
            self.starter.start(supervisor);
        }
        binding.tasks.registry.insert(
            call.task_id.clone(),
            Task {
                fingerprint: call.fingerprint.clone(),
                tool,
                supervisor,
                state: if staged {
                    TaskState::Staged
                } else {
                    TaskState::Started
                },
                namespace: None,
                gone: false,
            },
        );
        if staged {
            binding.tasks.staged.push_back(call.task_id.clone());
        }
        Ok(())
    }

    /// The admission of a `tool.stdin` or `tool.cancel`: the ledger's room,
    /// its schema and budgets, then its task in the named binding's registry,
    /// and for a `tool.stdin` the task's tool. An admitted control goes to
    /// the task's supervisor; a cancel of a staged task also prevents its
    /// start.
    fn control(&mut self, call: &ToolCall) -> Result<(), Body> {
        let payload = control_checks(call)?;
        let task = self
            .record
            .as_mut()
            .and_then(|record| record.current.as_mut())
            .filter(|binding| {
                binding.binding_id == call.binding_id && binding.connection_id == call.connection_id
            })
            .and_then(|binding| {
                let tasks = &mut binding.tasks;
                tasks
                    .registry
                    .get_mut(&call.task_id)
                    .map(|task| (task, &mut tasks.staged))
            });
        let Some((task, staged)) = task else {
            return Err(task_not_found());
        };
        let message = match payload {
            DownstreamPayload::ToolStdin(stdin) => {
                if task.tool != ExecutionTool::ExecCommand {
                    return Err(control_error(
                        ControlErrorCode::UnsupportedControl,
                        "tool.stdin works only on an exec_command task",
                    ));
                }
                SupervisorMessage::Stdin(Box::new(control(call, stdin)))
            }
            DownstreamPayload::ToolCancel(cancel) => {
                if task.state == TaskState::Staged {
                    task.state = TaskState::Prevented;
                    staged.retain(|task_id| *task_id != call.task_id);
                }
                SupervisorMessage::Cancel(Box::new(control(call, cancel)))
            }
            _ => return Ok(()),
        };
        if let Some(supervisor) = &task.supervisor {
            let _ = supervisor.send(message);
        }
        Ok(())
    }

    /// Starts the binding's staged tasks in arrival order, once the ACK of
    /// its `prepared` report has made it `prepared`, before any later
    /// instruction of the binding.
    pub(crate) fn start_staged(&mut self) {
        let Some(binding) = self
            .record
            .as_mut()
            .and_then(|record| record.current.as_mut())
            .filter(|binding| binding.state == SessionState::Prepared)
        else {
            return;
        };
        for supervisor in binding.tasks.start_staged() {
            self.starter.start(&supervisor);
        }
    }

    /// What a task's supervisor reported about the task: its namespace is
    /// noted, once its processes are gone it no longer keeps its Session
    /// `cleanup_pending`, and a task whose start was rejected leaves the
    /// registry, so that its `task_id` names no task.
    pub(crate) fn task_event(&mut self, task: TaskKey, event: TaskEvent) {
        if event == TaskEvent::Gone {
            self.unconfirmed
                .remove(&(task.connection_id.clone(), task.task_id.clone()));
        }
        let Some(record) = self.record.as_mut() else {
            return;
        };
        let Some(tasks) = [record.current.as_mut(), record.unavailable.as_mut()]
            .into_iter()
            .flatten()
            .find(|binding| {
                binding.binding_id == task.binding_id && binding.connection_id == task.connection_id
            })
            .map(|binding| &mut binding.tasks)
        else {
            return;
        };
        if event == TaskEvent::Withdrawn {
            tasks.registry.remove(&task.task_id);
            return;
        }
        let Some(entry) = tasks.registry.get_mut(&task.task_id) else {
            return;
        };
        match event {
            TaskEvent::Started(namespace) => entry.namespace = Some(namespace),
            TaskEvent::Gone => entry.gone = true,
            TaskEvent::Withdrawn => {}
        }
    }

    /// After the shutdown has ended every task, tells the Connection owner
    /// once that each of this Session's tasks is confirmed gone.
    pub(crate) fn answer_shutdown(&mut self) {
        if !self.shutting_down || self.shutdown_answered || !self.unconfirmed.is_empty() {
            return;
        }
        let running = self.record.as_ref().is_some_and(|record| {
            [record.current.as_ref(), record.unavailable.as_ref()]
                .into_iter()
                .flatten()
                .any(|binding| !binding.tasks.all_gone())
        });
        if running {
            return;
        }
        self.shutdown_answered = true;
        if let Some(preparation) = &self.preparation {
            preparation.connection.tasks_gone(&preparation.session_id);
        }
    }

    /// Whether an earlier execution of this Session may still run: a task
    /// of an ended connection, or a record an earlier App Server left, whose
    /// processes are not confirmed gone.
    pub(crate) fn cleanup_pending(&self) -> bool {
        !self.unconfirmed.is_empty()
            || self.preparation.as_ref().is_some_and(|preparation| {
                preparation
                    .shared
                    .leftovers
                    .pending(preparation.session_id.as_str())
            })
    }

    /// The `task_id` of the task whose PID namespace holds the host process
    /// `pid`, among the tasks whose supervisors reported their namespace.
    pub(crate) fn task_owner(&self, pid: u32, reply: TaskOwnerReply) {
        let owner = process::namespace(pid).and_then(|namespace| {
            let record = self.record.as_ref()?;
            [record.current.as_ref(), record.unavailable.as_ref()]
                .into_iter()
                .flatten()
                .flat_map(|binding| binding.tasks.registry.iter())
                .find(|(_, task)| task.namespace == Some(namespace))
                .map(|(task_id, _)| task_id.clone())
        });
        reply(owner);
    }
}

/// A task as its supervisor's events name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskKey {
    pub connection_id: Id,
    pub binding_id: Id,
    pub task_id: Id,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskEvent {
    /// The task's PID namespace.
    Started(u64),
    Gone,
    /// Its start was rejected.
    Withdrawn,
}

/// Gives a supervisor's reports about its task back to the owner as
/// messages.
struct Events {
    mailbox: SessionMailbox,
    connection_id: Id,
    binding_id: Id,
    task_id: Id,
}

impl Events {
    fn send(&self, event: TaskEvent) {
        let _ = self.mailbox.send(SessionMessage::Task {
            task: TaskKey {
                connection_id: self.connection_id.clone(),
                binding_id: self.binding_id.clone(),
                task_id: self.task_id.clone(),
            },
            event,
        });
    }
}

impl TaskListener for Events {
    fn started(&self, namespace: u64) {
        self.send(TaskEvent::Started(namespace));
    }

    fn gone(&self) {
        self.send(TaskEvent::Gone);
    }

    fn withdrawn(&self) {
        self.send(TaskEvent::Withdrawn);
    }
}

/// Rejects a tool instruction of a Session without an owner here: such a
/// Session has no binding, so a `tool.execute` fails at its binding step at
/// the latest and a control finds no task.
pub(crate) fn unbound(shared: &Shared, call: ToolCall, reports: &dyn ReportSink) {
    let body = match call.kind {
        DownstreamType::ToolExecute if !call.recorded => rejected(
            RejectionCode::ResourceExhausted,
            "the connection has no room to record a new task",
        ),
        DownstreamType::ToolExecute => {
            match validated(shared, &call, &shared.paths.canonical, &store::task_name()) {
                Err(body) => body,
                Ok(_) => binding_unavailable(),
            }
        }
        _ => control_checks(&call).err().unwrap_or_else(task_not_found),
    };
    submit(reports, &call, body);
}

/// The binding `call` names, when it is the Session's binding on the call's
/// connection.
fn named_binding<'a>(record: Option<&'a Record>, call: &ToolCall) -> Option<&'a Binding> {
    record
        .and_then(|record| record.current.as_ref())
        .filter(|binding| {
            binding.binding_id == call.binding_id && binding.connection_id == call.connection_id
        })
}

/// Steps 3 and 4 of a `tool.execute`: its schema, budgets and tool against
/// the ready's list, then its policy, translated with `cwd` as the working
/// directory its trusted programs are looked up from and `task_name` naming
/// its temporary directory.
fn validated(
    shared: &Shared,
    call: &ToolCall,
    cwd: &Path,
    task_name: &str,
) -> Result<(ExecutePayload, TaskPermissions), Body> {
    let (payload, value) = match protocol::decode_value::<Downstream>(call.data.as_bytes()) {
        Ok((
            Downstream {
                payload: DownstreamPayload::ToolExecute(payload),
                ..
            },
            value,
        )) => (payload, value),
        _ => return Err(schema_failure(call)),
    };
    let listed = shared.tools.capability.as_ref().is_some_and(|capability| {
        capability
            .tools
            .iter()
            .any(|tool| *tool == tool_of(&payload.tool))
    });
    if !listed {
        return Err(rejected(
            RejectionCode::InvalidInput,
            "tool is not one that this CLI's ready lists",
        ));
    }
    let root = &shared.paths.canonical;
    let parent = store::task_parent(root);
    let inputs = TranslationInputs {
        state_root: root.clone(),
        socket_directory: shared.tools.socket_directory.clone(),
        task_temporary_directory: store::task_temporary_directory(&parent, task_name),
        task_directories: parent,
        drives: None,
        trusted_programs: isolation::trusted_programs(&shared.tools.tetral, cwd),
    };
    let permissions = TaskPermissions::admit(value["payload"].get("sandbox_policy"), &inputs)
        .map_err(Body::Rejected)?;
    Ok((payload, permissions))
}

/// The rejection of a `tool.execute` that does not decode: `invalid_input`,
/// unless it decodes with a valid stand-in for its policy, when its own
/// policy decides as step 4 judges it.
fn schema_failure(call: &ToolCall) -> Body {
    let invalid = || {
        rejected(
            RejectionCode::InvalidInput,
            "the instruction does not match its schema or exceeds a size budget",
        )
    };
    let Ok(mut value) = serde_json::from_str::<Value>(&call.data) else {
        return invalid();
    };
    let Some(policy) = value
        .get_mut("payload")
        .and_then(|payload| payload.get_mut("sandbox_policy"))
    else {
        return invalid();
    };
    let policy = std::mem::replace(policy, stand_in_policy());
    if protocol::decode::<Downstream>(&protocol::encode(&value)).is_err() {
        return invalid();
    }
    match validate(Some(&policy)) {
        Err(rejection) => Body::Rejected(rejection),
        Ok(_) => invalid(),
    }
}

/// The checks of a control before its task is looked up; gives its
/// payload.
fn control_checks(call: &ToolCall) -> Result<DownstreamPayload, Body> {
    if !call.recorded {
        return Err(control_error(
            ControlErrorCode::ResourceExhausted,
            "the connection has no room to record the control",
        ));
    }
    match protocol::decode::<Downstream>(call.data.as_bytes()) {
        Ok(Downstream {
            payload: payload @ (DownstreamPayload::ToolStdin(_) | DownstreamPayload::ToolCancel(_)),
            ..
        }) => Ok(payload),
        _ => Err(control_error(
            ControlErrorCode::InvalidInput,
            "the control does not match its schema or exceeds a size budget",
        )),
    }
}

fn tool_of(input: &ToolInput) -> ExecutionTool {
    match input {
        ToolInput::ExecCommand(_) => ExecutionTool::ExecCommand,
        ToolInput::Edit(_) => ExecutionTool::Edit,
        ToolInput::ViewImage(_) => ExecutionTool::ViewImage,
    }
}

fn rejected(code: RejectionCode, message: &str) -> Body {
    Body::Rejected(ProtocolError {
        code,
        message: message.to_owned(),
    })
}

fn binding_unavailable() -> Body {
    rejected(
        RejectionCode::BindingUnavailable,
        "binding_id names no prepared binding of this Session on this connection",
    )
}

fn control_error(code: ControlErrorCode, message: &str) -> Body {
    Body::Control {
        result: None,
        error: Some(ProtocolError {
            code,
            message: message.to_owned(),
        }),
    }
}

fn task_not_found() -> Body {
    control_error(ControlErrorCode::TaskNotFound, TASK_NOT_FOUND)
}

/// The control `call` with its decoded `payload`, as its task's supervisor
/// takes it.
fn control<P>(call: &ToolCall, payload: P) -> Control<P> {
    Control {
        connection_id: call.connection_id.clone(),
        correlation: correlation(call),
        traceparent: call.traceparent.clone(),
        payload,
    }
}

fn correlation(call: &ToolCall) -> Correlation {
    Correlation {
        session_id: call.session_id.clone(),
        binding_id: call.binding_id.clone(),
        tool_use_event_id: call.tool_use_event_id.clone(),
        task_id: call.task_id.clone(),
        request_message_id: call.message_id.clone(),
    }
}

/// Hands the report that answers `call` to the outbox.
fn submit(reports: &dyn ReportSink, call: &ToolCall, body: Body) {
    let report = Report {
        correlation: correlation(call),
        body,
    };
    let mut submission = report.submission(call.connection_id.clone());
    submission.traceparent = call.traceparent.clone();
    reports.submit(submission);
}
