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

use execution::policy::{DriveMount, SessionDrives, TaskPermissions, TranslationInputs, validate};
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
    /// The cua-driver that `config.toml` recorded when the App Server
    /// started.
    pub cua_driver: Option<PathBuf>,
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
    /// The Session's drive sync, once it is open.
    pub drive_sync: Option<Arc<dyn protocol::ports::DriveSyncPort>>,
}

/// One binding's tasks: its registry by `task_id` and its staging queue.
#[derive(Debug, Default)]
pub struct Tasks {
    registry: HashMap<Id, Task>,
    /// The tasks admitted while the binding waits for its ACK, in arrival
    /// order, each with the time on the seam clock it was staged.
    staged: VecDeque<(Id, Duration)>,
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
    /// another binding's ACK. Gives when each one with a supervisor was
    /// staged.
    fn prevent_staged(&mut self) -> Vec<Duration> {
        let mut unstaged = Vec::new();
        for (task_id, staged) in std::mem::take(&mut self.staged) {
            if let Some(task) = self.registry.get_mut(&task_id) {
                task.state = TaskState::Prevented;
                if task.supervisor.is_some() {
                    unstaged.push(staged);
                }
            }
        }
        unstaged
    }

    /// The binding closes: staged tasks never start, and each makes the
    /// `terminal` report of a cancelled staged task. Gives when each one
    /// with a supervisor was staged.
    pub fn close(&mut self) -> Vec<Duration> {
        let unstaged = self.prevent_staged();
        self.send_all(|| SupervisorMessage::Prevent);
        unstaged
    }

    /// The connection ended: staged tasks never start, and every task's
    /// supervisor ends its task. Gives the tasks whose processes are not
    /// yet confirmed gone, and when each discarded staged task with a
    /// supervisor was staged.
    pub fn connection_ended(&mut self) -> (Vec<Id>, Vec<Duration>) {
        let unstaged = self.prevent_staged();
        self.send_all(|| SupervisorMessage::ConnectionEnded);
        (self.unconfirmed().cloned().collect(), unstaged)
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

    /// The staged tasks in arrival order, each now started, with its
    /// supervisor and the time it was staged.
    fn start_staged(&mut self) -> Vec<(Id, SupervisorMailbox, Duration)> {
        std::mem::take(&mut self.staged)
            .into_iter()
            .filter_map(|(task_id, staged)| {
                let task = self.registry.get_mut(&task_id)?;
                task.state = TaskState::Started;
                Some((task_id, task.supervisor.clone()?, staged))
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
            preparation.connection.task_refused();
            return Err(rejected(
                RejectionCode::ResourceExhausted,
                "the connection has no room to record a new task",
            ));
        }
        let named = named_binding(self.record.as_ref(), call);
        let cwd = named
            .and_then(|binding| binding.cwd.as_ref())
            .map(|cwd| PathBuf::from(cwd.as_str()));
        let drives = named.and_then(|binding| self.session_drives(binding));
        let task_name = store::task_name();
        let (payload, permissions) = validated(
            shared,
            call,
            cwd.as_deref().unwrap_or(&shared.paths.canonical),
            &task_name,
            drives,
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
            drive_sync: match &self.drive_sync {
                crate::drives::DriveSyncSlot::Open(sync) => Some(Arc::new(sync.clone())),
                _ => None,
            },
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
        let now = state::seams::now();
        if let Some(supervisor) = &supervisor {
            if staged {
                preparation.connection.task_staged(now);
            } else {
                self.starter.start(supervisor);
                preparation.connection.task_started(None);
                self.running
                    .insert((call.connection_id.clone(), call.task_id.clone()));
            }
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
            binding.tasks.staged.push_back((call.task_id.clone(), now));
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
                    if let Some(position) = staged
                        .iter()
                        .position(|(task_id, _)| *task_id == call.task_id)
                        && let Some((_, at)) = staged.remove(position)
                        && task.supervisor.is_some()
                        && let Some(preparation) = &self.preparation
                    {
                        preparation.connection.task_unstaged(at);
                    }
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
        let connection_id = binding.connection_id.clone();
        for (task_id, supervisor, staged) in binding.tasks.start_staged() {
            self.starter.start(&supervisor);
            if let Some(preparation) = &self.preparation {
                preparation.connection.task_started(Some(staged));
            }
            self.running.insert((connection_id.clone(), task_id));
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
        if matches!(event, TaskEvent::Gone | TaskEvent::Withdrawn)
            && self
                .running
                .remove(&(task.connection_id.clone(), task.task_id.clone()))
            && let Some(preparation) = &self.preparation
        {
            preparation.connection.task_ended();
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
        // The drive sync's upload triggers: the quiet period while an
        // `exec_command` runs, and the exit of the task that wrote.
        if let crate::drives::DriveSyncSlot::Open(sync) = &self.drive_sync {
            match event {
                TaskEvent::Started(_) if entry.tool == ExecutionTool::ExecCommand => {
                    sync.task_started(&task.task_id);
                }
                TaskEvent::Gone => sync.task_exited(&task.task_id),
                _ => {}
            }
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
    /// `pid`, among the tasks whose supervisors reported their namespace and
    /// whose processes are not confirmed gone: a confirmed task's namespace
    /// holds no process, and its inode may name a later namespace.
    pub(crate) fn task_owner(&self, pid: u32, reply: TaskOwnerReply) {
        let owner = process::namespace(pid).and_then(|namespace| {
            let record = self.record.as_ref()?;
            [record.current.as_ref(), record.unavailable.as_ref()]
                .into_iter()
                .flatten()
                .flat_map(|binding| binding.tasks.registry.iter())
                .find(|(_, task)| !task.gone && task.namespace == Some(namespace))
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
            match validated(
                shared,
                &call,
                &shared.paths.canonical,
                &store::task_name(),
                None,
            ) {
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

impl<S: TaskStarter> SessionOwner<S> {
    /// The drive mounts a task of `binding` reaches: those under the
    /// `TETRAL_DRIVES` folder its drive sync gives, each with its access.
    fn session_drives(&self, binding: &Binding) -> Option<SessionDrives> {
        let crate::drives::DriveSyncSlot::Open(sync) = &self.drive_sync else {
            return None;
        };
        let directory = protocol::ports::DriveSyncPort::drives_variable(sync)?;
        Some(SessionDrives {
            mounts: binding
                .drives
                .iter()
                .map(|drive| DriveMount {
                    path: directory.join(drive.drive_id.as_str()),
                    access: drive.access,
                })
                .collect(),
            directory,
        })
    }
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
    drives: Option<SessionDrives>,
) -> Result<(ExecutePayload, TaskPermissions), Body> {
    let (payload, value) = match protocol::decode_value::<Downstream>(call.data.as_bytes()) {
        Ok((
            Downstream {
                payload: DownstreamPayload::ToolExecute(payload),
                ..
            },
            value,
        )) => (payload, value),
        _ => return Err(schema_failure(shared, call)),
    };
    check_tool(shared, call, &payload.tool)?;
    let root = &shared.paths.canonical;
    let parent = store::task_parent(root);
    let inputs = TranslationInputs {
        state_root: root.clone(),
        socket_directory: shared.tools.socket_directory.clone(),
        task_temporary_directory: store::task_temporary_directory(&parent, task_name),
        task_directories: parent,
        drives,
        trusted_programs: isolation::trusted_programs(
            &shared.tools.tetral,
            shared.tools.cua_driver.as_deref(),
            cwd,
        ),
    };
    let permissions = TaskPermissions::admit(value["payload"].get("sandbox_policy"), &inputs)
        .map_err(Body::Rejected)?;
    Ok((payload, permissions))
}

/// The rest of step 3 for a decoded `tool`: one that the ready lists, and for
/// `exec_command` an `input.task_id`, when given, equal to the outer
/// `task_id`.
fn check_tool(shared: &Shared, call: &ToolCall, tool: &ToolInput) -> Result<(), Body> {
    let listed = shared.tools.capability.as_ref().is_some_and(|capability| {
        capability
            .tools
            .iter()
            .any(|listed| *listed == tool_of(tool))
    });
    if !listed {
        return Err(rejected(
            RejectionCode::InvalidInput,
            "tool is not one that this CLI's ready lists",
        ));
    }
    if let ToolInput::ExecCommand(input) = tool
        && input
            .task_id
            .value()
            .is_some_and(|task_id| *task_id != call.task_id)
    {
        return Err(rejected(
            RejectionCode::InvalidInput,
            "input.task_id differs from the instruction's task_id",
        ));
    }
    Ok(())
}

/// The rejection of a `tool.execute` that does not decode: `invalid_input`,
/// unless it passes step 3 with a valid stand-in for its policy, its budgets
/// measured on the instruction as sent, when its own policy decides as step 4
/// judges it.
fn schema_failure(shared: &Shared, call: &ToolCall) -> Body {
    let invalid = || {
        rejected(
            RejectionCode::InvalidInput,
            "the instruction does not match its schema or exceeds a size budget",
        )
    };
    let mut policy = None;
    let decoded = protocol::decode_replaced::<Downstream>(call.data.as_bytes(), |value| {
        if let Some(own) = value
            .get_mut("payload")
            .and_then(|payload| payload.get_mut("sandbox_policy"))
        {
            policy = Some(std::mem::replace(own, stand_in_policy()));
        }
    });
    let (
        Ok((
            Downstream {
                payload: DownstreamPayload::ToolExecute(payload),
                ..
            },
            _,
        )),
        Some(policy),
    ) = (decoded, policy)
    else {
        return invalid();
    };
    if let Err(body) = check_tool(shared, call, &payload.tool) {
        return body;
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
