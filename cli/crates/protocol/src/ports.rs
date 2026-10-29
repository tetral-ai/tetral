//! The interfaces through which the App Server's peer crates reach each other
//! without depending on each other.

use std::sync::Arc;
use std::time::Duration;

use crate::connection::{DownstreamType, McpTools, SessionPreparePayload, SessionStatus};
use crate::id::{Id, SafeId};
use crate::local::SessionEntry;
use crate::tool::{DriveSyncReport, ProtocolErrorReport, ToolStatus};
use crate::{DecodeError, Message, decode, encode};

/// The endpoints of the reports that answer or follow instructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReportEndpoint {
    SessionStatus,
    ToolStatus,
    DriveSync,
    McpTools,
    ProtocolError,
}

impl ReportEndpoint {
    pub fn path(self) -> &'static str {
        match self {
            ReportEndpoint::SessionStatus => "/v1/client/session-status",
            ReportEndpoint::ToolStatus => "/v1/client/tool-status",
            ReportEndpoint::DriveSync => "/v1/client/drive-sync",
            ReportEndpoint::McpTools => "/v1/client/mcp-tools",
            ReportEndpoint::ProtocolError => "/v1/client/protocol-error",
        }
    }
}

/// A report type and the endpoint that takes it.
pub trait Report: Message + Send + 'static {
    const ENDPOINT: ReportEndpoint;
}

impl Report for SessionStatus {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::SessionStatus;
}

impl Report for ToolStatus {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::ToolStatus;
}

impl Report for DriveSyncReport {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::DriveSync;
}

impl Report for McpTools {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::McpTools;
}

impl Report for ProtocolErrorReport {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::ProtocolError;
}

/// Makes a report's bytes from the connection's ID and the report's own
/// `message_id`, once they pass the report's own decoder.
pub type BuildReport = Box<dyn FnOnce(&Id, &Id) -> Result<Vec<u8>, DecodeError> + Send>;

/// One report handed to the Report outbox.
pub struct Submission {
    /// The connection the report belongs to; it is sent on no other.
    pub connection_id: Id,
    pub endpoint: ReportEndpoint,
    /// The `message_id`s of the instructions whose final report this is:
    /// the one it answers and each identical `tool.execute` that joined its
    /// task.
    pub final_for: Vec<Id>,
    /// The `traceparent` of the instruction the report was built for, when
    /// that value is usable.
    pub traceparent: Option<String>,
    /// Told whether the report was acknowledged or dropped.
    pub listener: Option<Arc<dyn ReportListener>>,
    pub build: BuildReport,
}

impl Submission {
    /// The report that `report` makes from the connection's ID and the
    /// report's `message_id`, for the connection `connection_id`.
    pub fn new<R: Report>(
        connection_id: Id,
        report: impl FnOnce(Id, Id) -> R + Send + 'static,
    ) -> Submission {
        Submission {
            connection_id,
            endpoint: R::ENDPOINT,
            final_for: Vec::new(),
            traceparent: None,
            listener: None,
            build: Box::new(move |connection, message| {
                let bytes = encode(&report(connection.clone(), message.clone()));
                decode::<R>(&bytes).map(|_| bytes)
            }),
        }
    }
}

/// The longest `message_id` that the Report outbox gives a report: `r` and a
/// `u64` in decimal. A builder that must fit a report within its limit
/// before the outbox names it counts this one.
pub const LONGEST_REPORT_MESSAGE_ID: &str = "r18446744073709551615";

/// The outbox's name for one submitted report, unique in the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReportId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOutcome {
    /// Its matching ACK came (`ReportAcked`).
    Acked,
    /// Engine refused it or its connection ended; it is never sent again
    /// (`ReportDropped`).
    Dropped,
}

/// Takes the outcome of each report its submitter handed to the outbox.
pub trait ReportListener: Send + Sync {
    fn outcome(&self, report: ReportId, outcome: ReportOutcome);
}

/// A new tool result did not fit the local retained-report budget. The
/// submission and its encoded body have been released; nothing was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportAdmissionError {
    ResourceExhausted,
}

/// Takes every upstream report that `session`, `execution`, `drive` and `mcp`
/// build; its implementation belongs to the Report outbox of `connection`.
pub trait ReportSink: Send + Sync {
    /// Submits a non-tool control reply or a compact tool error. It can use
    /// the reserved report budget when ordinary capacity is exhausted. If
    /// neither has room, the logical connection ends and the report is
    /// dropped. Only a matching ACK confirms delivery.
    fn submit(&self, submission: Submission) -> ReportId;

    /// Encodes and admits an ordinary tool result before it enters the
    /// mailbox. Capacity failure returns to the execution owner, which
    /// releases the result and submits a compact resource error, stopping
    /// and reaping a still-running command first. Admitted reports keep the
    /// same immutable bytes through every attempt. Existing sender-check
    /// failures and reports of ended connections still produce Dropped.
    fn try_submit(&self, submission: Submission) -> Result<ReportId, ReportAdmissionError> {
        Ok(self.submit(submission))
    }

    /// Stops the report's retries and deletes it.
    fn cancel(&self, report: ReportId);

    /// Makes the report also the final report of `instruction`, an identical
    /// `tool.execute` that joined its task after it was submitted; a report
    /// already acknowledged or dropped is left as it was.
    fn add_final(&self, report: ReportId, instruction: Id);
}

/// A `session.prepare` that passed the Connection owner's admission, for the
/// Session binding owner of its `session_id`.
#[derive(Debug, Clone)]
pub struct Prepare {
    /// The online connection the Connection owner routed it on.
    pub connection_id: Id,
    pub message_id: Id,
    /// The `connection_id` of the instruction's envelope.
    pub addressed_connection_id: Id,
    pub payload: SessionPreparePayload,
    /// The payload's content fingerprint (`fingerprint`).
    pub fingerprint: String,
    /// The instruction's `traceparent`, when it is usable.
    pub traceparent: Option<String>,
}

/// A `session.close` for the Session binding owner of its `session_id`.
#[derive(Debug, Clone)]
pub struct Close {
    pub connection_id: Id,
    pub message_id: Id,
    pub binding_id: Id,
}

/// A `tool.execute`, `tool.stdin` or `tool.cancel` that the replay ledger
/// has read, for the Session binding owner of its `session_id`, which
/// decodes, admits or rejects it.
#[derive(Debug, Clone)]
pub struct ToolCall {
    /// The online connection the Connection owner routed it on.
    pub connection_id: Id,
    pub message_id: Id,
    pub kind: DownstreamType,
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    /// The event's data as it arrived.
    pub data: String,
    /// The payload's content fingerprint (`fingerprint`), which tells a
    /// repeated task from a conflicting one.
    pub fingerprint: String,
    /// Whether the replay ledger recorded it: a new `tool.execute` it had no
    /// room for is rejected `resource_exhausted`, a control that did not fit
    /// even its reserve gets that control error.
    pub recorded: bool,
    /// The instruction's `traceparent`, when it is usable.
    pub traceparent: Option<String>,
    /// When the CLI received it, on the seam clock.
    pub received: Duration,
}

/// One probe-slot request of a Session binding owner, unique within it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProbeRequest(pub u64);

/// One git-ticket request of a Session binding owner, unique within it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TicketRequest(pub u64);

/// The Connection owner's answer to a git-ticket request: the ticket, or
/// the message a repository failure carries, the error body's `message` or
/// the CLI's description of an answer that failed its schema.
pub type TicketAnswer = Result<crate::connection::GitTicketResponse, String>;

/// The Connection owner's answer to a probe-slot request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSlot {
    /// The slot is the requester's until it returns it.
    Granted,
    /// No slot came free within the wait.
    Exhausted,
    /// The connection ended or the App Server shuts down.
    Cancelled,
}

/// Takes one Session binding owner's entries of `GET /v1/sessions`.
pub type EntriesReply = Box<dyn FnOnce(Vec<SessionEntry>) + Send>;

/// Creates the Session binding owner of a Session for the Connection owner;
/// its implementation belongs to `session`.
pub trait SessionFactory: Send + Sync {
    /// Starts the owner of `session_id`, which reaches the Connection owner
    /// through `connection` and hands its reports to `reports`.
    fn create(
        &self,
        session_id: SafeId,
        connection: Arc<dyn ConnectionPort>,
        reports: Arc<dyn ReportSink>,
    ) -> Arc<dyn SessionHandle>;

    /// Answers a tool instruction of a Session that has no owner here, so no
    /// binding: it is rejected at its first failing admission step, which is
    /// at the latest its binding's.
    fn unbound_tool(&self, call: ToolCall, reports: Arc<dyn ReportSink>);
}

/// The Connection owner's handle on one Session binding owner; its
/// implementation belongs to `session`. Each call is a message the owner
/// handles in arrival order.
pub trait SessionHandle: Send + Sync {
    fn prepare(&self, prepare: Prepare);
    fn close(&self, close: Close);
    fn tool(&self, call: ToolCall);
    /// The answer to the owner's request `request`.
    fn probe_slot(&self, request: ProbeRequest, answer: ProbeSlot);
    /// The answer to the owner's git-ticket request `request`.
    fn git_ticket(&self, request: TicketRequest, answer: TicketAnswer);
    /// The connection ended: its bindings become `unavailable`.
    fn connection_ended(&self, connection_id: Id);
    /// The App Server shuts down: running probes are killed and every task
    /// ends; once each task's processes are confirmed gone, the owner tells
    /// the Connection owner ([`ConnectionPort::tasks_gone`]).
    fn shutdown(&self);
    /// The owner's bindings as `GET /v1/sessions` lists them, read from its
    /// state alone.
    fn entries(&self, reply: EntriesReply);
}

/// Takes the `task_id` of the task a host process belongs to, or `None`.
pub type TaskOwnerReply = Box<dyn FnOnce(Option<Id>) + Send>;

/// Which task of a Session a host process belongs to, for the Session's
/// drive sync; its implementation belongs to `session`. On Linux a process
/// belongs to the task whose PID namespace holds it; a process in no task's
/// namespace belongs to none.
pub trait TaskOwnership: Send + Sync {
    fn task_of(&self, pid: u32, reply: TaskOwnerReply);
}

/// How Session binding owners and Drive sync owners reach the Connection
/// owner; its implementation belongs to `connection`. The Connection owner
/// counts the Session records and the running directory probes.
pub trait ConnectionPort: Send + Sync {
    /// Asks for a probe slot (`ProbeSlot`); the answer comes through
    /// [`SessionHandle::probe_slot`].
    fn probe_slot(&self, session_id: &SafeId, request: ProbeRequest);
    /// Withdraws a request that may still wait (`ProbeSlotWithdraw`).
    fn probe_slot_withdraw(&self, session_id: &SafeId, request: ProbeRequest);
    /// Returns a slot once its probe is reaped, or unused (`ProbeSlotFree`).
    fn probe_slot_free(&self);
    /// Fetches a git ticket for the binding `binding_id` of the connection
    /// `connection_id` (`FetchGitTicket`); the answer comes through
    /// [`SessionHandle::git_ticket`], and none comes once that connection
    /// has ended.
    fn fetch_git_ticket(
        &self,
        session_id: &SafeId,
        request: TicketRequest,
        connection_id: &Id,
        binding_id: &Id,
    );
    /// The Session's record is deleted (`RecordGone`).
    fn record_gone(&self, session_id: &SafeId);
    /// After the shutdown, every task of the Session is confirmed gone
    /// (`TasksGone`).
    fn tasks_gone(&self, session_id: &SafeId);
    /// A `tool.execute` staged at `staged`, on the seam clock, waits in its
    /// binding's staging queue (`TaskStaged`).
    fn task_staged(&self, staged: Duration);
    /// The `tool.execute` staged at `staged` left its queue without
    /// starting: a cancel or a close prevented it, or its connection's end
    /// discarded it (`TaskUnstaged`).
    fn task_unstaged(&self, staged: Duration);
    /// A task starts: from its staging queue when it was staged at `staged`,
    /// otherwise at its admission (`TaskStarted`).
    fn task_started(&self, staged: Option<Duration>);
    /// A started task ended: its processes are confirmed gone, or its start
    /// was rejected (`TaskEnded`).
    fn task_ended(&self);
    /// A `tool.execute` was rejected `resource_exhausted` (`TaskRefused`).
    fn task_refused(&self);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::{ProtocolError, ProtocolErrorCode};

    fn protocol_error(message: String) -> Submission {
        Submission::new(Id::new("conn_a").unwrap(), move |connection, message_id| {
            ProtocolErrorReport {
                protocol_version: Default::default(),
                connection_id: connection,
                message_id,
                request_message_id: None,
                error: ProtocolError {
                    code: ProtocolErrorCode::InvalidMessage,
                    message,
                },
            }
        })
    }

    #[test]
    fn a_report_gets_its_identity_at_build_and_one_over_its_limit_fails_its_own_decoder() {
        let connection = Id::new("conn_a").unwrap();
        let message_id = Id::new("r1").unwrap();
        let submission = protocol_error("the message cannot be parsed".to_owned());
        assert_eq!(submission.endpoint, ReportEndpoint::ProtocolError);
        let bytes = (submission.build)(&connection, &message_id).unwrap();
        let report: ProtocolErrorReport = decode(&bytes).unwrap();
        assert_eq!(
            (report.connection_id, report.message_id),
            (connection.clone(), message_id.clone())
        );

        let oversized = protocol_error("x".repeat(65_536));
        assert!((oversized.build)(&connection, &message_id).is_err());
    }
}
