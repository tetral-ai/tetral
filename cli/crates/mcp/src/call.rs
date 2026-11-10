//! Local MCP calls: an `mcp.call` from the checks that follow its binding's
//! own to its one final report.
//!
//! - Its binding's owner checks the binding, the task's identity and the
//!   payload ([`check`]), then hands the call over ([`Calls::call`]),
//!   staged when it waits for its binding's ACK ([`Calls::start`]).
//! - A call is pending until its `tools/call` is written, then sent until
//!   its terminal report. Check 4: the server is running and the binding's
//!   connection to it is open, or opens within its start limit
//!   (`binding.rs`); check 5: the replay ledger recorded it. A call failing
//!   either is `rejected` and withdrawn from its binding's registry.
//! - It then writes one `tools/call {name, arguments}` and waits for the
//!   answer at most `limits::CALL` from the write on the seam clock
//!   (counter `mcp.call_checked`); at that bound it sends
//!   `notifications/cancelled` with reason `request timeout` and reports
//!   `mcp_timeout`.
//! - The answer is checked in order: a JSON-RPC error is `mcp_error`, with
//!   the error bounds of every report and its own last fallback; the result
//!   must be a `CallToolResult` and its media valid base64, else
//!   `mcp_invalid_result`; refused images are replaced (`media.rs`); the
//!   media, the envelope without them and the report must each fit its
//!   bound, else `mcp_result_too_large` with the three sizes. Otherwise the
//!   result goes unchanged but for its top-level `_meta`.
//! - The terminal report takes only the outbox's ordinary capacity; one that
//!   does not fit is discarded for `resource_exhausted` with its report
//!   bytes, which the reserve admits, so a full budget ends only that call.
//! - A `tool.cancel` of a pending call ends it `cancelled` with `sent:
//!   false`; of a sent one, it sends `notifications/cancelled` with reason
//!   `cancelled by Engine` and ends it with `sent: true`; a later answer is
//!   discarded. A call its answer, timeout or connection's end ended stays
//!   as it ended. Each `tool.cancel` gets its reply.
//! - A closed binding, a disable, an enable and the shutdown end calls as a
//!   `tool.cancel` does; an Engine connection's end does too, without any
//!   report. A server that exits ends its sent calls `mcp_server_exited` and
//!   rejects the ones waiting for a connection.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use protocol::DecodeError;
use protocol::connection::{Downstream, DownstreamPayload};
use protocol::field::{Count, NonEmptyString, Nullable, PositiveInt};
use protocol::id::Id;
use protocol::ports::{ReportId, ReportSink};
use protocol::report::{Body, Correlation, ErrorBounds, Report, TASK_NOT_FOUND, bounded_error};
use protocol::tool::{
    ControlErrorCode, ExecResult, HelperEnvelope, HelperStatus, HelperTool, MCP_ARGUMENTS_OVER,
    McpCallPayload, McpResult, Metrics, Object, ProtocolError, RejectionCode, ReportPhase,
    StreamSnapshot, ToolError, ToolResult,
};
use serde_json::{Value, json};

use crate::limits;
use crate::transport::{RawAnswer, Writer};
use crate::{CUA, Manager, McpManager, Message};

/// A call by its connection, binding and `task_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CallKey {
    pub connection_id: Id,
    pub binding_id: Id,
    pub task_id: Id,
}

impl CallKey {
    pub fn binding(&self) -> BindingKey {
        BindingKey {
            connection_id: self.connection_id.clone(),
            binding_id: self.binding_id.clone(),
        }
    }
}

/// A binding by its connection and `binding_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BindingKey {
    pub connection_id: Id,
    pub binding_id: Id,
}

/// An `mcp.call` its binding's owner admitted through checks 1 to 3.
pub struct Call {
    pub connection_id: Id,
    pub correlation: Correlation,
    pub payload: McpCallPayload,
    /// The binding's canonical working directory, where its connection's
    /// process runs.
    pub cwd: PathBuf,
    /// Whether the replay ledger recorded the call.
    pub recorded: bool,
    pub traceparent: Option<String>,
    /// When the instruction was received, on the seam clock.
    pub received: Duration,
    /// It waits for its binding's ACK, until [`Calls::start`].
    pub staged: bool,
    pub reports: Arc<dyn ReportSink>,
    /// Runs when check 4 or 5 rejects the call, so that its `task_id` names
    /// no task.
    pub withdrawn: Box<dyn FnOnce() + Send>,
}

impl Call {
    pub fn key(&self) -> CallKey {
        CallKey {
            connection_id: self.connection_id.clone(),
            binding_id: self.correlation.binding_id.clone(),
            task_id: self.correlation.task_id.clone(),
        }
    }
}

/// A `tool.cancel` of a call, admitted on the call's connection: the
/// identities its reply answers.
pub struct Cancel {
    pub correlation: Correlation,
    pub traceparent: Option<String>,
}

/// Why a binding's calls and connections end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Release {
    /// The binding closes: each call ends `cancelled`.
    Closed,
    /// Its Engine connection ended: no report.
    ConnectionEnded,
}

/// Runs once a released binding's connections' processes have exited.
pub type Released = Box<dyn FnOnce() + Send>;

/// The calls of the bindings, as their owners hand them to the MCP manager.
pub trait Calls: Send + Sync + std::fmt::Debug {
    fn call(&self, call: Call);
    /// The staged call's binding is `prepared`.
    fn start(&self, call: CallKey);
    /// An identical `mcp.call` with this `message_id` joined the call: its
    /// final report ends that instruction too.
    fn join(&self, call: CallKey, message_id: Id);
    fn cancel(&self, call: CallKey, cancel: Cancel);
    /// Ends the binding's calls and closes its connections; `released` runs
    /// once their processes have exited.
    fn release(&self, binding: BindingKey, release: Release, released: Released);
}

/// Takes no call, for an App Server part that runs without the MCP
/// manager: a release runs at once.
#[derive(Debug, Clone, Copy)]
pub struct NoCalls;

impl Calls for NoCalls {
    fn call(&self, _: Call) {}

    fn start(&self, _: CallKey) {}

    fn join(&self, _: CallKey, _: Id) {}

    fn cancel(&self, _: CallKey, _: Cancel) {}

    fn release(&self, _: BindingKey, _: Release, released: Released) {
        released();
    }
}

impl Calls for McpManager {
    fn call(&self, call: Call) {
        self.send(Message::Call(Box::new(call)));
    }

    fn start(&self, call: CallKey) {
        self.send(Message::Start(call));
    }

    fn join(&self, call: CallKey, message_id: Id) {
        self.send(Message::Join(call, message_id));
    }

    fn cancel(&self, call: CallKey, cancel: Cancel) {
        self.send(Message::Cancel(call, Box::new(cancel)));
    }

    fn release(&self, binding: BindingKey, release: Release, released: Released) {
        self.send(Message::Release {
            binding,
            release,
            released,
        });
    }
}

/// The message of an `invalid_input` rejection for `arguments`.
pub const ARGUMENTS_INVALID: &str = "arguments must be a JSON object of at most 65536 bytes";

/// Check 3 of an `mcp.call`, the instruction `data`: `arguments` a JSON
/// object of at most 65,536 bytes as written, then every other rule of its
/// payload and its size budgets. Gives the payload, or the message of its
/// `invalid_input` rejection.
pub fn check(data: &str) -> Result<McpCallPayload, String> {
    let error = match protocol::decode::<Downstream>(data.as_bytes()) {
        Ok(Downstream {
            payload: DownstreamPayload::McpCall(payload),
            ..
        }) => return Ok(payload),
        Ok(_) => return Err("invalid mcp.call: not an mcp.call".to_owned()),
        Err(error) => error,
    };
    let arguments_object = serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|value| value.get("payload")?.get("arguments").map(Value::is_object));
    if error == DecodeError::Rule(MCP_ARGUMENTS_OVER) || arguments_object == Some(false) {
        return Err(ARGUMENTS_INVALID.to_owned());
    }
    Err(format!("invalid mcp.call: {error}"))
}

/// A call the manager holds, from its handing over until its binding is
/// released.
pub(crate) struct Entry {
    connection_id: Id,
    correlation: Correlation,
    server: String,
    tool: String,
    /// Until its `tools/call` is written.
    arguments: Option<Object>,
    pub cwd: PathBuf,
    recorded: bool,
    traceparent: Option<String>,
    received: Duration,
    reports: Arc<dyn ReportSink>,
    withdrawn: Option<Box<dyn FnOnce() + Send>>,
    /// The `message_id`s of the identical calls that joined it.
    joined: Vec<Id>,
    /// When the call left admission and any staging queue, after its binding
    /// connection opened and the replay-ledger check passed.
    left_queue: Option<Duration>,
    pub state: State,
}

pub(crate) enum State {
    Staged,
    /// Waits for its binding's connection to open.
    Opening,
    /// Its `tools/call`, tagged `tag`, is queued on the connection `key`,
    /// and written at `written`.
    Sent {
        tag: u64,
        key: u64,
        written: Option<Duration>,
    },
    Ended {
        report: ReportId,
        how: Ended,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ended {
    /// Its answer, its timeout or its connection's end.
    Finished,
    Cancelled {
        sent: bool,
    },
    /// Check 4 or 5.
    Rejected,
}

impl State {
    fn is_ended(&self) -> bool {
        matches!(self, State::Ended { .. })
    }
}

/// How a call that has not ended ends.
enum Outcome {
    Result(Box<McpResult>),
    Error(ToolError),
    Rejected(RejectionCode, String),
    /// A connection's end, with nothing to report.
    Silent,
}

impl Manager {
    pub(crate) fn call(&mut self, call: Call) {
        let key = call.key();
        let staged = call.staged;
        let entry = Entry {
            connection_id: call.connection_id,
            correlation: call.correlation,
            server: call.payload.server.as_str().to_owned(),
            tool: call.payload.tool.as_str().to_owned(),
            arguments: Some(call.payload.arguments),
            cwd: call.cwd,
            recorded: call.recorded,
            traceparent: call.traceparent,
            received: call.received,
            reports: call.reports,
            withdrawn: Some(call.withdrawn),
            joined: Vec::new(),
            left_queue: None,
            state: State::Staged,
        };
        self.config.activity.mcp_call_started();
        self.calls.insert(key.clone(), entry);
        if !staged {
            self.admit(key);
        }
    }

    pub(crate) fn start_call(&mut self, key: CallKey) {
        if self
            .calls
            .get(&key)
            .is_some_and(|entry| matches!(entry.state, State::Staged))
        {
            self.admit(key);
        }
    }

    /// Check 4: the server, then the binding's connection.
    fn admit(&mut self, key: CallKey) {
        let Some(entry) = self.calls.get_mut(&key) else {
            return;
        };
        // A server runs once its `initialize` succeeded, not from its spawn.
        let running = self.run.as_ref().is_some_and(|run| run.open.is_some());
        if entry.server != CUA || !running {
            let message = format!("server {} is not running", entry.server);
            return self.end(
                &key,
                Outcome::Rejected(RejectionCode::McpServerUnavailable, message),
            );
        }
        entry.state = State::Opening;
        let cwd = entry.cwd.clone();
        if let Some((connection, writer)) = self.binding_connection(&key, cwd) {
            self.write(&key, connection, &writer);
        }
    }

    /// Check 5, then the call's `tools/call` on the open connection `key`.
    pub(crate) fn write(&mut self, call: &CallKey, key: u64, writer: &Writer) {
        let Some(entry) = self
            .calls
            .get_mut(call)
            .filter(|entry| matches!(entry.state, State::Opening))
        else {
            return;
        };
        if !entry.recorded {
            return self.end(
                call,
                Outcome::Rejected(
                    RejectionCode::ResourceExhausted,
                    "the connection has no room to record a new task".to_owned(),
                ),
            );
        }
        self.next_tag += 1;
        let tag = self.next_tag;
        let arguments = entry.arguments.take().unwrap_or_default();
        entry.left_queue = Some(state::seams::now());
        writer.request(
            tag,
            "tools/call",
            json!({ "name": entry.tool, "arguments": arguments }),
        );
        entry.state = State::Sent {
            tag,
            key,
            written: None,
        };
        self.tagged.insert(tag, call.clone());
    }

    /// The `tools/call` tagged `tag` is written: its time bound starts.
    pub(crate) fn written(&mut self, tag: u64) {
        let Some(entry) = self
            .tagged
            .get(&tag)
            .and_then(|call| self.calls.get_mut(call))
        else {
            return;
        };
        if let State::Sent { written, .. } = &mut entry.state
            && written.is_none()
        {
            let now = state::seams::now();
            *written = Some(now);
            self.deadlines.insert((now + self.config.limits.call, tag));
        }
    }

    /// Whether `tag` is a call's; its answer then ends the call.
    pub(crate) fn call_answered(&mut self, tag: u64, answer: RawAnswer) -> bool {
        let Some(call) = self.tagged.get(&tag).cloned() else {
            return false;
        };
        let outcome = match answer {
            RawAnswer::Error(error) => mcp_error(error),
            RawAnswer::Result(result) => match call_tool_result(result) {
                Ok(result) => Outcome::Result(Box::new(result)),
                Err(violation) => invalid(&format!(
                    "result is not a valid CallToolResult: {violation}"
                )),
            },
        };
        self.end(&call, outcome);
        true
    }

    /// The earliest time bound of a sent call.
    pub(crate) fn call_deadline(&self) -> Option<Duration> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }

    /// Ends each sent call whose time bound has come.
    pub(crate) fn calls_timed_out(&mut self) {
        let now = state::seams::now();
        while let Some(&(deadline, tag)) = self.deadlines.first() {
            if deadline > now {
                return;
            }
            self.deadlines.pop_first();
            let Some(call) = self.tagged.get(&tag).cloned() else {
                continue;
            };
            self.notify_cancelled(&call, "request timeout");
            self.end(
                &call,
                Outcome::Error(tool_error(
                    "mcp_timeout",
                    "no answer within 300 seconds",
                    None,
                )),
            );
        }
    }

    pub(crate) fn join(&mut self, key: &CallKey, message_id: Id) {
        let Some(entry) = self.calls.get_mut(key) else {
            return;
        };
        match &entry.state {
            State::Ended { report, .. } => entry.reports.add_final(*report, message_id),
            _ => entry.joined.push(message_id),
        }
    }

    /// A `tool.cancel` of the call: the cancellation table, then its reply.
    pub(crate) fn cancel(&mut self, key: &CallKey, cancel: Cancel) {
        let Some(entry) = self.calls.get(key) else {
            return;
        };
        let reports = entry.reports.clone();
        let connection_id = entry.connection_id.clone();
        let ended = match &entry.state {
            State::Ended { how, .. } => Some(*how),
            _ => None,
        };
        let reply = match ended {
            Some(Ended::Rejected) => Err(()),
            Some(Ended::Finished) => Ok((false, false)),
            Some(Ended::Cancelled { sent }) => Ok((true, sent)),
            None => Ok((true, self.cancelled(key, true))),
        };
        let body = match reply {
            Ok((cancelled, was_running)) => cancel_reply(&cancel, cancelled, was_running),
            Err(()) => Body::Control {
                result: None,
                error: Some(ProtocolError {
                    code: ControlErrorCode::TaskNotFound,
                    message: TASK_NOT_FOUND.to_owned(),
                }),
            },
        };
        let mut submission = Report {
            correlation: cancel.correlation,
            body,
        }
        .submission(connection_id);
        submission.traceparent = cancel.traceparent;
        reports.submit(submission);
    }

    /// Ends a call that has not ended as `tool.cancel` does: a sent one
    /// gets `notifications/cancelled`. Reports `cancelled` when `report`;
    /// gives whether it was sent.
    fn cancelled(&mut self, key: &CallKey, report: bool) -> bool {
        let Some(entry) = self.calls.get(key) else {
            return false;
        };
        let sent = match entry.state {
            State::Ended { .. } => return false,
            State::Sent { .. } => true,
            State::Staged | State::Opening => false,
        };
        if sent {
            self.notify_cancelled(key, "cancelled by Engine");
        }
        let outcome = if report {
            cancelled(sent)
        } else {
            Outcome::Silent
        };
        self.end(key, outcome);
        if let Some(State::Ended { how, .. }) =
            self.calls.get_mut(key).map(|entry| &mut entry.state)
        {
            *how = Ended::Cancelled { sent };
        }
        sent
    }

    /// Sends `notifications/cancelled` for the sent call with `reason`.
    fn notify_cancelled(&self, key: &CallKey, reason: &str) {
        let Some(State::Sent {
            tag,
            key: connection,
            ..
        }) = self.calls.get(key).map(|entry| &entry.state)
        else {
            return;
        };
        if let Some(writer) = self.writer_of(*connection) {
            writer.notify(
                "notifications/cancelled",
                json!({ "requestId": Writer::id(*tag), "reason": reason }),
            );
        }
    }

    /// Ends every call that has not ended and that `matches`, as
    /// `tool.cancel` does, reporting when `report`.
    pub(crate) fn cancel_calls(&mut self, matches: impl Fn(&CallKey) -> bool, report: bool) {
        let keys: Vec<CallKey> = self
            .calls
            .iter()
            .filter(|(key, entry)| matches(key) && !entry.state.is_ended())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.cancelled(&key, report);
        }
    }

    /// The server is no longer running because it exited: each sent call
    /// ends `mcp_server_exited`, and each one waiting for a connection fails
    /// check 4.
    pub(crate) fn server_exited(&mut self) {
        let keys: Vec<CallKey> = self
            .calls
            .iter()
            .filter(|(_, entry)| matches!(entry.state, State::Sent { .. } | State::Opening))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            let outcome = match self.calls.get(&key).map(|entry| &entry.state) {
                Some(State::Sent { .. }) => exited(),
                _ => Outcome::Rejected(
                    RejectionCode::McpServerUnavailable,
                    format!("server {CUA} is not running"),
                ),
            };
            self.end(&key, outcome);
        }
    }

    /// The binding connection `connection` ended without being stopped:
    /// its sent calls end `mcp_server_exited`, and the calls waiting for it
    /// to open, when it never did, fail check 4 with `failure`.
    pub(crate) fn connection_lost(
        &mut self,
        connection: u64,
        waiting: &[CallKey],
        failure: Option<String>,
    ) {
        let sent: Vec<CallKey> = self
            .calls
            .iter()
            .filter(
                |(_, entry)| matches!(entry.state, State::Sent { key, .. } if key == connection),
            )
            .map(|(key, _)| key.clone())
            .collect();
        for key in sent {
            self.end(&key, exited());
        }
        if let Some(reason) = failure {
            for key in waiting {
                if self
                    .calls
                    .get(key)
                    .is_some_and(|entry| matches!(entry.state, State::Opening))
                {
                    let message = format!("could not open a connection to server {CUA}: {reason}");
                    self.end(
                        key,
                        Outcome::Rejected(RejectionCode::McpServerUnavailable, message),
                    );
                }
            }
        }
    }

    /// Ends the call with `outcome`: its final report, unless the outcome is
    /// silent, and its withdrawal when rejected.
    fn end(&mut self, key: &CallKey, outcome: Outcome) {
        let Some(entry) = self.calls.get_mut(key) else {
            return;
        };
        let written = match entry.state {
            State::Sent { tag, written, .. } => {
                self.tagged.remove(&tag);
                if let Some(at) = written {
                    self.deadlines.remove(&(at + self.config.limits.call, tag));
                }
                written
            }
            State::Ended { .. } => return,
            State::Staged | State::Opening => None,
        };
        let now = state::seams::now();
        self.config.activity.mcp_call_ended(now);
        let metrics = entry.metrics(written, now);
        // A tool report carries its encoded bytes; a rejection carries none.
        let (body, report_bytes, how) = match outcome {
            Outcome::Silent => {
                self.calls.remove(key);
                return;
            }
            Outcome::Rejected(code, message) => {
                if let Some(withdrawn) = entry.withdrawn.take() {
                    withdrawn();
                }
                (
                    Body::Rejected(ProtocolError { code, message }),
                    None,
                    Ended::Rejected,
                )
            }
            Outcome::Error(error) => {
                let body = error_body(Outcome::Error(error), metrics.clone());
                let (body, bytes) = measured(&entry.correlation, &entry.connection_id, body);
                (body, Some(bytes), Ended::Finished)
            }
            Outcome::Result(result) => {
                let (body, bytes) = success(
                    &entry.correlation,
                    &entry.connection_id,
                    *result,
                    metrics.clone(),
                );
                (body, Some(bytes), Ended::Finished)
            }
        };
        let joined = std::mem::take(&mut entry.joined);
        let submission = |body| {
            let mut submission = Report {
                correlation: entry.correlation.clone(),
                body,
            }
            .submission(entry.connection_id.clone());
            submission.final_for.extend(joined.iter().cloned());
            submission.traceparent = entry.traceparent.clone();
            submission
        };
        let report = match report_bytes {
            None => entry.reports.submit(submission(body)),
            // A tool report takes only ordinary capacity: one that does not
            // fit gives way to a small error that the reserve admits, so a
            // full budget ends this call, never the connection.
            Some(report_bytes) => entry
                .reports
                .try_submit(submission(body))
                .unwrap_or_else(|_| {
                    let message = format!(
                        "result does not fit the App Server's report budget: \
                         {report_bytes} report bytes"
                    );
                    let error = Outcome::Error(tool_error("resource_exhausted", &message, None));
                    entry.reports.submit(submission(error_body(error, metrics)))
                }),
        };
        entry.state = State::Ended { report, how };
    }
}

impl Entry {
    fn metrics(&self, written: Option<Duration>, now: Duration) -> Metrics {
        let millis = |duration: Duration| {
            Count::saturating(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        };
        let left_queue = self.left_queue.unwrap_or(now);
        Metrics {
            wall_time_ms: millis(now.saturating_sub(self.received)),
            queued_ms: millis(left_queue.saturating_sub(self.received)),
            drive_wait_ms: millis(Duration::ZERO),
            run_ms: millis(written.map_or(Duration::ZERO, |at| now.saturating_sub(at))),
            sync_ms: millis(Duration::ZERO),
        }
    }
}

/// `body` and the encoded bytes of its report on `connection_id`.
fn measured(correlation: &Correlation, connection_id: &Id, body: Body) -> (Body, usize) {
    let report = Report {
        correlation: correlation.clone(),
        body,
    };
    let bytes = report.encoded_len(connection_id);
    (report.body, bytes)
}

/// The body of a `success` report of the call `correlation` names on
/// `connection_id`, or of the error its media or sizes give, with the
/// encoded bytes of its report.
fn success(
    correlation: &Correlation,
    connection_id: &Id,
    mut result: McpResult,
    metrics: Metrics,
) -> (Body, usize) {
    let media = match crate::media::check(&mut result.content) {
        Ok(media) => media,
        Err(index) => {
            let body = error_body(
                invalid(&format!(
                    "content[{index}] holds media that is not valid base64"
                )),
                metrics,
            );
            return measured(correlation, connection_id, body);
        }
    };
    let envelope = envelope(
        HelperStatus::Success,
        None,
        ToolResult::Mcp(result),
        metrics.clone(),
    );
    let envelope_bytes = protocol::encode(&envelope).len() - media.encoded;
    let report = Report {
        correlation: correlation.clone(),
        body: terminal(envelope),
    };
    let report_bytes = report.encoded_len(connection_id);
    let media_bytes = media.decoded;
    if media_bytes <= limits::MEDIA_BYTES as u64
        && envelope_bytes <= limits::ENVELOPE_BYTES
        && report_bytes <= limits::RESULT_REPORT_BYTES
    {
        return (report.body, report_bytes);
    }
    let message = format!(
        "result over a size limit: {media_bytes} media bytes (limit {}), {envelope_bytes} \
         envelope bytes (limit {}), {report_bytes} report bytes (limit {})",
        limits::MEDIA_BYTES,
        limits::ENVELOPE_BYTES,
        limits::RESULT_REPORT_BYTES
    );
    let detail = json!({
        "media_bytes": media_bytes,
        "envelope_bytes": envelope_bytes,
        "report_bytes": report_bytes,
    });
    let body = error_body(
        Outcome::Error(tool_error("mcp_result_too_large", &message, Some(detail))),
        metrics,
    );
    measured(correlation, connection_id, body)
}

fn error_body(outcome: Outcome, metrics: Metrics) -> Body {
    let Outcome::Error(error) = outcome else {
        unreachable!("an error outcome");
    };
    terminal(envelope(
        HelperStatus::Error,
        Some(error),
        ToolResult::Object(Object::new()),
        metrics,
    ))
}

fn envelope(
    status: HelperStatus,
    error: Option<ToolError>,
    result: ToolResult,
    metrics: Metrics,
) -> HelperEnvelope {
    HelperEnvelope {
        tool: HelperTool::Mcp,
        status,
        truncated: false,
        error,
        result,
        metrics: Some(metrics),
        drive_sync: None,
    }
}

fn terminal(envelope: HelperEnvelope) -> Body {
    Body::Result {
        phase: ReportPhase::Terminal,
        report_seq: PositiveInt::new(1).expect("1 is positive"),
        result: Box::new(envelope),
    }
}

fn tool_error(kind: &str, message: &str, detail: Option<Value>) -> ToolError {
    ToolError {
        kind: NonEmptyString::new(kind).expect("an error kind is not empty"),
        message: message.to_owned(),
        path: None,
        detail: detail.and_then(|detail| match detail {
            Value::Object(detail) => Some(detail),
            _ => None,
        }),
    }
}

fn invalid(message: &str) -> Outcome {
    Outcome::Error(tool_error("mcp_invalid_result", message, None))
}

fn cancelled(sent: bool) -> Outcome {
    let message = if sent {
        "cancelled after the call was sent"
    } else {
        "cancelled before the call was sent"
    };
    Outcome::Error(tool_error(
        "cancelled",
        message,
        Some(json!({ "sent": sent })),
    ))
}

fn exited() -> Outcome {
    Outcome::Error(tool_error(
        "mcp_server_exited",
        "the MCP connection ended before the answer",
        None,
    ))
}

/// The scalar values an `mcp_error` keeps of its message when no smaller
/// `detail` fits.
const FALLBACK_SCALARS: usize = 64;

/// A JSON-RPC error answer: `mcp_error` with `detail` `{code, message,
/// data?}`, bounded as every error is; when no rebuilt `detail` fits, its
/// `detail` keeps `code` and the message's first scalar values.
fn mcp_error(error: Value) -> Outcome {
    let code = error.get("code").and_then(Value::as_i64);
    let message = error.get("message").and_then(Value::as_str);
    let (Some(code), Some(message)) = (code, message) else {
        return invalid(
            "result is not a valid CallToolResult: the error answer has no integer code and \
             string message",
        );
    };
    let mut detail = json!({ "code": code, "message": message });
    if let Some(data) = error.get("data") {
        detail["data"] = data.clone();
    }
    let mut bounded = bounded_error(
        tool_error("mcp_error", message, Some(detail)),
        ErrorBounds::default(),
    );
    if bounded
        .detail
        .as_ref()
        .is_some_and(|detail| !detail.contains_key("code"))
    {
        let first: String = message.chars().take(FALLBACK_SCALARS).collect();
        bounded.detail = match json!({ "code": code, "message": first, "truncated": true }) {
            Value::Object(detail) => Some(detail),
            _ => None,
        };
    }
    Outcome::Error(bounded)
}

/// The answer's `result` as an MCP `2025-06-18` `CallToolResult`, without
/// its top-level `_meta` and with its other members as sent; or its first
/// violation.
fn call_tool_result(result: Value) -> Result<McpResult, String> {
    let Value::Object(mut result) = result else {
        return Err("the result is not an object".to_owned());
    };
    let Some(Value::Array(content)) = result.remove("content") else {
        return Err("content is not an array".to_owned());
    };
    let mut blocks = Vec::with_capacity(content.len());
    for (index, block) in content.into_iter().enumerate() {
        match block {
            Value::Object(block) if valid_block(&block) => blocks.push(block),
            _ => return Err(format!("content[{index}] is not a valid content block")),
        }
    }
    let structured_content = match result.remove("structuredContent") {
        None => None,
        Some(Value::Object(structured)) => Some(structured),
        Some(_) => return Err("structuredContent is not an object".to_owned()),
    };
    let is_error = match result.remove("isError") {
        None => None,
        Some(Value::Bool(is_error)) => Some(is_error),
        Some(_) => return Err("isError is not a boolean".to_owned()),
    };
    result.remove("_meta");
    Ok(McpResult {
        content: blocks,
        structured_content,
        is_error,
        meta: Default::default(),
        other: result,
    })
}

/// A content block of one of the five types with its members of that type.
fn valid_block(block: &Object) -> bool {
    let string = |object: &Object, name: &str| object.get(name).is_some_and(Value::is_string);
    match block.get("type").and_then(Value::as_str) {
        Some("text") => string(block, "text"),
        Some("image" | "audio") => string(block, "data") && string(block, "mimeType"),
        Some("resource_link") => string(block, "uri") && string(block, "name"),
        Some("resource") => block
            .get("resource")
            .and_then(Value::as_object)
            .is_some_and(|resource| {
                string(resource, "uri") && (string(resource, "text") != string(resource, "blob"))
            }),
        _ => false,
    }
}

/// The reply to a `tool.cancel`.
fn cancel_reply(cancel: &Cancel, cancelled: bool, was_running: bool) -> Body {
    let empty = || StreamSnapshot {
        text: String::new(),
        total_bytes: Count::saturating(0),
        total_lines: Count::saturating(0),
        returned_bytes: Count::saturating(0),
        truncated: false,
    };
    Body::Control {
        result: Some(Box::new(HelperEnvelope {
            tool: HelperTool::Cancel,
            status: HelperStatus::Success,
            truncated: false,
            error: None,
            result: ToolResult::Exec(ExecResult {
                task_id: Nullable(Some(cancel.correlation.task_id.clone())),
                exit_code: Nullable(None),
                signal: Nullable(None),
                timed_out: false,
                cancelled: Some(cancelled),
                was_running: Some(was_running),
                written_bytes: None,
                stdout: empty(),
                stderr: empty(),
                duration_ms: Count::saturating(0),
            }),
            metrics: None,
            drive_sync: None,
        })),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use protocol::tool::ToolStatus;

    use super::*;

    #[tokio::test]
    async fn shutdown_ends_a_held_call_once_even_while_its_terminal_entry_is_retained() {
        use protocol::ports::{ActivitySink, Submission};
        use std::sync::Mutex;
        #[derive(Debug, Default)]
        struct Recorded(Mutex<Vec<&'static str>>);
        impl ActivitySink for Recorded {
            fn tool_instruction_arrived(&self, _: Duration) {}
            fn exec_started(&self) {}
            fn exec_exited(&self, _: Duration) {}
            fn preparation_started(&self, _: Duration) {}
            fn preparation_ended(&self, _: Duration) {}
            fn mcp_call_started(&self) {
                self.0.lock().unwrap().push("start");
            }
            fn mcp_call_ended(&self, _: Duration) {
                self.0.lock().unwrap().push("end");
            }
        }
        struct Reports;
        impl ReportSink for Reports {
            // Never ACK: the ended entry remains in the manager.
            fn submit(&self, _: Submission) -> ReportId {
                ReportId(1)
            }
            fn cancel(&self, _: ReportId) {}
            fn add_final(&self, _: ReportId, _: Id) {}
        }
        let root = tempfile::tempdir().unwrap();
        let activity = Arc::new(Recorded::default());
        let manager = crate::spawn(crate::ManagerConfig {
            root: state::paths::StateRoot::new(root.path().as_os_str()).unwrap(),
            cli_version: "0.1.0".to_owned(),
            limits: limits::Limits::default(),
            activity: activity.clone(),
        });
        let payload: McpCallPayload = serde_json::from_value(json!({
            "session_id":"sess_1", "binding_id":"bind_1", "task_id":"task_1", "tool_use_event_id":"use_1",
            "server":"cua", "tool":"click", "arguments":{}
        })).unwrap();
        manager.call(Call {
            connection_id: Id::new("conn_1").unwrap(),
            correlation: Correlation {
                session_id: payload.session_id.clone(),
                binding_id: payload.binding_id.clone(),
                task_id: payload.task_id.clone(),
                tool_use_event_id: payload.tool_use_event_id.clone(),
                request_message_id: Id::new("message_1").unwrap(),
            },
            payload,
            cwd: root.path().to_owned(),
            recorded: true,
            traceparent: None,
            received: Duration::ZERO,
            staged: true,
            reports: Arc::new(Reports),
            withdrawn: Box::new(|| {}),
        });
        manager.shutdown().await;
        manager.shutdown().await;
        assert_eq!(*activity.0.lock().unwrap(), vec!["start", "end"]);
    }

    fn protocol_file(name: &str) -> Value {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../protocol")
            .join(name);
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    /// `ToolStatus` of the tool-execution schema, whose references stay
    /// inside its file.
    fn tool_status_schema() -> jsonschema::Validator {
        let mut schema = protocol_file("tool-execution-v1.schema.json");
        let root = schema.as_object_mut().unwrap();
        root.remove("anyOf");
        root.insert("$ref".to_owned(), json!("#/$defs/ToolStatus"));
        jsonschema::validator_for(&schema).unwrap()
    }

    /// The value of the tool-execution fixture `name`.
    fn fixture(name: &str) -> Value {
        let fixtures = protocol_file("tool-execution-v1.fixtures.json");
        fixtures
            .as_array()
            .unwrap()
            .iter()
            .find(|fixture| fixture["name"] == name)
            .unwrap_or_else(|| panic!("no fixture {name}"))["value"]
            .clone()
    }

    fn id(value: &Value) -> Id {
        Id::new(value.as_str().unwrap()).unwrap()
    }

    fn correlation_of(report: &Value) -> Correlation {
        Correlation {
            session_id: id(&report["session_id"]),
            binding_id: id(&report["binding_id"]),
            tool_use_event_id: id(&report["tool_use_event_id"]),
            task_id: id(&report["task_id"]),
            request_message_id: id(&report["request_message_id"]),
        }
    }

    fn metrics() -> Metrics {
        serde_json::from_value(json!({
            "wall_time_ms": 1250, "queued_ms": 3, "drive_wait_ms": 40, "run_ms": 1180, "sync_ms": 20,
        }))
        .unwrap()
    }

    /// The report with `body` as the App Server sends it, with the
    /// identities of `like`.
    fn sent(like: &Value, body: Body) -> Value {
        let status = Report {
            correlation: correlation_of(like),
            body,
        }
        .status(id(&like["connection_id"]), id(&like["message_id"]));
        serde_json::from_slice(&protocol::encode(&status)).unwrap()
    }

    fn result_of(answer: Value) -> McpResult {
        call_tool_result(answer).unwrap()
    }

    #[test]
    fn reports_built_from_the_contracts_fixture_inputs_equal_those_fixtures() {
        let terminal = fixture("mcp_terminal");
        let reply = fixture("mcp_cancel_reply");
        let rows = [
            (
                "mcp_terminal",
                success(
                    &correlation_of(&terminal),
                    &id(&terminal["connection_id"]),
                    result_of(terminal["result"]["result"].clone()),
                    metrics(),
                )
                .0,
            ),
            (
                "mcp_cancelled_before_sent",
                error_body(cancelled(false), metrics()),
            ),
            (
                "mcp_cancel_reply",
                cancel_reply(
                    &Cancel {
                        correlation: correlation_of(&reply),
                        traceparent: None,
                    },
                    true,
                    true,
                ),
            ),
            (
                "mcp_rejected_server_unavailable",
                Body::Rejected(ProtocolError {
                    code: RejectionCode::McpServerUnavailable,
                    message: format!("server {CUA} is not running"),
                }),
            ),
        ];
        for (name, body) in rows {
            let expected = fixture(name);
            assert_eq!(sent(&expected, body), expected, "{name}");
        }
    }

    #[test]
    fn every_report_the_result_builder_makes_is_a_valid_tool_status() {
        let schema = tool_status_schema();
        let validate = |report: &Value| -> Result<(), String> {
            let errors: Vec<String> = schema
                .iter_errors(report)
                .map(|error| format!("{error} at {}", error.instance_path()))
                .collect();
            if !errors.is_empty() {
                return Err(format!("{errors:?}"));
            }
            let bytes = serde_json::to_vec(report).unwrap();
            protocol::decode::<ToolStatus>(&bytes)
                .map(drop)
                .map_err(|error| error.to_string())
        };
        let like = fixture("mcp_terminal");
        let connection = id(&like["connection_id"]);
        let built = |answer: Value| {
            success(
                &correlation_of(&like),
                &connection,
                result_of(answer),
                metrics(),
            )
            .0
        };
        let image = |data: &str| json!({"type": "image", "data": data, "mimeType": "image/png"});
        let error = |outcome: Outcome| error_body(outcome, metrics());
        // The densest result within the envelope bound: zeros to its end.
        let zeros =
            |count: usize| json!({"content": [], "structuredContent": {"z": vec![0; count]}});
        let envelope_len = |count: usize| {
            let result = ToolResult::Mcp(result_of(zeros(count)));
            protocol::encode(&envelope(HelperStatus::Success, None, result, metrics())).len()
        };
        let densest = 1 + (limits::ENVELOPE_BYTES - envelope_len(1)) / 2;
        // So many `data` members that no bounded detail keeps `code`.
        let members: Object = (0..5_000).map(|n| (format!("k{n}"), json!(n))).collect();
        let crowded =
            mcp_error(json!({"code": -32000, "message": "m".repeat(100), "data": members}));
        let Outcome::Error(fallback) = &crowded else {
            panic!("a JSON-RPC error is an mcp_error");
        };
        assert_eq!(
            fallback.detail,
            json!({"code": -32000, "message": "m".repeat(64), "truncated": true})
                .as_object()
                .cloned()
        );
        // Each row is named by what its report says: its error kind, its
        // status, or its phase or kind.
        let rows = [
            (
                "success",
                built(json!({
                    "content": [image("QUJD"), {"type": "text", "text": "a", "extra": 1}],
                    "isError": true, "_meta": {"k": 1},
                })),
            ),
            (
                "mcp_error",
                error(mcp_error(
                    json!({"code": -32602, "message": "unknown tool"}),
                )),
            ),
            (
                "mcp_error",
                error(mcp_error(json!({
                    "code": -32000, "message": "x".repeat(10_000), "data": "y".repeat(102_400),
                }))),
            ),
            ("mcp_error", error(crowded)),
            (
                "mcp_timeout",
                error(Outcome::Error(tool_error(
                    "mcp_timeout",
                    "no answer within 300 seconds",
                    None,
                ))),
            ),
            ("mcp_server_exited", error(exited())),
            ("cancelled", error(cancelled(false))),
            ("cancelled", error(cancelled(true))),
            ("success", built(zeros(densest))),
            (
                "mcp_result_too_large",
                built(json!({"content": [{"type": "text", "text": "x".repeat(262_144)}]})),
            ),
            (
                "mcp_invalid_result",
                built(json!({"content": [image("QQ")]})),
            ),
            (
                "resource_exhausted",
                error(Outcome::Error(tool_error(
                    "resource_exhausted",
                    "result does not fit the App Server's report budget: 14000000 report bytes",
                    None,
                ))),
            ),
            (
                "rejected",
                Body::Rejected(ProtocolError {
                    code: RejectionCode::McpServerUnavailable,
                    message: format!("could not open a connection to server {CUA}: gone"),
                }),
            ),
            (
                "control",
                cancel_reply(
                    &Cancel {
                        correlation: correlation_of(&like),
                        traceparent: None,
                    },
                    false,
                    false,
                ),
            ),
        ];
        for (name, body) in rows {
            let report = sent(&like, body);
            let said = match (&report["phase"], &report["result"]["error"]["kind"]) {
                (Value::String(phase), _) if phase == "rejected" => "rejected",
                (_, Value::String(kind)) => kind.as_str(),
                (Value::Null, _) => "control",
                _ => report["result"]["status"].as_str().unwrap(),
            };
            assert_eq!(said, name, "{report:#}");
            if let Err(error) = validate(&report) {
                panic!("{name}: {error}");
            }
        }
        // The same validation refuses each of the contract's invalid MCP reports.
        for name in [
            "mcp_result_truncated",
            "mcp_result_with_meta",
            "mcp_cancelled_without_sent",
        ] {
            assert!(validate(&fixture(name)).is_err(), "{name}");
        }
    }

    #[test]
    fn the_contracts_invalid_mcp_call_fixtures_fail_the_arguments_and_schema_check() {
        for (name, message) in [
            ("mcp_call_with_sandbox_policy", None),
            ("mcp_call_arguments_not_object", Some(ARGUMENTS_INVALID)),
        ] {
            let data = serde_json::to_string(&fixture(name)).unwrap();
            let refused = check(&data).expect_err(name);
            match message {
                Some(message) => assert_eq!(refused, message, "{name}"),
                None => assert!(
                    refused.starts_with("invalid mcp.call: "),
                    "{name}: {refused}"
                ),
            }
        }
    }
}
