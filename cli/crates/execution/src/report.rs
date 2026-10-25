//! Tool-status reports: their identities and bodies, the bounds of a result
//! envelope's error, and the fitting of a report into the protocol's byte
//! limits before it is handed to the Report outbox, so that no report is
//! saved that Engine could never accept.

use protocol::field::PositiveInt;
use protocol::id::Id;
use protocol::ports::{LONGEST_REPORT_MESSAGE_ID, Submission};
use protocol::tool::{
    ControlErrorCode, ControlReport, ExecResult, ExecutionReport, HelperEnvelope, Object,
    ProtocolError, RejectionCode, ReportHeader, ReportPhase, StreamSnapshot, ToolError, ToolResult,
    ToolStatus, limits,
};
use serde_json::Value;

/// The most bytes of an error's `message`.
pub const ERROR_MESSAGE_MAX_BYTES: usize = 4_096;

/// The most bytes of the compact JSON of an error's `detail`.
pub const ERROR_DETAIL_MAX_BYTES: usize = 32_768;

/// What stands between the head and the tail of a cut text.
const SEAM: &str = "\n[... truncated ...]\n";

/// Below this many Unicode scalar values a cut string keeps a plain prefix.
const SEAM_MIN_SCALARS: usize = 32;

/// The steps that rebuild an oversized `detail`: each string cut to the first
/// number of Unicode scalar values, each array to the second number of items.
const DETAIL_STEPS: [(usize, usize); 4] = [(4_096, 50), (1_024, 20), (256, 10), (64, 3)];

/// The bounds of an error, each a constant but for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorBounds {
    pub message_max_bytes: usize,
    pub detail_max_bytes: usize,
}

impl Default for ErrorBounds {
    fn default() -> ErrorBounds {
        ErrorBounds {
            message_max_bytes: ERROR_MESSAGE_MAX_BYTES,
            detail_max_bytes: ERROR_DETAIL_MAX_BYTES,
        }
    }
}

/// The identities of a report other than its connection's and its own
/// `message_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Correlation {
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    /// The instruction the report answers.
    pub request_message_id: Id,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// A `running` or `terminal` execution result.
    Result {
        phase: ReportPhase,
        report_seq: PositiveInt,
        result: Box<HelperEnvelope>,
    },
    /// The instruction was not admitted.
    Rejected(ProtocolError<RejectionCode>),
    /// A control's reply: its result, its error, or both.
    Control {
        result: Option<Box<HelperEnvelope>>,
        error: Option<ProtocolError<ControlErrorCode>>,
    },
}

/// A tool-status report before the outbox gives it its connection's and its
/// own `message_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub correlation: Correlation,
    pub body: Body,
}

impl Report {
    pub fn rejected(correlation: Correlation, code: RejectionCode, message: &str) -> Report {
        Report {
            correlation,
            body: Body::Rejected(ProtocolError {
                code,
                message: message.to_owned(),
            }),
        }
    }

    /// Whether it is the last report of its instruction: a `terminal` or
    /// `rejected` report, or a control's reply.
    pub fn is_final(&self) -> bool {
        !matches!(
            self.body,
            Body::Result {
                phase: ReportPhase::Running,
                ..
            }
        )
    }

    pub fn status(&self, connection_id: Id, message_id: Id) -> ToolStatus {
        let correlation = self.correlation.clone();
        let header = ReportHeader {
            protocol_version: Default::default(),
            connection_id,
            message_id,
            session_id: correlation.session_id,
            binding_id: correlation.binding_id,
            tool_use_event_id: correlation.tool_use_event_id,
            task_id: correlation.task_id,
            request_message_id: correlation.request_message_id,
        };
        match self.body.clone() {
            Body::Result {
                phase,
                report_seq,
                result,
            } => ToolStatus::Execution(ExecutionReport::Result {
                header,
                phase,
                report_seq,
                result,
            }),
            Body::Rejected(error) => {
                ToolStatus::Execution(ExecutionReport::Rejected { header, error })
            }
            Body::Control { result, error } => ToolStatus::Control(ControlReport {
                header,
                result,
                error,
            }),
        }
    }

    /// The bytes of its compact JSON on `connection_id`, with the longest
    /// `message_id` the outbox gives.
    pub fn encoded_len(&self, connection_id: &Id) -> usize {
        let longest = Id::new(LONGEST_REPORT_MESSAGE_ID).expect("an identity");
        protocol::encode(&self.status(connection_id.clone(), longest)).len()
    }

    /// The submission of this report on `connection_id`; a final report
    /// names the instruction it ends.
    pub fn submission(self, connection_id: Id) -> Submission {
        let final_for = self
            .is_final()
            .then(|| self.correlation.request_message_id.clone());
        let mut submission = Submission::new(connection_id, move |connection, message_id| {
            self.status(connection, message_id)
        });
        submission.final_for = final_for;
        submission
    }

    fn envelope_mut(&mut self) -> Option<&mut HelperEnvelope> {
        match &mut self.body {
            Body::Result { result, .. } => Some(result),
            Body::Control { result, .. } => result.as_deref_mut(),
            Body::Rejected(_) => None,
        }
    }
}

/// A result envelope with its error bounded and `error` present exactly
/// when `status` is `error`, as the protocol type requires.
pub fn bounded(mut envelope: HelperEnvelope, bounds: ErrorBounds) -> HelperEnvelope {
    envelope.error = envelope.error.map(|error| bounded_error(error, bounds));
    envelope
}

/// An error with its `message` cut to `message_max_bytes` on UTF-8
/// boundaries, keeping its head and tail around the seam, and its `detail`
/// rebuilt smaller while its compact JSON exceeds `detail_max_bytes`.
pub fn bounded_error(mut error: ToolError, bounds: ErrorBounds) -> ToolError {
    error.message = cut_bytes(&error.message, bounds.message_max_bytes);
    error.detail = error
        .detail
        .map(|detail| bounded_detail(detail, bounds.detail_max_bytes));
    error
}

fn bounded_detail(detail: Object, max_bytes: usize) -> Object {
    if detail.is_empty() || json_len(&detail) <= max_bytes {
        return detail;
    }
    let value = Value::Object(detail);
    for (scalars, items) in DETAIL_STEPS {
        if let Value::Object(mut fitted) = fit_value(&value, scalars, items) {
            fitted.insert("truncated".to_owned(), Value::Bool(true));
            if json_len(&fitted) <= max_bytes {
                return fitted;
            }
        }
    }
    let mut marker = Object::new();
    marker.insert("truncated".to_owned(), Value::Bool(true));
    marker
}

fn fit_value(value: &Value, scalars: usize, items: usize) -> Value {
    match value {
        Value::String(text) => Value::String(cut_scalars(text, scalars)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .take(items)
                .map(|value| fit_value(value, scalars, items))
                .collect(),
        ),
        Value::Object(members) => Value::Object(
            members
                .iter()
                .map(|(name, value)| (name.clone(), fit_value(value, scalars, items)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `text` cut to at most `max` Unicode scalar values: its head and its tail
/// around the seam, or a plain prefix below [`SEAM_MIN_SCALARS`].
fn cut_scalars(text: &str, max: usize) -> String {
    let count = text.chars().count();
    if count <= max {
        return text.to_owned();
    }
    if max < SEAM_MIN_SCALARS {
        return text.chars().take(max).collect();
    }
    let seam = SEAM.chars().count();
    let head = ((max - seam) / 2).max(1);
    let tail = (max - seam - head).max(1);
    let mut cut: String = text.chars().take(head).collect();
    cut.push_str(SEAM);
    cut.extend(text.chars().skip(count - tail));
    cut
}

/// `text` cut to at most `max` bytes on UTF-8 boundaries: its head and its
/// tail around the seam, or a plain prefix when the seam does not fit.
fn cut_bytes(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    if max <= SEAM.len() {
        return text[..floor_boundary(text, max)].to_owned();
    }
    let remaining = max - SEAM.len();
    let head_end = floor_boundary(text, remaining / 2);
    let tail_start = ceil_boundary(text, text.len() - (remaining - head_end));
    format!("{}{SEAM}{}", &text[..head_end], &text[tail_start..])
}

fn floor_boundary(text: &str, mut index: usize) -> usize {
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_boundary(text: &str, mut index: usize) -> usize {
    while !text.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn json_len(value: &impl serde::Serialize) -> usize {
    protocol::encode(value).len()
}

/// The stored output of one stream of a process result, rendered for a
/// visible byte and line budget.
pub trait StreamOutput {
    fn snapshot(&self, visible_bytes: usize, visible_lines: usize) -> StreamSnapshot;
}

/// The streams a process result was rendered from, and its budgets.
pub struct Rendering<'a> {
    pub stdout: &'a dyn StreamOutput,
    pub stderr: &'a dyn StreamOutput,
    pub visible_bytes: usize,
    pub visible_lines: usize,
}

/// Fits a report on a process result (`exec_command`, `stdin`, `cancel`)
/// within the ordinary limits before it is saved: while its result envelope
/// exceeds `HELPER_RESULT_JSON_BYTES` or the whole report
/// `ORDINARY_TOOL_STATUS_JSON_BYTES`, it is marked `truncated` and both
/// streams are rendered again from `rendering` with half the previous byte
/// budget and the same line budget, down to a budget of 0, which leaves only
/// the truncation marker. Nothing else of the report changes. Any other
/// report is left as it is: its error is bounded ([`bounded`]) and a
/// `view_image` result is never trimmed.
pub fn fit(report: &mut Report, connection_id: &Id, rendering: &Rendering<'_>) {
    let mut visible_bytes = rendering.visible_bytes;
    loop {
        let report_len = report.encoded_len(connection_id);
        let Some(envelope) = report.envelope_mut() else {
            return;
        };
        if json_len(envelope) <= limits::HELPER_RESULT_JSON_BYTES
            && report_len <= limits::ORDINARY_TOOL_STATUS_JSON_BYTES
        {
            return;
        }
        let ToolResult::Exec(result) = &mut envelope.result else {
            return;
        };
        if visible_bytes == 0 {
            return;
        }
        visible_bytes /= 2;
        render(result, rendering, visible_bytes);
        envelope.truncated = true;
    }
}

fn render(result: &mut ExecResult, rendering: &Rendering<'_>, visible_bytes: usize) {
    result.stdout = rendering
        .stdout
        .snapshot(visible_bytes, rendering.visible_lines);
    result.stderr = rendering
        .stderr
        .snapshot(visible_bytes, rendering.visible_lines);
}
