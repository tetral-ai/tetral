//! Tool-status reports before the Report outbox names them: their
//! identities and bodies, and the bounds of a result envelope's error, so
//! that no error is saved that Engine could never accept. Every crate that
//! answers a tool instruction builds its reports with these.

use serde_json::Value;

use crate::field::PositiveInt;
use crate::id::Id;
use crate::ports::{LONGEST_REPORT_MESSAGE_ID, Submission};
use crate::tool::{
    ControlErrorCode, ControlReport, ExecutionReport, HelperEnvelope, Object, ProtocolError,
    RejectionCode, ReportHeader, ReportPhase, ToolError, ToolStatus,
};
/// The message of the control error `task_not_found`, which also answers
/// every control of a task whose start was rejected.
pub const TASK_NOT_FOUND: &str = "task_id names no task of the named binding on this connection";

/// The most bytes of an error's `message`.
pub const ERROR_MESSAGE_MAX_BYTES: usize = 4_096;

/// The most bytes of an error's `path`. Linux and macOS refuse a longer
/// path, so the cut never shortens the path of a file that exists.
pub const ERROR_PATH_MAX_BYTES: usize = 4_096;

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
    pub path_max_bytes: usize,
    pub detail_max_bytes: usize,
}

impl Default for ErrorBounds {
    fn default() -> ErrorBounds {
        ErrorBounds {
            message_max_bytes: ERROR_MESSAGE_MAX_BYTES,
            path_max_bytes: ERROR_PATH_MAX_BYTES,
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
        crate::encode(&self.status(connection_id.clone(), longest)).len()
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
        submission.final_for = final_for.into_iter().collect();
        submission
    }

    /// Its result envelope: a result's, or a control reply's when it has one.
    pub fn envelope_mut(&mut self) -> Option<&mut HelperEnvelope> {
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

/// An error with its `message` cut to `message_max_bytes` and its `path` to
/// `path_max_bytes` on UTF-8 boundaries, each keeping its head and tail
/// around the seam, and its `detail` rebuilt smaller while its compact JSON
/// exceeds `detail_max_bytes`.
pub fn bounded_error(mut error: ToolError, bounds: ErrorBounds) -> ToolError {
    error.message = cut_bytes(&error.message, bounds.message_max_bytes);
    error.path = error
        .path
        .map(|path| cut_bytes(&path, bounds.path_max_bytes));
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
    crate::encode(value).len()
}
