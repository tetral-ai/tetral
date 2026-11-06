//! The fitting of a report on a process result into the protocol's byte
//! limits before it is handed to the Report outbox, so that no report is
//! saved that Engine could never accept. The reports themselves and the
//! bounds of their errors are `protocol::report`'s.

use protocol::id::Id;
use protocol::report::Report;
use protocol::tool::{ExecResult, StreamSnapshot, ToolResult, limits};

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
