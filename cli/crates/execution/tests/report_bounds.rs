//! The bounds of a report before it is saved: a process result over the
//! ordinary limits rendered smaller until it fits, and the bounds of an
//! error's `message` and `detail`; each report decodes with the protocol's
//! types.

use execution::report::{
    Body, Correlation, ErrorBounds, Rendering, Report, StreamOutput, bounded, fit,
};
use protocol::field::{Count, NonEmptyString, Nullable, PositiveInt};
use protocol::id::Id;
use protocol::tool::{
    ExecResult, HelperEnvelope, HelperStatus, HelperTool, Metrics, Object, ReportPhase,
    StreamSnapshot, ToolError, ToolResult, ToolStatus, limits,
};
use serde_json::{Value, json};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn correlation() -> Correlation {
    Correlation {
        session_id: id("sess_1"),
        binding_id: id("bind_1"),
        tool_use_event_id: id("call_1"),
        task_id: id("task_1"),
        request_message_id: id("execute_1"),
    }
}

fn count(value: usize) -> Count {
    Count::new(value as i64).unwrap()
}

fn metrics() -> Metrics {
    Metrics {
        wall_time_ms: count(5),
        queued_ms: count(1),
        drive_wait_ms: count(0),
        run_ms: count(4),
        sync_ms: count(0),
    }
}

fn terminal(result: HelperEnvelope) -> Report {
    Report {
        correlation: correlation(),
        body: Body::Result {
            phase: ReportPhase::Terminal,
            report_seq: PositiveInt::new(1).unwrap(),
            result: Box::new(result),
        },
    }
}

/// A report as the outbox saves it, decoded by the protocol's types.
fn saved(report: &Report) -> (Vec<u8>, ToolStatus) {
    let bytes = protocol::encode(&report.status(id("conn_1"), id("r7")));
    let decoded = protocol::decode::<ToolStatus>(&bytes).expect("the report decodes");
    (bytes, decoded)
}

/// A stream's whole output, rendered as its first bytes within the budget
/// and a marker for the rest; its counters are the whole output's.
struct Stored(String);

impl StreamOutput for Stored {
    fn snapshot(&self, visible_bytes: usize, _visible_lines: usize) -> StreamSnapshot {
        let kept = self.0.len().min(visible_bytes);
        let omitted = self.0.len() - kept;
        let mut text = self.0[..kept].to_owned();
        if omitted > 0 {
            text.push_str(&format!("\n[... {omitted} bytes truncated ...]\n"));
        }
        StreamSnapshot {
            text,
            total_bytes: count(self.0.len()),
            total_lines: count(self.0.matches('\n').count()),
            returned_bytes: count(kept),
            truncated: omitted > 0,
        }
    }
}

#[test]
fn a_process_result_over_the_ordinary_limits_is_rendered_smaller_until_it_fits() {
    let output = Stored("\u{1}".repeat(51_200));
    let rendering = Rendering {
        stdout: &output,
        stderr: &output,
        visible_bytes: 51_200,
        visible_lines: 2_000,
    };
    let result = ExecResult {
        task_id: Nullable(Some(id("task_1"))),
        exit_code: Nullable(Some(0)),
        signal: Nullable(None),
        timed_out: false,
        cancelled: None,
        was_running: None,
        written_bytes: None,
        stdout: output.snapshot(51_200, 2_000),
        stderr: output.snapshot(51_200, 2_000),
        duration_ms: count(4),
    };
    let mut report = terminal(HelperEnvelope {
        tool: HelperTool::ExecCommand,
        status: HelperStatus::Success,
        truncated: false,
        error: None,
        result: ToolResult::Exec(result),
        metrics: Some(metrics()),
        drive_sync: None,
    });
    let before = saved(&report).1;
    assert!(saved(&report).0.len() > limits::ORDINARY_TOOL_STATUS_JSON_BYTES);

    fit(&mut report, &id("conn_1"), &rendering);

    let (bytes, decoded) = saved(&report);
    assert!(bytes.len() <= limits::ORDINARY_TOOL_STATUS_JSON_BYTES);
    let Body::Result { result, .. } = &report.body else {
        panic!("a result report");
    };
    assert!(protocol::encode(result.as_ref()).len() <= limits::HELPER_RESULT_JSON_BYTES);
    assert!(result.truncated);
    let (ToolResult::Exec(fitted), ToolResult::Exec(original)) =
        (&result.result, original(&before))
    else {
        panic!("exec results");
    };
    for (stream, unchanged) in [
        (&fitted.stdout, &original.stdout),
        (&fitted.stderr, &original.stderr),
    ] {
        assert_eq!(
            (stream.total_bytes, stream.total_lines),
            (unchanged.total_bytes, unchanged.total_lines)
        );
        assert!(stream.returned_bytes.get() < unchanged.returned_bytes.get());
    }
    assert_eq!(
        (decoded.header(), fitted.exit_code.0, fitted.duration_ms),
        (before.header(), original.exit_code.0, original.duration_ms)
    );
}

fn original(status: &ToolStatus) -> &ToolResult {
    match status {
        ToolStatus::Execution(protocol::tool::ExecutionReport::Result { result, .. }) => {
            &result.result
        }
        other => panic!("a result report: {other:?}"),
    }
}

fn error_envelope(error: ToolError) -> HelperEnvelope {
    bounded(
        HelperEnvelope {
            tool: HelperTool::Edit,
            status: HelperStatus::Error,
            truncated: false,
            error: Some(error),
            result: ToolResult::Object(Object::new()),
            metrics: Some(metrics()),
            drive_sync: None,
        },
        ErrorBounds::default(),
    )
}

fn object(value: Value) -> Object {
    match value {
        Value::Object(members) => members,
        other => panic!("{other} is not an object"),
    }
}

#[test]
fn an_error_message_is_cut_to_its_bound_on_utf8_boundaries_around_the_seam() {
    // Two-byte characters cut the head inside a character, three-byte ones
    // the tail.
    for message in ["m".repeat(8_192), "é".repeat(4_096), "界".repeat(4_096)] {
        let envelope = error_envelope(ToolError {
            kind: NonEmptyString::new("write_failed").unwrap(),
            message: message.clone(),
            path: None,
            detail: None,
        });
        let (_, decoded) = saved(&terminal(envelope));
        let ToolStatus::Execution(protocol::tool::ExecutionReport::Result { result, .. }) = decoded
        else {
            panic!("a result report");
        };
        let cut = result.error.expect("an error").message;
        assert!(cut.len() <= 4_096, "{} bytes", cut.len());
        assert!(cut.contains("\n[... truncated ...]\n"));
        let first = message.chars().next().unwrap();
        assert!(cut.starts_with(first) && cut.ends_with(first));
    }
}

#[test]
fn an_error_detail_over_its_bound_is_rebuilt_smaller_or_replaced_by_the_marker() {
    let over = json!({"stderr": "x".repeat(40_000 - r#"{"stderr":""}"#.len())});
    assert_eq!(protocol::encode(&over).len(), 40_000);
    let unfittable: Object = (0..3_000)
        .map(|index| (format!("key_{index:05}"), json!("v")))
        .collect();
    assert!(protocol::encode(&unfittable).len() > 32_768);
    for (detail, expect_marker_only) in [(object(over), false), (unfittable, true)] {
        let envelope = error_envelope(ToolError {
            kind: NonEmptyString::new("patch_verify").unwrap(),
            message: "the patch does not apply".to_owned(),
            path: None,
            detail: Some(detail),
        });
        let (_, decoded) = saved(&terminal(envelope));
        let ToolStatus::Execution(protocol::tool::ExecutionReport::Result { result, .. }) = decoded
        else {
            panic!("a result report");
        };
        let detail = result.error.expect("an error").detail.expect("a detail");
        assert!(protocol::encode(&detail).len() <= 32_768);
        assert_eq!(detail.get("truncated"), Some(&Value::Bool(true)));
        assert_eq!(detail.len() == 1, expect_marker_only, "{detail:?}");
    }
}
