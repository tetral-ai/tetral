//! The bounds of an error's `message`, `path` and `detail` in a report before
//! it is saved; each report decodes with the protocol's types.

use protocol::field::{Count, NonEmptyString, PositiveInt};
use protocol::id::Id;
use protocol::report::{Body, Correlation, ErrorBounds, Report, bounded};
use protocol::tool::{
    HelperEnvelope, HelperStatus, HelperTool, Metrics, Object, ReportPhase, ToolError, ToolResult,
    ToolStatus,
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
fn an_error_message_and_path_are_cut_to_their_bounds_on_utf8_boundaries_around_the_seam() {
    // Two-byte characters cut the head inside a character, three-byte ones
    // the tail.
    for text in ["m".repeat(8_192), "é".repeat(4_096), "界".repeat(4_096)] {
        let envelope = error_envelope(ToolError {
            kind: NonEmptyString::new("write_failed").unwrap(),
            message: text.clone(),
            path: Some(text.clone()),
            detail: None,
        });
        let (_, decoded) = saved(&terminal(envelope));
        let ToolStatus::Execution(protocol::tool::ExecutionReport::Result { result, .. }) = decoded
        else {
            panic!("a result report");
        };
        let error = result.error.expect("an error");
        let first = text.chars().next().unwrap();
        for cut in [error.message, error.path.expect("a path")] {
            assert!(cut.len() <= 4_096, "{} bytes", cut.len());
            assert!(cut.contains("\n[... truncated ...]\n"));
            assert!(cut.starts_with(first) && cut.ends_with(first));
        }
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
