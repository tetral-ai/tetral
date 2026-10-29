//! The visible limits of a reply: an instruction's `limits` lower the byte
//! and line budget of each stream and never raise it.

use execution::capture::StreamCapture;
use execution::limits::Visible;
use execution::report::StreamOutput;
use protocol::tool::VisibleLimits;
use serde_json::{Value, json};

fn rendered(capture: &StreamCapture, limits: &Value) -> (i64, bool) {
    let limits: Option<VisibleLimits> = serde_json::from_value(limits.clone()).unwrap();
    let visible = Visible::requested(limits.as_ref());
    let snapshot = capture.snapshot(visible.bytes, visible.lines);
    (snapshot.returned_bytes.get(), snapshot.truncated)
}

#[test]
fn limits_lower_the_visible_bytes_and_lines_and_never_raise_or_hide_them() {
    // 2,001 lines of 10 bytes: each returned line is 10 bytes.
    let mut lines = StreamCapture::new();
    lines.write(&b"123456789\n".repeat(2_001));
    for (limits, returned_lines) in [
        (Value::Null, 2_000),
        (json!({"visible_lines": 10}), 10),
        (json!({"visible_lines": 5_000}), 2_000),
        (json!({"visible_lines": 0}), 2_000),
        (json!({"visible_lines": -1}), 2_000),
    ] {
        assert_eq!(
            rendered(&lines, &limits),
            (returned_lines * 10, true),
            "{limits}"
        );
    }
    let mut line = StreamCapture::new();
    line.write(&[b'a'; 60_000]);
    for (limits, returned_bytes) in [
        (Value::Null, 51_200),
        (json!({"visible_bytes": 1_000}), 1_000),
        (json!({"visible_bytes": 60_000}), 51_200),
        (json!({"visible_bytes": 0}), 51_200),
        (json!({"visible_bytes": -1}), 51_200),
    ] {
        assert_eq!(rendered(&line, &limits), (returned_bytes, true), "{limits}");
    }
}
