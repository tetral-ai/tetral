//! How long one delivery attempt of a report may take.

use std::time::Duration;

use protocol::ports::ReportEndpoint;

/// The total time of an attempt of a report whose limit is 64 KiB, and of
/// each 65,536 bytes of a size-scaled report.
pub const REPORT_TOTAL: Duration = Duration::from_secs(10);

/// The bytes that each [`REPORT_TOTAL`] of a size-scaled report covers.
pub const SCALE_BYTES: usize = 65_536;

/// The longest an attempt goes without progress: from its start to the
/// first byte taken, which covers connecting and the handshake, between
/// bytes, and from the last byte to the answer.
pub const STALL_LIMIT: Duration = Duration::from_secs(10);

/// Whether a report's total grows with its size: a tool-status or drive-sync
/// report once `tool_execution` is negotiated, whose limits then exceed
/// 64 KiB, and every local MCP tool-list report.
pub fn scales(endpoint: ReportEndpoint, tool_execution: bool) -> bool {
    match endpoint {
        ReportEndpoint::ToolStatus | ReportEndpoint::DriveSync => tool_execution,
        ReportEndpoint::McpTools => true,
        ReportEndpoint::SessionStatus | ReportEndpoint::ProtocolError => false,
    }
}

/// The total time of an attempt of a report of `bytes`: `unit` times
/// `max(1, ceil(bytes / 65,536))` when it scales, `unit` otherwise.
pub fn total(
    unit: Duration,
    endpoint: ReportEndpoint,
    bytes: usize,
    tool_execution: bool,
) -> Duration {
    if !scales(endpoint, tool_execution) {
        return unit;
    }
    let steps = bytes.div_ceil(SCALE_BYTES).max(1);
    unit * u32::try_from(steps).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_size_scaled_report_has_ten_seconds_for_each_65_536_bytes_started_and_a_protocol_error_ten_in_all()
     {
        for (bytes, seconds) in [
            (65_536, 10),
            (65_537, 20),
            (131_072, 20),
            (14_680_064, 2_240),
        ] {
            assert_eq!(
                total(REPORT_TOTAL, ReportEndpoint::ToolStatus, bytes, true),
                Duration::from_secs(seconds),
                "{bytes} bytes"
            );
        }
        for bytes in [1, 65_537, 14_680_064] {
            assert_eq!(
                total(REPORT_TOTAL, ReportEndpoint::ProtocolError, bytes, true),
                Duration::from_secs(10),
                "a protocol error of {bytes} bytes"
            );
        }
    }
}
