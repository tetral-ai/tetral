//! The bounds of the local MCP servers. Each is a named constant; a test
//! may run the manager with other [`Limits`].

use std::time::Duration;

/// From a server process's spawn to its `initialize` result.
pub const START: Duration = Duration::from_secs(30);
/// From a listing's first `tools/list` to its last answer.
pub const LISTING: Duration = Duration::from_secs(30);
/// The longest `nextCursor` a listing follows.
pub const CURSOR_BYTES: usize = 65_536;
/// The most pages one listing reads.
pub const PAGES: usize = 100;
/// The most tools one list holds.
pub const TOOLS: usize = protocol::connection::TOOLS_MAX;
/// The largest tool-list report, and the largest compact JSON of a list's
/// kept tools.
pub const REPORT_BYTES: usize = protocol::connection::TOOL_LIST_MAX_BYTES;
/// From SIGTERM to a stopped process's group to SIGKILL.
pub const STOP_GRACE: Duration = Duration::from_secs(2);
/// The longest JSON-RPC message, one line of a server's stdout, read before
/// the connection is closed.
pub const MCP_STDOUT_MESSAGE_BYTES: usize = 14_680_064;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub start: Duration,
    pub listing: Duration,
    pub cursor_bytes: usize,
    pub pages: usize,
    pub tools: usize,
    pub report_bytes: usize,
    pub stop_grace: Duration,
    pub stdout_message_bytes: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            start: START,
            listing: LISTING,
            cursor_bytes: CURSOR_BYTES,
            pages: PAGES,
            tools: TOOLS,
            report_bytes: REPORT_BYTES,
            stop_grace: STOP_GRACE,
            stdout_message_bytes: MCP_STDOUT_MESSAGE_BYTES,
        }
    }
}
