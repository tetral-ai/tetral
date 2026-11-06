//! A server's latest state and its tool-list reports on the online
//! connection. Per server, at most one report is in flight and, behind it,
//! only the newest unsent one. A report gets its `seq` when it goes in
//! flight: `1` for the server's first report on the connection and one more
//! than its previous one after that. The next report goes in flight after
//! the outbox's outcome for the one in flight.

use std::sync::Arc;

use protocol::connection::{McpListError, McpListErrorCode, McpServerInfo, McpTool, McpTools};
use protocol::field::{List, Nullable, PositiveInt, Text};
use protocol::id::Id;
use protocol::ports::{ReportAdmissionError, ReportId, ReportListener, ReportSink, Submission};

/// A server's latest state.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Latest {
    List {
        server_info: McpServerInfo,
        tools: Vec<McpTool>,
    },
    Failure {
        code: McpListErrorCode,
        message: String,
    },
    Disabled,
}

impl Latest {
    /// The failure that replaces a list whose report is over its limit.
    fn too_large() -> Latest {
        Latest::Failure {
            code: McpListErrorCode::McpListFailed,
            message: format!(
                "tool list report over {} bytes",
                protocol::connection::TOOL_LIST_MAX_BYTES
            ),
        }
    }

    fn report(self, server: &'static str, seq: i64, connection_id: Id, message_id: Id) -> McpTools {
        let (server_info, tools, error) = match self {
            Latest::List { server_info, tools } => (Some(server_info), tools, None),
            Latest::Failure { code, message } => {
                (None, Vec::new(), Some(McpListError { code, message }))
            }
            Latest::Disabled => (None, Vec::new(), None),
        };
        McpTools {
            protocol_version: Default::default(),
            connection_id,
            message_id,
            server: Text::new(server).expect("a server's name is not empty"),
            seq: PositiveInt::new(seq).expect("seq starts at 1"),
            server_info: Nullable(server_info),
            tools: List(tools),
            error: Nullable(error),
        }
    }
}

/// One server's reports on one online connection.
pub(crate) struct Queue {
    pub connection_id: Id,
    server: &'static str,
    sink: Arc<dyn ReportSink>,
    listener: Arc<dyn ReportListener>,
    last_seq: i64,
    in_flight: Option<ReportId>,
    unsent: Option<Latest>,
}

impl Queue {
    pub fn new(
        connection_id: Id,
        server: &'static str,
        sink: Arc<dyn ReportSink>,
        listener: Arc<dyn ReportListener>,
    ) -> Queue {
        Queue {
            connection_id,
            server,
            sink,
            listener,
            last_seq: 0,
            in_flight: None,
            unsent: None,
        }
    }

    /// Reports `latest`, now or after the report in flight. Gives the state
    /// that replaces the latest when its report is over its limit.
    pub fn push(&mut self, latest: Latest) -> Option<Latest> {
        if self.in_flight.is_some() {
            self.unsent = Some(latest);
            return None;
        }
        self.send(latest)
    }

    /// The outbox's outcome for `report`. Gives the state that replaces the
    /// latest when the next report is over its limit.
    pub fn outcome(&mut self, report: ReportId) -> Option<Latest> {
        if self.in_flight != Some(report) {
            return None;
        }
        self.in_flight = None;
        self.unsent.take().and_then(|next| self.send(next))
    }

    fn send(&mut self, latest: Latest) -> Option<Latest> {
        let seq = self.last_seq + 1;
        match self.sink.submit_tool_list(self.submission(seq, latest)) {
            Ok(report) => {
                self.in_flight = Some(report);
                self.last_seq = seq;
                None
            }
            Err(ReportAdmissionError::ReportTooLarge) => {
                let failure = Latest::too_large();
                if let Ok(report) = self
                    .sink
                    .submit_tool_list(self.submission(seq, failure.clone()))
                {
                    self.in_flight = Some(report);
                    self.last_seq = seq;
                }
                Some(failure)
            }
            // Reserved admission ends the connection instead.
            Err(ReportAdmissionError::ResourceExhausted) => None,
        }
    }

    fn submission(&self, seq: i64, latest: Latest) -> Submission {
        let server = self.server;
        let mut submission = Submission::new(
            self.connection_id.clone(),
            move |connection_id, message_id| latest.report(server, seq, connection_id, message_id),
        );
        submission.listener = Some(self.listener.clone());
        submission
    }
}
