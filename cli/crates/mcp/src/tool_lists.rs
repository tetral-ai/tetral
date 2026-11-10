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
    /// The failure that replaces a list whose report is over its byte or
    /// value limit, as `refused` says.
    fn over_limit(refused: ReportAdmissionError) -> Latest {
        let message = if refused == ReportAdmissionError::ReportTooComplex {
            format!(
                "tool list report over {} JSON values",
                protocol::strict::MAX_REPORT_VALUES
            )
        } else {
            format!(
                "tool list report over {} bytes",
                protocol::connection::TOOL_LIST_MAX_BYTES
            )
        };
        Latest::Failure {
            code: McpListErrorCode::McpListFailed,
            message,
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
    /// that replaces the latest when its report is over its byte or value
    /// limit.
    pub fn push(&mut self, latest: Latest) -> Option<Latest> {
        if self.in_flight.is_some() {
            self.unsent = Some(latest);
            return None;
        }
        self.send(latest)
    }

    /// The outbox's outcome for `report`. Gives the state that replaces the
    /// latest when the next report is over its byte or value limit.
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
            Err(
                refused @ (ReportAdmissionError::ReportTooLarge
                | ReportAdmissionError::ReportTooComplex),
            ) => {
                let failure = Latest::over_limit(refused);
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use protocol::ports::ReportOutcome;
    use serde_json::{Value, json};

    use super::*;

    /// Admits each tool list but answers one whose tool is named `over`
    /// with `refusal`, as the outbox answers a report over a limit.
    struct Sink {
        refusal: ReportAdmissionError,
        sent: Mutex<Vec<Value>>,
    }

    impl ReportSink for Sink {
        fn submit(&self, _: Submission) -> ReportId {
            unreachable!("a tool list is submitted with submit_tool_list")
        }
        fn submit_tool_list(
            &self,
            submission: Submission,
        ) -> Result<ReportId, ReportAdmissionError> {
            let mut sent = self.sent.lock().unwrap();
            let bytes =
                (submission.build)(&submission.connection_id, &Id::new("r").unwrap()).unwrap();
            let report: Value = serde_json::from_slice(&bytes).unwrap();
            if report["tools"][0]["name"] == "over" {
                return Err(self.refusal);
            }
            sent.push(report);
            Ok(ReportId(sent.len() as u64))
        }
        fn cancel(&self, _: ReportId) {}
        fn add_final(&self, _: ReportId, _: Id) {}
    }

    struct NoListener;

    impl ReportListener for NoListener {
        fn outcome(&self, _: ReportId, _: ReportOutcome) {}
    }

    fn list(name: &str) -> Latest {
        Latest::List {
            server_info: McpServerInfo {
                name: "cua".to_owned(),
                title: Nullable(None),
                version: "1".to_owned(),
            },
            tools: vec![
                serde_json::from_value(
                    json!({ "name": name, "inputSchema": { "type": "object" } }),
                )
                .unwrap(),
            ],
        }
    }

    /// A list refused over its byte or value limit goes as one failure under
    /// the list's `seq`; while that failure is in flight only the newest
    /// list waits, and goes next under the following `seq`; the next
    /// connection's reports start again at `seq` 1.
    #[test]
    fn a_list_over_a_limit_goes_as_one_failure_under_its_seq() {
        for (refusal, message) in [
            (
                ReportAdmissionError::ReportTooLarge,
                "tool list report over 4194304 bytes",
            ),
            (
                ReportAdmissionError::ReportTooComplex,
                "tool list report over 524288 JSON values",
            ),
        ] {
            let sink = Arc::new(Sink {
                refusal,
                sent: Mutex::default(),
            });
            let queue = |connection: &str| {
                Queue::new(
                    Id::new(connection).unwrap(),
                    "cua",
                    sink.clone(),
                    Arc::new(NoListener),
                )
            };
            let mut first = queue("conn_1");
            assert_eq!(
                first.push(list("over")),
                Some(Latest::Failure {
                    code: McpListErrorCode::McpListFailed,
                    message: message.to_owned(),
                })
            );
            assert_eq!(first.push(list("older")), None);
            assert_eq!(first.push(list("newest")), None);
            assert_eq!(first.outcome(ReportId(1)), None);
            assert_eq!(queue("conn_2").push(list("next")), None);
            let sent: Vec<Value> = sink
                .sent
                .lock()
                .unwrap()
                .iter()
                .map(|report| {
                    json!([
                        report["connection_id"],
                        report["seq"],
                        report["server_info"]["name"],
                        report["tools"][0]["name"],
                        report["error"]["message"]
                    ])
                })
                .collect();
            assert_eq!(
                sent,
                [
                    json!(["conn_1", 1, null, null, message]),
                    json!(["conn_1", 2, "cua", "newest", null]),
                    json!(["conn_2", 1, "cua", "next", null]),
                ],
                "{refusal:?}"
            );
        }
    }
}
