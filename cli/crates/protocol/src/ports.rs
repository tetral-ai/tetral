//! The interfaces through which the App Server's peer crates reach each other
//! without depending on each other.

use std::sync::Arc;

use crate::connection::{McpTools, SessionStatus};
use crate::id::Id;
use crate::tool::{DriveSyncReport, ProtocolErrorReport, ToolStatus};
use crate::{DecodeError, Message, decode, encode};

/// The endpoints of the reports that answer or follow instructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReportEndpoint {
    SessionStatus,
    ToolStatus,
    DriveSync,
    McpTools,
    ProtocolError,
}

impl ReportEndpoint {
    pub fn path(self) -> &'static str {
        match self {
            ReportEndpoint::SessionStatus => "/v1/client/session-status",
            ReportEndpoint::ToolStatus => "/v1/client/tool-status",
            ReportEndpoint::DriveSync => "/v1/client/drive-sync",
            ReportEndpoint::McpTools => "/v1/client/mcp-tools",
            ReportEndpoint::ProtocolError => "/v1/client/protocol-error",
        }
    }
}

/// A report type and the endpoint that takes it.
pub trait Report: Message + Send + 'static {
    const ENDPOINT: ReportEndpoint;
}

impl Report for SessionStatus {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::SessionStatus;
}

impl Report for ToolStatus {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::ToolStatus;
}

impl Report for DriveSyncReport {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::DriveSync;
}

impl Report for McpTools {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::McpTools;
}

impl Report for ProtocolErrorReport {
    const ENDPOINT: ReportEndpoint = ReportEndpoint::ProtocolError;
}

/// Makes a report's bytes from the connection's ID and the report's own
/// `message_id`, once they pass the report's own decoder.
pub type BuildReport = Box<dyn FnOnce(&Id, &Id) -> Result<Vec<u8>, DecodeError> + Send>;

/// One report handed to the Report outbox.
pub struct Submission {
    /// The connection the report belongs to; it is sent on no other.
    pub connection_id: Id,
    pub endpoint: ReportEndpoint,
    /// The `message_id` of the instruction whose final report this is.
    pub final_for: Option<Id>,
    /// The `traceparent` of the instruction the report was built for, when
    /// that value is usable.
    pub traceparent: Option<String>,
    /// Told whether the report was acknowledged or dropped.
    pub listener: Option<Arc<dyn ReportListener>>,
    pub build: BuildReport,
}

impl Submission {
    /// The report that `report` makes from the connection's ID and the
    /// report's `message_id`, for the connection `connection_id`.
    pub fn new<R: Report>(
        connection_id: Id,
        report: impl FnOnce(Id, Id) -> R + Send + 'static,
    ) -> Submission {
        Submission {
            connection_id,
            endpoint: R::ENDPOINT,
            final_for: None,
            traceparent: None,
            listener: None,
            build: Box::new(move |connection, message| {
                let bytes = encode(&report(connection.clone(), message.clone()));
                decode::<R>(&bytes).map(|_| bytes)
            }),
        }
    }
}

/// The outbox's name for one submitted report, unique in the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReportId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOutcome {
    /// Its matching ACK came (`ReportAcked`).
    Acked,
    /// Engine refused it or its connection ended; it is never sent again
    /// (`ReportDropped`).
    Dropped,
}

/// Takes the outcome of each report its submitter handed to the outbox.
pub trait ReportListener: Send + Sync {
    fn outcome(&self, report: ReportId, outcome: ReportOutcome);
}

/// Takes every upstream report that `session`, `execution`, `drive` and `mcp`
/// build; its implementation belongs to the Report outbox of `connection`.
pub trait ReportSink: Send + Sync {
    /// Freezes the report and sends it until its matching ACK or the end of
    /// its connection.
    fn submit(&self, submission: Submission) -> ReportId;

    /// Stops the report's retries and deletes it.
    fn cancel(&self, report: ReportId);
}

/// Creates the Session binding owner of a Session for the Connection owner;
/// its implementation belongs to `session`.
pub trait SessionFactory: Send + Sync {}

/// The Connection owner's handle on one Session binding owner; its
/// implementation belongs to `session`.
pub trait SessionHandle: Send + Sync {}

/// How Session binding owners and Drive sync owners reach the Connection
/// owner; its implementation belongs to `connection`.
pub trait ConnectionPort: Send + Sync {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::{ProtocolError, ProtocolErrorCode};

    fn protocol_error(message: String) -> Submission {
        Submission::new(Id::new("conn_a").unwrap(), move |connection, message_id| {
            ProtocolErrorReport {
                protocol_version: Default::default(),
                connection_id: connection,
                message_id,
                request_message_id: None,
                error: ProtocolError {
                    code: ProtocolErrorCode::InvalidMessage,
                    message,
                },
            }
        })
    }

    #[test]
    fn a_report_gets_its_identity_at_build_and_one_over_its_limit_fails_its_own_decoder() {
        let connection = Id::new("conn_a").unwrap();
        let message_id = Id::new("r1").unwrap();
        let submission = protocol_error("the message cannot be parsed".to_owned());
        assert_eq!(submission.endpoint, ReportEndpoint::ProtocolError);
        let bytes = (submission.build)(&connection, &message_id).unwrap();
        let report: ProtocolErrorReport = decode(&bytes).unwrap();
        assert_eq!(
            (report.connection_id, report.message_id),
            (connection.clone(), message_id.clone())
        );

        let oversized = protocol_error("x".repeat(65_536));
        assert!((oversized.build)(&connection, &message_id).is_err());
    }
}
