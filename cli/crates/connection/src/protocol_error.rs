//! The protocol errors the CLI reports: `message_conflict` for a
//! `message_id` reused with other content and `invalid_message` for every
//! other one, each with a fixed text naming its class. A report never holds
//! the invalid content and never guesses a Session or task identity.

use protocol::id::Id;
use protocol::ports::Submission;
use protocol::tool::{ProtocolError, ProtocolErrorCode, ProtocolErrorReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolErrorKind {
    /// Not one JSON object, or not the envelope's schema.
    Unparseable,
    /// A Session instruction without one of its identities.
    MissingIdentity,
    /// The `message_id` of an earlier instruction with other content.
    Conflict,
    /// A payload that fails its schema.
    InvalidPayload,
}

impl ProtocolErrorKind {
    pub fn code(self) -> ProtocolErrorCode {
        match self {
            ProtocolErrorKind::Conflict => ProtocolErrorCode::MessageConflict,
            _ => ProtocolErrorCode::InvalidMessage,
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            ProtocolErrorKind::Unparseable => {
                "the message is not one JSON object or fails the envelope schema"
            }
            ProtocolErrorKind::MissingIdentity => {
                "the Session instruction lacks a nonempty session_id, binding_id, tool_use_event_id or task_id"
            }
            ProtocolErrorKind::Conflict => "the message_id was used before with other content",
            ProtocolErrorKind::InvalidPayload => "the payload fails its schema",
        }
    }
}

/// The report of a protocol error on `connection`, correlated with the
/// message `request_message_id` when it is known.
pub fn report(
    connection: Id,
    kind: ProtocolErrorKind,
    request_message_id: Option<Id>,
) -> Submission {
    Submission::new(connection, move |connection_id, message_id| {
        ProtocolErrorReport {
            protocol_version: Default::default(),
            connection_id,
            message_id,
            request_message_id,
            error: ProtocolError {
                code: kind.code(),
                message: kind.message().to_owned(),
            },
        }
    })
}
