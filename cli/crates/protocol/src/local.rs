//! The JSON bodies of the App Server's local API, served on its Unix socket to
//! the `tetral` commands, the local SDK and apps.

use serde::{Deserialize, Serialize};

use crate::connection::PreparationError;
use crate::field::{AbsolutePath, Nullable};
use crate::id::Id;
use crate::time::Rfc3339Utc;
use crate::tool::ProtocolVersion;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerState {
    Running,
    Stopping,
}

/// A state of the connection lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Registering,
    Online,
    Resuming,
    Blocked,
    Stopping,
    Stopped,
}

/// A failure as a command result and `last_error` carry it. `message` is
/// displayable and never holds a key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ErrorObject {
    pub code: String,
    pub message: String,
}

/// `GET /v1/status`, also what `server status` and `server run` print.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Status {
    pub server_state: ServerState,
    pub connection_state: ConnectionState,
    pub instance_id: Id,
    /// `None` without a connection.
    pub connection_id: Nullable<Id>,
    pub credential_id: Id,
    pub expires_at: Rfc3339Utc,
    pub cli_version: String,
    pub protocol_version: ProtocolVersion,
    pub last_error: Nullable<ErrorObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Its `session.prepare` waits behind the ready ACK.
    Staged,
    Preparing,
    PreparedPendingAck,
    Prepared,
    Failed,
    Closing,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionEntry {
    pub session_id: Id,
    pub binding_id: Id,
    pub connection_id: Id,
    /// The canonical working directory, or `None` before one is known.
    pub cwd: Nullable<AbsolutePath>,
    pub state: SessionState,
    /// The latest preparation error.
    pub error: Nullable<PreparationError>,
}

/// `GET /v1/sessions`, read from saved state without filesystem checks.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Sessions {
    pub sessions: Vec<SessionEntry>,
}

/// The body of `POST /v1/computer-use`: exactly these two members.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComputerUseRequest {
    pub enabled: bool,
    /// The cua-driver executable when `enabled`; otherwise ignored, and the
    /// recorded driver kept.
    pub driver: Nullable<String>,
}
