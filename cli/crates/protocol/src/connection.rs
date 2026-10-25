//! The connection messages: the event-stream query, the downstream envelope
//! and its payloads, ready, heartbeats, Session reports, the local MCP
//! tool-list report and the credential and Git ticket bodies
//! (`connection-v1.schema.json`).

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::ErrorCode;
use crate::field::{
    self, Absent, AbsolutePath, Bounded, CommitSha, ConstText, Fixed, List, NonEmptyString,
    Nullable, PositiveInt, Text, TextRule, True, Unique, is_unique,
};
use crate::id::{Id, SafeId};
use crate::path::{DrivePath, ExcludePath};
use crate::time::Rfc3339Utc;
use crate::tool::{
    self, CancelPayload, DriveMountFailure, ExecutePayload, LocalMcpCapability, McpCallPayload,
    Object, ProtocolVersion, StdinPayload, ToolExecutionCapability, ToolInstruction, limits,
};
use crate::{DecodeError, Message, Received, rule};

/// Every connection message not given a larger limit: a request or response
/// body, an ACK, a ready and a downstream event other than a tool
/// instruction.
pub const MESSAGE_MAX_BYTES: usize = 65_536;

/// A local MCP tool-list report.
pub const TOOL_LIST_MAX_BYTES: usize = 4_194_304;

/// The `lost_changes` paths one entry lists.
pub const LOST_CHANGE_PATHS_MAX: usize = 100;

/// The tools one tool-list report lists.
pub const TOOLS_MAX: usize = 2_048;

/// The longest connection validity one ACK grants.
pub const GRANT_MAX_MS: i64 = 45_000;

/// The longest startup phase a ready reports.
pub const STARTUP_PHASE_MAX_MS: i64 = 2_147_483_647;

/// The prefix of every connection key, which no message but a credential
/// response may carry.
pub const CONNECTION_KEY_PREFIX: &str = "tetral_ck_";

pub enum QueryVersion {}

impl ConstText for QueryVersion {
    const TEXT: &'static str = "1";
}

/// `protocol_version` in a query string.
pub type QueryProtocolVersion = Fixed<QueryVersion>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Resume {
    #[serde(rename = "true")]
    True,
    #[serde(rename = "false")]
    False,
}

/// The query of `GET /v1/client/events`, as its members' strings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EventsQuery {
    pub protocol_version: QueryProtocolVersion,
    pub instance_id: Id,
    pub credential_id: Id,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_connection_id: Option<Id>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub resume: Option<Resume>,
}

impl Message for EventsQuery {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConnectedPayload {
    pub resumed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct HeartbeatPayload {
    pub heartbeat_seq: PositiveInt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DriveAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DriveBinding {
    pub drive_id: SafeId,
    pub access: DriveAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DriveExclude {
    pub drive_id: Id,
    pub paths: Vec<ExcludePath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Checkout {
    Branch {
        name: NonEmptyString,
        #[serde(default, skip_serializing)]
        sha: Absent,
    },
    Commit {
        sha: CommitSha,
        #[serde(default, skip_serializing)]
        name: Absent,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GitIdentity {
    pub name: String,
    pub email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Repository {
    pub url: NonEmptyString,
    /// `None` clones the repository's default branch.
    pub checkout: Nullable<Checkout>,
    pub path: AbsolutePath,
    pub git_identity: GitIdentity,
    #[serde(default, skip_serializing)]
    pub authorization_token: Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionPreparePayload {
    pub session_id: SafeId,
    pub binding_id: Id,
    pub cwd: AbsolutePath,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub create_cwd: Option<bool>,
    pub drives: Vec<DriveBinding>,
    pub drive_exclude: Vec<DriveExclude>,
    pub repositories: Vec<Repository>,
    #[serde(default, skip_serializing)]
    pub ticket: Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionClosePayload {
    pub session_id: SafeId,
    pub binding_id: Id,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionDrivesPayload {
    pub session_id: SafeId,
    pub binding_id: Id,
    pub drives: Vec<DriveBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CredentialRenewPayload {
    pub credential_id: Id,
    #[serde(default, skip_serializing)]
    pub connection_key: Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum DisconnectPayload {
    /// Only the event stream ends; the connection may resume.
    Stream {
        #[serde(default, skip_serializing)]
        retryable: Absent,
        #[serde(default, skip_serializing)]
        code: Absent,
    },
    /// The connection ends.
    Connection { retryable: bool, code: ErrorCode },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DriveChangedPayload {
    pub drive_id: Id,
    pub seq: PositiveInt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownstreamPayload {
    Connected(ConnectedPayload),
    Heartbeat(HeartbeatPayload),
    SessionPrepare(SessionPreparePayload),
    SessionClose(SessionClosePayload),
    SessionDrives(SessionDrivesPayload),
    CredentialRenew(CredentialRenewPayload),
    Disconnect(DisconnectPayload),
    DriveChanged(DriveChangedPayload),
    ToolExecute(ExecutePayload),
    ToolStdin(StdinPayload),
    ToolCancel(CancelPayload),
    McpCall(McpCallPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum DownstreamType {
    #[serde(rename = "connected")]
    Connected,
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(rename = "session.prepare")]
    SessionPrepare,
    #[serde(rename = "session.close")]
    SessionClose,
    #[serde(rename = "session.drives")]
    SessionDrives,
    #[serde(rename = "credential.renew")]
    CredentialRenew,
    #[serde(rename = "disconnect")]
    Disconnect,
    #[serde(rename = "drive.changed")]
    DriveChanged,
    #[serde(rename = "tool.execute")]
    ToolExecute,
    #[serde(rename = "tool.stdin")]
    ToolStdin,
    #[serde(rename = "tool.cancel")]
    ToolCancel,
    #[serde(rename = "mcp.call")]
    McpCall,
}

impl DownstreamType {
    /// `session.prepare`, `session.close`, `session.drives` and the tool and
    /// local MCP instructions.
    pub fn is_session_instruction(self) -> bool {
        matches!(
            self,
            DownstreamType::SessionPrepare
                | DownstreamType::SessionClose
                | DownstreamType::SessionDrives
        ) || self.is_tool_instruction()
    }

    /// `tool.execute`, `tool.stdin`, `tool.cancel` and `mcp.call`.
    pub fn is_tool_instruction(self) -> bool {
        matches!(
            self,
            DownstreamType::ToolExecute
                | DownstreamType::ToolStdin
                | DownstreamType::ToolCancel
                | DownstreamType::McpCall
        )
    }
}

impl DownstreamPayload {
    pub fn kind(&self) -> DownstreamType {
        match self {
            DownstreamPayload::Connected(_) => DownstreamType::Connected,
            DownstreamPayload::Heartbeat(_) => DownstreamType::Heartbeat,
            DownstreamPayload::SessionPrepare(_) => DownstreamType::SessionPrepare,
            DownstreamPayload::SessionClose(_) => DownstreamType::SessionClose,
            DownstreamPayload::SessionDrives(_) => DownstreamType::SessionDrives,
            DownstreamPayload::CredentialRenew(_) => DownstreamType::CredentialRenew,
            DownstreamPayload::Disconnect(_) => DownstreamType::Disconnect,
            DownstreamPayload::DriveChanged(_) => DownstreamType::DriveChanged,
            DownstreamPayload::ToolExecute(_) => DownstreamType::ToolExecute,
            DownstreamPayload::ToolStdin(_) => DownstreamType::ToolStdin,
            DownstreamPayload::ToolCancel(_) => DownstreamType::ToolCancel,
            DownstreamPayload::McpCall(_) => DownstreamType::McpCall,
        }
    }

    pub fn tool_instruction(&self) -> Option<ToolInstruction<'_>> {
        match self {
            DownstreamPayload::ToolExecute(payload) => Some(ToolInstruction::Execute(payload)),
            DownstreamPayload::ToolStdin(payload) => Some(ToolInstruction::Stdin(payload)),
            DownstreamPayload::ToolCancel(payload) => Some(ToolInstruction::Cancel(payload)),
            DownstreamPayload::McpCall(payload) => Some(ToolInstruction::McpCall(payload)),
            _ => None,
        }
    }

    fn read(kind: DownstreamType, payload: Value) -> Result<DownstreamPayload, serde_json::Error> {
        use serde_json::from_value as read;
        Ok(match kind {
            DownstreamType::Connected => DownstreamPayload::Connected(read(payload)?),
            DownstreamType::Heartbeat => DownstreamPayload::Heartbeat(read(payload)?),
            DownstreamType::SessionPrepare => DownstreamPayload::SessionPrepare(read(payload)?),
            DownstreamType::SessionClose => DownstreamPayload::SessionClose(read(payload)?),
            DownstreamType::SessionDrives => DownstreamPayload::SessionDrives(read(payload)?),
            DownstreamType::CredentialRenew => DownstreamPayload::CredentialRenew(read(payload)?),
            DownstreamType::Disconnect => DownstreamPayload::Disconnect(read(payload)?),
            DownstreamType::DriveChanged => DownstreamPayload::DriveChanged(read(payload)?),
            DownstreamType::ToolExecute => DownstreamPayload::ToolExecute(read(payload)?),
            DownstreamType::ToolStdin => DownstreamPayload::ToolStdin(read(payload)?),
            DownstreamType::ToolCancel => DownstreamPayload::ToolCancel(read(payload)?),
            DownstreamType::McpCall => DownstreamPayload::McpCall(read(payload)?),
        })
    }

    fn write(&self) -> Value {
        let value = match self {
            DownstreamPayload::Connected(payload) => serde_json::to_value(payload),
            DownstreamPayload::Heartbeat(payload) => serde_json::to_value(payload),
            DownstreamPayload::SessionPrepare(payload) => serde_json::to_value(payload),
            DownstreamPayload::SessionClose(payload) => serde_json::to_value(payload),
            DownstreamPayload::SessionDrives(payload) => serde_json::to_value(payload),
            DownstreamPayload::CredentialRenew(payload) => serde_json::to_value(payload),
            DownstreamPayload::Disconnect(payload) => serde_json::to_value(payload),
            DownstreamPayload::DriveChanged(payload) => serde_json::to_value(payload),
            DownstreamPayload::ToolExecute(payload) => serde_json::to_value(payload),
            DownstreamPayload::ToolStdin(payload) => serde_json::to_value(payload),
            DownstreamPayload::ToolCancel(payload) => serde_json::to_value(payload),
            DownstreamPayload::McpCall(payload) => serde_json::to_value(payload),
        };
        value.expect("protocol types always serialize")
    }
}

/// One event of the stream: Engine's envelope around a typed payload.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "Envelope", into = "Envelope")]
pub struct Downstream {
    pub connection_id: Id,
    pub message_id: Id,
    pub payload: DownstreamPayload,
    /// The W3C `traceparent` of the work the instruction belongs to, as sent.
    pub traceparent: Option<String>,
}

/// A downstream event's envelope with its payload as an object, read before
/// the payload's own schema.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Envelope {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    #[serde(rename = "type")]
    pub kind: DownstreamType,
    pub payload: Object,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub traceparent: Option<String>,
}

impl Message for Envelope {
    const MAX_BYTES: usize = limits::EXECUTE_SSE_JSON_BYTES;
}

impl TryFrom<Envelope> for Downstream {
    type Error = String;

    fn try_from(raw: Envelope) -> Result<Self, String> {
        let payload = DownstreamPayload::read(raw.kind, Value::Object(raw.payload))
            .map_err(|error| format!("payload: {error}"))?;
        Ok(Downstream {
            connection_id: raw.connection_id,
            message_id: raw.message_id,
            payload,
            traceparent: raw.traceparent,
        })
    }
}

impl From<Downstream> for Envelope {
    fn from(downstream: Downstream) -> Self {
        let payload = match downstream.payload.write() {
            Value::Object(payload) => payload,
            _ => Object::new(),
        };
        Envelope {
            protocol_version: tool::ProtocolVersion::default(),
            connection_id: downstream.connection_id,
            message_id: downstream.message_id,
            kind: downstream.payload.kind(),
            payload,
            traceparent: downstream.traceparent,
        }
    }
}

impl Message for Downstream {
    const MAX_BYTES: usize = limits::EXECUTE_SSE_JSON_BYTES;

    fn check(&self, received: &Received<'_>) -> Result<(), DecodeError> {
        if let Some(instruction) = self.payload.tool_instruction() {
            return tool::check_instruction(&instruction, received.bytes);
        }
        rule(
            received.bytes <= MESSAGE_MAX_BYTES,
            "event over 65,536 bytes",
        )?;
        match &self.payload {
            DownstreamPayload::SessionPrepare(prepare) => {
                rule(
                    prepare.create_cwd == Some(true) || prepare.repositories.is_empty(),
                    "repositories without create_cwd",
                )?;
                rule(
                    is_unique(prepare.drives.iter().map(|drive| drive.drive_id.as_str())),
                    "drive_id twice in drives",
                )?;
                rule(
                    is_unique(
                        prepare
                            .repositories
                            .iter()
                            .map(|repository| repository.path.as_str()),
                    ),
                    "repository path twice",
                )
            }
            DownstreamPayload::SessionDrives(drives) => rule(
                is_unique(drives.drives.iter().map(|drive| drive.drive_id.as_str())),
                "drive_id twice in drives",
            ),
            DownstreamPayload::CredentialRenew(_) => rule(
                !holds_key(&received.value["payload"]),
                "credential.renew carries a connection key",
            ),
            _ => Ok(()),
        }
    }
}

/// A downstream event of tool execution: a tool instruction.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "Downstream", into = "Downstream")]
pub struct ToolDownstream(pub Downstream);

impl TryFrom<Downstream> for ToolDownstream {
    type Error = String;

    fn try_from(downstream: Downstream) -> Result<Self, String> {
        if downstream.payload.tool_instruction().is_some() {
            Ok(ToolDownstream(downstream))
        } else {
            Err("not a tool instruction".to_owned())
        }
    }
}

impl From<ToolDownstream> for Downstream {
    fn from(downstream: ToolDownstream) -> Self {
        downstream.0
    }
}

impl Message for ToolDownstream {
    const MAX_BYTES: usize = limits::EXECUTE_SSE_JSON_BYTES;

    fn check(&self, received: &Received<'_>) -> Result<(), DecodeError> {
        self.0.check(received)
    }
}

/// Whether any string in `value`, a member name or a string value, holds a
/// connection key.
fn holds_key(value: &Value) -> bool {
    match value {
        Value::String(text) => text.contains(CONNECTION_KEY_PREFIX),
        Value::Array(items) => items.iter().any(holds_key),
        Value::Object(members) => members
            .iter()
            .any(|(name, value)| name.contains(CONNECTION_KEY_PREFIX) || holds_key(value)),
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DriveCapability {
    pub mount: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Capabilities {
    pub session_preparation: True,
    pub drives: DriveCapability,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub tool_execution: Option<ToolExecutionCapability>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_mcp: Option<LocalMcpCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LostChange {
    pub request_message_id: Id,
    pub drive_id: Id,
    pub paths: Unique<DrivePath, 0, LOST_CHANGE_PATHS_MAX>,
    pub paths_total: PositiveInt,
}

pub type LostChanges = List<LostChange, 1>;

/// Where a `lost_changes` list appears, which decides its order rules.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LostChangesIn {
    /// A ready repeats the kept entries of earlier reports in kept order.
    Ready,
    /// A Session report lists one entry per drive in `drive_id` order, its
    /// paths fitted entry after entry until one entry is cut.
    Report,
}

fn check_lost_changes(entries: &[LostChange], within: LostChangesIn) -> Result<(), DecodeError> {
    for entry in entries {
        let paths = entry.paths.iter().map(|path| path.as_bytes());
        rule(
            paths
                .clone()
                .zip(paths.skip(1))
                .all(|(earlier, later)| earlier < later),
            "lost_changes paths out of UTF-8 byte order",
        )?;
        rule(
            entry.paths_total.get() >= entry.paths.len() as i64,
            "paths_total below listed paths",
        )?;
    }
    let ascending = |ids: &[&[u8]]| ids.windows(2).all(|pair| pair[0] < pair[1]);
    match within {
        LostChangesIn::Report => {
            let ids: Vec<&[u8]> = entries
                .iter()
                .map(|entry| entry.drive_id.as_bytes())
                .collect();
            rule(
                ascending(&ids),
                "lost_changes entries out of drive_id order",
            )?;
            let mut cut = false;
            for entry in entries {
                rule(
                    !cut || entry.paths.is_empty(),
                    "paths listed after a cut entry",
                )?;
                let whole = entry.paths_total.get().min(LOST_CHANGE_PATHS_MAX as i64);
                cut = cut || (entry.paths.len() as i64) < whole;
            }
        }
        LostChangesIn::Ready => {
            let mut reports: Vec<(&str, Vec<&[u8]>)> = Vec::new();
            for entry in entries {
                let report = entry.request_message_id.as_str();
                match reports.iter_mut().find(|(id, _)| *id == report) {
                    Some((_, ids)) => ids.push(entry.drive_id.as_bytes()),
                    None => reports.push((report, vec![entry.drive_id.as_bytes()])),
                }
            }
            rule(
                reports.iter().all(|(_, ids)| ascending(ids)),
                "kept lost_changes entry repeated or reordered",
            )?;
        }
    }
    Ok(())
}

pub type StartupPhaseMs = Bounded<0, STARTUP_PHASE_MAX_MS>;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Startup {
    pub config_ms: StartupPhaseMs,
    pub connect_ms: StartupPhaseMs,
    pub ready_ms: StartupPhaseMs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Os {
    Linux,
    Macos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Arch {
    X86_64,
    Aarch64,
}

/// A semantic version, `MAJOR.MINOR.PATCH` with an optional pre-release and
/// build part.
pub type SemVer = Text<SemVerRule>;

pub enum SemVerRule {}

impl TextRule for SemVerRule {
    const NAME: &'static str = "semantic version";
    fn accepts(text: &str) -> bool {
        let (rest, build) = match text.split_once('+') {
            Some((rest, build)) => (rest, Some(build)),
            None => (text, None),
        };
        let (core, pre) = match rest.split_once('-') {
            Some((core, pre)) => (core, Some(pre)),
            None => (rest, None),
        };
        let numeric = |part: &str| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part == "0" || !part.starts_with('0'))
        };
        let alphanumeric = |part: &str| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        };
        let core_parts: Vec<&str> = core.split('.').collect();
        core_parts.len() == 3
            && core_parts.iter().all(|part| numeric(part))
            && pre.is_none_or(|pre| {
                pre.split('.').all(|part| {
                    alphanumeric(part)
                        && (part.bytes().any(|byte| !byte.is_ascii_digit()) || numeric(part))
                })
            })
            && build.is_none_or(|build| build.split('.').all(alphanumeric))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Ready {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub os: Os,
    pub arch: Arch,
    pub cli_version: SemVer,
    pub capabilities: Capabilities,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub lost_changes: Option<LostChanges>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub startup: Option<Startup>,
}

impl Message for Ready {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        let capabilities = &self.capabilities;
        let local_mcp = capabilities.local_mcp.is_some();
        rule(
            !local_mcp || capabilities.tool_execution.is_some(),
            "local_mcp without tool_execution",
        )?;
        if let Some(tool_execution) = &capabilities.tool_execution {
            tool_execution.check(local_mcp)?;
        }
        if let Some(lost_changes) = &self.lost_changes {
            check_lost_changes(lost_changes, LostChangesIn::Ready)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Heartbeat {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub request_message_id: Id,
    pub heartbeat_seq: PositiveInt,
}

impl Message for Heartbeat {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

/// The ACK of a ready or heartbeat: the lease Engine grants, from the
/// message's first send time.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GrantAcknowledgement {
    pub connection_id: Id,
    pub message_id: Id,
    pub accepted: True,
    pub valid_for_ms: Bounded<1, GRANT_MAX_MS>,
}

impl Message for GrantAcknowledgement {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationErrorCode {
    CwdNotFound,
    CwdNotDirectory,
    CwdForbidden,
    CwdInaccessible,
    CwdCheckTimeout,
    InvalidInput,
    StaleConnection,
    BindingConflict,
    CleanupPending,
    ResourceExhausted,
    RepositoryPathExists,
    RepositoryPathForbidden,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PreparationError {
    pub code: PreparationErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryFailureCause {
    Clone,
    NotFound,
    Authentication,
    Forbidden,
    Checkout,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RepositoryFailure {
    pub url: NonEmptyString,
    pub path: AbsolutePath,
    pub cause: RepositoryFailureCause,
    pub message: String,
}

pub type DriveMountFailures = List<DriveMountFailure, 1>;
pub type RepositoryFailures = List<RepositoryFailure, 1>;

/// What a Session report says, each with the members its status allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionOutcome {
    Prepared {
        cwd: AbsolutePath,
        drive_mount_failures: Option<DriveMountFailures>,
        repository_failures: Option<RepositoryFailures>,
    },
    Failed {
        error: PreparationError,
    },
    Closed {
        lost_changes: Option<LostChanges>,
    },
    Applied {
        drive_mount_failures: Option<DriveMountFailures>,
        lost_changes: Option<LostChanges>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "RawSessionStatus", into = "RawSessionStatus")]
pub struct SessionStatus {
    pub connection_id: Id,
    pub message_id: Id,
    pub request_message_id: Id,
    pub session_id: Id,
    pub binding_id: Id,
    pub outcome: SessionOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StatusKind {
    Prepared,
    Failed,
    Closed,
    Applied,
}

#[derive(Deserialize, Serialize)]
struct RawSessionStatus {
    protocol_version: ProtocolVersion,
    connection_id: Id,
    message_id: Id,
    request_message_id: Id,
    session_id: Id,
    binding_id: Id,
    status: StatusKind,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    cwd: Option<AbsolutePath>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    drive_mount_failures: Option<DriveMountFailures>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    repository_failures: Option<RepositoryFailures>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    error: Option<PreparationError>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    lost_changes: Option<LostChanges>,
}

impl TryFrom<RawSessionStatus> for SessionStatus {
    type Error = String;

    fn try_from(raw: RawSessionStatus) -> Result<Self, String> {
        let only = |allowed: bool, member: &str| {
            if allowed {
                Ok(())
            } else {
                Err(format!("{member} is not allowed with this status"))
            }
        };
        let outcome = match raw.status {
            StatusKind::Prepared => {
                only(raw.error.is_none(), "error")?;
                only(raw.lost_changes.is_none(), "lost_changes")?;
                let cwd = raw.cwd.ok_or("a prepared report without cwd")?;
                SessionOutcome::Prepared {
                    cwd,
                    drive_mount_failures: raw.drive_mount_failures,
                    repository_failures: raw.repository_failures,
                }
            }
            StatusKind::Failed => {
                only(raw.cwd.is_none(), "cwd")?;
                only(raw.drive_mount_failures.is_none(), "drive_mount_failures")?;
                only(raw.repository_failures.is_none(), "repository_failures")?;
                only(raw.lost_changes.is_none(), "lost_changes")?;
                SessionOutcome::Failed {
                    error: raw.error.ok_or("a failed report without error")?,
                }
            }
            StatusKind::Closed => {
                only(raw.cwd.is_none(), "cwd")?;
                only(raw.error.is_none(), "error")?;
                only(raw.drive_mount_failures.is_none(), "drive_mount_failures")?;
                only(raw.repository_failures.is_none(), "repository_failures")?;
                SessionOutcome::Closed {
                    lost_changes: raw.lost_changes,
                }
            }
            StatusKind::Applied => {
                only(raw.cwd.is_none(), "cwd")?;
                only(raw.error.is_none(), "error")?;
                only(raw.repository_failures.is_none(), "repository_failures")?;
                SessionOutcome::Applied {
                    drive_mount_failures: raw.drive_mount_failures,
                    lost_changes: raw.lost_changes,
                }
            }
        };
        Ok(SessionStatus {
            connection_id: raw.connection_id,
            message_id: raw.message_id,
            request_message_id: raw.request_message_id,
            session_id: raw.session_id,
            binding_id: raw.binding_id,
            outcome,
        })
    }
}

impl From<SessionStatus> for RawSessionStatus {
    fn from(report: SessionStatus) -> Self {
        let mut raw = RawSessionStatus {
            protocol_version: ProtocolVersion::default(),
            connection_id: report.connection_id,
            message_id: report.message_id,
            request_message_id: report.request_message_id,
            session_id: report.session_id,
            binding_id: report.binding_id,
            status: StatusKind::Prepared,
            cwd: None,
            drive_mount_failures: None,
            repository_failures: None,
            error: None,
            lost_changes: None,
        };
        match report.outcome {
            SessionOutcome::Prepared {
                cwd,
                drive_mount_failures,
                repository_failures,
            } => {
                raw.cwd = Some(cwd);
                raw.drive_mount_failures = drive_mount_failures;
                raw.repository_failures = repository_failures;
            }
            SessionOutcome::Failed { error } => {
                raw.status = StatusKind::Failed;
                raw.error = Some(error);
            }
            SessionOutcome::Closed { lost_changes } => {
                raw.status = StatusKind::Closed;
                raw.lost_changes = lost_changes;
            }
            SessionOutcome::Applied {
                drive_mount_failures,
                lost_changes,
            } => {
                raw.status = StatusKind::Applied;
                raw.drive_mount_failures = drive_mount_failures;
                raw.lost_changes = lost_changes;
            }
        }
        raw
    }
}

impl Message for SessionStatus {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        let (mounts, repositories, lost_changes) = match &self.outcome {
            SessionOutcome::Prepared {
                drive_mount_failures,
                repository_failures,
                ..
            } => (
                drive_mount_failures.as_ref(),
                repository_failures.as_ref(),
                None,
            ),
            SessionOutcome::Failed { .. } => (None, None, None),
            SessionOutcome::Closed { lost_changes } => (None, None, lost_changes.as_ref()),
            SessionOutcome::Applied {
                drive_mount_failures,
                lost_changes,
            } => (drive_mount_failures.as_ref(), None, lost_changes.as_ref()),
        };
        rule(
            mounts.is_none_or(|failures| {
                is_unique(failures.iter().map(|failure| failure.drive_id.as_str()))
            }),
            "drive listed twice in drive_mount_failures",
        )?;
        rule(
            repositories.is_none_or(|failures| {
                is_unique(failures.iter().map(|failure| failure.path.as_str()))
            }),
            "repository listed twice in repository_failures",
        )?;
        if let Some(lost_changes) = lost_changes {
            check_lost_changes(lost_changes, LostChangesIn::Report)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct McpServerInfo {
    pub name: String,
    pub title: Nullable<String>,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpTool {
    pub name: NonEmptyString,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub title: Option<String>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: Object,
    #[serde(
        rename = "outputSchema",
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_schema: Option<Object>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub annotations: Option<Object>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpListErrorCode {
    McpStartFailed,
    McpListFailed,
    McpServerExited,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct McpListError {
    pub code: McpListErrorCode,
    pub message: String,
}

/// The tool list of one local MCP server, numbered per server by `seq`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct McpTools {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub server: NonEmptyString,
    pub seq: PositiveInt,
    pub server_info: Nullable<McpServerInfo>,
    pub tools: List<McpTool, 0, TOOLS_MAX>,
    pub error: Nullable<McpListError>,
}

impl Message for McpTools {
    const MAX_BYTES: usize = TOOL_LIST_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        rule(
            self.error.0.is_none() || self.server_info.0.is_none(),
            "a list error with server_info",
        )?;
        rule(
            (self.error.0.is_none() && self.server_info.0.is_some()) || self.tools.is_empty(),
            "tools without a server",
        )?;
        rule(
            is_unique(self.tools.iter().map(|tool| tool.name.as_str())),
            "tool name twice",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CredentialRequest {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub credential_id: Id,
}

impl Message for CredentialRequest {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

/// A connection key: `tetral_ck_` and the key's text. It never appears in
/// `Debug` or `Display` output.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConnectionKey(Text<KeyRule>);

pub enum KeyRule {}

impl TextRule for KeyRule {
    const NAME: &'static str = "connection key";
    fn accepts(text: &str) -> bool {
        text.strip_prefix(CONNECTION_KEY_PREFIX)
            .is_some_and(|key| !key.is_empty() && !key.contains('\0'))
    }
}

impl ConnectionKey {
    /// The key's plaintext, for the `Authorization` header and the credential
    /// file only.
    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for ConnectionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for ConnectionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CredentialResponse {
    pub credential_id: Id,
    pub connection_key: ConnectionKey,
    pub expires_at: Rfc3339Utc,
}

impl Message for CredentialResponse {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GitTicketRequest {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub binding_id: Id,
}

impl Message for GitTicketRequest {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

/// An `https` origin with no path, query, fragment or user information.
pub type GitProxyUrl = Text<HttpsOrigin>;

pub enum HttpsOrigin {}

impl TextRule for HttpsOrigin {
    const NAME: &'static str = "https origin";
    fn accepts(text: &str) -> bool {
        text.strip_prefix("https://").is_some_and(|host| {
            !host.is_empty()
                && !host.chars().any(|character| {
                    matches!(character, '/' | '?' | '#' | '@') || character.is_whitespace()
                })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct GitTicketResponse {
    pub git_proxy_url: GitProxyUrl,
    pub ticket: NonEmptyString,
}

impl Message for GitTicketResponse {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

/// The ACK of every report but ready and heartbeat.
pub type Acknowledgement = tool::Acknowledgement;
pub type ToolStatus = tool::ToolStatus;
pub type DriveSyncReport = tool::DriveSyncReport;
pub type ProtocolErrorReport = tool::ProtocolErrorReport;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode;

    fn event(protocol_version: i64, kind: &str, payload: Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "protocol_version": protocol_version,
            "connection_id": "conn_a",
            "message_id": "msg_a",
            "type": kind,
            "payload": payload,
        }))
        .unwrap()
    }

    #[test]
    fn an_envelope_reads_any_payload_object_while_a_downstream_reads_only_its_schema() {
        let unsafe_close = event(
            1,
            "session.close",
            serde_json::json!({ "session_id": ".." }),
        );
        let envelope = decode::<Envelope>(&unsafe_close).unwrap();
        assert_eq!(envelope.kind, DownstreamType::SessionClose);
        assert_eq!(envelope.message_id.as_str(), "msg_a");
        assert!(decode::<Downstream>(&unsafe_close).is_err());

        for refused in [
            event(2, "session.close", serde_json::json!({})),
            event(1, "session.open", serde_json::json!({})),
            event(1, "session.close", serde_json::json!([])),
        ] {
            assert!(decode::<Envelope>(&refused).is_err());
        }
    }
}
