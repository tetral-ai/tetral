//! The tool execution messages: the payloads of `tool.execute`, `tool.stdin`,
//! `tool.cancel` and `mcp.call`, `sandbox_policy`, tool-status and drive sync
//! reports, acknowledgements, protocol-error reports and the `tool_execution`
//! and `local_mcp` fragments of a ready's capabilities
//! (`tool-execution-v1.schema.json`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::field::{
    self, Absent, AbsolutePath, Bounded, ConstInt, Count, List, Maybe, NonEmptyString, NulFree,
    Nullable, PositiveInt, Text, True, Unique,
};
use crate::id::Id;
use crate::{DecodeError, Message, Received, rule};

pub type ProtocolVersion = ConstInt<{ crate::PROTOCOL_VERSION }>;
pub type SchemaVersion = ConstInt<1>;
pub type Object = Map<String, Value>;

/// The byte limits of the tool messages, which a ready declares as
/// `TransportLimits`.
pub mod limits {
    pub const METADATA_JSON_BYTES: usize = 65_536;
    pub const TOOL_INPUT_JSON_BYTES: usize = 4_194_304;
    pub const EXECUTE_SSE_JSON_BYTES: usize = 4_259_840;
    pub const STDIN_UTF8_BYTES: usize = 32_768;
    pub const STDIN_SSE_JSON_BYTES: usize = 262_144;
    pub const DEFAULT_MESSAGE_JSON_BYTES: usize = 65_536;
    pub const HELPER_RESULT_JSON_BYTES: usize = 262_144;
    pub const DRIVE_SYNC_JSON_BYTES: usize = 65_536;
    pub const ORDINARY_TOOL_STATUS_JSON_BYTES: usize = 327_680;
    pub const DRIVE_SYNC_REPORT_JSON_BYTES: usize = 131_072;
    pub const VIEW_IMAGE_DECODED_BYTES: usize = 10_485_760;
    pub const MEDIA_TOOL_STATUS_JSON_BYTES: usize = 14_680_064;
    /// A complete `mcp.call` event.
    pub const MCP_CALL_SSE_JSON_BYTES: usize = 131_072;
}

/// The paths that tool inputs name: nonempty, without U+0000.
pub type ToolPath = Text<NulFree>;

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct VisibleLimits {
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub visible_bytes: Maybe<i64>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub visible_lines: Maybe<i64>,
}

/// An `exec_command` command: text with at least one character that is not
/// white space.
pub type Command = Text<HasText>;

pub enum HasText {}

impl field::TextRule for HasText {
    const NAME: &'static str = "command with a character that is not white space";
    fn accepts(text: &str) -> bool {
        text.chars().any(|character| !character.is_whitespace())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnWaitExpiry {
    #[serde(rename = "")]
    Unset,
    Kill,
    Detach,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExecInput {
    pub cmd: Command,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub cwd: Maybe<String>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub env: Maybe<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub wait_ms: Maybe<i64>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub on_wait_expiry: Maybe<OnWaitExpiry>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub task_lifetime_ms: Maybe<i64>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub task_id: Maybe<Id>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EditInput {
    pub path: ToolPath,
    pub old_string: NonEmptyString,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub new_string: Maybe<String>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub replace_all: Maybe<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ViewImageInput {
    pub path: ToolPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemAccess {
    Deny,
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemRule {
    pub path: AbsolutePath,
    pub access: FilesystemAccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemPolicy {
    pub rules: Vec<FilesystemRule>,
}

/// The highest TCP or UDP port.
pub const PORT_MAX: i64 = 65_535;

pub type Port = Bounded<1, PORT_MAX>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    pub from: Port,
    pub to: Port,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum NetworkDestination {
    Host(NonEmptyString),
    Cidr(NonEmptyString),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkRule {
    pub destination: NetworkDestination,
    pub transport: Transport,
    pub ports: List<PortRange, 1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkListen {
    pub address: NonEmptyString,
    pub transport: Transport,
    pub ports: List<PortRange, 1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedNetworkRules {
    pub allow: Vec<NetworkRule>,
    pub deny: Vec<NetworkRule>,
    pub listen: Vec<NetworkListen>,
}

// Struct variants throughout: serde reads a unit variant of an internally
// tagged enum without looking at its other members, so `deny_unknown_fields`
// would not hold for them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkPolicy {
    Disabled {},
    Direct {},
    Managed { rules: ManagedNetworkRules },
}

/// The permissions of one call. It is a closed object at every level, so a
/// misspelled member is refused rather than read as a grant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxPolicy {
    pub schema_version: SchemaVersion,
    pub filesystem: FilesystemPolicy,
    pub network: NetworkPolicy,
}

impl Message for SandboxPolicy {
    const MAX_BYTES: usize = limits::DEFAULT_MESSAGE_JSON_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "tool", content = "input", rename_all = "snake_case")]
pub enum ToolInput {
    ExecCommand(ExecInput),
    Edit(EditInput),
    ViewImage(ViewImageInput),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExecutePayload {
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    #[serde(flatten)]
    pub tool: ToolInput,
    pub sandbox_policy: SandboxPolicy,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub limits: Option<VisibleLimits>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StdinPayload {
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    pub chars: String,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub write_seq: Option<PositiveInt>,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub wait_ms: Maybe<i64>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub limits: Option<VisibleLimits>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CancelPayload {
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    #[serde(default, skip_serializing_if = "Maybe::is_absent")]
    pub reason: Maybe<String>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub limits: Option<VisibleLimits>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct McpCallPayload {
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    pub server: NonEmptyString,
    pub tool: NonEmptyString,
    pub arguments: Object,
    #[serde(default, skip_serializing)]
    pub sandbox_policy: Absent,
    #[serde(default, skip_serializing)]
    pub limits: Absent,
}

/// The encoded size limit of each tool instruction type, and a `tool.stdin`
/// that writes only with its `write_seq`.
pub(crate) fn check_instruction(
    instruction: &ToolInstruction<'_>,
    bytes: usize,
) -> Result<(), DecodeError> {
    let limit = match instruction {
        ToolInstruction::Execute(_) => limits::EXECUTE_SSE_JSON_BYTES,
        ToolInstruction::Stdin(stdin) => {
            rule(
                stdin.chars.is_empty() || stdin.write_seq.is_some(),
                "a nonempty tool.stdin without write_seq",
            )?;
            limits::STDIN_SSE_JSON_BYTES
        }
        ToolInstruction::Cancel(_) => limits::DEFAULT_MESSAGE_JSON_BYTES,
        ToolInstruction::McpCall(_) => limits::MCP_CALL_SSE_JSON_BYTES,
    };
    rule(bytes <= limit, "tool instruction over its limit")
}

/// The payload of a tool instruction, borrowed from its envelope.
pub enum ToolInstruction<'a> {
    Execute(&'a ExecutePayload),
    Stdin(&'a StdinPayload),
    Cancel(&'a CancelPayload),
    McpCall(&'a McpCallPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StreamSnapshot {
    pub text: String,
    pub total_bytes: Count,
    pub total_lines: Count,
    pub returned_bytes: Count,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExecResult {
    pub task_id: Nullable<Id>,
    pub exit_code: Nullable<i64>,
    pub signal: Nullable<String>,
    pub timed_out: bool,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cancelled: Option<bool>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub was_running: Option<bool>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub written_bytes: Option<Count>,
    pub stdout: StreamSnapshot,
    pub stderr: StreamSnapshot,
    pub duration_ms: Count,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum ImageMime {
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/gif")]
    Gif,
    #[serde(rename = "image/webp")]
    Webp,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MediaResult {
    pub mime: ImageMime,
    pub size_bytes: Count,
    pub data_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EditResult {
    pub replacements: Count,
    pub bytes_written: Count,
}

/// An MCP `tools/call` result as the CLI forwards it, without `_meta`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct McpResult {
    pub content: Vec<Object>,
    #[serde(
        rename = "structuredContent",
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub structured_content: Option<Object>,
    #[serde(
        rename = "isError",
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub is_error: Option<bool>,
    #[serde(rename = "_meta", default, skip_serializing)]
    pub meta: Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ToolError {
    pub kind: NonEmptyString,
    pub message: String,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub path: Option<String>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub detail: Option<Object>,
}

/// The error kinds of an `mcp` result, each with the `detail` it requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpToolError {
    /// `detail` `{code, message}`: the server's JSON-RPC error.
    McpError,
    McpTimeout,
    McpServerExited,
    /// `detail` `{sent}`: whether the call reached the server.
    Cancelled,
    /// `detail` `{media_bytes, envelope_bytes, report_bytes}`.
    McpResultTooLarge,
    McpInvalidResult,
}

impl McpToolError {
    pub fn of(error: &ToolError) -> Result<McpToolError, DecodeError> {
        let detail = error.detail.as_ref();
        let has = |name: &str, integer: bool| {
            detail
                .and_then(|detail| detail.get(name))
                .is_some_and(|value| if integer { value.is_i64() } else { true })
        };
        let kind = match error.kind.as_str() {
            "mcp_error" => {
                rule(
                    has("code", true)
                        && detail
                            .and_then(|detail| detail.get("message"))
                            .is_some_and(Value::is_string),
                    "mcp_error without detail code and message",
                )?;
                McpToolError::McpError
            }
            "mcp_timeout" => McpToolError::McpTimeout,
            "mcp_server_exited" => McpToolError::McpServerExited,
            "cancelled" => {
                rule(
                    detail
                        .and_then(|detail| detail.get("sent"))
                        .is_some_and(Value::is_boolean),
                    "cancelled without detail sent",
                )?;
                McpToolError::Cancelled
            }
            "mcp_result_too_large" => {
                let counted = ["media_bytes", "envelope_bytes", "report_bytes"]
                    .iter()
                    .all(|name| {
                        detail
                            .and_then(|detail| detail.get(*name))
                            .and_then(Value::as_i64)
                            .is_some_and(|bytes| bytes >= 0)
                    });
                rule(counted, "mcp_result_too_large without its byte counts")?;
                McpToolError::McpResultTooLarge
            }
            "mcp_invalid_result" => McpToolError::McpInvalidResult,
            _ => return Err(DecodeError::Rule("not an error kind of an mcp result")),
        };
        Ok(kind)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProtocolError<C> {
    pub code: C,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionCode {
    InvalidInput,
    TaskConflict,
    UnsupportedPolicy,
    SandboxUnavailable,
    BindingUnavailable,
    ResourceExhausted,
    McpServerUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlErrorCode {
    UnsupportedControl,
    TaskNotFound,
    WriteConflict,
    InvalidInput,
    ResultUnavailable,
    StdinClosed,
    ResourceExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    MessageConflict,
    InvalidMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DriveWriter {
    ApiActor { api_key_id: Id },
    UserActor { user_id: Id },
    ServiceAccountActor { service_account_id: Id },
    SessionActor { session_id: Id, computer_id: Id },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Write,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    Conflict,
    PathRule,
    Limit,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriveRejection {
    pub path: AbsolutePath,
    pub change: ChangeKind,
    pub reason: RejectionReason,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub rule: Option<NonEmptyString>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub current_version_id: Option<Id>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub current_writer: Option<DriveWriter>,
}

impl DriveRejection {
    fn check(&self) -> Result<(), DecodeError> {
        match self.reason {
            RejectionReason::PathRule | RejectionReason::Limit => rule(
                self.rule.is_some(),
                "a path_rule or limit rejection without rule",
            ),
            RejectionReason::Conflict => rule(
                self.current_version_id.is_some() == self.current_writer.is_some(),
                "a conflict with only one of current_version_id and current_writer",
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotSyncedReason {
    GitRepository,
    Symlink,
    SpecialFile,
    Uploading,
    UploadPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriveNotSynced {
    pub path: AbsolutePath,
    pub reason: NotSyncedReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriveDownload {
    pub path: AbsolutePath,
    pub downloaded_bytes: Count,
    pub size_bytes: Count,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriveMountFailure {
    pub drive_id: Id,
    pub error: NonEmptyString,
}

/// The listed entries of each kind; a `_total` member counts all of them when
/// more exist than are listed.
pub const DRIVE_SYNC_LISTED_MAX: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DriveSyncOutcome {
    pub committed: Count,
    pub deleted: Count,
    pub rejected: List<DriveRejection, 0, DRIVE_SYNC_LISTED_MAX>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub rejected_total: Option<PositiveInt>,
    pub not_synced: List<DriveNotSynced, 0, DRIVE_SYNC_LISTED_MAX>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub not_synced_total: Option<PositiveInt>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub downloading: Option<List<DriveDownload, 0, DRIVE_SYNC_LISTED_MAX>>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub downloading_total: Option<PositiveInt>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub unmounted: Option<Vec<DriveMountFailure>>,
}

impl DriveSyncOutcome {
    fn check(&self) -> Result<(), DecodeError> {
        rule(
            self.committed.get() > 0
                || self.deleted.get() > 0
                || !self.rejected.is_empty()
                || !self.not_synced.is_empty()
                || self
                    .downloading
                    .as_ref()
                    .is_some_and(|listed| !listed.is_empty())
                || self.rejected_total.is_some()
                || self.not_synced_total.is_some()
                || self.downloading_total.is_some()
                || self
                    .unmounted
                    .as_ref()
                    .is_some_and(|listed| !listed.is_empty()),
            "a drive_sync outcome that reports nothing",
        )?;
        for rejection in self.rejected.iter() {
            rejection.check()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HelperTool {
    ExecCommand,
    Edit,
    ViewImage,
    Mcp,
    Stdin,
    Cancel,
}

impl HelperTool {
    fn runs_a_process(self) -> bool {
        matches!(
            self,
            HelperTool::ExecCommand | HelperTool::Stdin | HelperTool::Cancel
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HelperStatus {
    Success,
    Error,
    Running,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Metrics {
    pub wall_time_ms: Count,
    pub queued_ms: Count,
    pub drive_wait_ms: Count,
    pub run_ms: Count,
    pub sync_ms: Count,
}

/// A result envelope's `result`, typed by its tool, status and `truncated`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ToolResult {
    Exec(ExecResult),
    Edit(EditResult),
    Media(MediaResult),
    Mcp(McpResult),
    /// An error's or a truncated result's object, which carries no fixed
    /// members.
    Object(Object),
}

/// The result envelope of one tool call.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "RawEnvelope", into = "RawEnvelope")]
pub struct HelperEnvelope {
    pub tool: HelperTool,
    pub status: HelperStatus,
    pub truncated: bool,
    pub error: Option<ToolError>,
    pub result: ToolResult,
    pub metrics: Option<Metrics>,
    pub drive_sync: Option<DriveSyncOutcome>,
}

#[derive(Deserialize, Serialize)]
struct RawEnvelope {
    schema_version: SchemaVersion,
    tool: HelperTool,
    status: HelperStatus,
    truncated: bool,
    error: Nullable<ToolError>,
    result: Object,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    metrics: Option<Metrics>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    drive_sync: Option<DriveSyncOutcome>,
}

impl TryFrom<RawEnvelope> for HelperEnvelope {
    type Error = String;

    fn try_from(raw: RawEnvelope) -> Result<Self, String> {
        let parse = |result: Object, kind: fn(Value) -> Result<ToolResult, serde_json::Error>| {
            kind(Value::Object(result)).map_err(|error| error.to_string())
        };
        let (tool, status, truncated) = (raw.tool, raw.status, raw.truncated);
        if (status == HelperStatus::Error) != raw.error.0.is_some() {
            return Err(
                "status error without an error, or an error without status error".to_owned(),
            );
        }
        if status == HelperStatus::Running && !tool.runs_a_process() {
            return Err("status running for a tool that runs no process".to_owned());
        }
        if tool == HelperTool::Mcp && (truncated || raw.drive_sync.is_some()) {
            return Err("an mcp result that is truncated or has drive_sync".to_owned());
        }
        let finished_or_running = matches!(status, HelperStatus::Success | HelperStatus::Running);
        let result = match tool {
            _ if tool.runs_a_process() && finished_or_running => parse(raw.result, |value| {
                serde_json::from_value(value).map(ToolResult::Exec)
            })?,
            HelperTool::Edit if status == HelperStatus::Success && !truncated => {
                parse(raw.result, |value| {
                    serde_json::from_value(value).map(ToolResult::Edit)
                })?
            }
            HelperTool::ViewImage if status == HelperStatus::Success && !truncated => {
                parse(raw.result, |value| {
                    serde_json::from_value(value).map(ToolResult::Media)
                })?
            }
            HelperTool::Mcp if status == HelperStatus::Success => parse(raw.result, |value| {
                serde_json::from_value(value).map(ToolResult::Mcp)
            })?,
            HelperTool::Mcp => {
                if !raw.result.is_empty() {
                    return Err("an mcp error with a nonempty result".to_owned());
                }
                if let Some(error) = &raw.error.0 {
                    McpToolError::of(error).map_err(|error| error.to_string())?;
                }
                ToolResult::Object(Object::new())
            }
            _ => ToolResult::Object(raw.result),
        };
        Ok(HelperEnvelope {
            tool,
            status,
            truncated,
            error: raw.error.0,
            result,
            metrics: raw.metrics,
            drive_sync: raw.drive_sync,
        })
    }
}

impl From<HelperEnvelope> for RawEnvelope {
    fn from(envelope: HelperEnvelope) -> Self {
        let result = match serde_json::to_value(&envelope.result) {
            Ok(Value::Object(result)) => result,
            _ => Object::new(),
        };
        RawEnvelope {
            schema_version: ConstInt,
            tool: envelope.tool,
            status: envelope.status,
            truncated: envelope.truncated,
            error: Nullable(envelope.error),
            result,
            metrics: envelope.metrics,
            drive_sync: envelope.drive_sync,
        }
    }
}

/// The identities every tool-status report carries.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReportHeader {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub session_id: Id,
    pub binding_id: Id,
    pub tool_use_event_id: Id,
    pub task_id: Id,
    pub request_message_id: Id,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportPhase {
    Running,
    Terminal,
}

/// A report on a `tool.execute` or `mcp.call`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionReport {
    /// A `running` or `terminal` result, numbered by `report_seq` from 1.
    Result {
        header: ReportHeader,
        phase: ReportPhase,
        report_seq: PositiveInt,
        result: Box<HelperEnvelope>,
    },
    Rejected {
        header: ReportHeader,
        error: ProtocolError<RejectionCode>,
    },
}

/// A report on a `tool.stdin` or `tool.cancel`: its result, its error, or
/// both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlReport {
    pub header: ReportHeader,
    pub result: Option<Box<HelperEnvelope>>,
    pub error: Option<ProtocolError<ControlErrorCode>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "RawToolStatus", into = "RawToolStatus")]
pub enum ToolStatus {
    Execution(ExecutionReport),
    Control(ControlReport),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReportKind {
    Execution,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum AnyPhase {
    Running,
    Terminal,
    Rejected,
}

#[derive(Deserialize, Serialize)]
struct RawToolStatus {
    #[serde(flatten)]
    header: ReportHeader,
    kind: ReportKind,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    phase: Option<AnyPhase>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    report_seq: Option<PositiveInt>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    result: Option<HelperEnvelope>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    error: Option<ProtocolError<String>>,
}

fn coded<C: serde::de::DeserializeOwned>(
    error: ProtocolError<String>,
) -> Result<ProtocolError<C>, String> {
    if error.code.is_empty() {
        return Err("an error code is empty".to_owned());
    }
    let code =
        serde_json::from_value(Value::String(error.code)).map_err(|error| error.to_string())?;
    Ok(ProtocolError {
        code,
        message: error.message,
    })
}

impl TryFrom<RawToolStatus> for ToolStatus {
    type Error = String;

    fn try_from(raw: RawToolStatus) -> Result<Self, String> {
        let header = raw.header;
        match (raw.kind, raw.phase) {
            (ReportKind::Execution, Some(AnyPhase::Running | AnyPhase::Terminal)) => {
                let phase = if raw.phase == Some(AnyPhase::Running) {
                    ReportPhase::Running
                } else {
                    ReportPhase::Terminal
                };
                let (Some(report_seq), Some(result), None) =
                    (raw.report_seq, raw.result, raw.error)
                else {
                    return Err(
                        "a result report without report_seq or result, or with an error".to_owned(),
                    );
                };
                if phase == ReportPhase::Terminal && result.metrics.is_none() {
                    return Err("a terminal result without metrics".to_owned());
                }
                Ok(ToolStatus::Execution(ExecutionReport::Result {
                    header,
                    phase,
                    report_seq,
                    result: Box::new(result),
                }))
            }
            (ReportKind::Execution, Some(AnyPhase::Rejected)) => {
                let (None, None, Some(error)) = (raw.report_seq, raw.result, raw.error) else {
                    return Err(
                        "a rejection without an error, or with report_seq or result".to_owned()
                    );
                };
                Ok(ToolStatus::Execution(ExecutionReport::Rejected {
                    header,
                    error: coded(error)?,
                }))
            }
            (ReportKind::Execution, None) => Err("an execution report without a phase".to_owned()),
            (ReportKind::Control, None) => {
                if raw.report_seq.is_some() {
                    return Err("a control report with report_seq".to_owned());
                }
                if raw.result.is_none() && raw.error.is_none() {
                    return Err("a control report with neither result nor error".to_owned());
                }
                Ok(ToolStatus::Control(ControlReport {
                    header,
                    result: raw.result.map(Box::new),
                    error: raw.error.map(coded).transpose()?,
                }))
            }
            (ReportKind::Control, Some(_)) => Err("a control report with a phase".to_owned()),
        }
    }
}

impl From<ToolStatus> for RawToolStatus {
    fn from(status: ToolStatus) -> Self {
        let uncoded = |code: Value, message: String| ProtocolError {
            code: code.as_str().unwrap_or_default().to_owned(),
            message,
        };
        match status {
            ToolStatus::Execution(ExecutionReport::Result {
                header,
                phase,
                report_seq,
                result,
            }) => RawToolStatus {
                header,
                kind: ReportKind::Execution,
                phase: Some(match phase {
                    ReportPhase::Running => AnyPhase::Running,
                    ReportPhase::Terminal => AnyPhase::Terminal,
                }),
                report_seq: Some(report_seq),
                result: Some(*result),
                error: None,
            },
            ToolStatus::Execution(ExecutionReport::Rejected { header, error }) => RawToolStatus {
                header,
                kind: ReportKind::Execution,
                phase: Some(AnyPhase::Rejected),
                report_seq: None,
                result: None,
                error: Some(uncoded(
                    serde_json::to_value(error.code).unwrap_or_default(),
                    error.message,
                )),
            },
            ToolStatus::Control(report) => RawToolStatus {
                header: report.header,
                kind: ReportKind::Control,
                phase: None,
                report_seq: None,
                result: report.result.map(|result| *result),
                error: report.error.map(|error| {
                    uncoded(
                        serde_json::to_value(error.code).unwrap_or_default(),
                        error.message,
                    )
                }),
            },
        }
    }
}

impl ToolStatus {
    pub fn header(&self) -> &ReportHeader {
        match self {
            ToolStatus::Execution(ExecutionReport::Result { header, .. })
            | ToolStatus::Execution(ExecutionReport::Rejected { header, .. }) => header,
            ToolStatus::Control(report) => &report.header,
        }
    }

    fn envelope(&self) -> Option<&HelperEnvelope> {
        match self {
            ToolStatus::Execution(ExecutionReport::Result { result, .. }) => Some(result),
            ToolStatus::Execution(ExecutionReport::Rejected { .. }) => None,
            ToolStatus::Control(report) => report.result.as_deref(),
        }
    }
}

impl Message for ToolStatus {
    /// Whether a report carries media, and so falls under this limit rather
    /// than `ORDINARY_TOOL_STATUS_JSON_BYTES`, is the budget of the side that
    /// builds it; reading bounds every report by the larger limit.
    const MAX_BYTES: usize = limits::MEDIA_TOOL_STATUS_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        match self
            .envelope()
            .and_then(|envelope| envelope.drive_sync.as_ref())
        {
            Some(outcome) => outcome.check(),
            None => Ok(()),
        }
    }
}

/// The drive outcome of a sync point that no tool-status report carries.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DriveSyncReport {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    pub session_id: Id,
    pub binding_id: Id,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub task_id: Option<Id>,
    pub drive_sync: DriveSyncOutcome,
}

impl Message for DriveSyncReport {
    const MAX_BYTES: usize = limits::DRIVE_SYNC_REPORT_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        self.drive_sync.check()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Acknowledgement {
    pub connection_id: Id,
    pub message_id: Id,
    pub accepted: True,
}

impl Message for Acknowledgement {
    const MAX_BYTES: usize = limits::DEFAULT_MESSAGE_JSON_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProtocolErrorReport {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub message_id: Id,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_message_id: Option<Id>,
    pub error: ProtocolError<ProtocolErrorCode>,
}

impl Message for ProtocolErrorReport {
    const MAX_BYTES: usize = limits::DEFAULT_MESSAGE_JSON_BYTES;
}

/// The byte limits a ready declares; every member is fixed to its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransportLimits {
    pub metadata_json_bytes: ConstInt<{ limits::METADATA_JSON_BYTES as i64 }>,
    pub tool_input_json_bytes: ConstInt<{ limits::TOOL_INPUT_JSON_BYTES as i64 }>,
    pub execute_sse_json_bytes: ConstInt<{ limits::EXECUTE_SSE_JSON_BYTES as i64 }>,
    pub stdin_utf8_bytes: ConstInt<{ limits::STDIN_UTF8_BYTES as i64 }>,
    pub stdin_sse_json_bytes: ConstInt<{ limits::STDIN_SSE_JSON_BYTES as i64 }>,
    pub default_message_json_bytes: ConstInt<{ limits::DEFAULT_MESSAGE_JSON_BYTES as i64 }>,
    pub helper_result_json_bytes: ConstInt<{ limits::HELPER_RESULT_JSON_BYTES as i64 }>,
    pub drive_sync_json_bytes: ConstInt<{ limits::DRIVE_SYNC_JSON_BYTES as i64 }>,
    pub ordinary_tool_status_json_bytes:
        ConstInt<{ limits::ORDINARY_TOOL_STATUS_JSON_BYTES as i64 }>,
    pub drive_sync_report_json_bytes: ConstInt<{ limits::DRIVE_SYNC_REPORT_JSON_BYTES as i64 }>,
    pub view_image_decoded_bytes: ConstInt<{ limits::VIEW_IMAGE_DECODED_BYTES as i64 }>,
    pub media_tool_status_json_bytes: ConstInt<{ limits::MEDIA_TOOL_STATUS_JSON_BYTES as i64 }>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedNetworkCapability {
    pub transports: Unique<Transport>,
    pub listen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTool {
    ExecCommand,
    Edit,
    ViewImage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    Stdin,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    Disabled,
    Direct,
    Managed,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ToolExecutionCapability {
    pub schema_version: SchemaVersion,
    pub tools: Unique<ExecutionTool>,
    pub controls: Unique<Control>,
    pub sandbox_policy_versions: Unique<SchemaVersion>,
    pub network_modes: Unique<NetworkMode>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub managed_network: Option<ManagedNetworkCapability>,
    pub limits: TransportLimits,
}

impl ToolExecutionCapability {
    /// `local_mcp` is whether the ready also declares local MCP. A ready or
    /// a `ReadyExtension` declares tools, local MCP or both, each with the
    /// controls it needs.
    pub(crate) fn check(&self, local_mcp: bool) -> Result<(), DecodeError> {
        rule(
            self.network_modes.contains(&NetworkMode::Managed) == self.managed_network.is_some(),
            "managed_network exactly when the managed network mode is declared",
        )?;
        rule(
            !self.tools.is_empty() || local_mcp,
            "tool_execution without tools or local_mcp",
        )?;
        let cancel = self.controls.contains(&Control::Cancel);
        rule(
            self.tools.is_empty() || cancel,
            "every tool requires cancel",
        )?;
        rule(
            !self.tools.contains(&ExecutionTool::ExecCommand)
                || self.controls.contains(&Control::Stdin),
            "exec_command requires stdin",
        )?;
        rule(!local_mcp || cancel, "local MCP requires cancel")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LocalMcpCapability {
    pub schema_version: SchemaVersion,
}

/// The tool execution part of a ready's capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReadyExtension {
    pub tool_execution: ToolExecutionCapability,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_mcp: Option<LocalMcpCapability>,
}

impl Message for ReadyExtension {
    const MAX_BYTES: usize = limits::DEFAULT_MESSAGE_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        self.tool_execution.check(self.local_mcp.is_some())
    }
}
