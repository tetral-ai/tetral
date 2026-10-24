//! The client error table: every code of a `/v1/client/*` error body with the
//! HTTP status Engine sends it with, and the status and code pairs of each
//! drive transfer endpoint (`DriveTransferEndpoints` of
//! `drive-transfer-v1.schema.json`). A 5xx answer carries no defined body and
//! is never decoded.

use serde::{Deserialize, Serialize};

use crate::connection::{CONNECTION_KEY_PREFIX, MESSAGE_MAX_BYTES};
use crate::drive_transfer;
use crate::field::{self, Bounded, Sha256, Unique};
use crate::{DecodeError, Message, Received, decode, rule};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidMessage,
    ContentHashMismatch,
    CredentialInvalid,
    CredentialExpired,
    CredentialRevoked,
    InstanceUnavailable,
    CliUpgradeRequired,
    DriveAccessDenied,
    NotFound,
    ContentNotFound,
    ConnectionConflict,
    PreviousConnectionActive,
    StaleConnection,
    StaleBinding,
    ContentMissing,
    MessageConflict,
    UnknownInstruction,
    DriveCursorExpired,
    RequestTooLarge,
    FileTooLarge,
    RangeNotSatisfiable,
    RateLimited,
}

impl ErrorCode {
    /// The one HTTP status the code is sent with.
    pub const fn status(self) -> u16 {
        use ErrorCode::*;
        match self {
            InvalidMessage | ContentHashMismatch => 400,
            CredentialInvalid | CredentialExpired | CredentialRevoked => 401,
            InstanceUnavailable | CliUpgradeRequired | DriveAccessDenied => 403,
            NotFound | ContentNotFound => 404,
            ConnectionConflict
            | PreviousConnectionActive
            | StaleConnection
            | StaleBinding
            | ContentMissing
            | MessageConflict
            | UnknownInstruction => 409,
            DriveCursorExpired => 410,
            RequestTooLarge | FileTooLarge => 413,
            RangeNotSatisfiable => 416,
            RateLimited => 429,
        }
    }
}

/// The hashes one `content_missing` error lists, at most the changes of one
/// commit.
pub const MISSING_CONTENT_MAX: usize = drive_transfer::limits::COMMIT_MAX_CHANGES;

/// The longest wait that a `previous_connection_active` error asks for.
pub const RETRY_AFTER_MAX_MS: i64 = 45_000;

/// The body of every `/v1/client/*` error response. `retry_after_ms` comes
/// only with `previous_connection_active` and `missing_sha256` only with
/// `content_missing`, each always with its code.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    /// Displayable text that never holds a connection key.
    pub message: String,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_after_ms: Option<Bounded<0, RETRY_AFTER_MAX_MS>>,
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub missing_sha256: Option<Unique<Sha256, 1, MISSING_CONTENT_MAX>>,
}

impl ErrorBody {
    fn check(&self) -> Result<(), DecodeError> {
        rule(
            self.retry_after_ms.is_some() == (self.code == ErrorCode::PreviousConnectionActive),
            "retry_after_ms exactly with previous_connection_active",
        )?;
        rule(
            self.missing_sha256.is_some() == (self.code == ErrorCode::ContentMissing),
            "missing_sha256 exactly with content_missing",
        )?;
        rule(
            !self.message.contains(CONNECTION_KEY_PREFIX),
            "error message holds a connection key",
        )
    }
}

impl Message for ErrorBody {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        ErrorBody::check(self)
    }
}

/// Reads the error body of a connection endpoint's answer with `status`.
pub fn decode_client_error(status: u16, input: &[u8]) -> Result<ErrorBody, DecodeError> {
    let body: ErrorBody = decode(input)?;
    rule(
        body.code.status() == status,
        "error code not sent with this status",
    )?;
    Ok(body)
}

/// An error body on a drive transfer endpoint. The protocol gives it no size
/// of its own, so the largest drive transfer body bounds it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct DriveTransferErrorBody(pub ErrorBody);

impl Message for DriveTransferErrorBody {
    const MAX_BYTES: usize = drive_transfer::limits::COMMIT_RESPONSE_MAX_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        self.0.check()
    }
}

/// Reads the error body of `endpoint`'s answer with `status`.
pub fn decode_drive_transfer_error(
    endpoint: DriveTransferEndpoint,
    status: u16,
    input: &[u8],
) -> Result<ErrorBody, DecodeError> {
    let DriveTransferErrorBody(body) = decode(input)?;
    rule(
        body.code.status() == status && endpoint.spec().errors.contains(&body.code),
        "error code not sent by this endpoint with this status",
    )?;
    Ok(body)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DriveTransferEndpoint {
    ListFiles,
    ListChanges,
    UploadContent,
    Commit,
    DownloadContent,
}

/// One row of `DriveTransferEndpoints`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointSpec {
    pub method: &'static str,
    /// The path, with `{drive_id}` and `{sha256}` for its parameters.
    pub path: &'static str,
    /// The error codes the endpoint answers with, each with its own status.
    pub errors: &'static [ErrorCode],
}

impl DriveTransferEndpoint {
    pub const ALL: [DriveTransferEndpoint; 5] = [
        DriveTransferEndpoint::ListFiles,
        DriveTransferEndpoint::ListChanges,
        DriveTransferEndpoint::UploadContent,
        DriveTransferEndpoint::Commit,
        DriveTransferEndpoint::DownloadContent,
    ];

    pub const fn spec(self) -> EndpointSpec {
        use ErrorCode::*;
        const ACCESS: [ErrorCode; 8] = [
            InvalidMessage,
            CredentialExpired,
            CredentialInvalid,
            CredentialRevoked,
            DriveAccessDenied,
            InstanceUnavailable,
            StaleBinding,
            StaleConnection,
        ];
        match self {
            DriveTransferEndpoint::ListFiles => EndpointSpec {
                method: "GET",
                path: "/v1/client/drives/{drive_id}/files",
                errors: &ACCESS,
            },
            DriveTransferEndpoint::ListChanges => EndpointSpec {
                method: "GET",
                path: "/v1/client/drives/{drive_id}/changes",
                errors: &[
                    InvalidMessage,
                    CredentialExpired,
                    CredentialInvalid,
                    CredentialRevoked,
                    DriveAccessDenied,
                    InstanceUnavailable,
                    StaleBinding,
                    StaleConnection,
                    DriveCursorExpired,
                ],
            },
            DriveTransferEndpoint::UploadContent => EndpointSpec {
                method: "PUT",
                path: "/v1/client/drives/{drive_id}/content/{sha256}",
                errors: &[
                    ContentHashMismatch,
                    InvalidMessage,
                    CredentialExpired,
                    CredentialInvalid,
                    CredentialRevoked,
                    DriveAccessDenied,
                    InstanceUnavailable,
                    StaleBinding,
                    StaleConnection,
                    FileTooLarge,
                    RateLimited,
                ],
            },
            DriveTransferEndpoint::Commit => EndpointSpec {
                method: "POST",
                path: "/v1/client/drives/{drive_id}/commits",
                errors: &[
                    InvalidMessage,
                    CredentialExpired,
                    CredentialInvalid,
                    CredentialRevoked,
                    DriveAccessDenied,
                    InstanceUnavailable,
                    ContentMissing,
                    StaleBinding,
                    StaleConnection,
                    RequestTooLarge,
                ],
            },
            DriveTransferEndpoint::DownloadContent => EndpointSpec {
                method: "GET",
                path: "/v1/client/drives/{drive_id}/content/{sha256}",
                errors: &[
                    InvalidMessage,
                    CredentialExpired,
                    CredentialInvalid,
                    CredentialRevoked,
                    DriveAccessDenied,
                    InstanceUnavailable,
                    ContentNotFound,
                    StaleBinding,
                    StaleConnection,
                    RangeNotSatisfiable,
                ],
            },
        }
    }
}
