//! The local API's routes. Each accepted connection is
//! served in its own task, so any number of clients may connect, and a
//! handler reads only what the owners report, never waiting for Engine or
//! the filesystem, so local status stays available while Engine is
//! unreachable. The one exception is `POST /v1/computer-use`, which waits for
//! the MCP manager to record the change and apply it. Unknown paths get `404`
//! and known paths with another method `405`, both with an empty body.

use std::path::{Path, PathBuf};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use connection::Connection;
use execution::cleanup::Leftovers;
use mcp::{McpManager, SetError};
use protocol::field::Nullable;
use protocol::id::Id;
use protocol::local::{ConnectionState, ErrorObject, Sessions, Status};
use protocol::tool::ProtocolVersion;
use serde_json::{Value, json};

use crate::lifecycle::Lifecycle;

/// The status fields that do not change while the App Server runs.
#[derive(Debug, Clone)]
pub struct FixedStatus {
    pub instance_id: Id,
    pub cli_version: String,
}

/// What the handlers read and reach: a status is read from the lifecycle
/// task, the Connection owner, the records an earlier App Server left and
/// the fixed fields; a computer-use change goes to the MCP manager.
#[derive(Debug, Clone)]
pub struct Sources {
    pub lifecycle: Lifecycle,
    pub connection: Connection,
    pub leftovers: Leftovers,
    pub fixed: FixedStatus,
    pub mcp: McpManager,
}

pub fn router(sources: Sources) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/sessions", get(sessions))
        .route("/v1/metrics", get(metrics))
        .route("/v1/shutdown", post(shutdown))
        .route("/v1/computer-use", post(computer_use))
        .with_state(sources)
}

/// The status as of now. A blocked connection keeps its actionable error;
/// otherwise unconfirmed processes take precedence over connection errors.
pub async fn current_status(sources: &Sources) -> Status {
    let server_state = sources.lifecycle.server_state().await;
    let connection = sources.connection.status().await;
    assemble_status(sources, server_state, connection)
}

/// A final status read never queues a new owner request after shutdown.
pub fn status_snapshot(sources: &Sources, server_state: protocol::local::ServerState) -> Status {
    assemble_status(sources, server_state, sources.connection.status_snapshot())
}

fn assemble_status(
    sources: &Sources,
    server_state: protocol::local::ServerState,
    connection: connection::ConnectionStatus,
) -> Status {
    Status {
        server_state,
        connection_state: connection.state,
        instance_id: sources.fixed.instance_id.clone(),
        connection_id: Nullable(connection.connection_id),
        credential_id: connection.credential_id,
        expires_at: connection.expires_at,
        cli_version: sources.fixed.cli_version.clone(),
        protocol_version: ProtocolVersion::default(),
        last_error: Nullable(status_error(
            connection.state,
            connection.last_error,
            sources.leftovers.any(),
        )),
        cua_driver: Nullable(sources.mcp.cua_driver()),
    }
}

fn status_error(
    state: ConnectionState,
    error: Option<ErrorObject>,
    cleanup_pending: bool,
) -> Option<ErrorObject> {
    if state == ConnectionState::Blocked && error.is_some() {
        error
    } else if cleanup_pending {
        Some(ErrorObject {
            code: "cleanup_pending".to_owned(),
            message: "processes of an earlier App Server are not yet confirmed gone".to_owned(),
        })
    } else {
        error
    }
}

async fn status(State(sources): State<Sources>) -> Json<Status> {
    Json(current_status(&sources).await)
}

async fn sessions(State(sources): State<Sources>) -> Json<Sessions> {
    Json(Sessions {
        sessions: sources.connection.sessions().await,
    })
}

/// The local metrics in the Prometheus text format 0.0.4, with the startup
/// phases of `observe::STARTUP` once a ready was accepted.
async fn metrics() -> ([(header::HeaderName, &'static str); 1], String) {
    if let Some(phases) = observe::STARTUP.accepted_phases() {
        let registry = observe::registry();
        for (phase, ms) in [
            ("config", phases.config),
            ("connect", phases.connect),
            ("ready", phases.ready),
        ] {
            registry
                .gauge("tetral_startup_phase_seconds", &[("phase", phase)])
                .set(ms.as_u64() as f64 / 1000.0);
        }
    }
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        observe::render(),
    )
}

async fn shutdown(State(sources): State<Sources>) -> (StatusCode, Json<Value>) {
    sources.lifecycle.shutdown();
    (StatusCode::ACCEPTED, Json(json!({})))
}

/// Records and applies Computer Use: `200` with `{}`; `400` with
/// `invalid_arguments` for a body it cannot take, changing nothing; `500`
/// with `driver_failed` when the driver did not start; `500` with
/// `invalid_configuration` or `io_failed` when `config.toml` does not parse
/// or could not be read or written, changing nothing. A request still
/// waiting when shutdown begins gets `500 io_failed`, requiring a later retry
/// and stating that configuration may already have changed.
async fn computer_use(State(sources): State<Sources>, body: Bytes) -> Response {
    let (enabled, driver) = match computer_use_body(&body) {
        Ok(change) => change,
        Err(message) => {
            return failure(StatusCode::BAD_REQUEST, "invalid_arguments", message).into_response();
        }
    };
    let (status, code, message) = match sources.mcp.set_computer_use(enabled, driver).await {
        Ok(()) => return (StatusCode::OK, Json(json!({}))).into_response(),
        Err(SetError::ShuttingDown) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "io_failed",
            "The App Server is shutting down; retry after it has stopped or restarted. The configuration may already have changed.".to_owned(),
        ),
        Err(SetError::InvalidArguments(message)) => {
            (StatusCode::BAD_REQUEST, "invalid_arguments", message)
        }
        Err(SetError::DriverFailed(message)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "driver_failed", message)
        }
        Err(SetError::InvalidConfiguration(message)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_configuration",
            message,
        ),
        Err(SetError::Io(message)) => (StatusCode::INTERNAL_SERVER_ERROR, "io_failed", message),
    };
    failure(status, code, message).into_response()
}

fn failure(status: StatusCode, code: &str, message: String) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "code": code, "message": message })))
}

/// `enabled` and, when it is true, `driver`, from a body that is one JSON
/// object with exactly the members `enabled`, a boolean, and `driver`, an
/// absolute path when `enabled` is true and otherwise a string or `null`.
fn computer_use_body(body: &[u8]) -> Result<(bool, Option<PathBuf>), String> {
    let Ok(Value::Object(members)) = serde_json::from_slice::<Value>(body) else {
        return Err("the body is not one JSON object".to_owned());
    };
    let (Some(enabled), Some(driver), 2) =
        (members.get("enabled"), members.get("driver"), members.len())
    else {
        return Err("the body must have exactly the members enabled and driver".to_owned());
    };
    let Some(enabled) = enabled.as_bool() else {
        return Err("enabled is not a boolean".to_owned());
    };
    match (enabled, driver) {
        (true, Value::String(driver)) if Path::new(driver).is_absolute() => {
            Ok((true, Some(PathBuf::from(driver))))
        }
        (true, _) => Err("driver is not an absolute path".to_owned()),
        (false, Value::String(_) | Value::Null) => Ok((false, None)),
        (false, _) => Err("driver is not a string or null".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_reasons_remain_visible_without_clearing_cleanup_restrictions() {
        for code in ["credential_expired", "credential_revoked", "protocol_error"] {
            let error = Some(ErrorObject {
                code: code.into(),
                message: "credential unavailable".into(),
            });
            assert_eq!(
                status_error(ConnectionState::Blocked, error.clone(), true),
                error
            );
            assert_eq!(
                status_error(ConnectionState::Blocked, error.clone(), false),
                error
            );
        }
        for state in [ConnectionState::Online, ConnectionState::Blocked] {
            assert_eq!(
                status_error(state, None, true).unwrap().code,
                "cleanup_pending"
            );
        }
        let transient = Some(ErrorObject {
            code: "network_error".into(),
            message: "retrying".into(),
        });
        assert_eq!(
            status_error(ConnectionState::Disconnected, transient, true)
                .unwrap()
                .code,
            "cleanup_pending"
        );
        assert_eq!(status_error(ConnectionState::Online, None, false), None);
    }
}

#[cfg(test)]
mod transfer_tests;
