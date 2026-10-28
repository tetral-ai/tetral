//! The local API's routes. Each accepted connection is served in its own
//! task, so any number of clients may connect, and a handler reads only what
//! the owners report, never waiting for Engine or the filesystem, so local
//! status stays available while Engine is unreachable. Unknown paths get `404` and known
//! paths with another method `405`, both with an empty body.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use connection::Connection;
use execution::cleanup::Leftovers;
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

/// What a status is read from: the lifecycle task, the Connection owner,
/// the records an earlier App Server left and the fixed fields.
#[derive(Debug, Clone)]
pub struct StatusSources {
    pub lifecycle: Lifecycle,
    pub connection: Connection,
    pub leftovers: Leftovers,
    pub fixed: FixedStatus,
}

pub fn router(sources: StatusSources) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/sessions", get(sessions))
        .route("/v1/shutdown", post(shutdown))
        .with_state(sources)
}

/// The status as of now. A blocked connection keeps its actionable error;
/// otherwise unconfirmed processes take precedence over connection errors.
pub async fn current_status(sources: &StatusSources) -> Status {
    let server_state = sources.lifecycle.server_state().await;
    let connection = sources.connection.status().await;
    assemble_status(sources, server_state, connection)
}

/// A final status read never queues a new owner request after shutdown.
pub fn status_snapshot(
    sources: &StatusSources,
    server_state: protocol::local::ServerState,
) -> Status {
    assemble_status(sources, server_state, sources.connection.status_snapshot())
}

fn assemble_status(
    sources: &StatusSources,
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

async fn status(State(sources): State<StatusSources>) -> Json<Status> {
    Json(current_status(&sources).await)
}

async fn sessions(State(sources): State<StatusSources>) -> Json<Sessions> {
    Json(Sessions {
        sessions: sources.connection.sessions().await,
    })
}

async fn shutdown(State(sources): State<StatusSources>) -> (StatusCode, Json<Value>) {
    sources.lifecycle.shutdown();
    (StatusCode::ACCEPTED, Json(json!({})))
}
