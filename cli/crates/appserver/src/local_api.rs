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
use protocol::field::Nullable;
use protocol::id::Id;
use protocol::local::{Sessions, Status};
use protocol::tool::ProtocolVersion;
use serde_json::{Value, json};

use crate::lifecycle::Lifecycle;

/// The status fields that do not change while the App Server runs.
#[derive(Debug, Clone)]
pub struct FixedStatus {
    pub instance_id: Id,
    pub cli_version: String,
}

/// What a status is read from: the lifecycle task, the Connection owner and
/// the fixed fields.
#[derive(Debug, Clone)]
pub struct StatusSources {
    pub lifecycle: Lifecycle,
    pub connection: Connection,
    pub fixed: FixedStatus,
}

pub fn router(sources: StatusSources) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/sessions", get(sessions))
        .route("/v1/shutdown", post(shutdown))
        .with_state(sources)
}

/// The status as of now.
pub async fn current_status(sources: &StatusSources) -> Status {
    let server_state = sources.lifecycle.server_state().await;
    let connection = sources.connection.status().await;
    Status {
        server_state,
        connection_state: connection.state,
        instance_id: sources.fixed.instance_id.clone(),
        connection_id: Nullable(connection.connection_id),
        credential_id: connection.credential_id,
        expires_at: connection.expires_at,
        cli_version: sources.fixed.cli_version.clone(),
        protocol_version: ProtocolVersion::default(),
        last_error: Nullable(connection.last_error),
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
