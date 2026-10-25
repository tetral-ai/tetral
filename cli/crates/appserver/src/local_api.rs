//! The local API's routes. Each accepted connection is served in its own
//! task, so any number of clients may connect, and a handler reads only what
//! the owners report, never waiting for Engine, so local status stays
//! available while Engine is unreachable. Unknown paths get `404` and known
//! paths with another method `405`, both with an empty body.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use protocol::field::Nullable;
use protocol::id::Id;
use protocol::local::{Sessions, Status};
use protocol::time::Rfc3339Utc;
use protocol::tool::ProtocolVersion;
use serde_json::{Value, json};

use crate::lifecycle::Lifecycle;

/// The status fields that do not change while the App Server runs.
#[derive(Debug, Clone)]
pub struct FixedStatus {
    pub instance_id: Id,
    pub credential_id: Id,
    pub expires_at: Rfc3339Utc,
    pub cli_version: String,
}

#[derive(Debug, Clone)]
struct ApiState {
    lifecycle: Lifecycle,
    fixed: FixedStatus,
}

pub fn router(lifecycle: Lifecycle, fixed: FixedStatus) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/sessions", get(sessions))
        .route("/v1/shutdown", post(shutdown))
        .with_state(ApiState { lifecycle, fixed })
}

/// The status as of now.
pub async fn current_status(lifecycle: &Lifecycle, fixed: &FixedStatus) -> Status {
    let states = lifecycle.states().await;
    Status {
        server_state: states.server,
        connection_state: states.connection,
        instance_id: fixed.instance_id.clone(),
        connection_id: Nullable(None),
        credential_id: fixed.credential_id.clone(),
        expires_at: fixed.expires_at.clone(),
        cli_version: fixed.cli_version.clone(),
        protocol_version: ProtocolVersion::default(),
        last_error: Nullable(None),
    }
}

async fn status(State(api): State<ApiState>) -> Json<Status> {
    Json(current_status(&api.lifecycle, &api.fixed).await)
}

async fn sessions() -> Json<Sessions> {
    Json(Sessions {
        sessions: Vec::new(),
    })
}

async fn shutdown(State(api): State<ApiState>) -> (StatusCode, Json<Value>) {
    api.lifecycle.shutdown();
    (StatusCode::ACCEPTED, Json(json!({})))
}
