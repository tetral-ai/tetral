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
pub struct Identity {
    pub instance_id: Id,
    pub credential_id: Id,
    pub expires_at: Rfc3339Utc,
    pub cli_version: String,
}

#[derive(Debug, Clone)]
struct Served {
    lifecycle: Lifecycle,
    identity: Identity,
}

pub fn router(lifecycle: Lifecycle, identity: Identity) -> Router {
    Router::new()
        .route("/v1/status", get(status))
        .route("/v1/sessions", get(sessions))
        .route("/v1/shutdown", post(shutdown))
        .with_state(Served {
            lifecycle,
            identity,
        })
}

/// The status as of now.
pub async fn current_status(lifecycle: &Lifecycle, identity: &Identity) -> Status {
    let states = lifecycle.states().await;
    Status {
        server_state: states.server,
        connection_state: states.connection,
        instance_id: identity.instance_id.clone(),
        connection_id: Nullable(None),
        credential_id: identity.credential_id.clone(),
        expires_at: identity.expires_at.clone(),
        cli_version: identity.cli_version.clone(),
        protocol_version: ProtocolVersion::default(),
        last_error: Nullable(None),
    }
}

async fn status(State(served): State<Served>) -> Json<Status> {
    Json(current_status(&served.lifecycle, &served.identity).await)
}

async fn sessions() -> Json<Sessions> {
    Json(Sessions {
        sessions: Vec::new(),
    })
}

async fn shutdown(State(served): State<Served>) -> (StatusCode, Json<Value>) {
    served.lifecycle.shutdown();
    (StatusCode::ACCEPTED, Json(json!({})))
}
