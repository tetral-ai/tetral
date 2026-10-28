//! What the Connection owner reports: the status fields it owns, and the
//! state's record and metrics, written at each change of state.

use observe::{Counter, Gauge, RecordEnum};
use protocol::id::Id;
use protocol::local::{ConnectionState, ErrorObject};
use protocol::time::Rfc3339Utc;

/// The fields of `GET /v1/status` that the Connection owner owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    /// The held connection in `registering`, `online` and `resuming`.
    pub connection_id: Option<Id>,
    pub credential_id: Id,
    pub expires_at: Rfc3339Utc,
    pub last_error: Option<ErrorObject>,
}

const STATES: [ConnectionState; 8] = [
    ConnectionState::Disconnected,
    ConnectionState::Connecting,
    ConnectionState::Registering,
    ConnectionState::Online,
    ConnectionState::Resuming,
    ConnectionState::Blocked,
    ConnectionState::Stopping,
    ConnectionState::Stopped,
];

struct StateName(ConnectionState);

impl RecordEnum for StateName {
    fn record_str(&self) -> &'static str {
        match self.0 {
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Connecting => "connecting",
            ConnectionState::Registering => "registering",
            ConnectionState::Online => "online",
            ConnectionState::Resuming => "resuming",
            ConnectionState::Blocked => "blocked",
            ConnectionState::Stopping => "stopping",
            ConnectionState::Stopped => "stopped",
        }
    }
}

/// How a connection was taken after the first: the same connection on a new
/// stream, or a new connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconnect {
    Resume,
    New,
}

/// The state as the record and the metrics show it.
pub struct Reported {
    state: ConnectionState,
    gauges: Vec<(ConnectionState, Gauge)>,
    resumes: Counter,
    new_connections: Counter,
}

impl Reported {
    /// Starts at `state`, which the gauges show at once.
    pub fn new(state: ConnectionState) -> Reported {
        let registry = observe::registry();
        let gauges = STATES
            .iter()
            .map(|&each| {
                let name = StateName(each).record_str();
                (
                    each,
                    registry.gauge("tetral_connection_state", &[("state", name)]),
                )
            })
            .collect();
        let reported = Reported {
            state,
            gauges,
            resumes: registry.counter("tetral_reconnects_total", &[("kind", "resume")]),
            new_connections: registry.counter("tetral_reconnects_total", &[("kind", "new")]),
        };
        reported.show();
        reported
    }

    /// Records `state` when it differs from the last one.
    pub fn change(&mut self, state: ConnectionState) {
        if state != self.state {
            self.state = state;
            self.show();
            observe::info!(
                "connection.state_changed",
                connection.state = StateName(state)
            );
        }
    }

    pub fn reconnected(&self, kind: Reconnect) {
        match kind {
            Reconnect::Resume => self.resumes.increment(),
            Reconnect::New => self.new_connections.increment(),
        }
    }

    fn show(&self) {
        for (each, gauge) in &self.gauges {
            gauge.set(if *each == self.state { 1.0 } else { 0.0 });
        }
    }
}
