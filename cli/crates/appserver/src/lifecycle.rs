//! The lifecycle task: the owner of whether the App Server is running or
//! stopping. The first shutdown trigger, `POST /v1/shutdown`, SIGINT or
//! SIGTERM, moves it to stopping and starts the shutdown sequence
//! (`shutdown`); later triggers change nothing. The task keeps answering
//! while the sequence runs, so status stays available until the listener
//! stops. [`run`] serves a started App Server until the sequence ends.

use std::time::Duration;

use protocol::local::{ConnectionState, ServerState};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use protocol::local::Status;
use tokio::signal::unix::{SignalKind, signal};

use crate::local_api::{self, Identity};
use crate::lock::RootLock;
use crate::shutdown::{self, Sequence};
use crate::startup::{self, Started};

/// The states the lifecycle task reports for `GET /v1/status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct States {
    pub server: ServerState,
    /// No connection to Engine is made: `disconnected`, then `stopping` and
    /// `stopped` as the shutdown passes its connection step.
    pub connection: ConnectionState,
}

/// The parts of the running App Server that the shutdown takes down.
#[derive(Debug)]
pub struct Parts {
    pub socket: std::path::PathBuf,
    pub lock: RootLock,
    /// How long after the trigger the shutdown stops waiting.
    pub deadline: Duration,
}

/// The local API server and the signal that stops its listener.
#[derive(Debug)]
pub struct Listener {
    pub stop: oneshot::Sender<()>,
    pub served: JoinHandle<()>,
}

enum Message {
    States(oneshot::Sender<States>),
    Serving(Listener),
    /// A trigger, with the seam clock's time when it arrived.
    Shutdown(Duration),
    ConnectionStopped,
    Finished,
}

/// A handle on the lifecycle task.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    mailbox: mpsc::UnboundedSender<Message>,
}

/// Starts the lifecycle task; the receiver resolves once the shutdown
/// sequence has finished.
pub fn spawn(parts: Parts) -> (Lifecycle, oneshot::Receiver<()>) {
    let (mailbox, inbox) = mpsc::unbounded_channel();
    let (finished, on_finished) = oneshot::channel();
    let lifecycle = Lifecycle { mailbox };
    tokio::spawn(own(lifecycle.clone(), inbox, parts, finished));
    (lifecycle, on_finished)
}

async fn own(
    lifecycle: Lifecycle,
    mut inbox: mpsc::UnboundedReceiver<Message>,
    parts: Parts,
    finished: oneshot::Sender<()>,
) {
    let mut states = States {
        server: ServerState::Running,
        connection: ConnectionState::Disconnected,
    };
    let mut parts = Some(parts);
    let mut listener = None;
    let mut finished = Some(finished);
    while let Some(message) = inbox.recv().await {
        match message {
            Message::States(reply) => {
                let _ = reply.send(states);
            }
            Message::Serving(serving) => listener = Some(serving),
            Message::Shutdown(triggered_at) => {
                if let Some(parts) = parts.take() {
                    states.server = ServerState::Stopping;
                    states.connection = ConnectionState::Stopping;
                    let sequence = Sequence {
                        deadline: triggered_at + parts.deadline,
                        socket: parts.socket,
                        lock: parts.lock,
                        listener: listener.take(),
                    };
                    tokio::spawn(shutdown::run(sequence, lifecycle.clone()));
                }
            }
            Message::ConnectionStopped => states.connection = ConnectionState::Stopped,
            Message::Finished => {
                if let Some(finished) = finished.take() {
                    let _ = finished.send(());
                }
            }
        }
    }
}

impl Lifecycle {
    pub async fn states(&self) -> States {
        let (reply, answer) = oneshot::channel();
        self.send(Message::States(reply));
        answer
            .await
            .expect("the lifecycle task answers every message")
    }

    /// Hands over the local API server, which the shutdown stops.
    pub fn serving(&self, listener: Listener) {
        self.send(Message::Serving(listener));
    }

    /// Starts the shutdown, unless it has started. The trigger's time is
    /// read here, before the caller answers it, so the deadline never starts
    /// later than the trigger's answer.
    pub fn shutdown(&self) {
        self.send(Message::Shutdown(state::seams::now()));
    }

    pub(crate) fn connection_stopped(&self) {
        self.send(Message::ConnectionStopped);
    }

    pub(crate) fn finished(&self) {
        self.send(Message::Finished);
    }

    fn send(&self, message: Message) {
        self.mailbox
            .send(message)
            .unwrap_or_else(|_| panic!("the lifecycle task runs until the process exits"));
    }
}

/// The durations the App Server runs by, each a constant but for tests.
#[derive(Debug, Clone, Copy)]
pub struct Times {
    pub shutdown_deadline: Duration,
    pub socket_touch_interval: Duration,
}

impl Default for Times {
    fn default() -> Times {
        Times {
            shutdown_deadline: shutdown::SHUTDOWN_DEADLINE,
            socket_touch_interval: startup::SOCKET_TOUCH_INTERVAL,
        }
    }
}

/// Serves the local API of a started App Server, keeps its socket fresh and
/// takes SIGINT and SIGTERM as shutdown triggers, until the shutdown sequence
/// ends; returns the status as of then.
pub async fn run(started: Started, identity: Identity, times: Times) -> std::io::Result<Status> {
    // Read before the local API serves, so that the socket's age never
    // exceeds one interval while a client can reach it.
    let first_touch = state::seams::now() + times.socket_touch_interval;
    let Started {
        lock,
        configuration: _,
        socket,
        listener,
    } = started;
    let (lifecycle, finished) = spawn(Parts {
        socket: socket.clone(),
        lock,
        deadline: times.shutdown_deadline,
    });
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let (stop, stopped) = oneshot::channel::<()>();
    let router = local_api::router(lifecycle.clone(), identity.clone());
    let served = tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await;
    });
    lifecycle.serving(Listener { stop, served });
    let triggered = lifecycle.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
        triggered.shutdown();
    });
    let touching = tokio::spawn(startup::keep_socket_fresh(
        socket,
        first_touch,
        times.socket_touch_interval,
    ));
    let _ = finished.await;
    touching.abort();
    Ok(local_api::current_status(&lifecycle, &identity).await)
}
