//! The lifecycle task: the owner of whether the App Server is running or
//! stopping. The first shutdown trigger, `POST /v1/shutdown`, SIGINT or
//! SIGTERM, moves it to stopping and starts the shutdown sequence
//! (`shutdown`); later triggers change nothing. The task keeps answering
//! while the sequence runs, so status stays available until the listener
//! stops. [`run`] serves a started App Server until the sequence ends.

use std::sync::Arc;
use std::time::Duration;

use connection::{Connection, Credential, ReadyFields};
use protocol::connection::SemVer;
use protocol::local::{ServerState, Status};
use session::{Factory, RootPaths, Shared, ToolContext};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::local_api::{self, FixedStatus, StatusSources};
use crate::lock::RootLock;
use crate::shutdown::{self, Sequence};
use crate::startup::{self, Started};
use crate::tasks::Supervisors;

/// The parts of the running App Server that the shutdown takes down.
#[derive(Debug)]
pub struct ShutdownParts {
    pub socket: std::path::PathBuf,
    pub lock: RootLock,
    pub connection: Connection,
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
    ServerState(oneshot::Sender<ServerState>),
    Serving(Listener),
    /// A trigger, with the seam clock's time when it arrived.
    Shutdown(Duration),
    /// The end of the shutdown sequence, with its deadline.
    Finished(Duration),
}

/// A handle on the lifecycle task.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    mailbox: mpsc::UnboundedSender<Message>,
}

/// Starts the lifecycle task; the receiver resolves once the shutdown
/// sequence has finished, with the sequence's deadline.
pub fn spawn(parts: ShutdownParts) -> (Lifecycle, oneshot::Receiver<Duration>) {
    let (mailbox, inbox) = mpsc::unbounded_channel();
    let (finished, on_finished) = oneshot::channel();
    let lifecycle = Lifecycle { mailbox };
    tokio::spawn(own(lifecycle.clone(), inbox, parts, finished));
    (lifecycle, on_finished)
}

async fn own(
    lifecycle: Lifecycle,
    mut inbox: mpsc::UnboundedReceiver<Message>,
    parts: ShutdownParts,
    finished: oneshot::Sender<Duration>,
) {
    let mut server = ServerState::Running;
    let mut parts = Some(parts);
    let mut listener = None;
    let mut finished = Some(finished);
    while let Some(message) = inbox.recv().await {
        match message {
            Message::ServerState(reply) => {
                let _ = reply.send(server);
            }
            Message::Serving(serving) => listener = Some(serving),
            Message::Shutdown(triggered_at) => {
                if let Some(parts) = parts.take() {
                    server = ServerState::Stopping;
                    let sequence = Sequence {
                        deadline: triggered_at + parts.deadline,
                        socket: parts.socket,
                        lock: parts.lock,
                        connection: parts.connection,
                        listener: listener.take(),
                    };
                    tokio::spawn(shutdown::run(sequence, lifecycle.clone()));
                }
            }
            Message::Finished(deadline) => {
                if let Some(finished) = finished.take() {
                    let _ = finished.send(deadline);
                }
            }
        }
    }
}

impl Lifecycle {
    pub async fn server_state(&self) -> ServerState {
        let (reply, answer) = oneshot::channel();
        self.send(Message::ServerState(reply));
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

    pub(crate) fn finished(&self, deadline: Duration) {
        self.send(Message::Finished(deadline));
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
    pub connection: connection::Times,
    pub session: session::Times,
}

impl Default for Times {
    fn default() -> Times {
        Times {
            shutdown_deadline: shutdown::SHUTDOWN_DEADLINE,
            socket_touch_interval: startup::SOCKET_TOUCH_INTERVAL,
            connection: connection::Times::default(),
            session: session::Times::default(),
        }
    }
}

/// What a stopped App Server leaves to its process's exit.
#[derive(Debug)]
pub struct Ended {
    /// The status as of the end of the shutdown sequence.
    pub status: Status,
    /// The time left then before the shutdown's deadline.
    pub left: Duration,
}

/// Starts the Connection owner with the Session binding owners it creates,
/// serves the local API of a started App Server, keeps its socket fresh and
/// takes SIGINT and SIGTERM as shutdown triggers, until the shutdown
/// sequence ends.
pub async fn run(started: Started, fixed: FixedStatus, times: Times) -> std::io::Result<Ended> {
    // Read before the local API serves, so that the socket's age never
    // exceeds one interval while a client can reach it.
    let start = state::seams::now();
    let first_touch = start + times.socket_touch_interval;
    let Started {
        lock,
        configuration,
        socket,
        listener,
        outbox,
        capabilities,
        root,
        canonical_root,
    } = started;
    let probe_program = std::env::current_exe()?;
    let socket_directory = socket.parent().unwrap_or(&socket).canonicalize()?;
    let tetral = probe_program.canonicalize()?;
    let tools = ToolContext {
        capability: capabilities.tool_execution.clone(),
        socket_directory,
        tetral: tetral.clone(),
    };
    let sessions = Factory::new(
        Shared {
            paths: Arc::new(RootPaths {
                given: root,
                canonical: canonical_root,
                probe_program,
            }),
            times: times.session,
            tools: Arc::new(tools),
        },
        Supervisors {
            tetral,
            isolation: execution::isolation::Isolation::new(),
            bounds: execution::supervisor::Bounds::default(),
        },
    );
    let credentials = configuration.credentials;
    let connection = Connection::spawn(connection::Config {
        server_url: credentials.server_url.as_str().to_owned(),
        instance_id: credentials.instance_id,
        credential: Credential {
            credential_id: credentials.credential_id,
            key: credentials.connection_key,
            expires_at: credentials.expires_at,
        },
        ready: ReadyFields {
            cli_version: SemVer::new(fixed.cli_version.as_str())
                .expect("the package version is a semantic version"),
            capabilities,
        },
        started: start,
        outbox,
        sessions: Arc::new(sessions),
        times: times.connection,
    });
    let (lifecycle, finished) = spawn(ShutdownParts {
        socket: socket.clone(),
        lock,
        connection: connection.clone(),
        deadline: times.shutdown_deadline,
    });
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let (stop, stopped) = oneshot::channel::<()>();
    let sources = StatusSources {
        lifecycle: lifecycle.clone(),
        connection,
        fixed,
    };
    let router = local_api::router(sources.clone());
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
    let deadline = finished.await.unwrap_or_else(|_| state::seams::now());
    touching.abort();
    let status = local_api::current_status(&sources).await;
    Ok(Ended {
        status,
        left: deadline.saturating_sub(state::seams::now()),
    })
}
