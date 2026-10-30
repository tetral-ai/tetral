//! The lifecycle task: the owner of whether the App Server is running or
//! stopping. The first shutdown trigger, `POST /v1/shutdown`, SIGINT or
//! SIGTERM, moves it to stopping and starts the shutdown sequence
//! (`shutdown`); later triggers change nothing. The task keeps answering
//! while the sequence runs, so status stays available until the listener
//! stops. With an idle exit it also runs its timer and the upload before
//! the exit (`idle`), whose end is one more trigger. [`run`] serves a
//! started App Server until the sequence ends.

use std::sync::Arc;
use std::time::Duration;

use connection::{Connection, Credential, ReadyFields};
use drive::cache::{CacheConfig, ContentCache, volume_free_space};
use protocol::connection::SemVer;
use protocol::local::{ServerState, Status};
use protocol::ports::{ActivitySink, NoActivity};
use session::drives::DriveContext;
use session::{Factory, RootPaths, Shared, ToolContext};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::idle::{self, IdleExit};
use crate::local_api::{self, FixedStatus, StatusSources};
use crate::shutdown::{self, Sequence};
use crate::startup::{self, Started};
use crate::tasks::Supervisors;

/// The parts of the running App Server that the shutdown takes down.
#[derive(Debug)]
pub struct ShutdownParts {
    pub ownership: startup::SocketOwnership,
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
    Activity(idle::Activity),
}

/// A handle on the lifecycle task.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    mailbox: mpsc::UnboundedSender<Message>,
}

/// The lifecycle task's messages, which wait until [`start`] takes them.
pub struct Inbox(mpsc::UnboundedReceiver<Message>);

/// The lifecycle task's handle, before the task starts.
pub fn channel() -> (Lifecycle, Inbox) {
    let (mailbox, inbox) = mpsc::unbounded_channel();
    (Lifecycle { mailbox }, Inbox(inbox))
}

/// Starts the lifecycle task without an idle exit; the receiver resolves
/// once the shutdown sequence has finished, with the sequence's deadline.
pub fn spawn(parts: ShutdownParts) -> (Lifecycle, oneshot::Receiver<Duration>) {
    let (lifecycle, inbox) = channel();
    let finished = start(lifecycle.clone(), inbox, parts, None);
    (lifecycle, finished)
}

/// Starts the lifecycle task of `lifecycle`, with `idle` when the App
/// Server has an idle exit; the receiver resolves as [`spawn`]'s does.
pub(crate) fn start(
    lifecycle: Lifecycle,
    Inbox(inbox): Inbox,
    parts: ShutdownParts,
    idle: Option<IdleExit>,
) -> oneshot::Receiver<Duration> {
    let (finished, on_finished) = oneshot::channel();
    tokio::spawn(own(lifecycle, inbox, parts, idle, finished));
    on_finished
}

async fn own(
    lifecycle: Lifecycle,
    mut inbox: mpsc::UnboundedReceiver<Message>,
    parts: ShutdownParts,
    mut idle: Option<IdleExit>,
    finished: oneshot::Sender<Duration>,
) {
    let mut server = ServerState::Running;
    let mut parts = Some(parts);
    let mut listener = None;
    let mut finished = Some(finished);
    loop {
        let running = server == ServerState::Running;
        let due = idle.as_ref().filter(|_| running).and_then(IdleExit::due);
        let message = tokio::select! {
            message = inbox.recv() => match message {
                Some(message) => message,
                None => return,
            },
            () = idle::sleep_until(due) => {
                if let Some(idle) = idle.as_mut() {
                    idle.begin().await;
                }
                continue;
            }
            current = async {
                match idle.as_mut() {
                    Some(idle) if running => idle.uploaded().await,
                    _ => std::future::pending().await,
                }
            } => {
                if !current {
                    continue;
                }
                Message::Shutdown(state::seams::now())
            }
        };
        match message {
            Message::ServerState(reply) => {
                let _ = reply.send(server);
            }
            Message::Serving(serving) => {
                if server == ServerState::Stopping {
                    let _ = serving.stop.send(());
                    serving.served.abort();
                } else {
                    listener = Some(serving);
                }
            }
            Message::Shutdown(triggered_at) => {
                if let Some(parts) = parts.take() {
                    server = ServerState::Stopping;
                    let sequence = Sequence {
                        deadline: triggered_at + parts.deadline,
                        ownership: parts.ownership,
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
            // Once the shutdown has begun, it admits nothing.
            Message::Activity(activity) => {
                if let Some(idle) = idle.as_mut().filter(|_| server == ServerState::Running) {
                    idle.activity(activity);
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

    pub(crate) fn activity(&self, activity: idle::Activity) {
        self.send(Message::Activity(activity));
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
/// With `idle_exit`, the App Server also exits once idle that long
/// (`idle`).
pub async fn run(
    started: Started,
    fixed: FixedStatus,
    times: Times,
    idle_exit: Option<Duration>,
) -> std::io::Result<Ended> {
    // Read before the local API serves, so that the socket's age never
    // exceeds one interval while a client can reach it.
    let start = state::seams::now();
    let first_touch = start + times.socket_touch_interval;
    let Started {
        ownership,
        configuration,
        socket,
        listener,
        capabilities,
        tetral,
        root,
        canonical_root,
        leftovers: (check, leftovers),
        mounting,
    } = started;
    let socket_directory = socket.parent().unwrap_or(&socket).canonicalize()?;
    let tools = ToolContext {
        capability: capabilities.tool_execution.clone(),
        socket_directory,
        tetral: tetral.clone(),
        cua_driver: configuration
            .config
            .computer_use
            .as_ref()
            .and_then(|computer_use| computer_use.driver.clone()),
    };
    let state_root =
        state::paths::StateRoot::new(root.as_os_str()).expect("startup took this root's path");
    let cache_root = canonical_root.clone();
    // The drive syncs' context comes once the Connection owner provides
    // their transfer client.
    let (drive_context, drives) = tokio::sync::watch::channel(None);
    // Activity waits in the lifecycle task's mailbox until it starts below.
    let (lifecycle, lifecycle_inbox) = channel();
    let activity: Arc<dyn ActivitySink> = match idle_exit {
        Some(_) => Arc::new(lifecycle.clone()),
        None => Arc::new(NoActivity),
    };
    let sessions = Factory::new(
        Shared {
            paths: Arc::new(RootPaths {
                given: root.clone(),
                canonical_utf8: canonical_root
                    .to_str()
                    .expect("startup validates the canonical root")
                    .to_owned(),
                canonical: canonical_root,
                probe_program: tetral.clone(),
            }),
            times: times.session,
            tools: Arc::new(tools),
            leftovers: leftovers.clone(),
            drives,
        },
        Supervisors {
            tetral,
            isolation: execution::isolation::Isolation::new(),
            bounds: execution::supervisor::Bounds::default(),
            boot_id: execution::process::boot_id(),
            activity: activity.clone(),
        },
    );
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::spawn(check.run());
    let credentials = configuration.credentials;
    let connection = Connection::spawn(connection::Config {
        server_url: credentials.server_url.as_str().to_owned(),
        instance_id: credentials.instance_id,
        credential: Credential {
            credential_id: credentials.credential_id,
            key: credentials.connection_key,
            expires_at: credentials.expires_at,
        },
        state_root: state_root.clone(),
        ready: ReadyFields {
            cli_version: SemVer::new(fixed.cli_version.as_str())
                .expect("the package version is a semantic version"),
            capabilities,
        },
        started: start,
        sessions: Arc::new(sessions),
        activity,
        times: times.connection,
    });
    // The drive syncs read from it; it holds the App Server's transfer slots.
    let content_cache = ContentCache::spawn(CacheConfig {
        free_space: volume_free_space(cache_root.clone()),
        root: cache_root,
        transfer: connection.drive_transfer(),
        clock: state::clock::Clock::seams(),
    })?;
    drive_context.send_replace(Some(Arc::new(DriveContext {
        transfer: connection.drive_transfer(),
        cache: content_cache,
        mounting,
        registry: Arc::new(connection.clone()),
        clock: state::clock::Clock::seams(),
    })));
    let idle = idle_exit.map(|limit| IdleExit::new(limit, start, connection.clone()));
    let finished = self::start(
        lifecycle.clone(),
        lifecycle_inbox,
        ShutdownParts {
            ownership,
            connection: connection.clone(),
            deadline: times.shutdown_deadline,
        },
        idle,
    );
    let (stop, stopped) = oneshot::channel::<()>();
    let sources = StatusSources {
        lifecycle: lifecycle.clone(),
        connection,
        leftovers,
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
    let status = local_api::status_snapshot(&sources, ServerState::Stopping);
    Ok(Ended {
        status,
        left: deadline.saturating_sub(state::seams::now()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::id::SafeId;
    use protocol::ports::{ConnectionPort, ReportSink, SessionFactory, SessionHandle, ToolCall};
    use serde_json::json;

    struct NoSessions;
    impl SessionFactory for NoSessions {
        fn create(
            &self,
            _: SafeId,
            _: Arc<dyn ConnectionPort>,
            _: Arc<dyn ReportSink>,
        ) -> Arc<dyn SessionHandle> {
            panic!("the lifecycle fixture creates no Session")
        }
        fn unbound_tool(&self, _: ToolCall, _: Arc<dyn ReportSink>) {
            panic!("the lifecycle fixture executes no tool")
        }
    }

    #[tokio::test]
    async fn shutdown_stops_a_late_listener_and_keeps_the_first_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let root = state::paths::StateRoot::new(directory.path().as_os_str()).unwrap();
        state::files::create_private_dirs(&root.join(state::paths::RUN)).unwrap();
        let lock = crate::lock::acquire(&root).unwrap();
        let bootstrap = protocol::bootstrap::Bootstrap::decode(
            br#"{"server_url":"http://127.0.0.1:1","instance_id":"instance","credential_id":"credential","connection_key":"tetral_ck_lifecycle","expires_at":"2099-01-01T00:00:00Z"}"#,
        ).unwrap();
        let connection = Connection::spawn(connection::Config {
            server_url: bootstrap.server_url.as_str().into(),
            instance_id: bootstrap.instance_id,
            credential: Credential {
                credential_id: bootstrap.credential_id,
                key: bootstrap.connection_key,
                expires_at: bootstrap.expires_at,
            },
            state_root: root.clone(),
            ready: ReadyFields {
                cli_version: SemVer::new("0.1.0").unwrap(),
                capabilities: serde_json::from_value(
                    json!({"session_preparation":true,"drives":{"mount":false}}),
                )
                .unwrap(),
            },
            started: state::seams::now(),
            sessions: Arc::new(NoSessions),
            activity: Arc::new(protocol::ports::NoActivity),
            times: connection::Times::default(),
        });
        let (lifecycle, finished) = spawn(ShutdownParts {
            ownership: startup::SocketOwnership::new(lock),
            connection,
            deadline: Duration::from_secs(5),
        });
        let (stop, stopped) = oneshot::channel();
        let served = tokio::spawn(std::future::pending::<()>());
        let task = served.abort_handle();
        lifecycle.send(Message::Shutdown(Duration::from_secs(100)));
        lifecycle.serving(Listener { stop, served });
        lifecycle.send(Message::Shutdown(Duration::from_secs(200)));
        assert_eq!(lifecycle.server_state().await, ServerState::Stopping);
        tokio::time::timeout(Duration::from_secs(2), stopped)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), finished)
                .await
                .unwrap()
                .unwrap(),
            Duration::from_secs(105)
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(crate::lock::acquire(&root).is_ok());
    }
}
