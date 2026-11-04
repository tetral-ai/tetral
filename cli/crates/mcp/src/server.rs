//! One MCP connection to a local server: its `<driver> mcp` process, the
//! rmcp client over its stdio and how both end. The connection's task
//! spawns the process, runs `initialize` within the start limit from the
//! spawn on the seam clock, hands the manager the connection once it is open
//! ([`Message::Opened`]) and ends with exactly one [`Message::Ended`]: a
//! failed start, an exit, a stdout message over its limit, or a stop the
//! manager asked for.
//!
//! The server's own messages on the connection are handled here: `ping`
//! gets an empty result and every other request the JSON-RPC error
//! `-32601`; `notifications/message` is recorded as its level and the size
//! of its `data`, never its content; every other notification that reaches
//! rmcp is ignored (`notifications/tools/list_changed` reaches the manager
//! from the transport).

use std::path::PathBuf;
use std::sync::Arc;

use protocol::connection::McpServerInfo;
use protocol::field::Nullable;
use rmcp::ErrorData;
use rmcp::model::{
    ClientCapabilities, ClientConfig, ClientResult, ErrorCode, Implementation, InitializeResult,
    ProtocolVersion, ServerNotification, ServerPeerInfo, ServerRequest,
};
use rmcp::service::{
    ClientInitializeError, NotificationContext, Peer, RequestContext, RoleClient, Service,
};
use tokio::sync::{mpsc, oneshot};

use crate::Message;
use crate::limits::Limits;
use crate::process::{self, Process};
use crate::transport::{self, Closed, Ends, Events, RawAnswer, Writer};

/// What a new connection needs.
pub(crate) struct Opening {
    /// The manager's name for the connection, which its messages carry.
    pub key: u64,
    pub server: &'static str,
    pub driver: PathBuf,
    /// The process's working directory.
    pub cwd: PathBuf,
    pub cli_version: String,
    pub limits: Limits,
    pub mailbox: mpsc::UnboundedSender<Message>,
}

/// An open connection, as the manager uses it.
pub(crate) struct Opened {
    pub peer: Peer<RoleClient>,
    pub server_info: McpServerInfo,
    pub writer: Writer,
}

/// Why a connection did not open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartFailure {
    Spawn(String),
    NoInitialize,
    InitializeFailed {
        code: i32,
        message: String,
    },
    InvalidResult(String),
    /// The process ended or closed its output before its `initialize`
    /// result.
    EndedEarly(String),
    MessageTooLarge,
}

impl StartFailure {
    /// The failure's description, after `could not start <driver>: ` for a
    /// failed spawn.
    pub fn describe(&self) -> String {
        match self {
            StartFailure::Spawn(error) => error.clone(),
            StartFailure::NoInitialize => "no initialize result within 30 seconds".to_owned(),
            StartFailure::InitializeFailed { code, message } => {
                format!("initialize failed: {code} {message}")
            }
            StartFailure::InvalidResult(error) => format!("invalid initialize result: {error}"),
            StartFailure::EndedEarly(how) => format!("{how} before its initialize result"),
            StartFailure::MessageTooLarge => message_too_large(),
        }
    }
}

/// How a connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum End {
    StartFailed(StartFailure),
    /// The process ended, or its stdout did, without a stop: the
    /// description of an exit.
    Exited(String),
    /// A stdout message over its limit closed the connection and its process
    /// was stopped.
    MessageTooLarge,
    /// The manager stopped it.
    Stopped,
}

pub(crate) fn message_too_large() -> String {
    format!(
        "server output message over {} bytes",
        crate::limits::MCP_STDOUT_MESSAGE_BYTES
    )
}

/// Asks the connection's task to stop its process and close.
pub(crate) struct Stop(Option<oneshot::Sender<()>>);

impl Stop {
    pub fn stop(&mut self) {
        if let Some(stop) = self.0.take() {
            let _ = stop.send(());
        }
    }
}

/// Starts the connection's task.
pub(crate) fn open(opening: Opening) -> Stop {
    let (stop, stopped) = oneshot::channel();
    tokio::spawn(run(opening, stopped));
    Stop(Some(stop))
}

async fn run(opening: Opening, mut stop: oneshot::Receiver<()>) {
    let spawned_at = state::seams::now();
    let end = match process::spawn(&opening.driver, &opening.cwd, opening.server) {
        Err(error) => End::StartFailed(StartFailure::Spawn(error.to_string())),
        Ok(process) => connect(&opening, process, spawned_at, &mut stop).await,
    };
    let _ = opening.mailbox.send(Message::Ended {
        key: opening.key,
        end,
    });
}

async fn connect(
    opening: &Opening,
    process: Process,
    spawned_at: std::time::Duration,
    stop: &mut oneshot::Receiver<()>,
) -> End {
    let group = process.group();
    let Process {
        mut child,
        stdin,
        stdout,
        ..
    } = process;
    let grace = opening.limits.stop_grace;
    let key = opening.key;
    let mailbox = opening.mailbox.clone();
    let events: Events = Arc::new(move |event| {
        let _ = mailbox.send(Message::Transport { key, event });
    });
    let (transport, ends) =
        transport::new(stdin, stdout, opening.limits.stdout_message_bytes, events);
    let Ends {
        mut initialize,
        mut closed,
        writer,
    } = ends;
    let handler = Handler {
        server: opening.server,
        info: client_info(&opening.cli_version),
    };
    let started = tokio::select! {
        started = rmcp::serve_client(handler, transport) => Some(started),
        () = state::seams::sleep_until_counted(spawned_at + opening.limits.start, "mcp.start_checked") => None,
        _ = &mut *stop => {
            process::stop(&mut child, group, grace).await;
            return End::Stopped;
        }
    };
    let running = match started {
        Some(Ok(running)) => running,
        failed => {
            let failure = match failed {
                None => StartFailure::NoInitialize,
                Some(Err(error)) => {
                    start_failure(
                        error,
                        initialize.try_recv().ok(),
                        &mut closed,
                        &mut child,
                        grace,
                    )
                    .await
                }
                Some(Ok(_)) => unreachable!("an open connection is handled above"),
            };
            process::stop(&mut child, group, grace).await;
            return End::StartFailed(failure);
        }
    };
    let result = running
        .peer()
        .peer_info()
        .expect("an initialized connection has its server's result");
    let _ = opening.mailbox.send(Message::Opened {
        key,
        opened: Opened {
            peer: running.peer().clone(),
            server_info: server_info(&result),
            writer,
        },
    });
    let end = tokio::select! {
        status = child.wait() => {
            let how = match status {
                Ok(status) => format!("server process exited: {}", process::describe(status)),
                Err(_) => "server process exited".to_owned(),
            };
            End::Exited(how)
        }
        how = &mut closed => match how {
            Ok(Closed::MessageTooLarge) => End::MessageTooLarge,
            Ok(Closed::EndOfOutput) | Err(_) => End::Exited(closed_output(&mut child, grace).await),
        },
        _ = &mut *stop => End::Stopped,
    };
    process::stop(&mut child, group, grace).await;
    drop(running);
    end
}

/// How a process ended once its output did: its exit status when it exits
/// within `grace`, otherwise that it closed its output.
async fn closed_output(child: &mut tokio::process::Child, grace: std::time::Duration) -> String {
    let deadline = state::seams::now() + grace;
    tokio::select! {
        status = child.wait() => match status {
            Ok(status) => format!("server process exited: {}", process::describe(status)),
            Err(_) => "server process exited".to_owned(),
        },
        () = state::seams::sleep_until(deadline) => "server closed its output".to_owned(),
    }
}

async fn start_failure(
    error: ClientInitializeError,
    answer: Option<RawAnswer>,
    closed: &mut oneshot::Receiver<Closed>,
    child: &mut tokio::process::Child,
    grace: std::time::Duration,
) -> StartFailure {
    match error {
        ClientInitializeError::JsonRpcError(error) => StartFailure::InitializeFailed {
            code: error.code.0,
            message: error.message.into_owned(),
        },
        ClientInitializeError::ExpectedInitResult(_) => {
            let reason = match answer {
                Some(RawAnswer::Result(result)) => {
                    serde_json::from_value::<InitializeResult>(result).err()
                }
                _ => None,
            };
            StartFailure::InvalidResult(reason.map_or_else(
                || "not an initialize result".to_owned(),
                |error| error.to_string(),
            ))
        }
        ClientInitializeError::ConnectionClosed(_)
        | ClientInitializeError::ExpectedInitResponse(None)
        | ClientInitializeError::TransportError { .. } => match closed.await {
            Ok(Closed::MessageTooLarge) => StartFailure::MessageTooLarge,
            _ => StartFailure::EndedEarly(closed_output(child, grace).await),
        },
        other => StartFailure::InvalidResult(other.to_string()),
    }
}

/// The client's `initialize`: protocol version `2025-06-18`, no
/// capabilities, and `clientInfo` `{name: "tetral-cli", version}`.
fn client_info(cli_version: &str) -> ClientConfig {
    ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("tetral-cli", cli_version),
    )
    .with_protocol_version(ProtocolVersion::V_2025_06_18)
}

fn server_info(result: &ServerPeerInfo) -> McpServerInfo {
    let server = result
        .server_info
        .as_ref()
        .expect("an initialize result has its serverInfo");
    McpServerInfo {
        name: server.name.clone(),
        title: Nullable(server.title.clone()),
        version: server.version.clone(),
    }
}

/// The client side of a connection: answers the server's requests and takes
/// its notifications.
struct Handler {
    server: &'static str,
    info: ClientConfig,
}

impl Service<RoleClient> for Handler {
    async fn handle_request(
        &self,
        request: ServerRequest,
        _: RequestContext<RoleClient>,
    ) -> Result<ClientResult, ErrorData> {
        match request {
            ServerRequest::PingRequest(_) => Ok(ClientResult::empty(())),
            _ => Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "Method not found",
                None,
            )),
        }
    }

    async fn handle_notification(
        &self,
        notification: ServerNotification,
        _: NotificationContext<RoleClient>,
    ) -> Result<(), ErrorData> {
        if let ServerNotification::LoggingMessageNotification(message) = notification {
            // Read through the wire form: rmcp marks its typed members
            // deprecated, and only the level and the size are kept.
            let params = serde_json::to_value(&message.params).unwrap_or_default();
            let bytes = params
                .get("data")
                .and_then(|data| serde_json::to_vec(data).ok())
                .map_or(0, |data| data.len());
            if let Some(level) = params
                .get("level")
                .and_then(|level| Level::of(level.as_str()?))
            {
                observe::info!(
                    "mcp.server_message",
                    mcp.server = observe::Id(self.server),
                    mcp.log_level = level,
                    mcp.data_bytes = bytes
                );
            }
        }
        Ok(())
    }

    fn get_info(&self) -> ClientConfig {
        self.info.clone()
    }
}

/// A `notifications/message` level, one of the eight of MCP.
#[derive(Debug, Clone, Copy)]
struct Level(&'static str);

impl Level {
    const ALL: [&'static str; 8] = [
        "debug",
        "info",
        "notice",
        "warning",
        "error",
        "critical",
        "alert",
        "emergency",
    ];

    fn of(text: &str) -> Option<Level> {
        Level::ALL
            .into_iter()
            .find(|level| *level == text)
            .map(Level)
    }
}

impl observe::RecordEnum for Level {
    fn record_str(&self) -> &'static str {
        self.0
    }
}
