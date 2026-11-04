#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

pub mod limits;
mod listing;
mod process;
mod server;
mod tool_lists;
mod transport;

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use protocol::connection::McpListErrorCode;
use protocol::id::Id;
use protocol::ports::{McpPort, ReportId, ReportListener, ReportOutcome, ReportSink};
use rmcp::model::{
    ClientRequest, ListToolsRequest, MetaObject, PaginatedRequestParams, RequestMetaObject,
};
use rmcp::service::{Peer, PeerRequestOptions, RoleClient};
use state::config::{ComputerUse, EncodeError};
use state::paths::StateRoot;
use tokio::sync::{mpsc, oneshot};

use crate::limits::Limits;
use crate::listing::{Listing, Step};
use crate::server::{End, Opened, Opening, Stop};
use crate::tool_lists::{Latest, Queue};
use crate::transport::{RawAnswer, TransportEvent};

/// The one local server: Computer Use, `<driver> mcp`.
pub const CUA: &str = "cua";

/// What the manager runs with.
#[derive(Debug, Clone)]
pub struct ManagerConfig {
    /// The state root whose `config.toml` records Computer Use.
    pub root: StateRoot,
    /// The `clientInfo.version` of every `initialize`.
    pub cli_version: String,
    pub limits: Limits,
}

/// Why `POST /v1/computer-use` changed nothing or did not start the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetError {
    /// The request cannot be recorded, such as a driver path that does not
    /// fit in `config.toml`.
    InvalidArguments(String),
    /// The record is written, and the driver did not start: the start
    /// failure's message.
    DriverFailed(String),
    /// `config.toml` could not be read or written, or the App Server shuts
    /// down.
    Io(String),
}

/// The handle on the MCP manager task.
#[derive(Debug, Clone)]
pub struct McpManager {
    mailbox: mpsc::UnboundedSender<Message>,
}

/// Starts the manager task.
pub fn spawn(config: ManagerConfig) -> McpManager {
    let (mailbox, inbox) = mpsc::unbounded_channel();
    let manager = Manager {
        config,
        mailbox: mailbox.clone(),
        next_key: 0,
        next_tag: 0,
        configured: false,
        run: None,
        latest: None,
        first_pending: false,
        queue: None,
        connections: HashMap::new(),
        requests: VecDeque::new(),
        active: None,
        shutdown: None,
    };
    tokio::spawn(own(manager, inbox));
    McpManager { mailbox }
}

impl McpManager {
    /// The App Server started with `computer_use` recorded: it starts `cua`
    /// when that records it enabled, at once and without waiting.
    pub fn start_recorded(&self, computer_use: Option<ComputerUse>) {
        self.send(Message::StartRecorded(computer_use));
    }

    /// `POST /v1/computer-use`, handled after every earlier one.
    pub async fn set_computer_use(
        &self,
        enabled: bool,
        driver: Option<PathBuf>,
    ) -> Result<(), SetError> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Set {
            enabled,
            driver,
            reply,
        });
        answer.await.unwrap_or_else(|_| Err(shutting_down()))
    }

    /// Stops every local server's processes; returns once each has exited or
    /// received SIGKILL.
    pub async fn shutdown(&self) {
        let (reply, done) = oneshot::channel();
        self.send(Message::Shutdown(reply));
        let _ = done.await;
    }

    fn send(&self, message: Message) {
        self.mailbox
            .send(message)
            .unwrap_or_else(|_| panic!("the MCP manager runs until the process exits"));
    }
}

impl McpPort for McpManager {
    fn connection_online(&self, connection_id: Id, reports: Arc<dyn ReportSink>) {
        self.send(Message::Online {
            connection_id,
            reports,
        });
    }

    fn connection_ended(&self, connection_id: Id) {
        self.send(Message::ConnectionEnded(connection_id));
    }
}

fn shutting_down() -> SetError {
    SetError::Io("the App Server is shutting down".to_owned())
}

pub(crate) enum Message {
    StartRecorded(Option<ComputerUse>),
    Set {
        enabled: bool,
        driver: Option<PathBuf>,
        reply: oneshot::Sender<Result<(), SetError>>,
    },
    Shutdown(oneshot::Sender<()>),
    Online {
        connection_id: Id,
        reports: Arc<dyn ReportSink>,
    },
    ConnectionEnded(Id),
    /// The connection `key` completed `initialize`.
    Opened {
        key: u64,
        opened: Opened,
    },
    /// The connection `key` ended; its process is stopped.
    Ended {
        key: u64,
        end: End,
    },
    Transport {
        key: u64,
        event: TransportEvent,
    },
    /// The server sent `notifications/tools/list_changed` on `key`.
    ListChanged {
        key: u64,
    },
    ReportOutcome(ReportId),
}

/// Tells the manager the outbox's outcome of each tool-list report.
struct Outcomes(mpsc::UnboundedSender<Message>);

impl ReportListener for Outcomes {
    fn outcome(&self, report: ReportId, _: ReportOutcome) {
        let _ = self.0.send(Message::ReportOutcome(report));
    }
}

/// A `POST /v1/computer-use` waiting for its turn.
struct Request {
    enabled: bool,
    driver: Option<PathBuf>,
    reply: oneshot::Sender<Result<(), SetError>>,
}

/// The request being handled.
enum Active {
    /// Waits for the stopped connections to end, then starts `driver` or,
    /// without one, answers.
    Stopping {
        start: Option<PathBuf>,
        reply: oneshot::Sender<Result<(), SetError>>,
    },
    /// Waits for the started server's `initialize`.
    Starting {
        reply: oneshot::Sender<Result<(), SetError>>,
    },
}

/// The running server: its server connection, starting or open, and its
/// listing.
struct Run {
    key: u64,
    driver: PathBuf,
    open: Option<Opened>,
    listing: Option<Listing>,
    /// A `notifications/tools/list_changed` came during the listing.
    again: bool,
}

/// A connection's task, until its end arrives.
struct Connection {
    stop: Stop,
    stopping: bool,
}

struct Manager {
    config: ManagerConfig,
    mailbox: mpsc::UnboundedSender<Message>,
    next_key: u64,
    next_tag: u64,
    /// Whether `config.toml` holds `[computer_use]`.
    configured: bool,
    run: Option<Run>,
    latest: Option<Latest>,
    /// The server's start or first listing has not ended: a new connection
    /// reports it once it has.
    first_pending: bool,
    /// The tool-list reports of the online connection.
    queue: Option<Queue>,
    connections: HashMap<u64, Connection>,
    requests: VecDeque<Request>,
    active: Option<Active>,
    /// The shutdown's waiters, once it began.
    shutdown: Option<Vec<oneshot::Sender<()>>>,
}

async fn own(mut manager: Manager, mut inbox: mpsc::UnboundedReceiver<Message>) {
    loop {
        let deadline = manager.listing_deadline();
        let message = tokio::select! {
            message = inbox.recv() => match message {
                Some(message) => message,
                None => return,
            },
            () = sleep_until(deadline) => {
                manager.listing_timed_out();
                continue;
            }
        };
        manager.handle(message);
    }
}

async fn sleep_until(deadline: Option<Duration>) {
    match deadline {
        Some(at) => state::seams::sleep_until_counted(at, "mcp.listing_checked").await,
        None => std::future::pending().await,
    }
}

impl Manager {
    fn handle(&mut self, message: Message) {
        match message {
            Message::StartRecorded(computer_use) => self.start_recorded(computer_use),
            Message::Set {
                enabled,
                driver,
                reply,
            } => {
                if self.shutdown.is_some() {
                    let _ = reply.send(Err(shutting_down()));
                    return;
                }
                self.requests.push_back(Request {
                    enabled,
                    driver,
                    reply,
                });
                self.next_request();
            }
            Message::Shutdown(reply) => self.begin_shutdown(reply),
            Message::Online {
                connection_id,
                reports,
            } => {
                let listener = Arc::new(Outcomes(self.mailbox.clone()));
                self.queue = Some(Queue::new(connection_id, CUA, reports, listener));
                if self.configured
                    && !self.first_pending
                    && let Some(latest) = self.latest.clone()
                {
                    self.report(latest);
                }
            }
            Message::ConnectionEnded(connection_id) => {
                if self
                    .queue
                    .as_ref()
                    .is_some_and(|queue| queue.connection_id == connection_id)
                {
                    self.queue = None;
                }
            }
            Message::Opened { key, opened } => self.opened(key, opened),
            Message::Ended { key, end } => self.ended(key, end),
            Message::Transport { key, event } => match event {
                TransportEvent::Answer { tag, answer } => self.answered(key, tag, answer),
            },
            Message::ListChanged { key } => self.list_changed(key),
            Message::ReportOutcome(report) => {
                if let Some(replacement) =
                    self.queue.as_mut().and_then(|queue| queue.outcome(report))
                {
                    self.latest = Some(replacement);
                }
            }
        }
    }

    fn start_recorded(&mut self, computer_use: Option<ComputerUse>) {
        if self.shutdown.is_some() {
            return;
        }
        self.configured = computer_use.is_some();
        match computer_use {
            Some(ComputerUse {
                enabled: true,
                driver: Some(driver),
            }) => self.start(driver),
            Some(ComputerUse { enabled: true, .. }) => self.set_latest(Latest::Failure {
                code: McpListErrorCode::McpStartFailed,
                message: "config.toml records no driver".to_owned(),
            }),
            Some(_) => self.set_latest(Latest::Disabled),
            None => {}
        }
    }

    /// Handles the next waiting request when none is being handled.
    fn next_request(&mut self) {
        while self.active.is_none() {
            let Some(Request {
                enabled,
                driver,
                reply,
            }) = self.requests.pop_front()
            else {
                return;
            };
            if let Err(error) = record(&self.config.root, enabled, driver.as_deref()) {
                let _ = reply.send(Err(error));
                continue;
            }
            self.configured = true;
            let start = driver.filter(|_| enabled);
            self.first_pending = enabled;
            self.stop_run();
            if !enabled {
                self.set_latest(Latest::Disabled);
            }
            self.active = Some(Active::Stopping { start, reply });
            self.stopped_or_waiting();
        }
    }

    /// Moves the request on once no stopped connection remains.
    fn stopped_or_waiting(&mut self) {
        if self
            .connections
            .values()
            .any(|connection| connection.stopping)
        {
            return;
        }
        if let Some(Active::Stopping { start, reply }) = self.active.take() {
            match start {
                Some(driver) => {
                    self.start(driver);
                    self.active = Some(Active::Starting { reply });
                }
                None => {
                    let _ = reply.send(Ok(()));
                }
            }
        }
        self.next_request();
    }

    /// Stops every connection of the running server; it is not running.
    fn stop_run(&mut self) {
        self.run = None;
        for connection in self.connections.values_mut() {
            connection.stopping = true;
            connection.stop.stop();
        }
    }

    fn start(&mut self, driver: PathBuf) {
        self.next_key += 1;
        let key = self.next_key;
        let stop = server::open(Opening {
            key,
            server: CUA,
            driver: driver.clone(),
            cwd: PathBuf::from("/"),
            cli_version: self.config.cli_version.clone(),
            limits: self.config.limits,
            mailbox: self.mailbox.clone(),
        });
        self.connections.insert(
            key,
            Connection {
                stop,
                stopping: false,
            },
        );
        self.first_pending = true;
        self.run = Some(Run {
            key,
            driver,
            open: None,
            listing: None,
            again: false,
        });
    }

    fn opened(&mut self, key: u64, opened: Opened) {
        let Some(run) = self.run.as_mut().filter(|run| run.key == key) else {
            return;
        };
        run.open = Some(opened);
        if let Some(Active::Starting { reply }) = self.active.take() {
            let _ = reply.send(Ok(()));
        }
        self.start_listing();
        self.next_request();
    }

    fn ended(&mut self, key: u64, end: End) {
        let Some(connection) = self.connections.remove(&key) else {
            return;
        };
        if connection.stopping {
            self.stopped_or_waiting();
            self.shutdown_progress();
            return;
        }
        let Some(run) = self.run.take_if(|run| run.key == key) else {
            return;
        };
        self.first_pending = false;
        let (code, message) = match end {
            End::StartFailed(failure) => {
                let message = match failure {
                    server::StartFailure::Spawn(error) => {
                        format!("could not start {}: {error}", run.driver.display())
                    }
                    other => other.describe(),
                };
                (McpListErrorCode::McpStartFailed, message)
            }
            End::Exited(how) => (McpListErrorCode::McpServerExited, how),
            End::MessageTooLarge => (
                McpListErrorCode::McpServerExited,
                server::message_too_large(),
            ),
            End::Stopped => return,
        };
        self.stop_run();
        if code == McpListErrorCode::McpStartFailed
            && let Some(Active::Starting { reply }) = self.active.take()
        {
            let _ = reply.send(Err(SetError::DriverFailed(message.clone())));
        }
        self.set_latest(Latest::Failure { code, message });
        self.next_request();
    }

    fn list_changed(&mut self, key: u64) {
        let Some(run) = self
            .run
            .as_mut()
            .filter(|run| run.key == key && run.open.is_some())
        else {
            return;
        };
        if run.listing.is_some() {
            run.again = true;
        } else {
            self.start_listing();
        }
    }

    fn start_listing(&mut self) {
        self.next_tag += 1;
        let tag = self.next_tag;
        let Some(run) = self.run.as_mut() else {
            return;
        };
        let Some(open) = run.open.as_ref() else {
            return;
        };
        run.listing = Some(Listing::new(state::seams::now(), tag, self.config.limits));
        list_tools(&open.peer, tag, None);
    }

    fn answered(&mut self, key: u64, tag: u64, answer: RawAnswer) {
        let Some(run) = self.run.as_mut().filter(|run| run.key == key) else {
            return;
        };
        let Some(listing) = run
            .listing
            .as_mut()
            .filter(|listing| listing.awaiting == tag)
        else {
            return;
        };
        match listing.answer(answer) {
            Step::Next(cursor) => {
                self.next_tag += 1;
                listing.awaiting = self.next_tag;
                let peer = &run
                    .open
                    .as_ref()
                    .expect("a listing runs on an open connection")
                    .peer;
                list_tools(peer, self.next_tag, Some(cursor));
            }
            Step::Done(tools) => {
                let server_info = run
                    .open
                    .as_ref()
                    .expect("a listing runs on an open connection")
                    .server_info
                    .clone();
                self.listing_ended(Latest::List { server_info, tools });
            }
            Step::Failed(message) => self.listing_ended(Latest::Failure {
                code: McpListErrorCode::McpListFailed,
                message,
            }),
        }
    }

    fn listing_deadline(&self) -> Option<Duration> {
        self.run
            .as_ref()?
            .listing
            .as_ref()
            .map(|listing| listing.deadline)
    }

    fn listing_timed_out(&mut self) {
        if self
            .listing_deadline()
            .is_some_and(|deadline| state::seams::now() >= deadline)
        {
            self.listing_ended(Latest::Failure {
                code: McpListErrorCode::McpListFailed,
                message: Listing::timed_out(),
            });
        }
    }

    /// The listing ended with `latest`; one more runs when a notification
    /// came during it.
    fn listing_ended(&mut self, latest: Latest) {
        state::seams::count("mcp.listed");
        self.first_pending = false;
        let again = self.run.as_mut().is_some_and(|run| {
            run.listing = None;
            std::mem::take(&mut run.again)
        });
        self.set_latest(latest);
        if again {
            self.start_listing();
        }
    }

    fn set_latest(&mut self, latest: Latest) {
        self.latest = Some(latest.clone());
        self.report(latest);
    }

    fn report(&mut self, latest: Latest) {
        if let Some(replacement) = self.queue.as_mut().and_then(|queue| queue.push(latest)) {
            self.latest = Some(replacement);
        }
    }

    fn begin_shutdown(&mut self, reply: oneshot::Sender<()>) {
        self.shutdown.get_or_insert_with(Vec::new).push(reply);
        self.stop_run();
        let active = self.active.take().map(|active| match active {
            Active::Stopping { reply, .. } | Active::Starting { reply } => reply,
        });
        let waiting = self.requests.drain(..).map(|request| request.reply);
        for reply in active.into_iter().chain(waiting) {
            let _ = reply.send(Err(shutting_down()));
        }
        self.shutdown_progress();
    }

    fn shutdown_progress(&mut self) {
        if !self.connections.is_empty() {
            return;
        }
        if let Some(waiters) = self.shutdown.as_mut() {
            for waiter in waiters.drain(..) {
                let _ = waiter.send(());
            }
        }
    }
}

/// Sends `tools/list` tagged `tag`, with `cursor` when it is not the first.
fn list_tools(peer: &Peer<RoleClient>, tag: u64, cursor: Option<String>) {
    let peer = peer.clone();
    tokio::spawn(async move {
        let request = ClientRequest::ListToolsRequest(ListToolsRequest::with_param(
            PaginatedRequestParams::default().with_cursor(cursor),
        ));
        let mut meta = serde_json::Map::new();
        meta.insert(transport::TAG_KEY.to_owned(), tag.into());
        let mut options = PeerRequestOptions::default();
        options.meta = Some(RequestMetaObject(MetaObject(meta)));
        // The answer comes as the tag's raw answer; a request that cannot be
        // written ends with its connection.
        let _ = peer.send_request_with_option(request, options).await;
    });
}

/// Writes `[computer_use]`: `enabled` with `driver` when enabled; disabled
/// keeping the recorded driver otherwise.
fn record(root: &StateRoot, enabled: bool, driver: Option<&Path>) -> Result<(), SetError> {
    let io = |error: String| SetError::Io(error);
    let mut config = state::config::read(root)
        .map_err(|error| io(error.to_string()))?
        .ok_or_else(|| io("config.toml is missing".to_owned()))?;
    config.set_computer_use(enabled, driver.map(Path::to_path_buf));
    let encoded = config.encode().map_err(|error| match error {
        EncodeError::TooLarge => SetError::InvalidArguments(
            "the driver path cannot fit in the configuration's 65,536-byte limit".to_owned(),
        ),
        EncodeError::Format => io(error.to_string()),
    })?;
    encoded
        .replace(root)
        .map_err(|failure| io(failure.to_string()))
}
