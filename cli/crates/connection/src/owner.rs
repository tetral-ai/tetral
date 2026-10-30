//! The Connection owner: one task that holds the connection to Engine
//! through its lifecycle, from the first attempt to `stopped`.
//!
//! Requests and stream reads run in their own tasks, each tagged with a
//! number unique in the process, and post their results to the owner's
//! mailbox; the owner applies a result only while its tag is still current,
//! so work of an ended attempt or connection never changes a later one. The
//! owner sets no timer for a deadline: it reads the seam clock whenever it
//! wakes, and while any deadline or wait is pending it wakes at least once
//! per recheck interval, so a deadline that passed while the machine slept
//! is acted on within that interval of waking. It acts on every passed time
//! before it handles the message it woke for, so an answer taken after a
//! deadline never undoes it; the test seam barrier `connection.message`
//! stands between taking a message and that check.
//!
//! Each event after a stream's first valid `connected` is handled in a
//! fixed order: its envelope and identities ([`crate::classify`]), the
//! connection's replay ledger, its payload's schema, then routing. A failure
//! is reported as a protocol error through the Report outbox
//! ([`crate::protocol_error`]), except that a `session.prepare` refused at
//! its admission gets a failed preparation report; none changes a task,
//! binding or result.
//!
//! A credential renewal runs on the held connection: the fetch, the write
//! of `credentials.json` and the resume with the new credential
//! ([`crate::credential`]). From the first resume attempt with it until
//! `connected` or the connection's end the owner starts no other request,
//! and a `401 credential_revoked` answer to a request sent with a credential
//! no longer in use is not applied: that request is sent again with the new
//! one. It tells the Report outbox the switch's start and the new key.
//!
//! The owner also admits each `session.prepare` before it reaches a Session
//! binding owner ([`crate::sessions`]) and holds the probe slots
//! ([`crate::probe_slots`]). Its stop is answered once every probe that
//! holds a slot has been reaped and every Session binding owner has
//! reported its tasks' processes gone, so that the shutdown waits for them
//! until its own deadline.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use drive::{DriveSync, DriveTransfer};
use observe::{Identity, STARTUP, TraceContext};
use protocol::bootstrap::BootstrapKey;
use protocol::connection::{
    DisconnectPayload, Downstream, DownstreamPayload, DownstreamType, Envelope, Heartbeat,
    LostChange, PreparationError, PreparationErrorCode, SessionOutcome, SessionStatus,
};
use protocol::errors::{ErrorBody, ErrorCode};
use protocol::field::{Nullable, PositiveInt};
use protocol::fingerprint::fingerprint;
use protocol::id::{Id, SafeId};
use protocol::local::{ConnectionState, ErrorObject, SessionEntry, SessionState};
use protocol::ports::{
    ActivitySink, Close, Drives, Prepare, ProbeSlot, ReportSink, SessionFactory, Submission,
    TicketRequest, ToolCall,
};
use protocol::time::Rfc3339Utc;
use protocol::tool::ProtocolVersion;
use state::clock::Clock;
use state::paths::StateRoot;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::attempt::{self, ATTEMPT_TOTAL, AttemptKind};
use crate::backoff::{BASE_MAX, BASE_START, Backoff, random_jitter};
use crate::classify::{self, Read, SessionMessage};
use crate::credential::{self, CREDENTIAL_PATH, INVALID_RESPONSE, Renewal};
use crate::drive_transfer::{DriveLink, HttpDriveTransfer};
use crate::git_ticket::{self, GIT_TICKET_PATH, TicketFetch};
use crate::heartbeat::Heartbeats;
use crate::http::{Answer, REQUEST_TOTAL, Transport};
use crate::ledger::{self, Entry, EntryState, Ledger, Need, Seen};
use crate::lost_changes::KeptLostChanges;
use crate::outbox::{Outbox, OutboxConfig, OutboxEvent, OutboxTimes};
use crate::probe_slots::{ProbeSlots, SlotLimits};
use crate::protocol_error::{self, ProtocolErrorKind};
use crate::ready::{self, ReadyFields};
use crate::sessions::{Owners, Port, PortMessage, RECORDS_MAX, SessionsView};
use crate::staging::{Admission, STAGING_MAX, Staging};
use crate::status::{ConnectionStatus, Reconnect, Reported};
use crate::stream::{EVENT_MAX_BYTES, EventLimit, TOOL_EVENT_MAX_BYTES};
use crate::task_pressure::TaskPressure;

/// The time from `connected` within which the ready ACK must arrive.
pub const REGISTRATION_DEADLINE: Duration = Duration::from_secs(45);

/// The time after App Server start within which `409 connection_conflict`
/// counts as a failed attempt, since the lease of an App Server that held
/// this root before may still last; later it blocks.
pub const CONFLICT_RETRY_WINDOW: Duration = Duration::from_secs(45);

/// The shortest wait after `409 previous_connection_active`.
pub const PREVIOUS_ACTIVE_MIN_WAIT: Duration = Duration::from_secs(1);

const READY_PATH: &str = "/v1/client/ready";
const HEARTBEAT_PATH: &str = "/v1/client/heartbeat";

/// The durations and limits the owner runs by, each a constant but for
/// tests.
#[derive(Debug, Clone, Copy)]
pub struct Times {
    pub attempt_total: Duration,
    pub request_total: Duration,
    pub registration_deadline: Duration,
    pub conflict_retry_window: Duration,
    pub previous_active_min_wait: Duration,
    pub backoff_start: Duration,
    pub backoff_max: Duration,
    pub staging_max: usize,
    /// The event limit of a stream until the CLI starts submitting a ready
    /// that advertises `tool_execution`.
    pub event_max_bytes: usize,
    /// The event limit from that moment, for the rest of the connection.
    pub tool_event_max_bytes: usize,
    pub ledger: ledger::Limits,
    pub outbox: OutboxTimes,
    pub records_max: usize,
    pub probe_slots: SlotLimits,
    /// The quiet interval of the task pressure point's episodes.
    pub task_pressure_quiet: Duration,
}

impl Default for Times {
    fn default() -> Times {
        Times {
            attempt_total: ATTEMPT_TOTAL,
            request_total: REQUEST_TOTAL,
            registration_deadline: REGISTRATION_DEADLINE,
            conflict_retry_window: CONFLICT_RETRY_WINDOW,
            previous_active_min_wait: PREVIOUS_ACTIVE_MIN_WAIT,
            backoff_start: BASE_START,
            backoff_max: BASE_MAX,
            staging_max: STAGING_MAX,
            event_max_bytes: EVENT_MAX_BYTES,
            tool_event_max_bytes: TOOL_EVENT_MAX_BYTES,
            ledger: ledger::Limits::default(),
            outbox: OutboxTimes::default(),
            records_max: RECORDS_MAX,
            probe_slots: SlotLimits::default(),
            task_pressure_quiet: observe::pressure::QUIET_INTERVAL,
        }
    }
}

/// The credential every request carries.
#[derive(Debug, Clone)]
pub struct Credential {
    pub credential_id: Id,
    pub key: BootstrapKey,
    pub expires_at: Rfc3339Utc,
}

#[derive(Clone)]
pub struct Config {
    pub server_url: String,
    pub instance_id: Id,
    pub credential: Credential,
    /// The state root whose `credentials.json` a renewal replaces.
    pub state_root: StateRoot,
    pub ready: ReadyFields,
    /// The App Server's start on the seam clock.
    pub started: Duration,
    /// Creates the Session binding owner of each Session.
    pub sessions: Arc<dyn SessionFactory>,
    /// Told of each tool and local MCP instruction read from the stream.
    pub activity: Arc<dyn ActivitySink>,
    pub times: Times,
}

/// A Session instruction as the Connection owner routes it, its payload
/// checked for a `session.close` and a `session.drives` at the ledger and
/// for a `session.prepare` at its admission; the owners of the others
/// decide their schema failures.
#[derive(Debug, Clone)]
pub struct Instruction {
    pub message: SessionMessage,
    /// Whether the replay ledger recorded it. A new operation it had no room
    /// for is refused with `resource_exhausted`, a `session.prepare` at its
    /// admission; a `session.close` or `session.drives` is handled without
    /// an entry.
    pub recorded: bool,
    /// Whether the ledger took it as a new operation: a `session.prepare` of
    /// a binding not yet routed on this connection, a `tool.execute` or an
    /// `mcp.call`.
    pub new_operation: bool,
    /// The instruction span its work runs in, carrying its identities and,
    /// when its `traceparent` is usable, its `trace_id`.
    pub span: tracing::Span,
    pub trace: Option<TraceContext>,
    /// When the stream delivered it, on the seam clock.
    pub received: Duration,
}

pub(crate) enum Message {
    Status(oneshot::Sender<ConnectionStatus>),
    Stop(oneshot::Sender<()>),
    /// The events request of stream `stream` got no stream.
    Refused {
        stream: u64,
        answer: Answer,
    },
    Event {
        stream: u64,
        data: String,
    },
    StreamEnded {
        stream: u64,
    },
    Answered {
        request: u64,
        answer: Answer,
    },
    /// The write of `credential` by the task tagged `tag` ended.
    Persisted {
        tag: u64,
        credential: Credential,
        saved: bool,
    },
    Outbox(OutboxEvent),
    Port(PortMessage),
    Sessions(oneshot::Sender<SessionsView>),
    DriveSyncs(DriveSyncs),
    DriveSyncList(oneshot::Sender<Vec<DriveSync>>),
}

/// A change of the drive syncs the Connection owner signals.
pub(crate) enum DriveSyncs {
    Added(SafeId, DriveSync),
    Removed(SafeId),
    Mounted(Id, SafeId),
    Detached(Id, SafeId),
}

/// A handle on the Connection owner.
#[derive(Debug, Clone)]
pub struct Connection {
    mailbox: mpsc::UnboundedSender<Message>,
    status: tokio::sync::watch::Receiver<ConnectionStatus>,
    drive_transfer: Arc<HttpDriveTransfer>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("connection message")
    }
}

impl Connection {
    /// Starts the owner, whose first attempt starts at once unless the
    /// credential's `expires_at` is not after the system's current time.
    pub fn spawn(config: Config) -> Connection {
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let (events, reports) = mpsc::unbounded_channel();
        let transport = Arc::new(Transport::new(&config.server_url));
        let (drive_link, link) = DriveLink::new(config.credential.key.clone());
        let drive_transfer = Arc::new(HttpDriveTransfer::with_transport(
            transport.clone(),
            link,
            events.clone(),
            Clock::seams(),
        ));
        let outbox = Outbox::spawn(OutboxConfig {
            transport: transport.clone(),
            key: config.credential.key.clone(),
            times: config.times.outbox,
            clock: Clock::seams(),
            events,
        });
        let port = Arc::new(Port {
            mailbox: mailbox.clone(),
        });
        let sink: Arc<dyn ReportSink> = Arc::new(outbox.clone());
        let owners = Owners::new(
            config.sessions.clone(),
            port,
            sink,
            config.times.records_max,
        );
        let owner = Owner::new(
            config,
            transport,
            outbox,
            drive_link,
            owners,
            mailbox.clone(),
        );
        let (status, snapshot) = tokio::sync::watch::channel(owner.status());
        tokio::spawn(own(owner, inbox, reports, status));
        Connection {
            mailbox,
            status: snapshot,
            drive_transfer,
        }
    }

    /// The drive transfer client of the connection's credential, for the
    /// Drive sync owners and the Content cache.
    pub fn drive_transfer(&self) -> Arc<dyn DriveTransfer> {
        self.drive_transfer.clone()
    }

    /// The last status published by the owner, without waiting for its mailbox.
    pub fn status_snapshot(&self) -> ConnectionStatus {
        self.status.borrow().clone()
    }

    pub async fn status(&self) -> ConnectionStatus {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Status(reply));
        answer
            .await
            .expect("the Connection owner answers every message")
    }

    /// Adds `session_id`'s Drive sync owner, which is told of every
    /// `connected`.
    pub fn add_drive_sync(&self, session_id: SafeId, sync: DriveSync) {
        self.send(Message::DriveSyncs(DriveSyncs::Added(session_id, sync)));
    }

    pub fn remove_drive_sync(&self, session_id: SafeId) {
        self.send(Message::DriveSyncs(DriveSyncs::Removed(session_id)));
    }

    /// `session_id`'s drive sync has `drive_id` mounted: it gets the
    /// drive's `drive.changed` notices until it is detached.
    pub fn drive_mounted(&self, drive_id: Id, session_id: SafeId) {
        self.send(Message::DriveSyncs(DriveSyncs::Mounted(
            drive_id, session_id,
        )));
    }

    pub fn drive_detached(&self, drive_id: Id, session_id: SafeId) {
        self.send(Message::DriveSyncs(DriveSyncs::Detached(
            drive_id, session_id,
        )));
    }

    /// Each Session's Drive sync owner.
    pub async fn drive_syncs(&self) -> Vec<DriveSync> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::DriveSyncList(reply));
        answer.await.unwrap_or_default()
    }

    /// The entries of `GET /v1/sessions`: each Session binding owner's,
    /// which this caller asks for, since the Connection owner never waits
    /// for another owner, then each preparation still staged.
    pub async fn sessions(&self) -> Vec<SessionEntry> {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Sessions(reply));
        let Ok(view) = answer.await else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        for owner in view.owners {
            let (reply, answer) = oneshot::channel();
            owner.entries(Box::new(move |each| {
                let _ = reply.send(each);
            }));
            if let Ok(each) = answer.await {
                entries.extend(each);
            }
        }
        entries.extend(view.staged);
        entries
    }

    /// Stops handling downstream messages, closes the stream, cancels every
    /// request, waiting probe slot and preparation, and returns once every
    /// probe that holds a slot has been reaped: `stopping`, then `stopped`,
    /// which lasts.
    pub async fn stop(&self) {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Stop(reply));
        let _ = answer.await;
    }

    fn send(&self, message: Message) {
        self.mailbox
            .send(message)
            .unwrap_or_else(|_| panic!("the Connection owner runs until the process exits"));
    }
}

async fn own(
    mut owner: Owner,
    mut inbox: mpsc::UnboundedReceiver<Message>,
    mut reports: mpsc::UnboundedReceiver<OutboxEvent>,
    status: tokio::sync::watch::Sender<ConnectionStatus>,
) {
    owner.start();
    status.send_replace(owner.status());
    loop {
        let wake = owner.wake_in();
        let message = tokio::select! {
            message = inbox.recv() => match message {
                Some(message) => Some(message),
                None => return,
            },
            Some(event) = reports.recv() => Some(Message::Outbox(event)),
            () = sleep_or_wait(wake) => None,
        };
        if let Some(message) = message {
            state::seams::barrier("connection.message").await;
            owner.check();
            owner.handle(message);
        }
        owner.check();
        status.send_replace(owner.status());
        // Stop waiters may read the snapshot as soon as their reply wakes them.
        owner.answer_stop();
    }
}

async fn sleep_or_wait(wake: Option<Duration>) {
    match wake {
        Some(wake) => tokio::time::sleep(wake).await,
        None => std::future::pending().await,
    }
}

enum Phase {
    /// Shown as `disconnected`.
    Waiting {
        at: Duration,
        kind: AttemptKind,
    },
    Connecting(Attempt),
    /// `registering`, `online` or `resuming`.
    Held(Box<Held>),
    Blocked,
    Stopped,
}

struct Attempt {
    stream: u64,
    kind: AttemptKind,
    /// The credential it authenticates with, the one in use from its
    /// `connected` on.
    credential: Credential,
    deadline: Duration,
    task: AbortHandle,
    /// The event limit its stream reads with.
    limit: EventLimit,
}

struct Held {
    id: Id,
    stream: Stream,
    /// The connection deadline, from the ACK that last set it.
    deadline: Option<Duration>,
    /// Until the ready ACK.
    registration: Option<Registration>,
    /// The number of the last upstream message, its `message_id` `c<n>`.
    messages: u64,
    heartbeats: Heartbeats,
    /// The latest heartbeat, until it is acknowledged or given up.
    heartbeat: Option<Outgoing>,
    ledger: Ledger,
    /// The `(session_id, binding_id)` of each `session.prepare` routed on
    /// this connection; a later one of the same binding is no new operation.
    routed: HashSet<(String, String)>,
    /// The event limit that a resume's stream starts with.
    event_max_bytes: usize,
    renewal: Option<Renewal>,
    /// The credentials of `credential.renew` notices not yet handled, each
    /// once: those received before `online` and during another renewal.
    notices: VecDeque<Id>,
    /// The git-ticket fetches until their answers, by number.
    tickets: HashMap<u64, TicketFetch>,
}

enum Stream {
    Live {
        stream: u64,
        task: AbortHandle,
    },
    /// Lost: the next resume attempt waits for `at`.
    Waiting {
        at: Duration,
    },
    Resuming(Attempt),
}

struct Registration {
    deadline: Duration,
    /// `None` when ready failed its own decoder.
    ready: Option<Outgoing>,
    /// The `lost_changes` entries kept when ready was built, which its ACK
    /// removes.
    lost_changes: Vec<LostChange>,
    staging: Staging<Instruction>,
}

/// An upstream message that is sent again, with its `message_id` and first
/// send time, until it is answered.
struct Outgoing {
    /// The stream that delivered the event, preserved through retries.
    stream: u64,
    message_id: Id,
    body: Vec<u8>,
    first_send: Duration,
    seq: i64,
    backoff: Backoff,
    retry_at: Option<Duration>,
}

#[derive(Clone)]
enum Operation {
    Ready,
    Heartbeat(HeartbeatRequest),
    Credential,
    /// The git-ticket fetch of that number.
    GitTicket(u64),
}

#[derive(Clone)]
struct HeartbeatRequest {
    seq: i64,
    message_id: Id,
    first_send: Duration,
    stream: u64,
}

struct Pending {
    connection: Id,
    operation: Operation,
    /// The credential the request carries.
    credential_id: Id,
    task: AbortHandle,
}

struct Owner {
    server_url: String,
    instance_id: Id,
    /// The credential in use, from the `connected` of the attempt that
    /// carried it.
    credential: Credential,
    /// A renewed credential written while no connection took it, which the
    /// next attempt carries, as a restart would.
    written: Option<Credential>,
    state_root: StateRoot,
    ready: ReadyFields,
    started: Duration,
    times: Times,
    transport: Arc<Transport>,
    outbox: Outbox,
    /// The drive transfer client's view of the held connection and its key.
    drive: DriveLink,
    mailbox: mpsc::UnboundedSender<Message>,
    phase: Phase,
    /// The reconnect backoff; each upstream message has its own.
    backoff: Backoff,
    last_error: Option<ErrorObject>,
    /// The last connection held, the predecessor of a new connection.
    last_held: Option<Id>,
    held_count: u64,
    last_tag: u64,
    requests: HashMap<u64, Pending>,
    reported: Reported,
    owners: Owners,
    /// Each Session's Drive sync owner, and the drive syncs that have each
    /// drive mounted.
    drive_syncs: HashMap<SafeId, DriveSync>,
    mounted_drives: HashMap<Id, BTreeSet<SafeId>>,
    slots: ProbeSlots,
    /// The task pressure point `tetral_tasks`.
    task_pressure: TaskPressure,
    /// The replies to stop, sent once no probe holds a slot and every
    /// Session's tasks are confirmed gone.
    stop_replies: Vec<oneshot::Sender<()>>,
    /// The Sessions whose tasks the stop still waits for.
    tasks_pending: BTreeSet<SafeId>,
    lost_changes: KeptLostChanges,
    activity: Arc<dyn ActivitySink>,
}

impl Owner {
    fn new(
        config: Config,
        transport: Arc<Transport>,
        outbox: Outbox,
        drive: DriveLink,
        owners: Owners,
        mailbox: mpsc::UnboundedSender<Message>,
    ) -> Owner {
        let times = config.times;
        Owner {
            owners,
            drive_syncs: HashMap::new(),
            mounted_drives: HashMap::new(),
            slots: ProbeSlots::new(times.probe_slots),
            task_pressure: TaskPressure::new(observe::registry(), times.task_pressure_quiet),
            stop_replies: Vec::new(),
            tasks_pending: BTreeSet::new(),
            transport,
            outbox,
            drive,
            server_url: config.server_url,
            instance_id: config.instance_id,
            credential: config.credential,
            written: None,
            lost_changes: KeptLostChanges::load(&config.state_root),
            activity: config.activity,
            state_root: config.state_root,
            ready: config.ready,
            started: config.started,
            times,
            mailbox,
            phase: Phase::Stopped,
            backoff: Backoff::new(times.backoff_start, times.backoff_max, random_jitter),
            last_error: None,
            last_held: None,
            held_count: 0,
            last_tag: 0,
            requests: HashMap::new(),
            reported: Reported::new(ConnectionState::Disconnected),
        }
    }

    fn start(&mut self) {
        let expires_at = protocol::time::system_time(&self.credential.expires_at);
        self.phase = if expires_at <= SystemTime::now() {
            self.last_error = Some(ErrorObject {
                code: "credential_expired".to_owned(),
                message: "the credential's expires_at is not after this machine's current time"
                    .to_owned(),
            });
            Phase::Blocked
        } else {
            let kind = AttemptKind::Fresh { previous: None };
            let limit = self.times.event_max_bytes;
            let credential = self.credential.clone();
            Phase::Connecting(self.attempt(kind, state::seams::now(), limit, credential))
        };
        self.reported.change(self.state());
    }

    fn handle(&mut self, message: Message) {
        let now = match message {
            Message::Status(reply) => {
                let _ = reply.send(self.status());
                return;
            }
            Message::Stop(reply) => {
                self.stop();
                self.stop_replies.push(reply);
                return;
            }
            Message::Port(message) => {
                self.port_message(message);
                return;
            }
            Message::Sessions(reply) => {
                let _ = reply.send(self.sessions_view());
                return;
            }
            Message::DriveSyncs(change) => {
                self.drive_syncs_changed(change);
                return;
            }
            Message::DriveSyncList(reply) => {
                let _ = reply.send(self.drive_syncs.values().cloned().collect());
                return;
            }
            _ if matches!(self.phase, Phase::Stopped) => return,
            _ => state::seams::now(),
        };
        let phase = std::mem::replace(&mut self.phase, Phase::Stopped);
        self.phase = match message {
            Message::Refused { stream, answer } => self.refused(phase, stream, answer, now),
            Message::Event { stream, data } => self.event(phase, stream, data, now),
            Message::StreamEnded { stream } => self.stream_ended(phase, stream, now),
            Message::Answered { request, answer } => self.answered(phase, request, answer, now),
            Message::Persisted {
                tag,
                credential,
                saved,
            } => self.persisted(phase, tag, credential, saved, now),
            Message::Outbox(event) => self.outbox_event(phase, event, now),
            Message::Status(_)
            | Message::Stop(_)
            | Message::Port(_)
            | Message::Sessions(_)
            | Message::DriveSyncs(_)
            | Message::DriveSyncList(_) => phase,
        };
        self.reported.change(self.state());
    }

    fn state(&self) -> ConnectionState {
        match &self.phase {
            Phase::Waiting { .. } => ConnectionState::Disconnected,
            Phase::Connecting(_) => ConnectionState::Connecting,
            Phase::Held(held) => match (&held.stream, &held.registration) {
                (Stream::Live { .. }, Some(_)) => ConnectionState::Registering,
                (Stream::Live { .. }, None) => ConnectionState::Online,
                (Stream::Waiting { .. } | Stream::Resuming(_), _) => ConnectionState::Resuming,
            },
            Phase::Blocked => ConnectionState::Blocked,
            Phase::Stopped => ConnectionState::Stopped,
        }
    }

    fn status(&self) -> ConnectionStatus {
        ConnectionStatus {
            state: self.state(),
            connection_id: match &self.phase {
                Phase::Held(held) => Some(held.id.clone()),
                _ => None,
            },
            credential_id: self.credential.credential_id.clone(),
            expires_at: self.credential.expires_at.clone(),
            last_error: self.last_error.clone(),
        }
    }

    fn stop(&mut self) {
        if matches!(self.phase, Phase::Stopped) {
            return;
        }
        self.reported.change(ConnectionState::Stopping);
        // Before the connection's end, so that tasks end with their grace.
        self.tasks_pending = self.owners.shutdown();
        let phase = std::mem::replace(&mut self.phase, Phase::Stopped);
        self.release(phase);
        for (_, pending) in self.requests.drain() {
            pending.task.abort();
        }
        for requester in self.slots.cancel(state::seams::now()) {
            self.owners.answer(&requester, ProbeSlot::Cancelled);
        }
        self.reported.change(ConnectionState::Stopped);
    }

    fn answer_stop(&mut self) {
        if self.slots.running() == 0 && self.tasks_pending.is_empty() {
            for reply in self.stop_replies.drain(..) {
                let _ = reply.send(());
            }
        }
    }

    /// A Session binding owner's call. A slot is asked for only while a
    /// connection is held; any other time the request is `Cancelled`, as
    /// the connection's end and the stop cancel the waiting ones.
    fn port_message(&mut self, message: PortMessage) {
        match message {
            PortMessage::Slot(requester) => {
                if !matches!(self.phase, Phase::Held(_)) {
                    self.owners.answer(&requester, ProbeSlot::Cancelled);
                } else if self.slots.request(requester.clone(), state::seams::now()) {
                    self.owners.answer(&requester, ProbeSlot::Granted);
                }
            }
            PortMessage::Withdraw(requester) => {
                self.slots.withdraw(&requester, state::seams::now());
            }
            PortMessage::Free => {
                if let Some(next) = self.slots.free(state::seams::now()) {
                    self.owners.answer(&next, ProbeSlot::Granted);
                }
            }
            PortMessage::GitTicket {
                session_id,
                request,
                connection_id,
                binding_id,
            } => self.fetch_git_ticket(session_id, request, &connection_id, &binding_id),
            PortMessage::RecordGone(session_id) => self.owners.record_gone(&session_id),
            PortMessage::OwnerIdle(session_id, received) => {
                self.owners.idle(&session_id, received);
            }
            PortMessage::TasksGone(session_id) => {
                self.tasks_pending.remove(&session_id);
            }
            PortMessage::TaskStaged(staged) => {
                self.task_pressure.staged(staged, state::seams::now());
            }
            PortMessage::TaskUnstaged(staged) => {
                self.task_pressure.unstaged(staged, state::seams::now());
            }
            PortMessage::TaskStarted(staged) => {
                self.task_pressure.started(staged, state::seams::now());
            }
            PortMessage::TaskEnded => self.task_pressure.ended(state::seams::now()),
            PortMessage::TaskRefused => self.task_pressure.refused(state::seams::now()),
            PortMessage::KeepLostChanges(entries, kept) => {
                self.lost_changes.keep(entries);
                (kept.0)();
            }
        }
    }

    /// The staged preparations, each binding once, and the owners.
    fn sessions_view(&self) -> SessionsView {
        let mut staged: Vec<SessionEntry> = Vec::new();
        if let Phase::Held(held) = &self.phase
            && let Some(registration) = &held.registration
        {
            for instruction in registration.staging.iter() {
                let message = &instruction.message;
                if message.envelope.kind != DownstreamType::SessionPrepare {
                    continue;
                }
                let (Ok(session_id), Ok(binding_id)) = (
                    Id::new(message.session_id.as_str()),
                    Id::new(message.binding_id.as_str()),
                ) else {
                    continue;
                };
                let entry = SessionEntry {
                    session_id,
                    binding_id,
                    connection_id: held.id.clone(),
                    cwd: Nullable(None),
                    state: SessionState::Staged,
                    error: Nullable(None),
                };
                if !staged.contains(&entry) {
                    staged.push(entry);
                }
            }
        }
        SessionsView {
            owners: self.owners.handles(),
            staged,
        }
    }

    /// The time until the owner must read the clock again, while anything
    /// waits for a time.
    fn wake_in(&self) -> Option<Duration> {
        let phase = match &self.phase {
            Phase::Waiting { at, .. } => Some(*at),
            Phase::Connecting(attempt) => Some(attempt.deadline),
            Phase::Held(held) => Some(held.next_time()),
            Phase::Blocked | Phase::Stopped => None,
        };
        let next = phase.into_iter().chain(self.slots.next_time()).min()?;
        let recheck = state::seams::recheck_interval();
        Some(next.saturating_sub(state::seams::now()).min(recheck))
    }

    /// Acts on every time that has come, then counts the check as the test
    /// seam counter `connection.checked`.
    fn check(&mut self) {
        let now = state::seams::now();
        let phase = std::mem::replace(&mut self.phase, Phase::Stopped);
        self.phase = match phase {
            Phase::Waiting { at, kind } if now >= at => {
                let limit = self.times.event_max_bytes;
                let credential = self.written.clone().unwrap_or(self.credential.clone());
                Phase::Connecting(self.attempt(kind, now, limit, credential))
            }
            Phase::Connecting(attempt) if now >= attempt.deadline => {
                self.attempt_failed(Phase::Connecting(attempt), now)
            }
            Phase::Held(held) => self.check_held(held, now),
            other => other,
        };
        for requester in self.slots.expire(now) {
            self.owners.answer(&requester, ProbeSlot::Exhausted);
        }
        self.task_pressure.show(now);
        self.reported.change(self.state());
        state::seams::count("connection.checked");
    }

    fn check_held(&mut self, mut held: Box<Held>, now: Duration) -> Phase {
        let registration_passed = held
            .registration
            .as_ref()
            .is_some_and(|registration| now >= registration.deadline);
        if registration_passed || held.deadline.is_some_and(|deadline| now >= deadline) {
            return self.end(Phase::Held(held), now);
        }
        match &held.stream {
            Stream::Waiting { at } if now >= *at => {
                held.ledger.resume_started();
                let kind = AttemptKind::Resume {
                    connection: held.id.clone(),
                };
                let credential = match &held.renewal {
                    Some(Renewal::Switching { credential }) => credential.clone(),
                    _ => self.credential.clone(),
                };
                let limit = held.event_max_bytes;
                held.stream = Stream::Resuming(self.attempt(kind, now, limit, credential));
            }
            Stream::Resuming(attempt) if now >= attempt.deadline => {
                attempt.task.abort();
                held.stream = Stream::Waiting {
                    at: now + self.backoff.next_wait(),
                };
            }
            _ => {}
        }
        self.send_due(&mut held, now);
        Phase::Held(held)
    }

    /// Sends ready, the latest heartbeat and a renewal's fetch again once
    /// their retry is due; during a switch it sends nothing.
    fn send_due(&mut self, held: &mut Held, now: Duration) {
        if let Some(Renewal::Fetching { body, retry_at, .. }) = held.renewal.as_mut()
            && retry_at.is_some_and(|at| now >= at)
        {
            *retry_at = None;
            let body = body.clone();
            self.post(&held.id, CREDENTIAL_PATH, Operation::Credential, body);
        }
        if matches!(held.renewal, Some(Renewal::Switching { .. })) {
            return;
        }
        let due: Vec<u64> = held
            .tickets
            .iter_mut()
            .filter(|(_, fetch)| fetch.retry_at.is_some_and(|at| now >= at))
            .map(|(number, fetch)| {
                fetch.retry_at = None;
                *number
            })
            .collect();
        for number in due {
            let body = held.tickets[&number].body.clone();
            self.post(
                &held.id,
                GIT_TICKET_PATH,
                Operation::GitTicket(number),
                body,
            );
        }
        if let Some(ready) = held
            .registration
            .as_mut()
            .and_then(|registration| registration.ready.as_mut())
            && ready.retry_at.is_some_and(|at| now >= at)
        {
            ready.retry_at = None;
            let body = ready.body.clone();
            self.post(&held.id, READY_PATH, Operation::Ready, body);
        }
        if let Some(heartbeat) = held.heartbeat.as_mut()
            && heartbeat.retry_at.is_some_and(|at| now >= at)
        {
            heartbeat.retry_at = None;
            let operation = Operation::Heartbeat(HeartbeatRequest {
                seq: heartbeat.seq,
                message_id: heartbeat.message_id.clone(),
                first_send: heartbeat.first_send,
                stream: heartbeat.stream,
            });
            let body = heartbeat.body.clone();
            self.post(&held.id, HEARTBEAT_PATH, operation, body);
        }
    }

    fn tag(&mut self) -> u64 {
        self.last_tag += 1;
        self.last_tag
    }

    /// Starts an attempt with `credential` whose stream reads events of up
    /// to `max_event_bytes`, counted as the test seam counter
    /// `connection.attempt`.
    fn attempt(
        &mut self,
        kind: AttemptKind,
        now: Duration,
        max_event_bytes: usize,
        credential: Credential,
    ) -> Attempt {
        state::seams::count("connection.attempt");
        let stream = self.tag();
        let limit = EventLimit::new(max_event_bytes);
        let task = tokio::spawn(attempt::run(
            self.transport.clone(),
            credential.key.clone(),
            kind.query(&self.instance_id, &credential.credential_id),
            limit.clone(),
            stream,
            self.mailbox.clone(),
        ))
        .abort_handle();
        Attempt {
            stream,
            kind,
            credential,
            deadline: now + self.times.attempt_total,
            task,
            limit,
        }
    }

    fn post(&mut self, connection: &Id, path: &'static str, operation: Operation, body: Vec<u8>) {
        let request = self.tag();
        let transport = self.transport.clone();
        let key = self.credential.key.clone();
        let mailbox = self.mailbox.clone();
        let deadline = state::seams::now() + self.times.request_total;
        let task = tokio::spawn(async move {
            let answer = transport.post(path, &key, body, deadline).await;
            let _ = mailbox.send(Message::Answered { request, answer });
        })
        .abort_handle();
        self.requests.insert(
            request,
            Pending {
                connection: connection.clone(),
                operation,
                credential_id: self.credential.credential_id.clone(),
                task,
            },
        );
    }

    /// The next attempt, a new connection, after the backoff's wait.
    fn waiting(&mut self, now: Duration) -> Phase {
        Phase::Waiting {
            at: now + self.backoff.next_wait(),
            kind: AttemptKind::Fresh {
                previous: self.last_held.clone(),
            },
        }
    }

    /// Ends the phase's attempt or connection; a new connection follows
    /// under the backoff.
    fn end(&mut self, phase: Phase, now: Duration) -> Phase {
        self.release(phase);
        self.waiting(now)
    }

    fn blocked(&mut self, phase: Phase, error: ErrorObject) -> Phase {
        self.release(phase);
        self.last_error = Some(error);
        Phase::Blocked
    }

    /// Closes the phase's stream. The requests of an ended connection run
    /// to their answer, which no later connection takes.
    fn release(&mut self, phase: Phase) {
        match phase {
            Phase::Connecting(attempt) => attempt.task.abort(),
            Phase::Held(held) => self.release_held(*held),
            Phase::Waiting { .. } | Phase::Blocked | Phase::Stopped => {}
        }
    }

    fn refused(&mut self, phase: Phase, stream: u64, answer: Answer, now: Duration) -> Phase {
        let current = match &phase {
            Phase::Connecting(attempt) => attempt.stream == stream,
            Phase::Held(held) => {
                matches!(&held.stream, Stream::Resuming(attempt) if attempt.stream == stream)
            }
            _ => false,
        };
        if !current {
            return phase;
        }
        let Answer::Refused(error) = answer else {
            return self.attempt_failed(phase, now);
        };
        self.last_error = Some(error_object(&error));
        match error.code {
            ErrorCode::ConnectionConflict => self.conflict(phase, &error, now),
            ErrorCode::PreviousConnectionActive => {
                let asked = error.retry_after_ms.map_or(0, |ms| ms.get().unsigned_abs());
                let at =
                    now + Duration::from_millis(asked).max(self.times.previous_active_min_wait);
                match phase {
                    Phase::Held(mut held) => {
                        held.stream = Stream::Waiting { at };
                        Phase::Held(held)
                    }
                    _ => Phase::Waiting {
                        at,
                        kind: AttemptKind::Fresh {
                            previous: self.last_held.clone(),
                        },
                    },
                }
            }
            ErrorCode::StaleConnection => self.end(phase, now),
            _ => self.blocked(phase, error_object(&error)),
        }
    }

    /// The phase's attempt ended without a valid `connected`: the next one
    /// waits under the backoff.
    fn attempt_failed(&mut self, phase: Phase, now: Duration) -> Phase {
        match phase {
            Phase::Connecting(attempt) => {
                attempt.task.abort();
                self.waiting(now)
            }
            Phase::Held(mut held) => {
                if let Stream::Resuming(attempt) = &held.stream {
                    attempt.task.abort();
                }
                held.stream = Stream::Waiting {
                    at: now + self.backoff.next_wait(),
                };
                Phase::Held(held)
            }
            other => other,
        }
    }

    fn conflict(&mut self, phase: Phase, error: &ErrorBody, now: Duration) -> Phase {
        if now.saturating_sub(self.started) < self.times.conflict_retry_window {
            self.end(phase, now)
        } else {
            self.blocked(phase, error_object(error))
        }
    }

    fn event(&mut self, phase: Phase, stream: u64, data: String, now: Duration) -> Phase {
        match phase {
            Phase::Connecting(attempt) if attempt.stream == stream => {
                match attempt.kind.connected(&data) {
                    Some((connection, false)) => {
                        self.adopt(attempt.credential);
                        self.new_connection(connection, stream, attempt.task, &attempt.limit, now)
                    }
                    _ => Phase::Connecting(attempt),
                }
            }
            Phase::Held(mut held) => {
                let resume = match &held.stream {
                    Stream::Resuming(attempt) if attempt.stream == stream => Some((
                        attempt.kind.connected(&data),
                        attempt.task.clone(),
                        attempt.limit.clone(),
                        attempt.credential.clone(),
                    )),
                    _ => None,
                };
                let live =
                    matches!(&held.stream, Stream::Live { stream: live, .. } if *live == stream);
                match resume {
                    Some((Some((_, true)), task, _, credential)) => {
                        held.stream = Stream::Live { stream, task };
                        self.reported.reconnected(Reconnect::Resume);
                        self.signal_connected();
                        if matches!(held.renewal, Some(Renewal::Switching { .. })) {
                            held.renewal = None;
                            self.adopt(credential);
                            let key = self.credential.key.clone();
                            self.drive.switch_ended(&held.id, key.clone());
                            self.outbox.switch_ended(held.id.clone(), key);
                            self.next_renewal(&mut held);
                        }
                        Phase::Held(held)
                    }
                    Some((Some((connection, false)), task, limit, credential)) => {
                        // The old connection ends, but not this stream, which
                        // carries the new one.
                        held.stream = Stream::Waiting { at: now };
                        self.release_held(*held);
                        self.adopt(credential);
                        self.new_connection(connection, stream, task, &limit, now)
                    }
                    Some((None, _, _, _)) => Phase::Held(held),
                    None if live => self.live_event(held, stream, data, now),
                    None => Phase::Held(held),
                }
            }
            other => other,
        }
    }

    /// Starts connection `id` on stream `stream`, whose reader reads with
    /// `limit`: the smaller event limit until the ready is submitted, the
    /// larger one from then on when that ready advertises `tool_execution`,
    /// so that no `tool.execute` is cut before the ready that allows it.
    fn new_connection(
        &mut self,
        id: Id,
        stream: u64,
        task: AbortHandle,
        limit: &EventLimit,
        now: Duration,
    ) -> Phase {
        limit.set(self.times.event_max_bytes);
        STARTUP.first_connected(now);
        self.signal_connected();
        if self.held_count > 0 {
            self.reported.reconnected(Reconnect::New);
        }
        self.held_count += 1;
        self.last_held = Some(id.clone());
        let tool_execution = self.ready.capabilities.tool_execution.is_some();
        self.drive
            .connection_started(id.clone(), self.credential.key.clone());
        self.outbox.connection_started(
            self.held_count,
            id.clone(),
            self.credential.key.clone(),
            tool_execution,
        );
        let mut held = Box::new(Held {
            id,
            stream: Stream::Live { stream, task },
            deadline: None,
            registration: None,
            messages: 0,
            heartbeats: Heartbeats::default(),
            heartbeat: None,
            ledger: Ledger::new(self.times.ledger),
            routed: HashSet::new(),
            event_max_bytes: self.times.event_max_bytes,
            renewal: None,
            notices: VecDeque::new(),
            tickets: HashMap::new(),
        });
        let message_id = held.next_message_id();
        let lost_changes = self.lost_changes.entries().to_vec();
        let built = ready::ready(
            &self.ready,
            &held.id,
            message_id.clone(),
            now,
            &lost_changes,
        );
        let ready = ready::checked(&built, &held.id, &message_id).map(|body| Outgoing {
            stream,
            message_id,
            body,
            first_send: now,
            seq: 0,
            backoff: self.message_backoff(),
            retry_at: None,
        });
        if let Some(ready) = &ready {
            if tool_execution {
                held.event_max_bytes = self.times.tool_event_max_bytes;
                limit.set(held.event_max_bytes);
            }
            self.post(&held.id, READY_PATH, Operation::Ready, ready.body.clone());
        }
        held.registration = Some(Registration {
            deadline: now + self.times.registration_deadline,
            ready,
            lost_changes,
            staging: Staging::with_capacity(self.times.staging_max),
        });
        Phase::Held(held)
    }

    fn message_backoff(&self) -> Backoff {
        Backoff::new(
            self.times.backoff_start,
            self.times.backoff_max,
            random_jitter,
        )
    }

    /// An event of the live stream `stream`, which began with a valid
    /// `connected`.
    fn live_event(&mut self, held: Box<Held>, stream: u64, data: String, now: Duration) -> Phase {
        match classify::read(data) {
            Read::Unparseable { kind, message_id } => {
                self.protocol_error(&held.id, kind, message_id, None);
                Phase::Held(held)
            }
            Read::Other(envelope, data) => self.connection_message(held, &envelope, &data, now),
            Read::Session(message) => {
                if message.is_activity() {
                    self.activity.tool_instruction_arrived(now);
                }
                self.session_instruction(held, stream, message, now)
            }
        }
    }

    fn session_instruction(
        &mut self,
        mut held: Box<Held>,
        stream: u64,
        message: SessionMessage,
        now: Duration,
    ) -> Phase {
        let trace = message
            .envelope
            .traceparent
            .as_deref()
            .and_then(TraceContext::parse);
        let span = instruction_span(&held.id, &message, trace.as_ref());
        let entered = span.clone().entered();
        let message_id = message.envelope.message_id.clone();
        let (recorded, new_operation) =
            match admit(&mut held.ledger, &held.routed, &message, stream) {
                Admitted::Duplicate => return Phase::Held(held),
                Admitted::Refused(kind) => {
                    self.protocol_error(&held.id, kind, Some(message_id), trace.as_ref());
                    return Phase::Held(held);
                }
                Admitted::Route {
                    recorded,
                    new_operation,
                } => (recorded, new_operation),
            };
        drop(entered);
        let instruction = Instruction {
            message,
            recorded,
            new_operation,
            span,
            trace,
            received: now,
        };
        let admission = match held.registration.as_mut() {
            Some(registration) => registration.staging.admit(instruction),
            None => Admission::Route(instruction),
        };
        match admission {
            Admission::Staged => Phase::Held(held),
            Admission::Route(instruction) => {
                self.route(&mut held, instruction);
                Phase::Held(held)
            }
            Admission::Overflow => self.end(Phase::Held(held), now),
        }
    }

    /// A message for the Connection owner's own handlers.
    fn connection_message(
        &mut self,
        mut held: Box<Held>,
        envelope: &Envelope,
        data: &str,
        now: Duration,
    ) -> Phase {
        if envelope.kind == DownstreamType::Connected {
            observe::warn!(
                "connection.unexpected_connected",
                connection.id = observe::Id(held.id.as_str())
            );
            return Phase::Held(held);
        }
        if envelope.connection_id != held.id {
            observe::warn!(
                "protocol.foreign_connection",
                connection.id = observe::Id(held.id.as_str()),
                message.id = observe::Id(envelope.message_id.as_str())
            );
            return Phase::Held(held);
        }
        let Ok(downstream) = protocol::decode::<Downstream>(data.as_bytes()) else {
            let kind = ProtocolErrorKind::InvalidPayload;
            self.protocol_error(&held.id, kind, Some(envelope.message_id.clone()), None);
            return Phase::Held(held);
        };
        match downstream.payload {
            DownstreamPayload::Heartbeat(heartbeat) => {
                self.heartbeat_event(
                    &mut held,
                    downstream.message_id,
                    heartbeat.heartbeat_seq,
                    now,
                );
                Phase::Held(held)
            }
            DownstreamPayload::DriveChanged(notice) => {
                // No ACK; a notice for a drive no drive sync has mounted goes.
                for session in self
                    .mounted_drives
                    .get(&notice.drive_id)
                    .into_iter()
                    .flatten()
                {
                    if let Some(sync) = self.drive_syncs.get(session) {
                        sync.drive_changed(notice.drive_id.clone(), notice.seq.get());
                    }
                }
                Phase::Held(held)
            }
            DownstreamPayload::CredentialRenew(renew) => {
                self.renewal_notice(&mut held, renew.credential_id);
                Phase::Held(held)
            }
            DownstreamPayload::Disconnect(DisconnectPayload::Stream { .. }) => {
                if let Stream::Live { task, .. } = &held.stream {
                    task.abort();
                }
                held.stream = Stream::Waiting { at: now };
                Phase::Held(held)
            }
            DownstreamPayload::Disconnect(DisconnectPayload::Connection {
                retryable: true,
                ..
            }) => self.end(Phase::Held(held), now),
            DownstreamPayload::Disconnect(DisconnectPayload::Connection {
                retryable: false,
                code,
            }) => {
                let error = ErrorObject {
                    code: code_text(code),
                    message: "Engine ended the connection and asked for no new one".to_owned(),
                };
                self.blocked(Phase::Held(held), error)
            }
            _ => Phase::Held(held),
        }
    }

    fn drive_syncs_changed(&mut self, change: DriveSyncs) {
        match change {
            DriveSyncs::Added(session, sync) => {
                self.drive_syncs.insert(session, sync);
            }
            DriveSyncs::Removed(session) => {
                self.drive_syncs.remove(&session);
                for sessions in self.mounted_drives.values_mut() {
                    sessions.remove(&session);
                }
                self.mounted_drives
                    .retain(|_, sessions| !sessions.is_empty());
            }
            DriveSyncs::Mounted(drive, session) => {
                self.mounted_drives
                    .entry(drive)
                    .or_default()
                    .insert(session);
            }
            DriveSyncs::Detached(drive, session) => {
                if let Some(sessions) = self.mounted_drives.get_mut(&drive) {
                    sessions.remove(&session);
                    if sessions.is_empty() {
                        self.mounted_drives.remove(&drive);
                    }
                }
            }
        }
    }

    /// Every drive sync catches up after each `connected`, resumed or not.
    fn signal_connected(&self) {
        for sync in self.drive_syncs.values() {
            sync.connected();
        }
    }

    fn protocol_error(
        &self,
        connection: &Id,
        kind: ProtocolErrorKind,
        request_message_id: Option<Id>,
        trace: Option<&TraceContext>,
    ) {
        let mut report = protocol_error::report(connection.clone(), kind, request_message_id);
        report.traceparent = trace.map(|trace| trace.header_value().to_owned());
        self.outbox.submit(report);
    }

    fn outbox_event(&mut self, phase: Phase, event: OutboxEvent, now: Duration) -> Phase {
        let mut held = match phase {
            Phase::Held(held) if held.id == *event.connection() => held,
            other => return other,
        };
        match event {
            OutboxEvent::FinalAcked { instruction, .. } => {
                let current = match &held.stream {
                    Stream::Live { stream, .. } => Some(*stream),
                    Stream::Waiting { .. } | Stream::Resuming(_) => None,
                };
                held.ledger.final_acked(&instruction, current);
                self.lost_changes.report_acked(&instruction);
                state::seams::count("connection.final_acked");
                Phase::Held(held)
            }
            OutboxEvent::Refused { error, .. } => self.request_refused(held, &error, now),
            OutboxEvent::ResourceExhausted { .. } => {
                observe::error!(
                    "report.memory_exhausted",
                    connection.id = observe::Id(held.id.as_str()),
                    error.code = observe::Code("report_memory_exhausted")
                );
                self.end(Phase::Held(held), now)
            }
        }
    }

    fn heartbeat_event(&mut self, held: &mut Held, event: Id, seq: PositiveInt, now: Duration) {
        let Stream::Live { stream, .. } = &held.stream else {
            return;
        };
        let stream = *stream;
        if !held.heartbeats.admit(&event, seq.get()) {
            return;
        }
        let message_id = held.next_message_id();
        let message = Heartbeat {
            protocol_version: ProtocolVersion::default(),
            connection_id: held.id.clone(),
            message_id: message_id.clone(),
            request_message_id: event,
            heartbeat_seq: seq,
        };
        held.heartbeat = None;
        let Some(body) = ready::checked(&message, &held.id, &message_id) else {
            return;
        };
        let operation = Operation::Heartbeat(HeartbeatRequest {
            seq: seq.get(),
            message_id: message_id.clone(),
            first_send: now,
            stream,
        });
        self.post(&held.id, HEARTBEAT_PATH, operation, body.clone());
        held.heartbeat = Some(Outgoing {
            stream,
            message_id,
            body,
            first_send: now,
            seq: seq.get(),
            backoff: self.message_backoff(),
            retry_at: None,
        });
    }

    fn stream_ended(&mut self, phase: Phase, stream: u64, now: Duration) -> Phase {
        match phase {
            Phase::Connecting(attempt) if attempt.stream == stream => {
                self.attempt_failed(Phase::Connecting(attempt), now)
            }
            Phase::Held(mut held) => {
                let ours = match &held.stream {
                    Stream::Live { stream: live, .. } => *live == stream,
                    Stream::Resuming(attempt) => attempt.stream == stream,
                    Stream::Waiting { .. } => false,
                };
                if ours {
                    held.stream = Stream::Waiting {
                        at: now + self.backoff.next_wait(),
                    };
                }
                Phase::Held(held)
            }
            other => other,
        }
    }

    /// Applies an answer, counted as the test seam counter
    /// `connection.answered` whether or not it is still current.
    fn answered(&mut self, phase: Phase, request: u64, answer: Answer, now: Duration) -> Phase {
        let Some(pending) = self.requests.remove(&request) else {
            return phase;
        };
        state::seams::count("connection.answered");
        let mut held = match phase {
            Phase::Held(held) if held.id == pending.connection => held,
            other => return other,
        };
        let switching = matches!(held.renewal, Some(Renewal::Switching { .. }));
        if let Answer::Refused(error) = &answer
            && error.code == ErrorCode::CredentialRevoked
            && (switching || pending.credential_id != self.credential.credential_id)
        {
            held.send_again(&pending.operation, now);
            return Phase::Held(held);
        }
        match (answer, pending.operation) {
            (Answer::Refused(error), Operation::Credential) if !ends_connection(&error) => {
                self.fetch_rejected(&mut held, error.code.as_str());
                Phase::Held(held)
            }
            (Answer::Refused(error), Operation::GitTicket(number)) if !ends_connection(&error) => {
                self.ticket_answered(&mut held, number, Answer::Refused(error), now);
                Phase::Held(held)
            }
            (Answer::Refused(error), _) => self.request_refused(held, &error, now),
            (answer, Operation::Credential) => self.credential_answered(held, answer, now),
            (answer, Operation::GitTicket(number)) => {
                self.ticket_answered(&mut held, number, answer, now);
                Phase::Held(held)
            }
            (answer, Operation::Ready) => self.ready_answered(held, &answer, now),
            (answer, Operation::Heartbeat(heartbeat)) => {
                self.heartbeat_answered(held, &answer, heartbeat, now)
            }
        }
    }

    /// An error answer to ready or a heartbeat, which is never sent again.
    fn request_refused(&mut self, held: Box<Held>, error: &ErrorBody, now: Duration) -> Phase {
        if matches!(error.code.status(), 401 | 403) {
            return self.blocked(Phase::Held(held), error_object(error));
        }
        match error.code {
            ErrorCode::ConnectionConflict => self.conflict(Phase::Held(held), error, now),
            ErrorCode::StaleConnection => self.end(Phase::Held(held), now),
            _ => Phase::Held(held),
        }
    }

    fn ready_answered(&mut self, mut held: Box<Held>, answer: &Answer, now: Duration) -> Phase {
        let held_ref = &mut *held;
        let Some(registration) = held_ref.registration.as_mut() else {
            return Phase::Held(held);
        };
        let Some(ready) = registration.ready.as_mut() else {
            return Phase::Held(held);
        };
        let granted = match answer {
            Answer::Success(body) => {
                ready::grant(body, &held_ref.id, &ready.message_id, ready.first_send, now)
            }
            _ => None,
        };
        match granted {
            Some(deadline) => {
                STARTUP.ready_accepted(ready.first_send);
                held_ref.deadline = Some(deadline);
                state::seams::count("connection.granted");
                self.last_error = None;
                let staged = registration.staging.open();
                self.lost_changes.ready_acked(&registration.lost_changes);
                held_ref.registration = None;
                for instruction in staged {
                    self.route(held_ref, instruction);
                }
                self.next_renewal(held_ref);
            }
            None => {
                let at = now + ready.backoff.next_wait();
                if at < registration.deadline {
                    ready.retry_at = Some(at);
                }
            }
        }
        Phase::Held(held)
    }

    fn heartbeat_answered(
        &mut self,
        mut held: Box<Held>,
        answer: &Answer,
        heartbeat: HeartbeatRequest,
        now: Duration,
    ) -> Phase {
        let HeartbeatRequest {
            seq,
            message_id,
            first_send,
            stream,
        } = heartbeat;
        let granted = match answer {
            Answer::Success(body) => ready::grant(body, &held.id, &message_id, first_send, now),
            _ => None,
        };
        let latest = held.heartbeats.is_latest(seq);
        match granted {
            Some(deadline) => {
                if latest {
                    held.heartbeat = None;
                }
                if held.registration.is_none() && held.heartbeats.acknowledge(seq) {
                    held.deadline = Some(deadline);
                    state::seams::count("connection.granted");
                    // A lease ACK may arrive after its stream ended. It can
                    // extend the lease, but only a heartbeat received on the
                    // current live stream confirms this recovery.
                    if matches!(&held.stream, Stream::Live { stream: current, .. } if *current == stream)
                    {
                        self.backoff.reset();
                    }
                }
            }
            None if latest => {
                let limit = held.deadline.or(held
                    .registration
                    .as_ref()
                    .map(|registration| registration.deadline));
                if let Some(heartbeat) = held.heartbeat.as_mut() {
                    let at = now + heartbeat.backoff.next_wait();
                    if limit.is_some_and(|limit| at < limit) {
                        heartbeat.retry_at = Some(at);
                    }
                }
            }
            None => {}
        }
        Phase::Held(held)
    }
}

/// What the replay ledger and the payload check make of a Session
/// instruction.
#[derive(Debug, PartialEq, Eq)]
enum Admitted {
    /// The same instruction again: nothing new starts.
    Duplicate,
    Refused(ProtocolErrorKind),
    Route {
        recorded: bool,
        new_operation: bool,
    },
}

/// Looks the instruction up in `ledger` before anything else, checks the
/// payload of a `session.close` or `session.drives`, and records it, as a
/// new operation when it is a `tool.execute`, an `mcp.call` or a
/// `session.prepare` whose binding is not in `routed`.
fn admit(
    ledger: &mut Ledger,
    routed: &HashSet<(String, String)>,
    message: &SessionMessage,
    stream: u64,
) -> Admitted {
    let envelope = &message.envelope;
    match ledger.seen(&envelope.message_id, &message.fingerprint, stream) {
        Seen::Duplicate => return Admitted::Duplicate,
        Seen::Conflict => return Admitted::Refused(ProtocolErrorKind::Conflict),
        Seen::New => {}
    }
    let need = match envelope.kind {
        DownstreamType::SessionClose | DownstreamType::SessionDrives => {
            if protocol::decode::<Downstream>(message.data.as_bytes()).is_err() {
                return Admitted::Refused(ProtocolErrorKind::InvalidPayload);
            }
            Need::Reserve
        }
        DownstreamType::SessionPrepare
            if routed.contains(&(message.session_id.clone(), message.binding_id.clone())) =>
        {
            Need::Reserve
        }
        DownstreamType::SessionPrepare | DownstreamType::ToolExecute | DownstreamType::McpCall => {
            Need::NewOperation
        }
        _ => Need::Reserve,
    };
    let entry = Entry {
        message_id: envelope.message_id.clone(),
        kind: envelope.kind,
        fingerprint: message.fingerprint.clone(),
        session_id: message.session_id.clone(),
        binding_id: message.binding_id.clone(),
        tool_use_event_id: message.tool_use_event_id.clone(),
        task_id: message.task_id.clone(),
        stream,
        state: EntryState::Open,
        reports: Vec::new(),
        body: None,
    };
    Admitted::Route {
        recorded: ledger.record(entry, need).is_ok(),
        new_operation: need == Need::NewOperation,
    }
}

fn instruction_span(
    connection: &Id,
    message: &SessionMessage,
    trace: Option<&TraceContext>,
) -> tracing::Span {
    let mut identities = vec![
        (Identity::Connection, connection.as_str()),
        (Identity::Message, message.envelope.message_id.as_str()),
        (Identity::Session, message.session_id.as_str()),
        (Identity::Binding, message.binding_id.as_str()),
    ];
    if let Some(event) = &message.tool_use_event_id {
        identities.push((Identity::ToolUseEvent, event));
    }
    if let Some(task) = &message.task_id {
        identities.push((Identity::Task, task));
    }
    observe::correlation_span(&identities, trace)
}

impl Owner {
    /// Takes `credential` as the one in use, from the `connected` of the
    /// attempt that carried it.
    fn adopt(&mut self, credential: Credential) {
        if credential.credential_id != self.credential.credential_id {
            self.credential = credential;
        }
        self.written = None;
    }

    /// A `credential.renew` for `credential_id`. One for the credential of
    /// the running renewal, or already waiting, changes nothing.
    fn renewal_notice(&mut self, held: &mut Held, credential_id: Id) {
        let running = held
            .renewal
            .as_ref()
            .is_some_and(|renewal| *renewal.credential_id() == credential_id);
        if running || held.notices.contains(&credential_id) {
            return;
        }
        held.notices.push_back(credential_id);
        self.next_renewal(held);
    }

    /// Fetches the credential of the next notice, once the connection is
    /// `online` and no renewal runs.
    fn next_renewal(&mut self, held: &mut Held) {
        if held.registration.is_some() || held.renewal.is_some() {
            return;
        }
        while let Some(credential_id) = held.notices.pop_front() {
            let message_id = held.next_message_id();
            let Some(body) = credential::request(&held.id, &message_id, &credential_id) else {
                continue;
            };
            self.post(
                &held.id,
                CREDENTIAL_PATH,
                Operation::Credential,
                body.clone(),
            );
            held.renewal = Some(Renewal::Fetching {
                credential_id,
                body,
                backoff: self.message_backoff(),
                retry_at: None,
            });
            return;
        }
    }

    /// A fetch's answer that is never sent again: the credential in use
    /// stays.
    fn fetch_rejected(&mut self, held: &mut Held, code: &'static str) {
        if let Some(renewal) = held.renewal.take() {
            credential::rejected(renewal.credential_id(), code);
        }
        self.next_renewal(held);
    }

    /// A `2xx` answer or no answer to the fetch: the credential is written,
    /// or the fetch is sent again within the connection deadline.
    fn credential_answered(&mut self, mut held: Box<Held>, answer: Answer, now: Duration) -> Phase {
        let Some(Renewal::Fetching {
            credential_id,
            backoff,
            retry_at,
            ..
        }) = held.renewal.as_mut()
        else {
            return Phase::Held(held);
        };
        match answer {
            Answer::Success(body) => match credential::fetched(&body, credential_id) {
                Some(credential) => self.persist(&mut held, credential),
                None => self.fetch_rejected(&mut held, INVALID_RESPONSE),
            },
            // The connection's end ends the renewal, so a retry past the
            // current deadline is kept: heartbeat ACKs move the deadline.
            _ => *retry_at = Some(now + backoff.next_wait()),
        }
        Phase::Held(held)
    }

    /// Writes `credential` in a task of its own; the test seam barrier
    /// `renewal.persisted` follows a write that succeeded.
    fn persist(&mut self, held: &mut Held, credential: Credential) {
        let tag = self.tag();
        let root = self.state_root.clone();
        let server_url = self.server_url.clone();
        let instance_id = self.instance_id.clone();
        let mailbox = self.mailbox.clone();
        let written = credential.clone();
        tokio::spawn(async move {
            let writing = written.clone();
            let saved = tokio::task::spawn_blocking(move || {
                credential::persist(&root, &server_url, &instance_id, &writing).is_ok()
            })
            .await
            .unwrap_or(false);
            if saved {
                state::seams::barrier("renewal.persisted").await;
            }
            let _ = mailbox.send(Message::Persisted {
                tag,
                credential: written,
                saved,
            });
        });
        held.renewal = Some(Renewal::Persisting { credential, tag });
    }

    /// The end of a write. Once written, the stream closes and the resume
    /// with the new credential starts at the owner's next check; a write of
    /// a renewal no longer running leaves the credential for the next
    /// attempt, which reads the same file a restart would.
    fn persisted(
        &mut self,
        phase: Phase,
        tag: u64,
        credential: Credential,
        saved: bool,
        now: Duration,
    ) -> Phase {
        let mut held = match phase {
            Phase::Held(held) if matches!(&held.renewal, Some(Renewal::Persisting { tag: running, .. }) if *running == tag) => {
                held
            }
            other => {
                if saved {
                    self.written = Some(credential);
                }
                return other;
            }
        };
        if !saved {
            held.renewal = None;
            self.next_renewal(&mut held);
            return Phase::Held(held);
        }
        self.drive.switch_started(&held.id);
        self.outbox.switch_started(held.id.clone());
        match std::mem::replace(&mut held.stream, Stream::Waiting { at: now }) {
            Stream::Live { task, .. } => task.abort(),
            Stream::Resuming(attempt) => attempt.task.abort(),
            Stream::Waiting { .. } => {}
        }
        held.renewal = Some(Renewal::Switching { credential });
        Phase::Held(held)
    }

    /// Fetches a git ticket for a binding of the held connection; a fetch
    /// asked for during a credential switch goes once the switch ends, and
    /// one for another connection gets no answer, since that connection's
    /// end has ended its preparation.
    fn fetch_git_ticket(
        &mut self,
        session_id: SafeId,
        request: TicketRequest,
        connection_id: &Id,
        binding_id: &Id,
    ) {
        let backoff = self.message_backoff();
        let Phase::Held(held) = &mut self.phase else {
            return;
        };
        if held.id != *connection_id {
            return;
        }
        let message_id = held.next_message_id();
        let Some(body) = git_ticket::request(&held.id, &message_id, binding_id) else {
            self.owners.git_ticket(
                &session_id,
                request,
                Err("the git ticket request failed its own check".to_owned()),
            );
            return;
        };
        self.last_tag += 1;
        let number = self.last_tag;
        let switching = matches!(held.renewal, Some(Renewal::Switching { .. }));
        held.tickets.insert(
            number,
            TicketFetch {
                session_id,
                request,
                body: body.clone(),
                backoff,
                retry_at: switching.then(state::seams::now),
            },
        );
        if !switching {
            let connection = held.id.clone();
            self.post(
                &connection,
                GIT_TICKET_PATH,
                Operation::GitTicket(number),
                body,
            );
        }
    }

    /// The answer to the git-ticket fetch `number`: the Session binding
    /// owner gets the ticket or the failure's message. A fetch that got no
    /// answer is sent again when its next attempt starts before the
    /// connection deadline, and fails otherwise.
    fn ticket_answered(&mut self, held: &mut Held, number: u64, answer: Answer, now: Duration) {
        let Some(fetch) = held.tickets.get_mut(&number) else {
            return;
        };
        if matches!(answer, Answer::Failed(_)) {
            let at = now + fetch.backoff.next_wait();
            if held.deadline.is_some_and(|deadline| at < deadline) {
                fetch.retry_at = Some(at);
                return;
            }
        }
        let answer = git_ticket::answer(answer);
        let fetch = held.tickets.remove(&number).expect("the fetch is pending");
        self.owners
            .git_ticket(&fetch.session_id, fetch.request, answer);
    }

    /// Closes the connection's stream; the outbox drops its reports, the
    /// waiting probe-slot requests are cancelled and every Session binding
    /// owner ends the connection's bindings.
    fn release_held(&mut self, held: Held) {
        if let Some(Renewal::Switching { credential }) = held.renewal {
            self.written = Some(credential);
        }
        match held.stream {
            Stream::Live { task, .. } => task.abort(),
            Stream::Resuming(attempt) => attempt.task.abort(),
            Stream::Waiting { .. } => {}
        }
        for requester in self.slots.cancel(state::seams::now()) {
            self.owners.answer(&requester, ProbeSlot::Cancelled);
        }
        self.owners.connection_ended(&held.id);
        self.drive.connection_ended(&held.id);
        self.outbox.connection_ended(held.id);
    }

    /// Hands a Session instruction on once the ready ACK came, counted as
    /// the test seam counter `route.session`: a `session.prepare`, a
    /// `session.close`, a `session.drives` or a tool instruction to the
    /// Session binding owner of its `session_id`.
    fn route(&mut self, held: &mut Held, instruction: Instruction) {
        state::seams::count("route.session");
        let _entered = instruction.span.clone().entered();
        match instruction.message.envelope.kind {
            DownstreamType::SessionPrepare => self.route_prepare(held, instruction),
            DownstreamType::SessionClose => self.route_close(held, &instruction),
            DownstreamType::SessionDrives => self.route_drives(held, &instruction),
            DownstreamType::ToolExecute
            | DownstreamType::ToolStdin
            | DownstreamType::ToolCancel => self.route_tool(held, instruction),
            _ => {}
        }
    }

    /// Hands a tool instruction to its Session's owner, which decodes and
    /// admits it. One whose identities no report can carry back is a
    /// protocol error.
    fn route_tool(&mut self, held: &Held, instruction: Instruction) {
        let Instruction {
            mut message,
            recorded,
            trace,
            received,
            ..
        } = instruction;
        let identity = |value: Option<&str>| value.and_then(|value| Id::new(value).ok());
        let (Some(session_id), Some(binding_id), Some(tool_use_event_id), Some(task_id)) = (
            identity(Some(&message.session_id)),
            identity(Some(&message.binding_id)),
            identity(message.tool_use_event_id.as_deref()),
            identity(message.task_id.as_deref()),
        ) else {
            let kind = ProtocolErrorKind::InvalidPayload;
            let message_id = message.envelope.message_id.clone();
            self.protocol_error(&held.id, kind, Some(message_id), trace.as_ref());
            return;
        };
        // The factory rejects a new `tool.execute` of a Session without an
        // owner that the ledger could not record `resource_exhausted`, at
        // its first step.
        if !recorded
            && message.envelope.kind == DownstreamType::ToolExecute
            && !self.owners.owns(&message.session_id)
        {
            self.task_pressure.refused(state::seams::now());
        }
        let payload = serde_json::Value::Object(std::mem::take(&mut message.envelope.payload));
        self.owners.tool(ToolCall {
            connection_id: held.id.clone(),
            message_id: message.envelope.message_id,
            kind: message.envelope.kind,
            session_id,
            binding_id,
            tool_use_event_id,
            task_id,
            data: message.data,
            fingerprint: fingerprint(&payload),
            recorded,
            traceparent: trace.map(|trace| trace.header_value().to_owned()),
            received,
        });
    }

    /// Admits a `session.prepare`: one whose payload fails its schema, one
    /// for a Session without a record while `records_max` exist, and a new
    /// operation that the replay ledger could not record fail here, with no
    /// record and no Session binding owner created.
    fn route_prepare(&mut self, held: &mut Held, instruction: Instruction) {
        let message = &instruction.message;
        let payload = match protocol::decode::<Downstream>(message.data.as_bytes()) {
            Ok(Downstream {
                payload: DownstreamPayload::SessionPrepare(payload),
                ..
            }) => payload,
            _ => {
                return self.prepare_failed(held, &instruction, PreparationErrorCode::InvalidInput);
            }
        };
        if self.owners.full_for(&payload.session_id)
            || (instruction.new_operation && !instruction.recorded)
        {
            return self.prepare_failed(
                held,
                &instruction,
                PreparationErrorCode::ResourceExhausted,
            );
        }
        held.routed
            .insert((message.session_id.clone(), message.binding_id.clone()));
        let envelope = &message.envelope;
        let session_id = payload.session_id.clone();
        let prepare = Prepare {
            connection_id: held.id.clone(),
            message_id: envelope.message_id.clone(),
            addressed_connection_id: envelope.connection_id.clone(),
            fingerprint: fingerprint(&serde_json::Value::Object(envelope.payload.clone())),
            payload,
            traceparent: instruction
                .trace
                .as_ref()
                .map(|trace| trace.header_value().to_owned()),
        };
        self.owners.prepare(session_id, prepare);
    }

    /// A failed report for a `session.prepare` refused at admission. One
    /// whose identities no report can carry back is a protocol error.
    fn prepare_failed(&self, held: &Held, instruction: &Instruction, code: PreparationErrorCode) {
        let message = &instruction.message;
        let request_message_id = message.envelope.message_id.clone();
        let (Ok(session_id), Ok(binding_id)) = (
            Id::new(message.session_id.as_str()),
            Id::new(message.binding_id.as_str()),
        ) else {
            let kind = ProtocolErrorKind::InvalidPayload;
            let trace = instruction.trace.as_ref();
            self.protocol_error(&held.id, kind, Some(request_message_id), trace);
            return;
        };
        let error = PreparationError::new(code, None);
        let answered = request_message_id.clone();
        let mut report = Submission::new(held.id.clone(), move |connection_id, message_id| {
            SessionStatus {
                connection_id,
                message_id,
                request_message_id: answered,
                session_id,
                binding_id,
                outcome: SessionOutcome::Failed { error },
            }
        });
        report.final_for = vec![request_message_id];
        report.traceparent = instruction
            .trace
            .as_ref()
            .map(|trace| trace.header_value().to_owned());
        self.outbox.submit(report);
    }

    /// Hands a `session.close`, whose payload admission checked, to its
    /// Session's owner.
    fn route_close(&mut self, held: &Held, instruction: &Instruction) {
        let message = &instruction.message;
        let Ok(Downstream {
            payload: DownstreamPayload::SessionClose(close),
            ..
        }) = protocol::decode::<Downstream>(message.data.as_bytes())
        else {
            return;
        };
        let close_message = Close {
            connection_id: held.id.clone(),
            message_id: message.envelope.message_id.clone(),
            binding_id: close.binding_id,
        };
        self.owners.close(&close.session_id, close_message);
    }

    /// Hands a `session.drives` to its Session's owner; one whose binding
    /// no `session.prepare` of this connection named is a protocol error.
    fn route_drives(&mut self, held: &Held, instruction: &Instruction) {
        let message = &instruction.message;
        let Ok(Downstream {
            payload: DownstreamPayload::SessionDrives(drives),
            ..
        }) = protocol::decode::<Downstream>(message.data.as_bytes())
        else {
            return;
        };
        let routed = (
            drives.session_id.as_str().to_owned(),
            drives.binding_id.as_str().to_owned(),
        );
        if !held.routed.contains(&routed) {
            return self.protocol_error(
                &held.id,
                ProtocolErrorKind::UnknownBinding,
                Some(message.envelope.message_id.clone()),
                instruction.trace.as_ref(),
            );
        }
        let drives_message = Drives {
            connection_id: held.id.clone(),
            message_id: message.envelope.message_id.clone(),
            binding_id: drives.binding_id,
            drives: drives.drives,
            traceparent: instruction
                .trace
                .as_ref()
                .map(|trace| trace.header_value().to_owned()),
        };
        self.owners.drives(&drives.session_id, drives_message);
    }
}

impl Held {
    /// Sends `operation` again at the end of the switch, or at once when
    /// none runs: a `401 credential_revoked` answered it under a credential
    /// that is being or has been replaced.
    fn send_again(&mut self, operation: &Operation, now: Duration) {
        match operation {
            Operation::Ready => {
                if let Some(ready) = self
                    .registration
                    .as_mut()
                    .and_then(|registration| registration.ready.as_mut())
                {
                    ready.retry_at = Some(now);
                }
            }
            Operation::Heartbeat(heartbeat) => {
                if let Some(latest) = self
                    .heartbeat
                    .as_mut()
                    .filter(|latest| latest.seq == heartbeat.seq)
                {
                    latest.retry_at = Some(now);
                }
            }
            Operation::Credential => {
                if let Some(Renewal::Fetching { retry_at, .. }) = self.renewal.as_mut() {
                    *retry_at = Some(now);
                }
            }
            Operation::GitTicket(number) => {
                if let Some(fetch) = self.tickets.get_mut(number) {
                    fetch.retry_at = Some(now);
                }
            }
        }
    }

    fn next_message_id(&mut self) -> Id {
        self.messages += 1;
        Id::new(format!("c{}", self.messages)).expect("c<n> is an Id")
    }

    /// The earliest time something of the connection waits for.
    fn next_time(&self) -> Duration {
        let stream = match &self.stream {
            Stream::Live { .. } => None,
            Stream::Waiting { at } => Some(*at),
            Stream::Resuming(attempt) => Some(attempt.deadline),
        };
        let registration = self.registration.as_ref();
        [
            self.deadline,
            registration.map(|registration| registration.deadline),
            registration
                .and_then(|registration| registration.ready.as_ref())
                .and_then(|ready| ready.retry_at),
            self.heartbeat
                .as_ref()
                .and_then(|heartbeat| heartbeat.retry_at),
            match &self.renewal {
                Some(Renewal::Fetching { retry_at, .. }) => *retry_at,
                _ => None,
            },
            self.tickets
                .values()
                .filter_map(|fetch| fetch.retry_at)
                .min(),
            stream,
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(Duration::MAX)
    }
}

/// Whether a refusal ends the connection or blocks it ([`Owner::request_refused`]).
fn ends_connection(error: &ErrorBody) -> bool {
    matches!(error.code.status(), 401 | 403)
        || matches!(
            error.code,
            ErrorCode::ConnectionConflict | ErrorCode::StaleConnection
        )
}

fn error_object(error: &ErrorBody) -> ErrorObject {
    ErrorObject {
        code: code_text(error.code),
        message: error.message.clone(),
    }
}

fn code_text(code: ErrorCode) -> String {
    match serde_json::to_value(code) {
        Ok(serde_json::Value::String(text)) => text,
        _ => unreachable!("an error code serializes as its name"),
    }
}

impl drive::mount::Registry for Connection {
    fn add(&self, session_id: SafeId, sync: DriveSync) {
        self.add_drive_sync(session_id, sync);
    }

    fn remove(&self, session_id: SafeId) {
        self.remove_drive_sync(session_id);
    }

    fn mounted(&self, drive_id: Id, session_id: SafeId) {
        self.drive_mounted(drive_id, session_id);
    }

    fn detached(&self, drive_id: Id, session_id: SafeId) {
        self.drive_detached(drive_id, session_id);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    struct NoSessions;

    impl SessionFactory for NoSessions {
        fn create(
            &self,
            _: SafeId,
            _: Arc<dyn protocol::ports::ConnectionPort>,
            _: Arc<dyn ReportSink>,
        ) -> Arc<dyn protocol::ports::SessionHandle> {
            Arc::new(Silent)
        }
        fn unbound_tool(&self, _: ToolCall, _: Arc<dyn ReportSink>) {
            panic!("this connection fixture admits no tools")
        }
    }

    /// The owner a close creates in this fixture: it answers nothing.
    struct Silent;

    impl protocol::ports::SessionHandle for Silent {
        fn prepare(&self, _: Prepare) {}
        fn close(&self, _: Close) {}
        fn drives(&self, _: Drives) {}
        fn tool(&self, _: ToolCall) {}
        fn probe_slot(&self, _: protocol::ports::ProbeRequest, _: ProbeSlot) {}
        fn git_ticket(&self, _: TicketRequest, _: protocol::ports::TicketAnswer) {}
        fn connection_ended(&self, _: Id) {}
        fn shutdown(&self) {}
        fn entries(&self, reply: protocol::ports::EntriesReply) {
            reply(Vec::new());
        }
        fn end(&self) {}
    }

    fn owner_with_budget(
        budget: Arc<crate::report_memory::Budget>,
    ) -> (Owner, mpsc::UnboundedReceiver<OutboxEvent>) {
        owner_at(budget, "http://127.0.0.1:1")
    }

    fn owner_at(
        budget: Arc<crate::report_memory::Budget>,
        server_url: &str,
    ) -> (Owner, mpsc::UnboundedReceiver<OutboxEvent>) {
        let root = StateRoot::new(std::env::temp_dir().as_os_str()).unwrap();
        owner_rooted(budget, server_url, root)
    }

    fn owner_rooted(
        budget: Arc<crate::report_memory::Budget>,
        server_url: &str,
        state_root: StateRoot,
    ) -> (Owner, mpsc::UnboundedReceiver<OutboxEvent>) {
        let bootstrap = protocol::bootstrap::Bootstrap::decode(
            br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
        ).unwrap();
        let config = Config {
            server_url: server_url.into(),
            instance_id: Id::new("inst_a").unwrap(),
            credential: Credential {
                credential_id: Id::new("cred_a").unwrap(),
                key: bootstrap.connection_key.clone(),
                expires_at: bootstrap.expires_at,
            },
            ready: ReadyFields {
                cli_version: serde_json::from_value(json!("0.1.0")).unwrap(),
                capabilities: serde_json::from_value(
                    json!({"session_preparation": true, "drives": {"mount": false}}),
                )
                .unwrap(),
            },
            state_root,
            started: Duration::ZERO,
            sessions: Arc::new(NoSessions),
            activity: Arc::new(protocol::ports::NoActivity),
            times: Times::default(),
        };
        let transport = Arc::new(Transport::new(&config.server_url));
        let (events, reports) = mpsc::unbounded_channel();
        let outbox = Outbox::with_budget(
            OutboxConfig {
                transport: transport.clone(),
                key: bootstrap.connection_key,
                times: OutboxTimes::default(),
                clock: Clock::seams(),
                events,
            },
            budget,
        );
        let (mailbox, _inbox) = mpsc::unbounded_channel();
        let owners = Owners::new(
            config.sessions.clone(),
            Arc::new(Port {
                mailbox: mailbox.clone(),
            }),
            Arc::new(outbox.clone()),
            128,
        );
        let drive = DriveLink::new(config.credential.key.clone()).0;
        let mut owner = Owner::new(config, transport, outbox, drive, owners, mailbox);
        owner.backoff = Backoff::new(BASE_START, BASE_MAX, || 0.0);
        (owner, reports)
    }

    fn held(stream: u64) -> (Box<Held>, tokio::task::JoinHandle<()>) {
        let task = tokio::spawn(std::future::pending());
        (
            Box::new(Held {
                id: Id::new("conn_a").unwrap(),
                stream: Stream::Live {
                    stream,
                    task: task.abort_handle(),
                },
                deadline: Some(Duration::from_secs(300)),
                registration: None,
                messages: 0,
                heartbeats: Heartbeats::default(),
                heartbeat: None,
                ledger: Ledger::new(ledger::Limits::default()),
                routed: HashSet::new(),
                event_max_bytes: crate::stream::TOOL_EVENT_MAX_BYTES,
                renewal: None,
                notices: VecDeque::new(),
                tickets: HashMap::new(),
            }),
            task,
        )
    }

    fn heartbeat_ack(seq: i64, stream: u64) -> (HeartbeatRequest, Answer) {
        let message_id = Id::new(format!("heartbeat_{seq}")).unwrap();
        let answer = Answer::Success(
            serde_json::to_vec(&json!({
                "connection_id": "conn_a", "message_id": message_id,
                "accepted": true, "valid_for_ms": 45_000,
            }))
            .unwrap(),
        );
        (
            HeartbeatRequest {
                seq,
                message_id,
                first_send: Duration::ZERO,
                stream,
            },
            answer,
        )
    }

    #[tokio::test]
    async fn stop_completion_publishes_its_snapshot_before_waking_the_waiter() {
        struct ObserveWake {
            status: tokio::sync::watch::Receiver<ConnectionStatus>,
            observed: std::sync::Mutex<Vec<ConnectionState>>,
        }
        impl std::task::Wake for ObserveWake {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.observed
                    .lock()
                    .unwrap()
                    .push(self.status.borrow().state);
            }
        }

        let (mut owner, reports) = owner_with_budget(crate::report_memory::Budget::new(512, 512));
        let (mailbox, inbox) = mpsc::unbounded_channel();
        owner.mailbox = mailbox.clone();
        let (status, snapshot) = tokio::sync::watch::channel(owner.status());
        let observer = Arc::new(ObserveWake {
            status: snapshot,
            observed: std::sync::Mutex::new(Vec::new()),
        });
        let wake = std::task::Waker::from(observer.clone());
        let mut context = std::task::Context::from_waker(&wake);
        let (reply, mut answer) = oneshot::channel();
        assert!(
            std::future::Future::poll(std::pin::Pin::new(&mut answer), &mut context).is_pending()
        );
        mailbox.send(Message::Stop(reply)).unwrap();

        // Poll the real owner loop. The waiter observes the snapshot inside
        // send's wake, before the owner can execute a later publication.
        let mut running = Box::pin(own(owner, inbox, reports, status));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(running.as_mut(), &mut context).is_pending());
        assert_eq!(
            *observer.observed.lock().unwrap(),
            vec![ConnectionState::Stopped]
        );
        assert!(answer.try_recv().is_ok());
    }

    #[tokio::test]
    async fn a_credential_fetch_is_retried_past_the_deadline_and_a_ticket_fetch_fails_there() {
        let (mut owner, _) = owner_with_budget(crate::report_memory::Budget::new(512, 512));
        let (mut held, task) = held(1);
        held.deadline = Some(Duration::from_secs(10));
        let backoff = || Backoff::new(Duration::from_secs(30), BASE_MAX, || 0.0);
        held.renewal = Some(Renewal::Fetching {
            credential_id: Id::new("cred_b").unwrap(),
            body: Vec::new(),
            backoff: backoff(),
            retry_at: None,
        });
        let failed = || Answer::Failed(crate::http::Failure::Server);
        let Phase::Held(mut held) = owner.credential_answered(held, failed(), Duration::ZERO)
        else {
            panic!("held connection");
        };
        // Heartbeat ACKs may still move the deadline past the retry.
        assert!(matches!(
            held.renewal,
            Some(Renewal::Fetching { retry_at: Some(at), .. }) if at == Duration::from_secs(30)
        ));
        let fetch = |backoff| TicketFetch {
            session_id: SafeId::new("sess_a").unwrap(),
            request: TicketRequest(1),
            body: Vec::new(),
            backoff,
            retry_at: None,
        };
        held.tickets.insert(
            1,
            fetch(Backoff::new(Duration::from_secs(1), BASE_MAX, || 0.0)),
        );
        held.tickets.insert(2, fetch(backoff()));
        owner.ticket_answered(&mut held, 1, failed(), Duration::ZERO);
        owner.ticket_answered(&mut held, 2, failed(), Duration::ZERO);
        assert_eq!(held.tickets[&1].retry_at, Some(Duration::from_secs(1)));
        assert!(!held.tickets.contains_key(&2), "answered with its failure");
        task.abort();
    }

    #[tokio::test]
    async fn each_resumed_streams_own_heartbeat_resets_backoff_and_old_acks_do_not() {
        let (mut owner, _) = owner_with_budget(crate::report_memory::Budget::new(512, 512));
        for stream in 1..=3 {
            // Earlier failures have accumulated a longer delay.
            for _ in 0..3 {
                owner.backoff.next_wait();
            }
            let (held, task) = held(stream);
            let (heartbeat, answer) = heartbeat_ack(stream as i64, stream);
            let phase = owner.heartbeat_answered(held, &answer, heartbeat, Duration::from_secs(1));
            let Phase::Held(held) = owner.stream_ended(phase, stream, Duration::from_secs(2))
            else {
                panic!("held connection");
            };
            assert!(matches!(held.stream, Stream::Waiting { at } if at == Duration::from_secs(3)));
            task.abort();
        }
        owner.backoff.reset();
        assert_eq!(owner.backoff.next_wait(), Duration::from_secs(1));
        assert_eq!(owner.backoff.next_wait(), Duration::from_secs(2));
        let (held, task) = held(10);
        let (old, answer) = heartbeat_ack(1, 9);
        let Phase::Held(held) =
            owner.heartbeat_answered(held, &answer, old, Duration::from_secs(1))
        else {
            panic!("held connection");
        };
        assert_eq!(
            held.deadline,
            Some(Duration::from_secs(45)),
            "old valid ACK still grants its lease"
        );
        let Phase::Held(held) = owner.stream_ended(Phase::Held(held), 10, Duration::from_secs(2))
        else {
            panic!("held connection");
        };
        assert!(
            matches!(held.stream, Stream::Waiting { at } if at == Duration::from_secs(6)),
            "the old ACK did not reset the four-second recovery wait"
        );
        task.abort();
    }

    #[tokio::test]
    async fn credential_revoked_sends_a_heartbeat_of_the_old_credential_again_and_blocks_on_the_current_one()
     {
        let revoked = || {
            Answer::Refused(ErrorBody {
                code: ErrorCode::CredentialRevoked,
                message: String::new(),
                retry_after_ms: None,
                missing_sha256: None,
            })
        };
        // (the credential the heartbeat carried, whether it is sent again)
        for (carried, again) in [("cred_old", true), ("cred_a", false)] {
            let (mut owner, _) = owner_with_budget(crate::report_memory::Budget::new(512, 512));
            let (mut held, task) = held(1);
            let (heartbeat, _) = heartbeat_ack(1, 1);
            held.heartbeat = Some(Outgoing {
                stream: 1,
                message_id: heartbeat.message_id.clone(),
                body: Vec::new(),
                first_send: Duration::ZERO,
                seq: 1,
                backoff: Backoff::new(BASE_START, BASE_MAX, || 0.0),
                retry_at: None,
            });
            owner.requests.insert(
                7,
                Pending {
                    connection: held.id.clone(),
                    operation: Operation::Heartbeat(heartbeat),
                    credential_id: Id::new(carried).unwrap(),
                    task: task.abort_handle(),
                },
            );
            let now = Duration::from_secs(5);
            match owner.answered(Phase::Held(held), 7, revoked(), now) {
                Phase::Held(held) if again => {
                    assert_eq!(held.heartbeat.unwrap().retry_at, Some(now));
                }
                Phase::Blocked if !again => {}
                _ => panic!("{carried}: sent again {again} expected"),
            }
            task.abort();
        }
    }

    /// A Session's drive sync under `root` that mounts nothing and whose
    /// requests go nowhere.
    async fn drive_sync(root: &std::path::Path, session: &str) -> DriveSync {
        struct NoRegistry;
        impl drive::mount::Registry for NoRegistry {
            fn add(&self, _: SafeId, _: DriveSync) {}
            fn remove(&self, _: SafeId) {}
            fn mounted(&self, _: Id, _: SafeId) {}
            fn detached(&self, _: Id, _: SafeId) {}
        }
        struct NoTasks;
        impl protocol::ports::TaskOwnership for NoTasks {
            fn task_of(&self, _: u32, reply: protocol::ports::TaskOwnerReply) {
                reply(None);
            }
        }
        struct NoReports;
        impl ReportSink for NoReports {
            fn submit(&self, _: Submission) -> protocol::ports::ReportId {
                protocol::ports::ReportId(0)
            }
            fn cancel(&self, _: protocol::ports::ReportId) {}
            fn add_final(&self, _: protocol::ports::ReportId, _: Id) {}
        }
        let key = protocol::bootstrap::Bootstrap::decode(
            br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap()
        .connection_key;
        let clock = Clock::new(|| Duration::ZERO, Duration::from_millis(1));
        let (events, _) = mpsc::unbounded_channel();
        let transfer: Arc<dyn DriveTransfer> =
            Arc::new(crate::drive_transfer::HttpDriveTransfer::new(
                "http://127.0.0.1:1",
                DriveLink::new(key).1,
                events,
                clock.clone(),
            ));
        let cache = drive::cache::ContentCache::spawn(drive::cache::CacheConfig {
            root: root.to_owned(),
            transfer: transfer.clone(),
            clock: clock.clone(),
            free_space: Arc::new(|| Ok(u64::MAX)),
        })
        .unwrap();
        let session_dir = root.join("sessions").join(session);
        std::fs::create_dir_all(&session_dir).unwrap();
        let state = drive::state::SyncState::create(&session_dir.join("sync-state.db"))
            .await
            .unwrap();
        DriveSync::spawn(drive::SyncConfig {
            session_id: SafeId::new(session).unwrap(),
            root: root.to_owned(),
            state,
            transfer,
            cache,
            tasks: Arc::new(NoTasks),
            mounting: drive::mount::Mounting::Fuse,
            registry: Arc::new(NoRegistry),
            clock,
            reports: Arc::new(NoReports),
        })
    }

    fn downstream(connection: &str, message_id: &str, kind: &str, payload: Value) -> String {
        json!({
            "protocol_version": 1, "connection_id": connection, "message_id": message_id,
            "type": kind, "payload": payload,
        })
        .to_string()
    }

    #[tokio::test]
    async fn every_connected_reaches_every_drive_sync_and_drive_changed_those_that_mounted_its_drive()
     {
        let root = tempfile::tempdir().unwrap();
        let (mut owner, _) = owner_with_budget(crate::report_memory::Budget::new(512, 512));
        let (a, b) = (
            drive_sync(root.path(), "sess_a").await,
            drive_sync(root.path(), "sess_b").await,
        );
        let session = |text: &str| SafeId::new(text).unwrap();
        for change in [
            DriveSyncs::Added(session("sess_a"), a.clone()),
            DriveSyncs::Added(session("sess_b"), b.clone()),
            DriveSyncs::Mounted(id_of("drive_a"), session("sess_a")),
            DriveSyncs::Mounted(id_of("drive_b"), session("sess_a")),
            DriveSyncs::Mounted(id_of("drive_b"), session("sess_b")),
        ] {
            owner.drive_syncs_changed(change);
        }
        let now = Duration::from_secs(1);
        let (held, task) = held(1);
        let mut phase = Phase::Held(held);
        let changed = |drive: &str, seq: i64| {
            downstream(
                "conn_a",
                &format!("msg_changed_{seq}"),
                "drive.changed",
                json!({"drive_id": drive, "seq": seq}),
            )
        };
        for (drive, seq) in [("drive_a", 5), ("drive_b", 6), ("drive_c", 7)] {
            phase = owner.event(phase, 1, changed(drive, seq), now);
        }
        owner.drive_syncs_changed(DriveSyncs::Detached(id_of("drive_b"), session("sess_b")));
        phase = owner.event(phase, 1, changed("drive_b", 8), now);

        // A resumed connected, then a new connection's.
        let credential = owner.credential.clone();
        let attempt = |stream, kind| Attempt {
            stream,
            kind,
            credential: credential.clone(),
            deadline: Duration::from_secs(60),
            task: task.abort_handle(),
            limit: EventLimit::new(1_024),
        };
        let Phase::Held(mut held) = phase else {
            panic!("held connection");
        };
        held.stream = Stream::Resuming(attempt(
            2,
            AttemptKind::Resume {
                connection: id_of("conn_a"),
            },
        ));
        let connected = |connection: &str, resumed: bool| {
            downstream(
                connection,
                "msg_connected",
                "connected",
                json!({"resumed": resumed}),
            )
        };
        let phase = owner.event(Phase::Held(held), 2, connected("conn_a", true), now);
        assert!(
            matches!(&phase, Phase::Held(held) if matches!(held.stream, Stream::Live { stream: 2, .. }))
        );
        let fresh = attempt(3, AttemptKind::Fresh { previous: None });
        let phase = owner.event(Phase::Connecting(fresh), 3, connected("conn_b", false), now);
        assert!(matches!(&phase, Phase::Held(held) if held.id.as_str() == "conn_b"));

        let seqs = |pairs: &[(&str, i64)]| {
            (pairs.iter().map(|(drive, seq)| ((*drive).to_owned(), *seq))).collect()
        };
        assert_eq!(
            a.notices_taken().await,
            (2, seqs(&[("drive_a", 5), ("drive_b", 8)]))
        );
        assert_eq!(b.notices_taken().await, (2, seqs(&[("drive_b", 6)])));
        task.abort();
    }

    #[tokio::test]
    async fn a_real_final_report_ack_releases_only_its_instruction_from_the_owner_ledger() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (release, released) = oneshot::channel();
        let (received, body_received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            let mut length = 0;
            while stream.read_line(&mut line).await.unwrap() > 2 {
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                line.clear();
            }
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            received.send(()).unwrap();
            released.await.unwrap();
            let ack = json!({"connection_id": body["connection_id"], "message_id": body["message_id"], "accepted": true}).to_string();
            stream
                .get_mut()
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ack}",
                        ack.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let (mut owner, mut events) = owner_at(crate::report_memory::Budget::new(4_096, 512), &url);
        let (held, stream_task) = held(1);
        // A prepare with readable identities but a missing cwd reaches the
        // ledger, then receives the normal invalid_input final report.
        let first = message(
            "session.prepare",
            "msg_first",
            json!({
                "session_id": "sess_a", "binding_id": "bind_a",
            }),
        );
        let other = close("msg_other", "bind_b");
        let connection = held.id.clone();
        owner
            .outbox
            .connection_started(1, connection.clone(), owner.credential.key.clone(), false);
        owner.phase = Phase::Held(held);
        owner.handle(Message::Event {
            stream: 1,
            data: first.data.clone(),
        });
        owner.handle(Message::Event {
            stream: 1,
            data: other.data.clone(),
        });
        tokio::time::timeout(Duration::from_secs(2), body_received)
            .await
            .unwrap()
            .unwrap();
        assert!(
            events.try_recv().is_err(),
            "receiving the body alone does not acknowledge its instruction"
        );
        let Phase::Held(held) = &owner.phase else {
            panic!("held connection");
        };
        assert_eq!(held.ledger.len(), 2);
        release.send(()).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&event, OutboxEvent::FinalAcked { instruction, .. } if *instruction == first.envelope.message_id)
        );
        owner.handle(Message::Outbox(event));
        let Phase::Held(held) = &mut owner.phase else {
            panic!("held connection");
        };
        assert_eq!(held.ledger.len(), 1);
        assert_eq!(
            held.ledger
                .seen(&first.envelope.message_id, &first.fingerprint, 1),
            Seen::New
        );
        assert_eq!(
            held.ledger
                .seen(&other.envelope.message_id, &other.fingerprint, 1),
            Seen::Duplicate
        );
        server.await.unwrap();
        stream_task.abort();
    }

    #[tokio::test]
    async fn reserve_exhaustion_reaches_logical_connection_release() {
        let budget = crate::report_memory::Budget::new(512, 0);
        let (mut owner, mut reports) = owner_with_budget(budget.clone());
        let (held, stream_task) = held(1);
        let connection = held.id.clone();
        owner.last_held = Some(connection.clone());
        owner
            .outbox
            .connection_started(1, connection.clone(), owner.credential.key.clone(), false);
        owner.phase = Phase::Held(held);
        let report =
            protocol_error::report(connection.clone(), ProtocolErrorKind::Unparseable, None);
        owner.outbox.submit(report);
        let witnesses = budget.witnesses();
        assert_eq!(witnesses.len(), 1);
        let report = Submission::new(connection.clone(), |connection_id, message_id| {
            protocol::tool::ProtocolErrorReport {
                protocol_version: Default::default(),
                connection_id,
                message_id,
                request_message_id: None,
                error: protocol::tool::ProtocolError {
                    code: protocol::tool::ProtocolErrorCode::InvalidMessage,
                    message: "x".repeat(1_000),
                },
            }
        });
        owner.outbox.submit(report);
        let event = reports.recv().await.unwrap();
        assert!(matches!(event, OutboxEvent::ResourceExhausted { .. }));
        owner.handle(Message::Outbox(event));
        assert!(
            matches!(&owner.phase, Phase::Waiting { kind: AttemptKind::Fresh { previous: Some(previous) }, .. } if *previous == connection)
        );
        assert!(stream_task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), async {
            while witnesses[0].upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ended connection releases pending and transport body owners");
        assert_eq!(budget.used(), [0, 0]);
    }

    fn message(kind: &str, message_id: &str, payload: Value) -> SessionMessage {
        let data = json!({
            "protocol_version": 1,
            "connection_id": "conn_a",
            "message_id": message_id,
            "type": kind,
            "payload": payload,
        })
        .to_string();
        match classify::read(data) {
            Read::Session(message) => message,
            other => panic!("{kind} read as {other:?}"),
        }
    }

    fn close(message_id: &str, binding: &str) -> SessionMessage {
        message(
            "session.close",
            message_id,
            json!({ "session_id": "sess_a", "binding_id": binding }),
        )
    }

    fn prepare(message_id: &str) -> SessionMessage {
        message(
            "session.prepare",
            message_id,
            json!({
                "session_id": "sess_new",
                "binding_id": "bind_new",
                "cwd": "/tmp",
                "drives": [],
                "drive_exclude": [],
                "repositories": [],
            }),
        )
    }

    const ROUTED: Admitted = Admitted::Route {
        recorded: true,
        new_operation: false,
    };

    #[test]
    fn with_only_the_reserve_free_a_new_binding_finds_no_room_and_a_close_or_a_routed_binding_is_recorded_from_it()
     {
        let mut ledger = Ledger::new(ledger::Limits::default());
        let mut routed = HashSet::new();
        for index in 0..ledger::ENTRIES_MAX - ledger::RESERVE_ENTRIES {
            let filler = close(&format!("msg_{index}"), "bind_a");
            assert_eq!(admit(&mut ledger, &routed, &filler, 1), ROUTED);
        }
        assert_eq!(ledger.len(), 4_032);

        assert_eq!(
            admit(&mut ledger, &routed, &prepare("msg_prepare"), 1),
            Admitted::Route {
                recorded: false,
                new_operation: true
            }
        );
        assert_eq!(ledger.len(), 4_032);
        assert_eq!(
            admit(&mut ledger, &routed, &close("msg_close", "bind_a"), 1),
            ROUTED
        );
        assert_eq!(ledger.len(), 4_033);
        assert_eq!(
            admit(&mut ledger, &routed, &close("msg_close", "bind_a"), 1),
            Admitted::Duplicate
        );
        routed.insert(("sess_new".to_owned(), "bind_new".to_owned()));
        assert_eq!(
            admit(&mut ledger, &routed, &prepare("msg_prepare_again"), 1),
            ROUTED
        );
        assert_eq!(ledger.len(), 4_034);
    }

    #[test]
    fn the_ledger_is_read_before_the_payload_and_only_closes_and_session_drives_have_their_payload_checked()
     {
        let mut ledger = Ledger::new(ledger::Limits::default());
        let invalid_close = message(
            "session.close",
            "msg_invalid",
            json!({ "session_id": "a/b", "binding_id": "bind_a" }),
        );
        let invalid_drives = message(
            "session.drives",
            "msg_no_drives",
            json!({ "session_id": "sess_a", "binding_id": "bind_a" }),
        );
        let invalid_prepare = message(
            "session.prepare",
            "msg_no_cwd",
            json!({ "session_id": "sess_a", "binding_id": "bind_a" }),
        );
        let invalid_execute = message(
            "tool.execute",
            "msg_no_input",
            json!({
                "session_id": "sess_a",
                "binding_id": "bind_a",
                "tool_use_event_id": "tue_a",
                "task_id": "task_a",
            }),
        );
        let rows = [
            ("a close", close("msg_a", "bind_a"), ROUTED),
            (
                "the same close again",
                close("msg_a", "bind_a"),
                Admitted::Duplicate,
            ),
            (
                "its message_id with another binding",
                close("msg_a", "bind_b"),
                Admitted::Refused(ProtocolErrorKind::Conflict),
            ),
            (
                "a close whose payload fails its schema",
                invalid_close.clone(),
                Admitted::Refused(ProtocolErrorKind::InvalidPayload),
            ),
            (
                "the same invalid close again",
                invalid_close,
                Admitted::Refused(ProtocolErrorKind::InvalidPayload),
            ),
            (
                "a session.drives without drives",
                invalid_drives,
                Admitted::Refused(ProtocolErrorKind::InvalidPayload),
            ),
            (
                "a session.prepare without cwd",
                invalid_prepare,
                Admitted::Route {
                    recorded: true,
                    new_operation: true,
                },
            ),
            (
                "a tool.execute without input",
                invalid_execute,
                Admitted::Route {
                    recorded: true,
                    new_operation: true,
                },
            ),
        ];
        let routed = HashSet::new();
        for (name, message, admitted) in rows {
            assert_eq!(admit(&mut ledger, &routed, &message, 1), admitted, "{name}");
        }
        assert_eq!(ledger.len(), 3);
    }

    /// An owner of the state root at `root`, as an App Server start makes it.
    fn started_in(root: &std::path::Path) -> Owner {
        let root = StateRoot::new(root.as_os_str()).unwrap();
        let budget = crate::report_memory::Budget::new(512, 512);
        owner_rooted(budget, "http://127.0.0.1:1", root).0
    }

    fn lost(request: &str, drive: &str) -> LostChange {
        serde_json::from_value(json!({
            "request_message_id": request, "drive_id": drive,
            "paths": ["a.txt"], "paths_total": 1,
        }))
        .unwrap()
    }

    fn keep(owner: &mut Owner, entries: Vec<LostChange>) {
        let (sent, kept) = std::sync::mpsc::channel();
        owner.port_message(PortMessage::KeepLostChanges(
            entries,
            crate::sessions::Kept(Box::new(move || sent.send(()).unwrap())),
        ));
        kept.try_recv().expect("kept is answered");
    }

    /// The ready a new connection `id` sends, and the connection.
    fn ready_of(owner: &mut Owner, id: &str) -> (Value, Box<Held>) {
        let task = tokio::spawn(std::future::pending::<()>());
        let limit = EventLimit::new(EVENT_MAX_BYTES);
        let Phase::Held(held) =
            owner.new_connection(id_of(id), 1, task.abort_handle(), &limit, Duration::ZERO)
        else {
            panic!("a held connection");
        };
        let ready = held.registration.as_ref().unwrap().ready.as_ref().unwrap();
        (serde_json::from_slice(&ready.body).unwrap(), held)
    }

    fn id_of(text: &str) -> Id {
        Id::new(text).unwrap()
    }

    fn ready_ack(owner: &mut Owner, held: Box<Held>) {
        let message_id = held
            .registration
            .as_ref()
            .unwrap()
            .ready
            .as_ref()
            .unwrap()
            .message_id
            .clone();
        let ack = json!({
            "connection_id": held.id, "message_id": message_id,
            "accepted": true, "valid_for_ms": 45_000,
        });
        let answer = Answer::Success(serde_json::to_vec(&ack).unwrap());
        owner.ready_answered(held, &answer, Duration::ZERO);
    }

    fn kept_file(root: &std::path::Path) -> Value {
        serde_json::from_slice(&std::fs::read(root.join("lost-changes.json")).unwrap()).unwrap()
    }

    /// The `event` of each record.
    #[derive(Clone, Default)]
    struct Events(Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Events {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Name(Option<String>);
            impl tracing::field::Visit for Name {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "event" {
                        self.0 = Some(value.to_owned());
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            let mut name = Name(None);
            event.record(&mut name);
            self.0.lock().unwrap().extend(name.0);
        }
    }

    impl Events {
        fn start(&self) -> tracing::subscriber::DefaultGuard {
            use tracing_subscriber::layer::SubscriberExt;
            tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
        }

        fn has(&self, event: &str) -> bool {
            self.0.lock().unwrap().iter().any(|each| each == event)
        }
    }

    #[tokio::test]
    async fn kept_lost_changes_survive_a_restart_until_a_ready_listing_them_is_acknowledged() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let mut owner = started_in(root);
        keep(&mut owner, vec![lost("close_1", "drive_a")]);
        keep(&mut owner, vec![lost("drives_2", "drive_a")]);
        // The close's report is final; the other report's connection ends.
        let held = ready_of(&mut owner, "conn_a").1;
        let final_acked = OutboxEvent::FinalAcked {
            connection: id_of("conn_a"),
            instruction: id_of("close_1"),
        };
        owner.outbox_event(Phase::Held(held), final_acked, Duration::ZERO);
        assert_eq!(kept_file(root), json!([lost("drives_2", "drive_a")]));

        let mut owner = started_in(root);
        let (ready, _unacknowledged) = ready_of(&mut owner, "conn_b");
        assert_eq!(ready["lost_changes"], json!([lost("drives_2", "drive_a")]));
        let (ready, held) = ready_of(&mut owner, "conn_c");
        assert_eq!(ready["lost_changes"], json!([lost("drives_2", "drive_a")]));
        // An entry kept after that ready was built stays for the next one.
        keep(&mut owner, vec![lost("drives_3", "drive_b")]);
        ready_ack(&mut owner, held);
        assert_eq!(kept_file(root), json!([lost("drives_3", "drive_b")]));
        let (ready, held) = ready_of(&mut owner, "conn_d");
        assert_eq!(ready["lost_changes"], json!([lost("drives_3", "drive_b")]));
        ready_ack(&mut owner, held);
        assert_eq!(kept_file(root), json!([]));
        let (ready, _) = ready_of(&mut owner, "conn_e");
        assert!(ready.get("lost_changes").is_none(), "{ready}");

        // A folder that cannot be written: the entry is still listed.
        let events = Events::default();
        let _guard = events.start();
        let mut permissions = std::fs::metadata(root).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o500);
        std::fs::set_permissions(root, permissions.clone()).unwrap();
        keep(&mut owner, vec![lost("close_4", "drive_a")]);
        let (ready, _) = ready_of(&mut owner, "conn_f");
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o700);
        std::fs::set_permissions(root, permissions).unwrap();
        assert_eq!(ready["lost_changes"], json!([lost("close_4", "drive_a")]));
        assert!(events.has("lost_changes.not_saved"));
        assert_eq!(kept_file(root), json!([]));

        // A file that holds no entry array starts an empty list.
        std::fs::write(root.join("lost-changes.json"), "{}").unwrap();
        let mut owner = started_in(root);
        assert!(events.has("lost_changes.unreadable"));
        let (ready, _) = ready_of(&mut owner, "conn_g");
        assert!(ready.get("lost_changes").is_none(), "{ready}");
    }

    #[tokio::test]
    async fn one_writer_keeps_the_entries_of_two_sessions_released_together() {
        let directory = tempfile::tempdir().unwrap();
        let mut owner = started_in(directory.path());
        let (mailbox, mut inbox) = mpsc::unbounded_channel();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let sessions: Vec<_> = ["sess_a", "sess_b"]
            .into_iter()
            .map(|session| {
                let port = Port {
                    mailbox: mailbox.clone(),
                };
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    protocol::ports::ConnectionPort::keep_lost_changes(
                        &port,
                        vec![lost(&format!("close_{session}"), "drive_a")],
                        Box::new(|| {}),
                    );
                })
            })
            .collect();
        for session in sessions {
            session.join().unwrap();
        }
        drop(mailbox);
        while let Some(Message::Port(message)) = inbox.recv().await {
            owner.port_message(message);
        }
        let mut kept: Vec<String> = kept_file(directory.path())
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["request_message_id"].as_str().unwrap().to_owned())
            .collect();
        kept.sort();
        assert_eq!(kept, ["close_sess_a", "close_sess_b"]);
    }
}
