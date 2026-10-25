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
//! The owner also admits each `session.prepare` before it reaches a Session
//! binding owner ([`crate::sessions`]) and holds the probe slots
//! ([`crate::probe_slots`]). Its stop is answered once every probe that
//! holds a slot has been reaped, so that the shutdown waits for them until
//! its own deadline.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use observe::{Identity, STARTUP, TraceContext};
use protocol::bootstrap::BootstrapKey;
use protocol::connection::{
    DisconnectPayload, Downstream, DownstreamPayload, DownstreamType, Envelope, Heartbeat,
    PreparationError, PreparationErrorCode, SessionOutcome, SessionStatus,
};
use protocol::errors::{ErrorBody, ErrorCode};
use protocol::field::{Nullable, PositiveInt};
use protocol::fingerprint::fingerprint;
use protocol::id::Id;
use protocol::local::{ConnectionState, ErrorObject, SessionEntry, SessionState};
use protocol::ports::{
    Close, Prepare, ProbeSlot, ReportSink, SessionFactory, Submission, ToolCall,
};
use protocol::time::Rfc3339Utc;
use protocol::tool::ProtocolVersion;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::attempt::{self, ATTEMPT_TOTAL, AttemptKind};
use crate::backoff::{BASE_MAX, BASE_START, Backoff, random_jitter};
use crate::classify::{self, Read, SessionMessage};
use crate::clock::Clock;
use crate::heartbeat::Heartbeats;
use crate::http::{Answer, REQUEST_TOTAL, Transport};
use crate::ledger::{self, Entry, EntryState, Ledger, Need, Seen};
use crate::outbox::{Outbox, OutboxConfig, OutboxEvent, OutboxTimes};
use crate::probe_slots::{ProbeSlots, SlotLimits};
use crate::protocol_error::{self, ProtocolErrorKind};
use crate::ready::{self, ReadyFields};
use crate::sessions::{Owners, Port, PortMessage, RECORDS_MAX, SessionsView};
use crate::staging::{Admission, STAGING_MAX, Staging};
use crate::status::{ConnectionStatus, Reconnect, Reported};
use crate::stream::{EVENT_MAX_BYTES, EventLimit, TOOL_EVENT_MAX_BYTES};

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
    pub ready: ReadyFields,
    /// The App Server's start on the seam clock.
    pub started: Duration,
    /// `outbox/` in the state root, emptied at App Server start.
    pub outbox: PathBuf,
    /// Creates the Session binding owner of each Session.
    pub sessions: Arc<dyn SessionFactory>,
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
    Outbox(OutboxEvent),
    Port(PortMessage),
    Sessions(oneshot::Sender<SessionsView>),
}

/// A handle on the Connection owner.
#[derive(Debug, Clone)]
pub struct Connection {
    mailbox: mpsc::UnboundedSender<Message>,
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
        let outbox = Outbox::spawn(OutboxConfig {
            directory: config.outbox.clone(),
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
        let owner = Owner::new(config, transport, outbox, owners, mailbox.clone());
        tokio::spawn(own(owner, inbox, reports));
        Connection { mailbox }
    }

    pub async fn status(&self) -> ConnectionStatus {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Status(reply));
        answer
            .await
            .expect("the Connection owner answers every message")
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
) {
    owner.start();
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
    /// Whether the confirmed heartbeat came.
    confirmed: bool,
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
    staging: Staging<Instruction>,
}

/// An upstream message that is sent again, with its `message_id` and first
/// send time, until it is answered.
struct Outgoing {
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
    Heartbeat {
        seq: i64,
        message_id: Id,
        first_send: Duration,
    },
}

struct Pending {
    connection: Id,
    operation: Operation,
    task: AbortHandle,
}

struct Owner {
    instance_id: Id,
    credential: Credential,
    ready: ReadyFields,
    started: Duration,
    times: Times,
    transport: Arc<Transport>,
    outbox: Outbox,
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
    slots: ProbeSlots,
    /// The replies to stop, sent once no probe holds a slot.
    stop_replies: Vec<oneshot::Sender<()>>,
}

impl Owner {
    fn new(
        config: Config,
        transport: Arc<Transport>,
        outbox: Outbox,
        owners: Owners,
        mailbox: mpsc::UnboundedSender<Message>,
    ) -> Owner {
        let times = config.times;
        Owner {
            owners,
            slots: ProbeSlots::new(times.probe_slots),
            stop_replies: Vec::new(),
            transport,
            outbox,
            instance_id: config.instance_id,
            credential: config.credential,
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
            Phase::Connecting(self.attempt(kind, state::seams::now(), limit))
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
                self.answer_stop();
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
            _ if matches!(self.phase, Phase::Stopped) => return,
            _ => state::seams::now(),
        };
        let phase = std::mem::replace(&mut self.phase, Phase::Stopped);
        self.phase = match message {
            Message::Refused { stream, answer } => self.refused(phase, stream, answer, now),
            Message::Event { stream, data } => self.event(phase, stream, data, now),
            Message::StreamEnded { stream } => self.stream_ended(phase, stream, now),
            Message::Answered { request, answer } => self.answered(phase, request, answer, now),
            Message::Outbox(event) => self.outbox_event(phase, event, now),
            Message::Status(_) | Message::Stop(_) | Message::Port(_) | Message::Sessions(_) => {
                phase
            }
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
        let phase = std::mem::replace(&mut self.phase, Phase::Stopped);
        self.release(phase);
        for (_, pending) in self.requests.drain() {
            pending.task.abort();
        }
        for requester in self.slots.cancel(state::seams::now()) {
            self.owners.answer(&requester, ProbeSlot::Cancelled);
        }
        self.owners.shutdown();
        self.reported.change(ConnectionState::Stopped);
    }

    fn answer_stop(&mut self) {
        if self.slots.running() == 0 {
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
                self.answer_stop();
            }
            PortMessage::RecordGone(session_id) => self.owners.record_gone(&session_id),
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
                Phase::Connecting(self.attempt(kind, now, limit))
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
                held.stream = Stream::Resuming(self.attempt(kind, now, held.event_max_bytes));
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

    /// Sends ready and the latest heartbeat again once their retry is due.
    fn send_due(&mut self, held: &mut Held, now: Duration) {
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
            let operation = Operation::Heartbeat {
                seq: heartbeat.seq,
                message_id: heartbeat.message_id.clone(),
                first_send: heartbeat.first_send,
            };
            let body = heartbeat.body.clone();
            self.post(&held.id, HEARTBEAT_PATH, operation, body);
        }
    }

    fn tag(&mut self) -> u64 {
        self.last_tag += 1;
        self.last_tag
    }

    /// Starts an attempt whose stream reads events of up to
    /// `max_event_bytes`, counted as the test seam counter
    /// `connection.attempt`.
    fn attempt(&mut self, kind: AttemptKind, now: Duration, max_event_bytes: usize) -> Attempt {
        state::seams::count("connection.attempt");
        let stream = self.tag();
        let limit = EventLimit::new(max_event_bytes);
        let task = tokio::spawn(attempt::run(
            self.transport.clone(),
            self.credential.key.clone(),
            kind.query(&self.instance_id, &self.credential.credential_id),
            limit.clone(),
            stream,
            self.mailbox.clone(),
        ))
        .abort_handle();
        Attempt {
            stream,
            kind,
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
                    )),
                    _ => None,
                };
                let live =
                    matches!(&held.stream, Stream::Live { stream: live, .. } if *live == stream);
                match resume {
                    Some((Some((_, true)), task, _)) => {
                        held.stream = Stream::Live { stream, task };
                        self.reported.reconnected(Reconnect::Resume);
                        Phase::Held(held)
                    }
                    Some((Some((connection, false)), task, limit)) => {
                        // The old connection ends, but not this stream, which
                        // carries the new one.
                        held.stream = Stream::Waiting { at: now };
                        self.release_held(*held);
                        self.new_connection(connection, stream, task, &limit, now)
                    }
                    Some((None, _, _)) => Phase::Held(held),
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
        if self.held_count > 0 {
            self.reported.reconnected(Reconnect::New);
        }
        self.held_count += 1;
        self.last_held = Some(id.clone());
        let tool_execution = self.ready.capabilities.tool_execution.is_some();
        self.outbox
            .connection_started(self.held_count, id.clone(), tool_execution);
        let mut held = Box::new(Held {
            id,
            stream: Stream::Live { stream, task },
            deadline: None,
            registration: None,
            confirmed: false,
            messages: 0,
            heartbeats: Heartbeats::default(),
            heartbeat: None,
            ledger: Ledger::new(self.times.ledger),
            routed: HashSet::new(),
            event_max_bytes: self.times.event_max_bytes,
        });
        let message_id = held.next_message_id();
        let built = ready::ready(&self.ready, &held.id, message_id.clone(), now);
        let ready = ready::checked(&built, &held.id, &message_id).map(|body| Outgoing {
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
            Read::Session(message) => self.session_instruction(held, stream, message, now),
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
                Phase::Held(held)
            }
            OutboxEvent::Refused { error, .. } => self.request_refused(held, &error, now),
        }
    }

    fn heartbeat_event(&mut self, held: &mut Held, event: Id, seq: PositiveInt, now: Duration) {
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
        let operation = Operation::Heartbeat {
            seq: seq.get(),
            message_id: message_id.clone(),
            first_send: now,
        };
        self.post(&held.id, HEARTBEAT_PATH, operation, body.clone());
        held.heartbeat = Some(Outgoing {
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
        let held = match phase {
            Phase::Held(held) if held.id == pending.connection => held,
            other => return other,
        };
        match (answer, pending.operation) {
            (Answer::Refused(error), _) => self.request_refused(held, &error, now),
            (answer, Operation::Ready) => self.ready_answered(held, &answer, now),
            (
                answer,
                Operation::Heartbeat {
                    seq,
                    message_id,
                    first_send,
                },
            ) => self.heartbeat_answered(held, &answer, seq, &message_id, first_send, now),
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
                held_ref.registration = None;
                for instruction in staged {
                    self.route(held_ref, instruction);
                }
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
        seq: i64,
        message_id: &Id,
        first_send: Duration,
        now: Duration,
    ) -> Phase {
        let granted = match answer {
            Answer::Success(body) => ready::grant(body, &held.id, message_id, first_send, now),
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
                    if !held.confirmed {
                        held.confirmed = true;
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
    /// Closes the connection's stream; the outbox drops its reports, the
    /// waiting probe-slot requests are cancelled and every Session binding
    /// owner ends the connection's bindings.
    fn release_held(&mut self, held: Held) {
        match held.stream {
            Stream::Live { task, .. } => task.abort(),
            Stream::Resuming(attempt) => attempt.task.abort(),
            Stream::Waiting { .. } => {}
        }
        for requester in self.slots.cancel(state::seams::now()) {
            self.owners.answer(&requester, ProbeSlot::Cancelled);
        }
        self.owners.connection_ended(&held.id);
        self.outbox.connection_ended(held.id);
    }

    /// Hands a Session instruction on once the ready ACK came, counted as
    /// the test seam counter `route.session`: a `session.prepare`, a
    /// `session.close` or a tool instruction to the Session binding owner of
    /// its `session_id`.
    fn route(&mut self, held: &mut Held, instruction: Instruction) {
        state::seams::count("route.session");
        let _entered = instruction.span.clone().entered();
        match instruction.message.envelope.kind {
            DownstreamType::SessionPrepare => self.route_prepare(held, instruction),
            DownstreamType::SessionClose => self.route_close(held, &instruction),
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
        report.final_for = Some(request_message_id);
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
}

impl Held {
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
            stream,
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(Duration::MAX)
    }
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

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
}
