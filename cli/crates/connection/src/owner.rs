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
//! is acted on within that interval of waking.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use observe::STARTUP;
use protocol::bootstrap::BootstrapKey;
use protocol::connection::{DisconnectPayload, Downstream, DownstreamPayload, Heartbeat};
use protocol::errors::{ErrorBody, ErrorCode};
use protocol::field::PositiveInt;
use protocol::id::Id;
use protocol::local::{ConnectionState, ErrorObject};
use protocol::time::Rfc3339Utc;
use protocol::tool::ProtocolVersion;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::attempt::{self, ATTEMPT_TOTAL, AttemptKind};
use crate::backoff::{BASE_MAX, BASE_START, Backoff, random_jitter};
use crate::heartbeat::Heartbeats;
use crate::http::{Answer, REQUEST_TOTAL, Transport};
use crate::ready::{self, ReadyFields};
use crate::staging::{Admission, STAGING_MAX, Staging};
use crate::status::{ConnectionStatus, Reconnect, Reported};
use crate::stream::EVENT_MAX_BYTES;

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
    pub event_max_bytes: usize,
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

#[derive(Debug, Clone)]
pub struct Config {
    pub server_url: String,
    pub instance_id: Id,
    pub credential: Credential,
    pub ready: ReadyFields,
    /// The App Server's start on the seam clock.
    pub started: Duration,
    pub times: Times,
}

/// A Session instruction as it arrived on the stream.
#[derive(Debug, Clone)]
pub struct Instruction {
    pub downstream: Downstream,
    pub data: String,
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
        let owner = Owner::new(config, mailbox.clone());
        tokio::spawn(own(owner, inbox));
        Connection { mailbox }
    }

    pub async fn status(&self) -> ConnectionStatus {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Status(reply));
        answer
            .await
            .expect("the Connection owner answers every message")
    }

    /// Stops handling downstream messages, closes the stream and cancels
    /// every request: `stopping`, then `stopped`, which lasts.
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

async fn own(mut owner: Owner, mut inbox: mpsc::UnboundedReceiver<Message>) {
    owner.start();
    loop {
        let wake = owner.wake_in();
        tokio::select! {
            message = inbox.recv() => match message {
                Some(message) => owner.handle(message),
                None => return,
            },
            () = sleep_or_wait(wake) => {}
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
}

impl Owner {
    fn new(config: Config, mailbox: mpsc::UnboundedSender<Message>) -> Owner {
        let times = config.times;
        Owner {
            transport: Arc::new(Transport::new(&config.server_url)),
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
            Phase::Connecting(self.attempt(kind, state::seams::now()))
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
                let _ = reply.send(());
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
            Message::Status(_) | Message::Stop(_) => phase,
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
        self.reported.change(ConnectionState::Stopped);
    }

    /// The time until the owner must read the clock again, while anything
    /// waits for a time.
    fn wake_in(&self) -> Option<Duration> {
        let next = match &self.phase {
            Phase::Waiting { at, .. } => *at,
            Phase::Connecting(attempt) => attempt.deadline,
            Phase::Held(held) => held.next_time(),
            Phase::Blocked | Phase::Stopped => return None,
        };
        let recheck = state::seams::recheck_interval();
        Some(next.saturating_sub(state::seams::now()).min(recheck))
    }

    /// Acts on every time that has come, then counts the check as the test
    /// seam counter `connection.checked`.
    fn check(&mut self) {
        let now = state::seams::now();
        let phase = std::mem::replace(&mut self.phase, Phase::Stopped);
        self.phase = match phase {
            Phase::Waiting { at, kind } if now >= at => Phase::Connecting(self.attempt(kind, now)),
            Phase::Connecting(attempt) if now >= attempt.deadline => {
                self.attempt_failed(Phase::Connecting(attempt), now)
            }
            Phase::Held(held) => self.check_held(held, now),
            other => other,
        };
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
                let kind = AttemptKind::Resume {
                    connection: held.id.clone(),
                };
                held.stream = Stream::Resuming(self.attempt(kind, now));
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

    /// Starts an attempt, counted as the test seam counter
    /// `connection.attempt`.
    fn attempt(&mut self, kind: AttemptKind, now: Duration) -> Attempt {
        state::seams::count("connection.attempt");
        let stream = self.tag();
        let task = tokio::spawn(attempt::run(
            self.transport.clone(),
            self.credential.key.clone(),
            kind.query(&self.instance_id, &self.credential.credential_id),
            self.times.event_max_bytes,
            stream,
            self.mailbox.clone(),
        ))
        .abort_handle();
        Attempt {
            stream,
            kind,
            deadline: now + self.times.attempt_total,
            task,
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
            Phase::Held(held) => release_held(*held),
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
                        self.new_connection(connection, stream, attempt.task, now)
                    }
                    _ => Phase::Connecting(attempt),
                }
            }
            Phase::Held(mut held) => {
                let resume = match &held.stream {
                    Stream::Resuming(attempt) if attempt.stream == stream => {
                        Some((attempt.kind.connected(&data), attempt.task.clone()))
                    }
                    _ => None,
                };
                let live =
                    matches!(&held.stream, Stream::Live { stream: live, .. } if *live == stream);
                match resume {
                    Some((Some((_, true)), task)) => {
                        held.stream = Stream::Live { stream, task };
                        self.reported.reconnected(Reconnect::Resume);
                        Phase::Held(held)
                    }
                    Some((Some((connection, false)), task)) => {
                        // The old connection ends, but not this stream, which
                        // carries the new one.
                        held.stream = Stream::Waiting { at: now };
                        release_held(*held);
                        self.new_connection(connection, stream, task, now)
                    }
                    Some((None, _)) => Phase::Held(held),
                    None if live => self.live_event(held, data, now),
                    None => Phase::Held(held),
                }
            }
            other => other,
        }
    }

    fn new_connection(&mut self, id: Id, stream: u64, task: AbortHandle, now: Duration) -> Phase {
        STARTUP.first_connected(now);
        if self.held_count > 0 {
            self.reported.reconnected(Reconnect::New);
        }
        self.held_count += 1;
        self.last_held = Some(id.clone());
        let mut held = Box::new(Held {
            id,
            stream: Stream::Live { stream, task },
            deadline: None,
            registration: None,
            confirmed: false,
            messages: 0,
            heartbeats: Heartbeats::default(),
            heartbeat: None,
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

    fn live_event(&mut self, mut held: Box<Held>, data: String, now: Duration) -> Phase {
        // A message that does not decode changes nothing here; the protocol
        // error reports take it.
        let Ok(downstream) = protocol::decode::<Downstream>(data.as_bytes()) else {
            return Phase::Held(held);
        };
        if downstream.connection_id != held.id {
            return Phase::Held(held);
        }
        if is_session_instruction(&downstream.payload) {
            let instruction = Instruction { downstream, data };
            let admission = match held.registration.as_mut() {
                Some(registration) => registration.staging.admit(instruction),
                None => Admission::Route(instruction),
            };
            return match admission {
                Admission::Staged => Phase::Held(held),
                Admission::Route(instruction) => {
                    route(instruction);
                    Phase::Held(held)
                }
                Admission::Overflow => self.end(Phase::Held(held), now),
            };
        }
        match downstream.payload {
            DownstreamPayload::Connected(_) => {
                observe::warn!(
                    "connection.unexpected_connected",
                    connection.id = observe::Id(held.id.as_str())
                );
                Phase::Held(held)
            }
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
                    route(instruction);
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

fn release_held(held: Held) {
    match held.stream {
        Stream::Live { task, .. } => task.abort(),
        Stream::Resuming(attempt) => attempt.task.abort(),
        Stream::Waiting { .. } => {}
    }
}

fn is_session_instruction(payload: &DownstreamPayload) -> bool {
    matches!(
        payload,
        DownstreamPayload::SessionPrepare(_)
            | DownstreamPayload::SessionClose(_)
            | DownstreamPayload::SessionDrives(_)
            | DownstreamPayload::ToolExecute(_)
            | DownstreamPayload::ToolStdin(_)
            | DownstreamPayload::ToolCancel(_)
            | DownstreamPayload::McpCall(_)
    )
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

/// Hands a Session instruction on once the ready ACK came. Its consumers
/// are the Session binding owners and the tool path; it is counted as the
/// test seam counter `route.session`.
fn route(_instruction: Instruction) {
    state::seams::count("route.session");
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
