//! In-memory reports, admitted before the mailbox and shared unchanged by
//! pending delivery and every HTTP attempt. The last body reference releases
//! its budget; a connection end cancels delivery and drops pending reports.
//!
//! During a credential switch, from `switch_started` until the new key or
//! the connection's end, no attempt or retry starts; a `401
//! credential_revoked` answer to an attempt sent with an earlier key is no
//! refusal, and the report is sent again with the new key under its
//! `message_id`, because Engine revokes the old key when it commits the new
//! one while attempts sent with it may still be in flight.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use protocol::bootstrap::BootstrapKey;
use protocol::errors::{ErrorBody, ErrorCode};
use protocol::id::Id;
use protocol::ports::{
    ReportAdmissionError, ReportEndpoint, ReportId, ReportListener, ReportOutcome, ReportSink,
    Submission,
};
use protocol::tool::Acknowledgement;
use state::clock::Clock;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::backoff::{BASE_MAX, BASE_START, Backoff, random_jitter};
use crate::http::{Answer, ReportRequest, Transport};
use crate::report_deadline::{self, REPORT_TOTAL, STALL_LIMIT};
use crate::report_memory::{Budget, NORMAL_BYTES, RESERVED_BYTES};

/// The durations the outbox runs by, each a constant but for tests.
#[derive(Debug, Clone, Copy)]
pub struct OutboxTimes {
    pub report_total: Duration,
    pub stall: Duration,
    pub backoff_start: Duration,
    pub backoff_max: Duration,
}

impl Default for OutboxTimes {
    fn default() -> OutboxTimes {
        OutboxTimes {
            report_total: REPORT_TOTAL,
            stall: STALL_LIMIT,
            backoff_start: BASE_START,
            backoff_max: BASE_MAX,
        }
    }
}

/// What the outbox tells the Connection owner.
#[derive(Debug)]
pub enum OutboxEvent {
    /// The final report of the instruction `instruction` of `connection` is
    /// acknowledged.
    FinalAcked { connection: Id, instruction: Id },
    /// Engine answered a report or a drive transfer request of
    /// `connection` with `401`, `403` or `409 stale_connection`, which end
    /// the connection; the request waits for that end.
    Refused { connection: Id, error: ErrorBody },
    /// Even reserved capacity could not retain a control reply or compact error.
    ResourceExhausted { connection: Id },
}

impl OutboxEvent {
    pub fn connection(&self) -> &Id {
        match self {
            OutboxEvent::FinalAcked { connection, .. }
            | OutboxEvent::Refused { connection, .. }
            | OutboxEvent::ResourceExhausted { connection } => connection,
        }
    }
}

pub struct OutboxConfig {
    pub transport: Arc<Transport>,
    pub key: BootstrapKey,
    pub times: OutboxTimes,
    pub clock: Clock,
    pub events: mpsc::UnboundedSender<OutboxEvent>,
}

/// A handle on the Report outbox.
#[derive(Clone)]
pub struct Outbox {
    mailbox: mpsc::UnboundedSender<Message>,
    ids: Arc<AtomicU64>,
    admission: Arc<Mutex<Option<(Id, u64)>>>,
    budget: Arc<Budget>,
    events: mpsc::UnboundedSender<OutboxEvent>,
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Report outbox")
    }
}

enum Message {
    Submit(ReportId, Admitted),
    Cancel(ReportId),
    AddFinal(ReportId, Id),
    ConnectionStarted {
        connection: Id,
        key: BootstrapKey,
        tool_execution: bool,
    },
    ConnectionEnded {
        connection: Id,
    },
    SwitchStarted {
        connection: Id,
    },
    SwitchEnded {
        connection: Id,
        key: BootstrapKey,
    },
    Answered {
        report: ReportId,
        attempt: u64,
        answer: Answer,
    },
}

impl Outbox {
    pub fn spawn(config: OutboxConfig) -> Outbox {
        Self::with_budget(config, Budget::new(NORMAL_BYTES, RESERVED_BYTES))
    }

    pub(crate) fn with_budget(config: OutboxConfig, budget: Arc<Budget>) -> Outbox {
        let events = config.events.clone();
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let owner = Owner {
            key: config.key.clone(),
            config,
            mailbox: mailbox.clone(),
            current: None,
            last_attempt: 0,
            key_number: 0,
        };
        tokio::spawn(own(owner, inbox));
        Outbox {
            mailbox,
            ids: Arc::new(AtomicU64::new(0)),
            admission: Arc::new(Mutex::new(None)),
            budget,
            events,
        }
    }

    /// The connection `connection`, the App Server's `number`th, is new and
    /// authenticated with `key`; `tool_execution` says whether its ready
    /// advertised tool execution.
    pub fn connection_started(
        &self,
        _number: u64,
        connection: Id,
        key: BootstrapKey,
        tool_execution: bool,
    ) {
        let mut admission = self.admission.lock().expect("report admission lock");
        *admission = Some((connection.clone(), 0));
        self.send(Message::ConnectionStarted {
            connection,
            key,
            tool_execution,
        });
    }

    /// A credential switch of `connection` begins: no attempt starts until
    /// `switch_ended` or the connection's end.
    pub fn switch_started(&self, connection: Id) {
        self.send(Message::SwitchStarted { connection });
    }

    /// The switch of `connection` ends with `key`, which every later
    /// attempt carries.
    pub fn switch_ended(&self, connection: Id, key: BootstrapKey) {
        self.send(Message::SwitchEnded { connection, key });
    }

    pub fn connection_ended(&self, connection: Id) {
        let mut admission = self.admission.lock().expect("report admission lock");
        if admission
            .as_ref()
            .is_some_and(|(current, _)| *current == connection)
        {
            *admission = None;
        }
        self.send(Message::ConnectionEnded { connection });
    }

    fn send(&self, message: Message) {
        self.mailbox
            .send(message)
            .unwrap_or_else(|_| panic!("the Report outbox runs until the process exits"));
    }
}

impl Outbox {
    fn admit(
        &self,
        submission: Submission,
        reserve: bool,
    ) -> Result<ReportId, ReportAdmissionError> {
        let report = ReportId(self.ids.fetch_add(1, Ordering::Relaxed) + 1);
        let message_id = {
            let mut admission = self.admission.lock().expect("report admission lock");
            match admission
                .as_mut()
                .filter(|(connection, _)| *connection == submission.connection_id)
            {
                Some((_, last)) => {
                    *last += 1;
                    Id::new(format!("r{last}")).expect("r<n> is an Id")
                }
                None => {
                    drop(admission);
                    tell(&submission.listener, report, ReportOutcome::Dropped);
                    return Ok(report);
                }
            }
        };
        // Build and sender-check without holding either admission or budget locks.
        let Ok(bytes) = (submission.build)(&submission.connection_id, &message_id) else {
            observe::error!(
                "protocol.sender_check_failed",
                connection.id = observe::Id(submission.connection_id.as_str()),
                message.id = observe::Id(message_id.as_str()),
                error.code = observe::Code("sender_check_failed")
            );
            tell(&submission.listener, report, ReportOutcome::Dropped);
            return Ok(report);
        };
        let mut admission = self.admission.lock().expect("report admission lock");
        if !admission
            .as_ref()
            .is_some_and(|(connection, _)| *connection == submission.connection_id)
        {
            drop(admission);
            tell(&submission.listener, report, ReportOutcome::Dropped);
            return Ok(report);
        }
        let Some(bytes) = self.budget.retain(bytes, reserve) else {
            if !reserve {
                return Err(ReportAdmissionError::ResourceExhausted);
            }
            *admission = None;
            drop(admission);
            tell(&submission.listener, report, ReportOutcome::Dropped);
            let _ = self.events.send(OutboxEvent::ResourceExhausted {
                connection: submission.connection_id,
            });
            return Ok(report);
        };
        self.send(Message::Submit(
            report,
            Admitted {
                connection_id: submission.connection_id,
                message_id,
                bytes,
                endpoint: submission.endpoint,
                final_for: submission.final_for,
                traceparent: submission.traceparent,
                listener: submission.listener,
            },
        ));
        Ok(report)
    }
}

impl ReportSink for Outbox {
    fn submit(&self, submission: Submission) -> ReportId {
        self.admit(submission, true)
            .expect("reserved admission ends the connection on exhaustion")
    }

    fn try_submit(&self, submission: Submission) -> Result<ReportId, ReportAdmissionError> {
        self.admit(submission, false)
    }

    fn cancel(&self, report: ReportId) {
        self.send(Message::Cancel(report));
    }

    fn add_final(&self, report: ReportId, instruction: Id) {
        self.send(Message::AddFinal(report, instruction));
    }
}

async fn own(mut owner: Owner, mut inbox: mpsc::UnboundedReceiver<Message>) {
    loop {
        let wake = owner.next_retry();
        let clock = owner.config.clock.clone();
        tokio::select! {
            message = inbox.recv() => match message {
                Some(message) => owner.handle(message).await,
                None => return,
            },
            () = async {
                match wake {
                    Some(at) => clock.sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {}
        }
        owner.send_due();
    }
}

struct Owner {
    config: OutboxConfig,
    mailbox: mpsc::UnboundedSender<Message>,
    current: Option<Current>,
    last_attempt: u64,
    /// The key every new attempt carries, and its number, which a new key
    /// raises.
    key: BootstrapKey,
    key_number: u64,
}

/// The current connection and its reports.
struct Current {
    id: Id,
    tool_execution: bool,
    /// From the switch's start until its new key: no attempt starts.
    switching: bool,
    reports: HashMap<ReportId, Pending>,
}

struct Admitted {
    connection_id: Id,
    message_id: Id,
    bytes: Bytes,
    endpoint: ReportEndpoint,
    final_for: Vec<Id>,
    traceparent: Option<String>,
    listener: Option<Arc<dyn ReportListener>>,
}

struct Pending {
    message_id: Id,
    endpoint: ReportEndpoint,
    bytes: Bytes,
    final_for: Vec<Id>,
    traceparent: Option<String>,
    listener: Option<Arc<dyn ReportListener>>,
    backoff: Backoff,
    retry_at: Option<Duration>,
    /// The running attempt, its tag and the number of the key it carries.
    attempt: Option<Running>,
}

struct Running {
    tag: u64,
    task: AbortHandle,
    key_number: u64,
}

impl Owner {
    async fn handle(&mut self, message: Message) {
        match message {
            Message::Submit(report, submission) => self.submit(report, submission).await,
            Message::Cancel(report) => self.cancel(report).await,
            Message::AddFinal(report, instruction) => {
                if let Some(pending) = self
                    .current
                    .as_mut()
                    .and_then(|current| current.reports.get_mut(&report))
                {
                    pending.final_for.push(instruction);
                }
            }
            Message::ConnectionStarted {
                connection,
                key,
                tool_execution,
            } => {
                if let Some(current) = &self.current {
                    let ended = current.id.clone();
                    self.connection_ended(&ended).await;
                }
                self.new_key(key);
                self.current = Some(Current {
                    id: connection,
                    tool_execution,
                    switching: false,
                    reports: HashMap::new(),
                });
            }
            Message::ConnectionEnded { connection } => self.connection_ended(&connection).await,
            Message::SwitchStarted { connection } => {
                if let Some(current) = self.current.as_mut().filter(|c| c.id == connection) {
                    current.switching = true;
                }
            }
            Message::SwitchEnded { connection, key } => self.switch_ended(&connection, key),
            Message::Answered {
                report,
                attempt,
                answer,
            } => self.answered(report, attempt, answer).await,
        }
    }

    async fn submit(&mut self, report: ReportId, submission: Admitted) {
        let Some(current) = self
            .current
            .as_mut()
            .filter(|current| current.id == submission.connection_id)
        else {
            tell(&submission.listener, report, ReportOutcome::Dropped);
            return;
        };
        let times = self.config.times;
        current.reports.insert(
            report,
            Pending {
                message_id: submission.message_id,
                bytes: submission.bytes,
                endpoint: submission.endpoint,
                final_for: submission.final_for,
                traceparent: submission.traceparent,
                listener: submission.listener,
                backoff: Backoff::new(times.backoff_start, times.backoff_max, random_jitter),
                retry_at: None,
                attempt: None,
            },
        );
        self.attempt(report);
    }

    fn new_key(&mut self, key: BootstrapKey) {
        if key != self.key {
            self.key = key;
            self.key_number += 1;
        }
    }

    /// Takes the switch's new key and starts every report that waits for
    /// it: one submitted during the switch, and one whose attempt with an
    /// earlier key got `401 credential_revoked`.
    fn switch_ended(&mut self, connection: &Id, key: BootstrapKey) {
        let Some(current) = self.current.as_mut().filter(|c| c.id == *connection) else {
            return;
        };
        current.switching = false;
        let waiting: Vec<ReportId> = current
            .reports
            .iter()
            .filter(|(_, pending)| pending.attempt.is_none() && pending.retry_at.is_none())
            .map(|(report, _)| *report)
            .collect();
        self.new_key(key);
        for report in waiting {
            self.attempt(report);
        }
    }

    /// Every attempt shares the original immutable body and its budget
    /// charge. None starts during a switch: the report waits for its key,
    /// counted as the test seam counter `outbox.key_awaited`.
    fn attempt(&mut self, report: ReportId) {
        self.last_attempt += 1;
        let tag = self.last_attempt;
        let Some(current) = self.current.as_mut() else {
            return;
        };
        if current.switching {
            state::seams::count("outbox.key_awaited");
            return;
        }
        let Some(pending) = current.reports.get_mut(&report) else {
            return;
        };
        let total = report_deadline::total(
            self.config.times.report_total,
            pending.endpoint,
            pending.bytes.len(),
            current.tool_execution,
        );
        let path = pending.endpoint.path();
        let bytes = pending.bytes.clone();
        let traceparent = pending.traceparent.clone();
        let transport = self.config.transport.clone();
        let key = self.key.clone();
        let clock = self.config.clock.clone();
        let stall = self.config.times.stall;
        let mailbox = self.mailbox.clone();
        let task = tokio::spawn(async move {
            let request = ReportRequest {
                bytes,
                traceparent,
                deadline: clock.now() + total,
                stall,
            };
            let answer = transport.post_report(path, &key, request, &clock).await;
            let _ = mailbox.send(Message::Answered {
                report,
                attempt: tag,
                answer,
            });
        })
        .abort_handle();
        pending.attempt = Some(Running {
            tag,
            task,
            key_number: self.key_number,
        });
    }

    /// Applies the answer to a report's current attempt, counted as the test
    /// seam counter `outbox.answered` once it is applied, so that a test
    /// that waits for the count finds a retry already scheduled.
    async fn answered(&mut self, report: ReportId, attempt: u64, answer: Answer) {
        if self.apply_answer(report, attempt, answer).await {
            state::seams::count("outbox.answered");
        }
    }

    /// Whether the answer was to the report's current attempt.
    async fn apply_answer(&mut self, report: ReportId, attempt: u64, answer: Answer) -> bool {
        let Some(current) = self.current.as_mut() else {
            return false;
        };
        let Some(pending) = current.reports.get_mut(&report) else {
            return false;
        };
        let Some(running) = pending.attempt.take_if(|running| running.tag == attempt) else {
            return false;
        };
        let outcome = match answer {
            Answer::Success(body) if acknowledges(&body, &current.id, &pending.message_id) => {
                ReportOutcome::Acked
            }
            Answer::Refused(error)
                if error.code == ErrorCode::CredentialRevoked
                    && (current.switching || running.key_number < self.key_number) =>
            {
                // It waits for the switch's key, or goes again with the key
                // that replaced its own.
                let again = !current.switching;
                if again {
                    self.attempt(report);
                }
                return true;
            }
            Answer::Refused(error)
                if matches!(error.code.status(), 401 | 403)
                    || error.code == ErrorCode::StaleConnection =>
            {
                let _ = self.config.events.send(OutboxEvent::Refused {
                    connection: current.id.clone(),
                    error,
                });
                return true;
            }
            Answer::Refused(error) => {
                observe::error!(
                    "report.dropped",
                    connection.id = observe::Id(current.id.as_str()),
                    message.id = observe::Id(pending.message_id.as_str()),
                    http.response.status_code = i64::from(error.code.status()),
                    error.code = observe::Code(error.code.as_str())
                );
                ReportOutcome::Dropped
            }
            Answer::Success(_) | Answer::Failed(_) => {
                pending.retry_at = Some(self.config.clock.now() + pending.backoff.next_wait());
                return true;
            }
        };
        let connection = current.id.clone();
        let Some(pending) = self.remove(report).await else {
            return true;
        };
        tell(&pending.listener, report, outcome);
        if outcome == ReportOutcome::Acked {
            for instruction in pending.final_for {
                let _ = self.config.events.send(OutboxEvent::FinalAcked {
                    connection: connection.clone(),
                    instruction,
                });
            }
        }
        true
    }

    async fn cancel(&mut self, report: ReportId) {
        if let Some(pending) = self.remove(report).await
            && let Some(running) = pending.attempt
        {
            running.task.abort();
        }
    }

    /// Releases the pending owner; a live transport reference remains charged.
    async fn remove(&mut self, report: ReportId) -> Option<Pending> {
        self.current.as_mut()?.reports.remove(&report)
    }

    async fn connection_ended(&mut self, connection: &Id) {
        let Some(current) = self.current.take_if(|current| current.id == *connection) else {
            return;
        };
        for pending in current.reports.values() {
            if let Some(running) = &pending.attempt {
                running.task.abort();
            }
        }
        for (report, pending) in current.reports {
            tell(&pending.listener, report, ReportOutcome::Dropped);
        }
    }

    fn next_retry(&self) -> Option<Duration> {
        self.current
            .as_ref()?
            .reports
            .values()
            .filter_map(|pending| pending.retry_at)
            .min()
    }

    /// Starts every retry whose wait has passed.
    fn send_due(&mut self) {
        let now = self.config.clock.now();
        let Some(current) = self.current.as_mut().filter(|current| !current.switching) else {
            return;
        };
        let due: Vec<ReportId> = current
            .reports
            .iter_mut()
            .filter(|(_, pending)| pending.retry_at.is_some_and(|at| now >= at))
            .map(|(report, pending)| {
                pending.retry_at = None;
                *report
            })
            .collect();
        for report in due {
            self.attempt(report);
        }
    }
}

/// Whether `body` is the matching ACK of report `message_id` of `connection`.
fn acknowledges(body: &[u8], connection: &Id, message_id: &Id) -> bool {
    protocol::decode::<Acknowledgement>(body)
        .is_ok_and(|ack| ack.connection_id == *connection && ack.message_id == *message_id)
}

fn tell(listener: &Option<Arc<dyn ReportListener>>, report: ReportId, outcome: ReportOutcome) {
    if let Some(listener) = listener {
        listener.outcome(report, outcome);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    use protocol::tool::{ProtocolError, ProtocolErrorCode, ProtocolErrorReport};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    use super::*;

    const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    /// Bounds the wait for something that must happen, so a failure ends the
    /// test instead of hanging it.
    const WAIT_LIMIT: Duration = Duration::from_secs(10);

    #[derive(Debug, Clone)]
    struct Request {
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Request {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(header, _)| header.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }

        fn message_id(&self) -> String {
            let report: serde_json::Value = serde_json::from_slice(&self.body).unwrap();
            report["message_id"].as_str().unwrap().to_owned()
        }
    }

    #[derive(Debug, Clone)]
    enum Reply {
        Unavailable,
        LostAck,
        Refused,
        WrongAck,
        Ack,
        /// The ACK, once the test notifies.
        HeldAck(Arc<tokio::sync::Notify>),
    }

    /// An Engine that answers each report with the next scripted reply,
    /// `503` once the script is done.
    struct Engine {
        url: String,
        requests: Arc<Mutex<Vec<Request>>>,
    }

    impl Engine {
        async fn start(replies: Vec<Reply>) -> Engine {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
            let log = requests.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let (log, replies) = (log.clone(), replies.clone());
                    tokio::spawn(async move {
                        let mut stream = BufReader::new(stream);
                        let mut headers = Vec::new();
                        let mut line = String::new();
                        stream.read_line(&mut line).await.unwrap();
                        loop {
                            line.clear();
                            stream.read_line(&mut line).await.unwrap();
                            let Some((name, value)) = line.trim_end().split_once(": ") else {
                                break;
                            };
                            headers.push((name.to_owned(), value.to_owned()));
                        }
                        let length: usize = headers
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.parse().unwrap())
                            .unwrap();
                        let mut body = vec![0; length];
                        stream.read_exact(&mut body).await.unwrap();
                        let report: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let message_id = report["message_id"].as_str().unwrap().to_owned();
                        log.lock().unwrap().push(Request { headers, body });
                        let reply = replies.lock().unwrap().pop_front();
                        let ack = |message_id: &str| {
                            serde_json::json!({
                                "connection_id": report["connection_id"],
                                "message_id": message_id,
                                "accepted": true,
                            })
                            .to_string()
                        };
                        let (status, answer) = match reply.unwrap_or(Reply::Unavailable) {
                            Reply::Unavailable => (503, String::new()),
                            Reply::LostAck => return,
                            Reply::Refused => (
                                400,
                                serde_json::json!({
                                    "code": "invalid_message", "message": "invalid report"
                                })
                                .to_string(),
                            ),
                            Reply::WrongAck => (200, ack("r999")),
                            Reply::Ack => (200, ack(&message_id)),
                            Reply::HeldAck(release) => {
                                release.notified().await;
                                (200, ack(&message_id))
                            }
                        };
                        let response = format!(
                            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                            answer.len()
                        );
                        let _ = stream.get_mut().write_all(response.as_bytes()).await;
                    });
                }
            });
            Engine { url, requests }
        }

        fn requests(&self) -> Vec<Request> {
            self.requests.lock().unwrap().clone()
        }
    }

    /// A clock that moves only when the test moves it.
    #[derive(Clone)]
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn clock(&self) -> Clock {
            let nanos = self.0.clone();
            Clock::new(
                move || Duration::from_nanos(nanos.load(Ordering::Acquire)),
                Duration::from_millis(1),
            )
        }

        fn advance(&self, step: Duration) {
            self.0
                .fetch_add(u64::try_from(step.as_nanos()).unwrap(), Ordering::AcqRel);
        }

        fn now(&self) -> Duration {
            Duration::from_nanos(self.0.load(Ordering::Acquire))
        }
    }

    #[derive(Default)]
    struct Outcomes(Mutex<Vec<(ReportId, ReportOutcome)>>);

    impl ReportListener for Outcomes {
        fn outcome(&self, report: ReportId, outcome: ReportOutcome) {
            self.0.lock().unwrap().push((report, outcome));
        }
    }

    impl Outcomes {
        fn of(&self, report: ReportId) -> Vec<ReportOutcome> {
            let outcomes = self.0.lock().unwrap();
            outcomes
                .iter()
                .filter(|(each, _)| *each == report)
                .map(|(_, outcome)| *outcome)
                .collect()
        }
    }

    struct Setup {
        outbox: Outbox,
        engine: Engine,
        clock: TestClock,
        outcomes: Arc<Outcomes>,
        _events: mpsc::UnboundedReceiver<OutboxEvent>,
    }

    fn test_clock() -> TestClock {
        TestClock(Arc::new(AtomicU64::new(1_000_000_000_000)))
    }

    fn test_key() -> BootstrapKey {
        protocol::bootstrap::Bootstrap::decode(
            br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap()
        .connection_key
    }

    fn spawn_outbox(
        transport: Arc<Transport>,
        clock: &TestClock,
        times: OutboxTimes,
    ) -> (Outbox, mpsc::UnboundedReceiver<OutboxEvent>) {
        let (events, receiver) = mpsc::unbounded_channel();
        let outbox = Outbox::spawn(OutboxConfig {
            transport,
            key: test_key(),
            times,
            clock: clock.clock(),
            events,
        });
        (outbox, receiver)
    }

    async fn setup(replies: Vec<Reply>) -> Setup {
        let engine = Engine::start(replies).await;
        let clock = test_clock();
        let transport = Arc::new(Transport::new(&engine.url));
        let (outbox, events) = spawn_outbox(transport, &clock, OutboxTimes::default());
        Setup {
            outbox,
            engine,
            clock,
            outcomes: Arc::new(Outcomes::default()),
            _events: events,
        }
    }

    fn id(text: &str) -> Id {
        Id::new(text).unwrap()
    }

    fn protocol_error(setup: &Setup, connection: &str, traceparent: Option<&str>) -> Submission {
        let mut submission = Submission::new(id(connection), |connection_id, message_id| {
            ProtocolErrorReport {
                protocol_version: Default::default(),
                connection_id,
                message_id,
                request_message_id: Some(Id::new("msg_bad").unwrap()),
                error: ProtocolError {
                    code: ProtocolErrorCode::InvalidMessage,
                    message: "the message cannot be parsed".to_owned(),
                },
            }
        });
        submission.traceparent = traceparent.map(str::to_owned);
        let listener: Arc<dyn ReportListener> = setup.outcomes.clone();
        submission.listener = Some(listener);
        submission
    }

    /// Waits until `done` holds, moving the clock a second at a time so that
    /// every pending retry comes due.
    async fn until(setup: &Setup, what: &str, done: impl Fn() -> bool) {
        let start = std::time::Instant::now();
        while !done() {
            assert!(start.elapsed() < WAIT_LIMIT, "{what} did not happen");
            setup.clock.advance(Duration::from_secs(1));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn admission_charges_the_mailbox_and_rejects_only_the_new_result() {
        let mut setup = setup(vec![]).await;
        setup.outbox.budget = Budget::new(512, 512);
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        // This current-thread task has not yielded since the submission;
        // the outbox owner has not read its mailbox yet.
        let first = setup
            .outbox
            .try_submit(protocol_error(&setup, "conn_a", None))
            .unwrap();
        let held = setup.outbox.budget.used()[0];
        assert!(held > 0);
        let witnesses = setup.outbox.budget.witnesses();
        assert_eq!(witnesses.len(), 1);
        assert!(witnesses[0].upgrade().is_some());
        // A complete legal larger report cannot use the reserved capacity.
        assert_eq!(
            setup.outbox.try_submit(tool_list(1_000)),
            Err(ReportAdmissionError::ResourceExhausted)
        );
        assert_eq!(setup.outbox.budget.used(), [held, 0]);
        assert!(setup.outcomes.of(first).is_empty());
        setup.outbox.cancel(first);
        until(&setup, "cancelled body destroyed", || {
            witnesses[0].upgrade().is_none()
        })
        .await;
        assert_eq!(setup.outbox.budget.used(), [0, 0]);
        assert!(
            setup.outcomes.of(first).is_empty(),
            "explicit cancel promises no callback"
        );
    }

    #[tokio::test]
    async fn a_terminal_ack_and_a_rejected_new_result_leave_the_unacked_running_body_unchanged() {
        let mut setup = setup(vec![Reply::Unavailable, Reply::Ack, Reply::Unavailable]).await;
        setup.outbox.budget = Budget::new(4_096, 512);
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), true);
        let report = |phase: &'static str, status: &'static str, seq: i64| {
            let mut submission = Submission::new(id("conn_a"), move |connection_id, message_id| {
                serde_json::from_value::<protocol::tool::ToolStatus>(serde_json::json!({
                    "protocol_version": 1, "connection_id": connection_id, "message_id": message_id,
                    "session_id": "sess_a", "binding_id": "bind_a", "task_id": "task_a",
                    "tool_use_event_id": "call_a", "request_message_id": "msg_execute",
                    "kind": "execution", "phase": phase, "report_seq": seq,
                    "result": {
                        "schema_version": 1, "tool": "exec_command", "status": status,
                        "truncated": false, "error": null, "result": {
                            "task_id": "task_a", "exit_code": if phase == "terminal" { Some(0) } else { None },
                            "signal": null, "timed_out": false, "duration_ms": 0,
                            "stdout": { "text": "", "total_bytes": 0, "total_lines": 0, "returned_bytes": 0, "truncated": false },
                            "stderr": { "text": "", "total_bytes": 0, "total_lines": 0, "returned_bytes": 0, "truncated": false },
                        },
                        "metrics": { "wall_time_ms": 0, "queued_ms": 0, "drive_wait_ms": 0, "run_ms": 0, "sync_ms": 0 },
                    },
                })).unwrap()
            });
            submission.listener = Some(setup.outcomes.clone());
            submission
        };
        let running = setup
            .outbox
            .try_submit(report("running", "running", 1))
            .unwrap();
        let running_bytes = setup.outbox.budget.used()[0];
        tokio::time::timeout(WAIT_LIMIT, async {
            while setup.engine.requests().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let terminal = setup
            .outbox
            .try_submit(report("terminal", "success", 2))
            .unwrap();
        let witnesses = setup.outbox.budget.witnesses();
        assert_eq!(witnesses.len(), 2);
        tokio::time::timeout(WAIT_LIMIT, async {
            while setup.outcomes.of(terminal) != [ReportOutcome::Acked]
                || witnesses[1].upgrade().is_some()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(setup.outcomes.of(running).is_empty());
        assert!(witnesses[0].upgrade().is_some());
        assert_eq!(setup.outbox.budget.used(), [running_bytes, 0]);
        assert_eq!(
            setup.outbox.try_submit(tool_list(5_000)),
            Err(ReportAdmissionError::ResourceExhausted)
        );
        assert_eq!(setup.outbox.budget.used(), [running_bytes, 0]);
        setup.clock.advance(Duration::from_secs(2));
        until(&setup, "the unconfirmed running report retried", || {
            setup.engine.requests().len() >= 3
        })
        .await;
        let requests = setup.engine.requests();
        assert_eq!(requests[0].message_id(), requests[2].message_id());
        assert_eq!(requests[0].body, requests[2].body);
        setup.outbox.connection_ended(id("conn_a"));
        until(&setup, "the remaining body released", || {
            witnesses[0].upgrade().is_none()
        })
        .await;
        assert_eq!(setup.outbox.budget.used(), [0, 0]);
    }

    #[tokio::test]
    async fn a_prepared_reply_uses_reserve_and_only_its_matching_ack_completes_it() {
        use protocol::connection::{SessionOutcome, SessionStatus};
        let mut setup = setup(vec![Reply::WrongAck, Reply::Ack]).await;
        setup.outbox.budget = Budget::new(0, 4_096);
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        let mut submission =
            Submission::new(id("conn_a"), |connection_id, message_id| SessionStatus {
                connection_id,
                message_id,
                request_message_id: id("msg_prepare"),
                session_id: id("sess_a"),
                binding_id: id("bind_a"),
                outcome: SessionOutcome::Prepared {
                    cwd: serde_json::from_str("\"/tmp\"").unwrap(),
                    drive_mount_failures: None,
                    repository_failures: None,
                },
            });
        submission.final_for.push(id("msg_prepare"));
        submission.listener = Some(setup.outcomes.clone());
        let report = setup.outbox.submit(submission);
        assert_eq!(setup.outbox.budget.used()[0], 0);
        assert!(setup.outbox.budget.used()[1] > 0);
        until(&setup, "prepared reply acknowledged", || {
            setup.outcomes.of(report) == [ReportOutcome::Acked]
        })
        .await;
        let requests = setup.engine.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].body, requests[1].body);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&requests[0].body).unwrap()["status"],
            "prepared"
        );
        assert!(
            matches!(setup._events.recv().await, Some(OutboxEvent::FinalAcked { instruction, .. }) if instruction == id("msg_prepare"))
        );
        until(&setup, "reserved body released", || {
            setup.outbox.budget.used() == [0, 0]
        })
        .await;
    }

    #[tokio::test]
    async fn exhausted_reserve_ends_admission_without_acknowledging_any_report() {
        let mut setup = setup(vec![]).await;
        setup.outbox.budget = Budget::new(0, 0);
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        let report = setup.outbox.submit(protocol_error(&setup, "conn_a", None));
        assert_eq!(setup.outcomes.of(report), [ReportOutcome::Dropped]);
        assert!(
            matches!(setup._events.recv().await, Some(OutboxEvent::ResourceExhausted { connection }) if connection == id("conn_a"))
        );
        let later = setup.outbox.submit(protocol_error(&setup, "conn_a", None));
        assert_eq!(setup.outcomes.of(later), [ReportOutcome::Dropped]);
        assert!(setup._events.try_recv().is_err());
        assert!(setup.engine.requests().is_empty());
        assert_eq!(setup.outbox.budget.used(), [0, 0]);
    }

    #[tokio::test]
    async fn a_deterministic_refusal_drops_and_destroys_the_body() {
        let setup = setup(vec![Reply::Refused]).await;
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        let report = setup.outbox.submit(protocol_error(&setup, "conn_a", None));
        let witnesses = setup.outbox.budget.witnesses();
        until(&setup, "refused body dropped", || {
            setup.outcomes.of(report) == [ReportOutcome::Dropped]
        })
        .await;
        until(&setup, "refused body destroyed", || {
            witnesses.iter().all(|body| body.upgrade().is_none())
        })
        .await;
        assert!(setup.engine.requests().len() == 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_report_retries_the_same_bytes_until_its_matching_ack() {
        let setup = setup(vec![
            Reply::LostAck,
            Reply::Unavailable,
            Reply::WrongAck,
            Reply::Ack,
            Reply::Ack,
        ])
        .await;
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        let traced = setup
            .outbox
            .submit(protocol_error(&setup, "conn_a", Some(TRACEPARENT)));
        until(&setup, "four attempts", || {
            setup.engine.requests().len() >= 4
        })
        .await;
        until(&setup, "the ACK", || {
            setup.outcomes.of(traced) == [ReportOutcome::Acked]
        })
        .await;
        let attempts = setup.engine.requests();
        let first = attempts[0].body.clone();
        for (index, attempt) in attempts.iter().take(4).enumerate() {
            assert_eq!(attempt.body, first, "attempt {index}");
            assert_eq!(
                attempt.header("traceparent"),
                Some(TRACEPARENT),
                "attempt {index}"
            );
            assert_eq!(
                attempt.header("content-length"),
                Some(first.len().to_string().as_str())
            );
        }
        assert_eq!(attempts[0].message_id(), "r1");
        until(&setup, "the acknowledged body released", || {
            setup.outbox.budget.used() == [0, 0]
        })
        .await;
        assert!(
            setup
                .outbox
                .budget
                .witnesses()
                .iter()
                .all(|body| body.upgrade().is_none())
        );

        let untraced = setup.outbox.submit(protocol_error(&setup, "conn_a", None));
        until(&setup, "the second report's ACK", || {
            setup.outcomes.of(untraced) == [ReportOutcome::Acked]
        })
        .await;
        let second = setup.engine.requests().last().unwrap().clone();
        assert_eq!(second.message_id(), "r2");
        assert_eq!(second.header("traceparent"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_ack_to_an_attempt_with_the_key_a_switch_replaced_acknowledges_the_report() {
        let release = Arc::new(tokio::sync::Notify::new());
        let setup = setup(vec![Reply::HeldAck(release.clone())]).await;
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        let report = setup.outbox.submit(protocol_error(&setup, "conn_a", None));
        until(&setup, "the attempt with the old key", || {
            setup.engine.requests().len() == 1
        })
        .await;
        let renewed = protocol::bootstrap::Bootstrap::decode(
            br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_b","connection_key":"tetral_ck_b","expires_at":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap()
        .connection_key;
        setup.outbox.switch_started(id("conn_a"));
        setup.outbox.switch_ended(id("conn_a"), renewed);
        release.notify_one();
        until(&setup, "the ACK", || {
            setup.outcomes.of(report) == [ReportOutcome::Acked]
        })
        .await;
        assert_eq!(setup.engine.requests().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_end_deletes_its_reports_and_sends_none_of_them_again() {
        let setup = setup(vec![Reply::Unavailable, Reply::Unavailable, Reply::Ack]).await;
        setup
            .outbox
            .connection_started(1, id("conn_a"), test_key(), false);
        let reports = [
            setup.outbox.submit(protocol_error(&setup, "conn_a", None)),
            setup.outbox.submit(protocol_error(&setup, "conn_a", None)),
        ];
        until(&setup, "both first attempts", || {
            setup.engine.requests().len() >= 2
        })
        .await;
        assert!(setup.outbox.budget.used()[0] > 0);
        setup.outbox.connection_ended(id("conn_a"));
        until(&setup, "both reports dropped", || {
            reports
                .iter()
                .all(|report| setup.outcomes.of(*report) == [ReportOutcome::Dropped])
        })
        .await;
        until(&setup, "ended bodies released", || {
            setup.outbox.budget.used() == [0, 0]
        })
        .await;
        setup.clock.advance(Duration::from_secs(120));

        setup
            .outbox
            .connection_started(2, id("conn_b"), test_key(), false);
        let stale = setup.outbox.submit(protocol_error(&setup, "conn_a", None));
        let next = setup.outbox.submit(protocol_error(&setup, "conn_b", None));
        until(&setup, "the next connection's ACK", || {
            setup.outcomes.of(next) == [ReportOutcome::Acked]
        })
        .await;
        assert_eq!(setup.outcomes.of(stale), [ReportOutcome::Dropped]);
        let requests = setup.engine.requests();
        let sent: Vec<(String, String)> = requests
            .iter()
            .map(|request| {
                let report: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                (
                    report["connection_id"].as_str().unwrap().to_owned(),
                    request.message_id(),
                )
            })
            .collect();
        let expected = [("conn_a", "r1"), ("conn_a", "r2"), ("conn_b", "r1")]
            .map(|(connection, message)| (connection.to_owned(), message.to_owned()));
        let mut sorted = sent.clone();
        sorted[..2].sort();
        assert_eq!(sorted, expected);
    }

    /// How a listener stops an attempt from making progress.
    #[derive(Debug, Clone, Copy)]
    enum Stall {
        /// An `https` endpoint that accepts the TCP connection and never
        /// answers the TLS handshake.
        Handshake,
        /// One that reads nothing of the request, so that its body stops
        /// short of being acknowledged; a read would let the kernel open its
        /// window again at a time of its own.
        Body,
        /// One that reads the whole request and never answers.
        Answer,
    }

    /// What a stalling listener saw: the clock's time at each connection it
    /// accepted, and how many requests it has read as far as it reads them.
    #[derive(Default)]
    struct Seen {
        accepted: Vec<Duration>,
        read: usize,
        /// The connections a body stall reads nothing from.
        unread: Vec<std::net::TcpStream>,
    }

    impl Seen {
        /// The bytes that the first unread connection's kernel has taken.
        fn queued(&self) -> u64 {
            let mut buffer = vec![0; 1 << 20];
            self.unread[0]
                .peek(&mut buffer)
                .map_or(0, |count| count as u64)
        }
    }

    /// A local listener that stalls each connection as `stall` says. Its
    /// receive buffer is far smaller than a report, so that a body it does not
    /// read stops short of being acknowledged.
    async fn stalling(stall: Stall, clock: TestClock) -> (String, Arc<Mutex<Seen>>) {
        let listening = tokio::net::TcpSocket::new_v4().unwrap();
        listening.set_recv_buffer_size(4_096).unwrap();
        listening.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listening.listen(16).unwrap();
        let scheme = match stall {
            Stall::Handshake => "https",
            Stall::Body | Stall::Answer => "http",
        };
        let url = format!("{scheme}://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Seen::default()));
        let log = seen.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                log.lock().unwrap().accepted.push(clock.now());
                if matches!(stall, Stall::Body) {
                    let mut seen = log.lock().unwrap();
                    seen.unread.push(stream.into_std().unwrap());
                    seen.read += 1;
                    continue;
                }
                let mut stream = BufReader::new(stream);
                if matches!(stall, Stall::Answer) {
                    let mut length = 0;
                    let mut line = String::new();
                    while stream.read_line(&mut line).await.unwrap() > 2 {
                        if let Some(value) =
                            line.to_ascii_lowercase().strip_prefix("content-length: ")
                        {
                            length = value.trim().parse().unwrap();
                        }
                        line.clear();
                    }
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).await.unwrap();
                }
                log.lock().unwrap().read += 1;
                held.push(stream);
            }
        });
        (url, seen)
    }

    /// A local MCP tool-list report of about `bytes`.
    fn tool_list(bytes: usize) -> Submission {
        Submission::new(id("conn_a"), move |connection_id, message_id| {
            serde_json::from_value::<protocol::connection::McpTools>(serde_json::json!({
                "protocol_version": 1,
                "connection_id": connection_id,
                "message_id": message_id,
                "server": "files",
                "seq": 1,
                "server_info": { "name": "files", "title": null, "version": "1.0.0" },
                "tools": [{
                    "name": "read",
                    "description": "x".repeat(bytes),
                    "inputSchema": { "type": "object" },
                }],
                "error": null,
            }))
            .unwrap()
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_handshake_body_or_answer_ends_the_attempt_at_ten_seconds_of_a_twenty_second_total()
     {
        let size = (tool_list(100_000).build)(&id("conn_a"), &id("r1"))
            .unwrap()
            .len();
        assert_eq!(
            report_deadline::total(REPORT_TOTAL, ReportEndpoint::McpTools, size, false),
            Duration::from_secs(20)
        );
        for stall in [Stall::Handshake, Stall::Body, Stall::Answer] {
            let clock = test_clock();
            let (url, seen) = stalling(stall, clock.clone()).await;
            // A retry follows the end of an attempt at the same time.
            let times = OutboxTimes {
                backoff_start: Duration::ZERO,
                backoff_max: Duration::ZERO,
                ..OutboxTimes::default()
            };
            let transport = Arc::new(Transport::new(&url));
            let (outbox, _events) = spawn_outbox(transport.clone(), &clock, times);
            outbox.connection_started(1, id("conn_a"), test_key(), false);
            outbox.submit(tool_list(100_000));
            // The clock stands still until the listener has read the first
            // request as far as it reads it.
            let start = std::time::Instant::now();
            while seen.lock().unwrap().read == 0 {
                assert!(start.elapsed() < WAIT_LIMIT, "{stall:?}: no first attempt");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // The attempt sees its socket's acknowledgements only when it
            // reads the socket, and the listener's kernel may acknowledge the
            // last bytes it takes only after a delay of its own, so the clock
            // stands still until the attempt has seen them: all that the
            // unread connection holds, or the body sent.
            while !match stall {
                Stall::Handshake => true,
                Stall::Body => {
                    transport.attempts()[0].acknowledged == Some(seen.lock().unwrap().queued())
                }
                Stall::Answer => transport.attempts()[0].sent,
            } {
                assert!(
                    start.elapsed() < WAIT_LIMIT,
                    "{stall:?}: {:?}",
                    transport.attempts()
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // Coarse steps up to 9.5 seconds, then fine ones, so that a retry
            // before 10 seconds shows at the clock's time; the clock then
            // stays at 10 seconds until the retry, however long that takes.
            while seen.lock().unwrap().accepted.len() < 2 {
                assert!(start.elapsed() < WAIT_LIMIT, "{stall:?}: no second attempt");
                let elapsed = clock.now() - seen.lock().unwrap().accepted[0];
                if elapsed < Duration::from_millis(9_500) {
                    clock.advance(Duration::from_millis(500));
                } else if elapsed < Duration::from_secs(10) {
                    clock.advance(Duration::from_millis(1));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let accepted = seen.lock().unwrap().accepted.clone();
            let ended = accepted[1] - accepted[0];
            assert_eq!(
                ended,
                Duration::from_secs(10),
                "{stall:?}: the attempt was sent again after {ended:?}"
            );
        }
    }
}
