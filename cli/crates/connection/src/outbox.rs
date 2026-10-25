//! The Report outbox: one task per App Server that owns every report that
//! answers or follows an instruction, from the moment it is frozen under
//! `outbox/` until Engine acknowledges or refuses it or its connection ends.
//!
//! A report gets the next `r<n>` identity of its connection, at most
//! `protocol::ports::LONGEST_REPORT_MESSAGE_ID`, is built and checked by its
//! own decoder, frozen in `outbox/<n>/<message_id>.json` and sent at once in
//! its own request; each retry sends the frozen file again, so its bytes
//! never change. Reports are sent concurrently with no count
//! limit and apart from the Connection owner's requests, so a heartbeat never
//! waits behind one. A report is never sent on another connection: the end
//! of its connection cancels its uploads at once and deletes its directory.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use protocol::bootstrap::BootstrapKey;
use protocol::errors::{ErrorBody, ErrorCode};
use protocol::id::Id;
use protocol::ports::{
    ReportEndpoint, ReportId, ReportListener, ReportOutcome, ReportSink, Submission,
};
use protocol::tool::Acknowledgement;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::backoff::{BASE_MAX, BASE_START, Backoff, random_jitter};
use crate::clock::Clock;
use crate::http::{Answer, Failure, ReportRequest, Transport};
use crate::report_deadline::{self, REPORT_TOTAL, STALL_LIMIT};

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
    /// Engine answered a report of `connection` with `401`, `403` or
    /// `409 stale_connection`, which end the connection; the report waits
    /// for that end.
    Refused { connection: Id, error: ErrorBody },
}

impl OutboxEvent {
    pub fn connection(&self) -> &Id {
        match self {
            OutboxEvent::FinalAcked { connection, .. }
            | OutboxEvent::Refused { connection, .. } => connection,
        }
    }
}

pub struct OutboxConfig {
    /// `outbox/` in the state root, empty at the start.
    pub directory: PathBuf,
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
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Report outbox")
    }
}

enum Message {
    Submit(ReportId, Submission),
    Cancel(ReportId),
    ConnectionStarted {
        number: u64,
        connection: Id,
        tool_execution: bool,
    },
    ConnectionEnded {
        connection: Id,
    },
    Answered {
        report: ReportId,
        attempt: u64,
        answer: Answer,
    },
}

impl Outbox {
    pub fn spawn(config: OutboxConfig) -> Outbox {
        let (mailbox, inbox) = mpsc::unbounded_channel();
        let owner = Owner {
            config,
            mailbox: mailbox.clone(),
            current: None,
            last_attempt: 0,
        };
        tokio::spawn(own(owner, inbox));
        Outbox {
            mailbox,
            ids: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The connection `connection`, the App Server's `number`th, is new;
    /// `tool_execution` says whether its ready advertised tool execution.
    pub fn connection_started(&self, number: u64, connection: Id, tool_execution: bool) {
        self.send(Message::ConnectionStarted {
            number,
            connection,
            tool_execution,
        });
    }

    pub fn connection_ended(&self, connection: Id) {
        self.send(Message::ConnectionEnded { connection });
    }

    fn send(&self, message: Message) {
        self.mailbox
            .send(message)
            .unwrap_or_else(|_| panic!("the Report outbox runs until the process exits"));
    }
}

impl ReportSink for Outbox {
    fn submit(&self, submission: Submission) -> ReportId {
        let report = ReportId(self.ids.fetch_add(1, Ordering::Relaxed) + 1);
        self.send(Message::Submit(report, submission));
        report
    }

    fn cancel(&self, report: ReportId) {
        self.send(Message::Cancel(report));
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
}

/// The current connection and its reports.
struct Current {
    number: u64,
    id: Id,
    tool_execution: bool,
    /// The number of its last report, whose `message_id` is `r<n>`.
    last: u64,
    reports: HashMap<ReportId, Pending>,
}

struct Pending {
    message_id: Id,
    endpoint: ReportEndpoint,
    size: usize,
    final_for: Option<Id>,
    traceparent: Option<String>,
    listener: Option<Arc<dyn ReportListener>>,
    backoff: Backoff,
    retry_at: Option<Duration>,
    /// The running attempt and its tag.
    attempt: Option<(u64, AbortHandle)>,
}

impl Owner {
    async fn handle(&mut self, message: Message) {
        match message {
            Message::Submit(report, submission) => self.submit(report, submission).await,
            Message::Cancel(report) => self.cancel(report).await,
            Message::ConnectionStarted {
                number,
                connection,
                tool_execution,
            } => {
                if let Some(current) = &self.current {
                    let ended = current.id.clone();
                    self.connection_ended(&ended).await;
                }
                self.current = Some(Current {
                    number,
                    id: connection,
                    tool_execution,
                    last: 0,
                    reports: HashMap::new(),
                });
            }
            Message::ConnectionEnded { connection } => self.connection_ended(&connection).await,
            Message::Answered {
                report,
                attempt,
                answer,
            } => self.answered(report, attempt, answer).await,
        }
    }

    async fn submit(&mut self, report: ReportId, submission: Submission) {
        let Some(current) = self
            .current
            .as_mut()
            .filter(|current| current.id == submission.connection_id)
        else {
            tell(&submission.listener, report, ReportOutcome::Dropped);
            return;
        };
        current.last += 1;
        let message_id = Id::new(format!("r{}", current.last)).expect("r<n> is an Id");
        let Ok(bytes) = (submission.build)(&current.id, &message_id) else {
            observe::error!(
                "protocol.sender_check_failed",
                connection.id = observe::Id(current.id.as_str()),
                message.id = observe::Id(message_id.as_str()),
                error.code = observe::Code("sender_check_failed")
            );
            tell(&submission.listener, report, ReportOutcome::Dropped);
            return;
        };
        let (directory, number) = (self.config.directory.clone(), current.number);
        let name = message_id.as_str().to_owned();
        let frozen = bytes.clone();
        let written = tokio::task::spawn_blocking(move || {
            state::outbox::write(&directory, number, &name, &frozen)
        })
        .await;
        if !matches!(written, Ok(Ok(()))) {
            observe::error!(
                "report.dropped",
                connection.id = observe::Id(current.id.as_str()),
                message.id = observe::Id(message_id.as_str()),
                error.code = observe::Code("report_not_written")
            );
            tell(&submission.listener, report, ReportOutcome::Dropped);
            return;
        }
        let times = self.config.times;
        current.reports.insert(
            report,
            Pending {
                message_id,
                endpoint: submission.endpoint,
                size: bytes.len(),
                final_for: submission.final_for,
                traceparent: submission.traceparent,
                listener: submission.listener,
                backoff: Backoff::new(times.backoff_start, times.backoff_max, random_jitter),
                retry_at: None,
                attempt: None,
            },
        );
        self.attempt(report, Some(bytes));
    }

    /// Starts an attempt of `report`, sending `bytes` or, for a retry, the
    /// frozen file.
    fn attempt(&mut self, report: ReportId, bytes: Option<Vec<u8>>) {
        self.last_attempt += 1;
        let tag = self.last_attempt;
        let Some(current) = self.current.as_mut() else {
            return;
        };
        let Some(pending) = current.reports.get_mut(&report) else {
            return;
        };
        let total = report_deadline::total(
            self.config.times.report_total,
            pending.endpoint,
            pending.size,
            current.tool_execution,
        );
        let path = pending.endpoint.path();
        let size = pending.size;
        let traceparent = pending.traceparent.clone();
        let (directory, number) = (self.config.directory.clone(), current.number);
        let name = pending.message_id.as_str().to_owned();
        let transport = self.config.transport.clone();
        let key = self.config.key.clone();
        let clock = self.config.clock.clone();
        let stall = self.config.times.stall;
        let mailbox = self.mailbox.clone();
        let task = tokio::spawn(async move {
            let bytes = match bytes {
                Some(bytes) => Some(bytes),
                None => tokio::task::spawn_blocking(move || {
                    state::outbox::read(&directory, number, &name, size)
                })
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
                .filter(|bytes| bytes.len() == size),
            };
            let answer = match bytes {
                Some(bytes) => {
                    let request = ReportRequest {
                        bytes,
                        traceparent,
                        deadline: clock.now() + total,
                        stall,
                    };
                    transport.post_report(path, &key, request, &clock).await
                }
                None => Answer::Failed(Failure::Unreadable),
            };
            let _ = mailbox.send(Message::Answered {
                report,
                attempt: tag,
                answer,
            });
        })
        .abort_handle();
        pending.attempt = Some((tag, task));
    }

    /// Applies the answer to a report's current attempt, counted as the test
    /// seam counter `outbox.answered`.
    async fn answered(&mut self, report: ReportId, attempt: u64, answer: Answer) {
        let Some(current) = self.current.as_mut() else {
            return;
        };
        let Some(pending) = current.reports.get_mut(&report) else {
            return;
        };
        if pending.attempt.as_ref().map(|(tag, _)| *tag) != Some(attempt) {
            return;
        }
        pending.attempt = None;
        state::seams::count("outbox.answered");
        let outcome = match answer {
            Answer::Success(body) if acknowledges(&body, &current.id, &pending.message_id) => {
                ReportOutcome::Acked
            }
            Answer::Refused(error)
                if matches!(error.code.status(), 401 | 403)
                    || error.code == ErrorCode::StaleConnection =>
            {
                let _ = self.config.events.send(OutboxEvent::Refused {
                    connection: current.id.clone(),
                    error,
                });
                return;
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
                return;
            }
        };
        let connection = current.id.clone();
        let Some(pending) = self.remove(report).await else {
            return;
        };
        tell(&pending.listener, report, outcome);
        if outcome == ReportOutcome::Acked
            && let Some(instruction) = pending.final_for
        {
            let _ = self.config.events.send(OutboxEvent::FinalAcked {
                connection,
                instruction,
            });
        }
    }

    async fn cancel(&mut self, report: ReportId) {
        if let Some(pending) = self.remove(report).await
            && let Some((_, task)) = pending.attempt
        {
            task.abort();
        }
    }

    /// Takes `report` from the current connection and deletes its file.
    async fn remove(&mut self, report: ReportId) -> Option<Pending> {
        let current = self.current.as_mut()?;
        let pending = current.reports.remove(&report)?;
        let (directory, number) = (self.config.directory.clone(), current.number);
        let name = pending.message_id.as_str().to_owned();
        let _ =
            tokio::task::spawn_blocking(move || state::outbox::remove(&directory, number, &name))
                .await;
        Some(pending)
    }

    async fn connection_ended(&mut self, connection: &Id) {
        let Some(current) = self.current.take_if(|current| current.id == *connection) else {
            return;
        };
        for pending in current.reports.values() {
            if let Some((_, task)) = &pending.attempt {
                task.abort();
            }
        }
        let (directory, number) = (self.config.directory.clone(), current.number);
        let _ = tokio::task::spawn_blocking(move || {
            state::outbox::remove_connection(&directory, number)
        })
        .await;
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
        let Some(current) = self.current.as_mut() else {
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
            self.attempt(report, None);
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
        /// Whether the report's frozen file existed when the request came.
        frozen_then: bool,
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

    #[derive(Debug, Clone, Copy)]
    enum Reply {
        Unavailable,
        WrongAck,
        Ack,
    }

    /// An Engine that answers each report with the next scripted reply,
    /// `503` once the script is done.
    struct Engine {
        url: String,
        requests: Arc<Mutex<Vec<Request>>>,
    }

    impl Engine {
        async fn start(replies: Vec<Reply>, outbox: PathBuf) -> Engine {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
            let log = requests.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let (log, replies, outbox) = (log.clone(), replies.clone(), outbox.clone());
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
                        let frozen_then = std::fs::read_dir(&outbox).unwrap().any(|connection| {
                            connection
                                .unwrap()
                                .path()
                                .join(format!("{message_id}.json"))
                                .exists()
                        });
                        log.lock().unwrap().push(Request {
                            headers,
                            body,
                            frozen_then,
                        });
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
                            Reply::WrongAck => (200, ack("r999")),
                            Reply::Ack => (200, ack(&message_id)),
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
        directory: PathBuf,
        outcomes: Arc<Outcomes>,
        _events: mpsc::UnboundedReceiver<OutboxEvent>,
        _root: tempfile::TempDir,
    }

    fn test_clock() -> TestClock {
        TestClock(Arc::new(AtomicU64::new(1_000_000_000_000)))
    }

    fn spawn_outbox(
        url: &str,
        directory: &std::path::Path,
        clock: &TestClock,
        times: OutboxTimes,
    ) -> (Outbox, mpsc::UnboundedReceiver<OutboxEvent>) {
        state::outbox::reset(directory).unwrap();
        let (events, receiver) = mpsc::unbounded_channel();
        let outbox = Outbox::spawn(OutboxConfig {
            directory: directory.to_owned(),
            transport: Arc::new(Transport::new(url)),
            key: protocol::bootstrap::Bootstrap::decode(
                br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
            )
            .unwrap()
            .connection_key,
            times,
            clock: clock.clock(),
            events,
        });
        (outbox, receiver)
    }

    async fn setup(replies: Vec<Reply>) -> Setup {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("outbox");
        state::outbox::reset(&directory).unwrap();
        let engine = Engine::start(replies, directory.clone()).await;
        let clock = test_clock();
        let (outbox, events) =
            spawn_outbox(&engine.url, &directory, &clock, OutboxTimes::default());
        Setup {
            outbox,
            engine,
            clock,
            directory,
            outcomes: Arc::new(Outcomes::default()),
            _events: events,
            _root: root,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn a_report_is_sent_again_from_its_frozen_file_until_its_matching_ack() {
        let setup = setup(vec![
            Reply::Unavailable,
            Reply::Unavailable,
            Reply::WrongAck,
            Reply::Ack,
            Reply::Ack,
        ])
        .await;
        setup.outbox.connection_started(1, id("conn_a"), false);
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
            assert!(attempt.frozen_then, "attempt {index}");
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
        assert!(!setup.directory.join("1/r1.json").exists());

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
    async fn a_connection_end_deletes_its_reports_and_sends_none_of_them_again() {
        let setup = setup(vec![Reply::Unavailable, Reply::Unavailable, Reply::Ack]).await;
        setup.outbox.connection_started(1, id("conn_a"), false);
        let reports = [
            setup.outbox.submit(protocol_error(&setup, "conn_a", None)),
            setup.outbox.submit(protocol_error(&setup, "conn_a", None)),
        ];
        until(&setup, "both first attempts", || {
            setup.engine.requests().len() >= 2
        })
        .await;
        assert!(setup.directory.join("1").is_dir());
        setup.outbox.connection_ended(id("conn_a"));
        until(&setup, "both reports dropped", || {
            reports
                .iter()
                .all(|report| setup.outcomes.of(*report) == [ReportOutcome::Dropped])
        })
        .await;
        assert!(!setup.directory.join("1").exists());
        setup.clock.advance(Duration::from_secs(120));

        setup.outbox.connection_started(2, id("conn_b"), false);
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
        /// One that reads the request's headers and nothing of its body.
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
    }

    /// A local listener that stalls each connection as `stall` says.
    async fn stalling(stall: Stall, clock: TestClock) -> (String, Arc<Mutex<Seen>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
                let mut stream = BufReader::new(stream);
                if !matches!(stall, Stall::Handshake) {
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
                    if matches!(stall, Stall::Answer) {
                        let mut body = vec![0; length];
                        stream.read_exact(&mut body).await.unwrap();
                    }
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
            let root = tempfile::tempdir().unwrap();
            let clock = test_clock();
            let (url, seen) = stalling(stall, clock.clone()).await;
            // A retry follows the end of an attempt at the same time.
            let times = OutboxTimes {
                backoff_start: Duration::ZERO,
                backoff_max: Duration::ZERO,
                ..OutboxTimes::default()
            };
            let (outbox, _events) = spawn_outbox(&url, &root.path().join("outbox"), &clock, times);
            outbox.connection_started(1, id("conn_a"), false);
            outbox.submit(tool_list(100_000));
            // The clock stands still until the listener has read the first
            // request as far as it reads it, so its bytes were taken then.
            let start = std::time::Instant::now();
            while seen.lock().unwrap().read == 0 {
                assert!(start.elapsed() < WAIT_LIMIT, "{stall:?}: no first attempt");
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
