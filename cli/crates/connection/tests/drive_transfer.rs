//! The drive transfer client against a local HTTP stub on loopback that
//! answers from `protocol/drive-transfer-v1.fixtures.json`, on a clock the
//! test moves: the answers it accepts, the class of each answer, the
//! deadlines, the retries and their delays, a rebind, the Connection
//! owner's answers and its link, and a download that resumes or restarts.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use connection::OutboxEvent;
use connection::drive_transfer::{DriveLink, HttpDriveTransfer, total};
use drive::transfer::{
    Binding, Commit, Content, ContentSink, Download, DriveTransfer, Ended, ListChanges, ListFiles,
    Request, Upload,
};
use protocol::bootstrap::{Bootstrap, BootstrapKey};
use protocol::drive_transfer::CommitChange;
use protocol::field::Sha256;
use protocol::id::Id;
use protocol::path::DrivePath;
use serde_json::Value;
use state::clock::Clock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc, watch};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

const CONTENT: &[u8] = b"hello, drive";

fn key() -> BootstrapKey {
    Bootstrap::decode(
        br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
    )
    .unwrap()
    .connection_key
}

/// The key a credential switch installs.
fn renewed_key() -> BootstrapKey {
    Bootstrap::decode(
        br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_b","connection_key":"tetral_ck_b","expires_at":"2099-01-01T00:00:00Z"}"#,
    )
    .unwrap()
    .connection_key
}

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn fixtures() -> &'static Vec<Value> {
    static FIXTURES: OnceLock<Vec<Value>> = OnceLock::new();
    FIXTURES.get_or_init(|| {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../protocol/drive-transfer-v1.fixtures.json"
        );
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    })
}

fn fixture(name: &str) -> &'static Value {
    fixtures()
        .iter()
        .find(|fixture| fixture["name"] == name)
        .unwrap_or_else(|| panic!("no fixture {name}"))
}

/// The `drive_transfer.answer_rejected` records, each as its error code,
/// followed by ` with error.message` when it carries a cause.
fn rejections() -> &'static Mutex<Vec<String>> {
    static RECORDS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();
    RECORDS.get_or_init(|| {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Recorded(records.clone()));
        tracing::subscriber::set_global_default(subscriber).unwrap();
        records
    })
}

struct Recorded(Arc<Mutex<Vec<String>>>);

#[derive(Default)]
struct Fields {
    event: Option<String>,
    code: Option<String>,
    cause: bool,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "event" => self.event = Some(value.to_owned()),
            "error.code" => self.code = Some(value.to_owned()),
            "error.message" => self.cause = true,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Recorded {
    fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.event.as_deref() == Some("drive_transfer.answer_rejected") {
            let cause = if fields.cause {
                " with error.message"
            } else {
                ""
            };
            self.0
                .lock()
                .unwrap()
                .push(fields.code.unwrap_or_default() + cause);
        }
    }
}

/// A clock the test moves, which counts its reads.
#[derive(Clone, Default)]
struct TestClock {
    nanos: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
}

impl TestClock {
    fn clock(&self) -> Clock {
        let (nanos, reads) = (self.nanos.clone(), self.reads.clone());
        Clock::new(
            move || {
                reads.fetch_add(1, Ordering::SeqCst);
                Duration::from_nanos(nanos.load(Ordering::SeqCst))
            },
            Duration::from_millis(1),
        )
    }

    fn now(&self) -> Duration {
        Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
    }

    fn set(&self, at: Duration) {
        self.nanos
            .store(u64::try_from(at.as_nanos()).unwrap(), Ordering::SeqCst);
    }

    /// Waits until the client has read the clock several times more, so
    /// that it has acted on the time it now shows, or briefly when it
    /// waits for something else and reads no clock.
    async fn settle(&self) {
        let start = self.reads.load(Ordering::SeqCst);
        let idle = Instant::now() + Duration::from_millis(200);
        while self.reads.load(Ordering::SeqCst) < start + 6 && Instant::now() < idle {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Waits until the client has read the clock several times more, with no
    /// escape: a client watching a stall reads it every millisecond.
    async fn read_again(&self) {
        let start = self.reads.load(Ordering::SeqCst);
        within("the client reads the clock again", || {
            self.reads.load(Ordering::SeqCst) >= start + 6
        })
        .await;
    }
}

/// The outcome of `sent`, which must come within ten seconds.
async fn finished(sent: tokio::task::JoinHandle<Outcome>) -> Outcome {
    tokio::time::timeout(Duration::from_secs(10), sent)
        .await
        .expect("the request ends")
        .unwrap()
}

async fn within(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// A request the stub read.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    /// The test clock's time when the request arrived.
    at: Duration,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(each, _)| each.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn query(&self, name: &str) -> Option<String> {
        let query = self.target.split_once('?')?.1;
        query.split('&').find_map(|pair| {
            let (each, value) = pair.split_once('=')?;
            (each == name).then(|| value.to_owned())
        })
    }
}

/// How the stub answers one request.
enum Reply {
    Answer {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
    },
    /// No answer while the client waits.
    Withhold,
    /// The head with the whole body's length, then `body`, then `more`'s
    /// bytes once it is notified, and nothing more.
    Stall {
        body: Vec<u8>,
        more: Option<(Arc<Notify>, Vec<u8>)>,
    },
}

fn json(status: u16, value: &Value) -> Reply {
    Reply::Answer {
        status,
        headers: vec![("Content-Type", "application/json".to_owned())],
        body: serde_json::to_vec(value).unwrap(),
    }
}

#[derive(Default)]
struct StubState {
    /// Requests read whole, answered or not.
    arrived: usize,
    clock: TestClock,
    /// Requests answered, each once its answer is written.
    seen: Vec<Seen>,
    replies: VecDeque<Reply>,
}

/// A local HTTP server that answers each request with the next reply.
struct Stub {
    url: String,
    state: Arc<Mutex<StubState>>,
    changed: Arc<Notify>,
}

impl Stub {
    async fn start(clock: TestClock) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(StubState {
            clock,
            ..StubState::default()
        }));
        let changed = Arc::new(Notify::new());
        let (serving, waking) = (state.clone(), changed.clone());
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve(socket, serving.clone(), waking.clone()));
            }
        });
        Stub {
            url,
            state,
            changed,
        }
    }

    fn reply(&self, reply: Reply) {
        self.state.lock().unwrap().replies.push_back(reply);
        self.changed.notify_waiters();
    }

    fn seen(&self) -> Vec<Seen> {
        self.state.lock().unwrap().seen.clone()
    }

    fn arrived(&self) -> usize {
        self.state.lock().unwrap().arrived
    }

    async fn wait_arrived(&self, count: usize) {
        within(&format!("{count} requests arrived"), || {
            self.state.lock().unwrap().arrived >= count
        })
        .await;
    }

    async fn wait_for(&self, count: usize) -> Vec<Seen> {
        within(&format!("the stub has {count} requests"), || {
            self.seen().len() >= count
        })
        .await;
        self.seen()
    }
}

async fn serve(socket: tokio::net::TcpStream, state: Arc<Mutex<StubState>>, changed: Arc<Notify>) {
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
        return;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            headers.push((name.trim().to_owned(), value.trim().to_owned()));
        }
    }
    let length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| value.parse().unwrap());
    let mut body = vec![0; length];
    if reader.read_exact(&mut body).await.is_err() {
        return;
    }
    let at = {
        let mut state = state.lock().unwrap();
        state.arrived += 1;
        state.clock.now()
    };
    let reply = loop {
        let waiting = changed.notified();
        if let Some(reply) = state.lock().unwrap().replies.pop_front() {
            break reply;
        }
        waiting.await;
    };
    let answered = || {
        state.lock().unwrap().seen.push(Seen {
            method,
            target,
            headers,
            at,
        });
    };
    let socket = reader.get_mut();
    match reply {
        Reply::Answer {
            status,
            headers,
            body,
        } => {
            let mut head = format!("HTTP/1.1 {status} Stub\r\n");
            for (name, value) in headers {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str(&format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            ));
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            answered();
        }
        Reply::Withhold => {
            answered();
            std::future::pending().await
        }
        Reply::Stall { body, more } => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                CONTENT.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            answered();
            if let Some((release, more)) = more {
                release.notified().await;
                let _ = socket.write_all(&more).await;
            }
            std::future::pending().await
        }
    }
}

/// The client under test with its stub, clock, binding and link.
struct Harness {
    stub: Stub,
    client: Arc<HttpDriveTransfer>,
    clock: TestClock,
    binding: watch::Sender<Option<Binding>>,
    events: mpsc::UnboundedReceiver<OutboxEvent>,
    link: DriveLink,
    /// The 429s an upload's `rate_limited` was told of.
    rate_limited: Arc<AtomicU64>,
}

impl Harness {
    async fn start() -> Harness {
        rejections();
        let clock = TestClock::default();
        clock.set(Duration::from_secs(1_000));
        let stub = Stub::start(clock.clone()).await;
        let (link, links) = DriveLink::new(key());
        link.connection_started(id("conn_a"), key());
        let (sender, events) = mpsc::unbounded_channel();
        let client = Arc::new(HttpDriveTransfer::new(
            &stub.url,
            links,
            sender,
            clock.clock(),
        ));
        let binding = watch::Sender::new(Some(Binding {
            connection_id: id("conn_a"),
            binding_id: id("bind_a"),
        }));
        Harness {
            stub,
            client,
            clock,
            binding,
            events,
            link,
            rate_limited: Arc::new(AtomicU64::new(0)),
        }
    }

    fn request<B>(&self, body: B) -> Request<B> {
        Request {
            drive_id: id("drive_a"),
            binding: self.binding.subscribe(),
            traceparent: None,
            body,
        }
    }

    /// Moves the clock by `step` and lets the client act on it.
    async fn advance(&self, step: Duration) {
        self.clock.set(self.clock.now() + step);
        self.clock.settle().await;
    }
}

/// A test sink of a download's bytes.
#[derive(Default)]
struct Bytes2(Vec<u8>, usize);

impl ContentSink for Bytes2 {
    fn received(&self) -> u64 {
        self.0.len() as u64
    }

    fn restart(&mut self) -> std::io::Result<()> {
        self.0.clear();
        self.1 += 1;
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

/// A sink whose bytes and restarts the test reads while the download runs.
#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Bytes2>>);

impl Shared {
    fn bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().0.clone()
    }

    fn restarts(&self) -> usize {
        self.0.lock().unwrap().1
    }
}

impl ContentSink for Shared {
    fn received(&self) -> u64 {
        self.0.lock().unwrap().received()
    }

    fn restart(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().restart()
    }

    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().append(bytes)
    }
}

/// One request of each endpoint and the fixture of a valid answer to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    ListFiles,
    ListChanges,
    Upload,
    Commit,
    Download,
}

impl Endpoint {
    fn named(name: &str) -> Endpoint {
        match name {
            "list_files" => Endpoint::ListFiles,
            "list_changes" => Endpoint::ListChanges,
            "upload_content" => Endpoint::Upload,
            "commit" => Endpoint::Commit,
            "download_content" => Endpoint::Download,
            other => panic!("no endpoint {other}"),
        }
    }

    fn of_schema(schema: &str) -> Endpoint {
        match schema {
            "ListFilesResponse" => Endpoint::ListFiles,
            "ListChangesResponse" => Endpoint::ListChanges,
            "UploadContentResponse" => Endpoint::Upload,
            "CommitResponse" => Endpoint::Commit,
            other => panic!("no response schema {other}"),
        }
    }

    /// The valid answer to [`Harness::send`] without a pair.
    fn valid_answer(self) -> Reply {
        match self {
            Endpoint::ListFiles => json(200, &fixture("list_files_first_page")["value"]),
            Endpoint::ListChanges => json(200, &fixture("list_changes_page")["value"]),
            Endpoint::Upload => json(200, &fixture("upload_response")["value"]),
            Endpoint::Commit => json(200, &fixture("commit_response")["value"]),
            Endpoint::Download => Reply::Answer {
                status: 200,
                headers: Vec::new(),
                body: CONTENT.to_vec(),
            },
        }
    }
}

/// What a request ended with, for comparing.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Rebind,
    Accepted,
    ReRead,
    CursorExpired,
    Decision(&'static str),
    Permanent(u16, Option<&'static str>),
}

fn outcome<T>(result: Result<T, Ended>) -> Outcome {
    match result {
        Ok(_) => Outcome::Accepted,
        Err(Ended::ReRead) => Outcome::ReRead,
        Err(Ended::Rebind(_)) => Outcome::Rebind,
        Err(Ended::CursorExpired) => Outcome::CursorExpired,
        Err(Ended::Decision(error)) => Outcome::Decision(error.code.as_str()),
        Err(Ended::Permanent { status, code }) => Outcome::Permanent(status, Some(code)),
        Err(Ended::Invalid { status, .. }) => Outcome::Permanent(status, None),
        Err(Ended::Sink(error)) => panic!("the sink failed: {error}"),
        Err(Ended::Unreadable(error)) => panic!("the upload's file failed: {error}"),
    }
}

impl Harness {
    /// Sends `endpoint`'s request paired with `pair` (the request fixture
    /// that a response fixture names), or its default one.
    fn send(&self, endpoint: Endpoint, pair: &Value) -> tokio::task::JoinHandle<Outcome> {
        let client = self.client.clone();
        match endpoint {
            Endpoint::ListFiles => {
                let after_path = pair
                    .get("query")
                    .and_then(|name| {
                        fixture(name.as_str().unwrap())["value"]["after_path"].as_str()
                    })
                    .map(|path| DrivePath::new(path).unwrap());
                let request = self.request(ListFiles { after_path });
                tokio::spawn(async move { outcome(client.list_files(request).await) })
            }
            Endpoint::ListChanges => {
                let query = &fixture(
                    pair.get("query")
                        .and_then(Value::as_str)
                        .unwrap_or("list_changes_query"),
                )["value"];
                let number = |value: &Value| value.as_str().map(|text| text.parse().unwrap());
                let request = self.request(ListChanges {
                    after_seq: number(&query["after_seq"]).unwrap(),
                    after_index: number(&query["after_index"]),
                });
                tokio::spawn(async move { outcome(client.list_changes(request).await) })
            }
            Endpoint::Upload => {
                let (path, headers) = match pair.get("path_params") {
                    Some(path) => (
                        fixture(path.as_str().unwrap()),
                        fixture(pair["request_headers"].as_str().unwrap()),
                    ),
                    None => (fixture("content_path_params"), fixture("upload_headers")),
                };
                let length: usize = headers["value"]["Content-Length"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                let counted = self.rate_limited.clone();
                let request = self.request(Upload {
                    sha256: Sha256::new(path["value"]["sha256"].as_str().unwrap()).unwrap(),
                    content: Content::Bytes(Bytes::from(vec![b'x'; length])),
                    rate_limited: Some(Arc::new(move || {
                        counted.fetch_add(1, Ordering::SeqCst);
                    })),
                });
                tokio::spawn(async move { outcome(client.upload_content(request).await) })
            }
            Endpoint::Commit => {
                let request = fixture(
                    pair.get("request")
                        .and_then(Value::as_str)
                        .unwrap_or("commit_request"),
                );
                let changes: Vec<CommitChange> =
                    serde_json::from_value(request["value"]["changes"].clone()).unwrap();
                let request = self.request(Commit { changes });
                tokio::spawn(async move { outcome(client.commit(request).await) })
            }
            Endpoint::Download => {
                let request = self.request(Download {
                    sha256: Sha256::new("a".repeat(64)).unwrap(),
                    size: CONTENT.len() as u64,
                });
                tokio::spawn(async move {
                    let mut sink = Bytes2::default();
                    let result = client.download_content(request, &mut sink).await;
                    outcome(result)
                })
            }
        }
    }
}

#[tokio::test]
async fn every_fixture_answer_is_accepted_refused_as_permanent_or_lands_in_its_class() {
    let harness = Harness::start().await;
    let responses = fixtures().iter().filter(|fixture| {
        matches!(
            fixture["schema"].as_str(),
            Some(
                "ListFilesResponse"
                    | "ListChangesResponse"
                    | "UploadContentResponse"
                    | "CommitResponse"
            )
        )
    });
    let mut rows = 0;
    for response in responses {
        let endpoint = Endpoint::of_schema(response["schema"].as_str().unwrap());
        let pair = response.get("pair").cloned().unwrap_or(Value::Null);
        let logged = rejections().lock().unwrap().len();
        harness.stub.reply(json(200, &response["value"]));
        let got = harness.send(endpoint, &pair).await.unwrap();
        let name = response["name"].as_str().unwrap();
        if response["valid"] == true {
            assert_eq!(got, Outcome::Accepted, "{name}");
            assert_eq!(rejections().lock().unwrap().len(), logged, "{name}");
        } else {
            assert_eq!(got, Outcome::Permanent(200, None), "{name}");
            assert_eq!(
                rejections().lock().unwrap()[logged..],
                if matches!(endpoint, Endpoint::Upload | Endpoint::Commit) {
                    vec![]
                } else {
                    vec!["invalid_response with error.message"]
                },
                "{name}"
            );
        }
        rows += 1;
    }
    assert!(rows > 0);

    // Each error fixture on its endpoint and status, in its class.
    for error in fixtures()
        .iter()
        .filter(|fixture| fixture["schema"] == "Error")
    {
        let name = error["name"].as_str().unwrap();
        let endpoint = Endpoint::named(error["endpoint"].as_str().unwrap());
        let status = u16::try_from(error["status"].as_u64().unwrap()).unwrap();
        let code = error["value"]["code"].as_str().unwrap();
        let seen = harness.stub.seen().len();
        let logged = rejections().lock().unwrap().len();
        let mut reply = json(status, &error["value"]);
        if code == "rate_limited"
            && let Reply::Answer { headers, .. } = &mut reply
        {
            headers.push(("Retry-After", "1".to_owned()));
        }
        harness.stub.reply(reply);
        let sent = harness.send(endpoint, &Value::Null);
        let expected = match (error["valid"] == true, code) {
            (false, _) => Outcome::Permanent(status, None),
            (true, "content_missing" | "file_too_large") => {
                Outcome::Decision(if code == "content_missing" {
                    "content_missing"
                } else {
                    "file_too_large"
                })
            }
            (true, "content_hash_mismatch") => Outcome::ReRead,
            (true, "drive_cursor_expired") => Outcome::CursorExpired,
            (true, "stale_binding")
                if matches!(endpoint, Endpoint::Upload | Endpoint::Download) =>
            {
                Outcome::Rebind
            }
            (true, "stale_binding") => {
                harness.stub.wait_for(seen + 1).await;
                harness.advance(Duration::from_secs(60)).await;
                assert_eq!(harness.stub.arrived(), seen + 1, "{name}");
                harness.stub.reply(endpoint.valid_answer());
                let next = id(&format!("bind_{seen}"));
                harness.binding.send_replace(Some(Binding {
                    connection_id: id("conn_a"),
                    binding_id: next.clone(),
                }));
                let again = harness.stub.wait_for(seen + 2).await;
                assert_eq!(
                    again[seen + 1].query("binding_id").as_deref(),
                    Some(next.as_str()),
                    "{name}"
                );
                Outcome::Accepted
            }
            (true, "rate_limited") => {
                let told = harness.rate_limited.load(Ordering::SeqCst);
                within("the 429 handled", || {
                    harness.rate_limited.load(Ordering::SeqCst) > told
                })
                .await;
                harness.stub.reply(endpoint.valid_answer());
                harness.advance(Duration::from_secs(1)).await;
                Outcome::Accepted
            }
            (true, "drive_access_denied") => {
                Outcome::Permanent(status, Some("drive_access_denied"))
            }
            (true, "content_not_found") => Outcome::Permanent(status, Some("content_not_found")),
            (true, "range_not_satisfiable") => {
                Outcome::Permanent(status, Some("range_not_satisfiable"))
            }
            (true, "request_too_large") => Outcome::Permanent(status, Some("request_too_large")),
            (true, other) => panic!("no class for {other}"),
        };
        assert_eq!(finished(sent).await, expected, "{name}");
        let logs = rejections().lock().unwrap()[logged..].to_vec();
        let expected_logs: Vec<String> = match &expected {
            _ if matches!(endpoint, Endpoint::Upload | Endpoint::Commit) => Vec::new(),
            Outcome::Permanent(_, None) => vec!["invalid_response with error.message".to_owned()],
            Outcome::Permanent(_, Some("drive_access_denied")) => Vec::new(),
            Outcome::Permanent(_, Some(code)) => vec![(*code).to_owned()],
            _ => Vec::new(),
        };
        assert_eq!(logs, expected_logs, "{name}");
    }
    for endpoint in [
        Endpoint::ListFiles,
        Endpoint::ListChanges,
        Endpoint::Upload,
        Endpoint::Commit,
        Endpoint::Download,
    ] {
        for status in [301, 302, 307, 308, 501, 505] {
            let before = harness.stub.arrived();
            harness.stub.reply(Reply::Answer {
                status,
                headers: vec![("Location", "/unexpected".to_owned())],
                body: b"{}".to_vec(),
            });
            assert_eq!(
                finished(harness.send(endpoint, &Value::Null)).await,
                Outcome::Permanent(status, None),
                "{endpoint:?} {status}"
            );
            assert_eq!(harness.stub.arrived(), before + 1);
        }
    }
}

#[test]
fn the_total_of_a_put_or_a_commit_scales_with_every_started_65536_bytes() {
    for (bytes, seconds) in [
        (0, 10),
        (65_536, 10),
        (65_537, 20),
        (16_777_216, 2_560),
        (500_000_000, 76_300),
    ] {
        assert_eq!(total(bytes), Duration::from_secs(seconds), "{bytes} bytes");
    }
}

#[tokio::test]
async fn withheld_headers_fail_the_attempt_after_ten_seconds_and_it_is_sent_again() {
    let harness = Harness::start().await;
    harness.stub.reply(Reply::Withhold);
    let sent = harness.send(Endpoint::ListFiles, &Value::Null);
    harness.stub.wait_for(1).await;
    // The client sees its request written before the clock moves.
    harness.clock.settle().await;
    harness
        .advance(Duration::from_secs(10) - Duration::from_millis(1))
        .await;
    harness.stub.reply(Endpoint::ListFiles.valid_answer());
    harness.advance(Duration::from_millis(1)).await;
    // Not sent again before the first temporary wait of 1 second.
    assert_eq!(harness.stub.arrived(), 1);
    // The first temporary failure waits 1 second and at most 20% more.
    harness.advance(Duration::from_millis(1_200)).await;
    harness.stub.wait_for(2).await;
    assert_eq!(sent.await.unwrap(), Outcome::Accepted);
}

impl Harness {
    /// Starts a download of [`CONTENT`] into a shared sink whose first
    /// answer stalls after five bytes, or after `more` once notified, and
    /// waits until the client has the five bytes and has looked at its clock
    /// since.
    async fn stalled_download(
        &self,
        more: Option<(Arc<Notify>, Vec<u8>)>,
    ) -> (Shared, tokio::task::JoinHandle<Outcome>) {
        self.stub.reply(Reply::Stall {
            body: CONTENT[..5].to_vec(),
            more,
        });
        let client = self.client.clone();
        let request = self.request(Download {
            sha256: Sha256::new("a".repeat(64)).unwrap(),
            size: CONTENT.len() as u64,
        });
        let sink = Shared::default();
        let mut writing = sink.clone();
        let sent =
            tokio::spawn(
                async move { outcome(client.download_content(request, &mut writing).await) },
            );
        within("five bytes received", || sink.bytes().len() == 5).await;
        self.clock.read_again().await;
        (sink, sent)
    }
}

#[tokio::test]
async fn a_stalled_download_fails_ten_seconds_after_its_last_byte_and_resumes_with_a_range() {
    let harness = Harness::start().await;
    let release = Arc::new(Notify::new());
    let (sink, sent) = harness
        .stalled_download(Some((release.clone(), CONTENT[5..6].to_vec())))
        .await;
    let start = harness.clock.now();
    // A byte received at 9 seconds moves the deadline past 10.
    harness.advance(Duration::from_secs(9)).await;
    release.notify_one();
    within("the sixth byte received", || sink.bytes().len() == 6).await;
    harness.clock.settle().await;
    harness.advance(Duration::from_secs(1)).await;
    assert_eq!(harness.stub.arrived(), 1);
    harness
        .advance(Duration::from_secs(9) - Duration::from_millis(1))
        .await;
    harness.advance(Duration::from_millis(1)).await;
    harness.stub.reply(Reply::Answer {
        status: 206,
        headers: vec![("Content-Range", format!("bytes 6-11/{}", CONTENT.len()))],
        body: CONTENT[6..].to_vec(),
    });
    // Not sent again before the first temporary wait of 1 second.
    assert_eq!(harness.stub.arrived(), 1);
    harness.advance(Duration::from_millis(1_200)).await;
    let seen = harness.stub.wait_for(2).await;
    assert_eq!(seen[1].header("range"), Some("bytes=6-"));
    // Sent again after the stall at 19 seconds and its wait of 1 second.
    assert!(
        seen[1].at >= start + Duration::from_secs(20),
        "{:?}",
        seen[1].at - start
    );
    assert_eq!(sent.await.unwrap(), Outcome::Accepted);
    assert_eq!(sink.bytes(), CONTENT);
    assert_eq!(sink.restarts(), 1);
}

#[tokio::test]
async fn a_resumed_download_answered_200_restarts_at_byte_zero() {
    let harness = Harness::start().await;
    let (sink, sent) = harness.stalled_download(None).await;
    harness.advance(Duration::from_secs(10)).await;
    harness.stub.reply(Endpoint::Download.valid_answer());
    harness.advance(Duration::from_millis(1_200)).await;
    let seen = harness.stub.wait_for(2).await;
    assert_eq!(seen[1].header("range"), Some("bytes=5-"));
    assert_eq!(sent.await.unwrap(), Outcome::Accepted);
    assert_eq!(sink.bytes(), CONTENT);
    // Once at the first answer and once at the 200 that restarted it.
    assert_eq!(sink.restarts(), 2);

    // A local write that fails ends the download as the sink's failure.
    struct Full;
    impl ContentSink for Full {
        fn received(&self) -> u64 {
            0
        }
        fn restart(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn append(&mut self, _: &[u8]) -> std::io::Result<()> {
            Err(std::io::ErrorKind::StorageFull.into())
        }
    }
    harness.stub.reply(Endpoint::Download.valid_answer());
    let request = harness.request(Download {
        sha256: Sha256::new("a".repeat(64)).unwrap(),
        size: CONTENT.len() as u64,
    });
    let ended = harness.client.download_content(request, &mut Full).await;
    assert!(
        matches!(&ended, Err(Ended::Sink(error)) if error.kind() == std::io::ErrorKind::StorageFull),
        "{:?}",
        ended.err().map(|ended| format!("{ended:?}"))
    );
}

#[tokio::test]
async fn a_429_waits_its_retry_after_and_503s_wait_one_to_thirty_seconds_with_jitter() {
    let harness = Harness::start().await;
    let mut reply = json(429, &fixture("error_rate_limited")["value"]);
    if let Reply::Answer { headers, .. } = &mut reply {
        headers.push(("Retry-After", "3".to_owned()));
    }
    harness.stub.reply(reply);
    let counted = Arc::new(AtomicU64::new(0));
    let counting = counted.clone();
    let client = harness.client.clone();
    let request = harness.request(Upload {
        sha256: Sha256::new(
            fixture("content_path_params")["value"]["sha256"]
                .as_str()
                .unwrap(),
        )
        .unwrap(),
        content: Content::Bytes(Bytes::from(vec![b'x'; 12])),
        rate_limited: Some(Arc::new(move || {
            counting.fetch_add(1, Ordering::SeqCst);
        })),
    });
    let sent = tokio::spawn(async move { outcome(client.upload_content(request).await) });
    within("the 429 handled", || counted.load(Ordering::SeqCst) == 1).await;
    let mut waits = vec![Duration::from_secs(3)];
    waits.extend([1, 2, 4, 8, 16, 30, 30].map(Duration::from_secs));
    for (index, wait) in waits.iter().enumerate() {
        let seen = index + 1;
        let last = seen == waits.len();
        harness.stub.reply(if last {
            Endpoint::Upload.valid_answer()
        } else {
            Reply::Answer {
                status: 503,
                headers: Vec::new(),
                body: Vec::new(),
            }
        });
        let start = harness.clock.now();
        harness.advance(*wait - Duration::from_millis(1)).await;
        assert_eq!(harness.stub.arrived(), seen, "before the wait of {wait:?}");
        let longest = if index == 0 { *wait } else { wait.mul_f64(1.2) };
        harness.clock.set(start + longest);
        let attempts = harness.stub.wait_for(seen + 1).await;
        assert!(
            attempts[seen].at >= start + *wait,
            "attempt {seen} started at {:?}, before {:?}",
            attempts[seen].at,
            start + *wait
        );
        harness.clock.settle().await;
    }
    assert_eq!(finished(sent).await, Outcome::Accepted);
    assert_eq!(counted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn drive_access_denied_is_not_sent_again_stale_connection_tells_the_connection_owner_and_a_dropped_binding_ends_a_request()
 {
    let mut harness = Harness::start().await;
    harness
        .stub
        .reply(json(403, &fixture("error_drive_access_denied")["value"]));
    let sent = harness.send(Endpoint::Commit, &Value::Null);
    assert_eq!(
        sent.await.unwrap(),
        Outcome::Permanent(403, Some("drive_access_denied"))
    );
    harness.advance(Duration::from_secs(60)).await;
    assert_eq!(harness.stub.arrived(), 1);

    harness.stub.reply(json(
        409,
        &serde_json::json!({"code": "stale_connection", "message": "not live"}),
    ));
    let sent = harness.send(Endpoint::ListFiles, &Value::Null);
    harness.stub.wait_for(2).await;
    let event = harness.events.recv().await.unwrap();
    assert!(
        matches!(&event, OutboxEvent::Refused { connection, error }
            if connection.as_str() == "conn_a" && error.code.as_str() == "stale_connection"),
        "{event:?}"
    );
    harness.stub.reply(Endpoint::ListFiles.valid_answer());
    harness.binding.send_replace(Some(Binding {
        connection_id: id("conn_a"),
        binding_id: id("bind_b"),
    }));
    let seen = harness.stub.wait_for(3).await;
    assert_eq!(seen[2].query("binding_id").as_deref(), Some("bind_b"));
    assert_eq!(seen[2].method, "GET");
    assert_eq!(seen[2].header("authorization"), Some("Bearer tetral_ck_a"));
    assert_eq!(sent.await.unwrap(), Outcome::Accepted);

    // A content transfer returns the rebind to its capacity owner.
    harness.stub.reply(json(
        409,
        &serde_json::json!({"code": "stale_binding", "message": "replaced"}),
    ));
    let sent = harness.send(Endpoint::Download, &Value::Null);
    harness.stub.wait_for(4).await;
    drop(std::mem::replace(
        &mut harness.binding,
        watch::Sender::new(None),
    ));
    let ended = tokio::time::timeout(Duration::from_secs(10), sent).await;
    assert_eq!(ended.expect("the request ends").unwrap(), Outcome::Rebind);
    assert_eq!(harness.stub.arrived(), 4);
}

#[tokio::test]
async fn connection_rule_answers_wait_for_a_new_binding_and_a_revoked_old_key_goes_again_with_the_new_one()
 {
    let mut harness = Harness::start().await;
    let rebind = |harness: &Harness, connection: &str, binding: &str| {
        harness.binding.send_replace(Some(Binding {
            connection_id: id(connection),
            binding_id: id(binding),
        }));
    };

    // Any 401 and a 403 instance_unavailable: the Connection owner is told
    // and the request waits for a new binding.
    for (status, code) in [(401, "credential_invalid"), (403, "instance_unavailable")] {
        let seen = harness.stub.seen().len();
        harness.stub.reply(json(
            status,
            &serde_json::json!({"code": code, "message": "refused"}),
        ));
        let sent = harness.send(Endpoint::ListFiles, &Value::Null);
        let event = tokio::time::timeout(Duration::from_secs(10), harness.events.recv())
            .await
            .expect("the Connection owner is told")
            .unwrap();
        assert!(
            matches!(&event, OutboxEvent::Refused { connection, error }
                if connection.as_str() == "conn_a" && error.code.as_str() == code),
            "{event:?}"
        );
        harness.stub.reply(Endpoint::ListFiles.valid_answer());
        let next = format!("bind_{status}");
        rebind(&harness, "conn_a", &next);
        let again = harness.stub.wait_for(seen + 2).await;
        assert_eq!(
            again[seen + 1].query("binding_id").as_deref(),
            Some(next.as_str()),
            "{code}"
        );
        assert_eq!(finished(sent).await, Outcome::Accepted, "{code}");
    }

    // A 401 credential_revoked to the old key during a switch: nothing is
    // refused and the request goes again with the switch's key.
    let seen = harness.stub.seen().len();
    let sent = harness.send(Endpoint::ListFiles, &Value::Null);
    harness.stub.wait_arrived(seen + 1).await;
    harness.link.switch_started(&id("conn_a"));
    harness.stub.reply(json(
        401,
        &serde_json::json!({"code": "credential_revoked", "message": "revoked"}),
    ));
    harness.stub.wait_for(seen + 1).await;
    harness.stub.reply(Endpoint::ListFiles.valid_answer());
    harness.link.switch_ended(&id("conn_a"), renewed_key());
    let again = harness.stub.wait_for(seen + 2).await;
    assert_eq!(
        again[seen + 1].header("authorization"),
        Some("Bearer tetral_ck_b")
    );
    assert_eq!(finished(sent).await, Outcome::Accepted);
    assert!(harness.events.try_recv().is_err());

    // The connection's end drops an attempt in flight; it goes again with
    // the next connection's binding.
    let seen = harness.stub.seen().len();
    let sent = harness.send(Endpoint::ListFiles, &Value::Null);
    harness.stub.wait_arrived(seen + 1).await;
    harness.link.connection_ended(&id("conn_a"));
    // The dropped attempt's request takes this reply.
    harness.stub.reply(Reply::Withhold);
    harness.stub.wait_for(seen + 1).await;
    harness.link.connection_started(id("conn_b"), renewed_key());
    harness.stub.reply(Endpoint::ListFiles.valid_answer());
    rebind(&harness, "conn_b", "bind_b");
    let again = harness.stub.wait_for(seen + 2).await;
    assert_eq!(
        again[seen + 1].query("binding_id").as_deref(),
        Some("bind_b")
    );
    assert_eq!(finished(sent).await, Outcome::Accepted);
}

const CONTENT_HASH: &str = "97b18506ce751ce9c6f30c3640336ef7847e02090f7d6e34589cf4f956c31ef8";
const OTHER_HASH: &str = "d9298a10d1b0735837dc4bd85dac641b0f3cef27a47e5d53a54f2f3f5b2fcffa";

fn content_needed(
    session: &str,
    hash: &str,
    size: usize,
    binding: &watch::Sender<Option<Binding>>,
) -> drive::cache::Needed {
    drive::cache::Needed {
        session_id: protocol::id::SafeId::new(session).unwrap(),
        drive_id: id("drive_a"),
        sha256: Sha256::new(hash).unwrap(),
        size: size as u64,
        binding: binding.subscribe(),
    }
}

fn shared_cache(harness: &Harness, root: &std::path::Path) -> drive::cache::ContentCache {
    drive::cache::ContentCache::spawn(drive::cache::CacheConfig {
        root: root.to_owned(),
        transfer: harness.client.clone(),
        clock: harness.clock.clock(),
        free_space: Arc::new(|| Ok(u64::MAX)),
    })
    .unwrap()
}

/// Also waits for the client to see the bytes as progress: it samples
/// progress when it reads the clock, so a clock moved before that measures
/// the stall from the moved time.
async fn received_prefix(
    harness: &Harness,
    cache: &drive::cache::ContentCache,
    session: &str,
    count: usize,
) {
    let session = protocol::id::SafeId::new(session).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let progress = cache.downloading(&session).await;
            if progress.len() == count && progress.iter().all(|(_, bytes)| *bytes == 5) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all partial response bodies reached the cache");
    harness.clock.read_again().await;
}

#[tokio::test]
async fn an_unbound_session_releases_downloads_and_resumes_only_on_new_demand() {
    use std::io::Read;
    for end_link in [false, true] {
        let harness = Harness::start().await;
        let root = tempfile::tempdir().unwrap();
        let cache = shared_cache(&harness, root.path());
        let mut reads = Vec::new();
        // One Session fills all eight slots, with a ninth read queued.
        for index in 0..8 {
            harness.stub.reply(Reply::Stall {
                body: CONTENT[..5].to_vec(),
                more: None,
            });
            let hash = if index == 0 {
                CONTENT_HASH.to_owned()
            } else {
                format!("{index:064x}")
            };
            reads.push(cache.read(content_needed(
                "sess_a",
                &hash,
                CONTENT.len(),
                &harness.binding,
            )));
            harness.stub.wait_for(index + 1).await;
        }
        received_prefix(&harness, &cache, "sess_a", 8).await;
        reads.push(cache.read(content_needed(
            "sess_a",
            &format!("{:064x}", 9),
            CONTENT.len(),
            &harness.binding,
        )));
        assert_eq!(
            cache
                .downloading(&protocol::id::SafeId::new("sess_a").unwrap())
                .await
                .len(),
            9
        );
        assert_eq!(harness.stub.arrived(), 8);
        if end_link {
            harness.link.connection_ended(&id("conn_a"));
        }
        harness.binding.send_replace(None);
        for read in reads {
            let error = tokio::time::timeout(Duration::from_secs(5), read)
                .await
                .expect("active and queued filesystem requests end")
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(5));
        }
        let new_connection = if end_link { "conn_b" } else { "conn_a" };
        harness.link.connection_started(id(new_connection), key());
        let other = watch::Sender::new(Some(Binding {
            connection_id: id(new_connection),
            binding_id: id("bind_other"),
        }));
        harness.stub.reply(Reply::Answer {
            status: 200,
            headers: vec![],
            body: b"other".to_vec(),
        });
        let mut file = tokio::time::timeout(
            Duration::from_secs(5),
            cache.read(content_needed("sess_b", OTHER_HASH, 5, &other)),
        )
        .await
        .expect("the usable Session acquires released capacity")
        .unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"other");
        assert_eq!(harness.stub.arrived(), 9);
        assert_eq!(
            std::fs::read_dir(root.path().join("cache/tmp"))
                .unwrap()
                .count(),
            8
        );
        // Cached content is still readable with no binding; missing content fails.
        cache
            .read(content_needed("sess_a", OTHER_HASH, 5, &harness.binding))
            .await
            .unwrap();
        assert_eq!(
            cache
                .read(content_needed(
                    "sess_a",
                    CONTENT_HASH,
                    CONTENT.len(),
                    &harness.binding
                ))
                .await
                .unwrap_err()
                .raw_os_error(),
            Some(5)
        );
        harness.binding.send_replace(Some(Binding {
            connection_id: id(new_connection),
            binding_id: id("bind_new"),
        }));
        harness.clock.settle().await;
        assert_eq!(
            harness.stub.arrived(),
            9,
            "rebinding alone must not resume failed reads"
        );
        harness.stub.reply(Reply::Answer {
            status: 206,
            headers: vec![("Content-Range", "bytes 5-11/12".to_owned())],
            body: CONTENT[5..].to_vec(),
        });
        let mut file = cache
            .read(content_needed(
                "sess_a",
                CONTENT_HASH,
                CONTENT.len(),
                &harness.binding,
            ))
            .await
            .unwrap();
        bytes.clear();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(
            bytes, CONTENT,
            "retained prefix and resumed suffix hash as one file"
        );
        let seen = harness.stub.wait_for(10).await;
        assert_eq!(seen[9].header("range"), Some("bytes=5-"));
        assert_eq!(seen[9].query("binding_id").as_deref(), Some("bind_new"));
        // Local Session removal cancels active content, as well as retained bytes.
        harness.stub.reply(Reply::Withhold);
        let closing = cache.read(content_needed(
            "sess_a",
            &format!("{:064x}", 1),
            CONTENT.len(),
            &harness.binding,
        ));
        harness.stub.wait_for(11).await;
        cache.forget_session(protocol::id::SafeId::new("sess_a").unwrap());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), closing)
                .await
                .unwrap()
                .unwrap_err()
                .raw_os_error(),
            Some(5)
        );
        within("closed Session temporary files removed", || {
            std::fs::read_dir(root.path().join("cache/tmp"))
                .unwrap()
                .count()
                == 0
        })
        .await;
        cache
            .downloading(&protocol::id::SafeId::new("sess_a").unwrap())
            .await;
        assert_eq!(
            std::fs::read_dir(root.path().join("cache/tmp"))
                .unwrap()
                .count(),
            0
        );
    }
}

#[tokio::test]
async fn a_stale_binding_suspends_partial_content_without_retrying_the_same_binding() {
    use std::io::Read;
    let harness = Harness::start().await;
    let root = tempfile::tempdir().unwrap();
    let cache = shared_cache(&harness, root.path());
    harness.stub.reply(Reply::Stall {
        body: CONTENT[..5].to_vec(),
        more: None,
    });
    let read = cache.read(content_needed(
        "sess_a",
        CONTENT_HASH,
        CONTENT.len(),
        &harness.binding,
    ));
    received_prefix(&harness, &cache, "sess_a", 1).await;
    harness.advance(Duration::from_secs(10)).await;
    harness.stub.reply(json(
        409,
        &serde_json::json!({"code":"stale_binding", "message":"replaced"}),
    ));
    harness.advance(Duration::from_millis(1200)).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .unwrap()
            .unwrap_err()
            .raw_os_error(),
        Some(5)
    );
    harness.stub.wait_for(2).await;
    // More filesystem callers do not create a stale-binding retry loop.
    for _ in 0..3 {
        cache
            .read(content_needed(
                "sess_a",
                CONTENT_HASH,
                CONTENT.len(),
                &harness.binding,
            ))
            .await
            .unwrap_err();
    }
    assert_eq!(harness.stub.arrived(), 2);
    harness.binding.send_replace(Some(Binding {
        connection_id: id("conn_a"),
        binding_id: id("bind_new"),
    }));
    harness.stub.reply(Reply::Answer {
        status: 206,
        headers: vec![("Content-Range", "bytes 5-11/12".to_owned())],
        body: CONTENT[5..].to_vec(),
    });
    let mut file = cache
        .read(content_needed(
            "sess_a",
            CONTENT_HASH,
            CONTENT.len(),
            &harness.binding,
        ))
        .await
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, CONTENT);
    assert_eq!(
        harness.stub.wait_for(3).await[2].header("range"),
        Some("bytes=5-")
    );
}

#[tokio::test]
async fn content_transfers_release_rebind_waits_during_retry_after_and_active_http() {
    let harness = Harness::start().await;
    for endpoint in [Endpoint::Upload, Endpoint::Download] {
        let seen = harness.stub.arrived();
        harness.stub.reply(Reply::Withhold);
        let sent = harness.send(endpoint, &Value::Null);
        harness.stub.wait_for(seen + 1).await;
        harness.link.connection_ended(&id("conn_a"));
        assert_eq!(finished(sent).await, Outcome::Rebind);
        harness.link.connection_started(id("conn_a"), key());
    }
    let mut reply = json(429, &fixture("error_rate_limited")["value"]);
    if let Reply::Answer { headers, .. } = &mut reply {
        headers.push(("Retry-After", "60".to_owned()));
    }
    harness.stub.reply(reply);
    let sent = harness.send(Endpoint::Upload, &Value::Null);
    within("the upload entered its retry wait", || {
        harness.rate_limited.load(Ordering::SeqCst) == 1
    })
    .await;
    harness.link.connection_ended(&id("conn_a"));
    assert_eq!(
        finished(sent).await,
        Outcome::Rebind,
        "connection loss does not wait out Retry-After"
    );
}

struct TransferOwners;
impl drive::mount::Registry for TransferOwners {
    fn add(&self, _: protocol::id::SafeId, _: drive::DriveSync) {}
    fn remove(&self, _: protocol::id::SafeId) {}
    fn mounted(&self, _: Id, _: protocol::id::SafeId) {}
    fn detached(&self, _: Id, _: protocol::id::SafeId) {}
}
impl protocol::ports::TaskOwnership for TransferOwners {
    fn task_of(&self, _: u32, reply: protocol::ports::TaskOwnerReply) {
        reply(None);
    }
}
impl protocol::ports::ReportSink for TransferOwners {
    fn submit(&self, _: protocol::ports::Submission) -> protocol::ports::ReportId {
        panic!("no reserved report in a content transfer")
    }
    fn try_submit(
        &self,
        _: protocol::ports::Submission,
    ) -> Result<protocol::ports::ReportId, protocol::ports::ReportAdmissionError> {
        Err(protocol::ports::ReportAdmissionError::ResourceExhausted)
    }
    fn cancel(&self, _: protocol::ports::ReportId) {}
    fn add_final(&self, _: protocol::ports::ReportId, _: Id) {}
}

async fn pending_sync(
    harness: &Harness,
    cache: &drive::cache::ContentCache,
    root: &std::path::Path,
    session: &str,
    count: usize,
) -> drive::DriveSync {
    use drive::state::{FileRow, PendingChange, SyncState, Write};
    let directory = root.join("sessions").join(session);
    let content = directory.join("content/drive_a");
    std::fs::create_dir_all(&content).unwrap();
    let mut state = SyncState::create(&directory.join("sync-state.db"))
        .await
        .unwrap();
    let mut seed = Write::default();
    for index in 0..count {
        let file_id = index as i64 + 3;
        let path = format!("file_{index}");
        std::fs::write(content.join(file_id.to_string()), CONTENT).unwrap();
        seed.files.push(FileRow {
            drive_id: "drive_a".to_owned(),
            path: path.clone(),
            file_id,
            kind: "file".to_owned(),
            present: true,
            version_id: None,
            synced_sha256: None,
            synced_size: None,
            synced_executable: None,
            content: Some("session".to_owned()),
            local_sha256: None,
            size: Some(CONTENT.len() as i64),
            executable: Some(false),
            mtime_ns: Some(0),
            ctime_ns: Some(0),
            origin: None,
            link_target: None,
            local_only: None,
        });
        seed.pending.push(PendingChange {
            drive_id: "drive_a".to_owned(),
            path,
            change: "write".to_owned(),
            base_sha256: None,
            content_sha256: None,
            executable: Some(false),
            uploaded: false,
            last_write_at: 0,
            task_id: None,
            fault_code: None,
        });
    }
    state.apply(seed).await.unwrap();
    drive::DriveSync::spawn(drive::SyncConfig {
        commit_batch_size: drive::commit::MAX_CHANGES,
        session_id: protocol::id::SafeId::new(session).unwrap(),
        root: root.to_owned(),
        state,
        transfer: harness.client.clone(),
        cache: cache.clone(),
        tasks: Arc::new(TransferOwners),
        mounting: drive::mount::Mounting::Unavailable("not mounted by transfer test".to_owned()),
        registry: Arc::new(TransferOwners),
        clock: harness.clock.clock(),
        reports: Arc::new(TransferOwners),
    })
}

#[tokio::test]
async fn upload_owners_release_running_and_queued_work_on_binding_loss_and_requeue_after_rebind() {
    for reason in ["binding_removed", "stale_binding", "connection_ended"] {
        let harness = Harness::start().await;
        let root = tempfile::tempdir().unwrap();
        let cache = shared_cache(&harness, root.path());
        let a = pending_sync(&harness, &cache, root.path(), "sess_a", 9).await;
        let b = pending_sync(&harness, &cache, root.path(), "sess_b", 1).await;
        // None of the durable pending writes can request capacity while unbound.
        a.task_exited(&id("task_a"));
        a.tables().await;
        assert_eq!(harness.stub.arrived(), 0);
        for _ in 0..7 {
            harness.stub.reply(Reply::Withhold);
        }
        a.binding_current(Binding {
            connection_id: id("conn_a"),
            binding_id: id("bind_a"),
        });
        harness.stub.wait_for(7).await;
        harness.stub.wait_arrived(8).await;
        b.binding_current(Binding {
            connection_id: id("conn_a"),
            binding_id: id("bind_b"),
        });
        b.tables().await;
        assert_eq!(
            harness.stub.arrived(),
            8,
            "eight actual HTTP attempts hold capacity"
        );
        if reason == "stale_binding" {
            harness.stub.reply(json(
                409,
                &serde_json::json!({"code":"stale_binding", "message":"replaced"}),
            ));
        } else {
            harness.stub.reply(Reply::Withhold);
            harness.stub.wait_for(8).await;
            if reason == "binding_removed" {
                a.binding_gone();
            } else {
                harness.link.connection_ended(&id("conn_a"));
                // Let the old operations finish before the new connection is installed.
                harness.clock.settle().await;
                harness.link.connection_started(id("conn_b"), key());
                b.binding_current(Binding {
                    connection_id: id("conn_b"),
                    binding_id: id("bind_b_new"),
                });
            }
        }
        // The already queued ninth A upload must not steal the released capacity.
        harness.stub.reply(json(
            200,
            &serde_json::json!({"content_sha256":CONTENT_HASH,"size_bytes":CONTENT.len()}),
        ));
        let seen = harness.stub.wait_for(9).await;
        assert!(
            seen[8].query("binding_id").unwrap().starts_with("bind_b"),
            "{reason}: {:?}",
            seen[8]
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if b.tables()
                    .await
                    .pending_changes
                    .values()
                    .all(|change| change.uploaded)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("B's successful PUT reaches its state owner");
        harness.stub.reply(json(200, &serde_json::json!({"seq":1,"results":[{"status":"accepted","version_id":"version_b"}]})));
        let committed = harness.stub.wait_for(10).await;
        assert_eq!(
            committed[9].method, "POST",
            "B commits its uploaded bytes: {reason}"
        );
        let pending = a.tables().await.pending_changes;
        assert_eq!(pending.len(), 9, "{reason}");
        assert!(
            pending
                .values()
                .all(|change| !change.uploaded && change.fault_code.is_none()),
            "{reason}"
        );
        assert_eq!(
            std::fs::read(root.path().join("sessions/sess_a/content/drive_a/3")).unwrap(),
            CONTENT
        );
        harness.clock.settle().await;
        assert_eq!(
            harness.stub.arrived(),
            10,
            "suspended uploads must not retry on the old binding"
        );
        let connection = if reason == "connection_ended" {
            "conn_b"
        } else {
            "conn_a"
        };
        for _ in 0..8 {
            harness.stub.reply(Reply::Withhold);
        }
        a.binding_current(Binding {
            connection_id: id(connection),
            binding_id: id("bind_a_new"),
        });
        let seen = harness.stub.wait_for(18).await;
        assert!(
            seen[10..]
                .iter()
                .all(|request| request.query("binding_id").as_deref() == Some("bind_a_new")),
            "{reason}"
        );
        assert_eq!(
            harness.stub.arrived(),
            18,
            "the resumed queue still admits only eight"
        );
        a.close().await;
        b.close().await;
    }
}

#[tokio::test]
async fn an_upload_source_that_cannot_be_opened_is_a_local_content_failure() {
    let harness = Harness::start().await;
    let directory = tempfile::tempdir().unwrap();
    let request = harness.request(Upload {
        sha256: Sha256::new("a".repeat(64)).unwrap(),
        content: Content::File {
            path: directory.path().join("missing"),
            size: 4,
        },
        rate_limited: None,
    });
    assert!(matches!(
        harness.client.upload_content(request).await,
        Err(Ended::Unreadable(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert_eq!(
        harness.stub.arrived(),
        0,
        "no HTTP request can read a missing source"
    );
}

#[tokio::test]
async fn a_resumed_200_restarts_the_real_cache_file_digest_and_byte_count() {
    use std::io::Read;
    let harness = Harness::start().await;
    let root = tempfile::tempdir().unwrap();
    let cache = shared_cache(&harness, root.path());
    harness.stub.reply(Reply::Stall {
        body: CONTENT[..5].to_vec(),
        more: None,
    });
    let read = cache.read(content_needed(
        "sess_a",
        CONTENT_HASH,
        CONTENT.len(),
        &harness.binding,
    ));
    received_prefix(&harness, &cache, "sess_a", 1).await;
    harness.advance(Duration::from_secs(10)).await;
    harness.stub.reply(Endpoint::Download.valid_answer());
    harness.advance(Duration::from_millis(1200)).await;
    let seen = harness.stub.wait_for(2).await;
    assert_eq!(seen[1].header("range"), Some("bytes=5-"));
    let mut file = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, CONTENT);
    assert_eq!(
        std::fs::read(root.path().join("cache/blobs").join(CONTENT_HASH)).unwrap(),
        CONTENT
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("cache/tmp"))
            .unwrap()
            .count(),
        0
    );
}
