//! The drive transfer client against a local HTTP stub on loopback that
//! answers from `protocol/drive-transfer-v1.fixtures.json`, on a clock the
//! test moves: the answers it accepts, the class of each answer, the
//! deadlines, the retries and their delays, a rebind, and a download that
//! resumes or restarts.

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

/// The `drive_transfer.answer_rejected` records, each as its error code.
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
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "event" => self.event = Some(value.to_owned()),
            "error.code" => self.code = Some(value.to_owned()),
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
            self.0.lock().unwrap().push(fields.code.unwrap_or_default());
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
    /// The head with the whole body's length, then `body` and nothing more.
    Stall { body: Vec<u8> },
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
    async fn start() -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(StubState::default()));
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
        Reply::Stall { body } => {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                CONTENT.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            answered();
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
    _link: DriveLink,
    /// The 429s an upload's `rate_limited` was told of.
    rate_limited: Arc<AtomicU64>,
}

impl Harness {
    async fn start() -> Harness {
        rejections();
        let stub = Stub::start().await;
        let clock = TestClock::default();
        clock.set(Duration::from_secs(1_000));
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
            _link: link,
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
        Err(Ended::CursorExpired) => Outcome::CursorExpired,
        Err(Ended::Decision(error)) => Outcome::Decision(error.code.as_str()),
        Err(Ended::Permanent { status, code }) => Outcome::Permanent(status, code),
        Err(Ended::Sink(error)) => panic!("the sink failed: {error}"),
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
                ["invalid_response"],
                "{name}"
            );
        }
        rows += 1;
    }
    assert_eq!(rows, 55);

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
            (true, "stale_binding") => {
                harness.stub.wait_for(seen + 1).await;
                harness.advance(Duration::from_secs(60)).await;
                assert_eq!(harness.stub.seen().len(), seen + 1, "{name}");
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
            Outcome::Permanent(_, None) => vec!["invalid_response".to_owned()],
            Outcome::Permanent(_, Some("drive_access_denied")) => Vec::new(),
            Outcome::Permanent(_, Some(code)) => vec![(*code).to_owned()],
            _ => Vec::new(),
        };
        assert_eq!(logs, expected_logs, "{name}");
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
    assert_eq!(harness.stub.seen().len(), 1);
    // The first temporary failure waits 1 second and at most 20% more.
    harness.advance(Duration::from_millis(1_200)).await;
    harness.stub.wait_for(2).await;
    assert_eq!(sent.await.unwrap(), Outcome::Accepted);
}

impl Harness {
    /// Starts a download of [`CONTENT`] into a shared sink whose first
    /// answer stalls after five bytes, and waits until the client has
    /// those bytes and has looked at its clock since.
    async fn stalled_download(&self) -> (Shared, tokio::task::JoinHandle<Outcome>) {
        self.stub.reply(Reply::Stall {
            body: CONTENT[..5].to_vec(),
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
        self.clock.settle().await;
        (sink, sent)
    }
}

#[tokio::test]
async fn a_stalled_download_fails_after_ten_seconds_and_resumes_with_a_range() {
    let harness = Harness::start().await;
    let (sink, sent) = harness.stalled_download().await;
    harness
        .advance(Duration::from_secs(10) - Duration::from_millis(1))
        .await;
    harness.advance(Duration::from_millis(1)).await;
    harness.stub.reply(Reply::Answer {
        status: 206,
        headers: vec![("Content-Range", format!("bytes 5-11/{}", CONTENT.len()))],
        body: CONTENT[5..].to_vec(),
    });
    // Not sent again before the first temporary wait of 1 second.
    assert_eq!(harness.stub.seen().len(), 1);
    harness.advance(Duration::from_millis(1_200)).await;
    let seen = harness.stub.wait_for(2).await;
    assert_eq!(seen[1].header("range"), Some("bytes=5-"));
    assert_eq!(sent.await.unwrap(), Outcome::Accepted);
    assert_eq!(sink.bytes(), CONTENT);
    assert_eq!(sink.restarts(), 1);
}

#[tokio::test]
async fn a_resumed_download_answered_200_restarts_at_byte_zero() {
    let harness = Harness::start().await;
    let (sink, sent) = harness.stalled_download().await;
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
    let mut seen = 1;
    let mut waits = vec![Duration::from_secs(3)];
    waits.extend([1, 2, 4, 8, 16, 30, 30].map(Duration::from_secs));
    for (index, wait) in waits.iter().enumerate() {
        let last = index + 1 == waits.len();
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
        assert_eq!(
            harness.stub.seen().len(),
            seen,
            "before the wait of {wait:?}"
        );
        let longest = if index == 0 { *wait } else { wait.mul_f64(1.2) };
        harness.clock.set(start + longest);
        harness.stub.wait_for(seen + 1).await;
        seen += 1;
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
    assert_eq!(harness.stub.seen().len(), 1);

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

    // A request waiting for a new binding ends with its drive sync.
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
    assert_eq!(
        ended.expect("the request ends").unwrap(),
        Outcome::Permanent(0, None)
    );
    assert_eq!(harness.stub.seen().len(), 4);
}
