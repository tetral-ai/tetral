//! The HTTP transport to Engine: clients that follow no redirect, the
//! credential's key as the bearer of every request, and every answer sorted
//! into what the Connection owner acts on. No request is ever recorded with
//! its headers or body. Reports go through their own client (`Uploads`),
//! whose connections bound how far the body counted as progress runs ahead
//! of what has left the machine.

use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes};
use http::uri::Scheme;
use http::{Request, Uri};
use http_body::{Frame, SizeHint};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper_rustls::{HttpsConnector, MaybeHttpsStream};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::proxy::Tunnel;
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::client::proxy::matcher::Matcher;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use protocol::bootstrap::BootstrapKey;
use protocol::connection::MESSAGE_MAX_BYTES;
use protocol::errors::{ErrorBody, decode_client_error};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, PROXY_AUTHORIZATION};
use reqwest::{Response, StatusCode, Url};
use rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tower_service::Service;

use crate::clock::Clock;

/// The total time of one attempt of an ordinary request.
pub const REQUEST_TOTAL: Duration = Duration::from_secs(10);

/// An answer to one request.
#[derive(Debug)]
pub enum Answer {
    /// A `2xx` answer and its body.
    Success(Vec<u8>),
    /// An error answer and the error body it carries.
    Refused(ErrorBody),
    /// No answer to act on, which is retried.
    Failed(Failure),
}

/// Why a request got no answer to act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Network,
    Timeout,
    /// A `3xx` answer, which is never followed.
    Redirect,
    /// A `5xx` answer, whose body is not defined.
    Server,
    /// A body over its limit, or an error answer whose body does not decode.
    Unreadable,
}

pub struct Transport {
    client: reqwest::Client,
    uploads: Uploads,
    server_url: String,
    chunk_bytes: usize,
}

impl Transport {
    /// The transport to `server_url`, whose rule allows plain HTTP only to a
    /// loopback address.
    pub fn new(server_url: &str) -> Transport {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("an HTTP client with no proxy or certificate option builds");
        Transport {
            client,
            uploads: Uploads::new(UPLOAD_SEND_BUFFER_BYTES, Matcher::from_system()),
            server_url: server_url.to_owned(),
            chunk_bytes: CHUNK_BYTES,
        }
    }

    fn url(&self, path: &str) -> Url {
        Url::parse(&format!("{}{path}", self.server_url))
            .expect("the server URL rule gives a URL that takes a path")
    }

    /// `POST <path>` with a JSON body, its answer within `deadline` on the
    /// CLI's clock.
    pub async fn post(
        &self,
        path: &str,
        key: &BootstrapKey,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Answer {
        let request = self
            .client
            .post(self.url(path))
            .header(AUTHORIZATION, format!("Bearer {}", key.expose()))
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        let answer = async {
            match request.send().await {
                Ok(response) => read_answer(response.into()).await,
                Err(_) => Answer::Failed(Failure::Network),
            }
        };
        tokio::select! {
            answer = answer => answer,
            () = state::seams::sleep_until(deadline) => Answer::Failed(Failure::Timeout),
        }
    }

    /// `POST <path>` of a frozen report with its exact `Content-Length` and
    /// the `traceparent` of its instruction. The attempt fails at `deadline`
    /// on `clock`, and earlier once `stall` passes without progress: before
    /// the first byte is taken, between bytes, or after the last one without
    /// an answer.
    pub async fn post_report(
        &self,
        path: &str,
        key: &BootstrapKey,
        report: ReportRequest,
        clock: &Clock,
    ) -> Answer {
        let ReportRequest {
            bytes,
            traceparent,
            deadline,
            stall,
        } = report;
        let length = bytes.len();
        let progress = Arc::new(AtomicU64::new(nanos(clock.now())));
        let mut request = Request::post(self.url(path).as_str())
            .header(AUTHORIZATION, format!("Bearer {}", key.expose()))
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, length);
        if let Some(traceparent) = traceparent {
            request = request.header("traceparent", traceparent);
        }
        let request = request.body(Chunks {
            bytes,
            taken: 0,
            chunk_bytes: self.chunk_bytes,
            clock: clock.clone(),
            progress: progress.clone(),
        });
        let answer = async {
            let Ok(request) = request else {
                return Answer::Failed(Failure::Network);
            };
            match self.uploads.send(request).await {
                Ok(response) => read_answer(response).await,
                Err(_) => Answer::Failed(Failure::Network),
            }
        };
        tokio::pin!(answer);
        loop {
            let last = Duration::from_nanos(progress.load(Ordering::Acquire));
            let until = deadline.min(last + stall);
            if clock.now() >= until {
                return Answer::Failed(Failure::Timeout);
            }
            tokio::select! {
                answer = &mut answer => return answer,
                () = clock.sleep_until(until) => {}
            }
        }
    }

    /// `GET /v1/client/events` with `query`: the response once its status is
    /// `200`, or the answer otherwise. Its time is bounded by the caller.
    pub async fn events(
        &self,
        query: &[(&str, &str)],
        key: &BootstrapKey,
    ) -> Result<Response, Answer> {
        let mut url = self.url("/v1/client/events");
        url.query_pairs_mut().extend_pairs(query);
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, format!("Bearer {}", key.expose()))
            .send()
            .await
            .map_err(|_| Answer::Failed(Failure::Network))?;
        if response.status() == StatusCode::OK {
            Ok(response)
        } else {
            Err(read_answer(response.into()).await)
        }
    }
}

/// The socket send buffer of each report upload's connection. Set before
/// connecting, it also stops the kernel from resizing the buffer, so that the
/// body that counts as progress is at most this far ahead of what has left
/// the machine.
const UPLOAD_SEND_BUFFER_BYTES: usize = 65_536;

/// hyper's write buffer limit, its least: hyper takes the next chunk of a
/// report's body, which counts as progress, only while fewer bytes than this
/// wait in its buffer, so it holds less than this plus one chunk
/// ([`CHUNK_BYTES`]) ahead of the socket's send buffer.
const UPLOAD_WRITE_BUFFER_BYTES: usize = 8_192;

/// The client of report uploads. reqwest sets no socket send buffer, so
/// uploads go through hyper's client with a connector of their own, which
/// takes the proxy of `proxies`: plain HTTP is sent to it in absolute form,
/// HTTPS through its `CONNECT` tunnel. `Matcher::from_system` is the matcher
/// that reqwest's client takes for the CLI's other requests.
struct Uploads {
    client: Client<UploadConnector, Chunks>,
    proxies: Arc<Matcher>,
}

impl Uploads {
    fn new(send_buffer_bytes: usize, proxies: Matcher) -> Uploads {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_send_buffer_size(Some(send_buffer_bytes));
        let tls = Arc::new(tls_config());
        let proxies = Arc::new(proxies);
        let connector = UploadConnector {
            https: HttpsConnector::from((http, tls.clone())),
            tls,
            proxies: proxies.clone(),
        };
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .http1_max_buf_size(UPLOAD_WRITE_BUFFER_BYTES)
            .build(connector);
        Uploads { client, proxies }
    }

    async fn send(
        &self,
        mut request: Request<Chunks>,
    ) -> Result<http::Response<Incoming>, BoxError> {
        // A tunnel carries the proxy's credentials in its `CONNECT`; a
        // forwarded request carries them itself.
        if request.uri().scheme() == Some(&Scheme::HTTP)
            && let Some(auth) = self
                .proxies
                .intercept(request.uri())
                .and_then(|proxy| proxy.basic_auth().cloned())
        {
            request.headers_mut().insert(PROXY_AUTHORIZATION, auth);
        }
        Ok(self.client.request(request).await?)
    }
}

/// The TLS configuration that reqwest's client builds for the CLI's other
/// requests: the platform's certificate verifier, TLS 1.2 and 1.3, and
/// HTTP/1.1.
fn tls_config() -> rustls::ClientConfig {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    let verifier = rustls_platform_verifier::Verifier::new(provider.clone())
        .expect("the platform's certificate verifier builds");
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the crypto provider supports TLS 1.2 and 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

type BoxError = Box<dyn StdError + Send + Sync>;

#[derive(Clone)]
struct UploadConnector {
    https: HttpsConnector<HttpConnector>,
    tls: Arc<rustls::ClientConfig>,
    proxies: Arc<Matcher>,
}

impl Service<Uri> for UploadConnector {
    type Response = UploadStream;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<UploadStream, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, destination: Uri) -> Self::Future {
        Box::pin(self.clone().connect(destination))
    }
}

impl UploadConnector {
    async fn connect(mut self, destination: Uri) -> Result<UploadStream, BoxError> {
        let Some(proxy) = self.proxies.intercept(&destination) else {
            let io = self.https.call(destination).await?;
            return Ok(UploadStream::new(io, false));
        };
        if destination.scheme() != Some(&Scheme::HTTPS) {
            let io = self.https.call(proxy.uri().clone()).await?;
            return Ok(UploadStream::new(io, true));
        }
        let mut tunnel = Tunnel::new(proxy.uri().clone(), self.https.clone());
        if let Some(auth) = proxy.basic_auth() {
            tunnel = tunnel.with_auth(auth.clone());
        }
        let tunneled = tunnel.call(destination.clone()).await?;
        let host = destination.host().ok_or("the destination has no host")?;
        let name = ServerName::try_from(host.to_owned())?;
        let tls = TlsConnector::from(self.tls)
            .connect(name, TokioIo::new(tunneled))
            .await?;
        Ok(UploadStream::new(
            MaybeHttpsStream::Https(TokioIo::new(tls)),
            false,
        ))
    }
}

trait UploadIo: hyper::rt::Read + hyper::rt::Write + Connection + Unpin + Send {}

impl<T: hyper::rt::Read + hyper::rt::Write + Connection + Unpin + Send> UploadIo for T {}

/// A report upload's connection: to its destination, through a proxy's
/// tunnel or, `forwarded`, to a proxy that takes the request in absolute
/// form.
struct UploadStream {
    io: Box<dyn UploadIo>,
    forwarded: bool,
}

impl UploadStream {
    fn new(io: impl UploadIo + 'static, forwarded: bool) -> UploadStream {
        UploadStream {
            io: Box::new(io),
            forwarded,
        }
    }

    fn io(self: Pin<&mut Self>) -> Pin<&mut dyn UploadIo> {
        Pin::new(&mut *self.get_mut().io)
    }
}

impl Connection for UploadStream {
    fn connected(&self) -> Connected {
        self.io.connected().proxy(self.forwarded)
    }
}

impl hyper::rt::Read for UploadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.io().poll_read(cx, buf)
    }
}

impl hyper::rt::Write for UploadStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.io().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.io().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.io().poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.io().poll_write_vectored(cx, bufs)
    }
}

/// One attempt of a frozen report.
#[derive(Debug)]
pub struct ReportRequest {
    pub bytes: Vec<u8>,
    pub traceparent: Option<String>,
    /// The end of the attempt on the outbox's clock.
    pub deadline: Duration,
    pub stall: Duration,
}

/// The bytes of a report handed to the connection one chunk at a time. Each
/// chunk taken stores the clock's time in `progress`, since a waiter that
/// noticed progress only when it woke would measure the stall from then.
struct Chunks {
    bytes: Vec<u8>,
    taken: usize,
    /// [`CHUNK_BYTES`] but for tests.
    chunk_bytes: usize,
    clock: Clock,
    progress: Arc<AtomicU64>,
}

fn nanos(time: Duration) -> u64 {
    u64::try_from(time.as_nanos()).unwrap_or(u64::MAX)
}

/// The size of each chunk of a report's body.
const CHUNK_BYTES: usize = 65_536;

impl http_body::Body for Chunks {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        let start = this.taken;
        if start >= this.bytes.len() {
            return Poll::Ready(None);
        }
        let end = (start + this.chunk_bytes).min(this.bytes.len());
        let chunk = Bytes::copy_from_slice(&this.bytes[start..end]);
        this.taken = end;
        this.progress
            .store(nanos(this.clock.now()), Ordering::Release);
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.taken >= self.bytes.len()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact((self.bytes.len() - self.taken) as u64)
    }
}

async fn read_answer<B: http_body::Body + Unpin>(response: http::Response<B>) -> Answer {
    let (parts, body) = response.into_parts();
    let status = parts.status;
    if status.is_redirection() {
        return Answer::Failed(Failure::Redirect);
    }
    if status.is_server_error() {
        return Answer::Failed(Failure::Server);
    }
    let Some(body) = read_bounded(body, MESSAGE_MAX_BYTES).await else {
        return Answer::Failed(Failure::Unreadable);
    };
    if status.is_success() {
        return Answer::Success(body);
    }
    match decode_client_error(status.as_u16(), &body) {
        Ok(error) => Answer::Refused(error),
        Err(_) => Answer::Failed(Failure::Unreadable),
    }
}

/// The body, refused before it is read past `limit` bytes.
async fn read_bounded<B: http_body::Body + Unpin>(mut body: B, limit: usize) -> Option<Vec<u8>> {
    if body
        .size_hint()
        .exact()
        .is_some_and(|length| length > limit as u64)
    {
        return None;
    }
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let Ok(chunk) = frame.ok()?.into_data() else {
            continue;
        };
        if bytes.len() + chunk.remaining() > limit {
            return None;
        }
        bytes.put(chunk);
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use http_body::Body as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    fn chunks(bytes: &[u8], progress: Arc<AtomicU64>) -> Chunks {
        Chunks {
            bytes: bytes.to_vec(),
            taken: 0,
            chunk_bytes: 4,
            clock: Clock::new(|| Duration::from_secs(7), Duration::from_millis(10)),
            progress,
        }
    }

    /// The head of the first request that `listener` takes, answered with
    /// `502`.
    async fn first_head(listener: TcpListener) -> String {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        let mut buffer = [0; 4_096];
        while !received.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0, "the request ended before its head");
            received.extend_from_slice(&buffer[..count]);
        }
        stream
            .write_all(b"HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n")
            .await
            .unwrap();
        let received = String::from_utf8(received).unwrap();
        received.split("\r\n\r\n").next().unwrap().to_owned()
    }

    #[tokio::test]
    async fn a_report_upload_reaches_its_proxy_with_the_proxy_credentials() {
        for (destination, request_line) in [
            (
                "http://engine.invalid/v1/client/tool-status",
                "POST http://engine.invalid/v1/client/tool-status HTTP/1.1",
            ),
            (
                "https://engine.invalid/v1/client/tool-status",
                "CONNECT engine.invalid:443 HTTP/1.1",
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = format!("http://user:secret@{}", listener.local_addr().unwrap());
            let uploads = Uploads::new(
                UPLOAD_SEND_BUFFER_BYTES,
                Matcher::builder().http(proxy.clone()).https(proxy).build(),
            );
            let request = Request::post(destination)
                .body(chunks(b"{}", Arc::default()))
                .unwrap();
            let upload = tokio::spawn(async move { uploads.send(request).await.is_ok() });
            let head = tokio::time::timeout(Duration::from_secs(10), first_head(listener))
                .await
                .expect("the proxy takes the upload");
            let mut lines = head.lines();
            assert_eq!(lines.next(), Some(request_line));
            let credentials = lines.find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("proxy-authorization")
                    .then(|| value.trim().to_owned())
            });
            assert_eq!(
                credentials.as_deref(),
                Some("Basic dXNlcjpzZWNyZXQ="),
                "{head}"
            );
            upload.await.unwrap();
        }
    }

    #[test]
    fn a_report_body_is_handed_over_in_chunks_of_the_chunk_size_each_marking_progress() {
        let progress = Arc::new(AtomicU64::new(0));
        let mut chunks = chunks(b"0123456789", progress.clone());
        let mut context = Context::from_waker(Waker::noop());
        let mut handed = Vec::new();
        while let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut chunks).poll_frame(&mut context) {
            handed.push((
                frame.into_data().unwrap().len(),
                progress.swap(0, Ordering::AcqRel),
            ));
        }
        let marked = nanos(Duration::from_secs(7));
        assert_eq!(handed, [(4, marked), (4, marked), (2, marked)]);
    }
}
