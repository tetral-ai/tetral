//! The HTTP transport to Engine: clients that follow no redirect, the
//! credential's key as the bearer of every request, and every answer sorted
//! into what the Connection owner acts on. No request is ever recorded with
//! its headers or body. Reports go through their own client (`Uploads`),
//! whose TCP connections let an upload count as progress only the bytes that
//! the other end has acknowledged.

use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::Future;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll, ready};
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
use hyper_util::client::legacy::connect::{
    CaptureConnection, Connected, Connection, HttpConnector, capture_connection,
};
use hyper_util::client::proxy::matcher::Matcher;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use protocol::bootstrap::BootstrapKey;
use protocol::connection::MESSAGE_MAX_BYTES;
use protocol::errors::{ErrorBody, decode_client_error};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, PROXY_AUTHORIZATION};
use reqwest::{Response, StatusCode, Url};
use rustls::pki_types::ServerName;
use state::clock::Clock;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tower_service::Service;

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
    /// A download's local write failed.
    Sink(std::io::ErrorKind),
}

pub struct Transport {
    client: reqwest::Client,
    uploads: Uploads,
    drive: Uploads<DriveBody>,
    server_url: String,
    /// What each report attempt has seen of its socket, for the tests that
    /// move the clock only once an attempt has seen what the kernel did.
    #[cfg(test)]
    attempts: Mutex<Vec<AttemptSeen>>,
    /// Controlled socket observations for deadline tests. Real socket and TLS
    /// tests exercise the production progress source separately.
    #[cfg(test)]
    progress_sample: Option<Arc<dyn Fn(Duration) -> (u64, u64, bool) + Send + Sync>>,
}

/// The acknowledged bytes a report attempt last read, and whether it saw its
/// body sent.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AttemptSeen {
    pub(crate) acknowledged: Option<u64>,
    pub(crate) sent: bool,
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
            uploads: Uploads::new(Matcher::from_system()),
            drive: Uploads::new(Matcher::from_system()),
            server_url: server_url.to_owned(),
            #[cfg(test)]
            attempts: Mutex::default(),
            #[cfg(test)]
            progress_sample: None,
        }
    }

    /// A transport whose attempts read their progress from `sample`
    /// instead of their sockets: the bytes moved by each time of the clock.
    #[cfg(test)]
    pub(crate) fn sampled(
        server_url: &str,
        sample: impl Fn(Duration) -> u64 + Send + Sync + 'static,
    ) -> Transport {
        let mut transport = Self::new(server_url);
        transport.progress_sample = Some(Arc::new(move |now| (sample(now), 0, false)));
        transport
    }

    /// The attempts of reports this transport has made, in their order.
    #[cfg(test)]
    pub(crate) fn attempts(&self) -> Vec<AttemptSeen> {
        self.attempts.lock().unwrap().clone()
    }

    #[cfg(test)]
    fn with_uploads(server_url: &str, proxies: Matcher, tls: rustls::ClientConfig) -> Transport {
        let mut transport = Self::new(server_url);
        transport.uploads = Uploads::configured(proxies, Arc::new(tls));
        transport
    }

    pub(crate) fn url(&self, path: &str) -> Url {
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

    /// `POST <path>` of an immutable report with its exact `Content-Length` and
    /// the `traceparent` of its instruction. The attempt fails at `deadline`
    /// on `clock`, and earlier once `stall` passes without progress: until
    /// its connection is up, between rises of the bytes that the other end
    /// has acknowledged, and from the body's being sent to the answer. The
    /// socket is read at each recheck of the clock, at least once a second;
    /// each reading is reported to the test seam counter
    /// `outbox.upload_checked`, so that a test can tell that the attempt has
    /// read its socket at a time it moved the clock to.
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
        let mut request = Request::post(self.url(path).as_str())
            .header(AUTHORIZATION, format!("Bearer {}", key.expose()))
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, length);
        if let Some(traceparent) = traceparent {
            request = request.header("traceparent", traceparent);
        }
        let taken = Arc::new(OnceLock::new());
        let Ok(mut request) = request.body(ReportBody {
            bytes: Some(bytes),
            connection: None,
            taken: taken.clone(),
        }) else {
            return Answer::Failed(Failure::Network);
        };
        let connection = capture_connection(&mut request);
        request.body_mut().connection = Some(connection.clone());
        let answer = async {
            match self.uploads.send(request).await {
                Ok(response) => read_answer(response).await,
                Err(_) => Answer::Failed(Failure::Network),
            }
        };
        tokio::pin!(answer);
        let mut progress = Progress::new(clock.now());
        #[cfg(test)]
        let attempt = {
            let mut attempts = self.attempts.lock().unwrap();
            attempts.push(AttemptSeen::default());
            attempts.len() - 1
        };
        let mut socket = None;
        loop {
            if socket.is_none() {
                socket = socket_of(&connection);
            }
            self.read_progress(&mut progress, socket.as_deref(), &taken, clock.now());
            if socket.is_some() {
                state::seams::count("outbox.upload_checked");
            }
            #[cfg(test)]
            {
                self.attempts.lock().unwrap()[attempt] = AttemptSeen {
                    acknowledged: progress.acknowledged,
                    sent: progress.sent,
                };
            }
            let until = deadline.min(progress.last + stall);
            if clock.now() >= until {
                return Answer::Failed(Failure::Timeout);
            }
            tokio::select! {
                answer = &mut answer => return answer,
                () = clock.sleep_step(until) => {}
            }
        }
    }

    fn read_progress(
        &self,
        progress: &mut Progress,
        socket: Option<&Socket>,
        taken: &OnceLock<u64>,
        now: Duration,
    ) {
        #[cfg(test)]
        if let Some(sample) = &self.progress_sample {
            let (acknowledged, held, written) = sample(now);
            progress.observe(acknowledged, held, written, now);
            return;
        }
        if let Some(socket) = socket {
            progress.read(socket, taken, now);
        }
    }

    /// One attempt of a drive transfer request, whose answer `read` takes
    /// and whose body it reads, counting each byte in the counter it gets.
    /// The attempt fails once `stall` passes on `clock` with no rise of the
    /// bytes written to its socket or read of its answer, counted from its
    /// start, so that connecting, TLS and waiting for the headers end then
    /// too, and at `deadline` when it has one.
    pub(crate) async fn drive<T, F>(
        &self,
        mut request: Request<DriveBody>,
        deadline: Option<Duration>,
        stall: Duration,
        clock: &Clock,
        read: impl FnOnce(http::Response<Incoming>, Arc<AtomicU64>) -> F,
    ) -> Result<T, Failure>
    where
        F: Future<Output = Result<T, Failure>>,
    {
        let connection = capture_connection(&mut request);
        let received = Arc::new(AtomicU64::new(0));
        let reading = received.clone();
        let answer = async {
            match self.drive.send(request).await {
                Ok(response) => read(response, reading).await,
                Err(_) => Err(Failure::Network),
            }
        };
        tokio::pin!(answer);
        let mut last = clock.now();
        let mut seen = 0;
        let mut socket = None;
        loop {
            if socket.is_none() {
                socket = socket_of(&connection);
            }
            let now = clock.now();
            let moved = self.drive_progress(socket.as_deref(), &received, now);
            if moved > seen {
                seen = moved;
                last = now;
            }
            let until = deadline.map_or(last + stall, |deadline| deadline.min(last + stall));
            if clock.now() >= until {
                return Err(Failure::Timeout);
            }
            tokio::select! {
                answer = &mut answer => return answer,
                () = clock.sleep_step(until) => {}
            }
        }
    }

    /// The bytes an attempt has written to its socket and read of its
    /// answer.
    fn drive_progress(&self, socket: Option<&Socket>, received: &AtomicU64, now: Duration) -> u64 {
        #[cfg(test)]
        if let Some(sample) = &self.progress_sample {
            return sample(now).0;
        }
        let _ = now;
        socket.map_or(0, |socket| socket.written.load(Ordering::Acquire))
            + received.load(Ordering::Acquire)
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

/// What an attempt has seen of its connection's socket.
struct Progress {
    /// The clock's time of the attempt's last progress: its start, the first
    /// read of its socket, each rise of the acknowledged bytes and the body's
    /// being sent.
    last: Duration,
    acknowledged: Option<u64>,
    sent: bool,
}

impl Progress {
    fn new(start: Duration) -> Progress {
        Progress {
            last: start,
            acknowledged: None,
            sent: false,
        }
    }

    /// Reads `socket` at `now`. The body is sent once a flush that ended
    /// after it was `taken` has written its last byte to the socket and the
    /// socket holds nothing unacknowledged; that is reported to the test
    /// seam counter `outbox.sent`.
    fn read(&mut self, socket: &Socket, taken: &OnceLock<u64>, now: Duration) {
        // The flushes are read before what the socket holds, so that the
        // body's last byte is in the socket when the socket is read.
        let written = taken
            .get()
            .is_some_and(|flushes| socket.flushes() > *flushes);
        let Some((acknowledged, held)) = socket.state() else {
            return;
        };
        self.observe(acknowledged, held, written, now);
    }

    fn observe(&mut self, acknowledged: u64, held: u64, written: bool, now: Duration) {
        if self.acknowledged.is_none_or(|before| acknowledged > before) {
            self.last = now;
        }
        self.acknowledged = Some(acknowledged);
        if written && held == 0 && !self.sent {
            self.sent = true;
            self.last = now;
            state::seams::count("outbox.sent");
        }
    }
}

/// What a report upload reads of its connection's TCP socket: the bytes
/// written to it, the flushes that ended, and, while the stream is open, how
/// many of those bytes the socket still holds unacknowledged.
struct Socket {
    /// The socket's descriptor until its stream is dropped, so that a read
    /// never reaches a descriptor number that was used again.
    descriptor: Mutex<Option<RawFd>>,
    written: AtomicU64,
    flushes: AtomicU64,
}

impl Socket {
    fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Acquire)
    }

    /// The bytes acknowledged and those still held, or `None` once the
    /// stream is gone or the socket cannot tell.
    fn state(&self) -> Option<(u64, u64)> {
        let descriptor = self
            .descriptor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Read before what the socket holds, so that bytes written in between
        // never count as acknowledged.
        let written = self.written.load(Ordering::Acquire);
        let held = unacknowledged((*descriptor)?).ok()?;
        Some((written.saturating_sub(held), held))
    }
}

/// The bytes that the TCP socket `descriptor` holds and its peer has not
/// acknowledged: `SIOCOUTQ`, which Linux defines as `TIOCOUTQ`.
#[cfg(target_os = "linux")]
fn unacknowledged(descriptor: RawFd) -> std::io::Result<u64> {
    let mut held: libc::c_int = 0;
    // SAFETY: the caller holds the descriptor open, and the request writes
    // one `c_int` through the pointer.
    let result = unsafe { libc::ioctl(descriptor, libc::TIOCOUTQ, &raw mut held) };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(u64::try_from(held).unwrap_or(0))
}

/// The bytes that the TCP socket `descriptor` holds and its peer has not
/// acknowledged: `SO_NWRITE`.
#[cfg(target_os = "macos")]
fn unacknowledged(descriptor: RawFd) -> std::io::Result<u64> {
    let mut held: libc::c_int = 0;
    let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .expect("a c_int's size fits a socklen_t");
    // SAFETY: the caller holds the descriptor open, and the option writes at
    // most `length` bytes through the pointer.
    let result = unsafe {
        libc::getsockopt(
            descriptor,
            libc::SOL_SOCKET,
            libc::SO_NWRITE,
            (&raw mut held).cast(),
            &raw mut length,
        )
    };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(u64::try_from(held).unwrap_or(0))
}

/// The socket of the connection that `connection` captured, once its request
/// has one.
fn socket_of(connection: &CaptureConnection) -> Option<Arc<Socket>> {
    let connected = connection.connection_metadata();
    let mut extras = http::Extensions::new();
    connected.as_ref()?.get_extras(&mut extras);
    extras.get::<Arc<Socket>>().cloned()
}

/// The client of report uploads. reqwest's client gives a request no way to
/// its connection's socket, so uploads go through hyper's client with a
/// connector of their own, whose TCP connections are [`Counted`] and each
/// tell their [`Socket`] to the request that uses them. It takes the proxy of
/// `proxies`: plain HTTP is sent to it in absolute form, HTTPS through its
/// `CONNECT` tunnel. `Matcher::from_system` is the matcher that reqwest's
/// client takes for the CLI's other requests.
struct Uploads<B = ReportBody> {
    client: Client<UploadConnector, B>,
    proxies: Arc<Matcher>,
}

impl<B> Uploads<B>
where
    B: http_body::Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    fn new(proxies: Matcher) -> Uploads<B> {
        Self::configured(proxies, Arc::new(tls_config()))
    }

    fn configured(proxies: Matcher, tls: Arc<rustls::ClientConfig>) -> Uploads<B> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        let proxies = Arc::new(proxies);
        let connector = UploadConnector {
            https: HttpsConnector::from((CountingConnector(http), tls.clone())),
            tls,
            proxies: proxies.clone(),
        };
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .build(connector);
        Uploads { client, proxies }
    }

    async fn send(&self, mut request: Request<B>) -> Result<http::Response<Incoming>, BoxError> {
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

/// [`HttpConnector`], each of whose connections is [`Counted`].
#[derive(Clone)]
struct CountingConnector(HttpConnector);

impl Service<Uri> for CountingConnector {
    type Response = Counted;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Counted, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.0.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, destination: Uri) -> Self::Future {
        let connecting = self.0.call(destination);
        Box::pin(async move { Ok(Counted::new(connecting.await?)) })
    }
}

/// The TCP connection of a report upload, to its destination or to its
/// proxy, which counts what is written to its socket for [`Socket`].
struct Counted {
    io: TokioIo<TcpStream>,
    socket: Arc<Socket>,
}

impl Counted {
    fn new(io: TokioIo<TcpStream>) -> Counted {
        let socket = Arc::new(Socket {
            descriptor: Mutex::new(Some(io.inner().as_raw_fd())),
            written: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        });
        Counted { io, socket }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        *self
            .socket
            .descriptor
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }
}

impl Connection for Counted {
    fn connected(&self) -> Connected {
        self.io.connected().extra(self.socket.clone())
    }
}

impl hyper::rt::Read for Counted {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for Counted {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let written = ready!(Pin::new(&mut this.io).poll_write(cx, buf))?;
        this.socket
            .written
            .fetch_add(written as u64, Ordering::AcqRel);
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.io).poll_flush(cx))?;
        this.socket.flushes.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let written = ready!(Pin::new(&mut this.io).poll_write_vectored(cx, bufs))?;
        this.socket
            .written
            .fetch_add(written as u64, Ordering::AcqRel);
        Poll::Ready(Ok(written))
    }
}

#[derive(Clone)]
struct UploadConnector {
    https: HttpsConnector<CountingConnector>,
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

/// One attempt of an immutable report.
#[derive(Debug)]
pub struct ReportRequest {
    pub bytes: Bytes,
    pub traceparent: Option<String>,
    /// The end of the attempt on the outbox's clock.
    pub deadline: Duration,
    pub stall: Duration,
}

/// A report's body, handed to its connection whole. Taking it notes in
/// `taken` how many flushes its connection's socket has ended, since a later
/// flush writes the body's last byte to the socket.
struct ReportBody {
    bytes: Option<Bytes>,
    /// The request's connection, set once the request is built.
    connection: Option<CaptureConnection>,
    taken: Arc<OnceLock<u64>>,
}

impl http_body::Body for ReportBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        let Some(bytes) = this.bytes.take() else {
            return Poll::Ready(None);
        };
        if let Some(socket) = this.connection.as_ref().and_then(socket_of) {
            let _ = this.taken.set(socket.flushes());
        }
        Poll::Ready(Some(Ok(Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.bytes.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.bytes.as_ref().map_or(0, |bytes| bytes.len() as u64))
    }
}

/// A drive transfer request's body: none, bytes, or a file of a given size
/// read in chunks, which fails when the file holds another count.
pub(crate) enum DriveBody {
    Empty,
    Bytes(Option<Bytes>),
    File {
        file: tokio::fs::File,
        size: u64,
        left: u64,
        buffer: Box<[u8]>,
    },
}

/// The bytes a file body reads at a time.
const FILE_CHUNK_BYTES: usize = 65_536;

impl DriveBody {
    pub(crate) fn file(file: tokio::fs::File, size: u64) -> DriveBody {
        DriveBody::File {
            file,
            size,
            left: size,
            buffer: vec![0; FILE_CHUNK_BYTES].into_boxed_slice(),
        }
    }
}

impl http_body::Body for DriveBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        match self.get_mut() {
            DriveBody::Empty => Poll::Ready(None),
            DriveBody::Bytes(bytes) => {
                Poll::Ready(bytes.take().map(|bytes| Ok(Frame::data(bytes))))
            }
            DriveBody::File {
                file, left, buffer, ..
            } => {
                // One byte more than is left, so that a longer file is seen.
                let want = usize::try_from(*left)
                    .unwrap_or(usize::MAX)
                    .saturating_add(1)
                    .min(buffer.len());
                let mut read = tokio::io::ReadBuf::new(&mut buffer[..want]);
                ready!(tokio::io::AsyncRead::poll_read(
                    Pin::new(file),
                    cx,
                    &mut read
                ))?;
                let count = read.filled().len() as u64;
                if count > *left || (count == 0 && *left > 0) {
                    return Poll::Ready(Some(Err(std::io::Error::other(
                        "the file no longer holds its size",
                    ))));
                }
                if count == 0 {
                    return Poll::Ready(None);
                }
                *left -= count;
                Poll::Ready(Some(Ok(Frame::data(Bytes::copy_from_slice(read.filled())))))
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            DriveBody::Empty => SizeHint::with_exact(0),
            DriveBody::Bytes(bytes) => {
                SizeHint::with_exact(bytes.as_ref().map_or(0, |bytes| bytes.len() as u64))
            }
            DriveBody::File { size, .. } => SizeHint::with_exact(*size),
        }
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
pub(crate) async fn read_bounded<B: http_body::Body + Unpin>(
    mut body: B,
    limit: usize,
) -> Option<Vec<u8>> {
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

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    fn body(bytes: &[u8]) -> ReportBody {
        ReportBody {
            bytes: Some(Bytes::copy_from_slice(bytes)),
            connection: None,
            taken: Arc::default(),
        }
    }

    #[tokio::test]
    async fn a_transport_frame_keeps_the_body_charge_after_other_owners_drop() {
        let budget = crate::report_memory::Budget::new(8, 0);
        let pending = budget.retain(vec![1; 8], false).unwrap();
        let witness = budget.witnesses().pop().unwrap();
        let mut body = ReportBody {
            bytes: Some(pending.clone()),
            connection: None,
            taken: Arc::default(),
        };
        let frame = body.frame().await.unwrap().unwrap();
        drop(body);
        drop(pending);
        assert_eq!(budget.used(), [8, 0]);
        assert!(witness.upgrade().is_some());
        drop(frame);
        assert_eq!(budget.used(), [0, 0]);
        assert!(witness.upgrade().is_none());
    }

    #[tokio::test]
    async fn observed_progress_moves_only_the_stall_deadline_and_never_the_total() {
        for continuing in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                // Keep the real request open without producing an HTTP answer.
                std::future::pending::<()>().await;
                drop(stream);
            });
            let tick = Arc::new(AtomicU64::new(0));
            let now = tick.clone();
            let clock = Clock::new(
                move || Duration::from_secs(now.load(Ordering::SeqCst)),
                Duration::from_millis(1),
            );
            let sampled = Arc::new(AtomicU64::new(u64::MAX));
            let seen = sampled.clone();
            let mut transport = Transport::new(&format!("http://{address}"));
            transport.progress_sample = Some(Arc::new(move |now| {
                seen.store(now.as_secs(), Ordering::SeqCst);
                let acknowledged = u64::from(now >= Duration::from_secs(7))
                    + u64::from(continuing && now >= Duration::from_secs(14));
                (acknowledged, 1, false)
            }));
            let key = protocol::bootstrap::Bootstrap::decode(
                br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
            ).unwrap().connection_key;
            let upload = tokio::spawn(async move {
                transport
                    .post_report(
                        "/v1/client/tool-status",
                        &key,
                        ReportRequest {
                            bytes: Bytes::from_static(b"{}"),
                            traceparent: None,
                            deadline: Duration::from_secs(20),
                            stall: Duration::from_secs(10),
                        },
                        &clock,
                    )
                    .await
            });
            let before_end: &[u64] = if continuing {
                &[0, 7, 10, 14, 19]
            } else {
                &[0, 7, 10, 16]
            };
            for &second in before_end {
                tick.store(second, Ordering::SeqCst);
                tokio::time::timeout(Duration::from_secs(2), async {
                    while sampled.load(Ordering::SeqCst) != second {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("attempt samples the controlled progress at this time");
                assert!(
                    !upload.is_finished(),
                    "attempt ended at {second}s, continuing={continuing}"
                );
            }
            let end = if continuing { 20 } else { 17 };
            tick.store(end, Ordering::SeqCst);
            let answer = tokio::time::timeout(Duration::from_secs(2), upload)
                .await
                .expect("attempt ends at the owning deadline")
                .unwrap();
            assert!(matches!(answer, Answer::Failed(Failure::Timeout)));
            assert_eq!(sampled.load(Ordering::SeqCst), end);
            server.abort();
        }
    }

    #[tokio::test]
    async fn tls_uploads_preserve_socket_progress_directly_and_through_connect() {
        use rustls::pki_types::PrivatePkcs8KeyDer;
        use tokio::io::{AsyncBufReadExt, BufReader};
        use tokio::sync::oneshot;

        for through_proxy in [false, true] {
            let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let certificate = certified.cert.der().clone();
            let private = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let server = rustls::ServerConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![certificate.clone()], private.into())
                .unwrap();
            let mut roots = rustls::RootCertStore::empty();
            roots.add(certificate).unwrap();
            let client = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(4_096).unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = socket.listen(1).unwrap();
            let address = listener.local_addr().unwrap();
            let (read_body, may_read) = oneshot::channel();
            let (answer, may_answer) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let tls = tokio_rustls::TlsAcceptor::from(Arc::new(server))
                    .accept(socket)
                    .await
                    .unwrap();
                let mut stream = BufReader::new(tls);
                let mut line = String::new();
                let mut length = 0;
                while stream.read_line(&mut line).await.unwrap() > 2 {
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ")
                    {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                    line.clear();
                }
                may_read.await.unwrap();
                let mut body = vec![0; length];
                stream.read_exact(&mut body).await.unwrap();
                assert_eq!(body, vec![b'x'; 524_288]);
                may_answer.await.unwrap();
                stream
                    .get_mut()
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                    )
                    .await
                    .unwrap();
            });
            let (proxies, proxy) = if through_proxy {
                let socket = tokio::net::TcpSocket::new_v4().unwrap();
                socket.set_recv_buffer_size(4_096).unwrap();
                socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let listener = socket.listen(1).unwrap();
                let proxy_url = format!("http://{}", listener.local_addr().unwrap());
                let proxy = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = BufReader::new(stream);
                    let mut line = String::new();
                    stream.read_line(&mut line).await.unwrap();
                    assert!(line.starts_with("CONNECT localhost:"), "{line}");
                    loop {
                        line.clear();
                        if stream.read_line(&mut line).await.unwrap() <= 2 {
                            break;
                        }
                    }
                    stream
                        .get_mut()
                        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                        .await
                        .unwrap();
                    let mut upstream = TcpStream::connect(address).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut stream, &mut upstream).await;
                });
                (Matcher::builder().https(proxy_url).build(), Some(proxy))
            } else {
                (Matcher::builder().build(), None)
            };
            let transport = Arc::new(Transport::with_uploads(
                &format!("https://localhost:{}", address.port()),
                proxies,
                client,
            ));
            let start = std::time::Instant::now();
            let clock = Clock::new(move || start.elapsed(), Duration::from_millis(1));
            let key = protocol::bootstrap::Bootstrap::decode(
                br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
            ).unwrap().connection_key;
            let posting = transport.clone();
            let upload = tokio::spawn(async move {
                posting
                    .post_report(
                        "/v1/client/tool-status",
                        &key,
                        ReportRequest {
                            bytes: Bytes::from(vec![b'x'; 524_288]),
                            traceparent: None,
                            deadline: Duration::from_secs(10),
                            stall: Duration::from_secs(10),
                        },
                        &clock,
                    )
                    .await
            });
            let first = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(seen) = transport.attempts().first().copied()
                        && let Some(acknowledged) = seen.acknowledged
                        && acknowledged > 0
                    {
                        break acknowledged;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("TLS upload exposes TCP acknowledgement progress");
            read_body.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let seen = transport.attempts()[0];
                    if seen.sent && seen.acknowledged.is_some_and(|bytes| bytes > first) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("body completion advances the captured TCP acknowledgement counter");
            answer.send(()).unwrap();
            assert!(matches!(upload.await.unwrap(), Answer::Success(_)));
            server.await.unwrap();
            if let Some(proxy) = proxy {
                proxy.abort();
            }
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
            let uploads = Uploads::new(Matcher::builder().http(proxy.clone()).https(proxy).build());
            let request = Request::post(destination).body(body(b"{}")).unwrap();
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

    #[tokio::test]
    async fn a_socket_holds_what_was_written_to_it_unacknowledged_until_its_peer_takes_it() {
        // A receive buffer far smaller than what is written, so that the peer
        // acknowledges only part of it until it reads.
        let listening = tokio::net::TcpSocket::new_v4().unwrap();
        listening.set_recv_buffer_size(4_096).unwrap();
        listening.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listening.listen(1).unwrap();
        let destination = format!("http://{}", listener.local_addr().unwrap());
        let (counted, accepted) = tokio::join!(
            CountingConnector(HttpConnector::new()).call(destination.parse().unwrap()),
            listener.accept()
        );
        let mut counted = counted.unwrap();
        let (mut peer, _) = accepted.unwrap();
        let socket = counted.socket.clone();
        let mut context = Context::from_waker(Waker::noop());
        let mut written = 0;
        while let Poll::Ready(count) =
            hyper::rt::Write::poll_write(Pin::new(&mut counted), &mut context, &[0; 65_536])
        {
            written += count.unwrap();
        }
        let (acknowledged, held) = socket.state().unwrap();
        assert!(held > 0, "{written} written, {acknowledged} acknowledged");
        peer.read_exact(&mut vec![0; written]).await.unwrap();
        let start = std::time::Instant::now();
        while socket.state().is_some_and(|(_, held)| held > 0) {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "the peer's acknowledgement did not come"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(socket.state(), Some((written as u64, 0)));
        drop(counted);
        assert_eq!(socket.state(), None);
    }
}
