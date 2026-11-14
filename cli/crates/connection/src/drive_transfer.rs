//! The drive transfer client: `drive::DriveTransfer` over HTTP to the
//! configured `server_url`, with the held connection's key, the deadlines
//! of the drive transfer wire, the bounds of its answers, their validation
//! against the request, and the class of each answer. A temporary answer is
//! sent again after its delay; a rebind, a refusal under the connection's
//! rules and a connection's end wait until the drive sync holds a binding of
//! a later preparation for list and commit requests. Content requests return
//! the rebind to their capacity owners. A credential switch holds every
//! attempt back until its new key.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use drive::transfer::{
    Binding, BindingWatch, Commit, Content, ContentSink, Download, DriveTransfer, Ended,
    ListChanges, ListFiles, Request, Transfer, Upload, answer,
};
use http::HeaderMap;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use protocol::bootstrap::BootstrapKey;
use protocol::drive_transfer::{
    CommitResponse, DownloadContentRequestHeaders, ListChangesResponse, ListFilesResponse,
    PartialContentResponseHeaders, RangeHeader, RateLimitedResponseHeaders, UploadContentResponse,
    limits,
};
use protocol::errors::{DriveTransferEndpoint, ErrorBody, ErrorCode, decode_drive_transfer_error};
use protocol::id::Id;
use reqwest::Url;
use reqwest::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE, RETRY_AFTER,
};
use state::clock::Clock;
use tokio::sync::{mpsc, watch};

use crate::backoff::{BASE_MAX, BASE_START, Backoff, random_jitter};
use crate::http::{DriveBody, Failure, Transport};
use crate::outbox::OutboxEvent;

/// The time an attempt may go without a byte written or read, from its
/// start: connecting and TLS, and waiting for the headers, included.
pub const STALL: Duration = Duration::from_secs(10);

/// The bytes read of a `2xx` body, by endpoint, and of an error body.
pub const UPLOAD_ANSWER_MAX_BYTES: usize = 65_536;
pub const ERROR_MAX_BYTES: usize = 16_777_216;

pub use drive::transfer::{SCALE_BYTES, TOTAL_PER_SCALE, total};

/// What drive transfer requests take from the Connection owner: the held
/// connection, if any, its key and whether a credential switch holds
/// attempts back. `key_number` counts each new key.
#[derive(Debug, Clone)]
pub struct Link {
    connection: Option<Id>,
    key: BootstrapKey,
    key_number: u64,
    switching: bool,
}

/// The Connection owner's side of [`Link`].
#[derive(Debug)]
pub struct DriveLink(watch::Sender<Link>);

impl DriveLink {
    /// A link with no connection yet, whose first connection uses `key`.
    pub fn new(key: BootstrapKey) -> (DriveLink, watch::Receiver<Link>) {
        let (sender, receiver) = watch::channel(Link {
            connection: None,
            key,
            key_number: 0,
            switching: false,
        });
        (DriveLink(sender), receiver)
    }

    pub fn connection_started(&self, connection: Id, key: BootstrapKey) {
        self.0.send_modify(|link| {
            if link.key.expose() != key.expose() {
                link.key = key;
                link.key_number += 1;
            }
            link.connection = Some(connection);
            link.switching = false;
        });
    }

    /// No attempt starts until the switch ends or the connection does.
    pub fn switch_started(&self, connection: &Id) {
        self.0.send_modify(|link| {
            if link.connection.as_ref() == Some(connection) {
                link.switching = true;
            }
        });
    }

    pub fn switch_ended(&self, connection: &Id, key: BootstrapKey) {
        self.0.send_modify(|link| {
            if link.connection.as_ref() == Some(connection) {
                link.key = key;
                link.key_number += 1;
                link.switching = false;
            }
        });
    }

    pub fn connection_ended(&self, connection: &Id) {
        self.0.send_modify(|link| {
            if link.connection.as_ref() == Some(connection) {
                link.connection = None;
                link.switching = false;
            }
        });
    }
}

/// The HTTP implementation of `drive::DriveTransfer`.
pub struct HttpDriveTransfer {
    transport: Arc<Transport>,
    link: watch::Receiver<Link>,
    events: mpsc::UnboundedSender<OutboxEvent>,
    clock: Clock,
}

impl std::fmt::Debug for HttpDriveTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("drive transfer client")
    }
}

impl HttpDriveTransfer {
    /// A client of `server_url` that takes its key from `link`, waits on
    /// `clock` and tells a refusal under the connection's rules as an
    /// [`OutboxEvent::Refused`] on `events`.
    pub fn new(
        server_url: &str,
        link: watch::Receiver<Link>,
        events: mpsc::UnboundedSender<OutboxEvent>,
        clock: Clock,
    ) -> HttpDriveTransfer {
        Self::with_transport(Arc::new(Transport::new(server_url)), link, events, clock)
    }

    pub(crate) fn with_transport(
        transport: Arc<Transport>,
        link: watch::Receiver<Link>,
        events: mpsc::UnboundedSender<OutboxEvent>,
        clock: Clock,
    ) -> HttpDriveTransfer {
        HttpDriveTransfer {
            transport,
            link,
            events,
            clock,
        }
    }
}

/// The parts of an attempt's answer that the client reads.
struct Raw {
    status: u16,
    headers: HeaderMap,
    /// The body of an error answer, or of a `2xx` that is not streamed;
    /// `None` when it passed its bound.
    body: Option<Vec<u8>>,
}

/// What one attempt is.
struct Attempt {
    method: http::Method,
    url: Url,
    headers: Vec<(http::HeaderName, String)>,
    body: DriveBody,
    /// The request's bytes, for the total of a PUT or a commit.
    total_bytes: Option<u64>,
}

/// Where a `2xx` body goes.
enum Into2xx<'a> {
    Memory(usize),
    Sink {
        sink: &'a mut dyn ContentSink,
        size: u64,
    },
}

/// What a `2xx` download answer said before its body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Streamed {
    Whole,
    Rest,
    /// A range other than the one asked for, or a size other than the
    /// content's.
    Unrelated,
}

impl HttpDriveTransfer {
    fn url(&self, drive_id: &Id, tail: &str, query: &[(&str, &str)]) -> Url {
        let mut url = self
            .transport
            .url(&format!("/v1/client/drives/{}/{tail}", drive_id.as_str()));
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        url
    }

    /// Sends attempts until an answer the caller acts on. `build` makes each
    /// attempt with the binding it carries and the bytes the sink holds.
    async fn exchange(
        &self,
        endpoint: DriveTransferEndpoint,
        binding: &mut BindingWatch,
        mut into: Into2xx<'_>,
        rate_limited: Option<&(dyn Fn() + Send + Sync)>,
        build: &(dyn Fn(&Binding, u64) -> std::io::Result<Attempt> + Send + Sync),
    ) -> Result<(Raw, Binding), Ended> {
        let content = matches!(
            endpoint,
            DriveTransferEndpoint::UploadContent | DriveTransferEndpoint::DownloadContent
        );
        let mut backoff = Backoff::new(BASE_START, BASE_MAX, random_jitter);
        let mut link = self.link.clone();
        loop {
            let held = loop {
                let held = binding.borrow_and_update().clone();
                if let Some(held) = held {
                    break held;
                }
                if content {
                    return Err(Ended::Rebind(None));
                }
                rebound(binding).await?;
            };
            let current = link.borrow_and_update().clone();
            if current.connection.as_ref() != Some(&held.connection_id) {
                // The binding's connection ended: a rebind.
                if content {
                    return Err(Ended::Rebind(Some(held)));
                }
                backoff.reset();
                tokio::select! {
                    rebound = rebound(binding) => rebound?,
                    () = wait(link.changed()) => {}
                }
                continue;
            }
            if current.switching {
                wait(link.changed()).await;
                continue;
            }
            let received = match &into {
                Into2xx::Sink { sink, .. } => sink.received(),
                Into2xx::Memory(_) => 0,
            };
            let attempt = build(&held, received).map_err(Ended::Unreadable)?;
            let answer = tokio::select! {
                answer = self.attempt(attempt, &current.key, &mut into) => Some(answer),
                () = ended(&mut link, &held.connection_id) => None,
            };
            let Some(answer) = answer else {
                if content {
                    return Err(Ended::Rebind(Some(held)));
                }
                backoff.reset();
                rebound(binding).await?;
                continue;
            };
            let raw = match answer {
                Ok(raw) => raw,
                Err(Failure::Network | Failure::Timeout | Failure::Server) => {
                    self.retry_wait(backoff.next_wait(), &mut link, &held, content)
                        .await?;
                    continue;
                }
                Err(Failure::Redirect | Failure::Unreadable) => {
                    return Err(self.rejected(endpoint, 0, None));
                }
                Err(Failure::Sink(kind)) => return Err(Ended::Sink(kind.into())),
            };
            if (200..300).contains(&raw.status) {
                if raw.body.is_none() && matches!(into, Into2xx::Memory(_)) {
                    return Err(self.rejected(endpoint, raw.status, None));
                }
                return Ok((raw, held));
            }
            if matches!(raw.status, 500 | 502 | 503 | 504) {
                self.retry_wait(backoff.next_wait(), &mut link, &held, content)
                    .await?;
                continue;
            }
            let error = raw
                .body
                .as_deref()
                .and_then(|body| decode_drive_transfer_error(endpoint, raw.status, body).ok());
            let Some(error) = error else {
                return Err(self.rejected(endpoint, raw.status, None));
            };
            backoff.reset();
            match error.code {
                ErrorCode::RateLimited => {
                    let retry_after = raw
                        .headers
                        .get(RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| {
                            protocol::decode::<RateLimitedResponseHeaders>(
                                &serde_json::to_vec(&serde_json::json!({ "Retry-After": value }))
                                    .expect("a header value encodes"),
                            )
                            .ok()
                        });
                    let Some(retry_after) = retry_after else {
                        return Err(self.rejected(endpoint, raw.status, Some(error.code)));
                    };
                    if let Some(rate_limited) = rate_limited {
                        rate_limited();
                    }
                    let seconds: u64 = retry_after
                        .retry_after
                        .as_str()
                        .parse()
                        .expect("a Retry-After holds whole seconds");
                    self.retry_wait(Duration::from_secs(seconds), &mut link, &held, content)
                        .await?;
                }
                ErrorCode::StaleConnection => {
                    self.refused(&held, error);
                    if content {
                        return Err(Ended::Rebind(Some(held)));
                    }
                    rebound(binding).await?;
                }
                ErrorCode::StaleBinding => {
                    if content {
                        return Err(Ended::Rebind(Some(held)));
                    }
                    rebound(binding).await?;
                }
                ErrorCode::CredentialRevoked
                    if {
                        let now = link.borrow();
                        now.switching || current.key_number < now.key_number
                    } =>
                {
                    // It waits for the switch's key, or goes again with the
                    // key that replaced its own.
                }
                code if code.status() == 401 || code == ErrorCode::InstanceUnavailable => {
                    self.refused(&held, error);
                    if content {
                        return Err(Ended::Rebind(Some(held)));
                    }
                    rebound(binding).await?;
                }
                ErrorCode::ContentHashMismatch => return Err(Ended::ReRead),
                ErrorCode::DriveCursorExpired => return Err(Ended::CursorExpired),
                ErrorCode::ContentMissing | ErrorCode::FileTooLarge => {
                    return Err(Ended::Decision(error));
                }
                ErrorCode::DriveAccessDenied => {
                    return Err(Ended::Permanent {
                        status: raw.status,
                        code: Some(error.code.as_str()),
                    });
                }
                code => return Err(self.rejected(endpoint, raw.status, Some(code))),
            }
        }
    }

    /// One attempt: its answer's status, headers and body, the `2xx` body
    /// into `into`.
    async fn attempt(
        &self,
        attempt: Attempt,
        key: &BootstrapKey,
        into: &mut Into2xx<'_>,
    ) -> Result<Raw, Failure> {
        let mut request = http::Request::builder()
            .method(attempt.method)
            .uri(attempt.url.as_str())
            .header(AUTHORIZATION, format!("Bearer {}", key.expose()));
        for (name, value) in attempt.headers {
            request = request.header(name, value);
        }
        let request = request.body(attempt.body).map_err(|_| Failure::Network)?;
        let deadline = attempt
            .total_bytes
            .map(|bytes| self.clock.now() + total(bytes));
        self.transport
            .drive(
                request,
                deadline,
                STALL,
                &self.clock,
                |response, received| read(response, received, into),
            )
            .await
    }

    async fn retry_wait(
        &self,
        duration: Duration,
        link: &mut watch::Receiver<Link>,
        held: &Binding,
        content: bool,
    ) -> Result<(), Ended> {
        let sleep = self.clock.sleep_until(self.clock.now() + duration);
        if content {
            tokio::select! {
                () = sleep => {},
                () = ended(link, &held.connection_id) => return Err(Ended::Rebind(Some(held.clone()))),
            }
        } else {
            sleep.await;
        }
        Ok(())
    }

    /// A refusal under the connection's rules goes to the Connection owner.
    fn refused(&self, held: &Binding, error: ErrorBody) {
        let _ = self.events.send(OutboxEvent::Refused {
            connection: held.connection_id.clone(),
            error,
        });
    }

    /// A permanent answer, recorded with its operation and status.
    fn rejected(
        &self,
        endpoint: DriveTransferEndpoint,
        status: u16,
        code: Option<ErrorCode>,
    ) -> Ended {
        let code_text = code.map_or("invalid_response", ErrorCode::as_str);
        // Upload/commit outcomes are handled and recorded once by DriveSync,
        // with the pending-change count and its Session/drive correlation.
        if !matches!(
            endpoint,
            DriveTransferEndpoint::UploadContent | DriveTransferEndpoint::Commit
        ) {
            observe::error!(
                "drive_transfer.answer_rejected",
                drive_transfer.operation = observe::Id(operation(endpoint)),
                http.response.status_code = i64::from(status),
                error.code = observe::Code(code_text)
            );
        }
        Ended::Permanent {
            status,
            code: code.map(ErrorCode::as_str),
        }
    }
}

fn operation(endpoint: DriveTransferEndpoint) -> &'static str {
    match endpoint {
        DriveTransferEndpoint::ListFiles => "list_files",
        DriveTransferEndpoint::ListChanges => "list_changes",
        DriveTransferEndpoint::UploadContent => "upload_content",
        DriveTransferEndpoint::Commit => "commit",
        DriveTransferEndpoint::DownloadContent => "download_content",
    }
}

/// Waits for a change of the binding; once its drive sync has dropped the
/// sender, the request ends.
async fn rebound(binding: &mut BindingWatch) -> Result<(), Ended> {
    binding.changed().await.map_err(|_| Ended::Permanent {
        status: 0,
        code: None,
    })
}

/// Waits for a change of the link, which closes only at shutdown; a closed
/// channel never changes again.
async fn wait<E>(changed: impl std::future::Future<Output = Result<(), E>>) {
    if changed.await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Ends once `link` holds no longer `connection`.
async fn ended(link: &mut watch::Receiver<Link>, connection: &Id) {
    loop {
        if link.borrow_and_update().connection.as_ref() != Some(connection) {
            return;
        }
        wait(link.changed()).await;
    }
}

/// Reads an answer: a `2xx` body into `into`, an error body up to
/// [`ERROR_MAX_BYTES`], each byte counted in `received`.
async fn read(
    response: http::Response<Incoming>,
    received: Arc<AtomicU64>,
    into: &mut Into2xx<'_>,
) -> Result<Raw, Failure> {
    let (parts, mut body) = response.into_parts();
    let status = parts.status.as_u16();
    let success = parts.status.is_success();
    if parts.status.is_server_error() {
        return Ok(Raw {
            status,
            headers: parts.headers,
            body: None,
        });
    }
    if success && let Into2xx::Sink { sink, size } = into {
        let streamed = streamed(status, &parts.headers, sink.received(), *size);
        if streamed == Streamed::Unrelated {
            return Ok(Raw {
                status,
                headers: parts.headers,
                body: None,
            });
        }
        if streamed == Streamed::Whole {
            sink.restart()
                .map_err(|error| Failure::Sink(error.kind()))?;
        }
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| Failure::Network)?;
            if let Ok(chunk) = frame.into_data() {
                received.fetch_add(chunk.len() as u64, Ordering::AcqRel);
                sink.append(&chunk)
                    .map_err(|error| Failure::Sink(error.kind()))?;
            }
        }
        return Ok(Raw {
            status,
            headers: parts.headers,
            body: Some(Vec::new()),
        });
    }
    let limit = match into {
        Into2xx::Memory(limit) if success => *limit,
        _ => ERROR_MAX_BYTES,
    };
    let mut bytes = Vec::new();
    let mut over = false;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| Failure::Network)?;
        if let Ok(chunk) = frame.into_data() {
            received.fetch_add(chunk.len() as u64, Ordering::AcqRel);
            if bytes.len() + chunk.len() > limit {
                over = true;
                break;
            }
            bytes.extend_from_slice(&chunk);
        }
    }
    Ok(Raw {
        status,
        headers: parts.headers,
        body: (!over).then_some(bytes),
    })
}

/// What a `2xx` download answer continues: the whole content from byte 0
/// on `200` with the full size, the rest on a `206` whose range starts at
/// `received` and names the full size.
fn streamed(status: u16, headers: &HeaderMap, received: u64, size: u64) -> Streamed {
    let header = |name| {
        headers
            .get(name)
            .and_then(|value: &http::HeaderValue| value.to_str().ok())
    };
    let length = header(CONTENT_LENGTH).and_then(|value| value.parse::<u64>().ok());
    match status {
        200 if length.is_none_or(|length| length == size) => Streamed::Whole,
        206 => {
            let Some(range) = header(CONTENT_RANGE) else {
                return Streamed::Unrelated;
            };
            let request = DownloadContentRequestHeaders {
                range: RangeHeader::new(format!("bytes={received}-")).ok(),
            };
            let parsed = protocol::decode_answer::<PartialContentResponseHeaders, _>(
                &serde_json::to_vec(&serde_json::json!({ "Content-Range": range }))
                    .expect("a header value encodes"),
                &request,
            );
            match parsed {
                Ok(answer) => {
                    let (first, _, whole) = answer.bounds();
                    if first as u64 == received && whole as u64 == size {
                        Streamed::Rest
                    } else {
                        Streamed::Unrelated
                    }
                }
                Err(_) => Streamed::Unrelated,
            }
        }
        _ => Streamed::Unrelated,
    }
}

fn common(binding: &Binding) -> [(&'static str, String); 3] {
    [
        ("protocol_version", "1".to_owned()),
        ("connection_id", binding.connection_id.as_str().to_owned()),
        ("binding_id", binding.binding_id.as_str().to_owned()),
    ]
}

impl DriveTransfer for HttpDriveTransfer {
    fn list_files(&self, request: Request<ListFiles>) -> Transfer<'_, ListFilesResponse> {
        Box::pin(async move {
            let Request {
                drive_id,
                mut binding,
                body,
                ..
            } = request;
            let build = |held: &Binding, _: u64| {
                let mut query = common(held).to_vec();
                if let Some(after) = &body.after_path {
                    query.push(("after_path", after.as_str().to_owned()));
                }
                let pairs: Vec<(&str, &str)> = query
                    .iter()
                    .map(|(name, value)| (*name, value.as_str()))
                    .collect();
                Ok(Attempt {
                    method: http::Method::GET,
                    url: self.url(&drive_id, "files", &pairs),
                    headers: Vec::new(),
                    body: DriveBody::Empty,
                    total_bytes: None,
                })
            };
            let endpoint = DriveTransferEndpoint::ListFiles;
            let (raw, held) = self
                .exchange(
                    endpoint,
                    &mut binding,
                    Into2xx::Memory(limits::LIST_PAGE_MAX_JSON_BYTES),
                    None,
                    &build,
                )
                .await?;
            self.checked(endpoint, &raw, |bytes| {
                answer::list_files(&held, &body, bytes).ok()
            })
        })
    }

    fn list_changes(&self, request: Request<ListChanges>) -> Transfer<'_, ListChangesResponse> {
        Box::pin(async move {
            let Request {
                drive_id,
                mut binding,
                body,
                ..
            } = request;
            let build = |held: &Binding, _: u64| {
                let mut query = common(held).to_vec();
                query.push(("after_seq", body.after_seq.to_string()));
                if let Some(index) = body.after_index {
                    query.push(("after_index", index.to_string()));
                }
                let pairs: Vec<(&str, &str)> = query
                    .iter()
                    .map(|(name, value)| (*name, value.as_str()))
                    .collect();
                Ok(Attempt {
                    method: http::Method::GET,
                    url: self.url(&drive_id, "changes", &pairs),
                    headers: Vec::new(),
                    body: DriveBody::Empty,
                    total_bytes: None,
                })
            };
            let endpoint = DriveTransferEndpoint::ListChanges;
            let (raw, held) = self
                .exchange(
                    endpoint,
                    &mut binding,
                    Into2xx::Memory(limits::LIST_PAGE_MAX_JSON_BYTES),
                    None,
                    &build,
                )
                .await?;
            self.checked(endpoint, &raw, |bytes| {
                answer::list_changes(&held, &body, bytes)?.ok()
            })
        })
    }

    fn upload_content(&self, request: Request<Upload>) -> Transfer<'_, UploadContentResponse> {
        Box::pin(async move {
            let Request {
                drive_id,
                mut binding,
                traceparent,
                body,
            } = request;
            let size = body.content.size();
            let tail = format!("content/{}", body.sha256.as_str());
            let content = body.content.clone();
            let build = |held: &Binding, _: u64| {
                let common = common(held);
                let pairs: Vec<(&str, &str)> = common
                    .iter()
                    .map(|(name, value)| (*name, value.as_str()))
                    .collect();
                let body = match &content {
                    Content::Bytes(bytes) => DriveBody::Bytes(Some(bytes.clone())),
                    Content::File { path, size } => DriveBody::file(
                        tokio::fs::File::from_std(std::fs::File::open(path)?),
                        *size,
                    ),
                };
                let mut headers = vec![
                    (CONTENT_TYPE, "application/octet-stream".to_owned()),
                    (CONTENT_LENGTH, size.to_string()),
                ];
                if let Some(traceparent) = &traceparent {
                    headers.push((
                        http::HeaderName::from_static("traceparent"),
                        traceparent.clone(),
                    ));
                }
                Ok(Attempt {
                    method: http::Method::PUT,
                    url: self.url(&drive_id, &tail, &pairs),
                    headers,
                    body,
                    total_bytes: Some(size),
                })
            };
            let endpoint = DriveTransferEndpoint::UploadContent;
            let rate_limited = body.rate_limited.clone();
            let (raw, _) = self
                .exchange(
                    endpoint,
                    &mut binding,
                    Into2xx::Memory(UPLOAD_ANSWER_MAX_BYTES),
                    rate_limited.as_deref(),
                    &build,
                )
                .await?;
            self.checked(endpoint, &raw, |bytes| {
                answer::upload(&drive_id, &body.sha256, size, bytes).ok()
            })
        })
    }

    fn commit(&self, request: Request<Commit>) -> Transfer<'_, CommitResponse> {
        Box::pin(async move {
            let Request {
                drive_id,
                mut binding,
                traceparent,
                body,
            } = request;
            let encoded = |held: &Binding| answer::commit_request(held, &body.changes);
            let build = |held: &Binding, _: u64| {
                let bytes = Bytes::from(protocol::encode(&encoded(held)));
                let mut headers = vec![(CONTENT_TYPE, "application/json".to_owned())];
                if let Some(traceparent) = &traceparent {
                    headers.push((
                        http::HeaderName::from_static("traceparent"),
                        traceparent.clone(),
                    ));
                }
                Ok(Attempt {
                    method: http::Method::POST,
                    url: self.url(&drive_id, "commits", &[]),
                    headers,
                    total_bytes: Some(bytes.len() as u64),
                    body: DriveBody::Bytes(Some(bytes)),
                })
            };
            let endpoint = DriveTransferEndpoint::Commit;
            let (raw, held) = self
                .exchange(
                    endpoint,
                    &mut binding,
                    Into2xx::Memory(limits::COMMIT_RESPONSE_MAX_JSON_BYTES),
                    None,
                    &build,
                )
                .await?;
            let sent = encoded(&held);
            self.checked(endpoint, &raw, |bytes| answer::commit(&sent, bytes).ok())
        })
    }

    fn download_content<'a>(
        &'a self,
        request: Request<Download>,
        sink: &'a mut dyn ContentSink,
    ) -> Transfer<'a, ()> {
        Box::pin(async move {
            let Request {
                drive_id,
                mut binding,
                body,
                ..
            } = request;
            let tail = format!("content/{}", body.sha256.as_str());
            let size = body.size;
            let build = |held: &Binding, received: u64| {
                let common = common(held);
                let pairs: Vec<(&str, &str)> = common
                    .iter()
                    .map(|(name, value)| (*name, value.as_str()))
                    .collect();
                let mut headers = Vec::new();
                if 0 < received && received < size {
                    headers.push((RANGE, format!("bytes={received}-")));
                }
                Ok(Attempt {
                    method: http::Method::GET,
                    url: self.url(&drive_id, &tail, &pairs),
                    headers,
                    body: DriveBody::Empty,
                    total_bytes: None,
                })
            };
            let endpoint = DriveTransferEndpoint::DownloadContent;
            let (raw, _) = self
                .exchange(
                    endpoint,
                    &mut binding,
                    Into2xx::Sink { sink, size },
                    None,
                    &build,
                )
                .await?;
            if raw.body.is_none() {
                return Err(self.rejected(endpoint, raw.status, None));
            }
            Ok(())
        })
    }
}

impl HttpDriveTransfer {
    /// A `2xx` body decoded and checked against its request, or a
    /// permanent answer.
    fn checked<T>(
        &self,
        endpoint: DriveTransferEndpoint,
        raw: &Raw,
        decode: impl FnOnce(&[u8]) -> Option<T>,
    ) -> Result<T, Ended> {
        raw.body
            .as_deref()
            .and_then(decode)
            .ok_or_else(|| self.rejected(endpoint, raw.status, None))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use protocol::field::Sha256;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn put_and_commit_attempts_end_at_the_total_while_bytes_keep_moving() {
        put_and_commit_deadline(false).await;
    }

    #[tokio::test]
    async fn put_and_commit_attempts_end_ten_seconds_after_the_last_progress() {
        put_and_commit_deadline(true).await;
    }

    async fn put_and_commit_deadline(stalled: bool) {
        use std::future::Future;
        async fn consume(stream: &mut tokio::net::TcpStream) {
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(stream.read_u8().await.unwrap());
            }
            let head = String::from_utf8(head).unwrap();
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            stream.read_exact(&mut vec![0; length]).await.unwrap();
        }
        for upload in [true, false] {
            for completes in [true, false] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                let calls = Arc::new(AtomicU64::new(0));
                let seen_calls = calls.clone();
                let hash = "a".repeat(64);
                let content = Bytes::from(vec![b'x'; if stalled { 131_072 } else { 2 }]);
                let changes: Vec<_> = (0..if stalled { 1000 } else { 1 })
                    .map(|index| protocol::drive_transfer::CommitChange::Delete {
                        path: format!("file{index:04}"),
                        base_sha256: Sha256::new(hash.clone()).unwrap(),
                    })
                    .collect();
                let response = if upload {
                    format!(
                        r#"{{"content_sha256":"{hash}","size_bytes":{}}}"#,
                        content.len()
                    )
                } else {
                    serde_json::json!({"seq":1,"results":vec![
                        serde_json::json!({"status":"accepted","version_id":"v_a"}); changes.len()
                    ]})
                    .to_string()
                };
                let last_alive = if stalled { 16_999 } else { 9_999 };
                let deadline = last_alive + 1;
                if stalled {
                    let size = if upload {
                        content.len()
                    } else {
                        serde_json::to_vec(&serde_json::json!({"changes":changes}))
                            .unwrap()
                            .len()
                    };
                    assert!(total(size as u64) > Duration::from_millis(deadline));
                }
                let (answer_first, answer) = tokio::sync::oneshot::channel::<()>();
                let mut answer_first = Some(answer_first);
                let server = tokio::spawn(async move {
                    let (mut first, _) = listener.accept().await.unwrap();
                    consume(&mut first).await;
                    seen_calls.fetch_add(1, Ordering::SeqCst);
                    let mut second;
                    let socket = tokio::select! {
                        _ = answer => &mut first,
                        accepted = listener.accept() => {
                            second = accepted.unwrap().0;
                            consume(&mut second).await;
                            seen_calls.fetch_add(1, Ordering::SeqCst);
                            &mut second
                        }
                    };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        response.len()
                    );
                    socket.write_all(head.as_bytes()).await.unwrap();
                    socket.write_all(response.as_bytes()).await.unwrap();
                });
                let tick = Arc::new(AtomicU64::new(0));
                let now = tick.clone();
                let sampled = Arc::new(AtomicU64::new(u64::MAX));
                let seen = sampled.clone();
                let clock = Clock::new(
                    move || Duration::from_millis(now.load(Ordering::SeqCst)),
                    Duration::from_millis(1),
                );
                let mut transport = Transport::new(&url);
                transport.progress_sample = Some(Arc::new(move |now| {
                    seen.store(now.as_millis() as u64, Ordering::SeqCst);
                    (
                        if stalled {
                            if now >= Duration::from_secs(7) { 2 } else { 1 }
                        } else {
                            now.as_millis() as u64 + 1
                        },
                        1,
                        false,
                    )
                }));
                let key = protocol::bootstrap::Bootstrap::decode(br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#).unwrap().connection_key;
                let (owner, link) = DriveLink::new(key.clone());
                owner.connection_started(Id::new("conn_a").unwrap(), key);
                let (events, _) = mpsc::unbounded_channel();
                let client =
                    HttpDriveTransfer::with_transport(Arc::new(transport), link, events, clock);
                let binding = watch::Sender::new(Some(Binding {
                    connection_id: Id::new("conn_a").unwrap(),
                    binding_id: Id::new("bind_a").unwrap(),
                }));
                let mut sending = Box::pin(async {
                    if upload {
                        client
                            .upload_content(Request {
                                drive_id: Id::new("drive_a").unwrap(),
                                binding: binding.subscribe(),
                                traceparent: None,
                                body: Upload {
                                    sha256: Sha256::new(hash.clone()).unwrap(),
                                    content: Content::Bytes(content.clone()),
                                    rate_limited: None,
                                },
                            })
                            .await
                            .map(|_| ())
                    } else {
                        client
                            .commit(Request {
                                drive_id: Id::new("drive_a").unwrap(),
                                binding: binding.subscribe(),
                                traceparent: None,
                                body: Commit {
                                    changes: changes.clone(),
                                },
                            })
                            .await
                            .map(|_| ())
                    }
                });
                // Poll the real caller in this task so each observed time also
                // includes its timeout/retry transition before advancing again.
                for millis in [0, 7000, last_alive] {
                    tick.store(millis, Ordering::SeqCst);
                    tokio::time::timeout(Duration::from_secs(2), async {
                        loop {
                            std::future::poll_fn(|cx| {
                                assert!(sending.as_mut().poll(cx).is_pending());
                                std::task::Poll::Ready(())
                            })
                            .await;
                            if sampled.load(Ordering::SeqCst) == millis
                                && calls.load(Ordering::SeqCst) == 1
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                }
                if completes {
                    answer_first.take().unwrap().send(()).unwrap();
                } else {
                    tick.store(deadline, Ordering::SeqCst);
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while sampled.load(Ordering::SeqCst) != deadline {
                            std::future::poll_fn(|cx| {
                                assert!(sending.as_mut().poll(cx).is_pending());
                                std::task::Poll::Ready(())
                            })
                            .await;
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    tick.store(deadline + 1200, Ordering::SeqCst);
                    // Keep the first answer held while the timed-out attempt retries.
                }
                tokio::time::timeout(Duration::from_secs(2), sending)
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "upload={upload} completes={completes} calls={} sample={}",
                            calls.load(Ordering::SeqCst),
                            sampled.load(Ordering::SeqCst)
                        )
                    })
                    .unwrap();
                server.await.unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), if completes { 1 } else { 2 });
            }
        }
    }

    #[tokio::test]
    async fn a_retry_wait_does_not_complete_before_its_clock_deadline() {
        use std::future::Future;
        for content in [false, true] {
            let nanos = Arc::new(AtomicU64::new(0));
            let now = nanos.clone();
            let clock = Clock::new(
                move || Duration::from_nanos(now.load(Ordering::SeqCst)),
                Duration::from_millis(1),
            );
            let key = protocol::bootstrap::Bootstrap::decode(
                br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
            ).unwrap().connection_key;
            let (owner, mut link) = DriveLink::new(key.clone());
            let held = Binding {
                connection_id: Id::new("conn_a").unwrap(),
                binding_id: Id::new("bind_a").unwrap(),
            };
            owner.connection_started(held.connection_id.clone(), key);
            let (events, _received) = mpsc::unbounded_channel();
            let client = HttpDriveTransfer::new("http://127.0.0.1:1", link.clone(), events, clock);
            let mut wait =
                Box::pin(client.retry_wait(Duration::from_secs(3), &mut link, &held, content));
            std::future::poll_fn(|cx| {
                assert!(wait.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            nanos.store(2_999_000_000, Ordering::SeqCst);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut wait)
                    .await
                    .is_err()
            );
            nanos.store(3_000_000_000, Ordering::SeqCst);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), wait)
                    .await
                    .unwrap()
                    .is_ok()
            );
        }
    }

    #[tokio::test]
    async fn a_put_whose_bytes_keep_moving_past_ten_seconds_completes_within_its_total() {
        // Far more than the loopback socket buffers hold, read 512 KiB in
        // each second of the clock: only the bytes the client writes move.
        const SIZE: usize = 32 << 20;
        const READ: usize = 512 << 10;
        let listening = tokio::net::TcpSocket::new_v4().unwrap();
        listening.set_recv_buffer_size(4_096).unwrap();
        listening.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listening.listen(1).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let sha256 = "a".repeat(64);
        let again = Arc::new(AtomicU64::new(0));
        let counting = again.clone();
        let nanos = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let clock = {
            let (nanos, reads) = (nanos.clone(), reads.clone());
            Clock::new(
                move || {
                    reads.fetch_add(1, Ordering::SeqCst);
                    Duration::from_nanos(nanos.load(Ordering::SeqCst))
                },
                Duration::from_millis(1),
            )
        };
        let transport = Arc::new(Transport::new(&url));
        let key = protocol::bootstrap::Bootstrap::decode(
            br#"{"server_url":"http://127.0.0.1:1","instance_id":"inst_a","credential_id":"cred_a","connection_key":"tetral_ck_a","expires_at":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap()
        .connection_key;
        let (link, links) = DriveLink::new(key.clone());
        link.connection_started(Id::new("conn_a").unwrap(), key);
        let (events, _refusals) = mpsc::unbounded_channel();
        let client = Arc::new(HttpDriveTransfer::with_transport(
            transport, links, events, clock,
        ));
        let binding = watch::Sender::new(Some(Binding {
            connection_id: Id::new("conn_a").unwrap(),
            binding_id: Id::new("bind_a").unwrap(),
        }));
        let request = Request {
            drive_id: Id::new("drive_a").unwrap(),
            binding: binding.subscribe(),
            traceparent: None,
            body: Upload {
                sha256: Sha256::new(sha256.clone()).unwrap(),
                content: Content::Bytes(Bytes::from(vec![b'x'; SIZE])),
                rate_limited: None,
            },
        };
        assert!(total(SIZE as u64) > Duration::from_secs(25));
        let sending = client.clone();
        let sent = tokio::spawn(async move { sending.upload_content(request).await });
        let (mut socket, _) = listener.accept().await.unwrap();
        // A second connection is an attempt sent again: the first failed.
        tokio::spawn(async move {
            while listener.accept().await.is_ok() {
                counting.fetch_add(1, Ordering::SeqCst);
            }
        });
        let mut read = vec![0; SIZE + 4_096];
        let mut taken = 0;
        for second in 1..=25 {
            socket
                .read_exact(&mut read[taken..taken + READ])
                .await
                .unwrap();
            taken += READ;
            let start = reads.load(Ordering::SeqCst);
            nanos.store(second * 1_000_000_000, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(10);
            while reads.load(Ordering::SeqCst) < start + 6 {
                assert!(Instant::now() < deadline, "the attempt reads its clock");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert!(!sent.is_finished(), "ended at {second} s");
            assert_eq!(again.load(Ordering::SeqCst), 0, "sent again by {second} s");
        }
        let body_at = loop {
            if let Some(end) = read[..taken]
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                && taken >= end + 4 + SIZE
            {
                break end + 4;
            }
            taken += socket.read(&mut read[taken..]).await.unwrap();
        };
        assert_eq!(taken, body_at + SIZE);
        let body = format!(r#"{{"content_sha256":"{sha256}","size_bytes":{SIZE}}}"#);
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
        let uploaded = sent.await.unwrap().unwrap();
        assert_eq!(uploaded.size_bytes.get(), SIZE as i64);
    }
}
