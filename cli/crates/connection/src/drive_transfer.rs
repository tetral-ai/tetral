//! The drive transfer client: `drive::DriveTransfer` over HTTP to the
//! configured `server_url`, with the held connection's key, the deadlines
//! of the drive transfer wire, the bounds of its answers, their validation
//! against the request, and the class of each answer. A temporary answer is
//! sent again after its delay; a rebind, a refusal under the connection's
//! rules and a connection's end wait until the drive sync holds a binding of
//! a later preparation; a credential switch holds every attempt back until
//! its new key. Only the other classes reach the caller.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use drive::transfer::{
    Binding, BindingWatch, Commit, Content, ContentSink, Download, DriveTransfer, Ended,
    ListChanges, ListFiles, Request, Transfer, Upload,
};
use http::HeaderMap;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use protocol::bootstrap::BootstrapKey;
use protocol::connection::QueryProtocolVersion;
use protocol::drive_transfer::{
    CommitRequest, CommitResponse, ContentPathParams, Decimal, DownloadContentRequestHeaders,
    ListChangesQuery, ListChangesResponse, ListFilesQuery, ListFilesResponse,
    PartialContentResponseHeaders, RangeHeader, RateLimitedResponseHeaders,
    UploadContentRequestHeaders, UploadContentResponse, UploadRequest, limits,
};
use protocol::errors::{DriveTransferEndpoint, ErrorBody, ErrorCode, decode_drive_transfer_error};
use protocol::field::{Fixed, List};
use protocol::id::Id;
use protocol::tool::ProtocolVersion;
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

/// The total of a PUT or a commit for each started share of this many
/// request bytes.
pub const SCALE_BYTES: u64 = 65_536;
pub const TOTAL_PER_SCALE: Duration = Duration::from_secs(10);

/// The bytes read of a `2xx` body, by endpoint, and of an error body.
pub const UPLOAD_ANSWER_MAX_BYTES: usize = 65_536;
pub const ERROR_MAX_BYTES: usize = 16_777_216;

/// The total of a PUT or a commit of `request_bytes`:
/// 10 s × max(1, ceil(request_bytes / 65,536)).
pub fn total(request_bytes: u64) -> Duration {
    let shares = request_bytes.div_ceil(SCALE_BYTES).max(1);
    TOTAL_PER_SCALE * u32::try_from(shares).unwrap_or(u32::MAX)
}

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
        let mut backoff = Backoff::new(BASE_START, BASE_MAX, random_jitter);
        let mut link = self.link.clone();
        loop {
            let held = loop {
                let held = binding.borrow_and_update().clone();
                if let Some(held) = held {
                    break held;
                }
                wait(binding.changed()).await;
            };
            let current = link.borrow_and_update().clone();
            if current.connection.as_ref() != Some(&held.connection_id) {
                // The binding's connection ended: a rebind.
                backoff.reset();
                tokio::select! {
                    () = wait(binding.changed()) => {}
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
            let attempt = build(&held, received).map_err(|_| Ended::Permanent {
                status: 0,
                code: None,
            })?;
            let answer = tokio::select! {
                answer = self.attempt(attempt, &current.key, &mut into) => Some(answer),
                () = ended(&mut link, &held.connection_id) => None,
            };
            let Some(answer) = answer else {
                backoff.reset();
                wait(binding.changed()).await;
                continue;
            };
            let raw = match answer {
                Ok(raw) => raw,
                Err(Failure::Network | Failure::Timeout | Failure::Server) => {
                    self.sleep(backoff.next_wait()).await;
                    continue;
                }
                Err(Failure::Redirect | Failure::Unreadable) => {
                    return Err(self.rejected(endpoint, 0, None));
                }
            };
            if (200..300).contains(&raw.status) {
                if raw.body.is_none() && matches!(into, Into2xx::Memory(_)) {
                    return Err(self.rejected(endpoint, raw.status, None));
                }
                return Ok((raw, held));
            }
            if matches!(raw.status, 500 | 502 | 503 | 504) {
                self.sleep(backoff.next_wait()).await;
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
                    self.sleep(Duration::from_secs(seconds)).await;
                }
                ErrorCode::StaleConnection => {
                    self.refused(&held, error);
                    wait(binding.changed()).await;
                }
                ErrorCode::StaleBinding => wait(binding.changed()).await,
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
                    wait(binding.changed()).await;
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

    async fn sleep(&self, wait: Duration) {
        self.clock.sleep_until(self.clock.now() + wait).await;
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
        observe::error!(
            "drive_transfer.answer_rejected",
            drive_transfer.operation = observe::Id(operation(endpoint)),
            http.response.status_code = i64::from(status),
            error.code = observe::Code(code_text)
        );
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

/// Waits for a watch change; a closed channel never changes again.
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
            sink.restart().map_err(|_| Failure::Unreadable)?;
        }
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| Failure::Network)?;
            if let Ok(chunk) = frame.into_data() {
                received.fetch_add(chunk.len() as u64, Ordering::AcqRel);
                sink.append(&chunk).map_err(|_| Failure::Unreadable)?;
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
            let query = ListFilesQuery {
                protocol_version: QueryProtocolVersion::default(),
                connection_id: held.connection_id,
                binding_id: held.binding_id,
                after_path: body.after_path.map(|path| path.as_str().to_owned()),
            };
            self.checked(endpoint, &raw, |bytes| {
                protocol::decode_answer::<ListFilesResponse, _>(bytes, &query)
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
            let decimal = |value: i64| Decimal::new(value.to_string()).ok();
            let (Some(after_seq), after_index) =
                (decimal(body.after_seq), body.after_index.map(decimal))
            else {
                return Err(self.rejected(endpoint, raw.status, None));
            };
            let query = ListChangesQuery {
                protocol_version: QueryProtocolVersion::default(),
                connection_id: held.connection_id,
                binding_id: held.binding_id,
                after_seq,
                after_index: after_index.flatten(),
            };
            self.checked(endpoint, &raw, |bytes| {
                protocol::decode_answer::<ListChangesResponse, _>(bytes, &query)
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
            let path = ContentPathParams {
                drive_id,
                sha256: body.sha256,
            };
            let headers = UploadContentRequestHeaders {
                content_type: Fixed::default(),
                content_length: Decimal::new(size.to_string()).expect("a size is a decimal"),
            };
            self.checked(endpoint, &raw, |bytes| {
                protocol::decode_answer::<UploadContentResponse, _>(
                    bytes,
                    &UploadRequest {
                        path: &path,
                        headers: &headers,
                    },
                )
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
            let encoded = |held: &Binding| CommitRequest {
                protocol_version: ProtocolVersion::default(),
                connection_id: held.connection_id.clone(),
                binding_id: held.binding_id.clone(),
                changes: List(body.changes.clone()),
            };
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
            self.checked(endpoint, &raw, |bytes| {
                protocol::decode_answer::<CommitResponse, _>(bytes, &sent)
            })
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
    fn checked<T, E>(
        &self,
        endpoint: DriveTransferEndpoint,
        raw: &Raw,
        decode: impl FnOnce(&[u8]) -> Result<T, E>,
    ) -> Result<T, Ended> {
        raw.body
            .as_deref()
            .and_then(|bytes| decode(bytes).ok())
            .ok_or_else(|| self.rejected(endpoint, raw.status, None))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use protocol::field::Sha256;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn a_put_whose_bytes_keep_moving_past_ten_seconds_completes_within_its_total() {
        const SIZE: usize = 200_000;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let sha256 = "a".repeat(64);
        let (answer, may_answer) = oneshot::channel::<()>();
        let answered = sha256.clone();
        let again = Arc::new(AtomicU64::new(0));
        let counting = again.clone();
        let stub = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // A second connection is an attempt sent again: the first failed.
            tokio::spawn(async move {
                while listener.accept().await.is_ok() {
                    counting.fetch_add(1, Ordering::SeqCst);
                }
            });
            let mut read = vec![0; SIZE + 4_096];
            let mut total = 0;
            while !read[..total].windows(4).any(|window| window == b"\r\n\r\n") || total < SIZE {
                total += socket.read(&mut read[total..]).await.unwrap();
            }
            may_answer.await.unwrap();
            let body = format!(r#"{{"content_sha256":"{answered}","size_bytes":{SIZE}}}"#);
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body.as_bytes()).await.unwrap();
        });
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
        // A byte moves in every second of the clock.
        let transport = Arc::new(Transport::sampled(&url, |now| now.as_secs()));
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
                sha256: Sha256::new(sha256).unwrap(),
                content: Content::Bytes(Bytes::from(vec![b'x'; SIZE])),
                rate_limited: None,
            },
        };
        assert_eq!(total(SIZE as u64), Duration::from_secs(40));
        let sending = client.clone();
        let sent = tokio::spawn(async move { sending.upload_content(request).await });
        for second in 1..=25 {
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
        answer.send(()).unwrap();
        let uploaded = sent.await.unwrap().unwrap();
        assert_eq!(uploaded.size_bytes.get(), SIZE as i64);
        stub.await.unwrap();
    }
}
