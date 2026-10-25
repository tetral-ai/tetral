//! The HTTP transport to Engine: one client that follows no redirect, the
//! credential's key as the bearer of every request, and every answer sorted
//! into what the Connection owner acts on. No request is ever recorded with
//! its headers or body.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Frame, SizeHint};
use protocol::bootstrap::BootstrapKey;
use protocol::connection::MESSAGE_MAX_BYTES;
use protocol::errors::{ErrorBody, decode_client_error};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Response, StatusCode, Url};

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
                Ok(response) => read_answer(response).await,
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
        let mut request = self
            .client
            .post(self.url(path))
            .header(AUTHORIZATION, format!("Bearer {}", key.expose()))
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_LENGTH, length);
        if let Some(traceparent) = traceparent {
            request = request.header("traceparent", traceparent);
        }
        let request = request.body(Body::wrap(Chunks {
            bytes,
            taken: 0,
            chunk_bytes: self.chunk_bytes,
            clock: clock.clone(),
            progress: progress.clone(),
        }));
        let answer = async {
            match request.send().await {
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
            Err(read_answer(response).await)
        }
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

async fn read_answer(response: Response) -> Answer {
    let status = response.status();
    if status.is_redirection() {
        return Answer::Failed(Failure::Redirect);
    }
    if status.is_server_error() {
        return Answer::Failed(Failure::Server);
    }
    let Some(body) = read_bounded(response, MESSAGE_MAX_BYTES).await else {
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
async fn read_bounded(mut response: Response, limit: usize) -> Option<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len() + chunk.len() > limit {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    Some(body)
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use http_body::Body as _;

    use super::*;

    #[test]
    fn a_report_body_is_handed_over_in_chunks_of_the_chunk_size_each_marking_progress() {
        let progress = Arc::new(AtomicU64::new(0));
        let mut chunks = Chunks {
            bytes: b"0123456789".to_vec(),
            taken: 0,
            chunk_bytes: 4,
            clock: Clock::new(|| Duration::from_secs(7), Duration::from_millis(10)),
            progress: progress.clone(),
        };
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
