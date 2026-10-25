//! The HTTP transport to Engine: one client that follows no redirect, the
//! credential's key as the bearer of every request, and every answer sorted
//! into what the Connection owner acts on. No request is ever recorded with
//! its headers or body.

use std::time::Duration;

use protocol::bootstrap::BootstrapKey;
use protocol::connection::MESSAGE_MAX_BYTES;
use protocol::errors::{ErrorBody, decode_client_error};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Response, StatusCode, Url};

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
