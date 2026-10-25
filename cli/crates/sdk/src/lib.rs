#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

use std::future::poll_fn;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;

use hyper::body::{Body, Incoming};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use protocol::local::{Sessions, Status};
use serde::de::DeserializeOwned;
use state::paths::StateRoot;
use state::private::{self, CheckError};
use tokio::net::UnixStream;

/// The largest response body the client reads.
pub const RESPONSE_MAX_BYTES: usize = 1_048_576;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No App Server serves the root: its socket is missing or refuses.
    #[error("no App Server owns the state root")]
    NotRunning,
    /// An answer other than the endpoint's success status.
    #[error("the App Server answered {status}")]
    Http { status: StatusCode, body: Vec<u8> },
    #[error("the App Server's answer does not decode: {0}")]
    Decode(String),
    #[error(transparent)]
    Io(io::Error),
}

/// The client of one state root's App Server.
#[derive(Debug, Clone)]
pub struct Client {
    socket: PathBuf,
    response_limit: usize,
}

impl Client {
    /// The client of the App Server that owns `root`, after the checks that
    /// `S` and the socket belong to the current user alone.
    pub fn new(root: &StateRoot) -> Result<Client, Error> {
        Client::with_response_limit(root, RESPONSE_MAX_BYTES)
    }

    pub fn with_response_limit(root: &StateRoot, response_limit: usize) -> Result<Client, Error> {
        let socket = private::check_socket(root)
            .map_err(|error| match error {
                CheckError::Io(failure) => Error::Io(failure.source),
                unsafe_path => Error::Io(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    unsafe_path.to_string(),
                )),
            })?
            .ok_or(Error::NotRunning)?;
        Ok(Client {
            socket,
            response_limit,
        })
    }

    pub async fn status(&self) -> Result<Status, Error> {
        self.call(Method::GET, "/v1/status", StatusCode::OK).await
    }

    pub async fn sessions(&self) -> Result<Sessions, Error> {
        self.call(Method::GET, "/v1/sessions", StatusCode::OK).await
    }

    /// Starts the App Server's shutdown; it returns on the `202`, before the
    /// App Server has exited.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let _: serde_json::Value = self
            .call(Method::POST, "/v1/shutdown", StatusCode::ACCEPTED)
            .await?;
        Ok(())
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        success: StatusCode,
    ) -> Result<T, Error> {
        let stream =
            UnixStream::connect(&self.socket)
                .await
                .map_err(|error| match error.kind() {
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => Error::NotRunning,
                    _ => Error::Io(error),
                })?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(io_error)?;
        tokio::spawn(connection);
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::HOST, "localhost")
            .body(String::new())
            .expect("a request of a fixed method and path builds");
        let response = sender.send_request(request).await.map_err(io_error)?;
        let status = response.status();
        let body = self.read(response.into_body()).await?;
        if status != success {
            return Err(Error::Http { status, body });
        }
        serde_json::from_slice(&body).map_err(|error| Error::Decode(error.to_string()))
    }

    async fn read(&self, mut body: Incoming) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        while let Some(frame) = poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await {
            if let Ok(data) = frame.map_err(io_error)?.into_data() {
                if bytes.len() + data.len() > self.response_limit {
                    return Err(Error::Decode(format!(
                        "the answer is over {} bytes",
                        self.response_limit
                    )));
                }
                bytes.extend_from_slice(&data);
            }
        }
        Ok(bytes)
    }
}

fn io_error(error: hyper::Error) -> Error {
    Error::Io(io::Error::other(error))
}
