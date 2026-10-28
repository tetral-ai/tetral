#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

use std::future::poll_fn;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

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
    #[error("the App Server did not answer within five seconds")]
    NotResponding,
    /// An answer other than the endpoint's success status.
    #[error("the App Server answered {status}")]
    Http { status: StatusCode, body: Vec<u8> },
    #[error("the App Server's answer does not decode: {0}")]
    Decode(String),
    #[error(transparent)]
    Io(io::Error),
}

/// The body of an accepted shutdown, the empty object.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Accepted {}

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
        Self::from_checked_socket(private::check_socket(root), response_limit)
    }

    /// Uses the harness's explicit socket parent without changing the
    /// process environment or bypassing the normal path and permission rules.
    #[cfg(feature = "test-seams")]
    pub fn with_test_socket_context(
        root: &StateRoot,
        context: &state::paths::TestSocketContext,
        response_limit: usize,
    ) -> Result<Client, Error> {
        Self::from_checked_socket(private::check_socket_in(root, context), response_limit)
    }

    fn from_checked_socket(
        socket: Result<Option<PathBuf>, CheckError>,
        response_limit: usize,
    ) -> Result<Client, Error> {
        let socket = socket
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
        self.call_with_deadline(Method::GET, "/v1/status", StatusCode::OK)
            .await
    }

    pub async fn sessions(&self) -> Result<Sessions, Error> {
        self.call(Method::GET, "/v1/sessions", StatusCode::OK).await
    }

    /// Starts the App Server's shutdown; it returns on the `202` with the
    /// body `{}`, before the App Server has exited.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let Accepted {} = self
            .call_with_deadline(Method::POST, "/v1/shutdown", StatusCode::ACCEPTED)
            .await?;
        Ok(())
    }

    /// One deadline covers connecting, HTTP exchange and the complete body.
    async fn call_with_deadline<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        success: StatusCode,
    ) -> Result<T, Error> {
        let deadline = state::seams::now() + Duration::from_secs(5);
        self.call_until(
            method,
            path,
            success,
            state::seams::sleep_until_counted(deadline, "local.response_deadline_checked"),
        )
        .await
    }

    async fn call_until<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        success: StatusCode,
        expired: impl std::future::Future<Output = ()>,
    ) -> Result<T, Error> {
        tokio::select! {
            answer = self.call(method, path, success) => answer,
            () = expired => Err(Error::NotResponding),
        }
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
        let _connection = ConnectionTask(tokio::spawn(connection));
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

/// Cancellation and completed requests both end the HTTP connection driver.
struct ConnectionTask(tokio::task::JoinHandle<Result<(), hyper::Error>>);

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;
    use tokio::sync::oneshot;

    #[cfg(feature = "test-seams")]
    #[tokio::test]
    async fn explicit_socket_context_reaches_only_its_parent_and_keeps_permission_checks() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let parent = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let context = state::paths::TestSocketContext::new(parent.path()).unwrap();
        let other = state::paths::TestSocketContext::new(elsewhere.path()).unwrap();
        let root = StateRoot::new(root.path().as_os_str()).unwrap();
        std::fs::create_dir(context.socket_dir()).unwrap();
        std::fs::set_permissions(context.socket_dir(), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        let socket = context.socket_path(&root).unwrap().unwrap();
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\n{}")
                .await
                .unwrap();
        });
        let client = Client::with_test_socket_context(&root, &context, RESPONSE_MAX_BYTES).unwrap();
        client.shutdown().await.unwrap();
        peer.await.unwrap();
        let other_client =
            Client::with_test_socket_context(&root, &other, RESPONSE_MAX_BYTES).unwrap();
        assert!(matches!(
            other_client.shutdown().await,
            Err(Error::NotRunning)
        ));
        std::fs::set_permissions(context.socket_dir(), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        assert!(
            matches!(Client::with_test_socket_context(&root, &context, RESPONSE_MAX_BYTES),
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::PermissionDenied)
        );
    }

    #[tokio::test]
    async fn one_deadline_cancels_a_missing_response_or_an_incomplete_body() {
        for headers in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("server.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let (ready, received) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 2048];
                assert!(stream.read(&mut request).await.unwrap() > 0);
                if headers {
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{")
                        .await
                        .unwrap();
                }
                ready.send(()).unwrap();
                let mut byte = [0];
                // Cancellation must close the spawned HTTP driver too.
                assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
            });
            let client = Client {
                socket,
                response_limit: RESPONSE_MAX_BYTES,
            };
            let (expire, expired) = oneshot::channel();
            let call = tokio::spawn(async move {
                client
                    .call_until::<serde_json::Value>(
                        Method::GET,
                        "/v1/status",
                        StatusCode::OK,
                        async {
                            let _ = expired.await;
                        },
                    )
                    .await
            });
            received.await.unwrap();
            assert!(!call.is_finished());
            expire.send(()).unwrap();
            assert!(matches!(call.await.unwrap(), Err(Error::NotResponding)));
            tokio::time::timeout(Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_complete_response_finishes_before_the_deadline() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\n{}")
                .await
                .unwrap();
        });
        let client = Client {
            socket,
            response_limit: RESPONSE_MAX_BYTES,
        };
        client.shutdown().await.unwrap();
        peer.await.unwrap();
    }
}
