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
/// The shared time budget of one local request or diagnostic pair.
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

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

/// The body of an accepted shutdown or computer-use change, the empty
/// object.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// The client of one state root's App Server.
#[derive(Debug, Clone)]
pub struct Client {
    socket: PathBuf,
    response_limit: usize,
    response_timeout: Duration,
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
            response_timeout: RESPONSE_TIMEOUT,
        })
    }

    pub async fn status(&self) -> Result<Status, Error> {
        self.call_with_deadline(Method::GET, "/v1/status", StatusCode::OK)
            .await
    }

    /// `GET /v1/status`, then `GET /v1/metrics` as text, both within one
    /// five-second deadline.
    pub async fn status_and_metrics(&self) -> Result<(Status, String), Error> {
        let deadline = state::seams::now() + self.response_timeout;
        let calls = async {
            let status = self.call(Method::GET, "/v1/status", StatusCode::OK).await?;
            let metrics = self
                .fetch(Method::GET, "/v1/metrics", StatusCode::OK)
                .await?;
            let metrics =
                String::from_utf8(metrics).map_err(|error| Error::Decode(error.to_string()))?;
            Ok((status, metrics))
        };
        tokio::select! {
            answer = calls => answer,
            () = state::seams::sleep_until_counted(deadline, "local.response_deadline_checked") => {
                Err(Error::NotResponding)
            }
        }
    }

    pub async fn sessions(&self) -> Result<Sessions, Error> {
        self.call(Method::GET, "/v1/sessions", StatusCode::OK).await
    }

    /// Starts the App Server's shutdown; it returns on the `202` with the
    /// body `{}`, before the App Server has exited.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let Empty {} = self
            .call_with_deadline(Method::POST, "/v1/shutdown", StatusCode::ACCEPTED)
            .await?;
        Ok(())
    }

    /// `POST /v1/computer-use` with `{enabled, driver}`, `driver` sent as
    /// `null` when absent. It returns on the `200`, which an enable gets
    /// once the driver's `initialize` succeeded, so the call has no deadline
    /// of its own; the App Server's start bound ends it.
    pub async fn set_computer_use(&self, enabled: bool, driver: Option<&str>) -> Result<(), Error> {
        let body = serde_json::json!({ "enabled": enabled, "driver": driver }).to_string();
        let answer = self
            .send(Method::POST, "/v1/computer-use", body, StatusCode::OK)
            .await?;
        let Empty {} =
            serde_json::from_slice(&answer).map_err(|error| Error::Decode(error.to_string()))?;
        Ok(())
    }

    /// One deadline covers connecting, HTTP exchange and the complete body.
    async fn call_with_deadline<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        success: StatusCode,
    ) -> Result<T, Error> {
        let deadline = state::seams::now() + self.response_timeout;
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
        let body = self.fetch(method, path, success).await?;
        serde_json::from_slice(&body).map_err(|error| Error::Decode(error.to_string()))
    }

    /// The body of the answer to `method` `path` without a body, which must
    /// be `success`.
    async fn fetch(
        &self,
        method: Method,
        path: &str,
        success: StatusCode,
    ) -> Result<Vec<u8>, Error> {
        self.send(method, path, String::new(), success).await
    }

    /// The body of the answer to `method` `path` with the JSON `body`, which
    /// must be `success`.
    async fn send(
        &self,
        method: Method,
        path: &str,
        body: String,
        success: StatusCode,
    ) -> Result<Vec<u8>, Error> {
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
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::HOST, "localhost");
        if !body.is_empty() {
            request = request.header(hyper::header::CONTENT_TYPE, "application/json");
        }
        let request = request
            .body(body)
            .expect("a request of a fixed method and path builds");
        let response = sender.send_request(request).await.map_err(io_error)?;
        let status = response.status();
        let body = self.read(response.into_body()).await?;
        if status != success {
            return Err(Error::Http { status, body });
        }
        Ok(body)
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

    #[tokio::test]
    async fn a_setting_connection_closed_after_the_request_is_an_io_error() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let head = String::from_utf8(request).unwrap().to_ascii_lowercase();
            assert!(head.starts_with("post /v1/computer-use http/1.1"));
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .unwrap()
                .parse()
                .unwrap();
            let mut body = vec![0; length];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                serde_json::json!({"enabled":false,"driver":null})
            );
        });
        let client = Client {
            socket,
            response_limit: RESPONSE_MAX_BYTES,
            response_timeout: RESPONSE_TIMEOUT,
        };
        assert!(matches!(
            client.set_computer_use(false, None).await,
            Err(Error::Io(_))
        ));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn a_client_uses_its_constructed_response_timeout() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("server.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        });
        let client = Client {
            socket,
            response_limit: RESPONSE_MAX_BYTES,
            response_timeout: Duration::from_millis(20),
        };
        let result = tokio::time::timeout(Duration::from_secs(1), client.status())
            .await
            .unwrap();
        assert!(matches!(result, Err(Error::NotResponding)));
        peer.await.unwrap();
    }

    #[cfg(feature = "test-seams")]
    #[tokio::test]
    async fn explicit_socket_context_reaches_only_its_parent_and_keeps_permission_checks() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        // The temporary directory of macOS is too long for a socket parent.
        let parent = tempfile::tempdir_in("/tmp").unwrap();
        let elsewhere = tempfile::tempdir_in("/tmp").unwrap();
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
                response_timeout: RESPONSE_TIMEOUT,
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
    async fn set_computer_use_sends_enabled_and_the_driver_or_null() {
        for (enabled, driver, sent) in [
            (
                true,
                Some("/opt/cua/cua-driver"),
                serde_json::json!({ "enabled": true, "driver": "/opt/cua/cua-driver" }),
            ),
            (
                false,
                None,
                serde_json::json!({ "enabled": false, "driver": null }),
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("server.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0; 2048];
                let (head, body) = loop {
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert!(read > 0, "the request ended early");
                    request.extend_from_slice(&chunk[..read]);
                    let Some(end) = request.windows(4).position(|four| four == b"\r\n\r\n") else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let length: usize = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .expect("a content-length header")
                        .parse()
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break (head, request[end + 4..end + 4 + length].to_vec());
                    }
                };
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                    .await
                    .unwrap();
                (head, body)
            });
            let client = Client {
                socket,
                response_limit: RESPONSE_MAX_BYTES,
                response_timeout: RESPONSE_TIMEOUT,
            };
            client.set_computer_use(enabled, driver).await.unwrap();
            let (head, body) = peer.await.unwrap();
            assert!(head.starts_with("post /v1/computer-use http/1.1"), "{head}");
            assert!(head.contains("content-type: application/json"), "{head}");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                sent
            );
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
            response_timeout: RESPONSE_TIMEOUT,
        };
        client.shutdown().await.unwrap();
        peer.await.unwrap();
    }
}
