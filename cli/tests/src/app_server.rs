//! A `tetral server run` under test, and its local API reached as any client
//! reaches it: raw HTTP/1.1 over the socket, whose path is derived here
//! independently of the code under test.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::run::{self, Run, Tetral};
use crate::seams::WAIT_LIMIT;
use crate::socket_directory::socket_dir;

/// The root's socket as the local API's protocol derives it: `S/<h>.sock`,
/// `<h>` the first 16 hexadecimal characters of the SHA-256 of the root's
/// canonical path.
pub fn socket_path(root: &Path) -> PathBuf {
    let canonical = root.canonicalize().expect("the root exists");
    let digest = Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let name: String = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    socket_dir().join(format!("{name}.sock"))
}

/// An answer of the local API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|error| panic!("{error}: {:?}", String::from_utf8_lossy(&self.body)))
    }
}

/// Sends one request with an empty body and reads the whole answer; `None`
/// when the socket refuses the connection or does not exist.
pub fn request(socket: &Path, method: &str, path: &str) -> Option<Response> {
    let mut stream = UnixStream::connect(socket).ok()?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).ok()?;
    let end = answer.windows(4).position(|window| window == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&answer[..end]).into_owned();
    let status = head.split(' ').nth(1)?.parse().ok()?;
    Some(Response {
        status,
        body: answer[end + 4..].to_vec(),
    })
}

/// Waits until `ready` holds, failing the test past [`WAIT_LIMIT`].
pub fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let start = Instant::now();
    while !ready() {
        assert!(start.elapsed() < WAIT_LIMIT, "{what} did not happen");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A running `server run`, killed when dropped, which also removes the
/// socket that the killed process leaves in the shared `S`.
pub struct AppServer {
    child: Option<Child>,
    pub socket: PathBuf,
}

impl AppServer {
    /// Starts `tetral`, a `server run` command line, and waits until its
    /// socket answers `GET /v1/status`.
    pub fn start(tetral: Tetral, root: &Path) -> AppServer {
        let socket = socket_path(root);
        let mut child = tetral.spawn();
        wait_until("the App Server's status", || {
            if let Ok(Some(status)) = child.try_wait() {
                panic!("server run exited with {status}");
            }
            request(&socket, "GET", "/v1/status").is_some_and(|response| response.status == 200)
        });
        AppServer {
            child: Some(child),
            socket,
        }
    }

    pub fn status(&self) -> Value {
        let response = request(&self.socket, "GET", "/v1/status").expect("status answers");
        assert_eq!(response.status, 200);
        response.json()
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("the process runs").id()
    }

    /// Waits for the process to exit by itself, within [`WAIT_LIMIT`], and
    /// gives its run and how long the exit took.
    pub fn exited(mut self) -> (Run, Duration) {
        let start = Instant::now();
        let child = self.child.as_mut().expect("the process runs");
        wait_until("the exit of server run", || {
            child
                .try_wait()
                .expect("the process can be waited for")
                .is_some()
        });
        let elapsed = start.elapsed();
        let child = self.child.take().expect("the process runs");
        (run::finish(child), elapsed)
    }

    /// Kills the process with SIGKILL and waits for it.
    pub fn kill(mut self) {
        let mut child = self.child.take().expect("the process runs");
        child.kill().expect("SIGKILL reaches the process");
        let _ = child.wait();
    }
}

impl Drop for AppServer {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_file(&self.socket);
        }
    }
}
