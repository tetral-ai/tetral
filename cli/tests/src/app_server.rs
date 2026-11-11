//! A `tetral server run` under test, and its local API reached as any client
//! reaches it: raw HTTP/1.1 over the socket, whose path is derived here
//! independently of the code under test.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::run::{self, Run, RunningChild, Tetral};
use crate::seams::WAIT_LIMIT;
use crate::socket_directory::{self, SocketContext};

/// The root's socket as the local API's protocol derives it: `S/<h>.sock`,
/// `<h>` the first 16 hexadecimal characters of the SHA-256 of the root's
/// canonical path.
pub fn socket_path(root: &Path) -> PathBuf {
    socket_path_in(root, &socket_directory::context())
}

fn socket_path_in(root: &Path, context: &SocketContext) -> PathBuf {
    let canonical = root.canonicalize().expect("the root exists");
    let digest = Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    let name: String = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let path = context.socket_dir().join(format!("{name}.sock"));
    assert!(path.starts_with(context.paths().parent()));
    path
}

/// An answer of the local API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    /// The status line and the headers.
    pub head: String,
    pub body: Vec<u8>,
}

impl Response {
    /// The value of the header `name`, matched without case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().skip(1).find_map(|line| {
            let (header, value) = line.split_once(':')?;
            header.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|error| panic!("{error}: {:?}", String::from_utf8_lossy(&self.body)))
    }
}

/// Sends one request with an empty body and reads the whole answer; `None`
/// when the socket refuses the connection or does not exist, or when the
/// answer has not ended within [`WAIT_LIMIT`].
pub fn request(socket: &Path, method: &str, path: &str) -> Option<Response> {
    request_with_body(socket, method, path, b"")
}

/// [`request`] with `body` as its JSON body.
pub fn request_with_body(socket: &Path, method: &str, path: &str, body: &[u8]) -> Option<Response> {
    let mut stream = UnixStream::connect(socket).ok()?;
    stream.set_read_timeout(Some(WAIT_LIMIT)).ok()?;
    stream.set_write_timeout(Some(WAIT_LIMIT)).ok()?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .ok()?;
    stream.write_all(body).ok()?;
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).ok()?;
    let end = answer.windows(4).position(|window| window == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&answer[..end]).into_owned();
    let status = head.split(' ').nth(1)?.parse().ok()?;
    Some(Response {
        status,
        head,
        body: answer[end + 4..].to_vec(),
    })
}

/// Waits until `ready` holds, failing the test past [`WAIT_LIMIT`].
pub fn wait_until(what: &str, ready: impl FnMut() -> bool) {
    wait_until_within(WAIT_LIMIT, what, ready);
}

/// [`wait_until`] with `limit` in place of [`WAIT_LIMIT`], for a step whose
/// own work takes longer; the caller states the estimate it comes from.
pub fn wait_until_within(limit: Duration, what: &str, mut ready: impl FnMut() -> bool) {
    let start = Instant::now();
    while !ready() {
        assert!(start.elapsed() < limit, "{what} did not happen");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A running `server run`, reaped when dropped. Its owned test parent handles
/// stale paths after every related process is gone; this guard never unlinks
/// a socket that a successor may now own. On macOS no task dies with the App
/// Server, so a drop that kills it also kills the process group of each task
/// its records name, as the next start would.
pub struct AppServer {
    child: Option<RunningChild>,
    pub socket: PathBuf,
    #[cfg_attr(not(target_os = "macos"), expect(dead_code))]
    root: PathBuf,
}

impl Drop for AppServer {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return;
        };
        drop(child);
        #[cfg(target_os = "macos")]
        for entry in std::fs::read_dir(self.root.join("execution"))
            .into_iter()
            .flatten()
            .flatten()
        {
            let group = std::fs::read(entry.path())
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|record| record["pid_namespace"].as_u64())
                .and_then(|group| i32::try_from(group).ok())
                .and_then(rustix::process::Pid::from_raw);
            if let Some(group) = group {
                let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
            }
        }
    }
}

impl AppServer {
    /// Starts `tetral`, a `server run` command line, and waits until its
    /// socket answers `GET /v1/status`.
    pub fn start(tetral: Tetral, root: &Path) -> AppServer {
        let socket = socket_path_in(root, tetral.socket_context());
        let mut server = AppServer {
            child: Some(tetral.spawn()),
            socket,
            root: root.to_path_buf(),
        };
        wait_until("the App Server's status", || {
            if let Ok(Some(_)) = server.child.as_mut().unwrap().try_wait() {
                let run = run::finish(server.child.take().unwrap());
                panic!(
                    "server run exited with {}; stdout: {}; stderr: {}",
                    run.status,
                    String::from_utf8_lossy(&run.stdout),
                    String::from_utf8_lossy(&run.stderr)
                );
            }
            request(&server.socket, "GET", "/v1/status")
                .is_some_and(|response| response.status == 200)
        });
        server
    }

    pub fn status(&self) -> Value {
        let response = request(&self.socket, "GET", "/v1/status").expect("status answers");
        assert_eq!(response.status, 200);
        response.json()
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("the process runs").id()
    }

    /// Whether the process has not exited.
    pub fn running(&mut self) -> bool {
        let child = self.child.as_mut().expect("the process was started");
        child
            .try_wait()
            .expect("the process can be waited for")
            .is_none()
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
