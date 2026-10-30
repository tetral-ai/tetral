//! A loopback Git smart-HTTP server over HTTPS that stands in for git-proxy:
//! `git http-backend` behind a test certificate, serving only
//! `/github.com/<owner>/<repo>` paths and answering `401` to a request whose
//! `X-Tetral-Git-Ticket` is not a current ticket of the reference control
//! server. Each request is a connection of its own; the server records
//! every request, can answer the next clone with a fault and can hold the
//! next pack response until it is released.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use rustls::pki_types::PrivatePkcs8KeyDer;

use crate::reference::CurrentTickets;

/// The header that carries the ticket on every request to git-proxy.
pub const TICKET_HEADER: &str = "x-tetral-git-ticket";

/// One request the server answered.
#[derive(Debug, Clone)]
pub struct GitRequest {
    /// The path, `/github.com/<owner>/<repo>/...`.
    pub path: String,
    pub ticket: Option<String>,
    pub status: u16,
}

/// How the server answers the first request of the next clone.
#[derive(Debug, Clone)]
pub enum Fault {
    /// This status; a `404` carries GitHub's `Repository not found.`.
    Status(u16),
    /// A `200` advertisement holding only `ERR <message>`, which Git prints
    /// as its last line `fatal: remote error: <message>`.
    RemoteError(String),
}

/// A repository the server serves, with the commits it holds.
pub struct Remote {
    /// The repository's `https://github.com/` URL.
    pub url: String,
    /// The first commit, an ancestor of both branches.
    pub first: String,
    /// The head of `main`, the default branch.
    pub main: String,
    /// The head of the branch `feature`, which forks from `first`.
    pub feature: String,
}

#[derive(Default)]
struct State {
    tickets: Option<CurrentTickets>,
    faults: VecDeque<Fault>,
    hold_pack: bool,
    held: bool,
    requests: Vec<GitRequest>,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    root: PathBuf,
}

/// A running server; dropping it stops accepting.
pub struct GitServer {
    /// The origin the reference control server names as `git_proxy_url`.
    pub url: String,
    /// The PEM of the test certificate, for the App Server's
    /// `GIT_SSL_CAINFO`.
    pub ca_file: PathBuf,
    /// The `HOME` and `XDG_CONFIG_HOME` of the `git` commands that set up
    /// the repositories.
    home: PathBuf,
    shared: Arc<Shared>,
    stopped: Arc<AtomicBool>,
    address: std::net::SocketAddr,
}

impl GitServer {
    /// Starts the server with its repositories and certificate under
    /// `directory`.
    pub fn start(directory: &Path) -> GitServer {
        let root = directory.join("repositories");
        let home = directory.join("home");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let certificate = certified.cert.der().clone();
        let encoded = base64::engine::general_purpose::STANDARD.encode(certificate.as_ref());
        let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
        for line in encoded.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
        let ca_file = directory.join("git-server-ca.pem");
        std::fs::write(&ca_file, pem).unwrap();
        let private = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = Arc::new(
            rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![certificate], private.into())
                .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            root,
        });
        let stopped = Arc::new(AtomicBool::new(false));
        let (accepting, stopping) = (shared.clone(), stopped.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                if stopping.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let (shared, config) = (accepting.clone(), config.clone());
                thread::spawn(move || serve(&shared, config, stream));
            }
        });
        GitServer {
            url: format!("https://{address}"),
            ca_file,
            home,
            shared,
            stopped,
            address,
        }
    }

    /// Accepts only the current tickets that `tickets` reads.
    pub fn check_tickets(&self, tickets: CurrentTickets) {
        self.shared.state.lock().unwrap().tickets = Some(tickets);
    }

    /// Creates `acme/<name>`: `main` with two commits and `feature` forked
    /// from the first with one of its own.
    pub fn repository(&self, name: &str) -> Remote {
        let work = self.home.join(format!("work-{name}"));
        let git = |arguments: &[&str]| self.git(&work, arguments);
        std::fs::create_dir_all(&work).unwrap();
        git(&["init", "--quiet", "--initial-branch", "main"]);
        let commit = |file: &str| {
            std::fs::write(work.join(file), file).unwrap();
            git(&["add", file]);
            git(&["commit", "--quiet", "-m", file]);
            git(&["rev-parse", "HEAD"])
        };
        let first = commit("first");
        let main = commit("second");
        git(&["checkout", "--quiet", "-b", "feature", &first]);
        let feature = commit("feature");
        git(&["checkout", "--quiet", "main"]);
        let bare = self.shared.root.join("github.com/acme").join(name);
        self.git(
            &self.home,
            &[
                "clone",
                "--quiet",
                "--bare",
                work.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
        );
        Remote {
            url: format!("https://github.com/acme/{name}"),
            first,
            main,
            feature,
        }
    }

    /// Runs `git` in `directory` with the server's own configuration only
    /// and gives its trimmed stdout.
    fn git(&self, directory: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args(["-c", "user.name=Test", "-c", "user.email=test@example.com"])
            .args(arguments)
            .current_dir(directory)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", &self.home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    /// Answers the first request of the next clone with `fault`.
    pub fn fault(&self, fault: Fault) {
        self.shared.state.lock().unwrap().faults.push_back(fault);
    }

    /// Holds the next pack response, after Git produced it, until
    /// [`GitServer::release`].
    pub fn hold_pack(&self) {
        self.shared.state.lock().unwrap().hold_pack = true;
    }

    /// Waits until a pack response is held.
    pub fn wait_until_held(&self) {
        self.wait("a held pack response", |state| state.held);
    }

    pub fn release(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.hold_pack = false;
        self.shared.changed.notify_all();
    }

    /// The requests answered so far, in order.
    pub fn requests(&self) -> Vec<GitRequest> {
        self.shared.state.lock().unwrap().requests.clone()
    }

    fn wait(&self, what: &str, mut ready: impl FnMut(&State) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut state = self.shared.state.lock().unwrap();
        while !ready(&state) {
            let left = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| panic!("timed out waiting for {what}"));
            state = self.shared.changed.wait_timeout(state, left).unwrap().0;
        }
    }
}

impl Drop for GitServer {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.release();
        let _ = TcpStream::connect(self.address);
    }
}

struct Request {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(each, _)| each == name)
            .map(|(_, value)| value.as_str())
    }
}

fn serve(shared: &Shared, config: Arc<rustls::ServerConfig>, stream: TcpStream) {
    let connection = rustls::ServerConnection::new(config).unwrap();
    let mut tls = rustls::StreamOwned::new(connection, stream);
    let Some(request) = read_request(&mut tls) else {
        return;
    };
    let (status, headers, body) = answer(shared, &request);
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Error",
    };
    let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let _ = tls.write_all(head.as_bytes());
    let _ = tls.write_all(&body);
    tls.conn.send_close_notify();
    let _ = tls.flush();
}

/// The request's head and body, its body de-chunked; `None` when the
/// connection ends first.
fn read_request(stream: &mut impl Read) -> Option<Request> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?.to_owned();
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    let (path, query) = (path.to_owned(), query.to_owned());
    let mut headers = Vec::new();
    loop {
        line.clear();
        reader.read_line(&mut line).ok()?;
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        let (name, value) = trimmed.split_once(':')?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
    let mut request = Request {
        method,
        path,
        query,
        headers,
        body: Vec::new(),
    };
    if request.header("transfer-encoding") == Some("chunked") {
        loop {
            line.clear();
            reader.read_line(&mut line).ok()?;
            let size = usize::from_str_radix(line.trim(), 16).ok()?;
            let mut chunk = vec![0; size + 2];
            reader.read_exact(&mut chunk).ok()?;
            if size == 0 {
                break;
            }
            request.body.extend_from_slice(&chunk[..size]);
        }
    } else if let Some(length) = request.header("content-length") {
        let mut body = vec![0; length.parse().ok()?];
        reader.read_exact(&mut body).ok()?;
        request.body = body;
    }
    Some(request)
}

type Answer = (u16, Vec<(String, String)>, Vec<u8>);

fn answer(shared: &Shared, request: &Request) -> Answer {
    let ticket = request.header(TICKET_HEADER).map(str::to_owned);
    let answer = checked(shared, request, ticket.as_deref());
    let mut state = shared.state.lock().unwrap();
    state.requests.push(GitRequest {
        path: request.path.clone(),
        ticket,
        status: answer.0,
    });
    shared.changed.notify_all();
    answer
}

fn checked(shared: &Shared, request: &Request, ticket: Option<&str>) -> Answer {
    if !request.path.starts_with("/github.com/") || request.path.contains("..") {
        return plain(404, "not a github.com repository path");
    }
    let tickets = shared.state.lock().unwrap().tickets.clone();
    let current = tickets.map(|tickets| tickets.read()).unwrap_or_default();
    if !ticket.is_some_and(|ticket| current.iter().any(|each| each == ticket)) {
        return plain(401, "the ticket is not current");
    }
    let first_of_clone = request.method == "GET" && request.path.ends_with("/info/refs");
    if first_of_clone {
        let fault = shared.state.lock().unwrap().faults.pop_front();
        match fault {
            Some(Fault::Status(404)) => return plain(404, "Repository not found."),
            Some(Fault::Status(status)) => return plain(status, "fault"),
            Some(Fault::RemoteError(message)) => {
                let line = format!("ERR {message}");
                let body = format!("{:04x}{line}", line.len() + 4);
                return (
                    200,
                    vec![(
                        "Content-Type".to_owned(),
                        "application/x-git-upload-pack-advertisement".to_owned(),
                    )],
                    body.into_bytes(),
                );
            }
            None => {}
        }
    }
    let answer = backend(&shared.root, request);
    if request.method == "POST" && request.path.ends_with("/git-upload-pack") {
        let mut state = shared.state.lock().unwrap();
        if state.hold_pack {
            state.held = true;
            shared.changed.notify_all();
            while state.hold_pack {
                state = shared.changed.wait(state).unwrap();
            }
            state.held = false;
        }
    }
    answer
}

fn plain(status: u16, text: &str) -> Answer {
    (
        status,
        vec![("Content-Type".to_owned(), "text/plain".to_owned())],
        text.as_bytes().to_vec(),
    )
}

/// `git http-backend` as a CGI program for `request`.
fn backend(root: &Path, request: &Request) -> Answer {
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("PATH_INFO", &request.path)
        .env("REQUEST_METHOD", &request.method)
        .env("QUERY_STRING", &request.query)
        .env("CONTENT_LENGTH", request.body.len().to_string())
        .env("REMOTE_ADDR", "127.0.0.1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    for (header, variable) in [
        ("content-type", "CONTENT_TYPE"),
        ("content-encoding", "HTTP_CONTENT_ENCODING"),
        ("git-protocol", "GIT_PROTOCOL"),
    ] {
        if let Some(value) = request.header(header) {
            command.env(variable, value);
        }
    }
    let mut child = command.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let body = request.body.clone();
    let writer = thread::spawn(move || {
        let _ = stdin.write_all(&body);
    });
    let output = child.wait_with_output().unwrap();
    writer.join().unwrap();
    let split = output
        .stdout
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| (at, 4))
        .or_else(|| {
            output
                .stdout
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|at| (at, 2))
        })
        .expect("http-backend writes its headers");
    let head = String::from_utf8_lossy(&output.stdout[..split.0]).into_owned();
    let body = output.stdout[split.0 + split.1..].to_vec();
    let mut status = 200;
    let mut headers = Vec::new();
    for line in head.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("status") {
            status = value[..3].parse().unwrap();
        } else {
            headers.push((name.to_owned(), value.to_owned()));
        }
    }
    (status, headers, body)
}
