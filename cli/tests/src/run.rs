//! Runs the `tetral` under test as a child process with a given environment
//! and stdin, and reads its one result object.

use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::binaries;
use crate::seams::{SOCKET_VARIABLE, Seams, WAIT_LIMIT};
use crate::socket_directory::{self, SocketContext};

/// A finished run.
#[derive(Debug)]
pub struct Run {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Run {
    /// The one JSON object of stdout; fails unless stdout is exactly one
    /// compact JSON object and a newline.
    pub fn object(&self) -> Value {
        let text = std::str::from_utf8(&self.stdout).expect("stdout is UTF-8");
        let line = text
            .strip_suffix('\n')
            .unwrap_or_else(|| panic!("stdout {text:?} does not end in one newline"));
        assert!(
            !line.contains('\n'),
            "stdout holds more than one line: {text:?}"
        );
        let object: Value =
            serde_json::from_str(line).unwrap_or_else(|error| panic!("stdout {line:?}: {error}"));
        assert!(object.is_object(), "stdout {line:?} is not an object");
        assert_eq!(
            serde_json::to_string(&object).unwrap(),
            line,
            "stdout is not compact"
        );
        object
    }

    /// The failure's code, after checking the exit status and the object's
    /// shape `{"error": {"code", "message"}}`.
    pub fn error_code(&self) -> String {
        assert_eq!(self.status.code(), Some(1), "exit status of a failure");
        let object = self.object();
        assert_eq!(
            object.as_object().map(|members| members.len()),
            Some(1),
            "a failure object holds error only: {object}"
        );
        let error = object["error"].as_object().expect("an error object");
        assert_eq!(error.len(), 2, "the error holds code and message only");
        assert!(
            error["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty())
        );
        error["code"].as_str().expect("a code").to_owned()
    }

    pub fn message(&self) -> String {
        self.object()["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    /// How the run exited, for a record.
    pub fn exit(&self) -> String {
        self.status.to_string()
    }
}

/// A `tetral` command line with an environment that holds only what the
/// test sets.
pub struct Tetral {
    command: Command,
    stdin: Vec<u8>,
    stdin_open: bool,
    sockets: SocketContext,
    stderr_backpressure: bool,
}

impl Tetral {
    pub fn new<I, S>(arguments: I) -> Tetral
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Tetral::at(binaries::tetral(), arguments)
    }

    /// A command line of the `tetral` at `program`, such as a copy of
    /// [`binaries::tetral`] in a directory of the test's.
    pub fn at<I, S>(program: &Path, arguments: I) -> Tetral
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(program);
        command.args(arguments).env_clear();
        let sockets = socket_directory::context();
        command.env(
            state::paths::TEST_SOCKET_PARENT_VARIABLE,
            sockets.paths().parent(),
        );
        Tetral {
            command,
            stdin: Vec::new(),
            stdin_open: false,
            sockets,
            stderr_backpressure: false,
        }
    }

    /// Runs the program under the file name `argv0`, which selects an entry
    /// point of `tetral`.
    pub fn arg0(mut self, argv0: &str) -> Tetral {
        self.command.arg0(argv0);
        self
    }

    pub fn home(mut self, root: &Path) -> Tetral {
        self.command.env("TETRAL_HOME", root);
        self
    }

    /// Makes the process read its clock, barriers and counters from `seams`.
    pub fn seams(mut self, seams: &Seams) -> Tetral {
        self.command.env(SOCKET_VARIABLE, seams.socket());
        self
    }

    pub fn env(mut self, name: &str, value: impl AsRef<OsStr>) -> Tetral {
        self.command.env(name, value);
        self
    }

    pub fn current_dir(mut self, directory: &Path) -> Tetral {
        self.command.current_dir(directory);
        self
    }

    pub fn stdin(mut self, bytes: impl Into<Vec<u8>>) -> Tetral {
        self.stdin = bytes.into();
        self
    }

    /// Keeps stdin open after the provided bytes, until the child exits or
    /// its owning guard is dropped. This distinguishes bounded readers from
    /// readers that wait for EOF before rejecting oversized input.
    pub fn keep_stdin_open(mut self) -> Self {
        self.stdin_open = true;
        self
    }

    pub fn socket_context(&self) -> &SocketContext {
        &self.sockets
    }

    /// A dedicated log-backpressure scenario. Ordinary runs always drain
    /// stderr immediately; this mode fills its pipe before spawn and retains
    /// the reader until explicitly drained. Prefill consists of blank lines.
    pub fn stderr_backpressure(mut self) -> Self {
        self.stderr_backpressure = true;
        self
    }

    /// Starts the child with concurrent, bounded output capture and an
    /// owned stdin writer. Dropping the returned handle reaps the child.
    pub fn spawn(mut self) -> RunningChild {
        let (stderr, blocked_stderr) = if self.stderr_backpressure {
            let (reader, mut writer) = std::io::pipe().expect("stderr pipe");
            nonblocking(&writer);
            loop {
                match writer.write(&[b'\n'; 4096]) {
                    Ok(0) => panic!("stderr prefill made no progress"),
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => panic!("stderr prefill: {error}"),
                }
            }
            let flags = rustix::fs::fcntl_getfl(&writer).expect("stderr flags");
            rustix::fs::fcntl_setfl(&writer, flags & !rustix::fs::OFlags::NONBLOCK)
                .expect("blocking stderr for the child");
            (
                Stdio::from(writer),
                Some(std::fs::File::from(OwnedFd::from(reader))),
            )
        } else {
            (Stdio::piped(), None)
        };
        let child = self
            .command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .expect("tetral starts");
        RunningChild::new(
            child,
            self.sockets,
            self.stdin,
            self.stdin_open,
            blocked_stderr,
            OUTPUT_LIMIT,
        )
    }

    pub fn run(self) -> Run {
        finish(self.spawn())
    }
}

/// Maximum retained diagnostics per stream. Excess bytes are still drained;
/// ordinary acceptance runs fail explicitly rather than inspect partial logs.
const OUTPUT_LIMIT: usize = 16 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const READER_WAIT_LIMIT: Duration = Duration::from_secs(1);

/// A child and all its I/O workers, owned from the instant it is spawned.
/// The socket context stays alive until the process and its readers end.
pub struct RunningChild {
    child: Child,
    stdout: Option<Reader>,
    stderr: Option<Reader>,
    blocked_stderr: Option<std::fs::File>,
    input: Option<Input>,
    output_limit: usize,
    _sockets: SocketContext,
}

impl RunningChild {
    fn new(
        child: Child,
        sockets: SocketContext,
        stdin: Vec<u8>,
        stdin_open: bool,
        blocked_stderr: Option<std::fs::File>,
        output_limit: usize,
    ) -> Self {
        let mut running = Self {
            child,
            stdout: None,
            stderr: None,
            blocked_stderr,
            input: None,
            output_limit,
            _sockets: sockets,
        };
        running.stdout = Some(Reader::start(
            running.child.stdout.take().expect("stdout is piped"),
            output_limit,
        ));
        if running.blocked_stderr.is_none() {
            let stderr = running.child.stderr.take().expect("stderr is piped");
            running.stderr = Some(Reader::start(stderr, output_limit));
        }
        running.input = Some(Input::start(
            running.child.stdin.take().expect("stdin is piped"),
            stdin,
            stdin_open,
        ));
        running
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait()
    }
    pub fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }

    /// Releases the explicit backpressure mode while the process still runs.
    pub fn drain_stderr(&mut self) {
        if let Some(pipe) = self.blocked_stderr.take() {
            self.stderr = Some(Reader::start(pipe, self.output_limit));
        }
    }

    fn output(&mut self) -> (Captured, Captured) {
        self.input.take();
        self.drain_stderr();
        let deadline = Instant::now() + READER_WAIT_LIMIT;
        let stdout = self
            .stdout
            .take()
            .expect("stdout capture exists")
            .finish(deadline);
        let stderr = self
            .stderr
            .take()
            .expect("stderr capture exists")
            .finish(deadline);
        (stdout, stderr)
    }
}

impl Drop for RunningChild {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        // Worker drops cancel nonblocking operations before joining; inherited
        // pipe handles cannot leave a reader or stdin writer waiting forever.
    }
}

/// Waits for a started process and collects output already being drained.
/// Timeout and panic leave the same owning handle responsible for cleanup.
pub fn finish(mut child: RunningChild) -> Run {
    let deadline = Instant::now() + WAIT_LIMIT;
    let status = loop {
        if let Some(status) = child.try_wait().expect("tetral can be waited for") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let (stdout, stderr) = child.output();
            panic!(
                "tetral did not exit within {WAIT_LIMIT:?}; stdout: {}; stderr: {}",
                stdout.diagnostic(),
                stderr.diagnostic()
            );
        }
        thread::sleep(POLL_INTERVAL);
    };
    let (stdout, stderr) = child.output();
    assert!(
        stdout.complete && stderr.complete,
        "child output did not reach EOF within {READER_WAIT_LIMIT:?}; stdout: {}; stderr: {}",
        stdout.diagnostic(),
        stderr.diagnostic()
    );
    assert!(
        stdout.total <= stdout.bytes.len() && stderr.total <= stderr.bytes.len(),
        "child output exceeded the {OUTPUT_LIMIT}-byte capture limit; stdout: {}; stderr: {}",
        stdout.diagnostic(),
        stderr.diagnostic()
    );
    Run {
        status,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
    }
}

struct Captured {
    bytes: Vec<u8>,
    total: usize,
    complete: bool,
    error: Option<String>,
}

impl Captured {
    fn diagnostic(&self) -> String {
        let mut text = String::from_utf8_lossy(&self.bytes).into_owned();
        if self.total > self.bytes.len() {
            text.push_str(&format!(
                "\n[... {} output bytes omitted by the test harness ...]",
                self.total - self.bytes.len()
            ));
        }
        if !self.complete {
            text.push_str("\n[output pipe did not reach EOF]");
        }
        if let Some(error) = &self.error {
            text.push_str(&format!("\n[output read failed: {error}]"));
        }
        text
    }
}

struct Reader {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<Captured>>,
}

impl Reader {
    fn start(mut pipe: impl Read + AsFd + Send + 'static, limit: usize) -> Self {
        nonblocking(&pipe);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let worker = thread::spawn(move || {
            let mut result = Captured {
                bytes: Vec::new(),
                total: 0,
                complete: false,
                error: None,
            };
            let mut buffer = [0; 16 * 1024];
            while !stopped.load(Ordering::Relaxed) {
                match pipe.read(&mut buffer) {
                    Ok(0) => {
                        result.complete = true;
                        break;
                    }
                    Ok(read) => {
                        result.total = result.total.saturating_add(read);
                        let retained = read.min(limit.saturating_sub(result.bytes.len()));
                        result.bytes.extend_from_slice(&buffer[..retained]);
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(POLL_INTERVAL)
                    }
                    Err(error) => {
                        result.error = Some(error.to_string());
                        break;
                    }
                }
            }
            result
        });
        Self {
            stop,
            worker: Some(worker),
        }
    }

    fn finish(mut self, deadline: Instant) -> Captured {
        let worker = self.worker.as_ref().expect("reader is running");
        while !worker.is_finished() && Instant::now() < deadline {
            thread::sleep(POLL_INTERVAL);
        }
        self.stop.store(true, Ordering::Relaxed);
        self.worker
            .take()
            .unwrap()
            .join()
            .expect("output reader finishes")
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct Input {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Input {
    fn start(
        mut pipe: impl Write + AsFd + Send + 'static,
        bytes: Vec<u8>,
        keep_open: bool,
    ) -> Self {
        nonblocking(&pipe);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let worker = thread::spawn(move || {
            let mut remaining = bytes.as_slice();
            while !remaining.is_empty() && !stopped.load(Ordering::Relaxed) {
                match pipe.write(remaining) {
                    Ok(0) => break,
                    Ok(written) => remaining = &remaining[written..],
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(POLL_INTERVAL)
                    }
                    Err(_) => break,
                }
            }
            while keep_open && !stopped.load(Ordering::Relaxed) {
                thread::sleep(POLL_INTERVAL);
            }
        });
        Self {
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn nonblocking(pipe: &impl AsFd) {
    let flags = rustix::fs::fcntl_getfl(pipe).expect("read pipe flags");
    rustix::fs::fcntl_setfl(pipe, flags | rustix::fs::OFlags::NONBLOCK)
        .expect("set nonblocking pipe");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output_child(bytes: usize, limit: usize) -> RunningChild {
        let child = Command::new("python3")
            .args([
                "-c",
                "import os, sys; n=int(sys.argv[1]); os.write(1,b'o'*n); os.write(2,b'e'*n)",
                &bytes.to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        RunningChild::new(
            child,
            socket_directory::context(),
            Vec::new(),
            false,
            None,
            limit,
        )
    }

    #[test]
    fn both_streams_are_drained_before_the_child_exits() {
        let mut child = output_child(256 * 1024, OUTPUT_LIMIT);
        let deadline = Instant::now() + Duration::from_secs(5);
        while child.try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "capture must start before finish is called"
            );
            thread::sleep(POLL_INTERVAL);
        }
        let run = finish(child);
        assert!(run.status.success());
        assert_eq!(run.stdout, vec![b'o'; 256 * 1024]);
        assert_eq!(run.stderr, vec![b'e'; 256 * 1024]);
    }

    #[test]
    fn a_full_capture_keeps_draining_and_marks_omitted_bytes() {
        let mut child = output_child(256 * 1024, 1024);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "a full capture must not block the child"
            );
            thread::sleep(POLL_INTERVAL);
        }
        let (stdout, stderr) = child.output();
        for output in [stdout, stderr] {
            assert!(output.complete);
            assert_eq!(output.total, 256 * 1024);
            assert_eq!(output.bytes.len(), 1024);
            assert!(
                output
                    .diagnostic()
                    .contains("261120 output bytes omitted by the test harness")
            );
        }
    }

    #[test]
    fn an_open_pipe_cannot_keep_a_reader_or_stdin_writer_alive_after_cancellation() {
        let (reader, mut writer) = std::io::pipe().unwrap();
        writer.write_all(b"last output").unwrap();
        let reader = Reader::start(reader, 1024);
        let start = Instant::now();
        let captured = reader.finish(start + Duration::from_millis(50));
        assert!(!captured.complete, "the live writer has not sent EOF");
        assert_eq!(captured.bytes, b"last output");
        assert!(start.elapsed() < Duration::from_secs(1));
        drop(writer);

        let (_reader, writer) = std::io::pipe().unwrap();
        let input = Input::start(writer, vec![b'i'; 1024 * 1024], false);
        let start = Instant::now();
        drop(input);
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
