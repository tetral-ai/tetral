//! Runs the `tetral` under test as a child process with a given environment
//! and stdin.

use std::ffi::OsStr;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::binaries;
use crate::seams::{SOCKET_VARIABLE, Seams, WAIT_LIMIT};

/// A finished run.
#[derive(Debug)]
pub struct Run {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Run {
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
}

impl Tetral {
    pub fn new<I, S>(arguments: I) -> Tetral
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(binaries::tetral());
        command.args(arguments).env_clear();
        Tetral {
            command,
            stdin: Vec::new(),
        }
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

    pub fn stdin(mut self, bytes: impl Into<Vec<u8>>) -> Tetral {
        self.stdin = bytes.into();
        self
    }

    /// Starts the process and writes its stdin from another thread, then
    /// closes it; a process that exits without reading it is no error.
    pub fn spawn(mut self) -> Child {
        let mut child = self
            .command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("tetral starts");
        let mut stdin = child.stdin.take().expect("stdin is piped");
        let bytes = self.stdin;
        std::thread::spawn(move || {
            let _ = stdin.write_all(&bytes);
        });
        child
    }

    pub fn run(self) -> Run {
        finish(self.spawn())
    }
}

/// Waits for a started process and collects its output. A process that has
/// not exited within [`WAIT_LIMIT`] is killed and fails the test, so that a
/// command that blocks ends the test instead of hanging it.
pub fn finish(mut child: Child) -> Run {
    let stdout = drain(child.stdout.take().expect("stdout is piped"));
    let stderr = drain(child.stderr.take().expect("stderr is piped"));
    let deadline = Instant::now() + WAIT_LIMIT;
    let status = loop {
        if let Some(status) = child.try_wait().expect("tetral can be waited for") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let stderr = stderr.join().expect("stderr is read");
            panic!(
                "tetral did not exit within {WAIT_LIMIT:?}; stderr: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        thread::sleep(POLL_INTERVAL);
    };
    Run {
        status,
        stdout: stdout.join().expect("stdout is read"),
        stderr: stderr.join().expect("stderr is read"),
    }
}

/// How often [`finish`] checks whether the process has exited.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

fn drain(mut pipe: impl Read + Send + 'static) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        bytes
    })
}
