//! The harness side of the seam protocol that `state::seams` defines: a
//! `tetral` built with the feature `test-seams` and started with
//! `TETRAL_TEST_SEAMS=<socket>` reads its clock from here, waits at a held
//! barrier until the test releases it, and counts named events here.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

/// The variable that names the seam socket to the process under test.
pub const SOCKET_VARIABLE: &str = "TETRAL_TEST_SEAMS";
const PROTOCOL: &str = "TETRAL_TEST_SEAMS_V1";

/// How long a test waits for the process under test before it fails; it
/// bounds a wait for something that must happen, never one for something
/// that must not.
pub const WAIT_LIMIT: Duration = Duration::from_secs(30);

/// The clock of a process under test, its barriers and its counters.
pub struct Seams {
    socket: PathBuf,
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    clock: Duration,
    held: HashSet<String>,
    waiting: HashMap<String, usize>,
    counters: HashMap<String, u64>,
    stopped: bool,
}

impl Seams {
    /// Serves the seams on a socket in `directory`. The clock starts at
    /// `start` and moves only when the test advances it; every barrier is
    /// open until the test holds it.
    pub fn serve(directory: &Path, start: Duration) -> Seams {
        let socket = directory.join("seams.sock");
        let listener = UnixListener::bind(&socket)
            .unwrap_or_else(|error| panic!("bind {}: {error}", socket.display()));
        let shared = Arc::new(Shared::default());
        shared.lock().clock = start;
        let serving = shared.clone();
        thread::spawn(move || {
            for connection in listener.incoming() {
                if serving.lock().stopped {
                    return;
                }
                let Ok(connection) = connection else { continue };
                let serving = serving.clone();
                thread::spawn(move || serving.answer(connection));
            }
        });
        Seams { socket, shared }
    }

    /// The socket to pass in [`SOCKET_VARIABLE`].
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn now(&self) -> Duration {
        self.shared.lock().clock
    }

    pub fn advance(&self, step: Duration) {
        self.shared.lock().clock += step;
        self.shared.changed.notify_all();
    }

    /// Makes every process that reaches the barrier `name` wait there.
    pub fn hold(&self, name: &str) {
        self.shared.lock().held.insert(name.to_owned());
    }

    /// Lets every waiting and later process pass the barrier `name`.
    pub fn release(&self, name: &str) {
        self.shared.lock().held.remove(name);
        self.shared.changed.notify_all();
    }

    /// Waits until `count` processes wait at the held barrier `name`.
    pub fn wait_until_waiting(&self, name: &str, count: usize) {
        self.wait_for(|state| state.waiting.get(name).copied().unwrap_or(0) >= count);
    }

    /// How often the counter `name` was reported.
    pub fn count(&self, name: &str) -> u64 {
        self.shared.lock().counters.get(name).copied().unwrap_or(0)
    }

    /// Waits until the counter `name` reaches `count`.
    pub fn wait_for_count(&self, name: &str, count: u64) {
        self.wait_for(|state| state.counters.get(name).copied().unwrap_or(0) >= count);
    }

    fn wait_for(&self, condition: impl Fn(&State) -> bool) {
        let deadline = Instant::now() + WAIT_LIMIT;
        let mut state = self.shared.lock();
        while !condition(&state) {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "the process under test never got there");
            state = self
                .shared
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }
}

impl Drop for Seams {
    fn drop(&mut self) {
        self.shared.lock().stopped = true;
        self.shared.changed.notify_all();
        let _ = UnixStream::connect(&self.socket);
        let _ = std::fs::remove_file(&self.socket);
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn answer(&self, connection: UnixStream) {
        let mut line = String::new();
        if BufReader::new(&connection).read_line(&mut line).is_err() {
            return;
        }
        let words: Vec<&str> = line.split_whitespace().collect();
        let answer = match words.as_slice() {
            [PROTOCOL, _pid, "clock"] => self.lock().clock.as_nanos().to_string(),
            [PROTOCOL, _pid, "barrier", name] => {
                self.pass(name);
                "released".to_owned()
            }
            [PROTOCOL, _pid, "count", name] => {
                *self.lock().counters.entry((*name).to_owned()).or_default() += 1;
                self.changed.notify_all();
                "ok".to_owned()
            }
            _ => "error".to_owned(),
        };
        let _ = (&connection).write_all(format!("{answer}\n").as_bytes());
    }

    fn pass(&self, name: &str) {
        let mut state = self.lock();
        *state.waiting.entry(name.to_owned()).or_default() += 1;
        self.changed.notify_all();
        while state.held.contains(name) && !state.stopped {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *state.waiting.entry(name.to_owned()).or_default() -= 1;
    }
}
