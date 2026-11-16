//! The harness side of the seam protocol that `state::seams` defines: a
//! `tetral` built with the feature `test-seams` and started with
//! `TETRAL_TEST_SEAMS=<socket>` reads its clock from here, in the file this
//! side keeps beside the socket, waits at a held barrier until the test
//! releases it, and counts named events here.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::FileExt;
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

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    /// The file the process under test reads the clock from.
    clock: File,
}

#[derive(Default)]
struct State {
    clock: Duration,
    /// How far tests have moved the clock.
    advanced: Duration,
    held: HashSet<String>,
    /// Per held barrier, how many more processes may pass it.
    passes: HashMap<String, usize>,
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
        let clock = socket.with_extension("clock");
        let shared = Arc::new(Shared {
            state: Mutex::default(),
            changed: Condvar::new(),
            clock: File::create(&clock)
                .unwrap_or_else(|error| panic!("create {}: {error}", clock.display())),
        });
        shared.set_clock(&mut shared.lock(), start);
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

    /// Moves the clock by `step`. A process under test that reads the clock
    /// after this returns reads the new time.
    pub fn advance(&self, step: Duration) {
        let mut state = self.shared.lock();
        let moved = state.clock + step;
        self.shared.set_clock(&mut state, moved);
        state.advanced += step;
        drop(state);
        self.shared.changed.notify_all();
    }

    /// How far the clock has moved since its start.
    pub fn advanced(&self) -> Duration {
        self.shared.lock().advanced
    }

    /// Makes every process that reaches the barrier `name` wait there.
    pub fn hold(&self, name: &str) {
        self.shared.lock().held.insert(name.to_owned());
    }

    /// Lets every waiting and later process pass the barrier `name`.
    pub fn release(&self, name: &str) {
        let mut state = self.shared.lock();
        state.held.remove(name);
        state.passes.remove(name);
        drop(state);
        self.shared.changed.notify_all();
    }

    /// Lets one process pass the held barrier `name`, one that waits there
    /// or the next to come; it stays held for the others.
    pub fn release_one(&self, name: &str) {
        *self
            .shared
            .lock()
            .passes
            .entry(name.to_owned())
            .or_default() += 1;
        self.shared.changed.notify_all();
    }

    /// Waits until `count` processes wait at the held barrier `name`.
    pub fn wait_until_waiting(&self, name: &str, count: usize) {
        self.wait_for(|state| state.waiting.get(name).copied().unwrap_or(0) >= count);
    }

    /// How many processes wait at the held barrier `name` now.
    pub fn waiting(&self, name: &str) -> usize {
        self.shared.lock().waiting.get(name).copied().unwrap_or(0)
    }

    /// How often the counter `name` was reported.
    pub fn count(&self, name: &str) -> u64 {
        self.shared.lock().counters.get(name).copied().unwrap_or(0)
    }

    /// Waits until the counter `name` reaches `count`.
    pub fn wait_for_count(&self, name: &str, count: u64) {
        self.wait_for(|state| state.counters.get(name).copied().unwrap_or(0) >= count);
    }

    /// Moves the clock by `step`, short of a counted wait's deadline, and
    /// waits until the wait counted as `counter` has read the clock twice
    /// since, so the process under test has seen the new time and waits on.
    pub fn advance_within(&self, step: Duration, counter: &str) {
        self.advance(step);
        self.wait_for_more(counter, 2);
    }

    /// Waits until the counter `name` has grown by `by` from now.
    pub fn wait_for_more(&self, name: &str, by: u64) {
        let start = self.count(name);
        self.wait_for_count(name, start + by);
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
        let _ = std::fs::remove_file(self.socket.with_extension("clock"));
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Sets the clock and writes it as `state::seams` reads it: its
    /// nanoseconds twice, in one write.
    fn set_clock(&self, state: &mut State, clock: Duration) {
        state.clock = clock;
        let nanoseconds = u64::try_from(clock.as_nanos())
            .expect("a clock within 584 years")
            .to_le_bytes();
        self.clock
            .write_all_at(&[nanoseconds, nanoseconds].concat(), 0)
            .expect("the clock's file");
    }

    /// Answers the requests of one connection, one at a time, until the
    /// process under test closes it.
    fn answer(&self, connection: UnixStream) {
        let mut requests = BufReader::new(&connection);
        let mut line = String::new();
        while requests.read_line(&mut line).is_ok_and(|read| read > 0) {
            let answer = self.answer_one(&line);
            if (&connection)
                .write_all(format!("{answer}\n").as_bytes())
                .is_err()
            {
                return;
            }
            line.clear();
        }
    }

    fn answer_one(&self, line: &str) -> String {
        let words: Vec<&str> = line.split_whitespace().collect();
        match words.as_slice() {
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
        }
    }

    fn pass(&self, name: &str) {
        let mut state = self.lock();
        *state.waiting.entry(name.to_owned()).or_default() += 1;
        self.changed.notify_all();
        while state.held.contains(name) && !state.stopped {
            if let Some(passes) = state.passes.get_mut(name)
                && *passes > 0
            {
                *passes -= 1;
                break;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *state.waiting.entry(name.to_owned()).or_default() -= 1;
    }
}
