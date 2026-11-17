//! The harness side of the seam protocol that `state::seams` defines: a
//! `tetral` built with the feature `test-seams` and started with
//! `TETRAL_TEST_SEAMS=<socket>` reads its clock from here, in the file this
//! side keeps beside the socket, waits at a held barrier until the test
//! releases it, and counts named events here.
//!
//! Each step of the clock, each read of the clock through the socket (a
//! process's first; it reads the file after), each barrier a process waits
//! at and each count it reports goes on the test's timeline, and a failure's
//! records get the clock, the barriers and the counters as they stand then.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use crate::timeline;

/// The variable that names the seam socket to the process under test.
pub const SOCKET_VARIABLE: &str = "TETRAL_TEST_SEAMS";
const PROTOCOL: &str = "TETRAL_TEST_SEAMS_V1";

/// How long a test waits for the process under test before it fails; it
/// bounds a wait for something that must happen, never one for something
/// that must not.
pub const WAIT_LIMIT: Duration = Duration::from_secs(30);

/// How long a failure's records wait for the clock's state, which the
/// harness's threads each hold only briefly.
const STANDING_WAIT: Duration = Duration::from_millis(100);

/// The clock of a process under test, its barriers and its counters.
pub struct Seams {
    socket: PathBuf,
    shared: Arc<Shared>,
    _at_failure: timeline::Registered,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    /// The file the process under test reads the clock from.
    clock: File,
    /// The clock's name on the timeline.
    label: String,
    start: Duration,
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
            label: timeline::label("seams"),
            start,
        });
        shared.set_clock(&mut shared.lock(), start);
        timeline::note(
            &shared.label,
            format!("the clock starts at {}", shared.time(start)),
        );
        let standing = Arc::downgrade(&shared);
        let at_failure = timeline::at_failure(&shared.label, move || {
            let Some(shared) = standing.upgrade() else {
                return timeline::AtFailure::default();
            };
            timeline::AtFailure {
                lines: vec![(timeline::now(), shared.standing())],
                files: Vec::new(),
            }
        });
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
        Seams {
            socket,
            shared,
            _at_failure: at_failure,
        }
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
        timeline::note(
            &self.shared.label,
            format!(
                "the test moves the clock by {step:?} to {}",
                self.shared.time(moved)
            ),
        );
        self.shared.changed.notify_all();
    }

    /// How far the clock has moved since its start.
    pub fn advanced(&self) -> Duration {
        self.shared.lock().advanced
    }

    /// Makes every process that reaches the barrier `name` wait there.
    pub fn hold(&self, name: &str) {
        self.shared.lock().held.insert(name.to_owned());
        timeline::note(&self.shared.label, format!("the test holds barrier {name}"));
    }

    /// Lets every waiting and later process pass the barrier `name`.
    pub fn release(&self, name: &str) {
        let mut state = self.shared.lock();
        state.held.remove(name);
        state.passes.remove(name);
        drop(state);
        timeline::note(
            &self.shared.label,
            format!("the test releases barrier {name}"),
        );
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
        timeline::note(
            &self.shared.label,
            format!("the test lets one process pass barrier {name}"),
        );
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
            if left.is_zero() {
                // Unlocked first: the failure's records read the state.
                drop(state);
                panic!("the process under test never got there");
            }
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

    /// The clock, barriers and counters as they stand, for a failure's
    /// records. It never blocks: the panicking thread may hold the state.
    fn standing(&self) -> String {
        let deadline = Instant::now() + STANDING_WAIT;
        let state = loop {
            match self.state.try_lock() {
                Ok(state) => break state,
                Err(TryLockError::Poisoned(poisoned)) => break poisoned.into_inner(),
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(TryLockError::WouldBlock) => {
                    return format!(
                        "its state stayed held for {STANDING_WAIT:?}, by the panicking thread or another, so its standing is not known"
                    );
                }
            }
        };
        let sorted = |map: &HashMap<String, u64>| {
            map.iter()
                .filter(|(_, count)| **count > 0)
                .map(|(name, count)| format!("{name}={count}"))
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", ")
        };
        let waiting: HashMap<String, u64> = state
            .waiting
            .iter()
            .map(|(name, count)| (name.clone(), *count as u64))
            .collect();
        let mut held: Vec<&String> = state.held.iter().collect();
        held.sort();
        format!(
            "stands at {}; held barriers [{}]; waiting [{}]; counters [{}]",
            self.time(state.clock),
            held.iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            sorted(&waiting),
            sorted(&state.counters)
        )
    }

    /// `clock` as the timeline shows it: its time and its distance from the
    /// start.
    fn time(&self, clock: Duration) -> String {
        format!(
            "{:.3} s (start + {:.3} s)",
            clock.as_secs_f64(),
            clock.saturating_sub(self.start).as_secs_f64()
        )
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
            [PROTOCOL, pid, "clock"] => {
                let clock = self.lock().clock;
                timeline::note(
                    &self.label,
                    format!("process {pid} reads the clock at {}", self.time(clock)),
                );
                clock.as_nanos().to_string()
            }
            [PROTOCOL, pid, "barrier", name] => {
                self.pass(pid, name);
                "released".to_owned()
            }
            [PROTOCOL, pid, "count", name] => {
                *self.lock().counters.entry((*name).to_owned()).or_default() += 1;
                timeline::note_repeated(
                    &self.label,
                    &format!("{pid} {name}"),
                    format!("process {pid} counts {name}"),
                );
                self.changed.notify_all();
                "ok".to_owned()
            }
            _ => "error".to_owned(),
        }
    }

    fn pass(&self, pid: &str, name: &str) {
        let mut state = self.lock();
        *state.waiting.entry(name.to_owned()).or_default() += 1;
        self.changed.notify_all();
        let held = state.held.contains(name);
        if held {
            timeline::note(
                &self.label,
                format!("process {pid} waits at barrier {name}"),
            );
        }
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
        drop(state);
        if held {
            timeline::note(&self.label, format!("process {pid} passes barrier {name}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failure_reads_the_clock_without_waiting_for_a_held_state() {
        let directory = tempfile::tempdir().unwrap();
        let seams = Seams::serve(directory.path(), Duration::from_secs(1000));
        let (taken, held) = std::sync::mpsc::channel();
        let shared = seams.shared.clone();
        let holder = thread::spawn(move || {
            let _state = shared.lock();
            taken.send(()).unwrap();
            thread::sleep(STANDING_WAIT * 3);
        });
        held.recv().unwrap();
        let while_held = seams.shared.standing();
        holder.join().unwrap();
        assert!(
            while_held.contains("its standing is not known"),
            "{while_held}"
        );
        let after = seams.shared.standing();
        assert!(after.starts_with("stands at 1000.000 s"), "{after}");
    }
}
