//! The CLI's monotonic clock and its test seams.
//!
//! Every timer and deadline of the CLI reads [`now`]: a monotonic clock that
//! counts the time the machine sleeps, `CLOCK_BOOTTIME` on Linux and
//! `clock_gettime(CLOCK_MONOTONIC)` on macOS, so a deadline that passes
//! during a sleep is seen as passed on waking. [`Instant`](std::time::Instant)
//! stops during a sleep on Linux and is never used for one.
//!
//! [`barrier`], [`barrier_blocking`] and [`count`] do nothing in a normal
//! build. With the crate feature `test-seams`, a process whose environment
//! holds `TETRAL_TEST_SEAMS=<socket path>` instead takes the clock's time
//! from the test harness listening on that Unix socket, waits at each named
//! barrier until the harness releases it, and reports each named counter.

use std::time::Duration;

/// The CLI's monotonic clock: the time since an arbitrary origin, the
/// machine's sleep included.
pub fn now() -> Duration {
    #[cfg(feature = "test-seams")]
    if let Some(harness) = harness::named() {
        return harness.clock();
    }
    system_clock()
}

fn system_clock() -> Duration {
    #[cfg(target_os = "linux")]
    let clock = rustix::time::ClockId::Boottime;
    #[cfg(target_os = "macos")]
    let clock = rustix::time::ClockId::Monotonic;
    let reading = rustix::time::clock_gettime(clock);
    Duration::new(
        reading.tv_sec.try_into().unwrap_or(0),
        reading.tv_nsec.try_into().unwrap_or(0),
    )
}

/// How long [`sleep_until`] waits before it reads the clock again, so that a
/// deadline that passes while the machine sleeps ends the wait within this
/// interval of waking.
pub const RECHECK_INTERVAL: Duration = Duration::from_secs(1);

/// [`RECHECK_INTERVAL`] while the test harness serves the clock, which moves
/// only when a test advances it.
#[cfg(feature = "test-seams")]
pub const HARNESS_RECHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Waits until [`now`] reaches `deadline`. It never sleeps through one timer
/// set for the whole wait: the runtime's timers do not count the machine's
/// sleep, so it reads the clock again after each recheck interval.
pub async fn sleep_until(deadline: Duration) {
    loop {
        let remaining = deadline.saturating_sub(now());
        if remaining.is_zero() {
            return;
        }
        tokio::time::sleep(remaining.min(recheck_interval())).await;
    }
}

/// The longest a wait for a time on [`now`] sleeps before it reads the
/// clock again.
pub fn recheck_interval() -> Duration {
    #[cfg(feature = "test-seams")]
    if harness::named().is_some() {
        return HARNESS_RECHECK_INTERVAL;
    }
    RECHECK_INTERVAL
}

/// Reads the clock once from the test harness that the environment names,
/// so that a harness that does not answer fails the process at its start.
pub fn check_harness() {
    #[cfg(feature = "test-seams")]
    harness::check();
}

/// The environment variable that names this process's test harness, for a
/// child process that runs with an otherwise empty environment.
pub fn harness_variable() -> Option<(&'static str, std::ffi::OsString)> {
    #[cfg(feature = "test-seams")]
    if let Some(harness) = harness::named() {
        return Some((harness::SOCKET_VARIABLE, harness.socket().into()));
    }
    None
}

/// Waits until the test harness releases the barrier `name`.
pub async fn barrier(name: &'static str) {
    #[cfg(feature = "test-seams")]
    if let Some(harness) = harness::named() {
        let waited = tokio::task::spawn_blocking(move || harness.barrier(name)).await;
        waited.expect("the barrier's wait panicked");
    }
    #[cfg(not(feature = "test-seams"))]
    let _ = name;
}

/// [`barrier`] for a thread outside the async runtime, such as a child
/// process's.
pub fn barrier_blocking(name: &'static str) {
    #[cfg(feature = "test-seams")]
    if let Some(harness) = harness::named() {
        harness.barrier(name);
    }
    #[cfg(not(feature = "test-seams"))]
    let _ = name;
}

/// Reports one occurrence of the counter `name` to the test harness.
pub fn count(name: &'static str) {
    #[cfg(feature = "test-seams")]
    if let Some(harness) = harness::named() {
        harness.count(name);
    }
    #[cfg(not(feature = "test-seams"))]
    let _ = name;
}

/// The process side of the seam protocol. Each request is one connection
/// to the harness's socket carrying one line, `TETRAL_TEST_SEAMS_V1 <pid>
/// <operation> [<name>]`, answered by one line: the clock's nanoseconds for
/// `clock`, `released` for `barrier` and `ok` for `count`. A connection per
/// request lets one task wait at a barrier while others read the clock.
#[cfg(feature = "test-seams")]
mod harness {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::time::Duration;

    /// The first word of every request, naming the protocol's version.
    pub const PROTOCOL: &str = "TETRAL_TEST_SEAMS_V1";
    /// The variable that names the harness's socket.
    pub const SOCKET_VARIABLE: &str = "TETRAL_TEST_SEAMS";

    #[derive(Debug, Clone)]
    pub struct Harness {
        socket: PathBuf,
    }

    static HARNESS: OnceLock<Option<Harness>> = OnceLock::new();

    /// The harness that `TETRAL_TEST_SEAMS` names, if any.
    pub fn named() -> Option<Harness> {
        HARNESS
            .get_or_init(|| {
                std::env::var_os(SOCKET_VARIABLE).map(|socket| Harness {
                    socket: socket.into(),
                })
            })
            .clone()
    }

    pub fn check() {
        if let Some(harness) = named() {
            harness.clock();
        }
    }

    impl Harness {
        pub fn socket(&self) -> &std::path::Path {
            &self.socket
        }

        fn request(&self, operation: &str, name: Option<&str>) -> String {
            let mut line = format!("{PROTOCOL} {} {operation}", std::process::id());
            if let Some(name) = name {
                assert!(
                    !name.is_empty() && name.bytes().all(|byte| byte.is_ascii_graphic()),
                    "a seam name is one printable ASCII word"
                );
                line.push(' ');
                line.push_str(name);
            }
            line.push('\n');
            let socket = &self.socket;
            let mut stream = UnixStream::connect(socket).unwrap_or_else(|error| {
                panic!(
                    "the test harness at {} does not answer: {error}",
                    socket.display()
                )
            });
            stream
                .write_all(line.as_bytes())
                .expect("the test harness takes the request");
            let mut answer = String::new();
            BufReader::new(stream)
                .read_line(&mut answer)
                .expect("the test harness answers");
            answer.trim_end().to_owned()
        }

        pub fn clock(&self) -> Duration {
            let answer = self.request("clock", None);
            Duration::from_nanos(
                answer.parse().unwrap_or_else(|_| {
                    panic!("the test harness answered the clock with {answer:?}")
                }),
            )
        }

        pub fn barrier(&self, name: &str) {
            let answer = self.request("barrier", Some(name));
            assert_eq!(
                answer, "released",
                "the test harness answered barrier {name}"
            );
        }

        pub fn count(&self, name: &str) {
            let answer = self.request("count", Some(name));
            assert_eq!(answer, "ok", "the test harness answered counter {name}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock_that_counts_sleep() -> Duration {
        #[cfg(target_os = "linux")]
        let clock = rustix::time::ClockId::Boottime;
        #[cfg(target_os = "macos")]
        let clock = rustix::time::ClockId::Monotonic;
        let reading = rustix::time::clock_gettime(clock);
        Duration::new(reading.tv_sec as u64, reading.tv_nsec as u32)
    }

    #[test]
    fn the_clock_reads_the_clock_that_counts_the_machines_sleep() {
        let before = clock_that_counts_sleep();
        let reading = now();
        let after = clock_that_counts_sleep();
        assert!(
            before <= reading && reading <= after,
            "{reading:?} is not between {before:?} and {after:?}"
        );
    }

    #[test]
    fn without_a_harness_a_wait_reads_the_clock_again_at_least_once_per_second() {
        assert_eq!(recheck_interval(), Duration::from_secs(1));
    }
}
