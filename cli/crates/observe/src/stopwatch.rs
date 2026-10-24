//! Durations on the caller's monotonic clock, and the startup phases of a
//! `server run` process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::record::RecordEnum;

/// The monotonic clock a caller passes in, so that a test can control it.
pub type MonotonicClock = fn() -> Duration;

/// A duration in whole milliseconds, measured by a [`Stopwatch`] or by
/// [`Startup`]; a record's duration field holds nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ms(u64);

impl Ms {
    fn between(start: Duration, end: Duration) -> Ms {
        Ms(end
            .saturating_sub(start)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX))
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

// A duration travels as a 128-bit integer, which no other `RecordValue`
// records, so that a duration field can tell an `Ms` from a plain number.
impl crate::record::RecordValue for Ms {
    type Value<'a> = u128;
    fn record_value(&self) -> u128 {
        u128::from(self.0)
    }
}

/// Measures from its start on the clock its caller passes in.
#[derive(Debug, Clone, Copy)]
pub struct Stopwatch {
    clock: MonotonicClock,
    started: Duration,
}

impl Stopwatch {
    pub fn start(clock: MonotonicClock) -> Stopwatch {
        Stopwatch {
            clock,
            started: clock(),
        }
    }

    pub fn elapsed(&self) -> Ms {
        Ms::between(self.started, (self.clock)())
    }
}

/// The three phases of a `server run` start, each from the end of the one
/// before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupPhases {
    pub config: Ms,
    pub connect: Ms,
    pub ready: Ms,
}

#[derive(Clone, Copy)]
enum Phase {
    Config,
    Connect,
    Ready,
}

impl RecordEnum for Phase {
    fn record_str(&self) -> &'static str {
        match self {
            Phase::Config => "config",
            Phase::Connect => "connect",
            Phase::Ready => "ready",
        }
    }
}

enum ColdStartKind {
    Cli,
}

impl RecordEnum for ColdStartKind {
    fn record_str(&self) -> &'static str {
        match self {
            ColdStartKind::Cli => "cli",
        }
    }
}

const UNSET: u64 = u64::MAX;

/// The process-start instant and the instants that end each startup phase,
/// the last being the first send of the ready that is accepted. Only atomics,
/// so any task may pass an instant or read the phases.
pub struct Startup {
    process_start: AtomicU64,
    config_loaded: AtomicU64,
    connected: AtomicU64,
    accepted_ready_sent: AtomicU64,
}

impl Default for Startup {
    fn default() -> Startup {
        Startup::new()
    }
}

impl Startup {
    pub const fn new() -> Startup {
        Startup {
            process_start: AtomicU64::new(UNSET),
            config_loaded: AtomicU64::new(UNSET),
            connected: AtomicU64::new(UNSET),
            accepted_ready_sent: AtomicU64::new(UNSET),
        }
    }

    /// Takes the process-start instant and records the start of `config`.
    pub fn process_started(&self, now: Duration) {
        self.process_start.store(nanos(now), Ordering::Release);
        phase_started(Phase::Config);
    }

    /// The configuration is read: `config` ends and `connect` starts.
    pub fn config_loaded(&self, now: Duration) {
        if store_once(&self.config_loaded, now) {
            let config_ms = between(load(&self.process_start), load(&self.config_loaded));
            crate::info!(
                "cold_start.phase_completed",
                cold_start.kind = ColdStartKind::Cli,
                cold_start.phase = Phase::Config,
                config_ms = config_ms
            );
            phase_started(Phase::Connect);
        }
    }

    /// The first `connected` event: `connect` ends and `ready` starts. Later
    /// events change nothing.
    pub fn first_connected(&self, now: Duration) {
        if store_once(&self.connected, now) {
            let connect_ms = between(load(&self.config_loaded), load(&self.connected));
            crate::info!(
                "cold_start.phase_completed",
                cold_start.kind = ColdStartKind::Cli,
                cold_start.phase = Phase::Connect,
                connect_ms = connect_ms
            );
            phase_started(Phase::Ready);
        }
    }

    /// The phases as of a ready first sent at `first_send`: its `ready` phase
    /// runs from the first `connected` event to that send.
    pub fn phases(&self, first_send: Duration) -> StartupPhases {
        self.phases_to(nanos(first_send))
    }

    /// The ready first sent at `first_send` was accepted, which ends `ready`.
    /// Later acceptances change and record nothing.
    pub fn ready_accepted(&self, first_send: Duration) {
        if store_once(&self.accepted_ready_sent, first_send) {
            crate::info!(
                "cold_start.phase_completed",
                cold_start.kind = ColdStartKind::Cli,
                cold_start.phase = Phase::Ready,
                ready_ms = between(load(&self.connected), load(&self.accepted_ready_sent))
            );
        }
    }

    /// The phases of the start, once a ready is accepted.
    pub fn accepted_phases(&self) -> Option<StartupPhases> {
        let sent = self.accepted_ready_sent.load(Ordering::Acquire);
        (sent != UNSET).then(|| self.phases_to(sent))
    }

    fn phases_to(&self, ready_sent: u64) -> StartupPhases {
        StartupPhases {
            config: between(load(&self.process_start), load(&self.config_loaded)),
            connect: between(load(&self.config_loaded), load(&self.connected)),
            ready: between(load(&self.connected), ready_sent),
        }
    }
}

fn phase_started(phase: Phase) {
    crate::info!(
        "cold_start.phase_started",
        cold_start.kind = ColdStartKind::Cli,
        cold_start.phase = phase
    );
}

fn nanos(at: Duration) -> u64 {
    at.as_nanos().try_into().unwrap_or(UNSET - 1)
}

/// Stores `now` into an instant that was never set, telling whether it did.
fn store_once(instant: &AtomicU64, now: Duration) -> bool {
    instant
        .compare_exchange(UNSET, nanos(now), Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

fn load(instant: &AtomicU64) -> u64 {
    instant.load(Ordering::Acquire)
}

fn between(start: u64, end: u64) -> Ms {
    if start == UNSET || end == UNSET {
        return Ms(0);
    }
    Ms::between(Duration::from_nanos(start), Duration::from_nanos(end))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::level::Level;
    use crate::record::tests::record_with;

    static NOW_MS: AtomicU64 = AtomicU64::new(0);

    fn controlled() -> Duration {
        Duration::from_millis(NOW_MS.load(Ordering::SeqCst))
    }

    #[test]
    fn a_stopwatch_reads_the_clock_it_is_given() {
        NOW_MS.store(1_000, Ordering::SeqCst);
        let stopwatch = Stopwatch::start(controlled);
        NOW_MS.store(3_500, Ordering::SeqCst);
        assert_eq!(stopwatch.elapsed(), Ms(2_500));
    }

    #[test]
    fn each_phase_runs_from_the_end_of_the_one_before_and_records_its_events_once() {
        let startup = Startup::new();
        let at = Duration::from_millis;
        let (stderr, _) = record_with(Level::Info, || {
            startup.process_started(at(100));
            startup.config_loaded(at(350));
            startup.config_loaded(at(500));
            startup.first_connected(at(2_350));
            startup.first_connected(at(9_000));
            let phases = StartupPhases {
                config: Ms(250),
                connect: Ms(2_000),
                ready: Ms(50),
            };
            assert_eq!(startup.phases(at(2_400)), phases);
            assert_eq!(startup.accepted_phases(), None);
            startup.ready_accepted(at(2_400));
            startup.ready_accepted(at(3_000));
            assert_eq!(startup.accepted_phases(), Some(phases));
        });
        let records: Vec<(String, String, Option<u64>)> = stderr
            .records()
            .iter()
            .map(|record| {
                let phase = record["cold_start.phase"].as_str().unwrap().to_owned();
                let ms = record.get(format!("{phase}_ms")).and_then(|ms| ms.as_u64());
                (record["event"].as_str().unwrap().to_owned(), phase, ms)
            })
            .collect();
        let expected = [
            ("cold_start.phase_started", "config", None),
            ("cold_start.phase_completed", "config", Some(250)),
            ("cold_start.phase_started", "connect", None),
            ("cold_start.phase_completed", "connect", Some(2_000)),
            ("cold_start.phase_started", "ready", None),
            ("cold_start.phase_completed", "ready", Some(50)),
        ]
        .map(|(event, phase, ms)| (event.to_owned(), phase.to_owned(), ms));
        assert_eq!(records, expected);
    }

    #[test]
    fn a_duration_field_holds_only_a_reading() {
        let (stderr, _) = record_with(Level::Info, || {
            crate::info!(
                "cold_start.phase_completed",
                cold_start.kind = ColdStartKind::Cli,
                cold_start.phase = Phase::Config,
                config_ms = 250u64
            );
        });
        assert!(stderr.records()[0].get("config_ms").is_none());
    }
}
