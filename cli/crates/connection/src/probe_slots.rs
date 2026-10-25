//! The probe slots: the Connection owner's count of the directory probes
//! that run, at most `PROBES_MAX`, and the requests that wait for a slot in
//! arrival order, each for at most `PROBE_WAIT`. A slot is taken from its
//! grant until the Session binding owner returns it, after reaping its probe.
//!
//! The slots are the pressure point `tetral_probes`: its series are set at
//! each change and at each check of the owner's times, and a request
//! answered `Exhausted` is a rejection with the cause `timeout`.

use std::collections::VecDeque;
use std::time::Duration;

use observe::pressure::{Episodes, Name, QUIET_INTERVAL};
use observe::{Counter, Gauge, Histogram};
use protocol::id::SafeId;
use protocol::ports::ProbeRequest;

/// The directory probes that may run at once.
pub const PROBES_MAX: usize = 8;

/// The time a request waits for a slot before it is answered `Exhausted`.
pub const PROBE_WAIT: Duration = Duration::from_secs(5);

const POINT: Name = Name("tetral_probes");
const TIMEOUT: Name = Name("timeout");

/// The limits of the probe slots, each a constant but for tests.
#[derive(Debug, Clone, Copy)]
pub struct SlotLimits {
    pub probes_max: usize,
    pub wait: Duration,
    pub pressure_quiet: Duration,
}

impl Default for SlotLimits {
    fn default() -> SlotLimits {
        SlotLimits {
            probes_max: PROBES_MAX,
            wait: PROBE_WAIT,
            pressure_quiet: QUIET_INTERVAL,
        }
    }
}

/// A request and the Session binding owner that made it.
pub type Requester = (SafeId, ProbeRequest);

#[derive(Debug)]
struct Waiting {
    requester: Requester,
    since: Duration,
}

#[derive(Debug)]
struct Series {
    in_progress: Gauge,
    queued: Gauge,
    oldest_queued: Gauge,
    queue_wait: Histogram,
    rejected: Counter,
}

#[derive(Debug)]
pub struct ProbeSlots {
    limits: SlotLimits,
    running: usize,
    waiting: VecDeque<Waiting>,
    series: Series,
    episodes: Episodes,
}

impl ProbeSlots {
    pub fn new(limits: SlotLimits) -> ProbeSlots {
        let registry = observe::registry();
        registry
            .gauge("tetral_probes_limit", &[])
            .set(limits.probes_max as f64);
        ProbeSlots {
            limits,
            running: 0,
            waiting: VecDeque::new(),
            series: Series {
                in_progress: registry.gauge("tetral_probes_in_progress", &[]),
                queued: registry.gauge("tetral_probes_queued", &[]),
                oldest_queued: registry.gauge("tetral_probes_oldest_queued_seconds", &[]),
                queue_wait: registry.histogram("tetral_probes_queue_wait_seconds", &[]),
                rejected: registry.counter("tetral_probes_rejected_total", &[("cause", "timeout")]),
            },
            episodes: Episodes::new(POINT, limits.pressure_quiet),
        }
    }

    /// Takes a request at `now`: granted at once, `true`, when a slot is
    /// free and no request waits; otherwise it waits.
    pub fn request(&mut self, requester: Requester, now: Duration) -> bool {
        let granted = self.running < self.limits.probes_max && self.waiting.is_empty();
        if granted {
            self.grant(now, Duration::ZERO);
        } else {
            self.waiting.push_back(Waiting {
                requester,
                since: now,
            });
        }
        self.show(now);
        granted
    }

    pub fn withdraw(&mut self, requester: &Requester, now: Duration) {
        self.waiting
            .retain(|waiting| waiting.requester != *requester);
        self.show(now);
    }

    /// A slot comes back; the longest-waiting request takes it.
    pub fn free(&mut self, now: Duration) -> Option<Requester> {
        self.running = self.running.saturating_sub(1);
        let next = if self.running < self.limits.probes_max {
            self.waiting.pop_front()
        } else {
            None
        };
        if let Some(next) = &next {
            self.grant(now, now.saturating_sub(next.since));
        }
        self.show(now);
        next.map(|next| next.requester)
    }

    /// The requests that have waited `PROBE_WAIT` by `now`, to be answered
    /// `Exhausted`.
    pub fn expire(&mut self, now: Duration) -> Vec<Requester> {
        let mut expired = Vec::new();
        while let Some(front) = self.waiting.front()
            && now >= front.since + self.limits.wait
        {
            if let Some(waiting) = self.waiting.pop_front() {
                self.series.rejected.increment();
                self.episodes.rejected(TIMEOUT, now);
                expired.push(waiting.requester);
            }
        }
        self.show(now);
        expired
    }

    /// Every waiting request, to be answered `Cancelled`.
    pub fn cancel(&mut self, now: Duration) -> Vec<Requester> {
        let cancelled = self
            .waiting
            .drain(..)
            .map(|waiting| waiting.requester)
            .collect();
        self.show(now);
        cancelled
    }

    pub fn running(&self) -> usize {
        self.running
    }

    /// The time the longest-waiting request is answered `Exhausted`.
    pub fn next_time(&self) -> Option<Duration> {
        self.waiting
            .front()
            .map(|waiting| waiting.since + self.limits.wait)
    }

    fn grant(&mut self, now: Duration, waited: Duration) {
        self.running += 1;
        self.episodes.admitted(now);
        self.series.queue_wait.observe(waited.as_secs_f64());
    }

    fn show(&self, now: Duration) {
        self.series.in_progress.set(self.running as f64);
        self.series.queued.set(self.waiting.len() as f64);
        let oldest = self
            .waiting
            .front()
            .map_or(Duration::ZERO, |waiting| now.saturating_sub(waiting.since));
        self.series.oldest_queued.set(oldest.as_secs_f64());
    }
}
