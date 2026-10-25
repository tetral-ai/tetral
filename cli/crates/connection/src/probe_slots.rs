//! The probe slots: the Connection owner's count of the directory probes
//! that run, at most `PROBES_MAX`, and the requests that wait for a slot in
//! arrival order, each for at most `PROBE_WAIT`. A slot is taken from its
//! grant until the Session binding owner returns it, after reaping its probe.

use std::collections::VecDeque;
use std::time::Duration;

use protocol::id::SafeId;
use protocol::ports::ProbeRequest;

/// The directory probes that may run at once.
pub const PROBES_MAX: usize = 8;

/// The time a request waits for a slot before it is answered `Exhausted`.
pub const PROBE_WAIT: Duration = Duration::from_secs(5);

/// The limits of the probe slots, each a constant but for tests.
#[derive(Debug, Clone, Copy)]
pub struct SlotLimits {
    pub probes_max: usize,
    pub wait: Duration,
}

impl Default for SlotLimits {
    fn default() -> SlotLimits {
        SlotLimits {
            probes_max: PROBES_MAX,
            wait: PROBE_WAIT,
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
pub struct ProbeSlots {
    limits: SlotLimits,
    running: usize,
    waiting: VecDeque<Waiting>,
}

impl ProbeSlots {
    pub fn new(limits: SlotLimits) -> ProbeSlots {
        ProbeSlots {
            limits,
            running: 0,
            waiting: VecDeque::new(),
        }
    }

    /// Takes a request at `now`: granted at once, `true`, when a slot is
    /// free and no request waits; otherwise it waits.
    pub fn request(&mut self, requester: Requester, now: Duration) -> bool {
        if self.running < self.limits.probes_max && self.waiting.is_empty() {
            self.running += 1;
            return true;
        }
        self.waiting.push_back(Waiting {
            requester,
            since: now,
        });
        false
    }

    pub fn withdraw(&mut self, requester: &Requester) {
        self.waiting
            .retain(|waiting| waiting.requester != *requester);
    }

    /// A slot comes back; the longest-waiting request takes it.
    pub fn free(&mut self) -> Option<Requester> {
        self.running = self.running.saturating_sub(1);
        if self.running >= self.limits.probes_max {
            return None;
        }
        let next = self.waiting.pop_front()?;
        self.running += 1;
        Some(next.requester)
    }

    /// The requests that have waited `PROBE_WAIT` by `now`, to be answered
    /// `Exhausted`.
    pub fn expire(&mut self, now: Duration) -> Vec<Requester> {
        let mut expired = Vec::new();
        while let Some(front) = self.waiting.front()
            && now >= front.since + self.limits.wait
        {
            if let Some(waiting) = self.waiting.pop_front() {
                expired.push(waiting.requester);
            }
        }
        expired
    }

    /// Every waiting request, to be answered `Cancelled`.
    pub fn cancel(&mut self) -> Vec<Requester> {
        self.waiting
            .drain(..)
            .map(|waiting| waiting.requester)
            .collect()
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
}
