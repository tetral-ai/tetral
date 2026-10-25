//! The reconnect backoff, which also spaces the retries of one message.

use std::time::Duration;

/// The base delay at the start and after a confirmed heartbeat.
pub const BASE_START: Duration = Duration::from_secs(1);

/// The base delay that doubling never passes.
pub const BASE_MAX: Duration = Duration::from_secs(30);

/// The largest share of the base delay that a wait adds at random.
pub const JITTER_MAX: f64 = 0.2;

/// Waits of the base delay times `1 + r`, `r` drawn from 0 to
/// [`JITTER_MAX`], the base doubling after each wait up to its maximum.
#[derive(Debug, Clone)]
pub struct Backoff {
    start: Duration,
    max: Duration,
    base: Duration,
    jitter: fn() -> f64,
}

impl Backoff {
    /// A backoff from `start` up to `max`, with `jitter` giving `r`.
    pub fn new(start: Duration, max: Duration, jitter: fn() -> f64) -> Backoff {
        Backoff {
            start,
            max,
            base: start,
            jitter,
        }
    }

    /// The next wait; the base then doubles.
    pub fn next_wait(&mut self) -> Duration {
        let wait = self
            .base
            .mul_f64(1.0 + (self.jitter)().clamp(0.0, JITTER_MAX));
        self.base = (self.base * 2).min(self.max);
        wait
    }

    pub fn reset(&mut self) {
        self.base = self.start;
    }
}

/// A jitter drawn uniformly from 0 to [`JITTER_MAX`].
pub fn random_jitter() -> f64 {
    rand::random_range(0.0..=JITTER_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waits(backoff: &mut Backoff, count: usize) -> Vec<Duration> {
        (0..count).map(|_| backoff.next_wait()).collect()
    }

    #[test]
    fn the_base_doubles_to_thirty_seconds_and_returns_to_one_second_on_reset() {
        let seconds = |values: &[f64]| -> Vec<Duration> {
            values
                .iter()
                .map(|value| Duration::from_secs_f64(*value))
                .collect()
        };
        let mut least = Backoff::new(BASE_START, BASE_MAX, || 0.0);
        assert_eq!(
            waits(&mut least, 7),
            seconds(&[1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0])
        );
        least.reset();
        assert_eq!(least.next_wait(), Duration::from_secs(1));

        let mut most = Backoff::new(BASE_START, BASE_MAX, || 0.2);
        assert_eq!(
            waits(&mut most, 7),
            seconds(&[1.2, 2.4, 4.8, 9.6, 19.2, 36.0, 36.0])
        );
        // A jitter source outside its range is held to it.
        let mut wild = Backoff::new(BASE_START, BASE_MAX, || 5.0);
        assert_eq!(wild.next_wait(), Duration::from_secs_f64(1.2));
    }
}
