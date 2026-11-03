//! The clock that the owners with deadlines of their own read and wait on,
//! given at their construction: the CLI's clock, or one a test moves.

use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct Clock {
    now: Arc<dyn Fn() -> Duration + Send + Sync>,
    recheck: Duration,
}

impl Clock {
    /// `seams::now`, read again at least every recheck interval.
    pub fn seams() -> Clock {
        Clock::new(crate::seams::now, crate::seams::recheck_interval())
    }

    /// A clock that reads `now`, waits for which read it again at least
    /// every `recheck`.
    pub fn new(now: impl Fn() -> Duration + Send + Sync + 'static, recheck: Duration) -> Clock {
        Clock {
            now: Arc::new(now),
            recheck,
        }
    }

    pub fn now(&self) -> Duration {
        (self.now)()
    }

    /// Waits until the clock reaches `deadline`, reading it again after each
    /// recheck interval, so that a clock which jumps ends the wait within one.
    pub async fn sleep_until(&self, deadline: Duration) {
        loop {
            let remaining = deadline.saturating_sub(self.now());
            if remaining.is_zero() {
                return;
            }
            tokio::time::sleep(remaining.min(self.recheck)).await;
        }
    }

    /// Waits until the clock reaches `deadline` or for one recheck interval,
    /// whichever comes first, so that a waiter can read something else at
    /// least that often.
    pub async fn sleep_step(&self, deadline: Duration) {
        let remaining = deadline.saturating_sub(self.now());
        tokio::time::sleep(remaining.min(self.recheck)).await;
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("clock")
    }
}
