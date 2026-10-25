//! The rejection episodes of one pressure point, which the point's owner
//! keeps and records. An episode per cause starts with a rejection that
//! comes `QUIET_INTERVAL` or more after the cause's last one, or its first,
//! recorded as `pressure.rejecting_started`; it ends at the first admission or
//! rejection at the point `QUIET_INTERVAL` or more after its last rejection,
//! recorded as `pressure.rejecting_ended` with the number it refused, before
//! anything else that admission or rejection records.

use std::time::Duration;

use crate::record::RecordEnum;

/// The time without a rejection for a cause after which its episode ends.
pub const QUIET_INTERVAL: Duration = Duration::from_secs(60);

/// A pressure point's or a cause's name, one of the declared values of
/// `pressure.point` or `pressure.cause`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Name(pub &'static str);

impl RecordEnum for Name {
    fn record_str(&self) -> &'static str {
        self.0
    }
}

#[derive(Debug)]
struct Episode {
    cause: Name,
    last: Duration,
    rejected: u64,
}

/// The open episodes of the pressure point `point`, read on its owner's
/// monotonic clock.
#[derive(Debug)]
pub struct Episodes {
    point: Name,
    quiet: Duration,
    open: Vec<Episode>,
}

impl Episodes {
    pub fn new(point: Name, quiet: Duration) -> Episodes {
        Episodes {
            point,
            quiet,
            open: Vec::new(),
        }
    }

    pub fn admitted(&mut self, now: Duration) {
        self.end_quiet(now);
    }

    pub fn rejected(&mut self, cause: Name, now: Duration) {
        self.end_quiet(now);
        if let Some(open) = self.open.iter_mut().find(|open| open.cause == cause) {
            open.last = now;
            open.rejected += 1;
            return;
        }
        crate::warn!(
            "pressure.rejecting_started",
            pressure.point = self.point,
            pressure.cause = cause
        );
        self.open.push(Episode {
            cause,
            last: now,
            rejected: 1,
        });
    }

    fn end_quiet(&mut self, now: Duration) {
        let (point, quiet) = (self.point, self.quiet);
        self.open.retain(|open| {
            if now.saturating_sub(open.last) < quiet {
                return true;
            }
            crate::warn!(
                "pressure.rejecting_ended",
                pressure.point = point,
                pressure.cause = open.cause,
                pressure.rejected = open.rejected
            );
            false
        });
    }
}
