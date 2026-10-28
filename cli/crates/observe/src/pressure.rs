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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::Level;
    use crate::record::tests::record_with;

    #[test]
    fn an_episode_ends_at_the_first_admission_or_rejection_a_quiet_interval_after_its_last_rejection()
     {
        let timeout = Name("timeout");
        let at = Duration::from_secs_f64;
        let (stderr, _) = record_with(Level::Info, || {
            let mut episodes = Episodes::new(Name("tetral_probes"), QUIET_INTERVAL);
            episodes.rejected(timeout, at(0.0));
            episodes.rejected(timeout, at(30.0));
            episodes.admitted(at(89.999));
            episodes.admitted(at(90.0));
            episodes.admitted(at(95.0));
            episodes.rejected(timeout, at(100.0));
            episodes.rejected(timeout, at(159.999));
            episodes.rejected(timeout, at(219.999));
        });
        let records: Vec<(Value, Value)> = stderr
            .records()
            .into_iter()
            .map(|record| (record["event"].clone(), record["pressure.rejected"].clone()))
            .collect();
        let started = (Value::from("pressure.rejecting_started"), Value::Null);
        let ended = |rejected: u64| {
            (
                Value::from("pressure.rejecting_ended"),
                Value::from(rejected),
            )
        };
        assert_eq!(
            records,
            [
                started.clone(),
                ended(2),
                started.clone(),
                ended(2),
                started
            ]
        );
        for record in stderr.records() {
            assert_eq!(
                (&record["pressure.point"], &record["pressure.cause"]),
                (&Value::from("tetral_probes"), &Value::from("timeout"))
            );
        }
    }
}
