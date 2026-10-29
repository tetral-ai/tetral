//! The task pressure point `tetral_tasks`: the running tasks and the
//! `tool.execute` instructions waiting in their bindings' staging queues,
//! which the Session binding owners report to the Connection owner, and the
//! `tool.execute` instructions rejected `resource_exhausted`. It has no
//! limit. Its series are set at each change and at each check of the
//! owner's times, so that the oldest wait grows between changes; an
//! admission is a `tool.execute` staged or started at once.

use std::collections::BTreeMap;
use std::time::Duration;

use observe::pressure::{Episodes, Name};
use observe::{Counter, Gauge, Histogram, Registry};

const POINT: Name = Name("tetral_tasks");
const RESOURCE_EXHAUSTED: Name = Name("resource_exhausted");

#[derive(Debug)]
struct Series {
    in_progress: Gauge,
    queued: Gauge,
    oldest_queued: Gauge,
    queue_wait: Histogram,
    rejected: Counter,
}

#[derive(Debug)]
pub struct TaskPressure {
    running: u64,
    /// The times on the seam clock at which the waiting instructions were
    /// staged, each with how many were staged then.
    staged: BTreeMap<Duration, u64>,
    series: Series,
    episodes: Episodes,
}

impl TaskPressure {
    /// The pressure point on `registry`'s series, whose episodes end after
    /// `quiet`.
    pub fn new(registry: &Registry, quiet: Duration) -> TaskPressure {
        TaskPressure {
            running: 0,
            staged: BTreeMap::new(),
            series: Series {
                in_progress: registry.gauge("tetral_tasks_in_progress", &[]),
                queued: registry.gauge("tetral_tasks_queued", &[]),
                oldest_queued: registry.gauge("tetral_tasks_oldest_queued_seconds", &[]),
                queue_wait: registry.histogram("tetral_tasks_queue_wait_seconds", &[]),
                rejected: registry.counter(
                    "tetral_tasks_rejected_total",
                    &[("cause", RESOURCE_EXHAUSTED.0)],
                ),
            },
            episodes: Episodes::new(POINT, quiet),
        }
    }

    /// A `tool.execute` staged at `staged` waits in a staging queue.
    pub fn staged(&mut self, staged: Duration, now: Duration) {
        *self.staged.entry(staged).or_default() += 1;
        self.episodes.admitted(now);
        self.show(now);
    }

    /// The instruction staged at `staged` left its queue without starting.
    pub fn unstaged(&mut self, staged: Duration, now: Duration) {
        self.leave(staged);
        self.show(now);
    }

    /// A task starts: from its staging queue, which it waited in since
    /// `staged`, or at once, as an admission.
    pub fn started(&mut self, staged: Option<Duration>, now: Duration) {
        match staged {
            Some(staged) => {
                self.leave(staged);
                self.series
                    .queue_wait
                    .observe(now.saturating_sub(staged).as_secs_f64());
            }
            None => self.episodes.admitted(now),
        }
        self.running += 1;
        self.show(now);
    }

    pub fn ended(&mut self, now: Duration) {
        self.running = self.running.saturating_sub(1);
        self.show(now);
    }

    /// A `tool.execute` rejected `resource_exhausted`.
    pub fn refused(&mut self, now: Duration) {
        self.series.rejected.increment();
        self.episodes.rejected(RESOURCE_EXHAUSTED, now);
        self.show(now);
    }

    pub fn show(&self, now: Duration) {
        self.series.in_progress.set(self.running as f64);
        self.series
            .queued
            .set(self.staged.values().sum::<u64>() as f64);
        let oldest = self
            .staged
            .keys()
            .next()
            .map_or(Duration::ZERO, |staged| now.saturating_sub(*staged));
        self.series.oldest_queued.set(oldest.as_secs_f64());
    }

    fn leave(&mut self, staged: Duration) {
        if let Some(count) = self.staged.get_mut(&staged) {
            *count -= 1;
            if *count == 0 {
                self.staged.remove(&staged);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use observe::pressure::QUIET_INTERVAL;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, SubscriberExt};

    use super::*;

    /// The string fields of one event, by name.
    type Fields = Vec<(String, String)>;

    /// The fields of each event recorded.
    #[derive(Clone, Default)]
    struct Recorded(Arc<Mutex<Vec<Fields>>>);

    struct Visitor(Fields);

    impl Visit for Visitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.push((field.name().to_owned(), value.to_owned()));
        }

        fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Recorded {
        fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
            let mut fields = Visitor(Vec::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }

    #[test]
    fn running_tasks_a_staged_instruction_and_a_refusal_set_the_series_and_one_episode() {
        let registry = Registry::new(observe::catalogue());
        let gauge = |name: &str| registry.gauge(name, &[]).get();
        let at = Duration::from_secs_f64;
        let recorded = Recorded::default();
        let subscriber = tracing_subscriber::registry().with(recorded.clone());
        tracing::subscriber::with_default(subscriber, || {
            let mut pressure = TaskPressure::new(&registry, QUIET_INTERVAL);
            pressure.started(None, at(0.0));
            pressure.started(None, at(0.5));
            pressure.staged(at(1.0), at(1.0));
            pressure.show(at(4.0));
            assert_eq!(
                [
                    gauge("tetral_tasks_in_progress"),
                    gauge("tetral_tasks_queued"),
                    gauge("tetral_tasks_oldest_queued_seconds"),
                ],
                [2.0, 1.0, 3.0]
            );
            pressure.started(Some(at(1.0)), at(4.0));
            pressure.refused(at(4.0));
        });
        let text = registry.render();
        assert!(
            text.contains("tetral_tasks_queue_wait_seconds_count 1\n")
                && text.contains("tetral_tasks_queue_wait_seconds_sum 3\n"),
            "{text}"
        );
        assert_eq!(
            [
                gauge("tetral_tasks_in_progress"),
                gauge("tetral_tasks_queued"),
                gauge("tetral_tasks_oldest_queued_seconds"),
            ],
            [3.0, 0.0, 0.0]
        );
        let rejected = registry.counter(
            "tetral_tasks_rejected_total",
            &[("cause", "resource_exhausted")],
        );
        assert_eq!(rejected.get(), 1);
        let started: Vec<Fields> = recorded
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|fields| {
                fields
                    .iter()
                    .any(|(_, value)| value == "pressure.rejecting_started")
            })
            .cloned()
            .collect();
        let named = |fields: &[(String, String)], name: &str| {
            fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| value.clone())
        };
        assert_eq!(started.len(), 1, "{started:?}");
        assert_eq!(
            (
                named(&started[0], "pressure.point"),
                named(&started[0], "pressure.cause")
            ),
            (
                Some("tetral_tasks".to_owned()),
                Some("resource_exhausted".to_owned())
            )
        );
    }
}
