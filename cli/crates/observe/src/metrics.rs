//! The metrics registry: every declared series, kept in atomics and rendered
//! in the Prometheus text format 0.0.4.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::catalogue::{Catalogue, MetricKind, MetricSpec, catalogue};
use crate::record::fail_undeclared;

#[derive(Debug, Clone, Default)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    pub fn increment(&self) {
        self.add(1);
    }

    pub fn add(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Gauge(Arc<AtomicU64>);

impl Gauge {
    pub fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }

    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

#[derive(Debug)]
struct HistogramCells {
    bounds: Vec<f64>,
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_bits: AtomicU64,
}

/// Observations of durations in seconds, counted into the family's buckets.
#[derive(Debug, Clone)]
pub struct Histogram(Arc<HistogramCells>);

impl Histogram {
    fn new(bounds: &[f64]) -> Histogram {
        Histogram(Arc::new(HistogramCells {
            bounds: bounds.to_vec(),
            buckets: bounds.iter().map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum_bits: AtomicU64::new(0f64.to_bits()),
        }))
    }

    pub fn observe(&self, seconds: f64) {
        let cells = &self.0;
        for (bound, bucket) in cells.bounds.iter().zip(&cells.buckets) {
            if seconds <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        cells.count.fetch_add(1, Ordering::Relaxed);
        let _ = cells
            .sum_bits
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |bits| {
                Some((f64::from_bits(bits) + seconds).to_bits())
            });
    }
}

#[derive(Debug, Clone)]
enum Cell {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
}

#[derive(Debug)]
struct Series {
    labels: Vec<(String, String)>,
    cell: Cell,
}

#[derive(Debug)]
struct Family {
    name: String,
    kind: MetricKind,
    series: Vec<Series>,
}

/// Every series of every declared family, each label combination created at
/// zero, so that a scrape always shows exactly the declared families.
#[derive(Debug)]
pub struct Registry {
    families: Vec<Family>,
}

/// The process's registry, built from the CLI's catalogue.
pub fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Registry::new(catalogue()))
}

/// The process's metrics in the Prometheus text format 0.0.4.
pub fn render() -> String {
    registry().render()
}

impl Registry {
    pub fn new(catalogue: &Catalogue) -> Registry {
        Registry {
            families: catalogue.metrics.iter().map(family).collect(),
        }
    }

    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> Counter {
        match self.cell(name, MetricKind::Counter, labels) {
            Some(Cell::Counter(counter)) => counter,
            _ => Counter::default(),
        }
    }

    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Gauge {
        match self.cell(name, MetricKind::Gauge, labels) {
            Some(Cell::Gauge(gauge)) => gauge,
            _ => Gauge::default(),
        }
    }

    pub fn histogram(&self, name: &str, labels: &[(&str, &str)]) -> Histogram {
        match self.cell(name, MetricKind::Histogram, labels) {
            Some(Cell::Histogram(histogram)) => histogram,
            _ => Histogram::new(&[]),
        }
    }

    /// The series of a declared family and label set. Anything else is
    /// undeclared and gets a handle that no scrape shows.
    fn cell(&self, name: &str, kind: MetricKind, labels: &[(&str, &str)]) -> Option<Cell> {
        let found = self
            .families
            .iter()
            .find(|family| family.name == name && family.kind == kind)
            .and_then(|family| {
                family.series.iter().find(|series| {
                    series.labels.len() == labels.len()
                        && series.labels.iter().all(|(label, value)| {
                            labels.iter().any(|(wanted, wanted_value)| {
                                wanted == label && wanted_value == value
                            })
                        })
                })
            })
            .map(|series| series.cell.clone());
        if found.is_none() {
            fail_undeclared(&format!("metric {name} {labels:?}"));
            crate::warn!("diagnostic.invalid_event");
        }
        found
    }

    pub fn render(&self) -> String {
        let mut text = String::new();
        for family in &self.families {
            let _ = writeln!(text, "# TYPE {} {}", family.name, family.kind.as_str());
            for series in &family.series {
                match &series.cell {
                    Cell::Counter(counter) => {
                        let _ = writeln!(
                            text,
                            "{}{} {}",
                            family.name,
                            label_text(&series.labels, None),
                            counter.get()
                        );
                    }
                    Cell::Gauge(gauge) => {
                        let _ = writeln!(
                            text,
                            "{}{} {}",
                            family.name,
                            label_text(&series.labels, None),
                            gauge.get()
                        );
                    }
                    Cell::Histogram(histogram) => {
                        render_histogram(&mut text, &family.name, &series.labels, histogram)
                    }
                }
            }
        }
        text
    }
}

fn family(spec: &MetricSpec) -> Family {
    let mut combinations: Vec<Vec<(String, String)>> = vec![Vec::new()];
    for label in &spec.labels {
        combinations = combinations
            .into_iter()
            .flat_map(|combination| {
                label.values.iter().map(move |value| {
                    let mut next = combination.clone();
                    next.push((label.name.clone(), value.clone()));
                    next
                })
            })
            .collect();
    }
    let series = combinations
        .into_iter()
        .map(|labels| Series {
            labels,
            cell: match spec.kind {
                MetricKind::Counter => Cell::Counter(Counter::default()),
                MetricKind::Gauge => Cell::Gauge(Gauge::default()),
                MetricKind::Histogram => Cell::Histogram(Histogram::new(&spec.buckets)),
            },
        })
        .collect();
    Family {
        name: spec.name.clone(),
        kind: spec.kind,
        series,
    }
}

fn render_histogram(
    text: &mut String,
    name: &str,
    labels: &[(String, String)],
    histogram: &Histogram,
) {
    let cells = &histogram.0;
    for (bound, bucket) in cells.bounds.iter().zip(&cells.buckets) {
        let bound = bound.to_string();
        let _ = writeln!(
            text,
            "{name}_bucket{} {}",
            label_text(labels, Some(&bound)),
            bucket.load(Ordering::Relaxed)
        );
    }
    let count = cells.count.load(Ordering::Relaxed);
    let _ = writeln!(
        text,
        "{name}_bucket{} {count}",
        label_text(labels, Some("+Inf"))
    );
    let sum = f64::from_bits(cells.sum_bits.load(Ordering::Relaxed));
    let _ = writeln!(text, "{name}_sum{} {sum}", label_text(labels, None));
    let _ = writeln!(text, "{name}_count{} {count}", label_text(labels, None));
}

fn label_text(labels: &[(String, String)], bound: Option<&str>) -> String {
    let mut pairs: Vec<String> = labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect();
    if let Some(bound) = bound {
        pairs.push(format!("le=\"{bound}\""));
    }
    if pairs.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", pairs.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREE_FAMILIES: &str = r#"{
        "events": [{"name": "diagnostic.invalid_event", "level": "WARN", "fields": [{"name": "event.original", "type": "event_name"}]}],
        "metrics": [
            {"name": "tetral_reconnects_total", "type": "counter", "labels": [{"name": "kind", "values": ["resume", "new"]}]},
            {"name": "tetral_probes_in_progress", "type": "gauge"},
            {"name": "tetral_wait_seconds", "type": "histogram", "buckets": [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]}
        ]
    }"#;

    const RENDERED: &str = "\
# TYPE tetral_reconnects_total counter
tetral_reconnects_total{kind=\"resume\"} 2
tetral_reconnects_total{kind=\"new\"} 0
# TYPE tetral_probes_in_progress gauge
tetral_probes_in_progress 3
# TYPE tetral_wait_seconds histogram
tetral_wait_seconds_bucket{le=\"0.005\"} 0
tetral_wait_seconds_bucket{le=\"0.01\"} 0
tetral_wait_seconds_bucket{le=\"0.025\"} 0
tetral_wait_seconds_bucket{le=\"0.05\"} 0
tetral_wait_seconds_bucket{le=\"0.1\"} 0
tetral_wait_seconds_bucket{le=\"0.25\"} 0
tetral_wait_seconds_bucket{le=\"0.5\"} 1
tetral_wait_seconds_bucket{le=\"1\"} 1
tetral_wait_seconds_bucket{le=\"2.5\"} 1
tetral_wait_seconds_bucket{le=\"5\"} 2
tetral_wait_seconds_bucket{le=\"10\"} 2
tetral_wait_seconds_bucket{le=\"+Inf\"} 3
tetral_wait_seconds_sum 23.5
tetral_wait_seconds_count 3
";

    #[test]
    fn a_counter_a_gauge_and_a_histogram_render_as_prometheus_text() {
        let catalogue = Catalogue::parse(THREE_FAMILIES).unwrap();
        let registry = Registry::new(&catalogue);
        let resumes = registry.counter("tetral_reconnects_total", &[("kind", "resume")]);
        resumes.increment();
        resumes.increment();
        registry.gauge("tetral_probes_in_progress", &[]).set(3.0);
        let waits = registry.histogram("tetral_wait_seconds", &[]);
        for seconds in [0.5, 3.0, 20.0] {
            waits.observe(seconds);
        }
        assert_eq!(registry.render(), RENDERED);
    }

    #[test]
    #[should_panic(expected = "is not declared")]
    fn an_undeclared_label_value_fails_the_test() {
        registry().counter("tetral_reconnects_total", &[("kind", "unknown")]);
    }
}
