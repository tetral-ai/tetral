//! The declared table of every event and metric family the CLI records.

use std::collections::HashSet;
use std::sync::OnceLock;

use serde::Deserialize;

use crate::record::{CORRELATION_FIELDS, RECORD_FIELDS};

const CATALOGUE_JSON: &str = include_str!("../../../observability.json");
/// The longest event name the catalogue accepts; a record's `event` value is
/// held to it whatever the string limit.
pub(crate) const EVENT_NAME_MAX_BYTES: usize = 128;
/// The record that a release build writes in place of an undeclared one, and
/// its one field, the rejected record's event.
pub(crate) const INVALID_EVENT: &str = "diagnostic.invalid_event";
pub(crate) const ORIGINAL_EVENT_FIELD: &str = "event.original";

/// The duration buckets, in seconds, of a histogram of requests.
pub const REQUEST_BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// The duration buckets, in seconds, of a histogram of long operations.
pub const LONG_OPERATION_BUCKETS: [f64; 12] = [
    0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0,
];

/// The identities of the protocol's vocabulary. Neither these nor the
/// correlation fields, written with `_` for `.`, ever label a metric.
const IDENTITY_LABELS: &[&str] = &[
    "sandbox_id",
    "instance_id",
    "credential_id",
    "connection_id",
    "computer_id",
    "binding_id",
    "task_id",
    "session_id",
    "message_id",
    "tool_use_event_id",
    "drive_id",
    "version_id",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalogue {
    pub events: Vec<EventSpec>,
    pub metrics: Vec<MetricSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSpec {
    pub name: String,
    pub level: EventLevel,
    #[serde(default)]
    pub log_type: LogType,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub errors: Vec<ErrorSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum EventLevel {
    #[serde(rename = "DEBUG")]
    Debug,
    #[serde(rename = "INFO")]
    Info,
    #[serde(rename = "WARN")]
    Warn,
    #[serde(rename = "ERROR")]
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogType {
    #[default]
    App,
    Audit,
}

impl LogType {
    pub fn as_str(self) -> &'static str {
        match self {
            LogType::App => "app",
            LogType::Audit => "audit",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: FieldKind,
    #[serde(default)]
    pub values: Vec<String>,
}

/// The value types a record field can have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    Integer,
    Boolean,
    /// One of the field's declared `values`.
    Enum,
    /// A duration in milliseconds, in a field whose name ends in `_ms`.
    Ms,
    /// An opaque protocol identity.
    Identity,
    /// The name of an event, following the catalogue's name rule.
    #[serde(rename = "event_name")]
    EventName,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorSpec {
    pub code: String,
    pub class: String,
    pub message_safe: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: MetricKind,
    #[serde(default)]
    pub labels: Vec<LabelSpec>,
    #[serde(default)]
    pub buckets: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelSpec {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CatalogueError {
    #[error("the catalogue is not valid JSON of the catalogue's shape: {0}")]
    Shape(String),
    #[error("event {0}: {1}")]
    Event(String, &'static str),
    #[error("metric family {0}: {1}")]
    Metric(String, &'static str),
}

/// The CLI's catalogue, `cli/observability.json`, parsed once.
pub fn catalogue() -> &'static Catalogue {
    static CATALOGUE: OnceLock<Catalogue> = OnceLock::new();
    CATALOGUE.get_or_init(|| {
        Catalogue::parse(CATALOGUE_JSON).expect("cli/observability.json is a valid catalogue")
    })
}

impl Catalogue {
    /// Parses a catalogue and checks it against the record and metric rules.
    pub fn parse(text: &str) -> Result<Catalogue, CatalogueError> {
        let catalogue: Catalogue =
            serde_json::from_str(text).map_err(|error| CatalogueError::Shape(error.to_string()))?;
        let mut names = HashSet::new();
        for event in &catalogue.events {
            check_event(event)?;
            if !names.insert(event.name.as_str()) {
                return Err(CatalogueError::Event(
                    event.name.clone(),
                    "is declared twice",
                ));
            }
        }
        let families: HashSet<&str> = catalogue
            .metrics
            .iter()
            .map(|metric| metric.name.as_str())
            .collect();
        if families.len() != catalogue.metrics.len() {
            return Err(CatalogueError::Metric(
                String::new(),
                "a family is declared twice",
            ));
        }
        for metric in &catalogue.metrics {
            check_metric(metric, &families)?;
        }
        let replaces_undeclared = catalogue.event(INVALID_EVENT).is_some_and(|event| {
            event.level == EventLevel::Warn
                && matches!(&event.fields[..], [field] if field.name == ORIGINAL_EVENT_FIELD
                    && field.kind == FieldKind::EventName)
        });
        if !replaces_undeclared {
            return Err(CatalogueError::Event(
                INVALID_EVENT.to_owned(),
                "is not declared at WARN with event.original alone",
            ));
        }
        Ok(catalogue)
    }

    pub fn event(&self, name: &str) -> Option<&EventSpec> {
        self.events.iter().find(|event| event.name == name)
    }

    pub fn metric(&self, name: &str) -> Option<&MetricSpec> {
        self.metrics.iter().find(|metric| metric.name == name)
    }
}

impl EventSpec {
    pub fn field(&self, name: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn error(&self, code: &str) -> Option<&ErrorSpec> {
        self.errors.iter().find(|error| error.code == code)
    }
}

fn check_event(event: &EventSpec) -> Result<(), CatalogueError> {
    let fail = |reason| Err(CatalogueError::Event(event.name.clone(), reason));
    if !is_event_name(&event.name) {
        return fail("the name breaks ^[a-z][a-z0-9_.-]{0,127}$");
    }
    let mut fields = HashSet::new();
    for field in &event.fields {
        let name = field.name.as_str();
        if RECORD_FIELDS.contains(&name)
            || CORRELATION_FIELDS.contains(&name)
            || !fields.insert(name)
        {
            return fail("a field repeats a record field, a correlation field or another field");
        }
        match field.kind {
            FieldKind::Enum if field.values.is_empty() => {
                return fail("an enum field has no values");
            }
            FieldKind::Ms if !name.ends_with("_ms") => {
                return fail("a duration field does not end in _ms");
            }
            FieldKind::Enum => {}
            _ if !field.values.is_empty() => return fail("only an enum field has values"),
            _ => {}
        }
    }
    let mut codes = HashSet::new();
    for error in &event.errors {
        if error.code.is_empty() || error.class.is_empty() || error.message_safe.is_empty() {
            return fail("an error has an empty code, class or text");
        }
        if !codes.insert(error.code.as_str()) {
            return fail("an error code is declared twice");
        }
    }
    Ok(())
}

pub(crate) fn is_event_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= EVENT_NAME_MAX_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.-".contains(byte))
}

fn check_metric(metric: &MetricSpec, families: &HashSet<&str>) -> Result<(), CatalogueError> {
    let name = metric.name.as_str();
    let fail = |reason| Err(CatalogueError::Metric(metric.name.clone(), reason));
    let words_ok = !name.is_empty()
        && name.split('_').all(|word| {
            !word.is_empty()
                && word
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
        && name.as_bytes()[0].is_ascii_lowercase();
    if !words_ok {
        return fail("the name is not lowercase words joined by _");
    }
    match metric.kind {
        MetricKind::Counter if !name.ends_with("_total") => {
            return fail("a counter does not end in _total");
        }
        MetricKind::Gauge | MetricKind::Histogram if name.ends_with("_total") => {
            return fail("only a counter ends in _total");
        }
        MetricKind::Histogram if !name.ends_with("_seconds") && !name.ends_with("_bytes") => {
            return fail("a histogram does not end in its unit");
        }
        _ => {}
    }
    let duration_buckets =
        metric.buckets == REQUEST_BUCKETS || metric.buckets == LONG_OPERATION_BUCKETS;
    match metric.kind {
        MetricKind::Histogram if name.ends_with("_seconds") && !duration_buckets => {
            return fail(
                "a duration histogram uses neither the request nor the long-operation bucket set",
            );
        }
        MetricKind::Histogram if metric.buckets.is_empty() => {
            return fail("a histogram has no buckets");
        }
        MetricKind::Counter | MetricKind::Gauge if !metric.buckets.is_empty() => {
            return fail("only a histogram has buckets");
        }
        _ => {}
    }
    let mut labels = HashSet::new();
    for label in &metric.labels {
        if label.values.is_empty() {
            return fail("a label has no fixed set of values");
        }
        if IDENTITY_LABELS.contains(&label.name.as_str())
            || CORRELATION_FIELDS
                .iter()
                .any(|field| field.replace('.', "_") == label.name)
        {
            return fail("an identity is a label");
        }
        if label.name == "le" || !labels.insert(label.name.as_str()) {
            return fail("a label name is reserved or repeated");
        }
    }
    check_pressure_point(metric, families)
}

/// The series of a pressure point: their types, and the siblings each one
/// needs. A counter ends in `_total` and a histogram in its unit, so every
/// `_queued` and `_limit` series that gets here is a gauge.
fn check_pressure_point(
    metric: &MetricSpec,
    families: &HashSet<&str>,
) -> Result<(), CatalogueError> {
    let name = metric.name.as_str();
    let fail = |reason| Err(CatalogueError::Metric(metric.name.clone(), reason));
    let gauge = metric.kind == MetricKind::Gauge;
    if let Some(point) = name.strip_suffix("_oldest_queued_seconds") {
        if !gauge || !families.contains(format!("{point}_queued").as_str()) {
            return fail("an oldest-queued series is not a gauge beside its queued series");
        }
    } else if let Some(point) = name.strip_suffix("_queue_wait_seconds") {
        if metric.kind != MetricKind::Histogram
            || !families.contains(format!("{point}_queued").as_str())
        {
            return fail("a queue-wait series is not a histogram beside its queued series");
        }
    } else if let Some(point) = name.strip_suffix("_queued") {
        let siblings = [
            format!("{point}_oldest_queued_seconds"),
            format!("{point}_queue_wait_seconds"),
        ];
        if !siblings
            .iter()
            .all(|sibling| families.contains(sibling.as_str()))
        {
            return fail("a queued series lacks its oldest-queued or queue-wait sibling");
        }
    } else if let Some(point) = name.strip_suffix("_limit") {
        if !families.contains(format!("{point}_in_progress").as_str()) {
            return fail("a limit series has no in-progress series beside it");
        }
    } else if name.ends_with("_rejected_total") {
        let cause = metric.labels.iter().any(|label| label.name == "cause");
        if metric.labels.len() != 1 || !cause {
            return fail("a rejection counter is not labelled by cause alone");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    type Labels = &'static [(&'static str, &'static [&'static str])];
    type Family<'a> = (&'a str, &'a str, Vec<(&'a str, Vec<&'a str>)>);

    /// The CLI's local metric families with their types, labels and label
    /// values, fixed here so that the catalogue cannot drift from them.
    const LOCAL_METRICS: &[(&str, &str, Labels)] = &[
        (
            "tetral_connection_state",
            "gauge",
            &[(
                "state",
                &[
                    "disconnected",
                    "connecting",
                    "registering",
                    "online",
                    "resuming",
                    "blocked",
                    "stopping",
                    "stopped",
                ],
            )],
        ),
        (
            "tetral_reconnects_total",
            "counter",
            &[("kind", &["resume", "new"])],
        ),
        (
            "tetral_startup_phase_seconds",
            "gauge",
            &[("phase", &["config", "connect", "ready"])],
        ),
        ("tetral_probes_in_progress", "gauge", &[]),
        ("tetral_probes_limit", "gauge", &[]),
        ("tetral_probes_queued", "gauge", &[]),
        ("tetral_probes_oldest_queued_seconds", "gauge", &[]),
        ("tetral_probes_queue_wait_seconds", "histogram", &[]),
        (
            "tetral_probes_rejected_total",
            "counter",
            &[("cause", &["timeout"])],
        ),
        ("tetral_tasks_in_progress", "gauge", &[]),
        ("tetral_tasks_queued", "gauge", &[]),
        ("tetral_tasks_oldest_queued_seconds", "gauge", &[]),
        ("tetral_tasks_queue_wait_seconds", "histogram", &[]),
        (
            "tetral_tasks_rejected_total",
            "counter",
            &[("cause", &["resource_exhausted"])],
        ),
        ("tetral_uploads_in_progress", "gauge", &[]),
        ("tetral_uploads_limit", "gauge", &[]),
        ("tetral_uploads_queued", "gauge", &[]),
        ("tetral_uploads_oldest_queued_seconds", "gauge", &[]),
        ("tetral_uploads_queue_wait_seconds", "histogram", &[]),
        (
            "tetral_uploads_rejected_total",
            "counter",
            &[("cause", &["rate_limited"])],
        ),
        ("tetral_downloads_in_progress", "gauge", &[]),
        ("tetral_downloads_limit", "gauge", &[]),
        ("tetral_downloads_queued", "gauge", &[]),
        ("tetral_downloads_oldest_queued_seconds", "gauge", &[]),
        ("tetral_downloads_queue_wait_seconds", "histogram", &[]),
        (
            "tetral_downloads_rejected_total",
            "counter",
            &[("cause", &["rate_limited"])],
        ),
        (
            "tetral_log_write_failures_total",
            "counter",
            &[("sink", &["stderr", "file"])],
        ),
    ];

    const LOCAL_HISTOGRAM_BUCKETS: &[(&str, &[f64])] = &[
        ("tetral_probes_queue_wait_seconds", &REQUEST_BUCKETS),
        ("tetral_tasks_queue_wait_seconds", &LONG_OPERATION_BUCKETS),
        ("tetral_uploads_queue_wait_seconds", &LONG_OPERATION_BUCKETS),
        (
            "tetral_downloads_queue_wait_seconds",
            &LONG_OPERATION_BUCKETS,
        ),
    ];

    #[test]
    fn the_cli_catalogue_parses_and_declares_exactly_the_local_metric_families() {
        let families: Vec<Family> = catalogue()
            .metrics
            .iter()
            .map(|metric| {
                let labels = metric
                    .labels
                    .iter()
                    .map(|label| {
                        let values = label.values.iter().map(String::as_str).collect();
                        (label.name.as_str(), values)
                    })
                    .collect();
                (metric.name.as_str(), metric.kind.as_str(), labels)
            })
            .collect();
        let expected: Vec<Family> = LOCAL_METRICS
            .iter()
            .map(|&(name, kind, labels)| {
                let labels = labels
                    .iter()
                    .map(|&(label, values)| (label, values.to_vec()))
                    .collect();
                (name, kind, labels)
            })
            .collect();
        assert_eq!(families, expected);

        let buckets: Vec<(&str, &[f64])> = catalogue()
            .metrics
            .iter()
            .filter(|metric| metric.kind == MetricKind::Histogram)
            .map(|metric| (metric.name.as_str(), metric.buckets.as_slice()))
            .collect();
        assert_eq!(buckets, LOCAL_HISTOGRAM_BUCKETS);

        assert_eq!(
            REQUEST_BUCKETS,
            [
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]
        );
        assert_eq!(
            LONG_OPERATION_BUCKETS,
            [
                0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0
            ]
        );
    }

    fn with_metric(
        name: &str,
        change: impl FnOnce(&mut Value),
    ) -> Result<Catalogue, CatalogueError> {
        let mut document: Value = serde_json::from_str(CATALOGUE_JSON).unwrap();
        let metric = document["metrics"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|metric| metric["name"] == name)
            .unwrap();
        change(metric);
        Catalogue::parse(&document.to_string())
    }

    #[test]
    fn a_family_that_breaks_a_metric_rule_is_refused_for_that_rule() {
        type Change = fn(&mut Value);
        let cases: &[(&str, Change, &str)] = &[
            (
                "tetral_reconnects_total",
                |m| m["name"] = json!("tetral_Reconnects_total"),
                "the name is not lowercase words joined by _",
            ),
            (
                "tetral_reconnects_total",
                |m| m["name"] = json!("tetral_reconnects"),
                "a counter does not end in _total",
            ),
            (
                "tetral_reconnects_total",
                |m| m["type"] = json!("gauge"),
                "only a counter ends in _total",
            ),
            (
                "tetral_startup_phase_seconds",
                |m| {
                    m["name"] = json!("tetral_startup_phase");
                    m["type"] = json!("histogram");
                    m["buckets"] = json!([1, 2]);
                },
                "a histogram does not end in its unit",
            ),
            (
                "tetral_probes_queue_wait_seconds",
                |m| m["buckets"] = json!([1, 2, 3]),
                "a duration histogram uses neither the request nor the long-operation bucket set",
            ),
            (
                "tetral_probes_in_progress",
                |m| m["buckets"] = json!([1]),
                "only a histogram has buckets",
            ),
            (
                "tetral_reconnects_total",
                |m| m["labels"][0]["values"] = json!([]),
                "a label has no fixed set of values",
            ),
            (
                "tetral_reconnects_total",
                |m| m["labels"][0]["name"] = json!("session_id"),
                "an identity is a label",
            ),
            (
                "tetral_reconnects_total",
                |m| m["labels"][0]["name"] = json!("sandbox_id"),
                "an identity is a label",
            ),
            (
                "tetral_reconnects_total",
                |m| m["labels"][0]["name"] = json!("workspace_id"),
                "an identity is a label",
            ),
            (
                "tetral_reconnects_total",
                |m| m["labels"][0]["name"] = json!("le"),
                "a label name is reserved or repeated",
            ),
            (
                "tetral_tasks_rejected_total",
                |m| m["labels"][0]["name"] = json!("reason"),
                "a rejection counter is not labelled by cause alone",
            ),
            (
                "tetral_startup_phase_seconds",
                |m| {
                    m["name"] = json!("tetral_startup_phase_bytes");
                    m["type"] = json!("histogram");
                    m["buckets"] = json!([]);
                },
                "a histogram has no buckets",
            ),
            (
                "tetral_tasks_queued",
                |m| m["name"] = json!("tetral_jobs_queued"),
                "a queued series lacks its oldest-queued or queue-wait sibling",
            ),
            (
                "tetral_tasks_oldest_queued_seconds",
                |m| {
                    m["type"] = json!("histogram");
                    m["buckets"] = json!(REQUEST_BUCKETS);
                },
                "an oldest-queued series is not a gauge beside its queued series",
            ),
            (
                "tetral_tasks_queue_wait_seconds",
                |m| {
                    m["type"] = json!("gauge");
                    m["buckets"] = json!([]);
                },
                "a queue-wait series is not a histogram beside its queued series",
            ),
            (
                "tetral_probes_limit",
                |m| m["name"] = json!("tetral_jobs_limit"),
                "a limit series has no in-progress series beside it",
            ),
        ];
        for &(family, change, rule) in cases {
            let refused = with_metric(family, change);
            assert!(
                matches!(&refused, Err(CatalogueError::Metric(_, broken)) if *broken == rule),
                "a changed {family}: {refused:?}, not refused for: {rule}"
            );
        }

        let mut document: Value = serde_json::from_str(CATALOGUE_JSON).unwrap();
        let metrics = document["metrics"].as_array_mut().unwrap();
        metrics.push(metrics[0].clone());
        let refused = Catalogue::parse(&document.to_string());
        assert!(
            matches!(
                &refused,
                Err(CatalogueError::Metric(_, "a family is declared twice"))
            ),
            "{refused:?}"
        );
    }

    #[test]
    fn an_event_whose_name_or_fields_break_the_record_rules_is_refused() {
        let base: Value = serde_json::from_str(CATALOGUE_JSON).unwrap();
        let cases = [
            json!({"name": "Cold_start", "level": "INFO"}),
            json!({"name": "x", "level": "INFO", "fields": [{"name": "event", "type": "integer"}]}),
            json!({"name": "x", "level": "INFO", "fields": [{"name": "elapsed", "type": "ms"}]}),
            json!({"name": "x", "level": "INFO", "fields": [{"name": "kind", "type": "enum"}]}),
            json!({"name": "x", "level": "INFO", "fields": [{"name": "path", "type": "string"}]}),
        ];
        for case in cases {
            let mut document = base.clone();
            document["events"]
                .as_array_mut()
                .unwrap()
                .push(case.clone());
            assert!(
                Catalogue::parse(&document.to_string()).is_err(),
                "accepted {case}"
            );
        }

        let mut document = base.clone();
        let invalid = document["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|event| event["name"] == INVALID_EVENT)
            .unwrap();
        invalid["fields"] = json!([]);
        assert!(matches!(
            Catalogue::parse(&document.to_string()),
            Err(CatalogueError::Event(name, _)) if name == INVALID_EVENT
        ));
    }
}
