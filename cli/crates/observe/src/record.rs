//! The record writer: a `tracing-subscriber` layer that turns each event of a
//! workspace crate into one JSON record line.

use std::fmt::{self, Write as _};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id as SpanId, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::catalogue::{
    Catalogue, ErrorSpec, EventSpec, FieldKind, INVALID_EVENT, ORIGINAL_EVENT_FIELD, catalogue,
    is_event_name,
};
use crate::level::Level;
use crate::sinks::Sinks;
use crate::trace_context::TraceContext;

/// The fields every record has, plus the failure fields.
pub const RECORD_FIELDS: &[&str] = &[
    "time",
    "level",
    "event",
    "msg",
    "service.name",
    "service.version",
    "service.instance.id",
    "log.type",
    "error.class",
    "error.code",
    "error.message_safe",
    "error.message",
];

/// The correlation fields a record carries when its work has them.
pub const CORRELATION_FIELDS: &[&str] = &[
    "trace_id",
    "span_id",
    "request.id",
    "workspace.id",
    "session.id",
    "thread.id",
    "model_request.id",
    "provider.request.id",
    "tool.use.event.id",
    "task.id",
    "message.id",
    "request.message.id",
    "connection.id",
    "instance.id",
    "credential.id",
    "binding.id",
    "computer.id",
    "drive.id",
];

/// The crates whose events become records; every other target, a
/// dependency's included, is dropped before formatting.
const WORKSPACE_CRATES: &[&str] = &[
    "tetral",
    "appserver",
    "sdk",
    "connection",
    "session",
    "execution",
    "drive",
    "mcp",
    "protocol",
    "state",
    "observe",
];

/// The longest string value a record holds; a longer one is replaced by
/// [`TRUNCATED`] without being copied, but a [`Cause`] keeps this many of
/// its first bytes.
pub const STRING_LIMIT_BYTES: usize = 1024;
const TRUNCATED: &str = "[TRUNCATED]";
/// What replaces everything after a Drive's mount directory in a [`Cause`].
const BELOW_MOUNT: &str = "[…]";

/// A protocol identity recorded in a correlation or identity field.
#[derive(Debug, Clone, Copy)]
pub struct Id<'a>(pub &'a str);

/// The `error.code` of a failure. The record takes the failure's class and
/// fixed text from the catalogue entry of that code.
#[derive(Debug, Clone, Copy)]
pub struct Code(pub &'static str);

/// The cause of a failure, its record's `error.message`. Its text comes only
/// from the CLI's own code, the operating system, a linked library, or a
/// helper process the CLI starts for its own work, up to the moment that
/// helper starts a Session's work, as the sandbox launcher does before the
/// command it launches runs; never from what Session work produced: the
/// output of a command, a tool, an MCP server, a model or a remote Git
/// server.
#[derive(Debug, Clone)]
pub struct Cause(String);

impl Cause {
    /// `text` with each URL reduced to its scheme, host and path, everything
    /// after a Drive's mount directory replaced by `[…]`, each control
    /// character by a space, and then cut to its first
    /// [`STRING_LIMIT_BYTES`] at a character boundary, followed by
    /// `[TRUNCATED]` when cut.
    pub fn system(text: &str) -> Cause {
        let mut text = sanitize(&cut_below_mount(&without_url_secrets(text)));
        if text.len() > STRING_LIMIT_BYTES {
            let mut end = STRING_LIMIT_BYTES;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
            text.push_str(TRUNCATED);
        }
        Cause(text)
    }
}

/// `text` with each URL, a scheme and `://` followed by anything up to white
/// space or one of ``"'<>` ``, reduced to its scheme, host and path: the
/// userinfo, the query and the fragment go, and a host that holds anything
/// but a host's characters becomes `[REDACTED]` and loses its path.
/// Punctuation that ends the URL's text stays after it.
fn without_url_secrets(text: &str) -> String {
    let scheme_byte = |byte: u8| byte.is_ascii_alphanumeric() || b"+.-".contains(&byte);
    let bytes = text.as_bytes();
    let mut shaped = String::with_capacity(text.len());
    let mut copied = 0;
    let mut from = 0;
    while let Some(found) = text[from..].find("://") {
        let separator = from + found;
        from = separator + 3;
        let mut start = separator;
        while start > copied && scheme_byte(bytes[start - 1]) {
            start -= 1;
        }
        while start < separator && !bytes[start].is_ascii_alphabetic() {
            start += 1;
        }
        let rest = &text[from..];
        let length = rest
            .find([' ', '\t', '\n', '\x0c', '\r', '"', '\'', '<', '>', '`'])
            .unwrap_or(rest.len());
        if start == separator || length == 0 {
            continue;
        }
        let mut url =
            rest[..length].trim_end_matches(['.', ',', ';', ':', '!', '?', ')', ']', '}']);
        if url.matches('[').count() > url.matches(']').count() && rest[url.len()..].starts_with(']')
        {
            // The bracket closes an IPv6 host and is part of the URL.
            url = &rest[..url.len() + 1];
        }
        let authority = &url[..url.find(['/', '?', '#']).unwrap_or(url.len())];
        let host = authority.rsplit('@').next().unwrap_or(authority);
        shaped.push_str(&text[copied..from]);
        if is_url_host(host) {
            let tail = &url[authority.len()..];
            shaped.push_str(host);
            shaped.push_str(&tail[..tail.find(['?', '#']).unwrap_or(tail.len())]);
        } else {
            shaped.push_str("[REDACTED]");
        }
        shaped.push_str(&rest[url.len()..length]);
        from += length;
        copied = from;
    }
    shaped.push_str(&text[copied..]);
    shaped
}

/// Whether `host` is a name or an address, in brackets when IPv6, with or
/// without a port.
fn is_url_host(host: &str) -> bool {
    let (name, port) = match host.strip_prefix('[') {
        Some(bracketed) => match bracketed.split_once(']') {
            Some((address, port))
                if !address.is_empty()
                    && address
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() || b":.".contains(&byte)) =>
            {
                ("", port.strip_prefix(':').or(port.is_empty().then_some("")))
            }
            _ => return false,
        },
        None => match host.split_once(':') {
            Some((name, port)) => (name, Some(port)),
            None => (host, Some("")),
        },
    };
    name.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"._~%-".contains(&byte))
        && port.is_some_and(|port| port.bytes().all(|byte| byte.is_ascii_digit()))
}

/// `text` up to the end of the first Drive mount directory,
/// `sessions/<session_id>/drives/<drive_id>` behind any prefix, that a `/`
/// follows, then [`BELOW_MOUNT`]. An identity may hold any character but `/`,
/// so each runs to the next `/`, and a mount directory followed by other
/// text and a path is cut at that path.
fn cut_below_mount(text: &str) -> String {
    const SESSIONS: &str = "sessions/";
    for (start, _) in text.match_indices(SESSIONS) {
        let rest = &text[start + SESSIONS.len()..];
        let Some(session) = rest.find('/').filter(|&end| end > 0) else {
            continue;
        };
        let Some(drive) = rest[session..].strip_prefix("/drives/") else {
            continue;
        };
        let length = drive.find('/').unwrap_or(drive.len());
        let end = text.len() - drive.len() + length;
        if length > 0 && end < text.len() {
            return format!("{}{BELOW_MOUNT}", &text[..end]);
        }
    }
    text.to_owned()
}

/// A value that is one of an enum field's declared values.
pub trait RecordEnum {
    fn record_str(&self) -> &'static str;
}

/// A value that a record field can hold: integers, booleans, `Id`, `Code`,
/// `Cause`, `Ms` and `RecordEnum` types. No string type implements it, so text
/// reaches a record only as an identity, an enum's declared value, a code's
/// catalogue text or a failure's cause, never as free text such as a command
/// line.
pub trait RecordValue {
    type Value<'a>: tracing::Value
    where
        Self: 'a;
    fn record_value(&self) -> Self::Value<'_>;
    /// The code of a `Code`, which the catalogue check reads.
    fn code(&self) -> Option<&'static str> {
        None
    }
    /// Whether it is a `Cause`, the one value `error.message` takes.
    fn is_cause(&self) -> bool {
        false
    }
}

macro_rules! record_as {
    ($target:ty: $($source:ty),*) => {$(
        impl RecordValue for $source {
            type Value<'a> = $target;
            fn record_value(&self) -> $target {
                *self as $target
            }
        }
    )*};
}
record_as!(u64: u8, u16, u32, u64, usize);
record_as!(i64: i8, i16, i32, i64, isize);

impl RecordValue for bool {
    type Value<'a> = bool;
    fn record_value(&self) -> bool {
        *self
    }
}

impl<'s> RecordValue for Id<'s> {
    type Value<'a>
        = &'a str
    where
        Self: 'a;
    fn record_value(&self) -> &str {
        self.0
    }
}

impl RecordValue for Code {
    type Value<'a> = &'static str;
    fn record_value(&self) -> &'static str {
        self.0
    }
    fn code(&self) -> Option<&'static str> {
        Some(self.0)
    }
}

impl RecordValue for Cause {
    type Value<'a> = &'a str;
    fn record_value(&self) -> &str {
        &self.0
    }
    fn is_cause(&self) -> bool {
        true
    }
}

impl<T: RecordEnum> RecordValue for T {
    type Value<'a>
        = &'static str
    where
        T: 'a;
    fn record_value(&self) -> &'static str {
        self.record_str()
    }
}

/// The identities a correlation span adds to every record inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity {
    Session,
    Binding,
    Connection,
    Instance,
    Credential,
    Computer,
    Drive,
    Message,
    RequestMessage,
    ToolUseEvent,
    Task,
}

impl Identity {
    fn field(self) -> &'static str {
        match self {
            Identity::Session => "session.id",
            Identity::Binding => "binding.id",
            Identity::Connection => "connection.id",
            Identity::Instance => "instance.id",
            Identity::Credential => "credential.id",
            Identity::Computer => "computer.id",
            Identity::Drive => "drive.id",
            Identity::Message => "message.id",
            Identity::RequestMessage => "request.message.id",
            Identity::ToolUseEvent => "tool.use.event.id",
            Identity::Task => "task.id",
        }
    }
}

/// A span whose records carry the given identities and, for work caused by a
/// usable `traceparent`, its `trace_id`. Inner spans override outer ones.
pub fn correlation_span(
    identities: &[(Identity, &str)],
    trace: Option<&TraceContext>,
) -> tracing::Span {
    let span = tracing::info_span!(
        target: "observe",
        "correlation",
        session.id = tracing::field::Empty,
        binding.id = tracing::field::Empty,
        connection.id = tracing::field::Empty,
        instance.id = tracing::field::Empty,
        credential.id = tracing::field::Empty,
        computer.id = tracing::field::Empty,
        drive.id = tracing::field::Empty,
        message.id = tracing::field::Empty,
        request.message.id = tracing::field::Empty,
        tool.use.event.id = tracing::field::Empty,
        task.id = tracing::field::Empty,
        trace_id = tracing::field::Empty,
    );
    for (identity, value) in identities {
        span.record(identity.field(), *value);
    }
    if let Some(trace) = trace {
        span.record("trace_id", trace.trace_id());
    }
    span
}

/// Writes each event of a workspace crate as one record to the sinks.
pub struct RecordLayer {
    level: Level,
    catalogue: &'static Catalogue,
    sinks: Arc<Sinks>,
    string_limit: usize,
    on_broken_rule: fn(&str),
}

impl RecordLayer {
    pub fn new(level: Level, sinks: Arc<Sinks>) -> RecordLayer {
        RecordLayer::with_string_limit(level, sinks, STRING_LIMIT_BYTES)
    }

    pub fn with_string_limit(level: Level, sinks: Arc<Sinks>, string_limit: usize) -> RecordLayer {
        RecordLayer {
            level,
            catalogue: catalogue(),
            sinks,
            string_limit,
            on_broken_rule: fail_record_rule,
        }
    }

    /// A layer that writes `diagnostic.invalid_event` for an undeclared
    /// record, as a release build does, instead of failing the test.
    #[cfg(test)]
    fn without_debug_panics(mut self) -> RecordLayer {
        self.on_broken_rule = |_| {};
        self
    }

    fn fields(&self) -> Fields {
        Fields {
            values: Vec::new(),
            string_limit: self.string_limit,
        }
    }
}

fn is_workspace_target(target: &str) -> bool {
    let crate_name = target.split("::").next().unwrap_or_default();
    WORKSPACE_CRATES.contains(&crate_name)
}

#[derive(Debug, Clone)]
enum Captured {
    Unsigned(u64),
    Signed(i64),
    Bool(bool),
    Text(String),
    Ms(u64),
}

struct Fields {
    values: Vec<(&'static str, Captured)>,
    string_limit: usize,
}

impl Default for Fields {
    fn default() -> Fields {
        Fields {
            values: Vec::new(),
            string_limit: STRING_LIMIT_BYTES,
        }
    }
}

impl Fields {
    fn get(&self, name: &str) -> Option<&Captured> {
        self.values
            .iter()
            .rev()
            .find(|(field, _)| *field == name)
            .map(|(_, value)| value)
    }

    fn set(&mut self, name: &'static str, value: Captured) {
        self.values.retain(|(field, _)| *field != name);
        self.values.push((name, value));
    }
}

/// Formats a value up to a limit and fails past it, so that a long value is
/// never formatted whole.
struct BoundedText {
    text: String,
    limit: usize,
}

impl fmt::Write for BoundedText {
    fn write_str(&mut self, part: &str) -> fmt::Result {
        if self.text.len() + part.len() > self.limit {
            return Err(fmt::Error);
        }
        self.text.push_str(part);
        Ok(())
    }
}

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.set(field.name(), Captured::Unsigned(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.set(field.name(), Captured::Signed(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.set(field.name(), Captured::Bool(value));
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        let ms = u64::try_from(value).unwrap_or(u64::MAX);
        self.set(field.name(), Captured::Ms(ms));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        let limit = match field.name() {
            "event" => crate::catalogue::EVENT_NAME_MAX_BYTES,
            "error.message" => STRING_LIMIT_BYTES + TRUNCATED.len(),
            _ => self.string_limit,
        };
        let text = if value.len() > limit {
            TRUNCATED.to_owned()
        } else {
            value.to_owned()
        };
        self.set(field.name(), Captured::Text(text));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let mut bounded = BoundedText {
            text: String::new(),
            limit: self.string_limit,
        };
        let text = match fmt::write(&mut bounded, format_args!("{value:?}")) {
            Ok(()) => bounded.text,
            Err(_) => TRUNCATED.to_owned(),
        };
        self.set(field.name(), Captured::Text(text));
    }
}

struct SpanFields(Fields);

impl SpanFields {
    fn keep_correlation(fields: Fields) -> SpanFields {
        let mut kept = Fields::default();
        for (name, value) in fields.values {
            if CORRELATION_FIELDS.contains(&name) && matches!(value, Captured::Text(_)) {
                kept.set(name, value);
            }
        }
        SpanFields(kept)
    }
}

/// A debug build checks every event of a workspace crate that reaches the
/// layer against the catalogue, whatever the level and the sinks' room, so
/// that one recorded without observe's macros fails its test too; a release
/// build reads only the events it writes.
const CHECKS_EVERY_EVENT: bool = cfg!(debug_assertions);

impl<S> Layer<S> for RecordLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        is_workspace_target(metadata.target())
            && (metadata.is_span() || CHECKS_EVERY_EVENT || self.level.allows(metadata.level()))
    }

    fn on_new_span(&self, attributes: &Attributes<'_>, id: &SpanId, ctx: Context<'_, S>) {
        let mut fields = self.fields();
        attributes.record(&mut fields);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut()
                .insert(SpanFields::keep_correlation(fields));
        }
    }

    fn on_record(&self, id: &SpanId, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = self.fields();
        values.record(&mut fields);
        let mut extensions = span.extensions_mut();
        if let Some(existing) = extensions.get_mut::<SpanFields>() {
            for (name, value) in SpanFields::keep_correlation(fields).0.values {
                existing.0.set(name, value);
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let allowed = self.level.allows(metadata.level());
        if !is_workspace_target(metadata.target()) || !(allowed || CHECKS_EVERY_EVENT) {
            return;
        }
        let mut fields = self.fields();
        event.record(&mut fields);
        let fail = |broken: &BrokenRule| {
            let event_name = match fields.get("event") {
                Some(Captured::Text(name)) => name.as_str(),
                _ => "",
            };
            let site = match (metadata.file(), metadata.line()) {
                (Some(file), Some(line)) => format!("{file}:{line}"),
                _ => metadata.target().to_owned(),
            };
            (self.on_broken_rule)(&broken.problem(event_name, &site));
        };
        let declared = declared_spec(self.catalogue, metadata.level(), &fields)
            .inspect_err(fail)
            .ok();
        // A failure that lacks its code or its cause is still written, as it
        // is, and one with a cause its code declares it has not without that
        // cause, which `encode` leaves out: replacing it would lose the failure.
        if let Some(spec) = declared {
            let error = match fields.get("error.code") {
                Some(Captured::Text(code)) => spec.error(code),
                _ => None,
            };
            let message = fields.get("error.message").is_some();
            if let Some(broken) = broken_failure_field(spec, error, message) {
                fail(&broken);
            }
        }
        let spec = declared.unwrap_or_else(|| {
            self.catalogue
                .event(INVALID_EVENT)
                .expect("the catalogue declares diagnostic.invalid_event")
        });
        // The level applies to the record written as well as to the event, so
        // `diagnostic.invalid_event` replaces an event only at its own level.
        if !allowed || !self.level.allows(&tracing_level(spec)) {
            return;
        }
        let mut correlation = Fields::default();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(span_fields) = span.extensions().get::<SpanFields>() {
                    for (name, value) in &span_fields.0.values {
                        correlation.set(name, value.clone());
                    }
                }
            }
        }
        self.sinks
            .write_with(|| encode(spec, declared.is_some(), &fields, &correlation));
    }
}

/// Fails the test that records a metric the catalogue does not declare,
/// since every test build has debug assertions. A release build goes on.
pub(crate) fn fail_undeclared(what: &str) {
    if cfg!(debug_assertions) {
        panic!("{what} is not declared in cli/observability.json");
    }
}

/// Fails the test whose record breaks a rule of the catalogue. A release
/// build goes on, and the layer writes what the rule leaves of the record.
fn fail_record_rule(problem: &str) {
    if cfg!(debug_assertions) {
        panic!("{problem}");
    }
}

/// A rule of the catalogue that a record breaks, and the record's error
/// code when the rule is about one.
struct BrokenRule {
    code: Option<String>,
    rule: String,
}

impl BrokenRule {
    fn of_event(rule: impl Into<String>) -> BrokenRule {
        BrokenRule {
            code: None,
            rule: rule.into(),
        }
    }

    fn of_code(code: &str, rule: &str) -> BrokenRule {
        BrokenRule {
            code: Some(code.to_owned()),
            rule: rule.to_owned(),
        }
    }

    /// What the failing test reads: the event, the code, the rule and the
    /// file and line that wrote the record.
    fn problem(&self, event: &str, site: &str) -> String {
        let code = match &self.code {
            Some(code) => format!(", error code {code:?}"),
            None => String::new(),
        };
        format!("event {event:?}{code}: {}; written at {site}", self.rule)
    }
}

const EVENT_NOT_DECLARED: &str = "the event is not declared in cli/observability.json";
const CODE_NOT_DECLARED: &str = "the event does not declare the code in cli/observability.json";
const CAUSE_NOT_DECLARED: &str = "the code is declared without a cause and was written with one";
const CAUSE_MISSING: &str = "the code is declared with a cause and was written without one";
const CAUSE_WITHOUT_CODE: &str =
    "error.message was written without an error code the event declares";
const CODE_NOT_A_CODE: &str = "error.code is written only as a Code";

fn at_another_level(spec: &EventSpec, level: &tracing::Level) -> BrokenRule {
    BrokenRule::of_event(format!(
        "the event is declared at level {} and was written at level {}",
        spec_level(spec),
        level_name(level)
    ))
}

/// The check that observe's macros run where an event is recorded: `fields`
/// holds each field's name, the code of a `Code`, and whether it is a
/// `Cause`; `site` is the file and line of the macro's call.
pub fn check_declared(
    level: &tracing::Level,
    event: &str,
    fields: &[(&str, Option<&str>, bool)],
    site: &str,
) {
    let broken = match catalogue().event(event) {
        None => Some(BrokenRule::of_event(EVENT_NOT_DECLARED)),
        Some(spec) if level_name(level) != spec_level(spec) => Some(at_another_level(spec, level)),
        Some(spec) => {
            let code = fields
                .iter()
                .find(|(name, ..)| *name == "error.code")
                .map(|(_, code, _)| *code);
            let written = code.flatten();
            let error = written.and_then(|code| spec.error(code));
            let message = fields.iter().find(|(name, ..)| *name == "error.message");
            match (written, error, message) {
                _ if code == Some(None) => Some(BrokenRule::of_event(CODE_NOT_A_CODE)),
                (Some(code), None, _) => Some(BrokenRule::of_code(code, CODE_NOT_DECLARED)),
                (Some(code), Some(_), Some((_, _, false))) => Some(BrokenRule::of_code(
                    code,
                    "error.message is written only as a Cause",
                )),
                (None, _, Some(_)) => Some(BrokenRule::of_event(CAUSE_WITHOUT_CODE)),
                _ => broken_failure_field(spec, error, message.is_some()),
            }
        }
    };
    if let Some(broken) = broken {
        fail_record_rule(&broken.problem(event, site));
    }
}

/// What is wrong with the failure fields of a record of `spec` that is still
/// a record of its event: the missing `error.code` of an event that declares
/// errors, the missing `error.message` of a code declared with a cause, or
/// the `error.message` of a code declared without one.
fn broken_failure_field(
    spec: &EventSpec,
    error: Option<&ErrorSpec>,
    message: bool,
) -> Option<BrokenRule> {
    match error {
        None if !spec.errors.is_empty() => Some(BrokenRule::of_event(
            "the event declares errors and was written without error.code",
        )),
        Some(error) if error.cause && !message => {
            Some(BrokenRule::of_code(&error.code, CAUSE_MISSING))
        }
        Some(error) if !error.cause && message => {
            Some(BrokenRule::of_code(&error.code, CAUSE_NOT_DECLARED))
        }
        _ => None,
    }
}

/// The catalogue entry of a record, or the rule that makes the record
/// undeclared: an event name the catalogue lacks, a level other than the
/// entry's, an `error.code` the entry does not list, or an `error.message`
/// without an error code.
fn declared_spec<'c>(
    catalogue: &'c Catalogue,
    level: &tracing::Level,
    fields: &Fields,
) -> Result<&'c EventSpec, BrokenRule> {
    let Some(Captured::Text(name)) = fields.get("event") else {
        return Err(BrokenRule::of_event(EVENT_NOT_DECLARED));
    };
    let spec = catalogue
        .event(name)
        .ok_or_else(|| BrokenRule::of_event(EVENT_NOT_DECLARED))?;
    if level_name(level) != spec_level(spec) {
        return Err(at_another_level(spec, level));
    }
    let error = match fields.get("error.code") {
        None => None,
        Some(Captured::Text(code)) => match spec.error(code) {
            Some(error) => Some(error),
            None => return Err(BrokenRule::of_code(code, CODE_NOT_DECLARED)),
        },
        Some(_) => return Err(BrokenRule::of_event(CODE_NOT_A_CODE)),
    };
    if error.is_none() && fields.get("error.message").is_some() {
        return Err(BrokenRule::of_event(CAUSE_WITHOUT_CODE));
    }
    Ok(spec)
}

/// The record of an event of entry `spec`; an undeclared event, whose entry
/// is `diagnostic.invalid_event`, keeps its spans' correlation fields and,
/// of its own fields, only its event's name.
fn encode(spec: &EventSpec, declared: bool, fields: &Fields, correlation: &Fields) -> Vec<u8> {
    let level = spec_level(spec);

    let mut record = JsonObject::new();
    record.text("time", &rfc3339_now());
    record.raw("level", &json_string(level));
    record.text("event", &spec.name);
    record.text("msg", &spec.name);
    record.text("service.name", "tetral");
    record.text("service.version", env!("CARGO_PKG_VERSION"));
    record.text("service.instance.id", instance_id());
    record.text("log.type", spec.log_type.as_str());

    let mut carried = Fields::default();
    for (name, value) in &correlation.values {
        carried.set(name, value.clone());
    }
    for (name, value) in fields.values.iter().filter(|_| declared) {
        if CORRELATION_FIELDS.contains(name) && matches!(value, Captured::Text(_)) {
            carried.set(name, value.clone());
        }
    }
    for (name, value) in &carried.values {
        record.captured(name, value);
    }

    if declared {
        for field in &spec.fields {
            let Some(value) = fields.get(&field.name) else {
                continue;
            };
            let typed = match (field.kind, value) {
                (FieldKind::Integer, Captured::Unsigned(_) | Captured::Signed(_)) => true,
                (FieldKind::Ms, Captured::Ms(_)) => true,
                (FieldKind::Boolean, Captured::Bool(_)) => true,
                (FieldKind::Identity, Captured::Text(_)) => true,
                (FieldKind::EventName, Captured::Text(text)) => is_event_name(text),
                (FieldKind::Enum, Captured::Text(text)) => {
                    field.values.iter().any(|allowed| allowed == text)
                }
                _ => false,
            };
            if typed {
                record.captured(&field.name, value);
            }
        }
        if let Some(Captured::Text(code)) = fields.get("error.code")
            && let Some(error) = spec.error(code)
        {
            record.text("error.class", &error.class);
            record.text("error.code", &error.code);
            record.text("error.message_safe", &error.message_safe);
            if let Some(Captured::Text(message)) = fields.get("error.message")
                && error.cause
            {
                record.text("error.message", message);
            }
        }
    } else if let Some(Captured::Text(name)) = fields.get("event")
        && is_event_name(name)
    {
        // An event that breaks the name rule may be any text, so it is left out.
        record.text(ORIGINAL_EVENT_FIELD, name);
    }
    record.finish()
}

fn level_name(level: &tracing::Level) -> &'static str {
    match *level {
        tracing::Level::TRACE | tracing::Level::DEBUG => "DEBUG",
        tracing::Level::INFO => "INFO",
        tracing::Level::WARN => "WARN",
        tracing::Level::ERROR => "ERROR",
    }
}

fn tracing_level(spec: &EventSpec) -> tracing::Level {
    use crate::catalogue::EventLevel;
    match spec.level {
        EventLevel::Debug => tracing::Level::DEBUG,
        EventLevel::Info => tracing::Level::INFO,
        EventLevel::Warn => tracing::Level::WARN,
        EventLevel::Error => tracing::Level::ERROR,
    }
}

fn spec_level(spec: &EventSpec) -> &'static str {
    use crate::catalogue::EventLevel;
    match spec.level {
        EventLevel::Debug => "DEBUG",
        EventLevel::Info => "INFO",
        EventLevel::Warn => "WARN",
        EventLevel::Error => "ERROR",
    }
}

/// Random per process: read from the operating system's random source, or,
/// where it cannot be read, made from the time and the process ID.
fn instance_id() -> &'static str {
    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID.get_or_init(|| {
        use std::io::Read as _;
        let mut bytes = [0u8; 16];
        let read = std::fs::File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut bytes));
        if read.is_err() {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            bytes = (nanos ^ u128::from(std::process::id())).to_le_bytes();
        }
        bytes.iter().fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
    })
}

struct JsonObject(String);

impl JsonObject {
    fn new() -> JsonObject {
        JsonObject(String::from("{"))
    }

    fn raw(&mut self, name: &str, value: &str) {
        if self.0.len() > 1 {
            self.0.push(',');
        }
        self.0.push_str(&json_string(name));
        self.0.push(':');
        self.0.push_str(value);
    }

    fn text(&mut self, name: &str, value: &str) {
        self.raw(name, &json_string(&sanitize(value)));
    }

    fn captured(&mut self, name: &str, value: &Captured) {
        match value {
            Captured::Unsigned(number) | Captured::Ms(number) => {
                self.raw(name, &number.to_string())
            }
            Captured::Signed(number) => self.raw(name, &number.to_string()),
            Captured::Bool(flag) => self.raw(name, if *flag { "true" } else { "false" }),
            Captured::Text(text) => self.text(name, text),
        }
    }

    fn finish(mut self) -> Vec<u8> {
        self.0.push_str("}\n");
        self.0.into_bytes()
    }
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("a string encodes as JSON")
}

/// Replaces each control character with a space, so that a value never
/// breaks the record's line.
fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// The current UTC time in RFC 3339 with exactly six fractional digits.
fn rfc3339_now() -> String {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    rfc3339(since.as_secs(), since.subsec_micros())
}

fn rfc3339(seconds: u64, micros: u32) -> String {
    let days = seconds / 86_400;
    let rest = seconds % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{micros:06}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's
/// algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::{self, Write};
    use std::sync::Mutex;

    use serde_json::Value;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::metrics::Registry;
    use crate::sinks::Sink;

    /// A flush limit no sink of a passing test reaches.
    pub(crate) const FLUSH: std::time::Duration = std::time::Duration::from_secs(30);

    /// A writer that keeps every line it gets.
    #[derive(Clone, Default)]
    pub(crate) struct Written(Arc<Mutex<Vec<u8>>>);

    impl Write for Written {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Written {
        pub(crate) fn records(&self) -> Vec<Value> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }
        pub(crate) fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    /// Runs `work` under a record layer whose two sinks write to buffers,
    /// and returns them once every record is written.
    pub(crate) fn record_with(level: Level, work: impl FnOnce()) -> (Written, Written) {
        record_with_limit(level, STRING_LIMIT_BYTES, work)
    }

    fn record_with_limit(level: Level, limit: usize, work: impl FnOnce()) -> (Written, Written) {
        record_through(
            |sinks| RecordLayer::with_string_limit(level, sinks, limit),
            work,
        )
    }

    /// As `record_with`, with an undeclared record written as
    /// `diagnostic.invalid_event`, so that a record the level should have
    /// dropped shows in the sinks.
    fn record_without_debug_panics(level: Level, work: impl FnOnce()) -> (Written, Written) {
        record_through(
            |sinks| RecordLayer::new(level, sinks).without_debug_panics(),
            work,
        )
    }

    fn record_through(
        layer: impl FnOnce(Arc<Sinks>) -> RecordLayer,
        work: impl FnOnce(),
    ) -> (Written, Written) {
        let registry = Registry::new(catalogue());
        let (stderr, file) = (Written::default(), Written::default());
        let sinks = Arc::new(Sinks::new(
            Sink::spawn_stderr(Box::new(stderr.clone()), &registry),
            crate::sinks::tests::TEST_CLOCK,
        ));
        sinks.attach_file(Sink::spawn_file(Box::new(file.clone()), &registry));
        let subscriber = tracing_subscriber::registry().with(layer(sinks.clone()));
        tracing::subscriber::with_default(subscriber, work);
        sinks.flush(FLUSH);
        (stderr, file)
    }

    enum Phase {
        Config,
    }

    impl RecordEnum for Phase {
        fn record_str(&self) -> &'static str {
            match self {
                Phase::Config => "config",
            }
        }
    }

    struct Cli;

    impl RecordEnum for Cli {
        fn record_str(&self) -> &'static str {
            "cli"
        }
    }

    #[test]
    fn a_record_inside_a_session_and_an_instruction_span_carries_their_identities() {
        let (stderr, file) = record_with(Level::Info, || {
            let session = correlation_span(
                &[(Identity::Session, "sess_1"), (Identity::Binding, "bind_1")],
                None,
            );
            let _session = session.enter();
            let trace =
                TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                    .unwrap();
            let instruction = correlation_span(
                &[
                    (Identity::Message, "msg_1"),
                    (Identity::ToolUseEvent, "tue_1"),
                    (Identity::Task, "task_1"),
                ],
                Some(&trace),
            );
            let _instruction = instruction.enter();
            crate::info!(
                "cold_start.phase_started",
                cold_start.kind = Cli,
                cold_start.phase = Phase::Config
            );
        });
        for records in [stderr.records(), file.records()] {
            assert_eq!(records.len(), 1);
            let record = &records[0];
            assert_eq!(record["level"], "INFO");
            assert_eq!(record["event"], "cold_start.phase_started");
            assert_eq!(record["msg"], record["event"]);
            assert_eq!(record["service.name"], "tetral");
            assert_eq!(record["service.version"], env!("CARGO_PKG_VERSION"));
            assert_eq!(record["log.type"], "app");
            assert_eq!(record["session.id"], "sess_1");
            assert_eq!(record["binding.id"], "bind_1");
            assert_eq!(record["message.id"], "msg_1");
            assert_eq!(record["tool.use.event.id"], "tue_1");
            assert_eq!(record["task.id"], "task_1");
            assert_eq!(record["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
            assert_eq!(record["cold_start.kind"], "cli");
            assert_eq!(record["cold_start.phase"], "config");
            let time = record["time"].as_str().unwrap();
            assert_eq!(time.len(), "2026-10-24T08:15:30.123456Z".len());
            assert!(time.ends_with('Z'));
            assert_eq!(record["service.instance.id"].as_str().unwrap().len(), 32);
        }
    }

    #[test]
    fn an_undeclared_string_field_is_left_out_of_the_record() {
        let (stderr, _) = record_with(Level::Info, || {
            tracing::info!(
                event = "cold_start.phase_started",
                cold_start.kind = "cli",
                cold_start.phase = "config",
                command_line = "rm -rf CANARY",
            );
        });
        let records = stderr.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["cold_start.phase"], "config");
        assert!(records[0].get("command_line").is_none());
        assert!(!stderr.text().contains("CANARY"));
    }

    #[test]
    fn content_held_in_a_span_stays_out_of_its_records() {
        let (stderr, file) = record_with(Level::Info, || {
            let work =
                tracing::info_span!("work", session.id = "sess_1", command = "rm -rf CANARY");
            let _work = work.enter();
            crate::info!(
                "cold_start.phase_started",
                cold_start.kind = Cli,
                cold_start.phase = Phase::Config
            );
        });
        for sink in [&stderr, &file] {
            let records = sink.records();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["session.id"], "sess_1");
            assert!(records[0].get("command").is_none());
            assert!(!sink.text().contains("CANARY"));
        }
    }

    #[test]
    fn a_cause_keeps_of_a_url_its_scheme_host_and_path_and_nothing_below_a_drive_mount_and_its_first_bytes()
     {
        let mount = "/newroot/home/u/.tetral/sessions/sess_1/drives/drv_1";
        let spaced = "/newroot/home/u/.tetral/sessions/sess_1/drives/my drive";
        let same = |text: String| (text.clone(), text);
        let both = |text: &str, expected: &str| (text.to_owned(), expected.to_owned());
        let rows: [(&str, (String, String)); 13] = [
            (
                "a URL with userinfo, a query and a fragment",
                both(
                    "error sending request for url (https://user:hunter2@engine.example.com:8443/v1/drives/drv_1/content?X-Signature=CANARY&part=3#CANARY): operation timed out",
                    "error sending request for url (https://engine.example.com:8443/v1/drives/drv_1/content): operation timed out",
                ),
            ),
            (
                "userinfo no URL parser accepts",
                both(
                    "dial postgres://user:CANARY%zz@db.internal/app failed",
                    "dial postgres://db.internal/app failed",
                ),
            ),
            (
                "a URL whose host cannot be told",
                both(
                    "dial postgres://user:CANARY/ss@db.internal/app failed",
                    "dial postgres://[REDACTED] failed",
                ),
            ),
            (
                "an IPv6 host with a port",
                both(
                    "connect http://[fd00::7]:4317/v1/traces?key=CANARY refused",
                    "connect http://[fd00::7]:4317/v1/traces refused",
                ),
            ),
            (
                "an IPv6 host without a port or path",
                same("Get \"http://[::1]\": refused; then (http://[fd00::7]).".to_owned()),
            ),
            (
                "a URL without a host",
                both(
                    "open file:///var/run/tetral.sock?mode=CANARY: no such file",
                    "open file:///var/run/tetral.sock: no such file",
                ),
            ),
            (
                "the punctuation after a URL, and a second URL",
                both(
                    "fetch (https://example.com/a?token=CANARY), then ://none and x1+y://h/p?q=CANARY.",
                    "fetch (https://example.com/a), then ://none and x1+y://h/p.",
                ),
            ),
            (
                "a URL of a file below a mount directory",
                (
                    format!("open file://{mount}/CANARY.txt?v=CANARY: denied"),
                    format!("open file://{mount}[…]"),
                ),
            ),
            (
                "a mount directory alone",
                same(format!("bwrap: Can't mkdir {mount}: Invalid argument")),
            ),
            (
                "a file below a mount directory",
                (
                    format!("bwrap: Can't create file at {mount}/CANARY.txt: No such file"),
                    format!("bwrap: Can't create file at {mount}[…]"),
                ),
            ),
            (
                "a file below a mount directory whose drive identity holds a space",
                (
                    format!("bwrap: Can't create file at {spaced}/CANARY.txt"),
                    format!("bwrap: Can't create file at {spaced}[…]"),
                ),
            ),
            (
                "a line break",
                ("first\nsecond".to_owned(), "first second".to_owned()),
            ),
            (
                "2,000 bytes",
                (
                    format!("a{}b", "é".repeat(999)),
                    format!("a{}[TRUNCATED]", "é".repeat(511)),
                ),
            ),
        ];
        for (case, (text, expected)) in rows {
            let (stderr, file) = record_with(Level::Info, || {
                crate::warn!(
                    "repository.git_launch_failed",
                    error.code = Code("git_launch_failed"),
                    error.message = Cause::system(&text)
                );
            });
            for sink in [&stderr, &file] {
                let records = sink.records();
                assert_eq!(records.len(), 1, "{case}");
                assert_eq!(records[0]["error.message"], expected.as_str(), "{case}");
                assert!(!sink.text().contains("CANARY"), "{case}");
            }
        }
    }

    #[test]
    fn an_event_of_a_dependency_reaches_neither_sink() {
        let (stderr, file) = record_with(Level::Debug, || {
            tracing::info!(target: "rmcp", event = "cold_start.phase_started", body = "CANARY");
            tracing::error!(target: "rmcp::transport", "CANARY");
        });
        assert_eq!(stderr.text(), "");
        assert_eq!(file.text(), "");
    }

    #[test]
    fn a_value_that_is_not_a_declared_enum_value_is_left_out() {
        let (stderr, _) = record_with(Level::Info, || {
            tracing::info!(
                event = "cold_start.phase_started",
                cold_start.kind = "CANARY",
                cold_start.phase = "config"
            );
        });
        let records = stderr.records();
        assert!(records[0].get("cold_start.kind").is_none());
        assert!(!stderr.text().contains("CANARY"));
    }

    #[test]
    fn recording_an_undeclared_event_fails_the_test() {
        // The rows "with no subscriber" record through observe's macros, so
        // no sink takes them; the others reach the record layer. Each row
        // names its case, the record, and the rule its failure states.
        let rows: [(&str, fn(), &str); 14] = [
            (
                "an event the catalogue lacks, with no subscriber",
                || crate::info!("connection.not_in_the_catalogue"),
                r#"event "connection.not_in_the_catalogue": the event is not declared in cli/observability.json"#,
            ),
            (
                "a declared event at another level, with no subscriber",
                || {
                    crate::warn!(
                        "cold_start.phase_started",
                        cold_start.kind = Cli,
                        cold_start.phase = Phase::Config
                    )
                },
                r#"event "cold_start.phase_started": the event is declared at level INFO and was written at level WARN"#,
            ),
            (
                "an error code the event does not declare, with no subscriber",
                || {
                    crate::info!(
                        "cold_start.phase_started",
                        cold_start.kind = Cli,
                        cold_start.phase = Phase::Config,
                        error.code = Code("nope")
                    )
                },
                r#"event "cold_start.phase_started", error code "nope": the event does not declare the code in cli/observability.json"#,
            ),
            (
                "an error.code that is not a Code, with no subscriber",
                || {
                    crate::warn!(
                        "repository.git_launch_failed",
                        error.code = Id("git_launch_failed")
                    )
                },
                r#"event "repository.git_launch_failed": error.code is written only as a Code"#,
            ),
            (
                "an event that declares errors without an error.code, with no subscriber",
                || crate::warn!("log.file_open_failed"),
                r#"event "log.file_open_failed": the event declares errors and was written without error.code"#,
            ),
            (
                "an error.message that is not a Cause, with no subscriber",
                || {
                    crate::warn!(
                        "repository.git_launch_failed",
                        error.code = Code("git_launch_failed"),
                        error.message = Id("rm -rf CANARY")
                    )
                },
                r#"event "repository.git_launch_failed", error code "git_launch_failed": error.message is written only as a Cause"#,
            ),
            (
                "an error.message without an error code, with no subscriber",
                || {
                    crate::info!(
                        "cold_start.phase_started",
                        cold_start.kind = Cli,
                        cold_start.phase = Phase::Config,
                        error.message = Cause::system("Permission denied")
                    )
                },
                r#"event "cold_start.phase_started": error.message was written without an error code the event declares"#,
            ),
            (
                "an event the catalogue lacks, in the layer",
                || {
                    record_with(Level::Info, || {
                        tracing::info!(event = "connection.not_in_the_catalogue")
                    });
                },
                r#"event "connection.not_in_the_catalogue": the event is not declared in cli/observability.json"#,
            ),
            (
                "an event the catalogue lacks, below the level",
                || {
                    record_with(Level::Info, || {
                        tracing::debug!(event = "connection.not_in_the_catalogue")
                    });
                },
                r#"event "connection.not_in_the_catalogue": the event is not declared in cli/observability.json"#,
            ),
            (
                "a declared event at another level, in the layer",
                || {
                    record_with(Level::Debug, || {
                        tracing::warn!(
                            event = "cold_start.phase_started",
                            cold_start.kind = "cli",
                            cold_start.phase = "config"
                        )
                    });
                },
                r#"event "cold_start.phase_started": the event is declared at level INFO and was written at level WARN"#,
            ),
            (
                "an error code the event does not declare, in the layer",
                || {
                    record_with(Level::Info, || {
                        tracing::info!(
                            event = "cold_start.phase_started",
                            cold_start.kind = "cli",
                            cold_start.phase = "config",
                            error.code = "nope"
                        )
                    });
                },
                r#"event "cold_start.phase_started", error code "nope": the event does not declare the code in cli/observability.json"#,
            ),
            (
                "an error.message without an error code, in the layer",
                || {
                    record_with(Level::Info, || {
                        tracing::info!(
                            event = "cold_start.phase_started",
                            cold_start.kind = "cli",
                            cold_start.phase = "config",
                            error.message = "Permission denied"
                        )
                    });
                },
                r#"event "cold_start.phase_started": error.message was written without an error code the event declares"#,
            ),
            (
                "a cause on a code declared without one, in the layer",
                || {
                    record_with(Level::Info, || {
                        tracing::error!(
                            event = "report.dropped",
                            error.code = "rate_limited",
                            error.message = "Engine said CANARY"
                        )
                    });
                },
                r#"event "report.dropped", error code "rate_limited": the code is declared without a cause and was written with one"#,
            ),
            (
                "a code declared with a cause without its error.message, in the layer",
                || {
                    record_with(Level::Info, || {
                        tracing::warn!(
                            event = "log.file_open_failed",
                            error.code = "log_file_open_failed"
                        )
                    });
                },
                r#"event "log.file_open_failed", error code "log_file_open_failed": the code is declared with a cause and was written without one"#,
            ),
        ];
        for (case, work, rule) in rows {
            let panic = std::panic::catch_unwind(work).expect_err(case);
            let message = panic.downcast_ref::<String>().map(String::as_str);
            assert!(
                message.is_some_and(|message| message.starts_with(&format!("{rule}; written at "))),
                "{case}: {message:?}; want {rule:?}"
            );
        }
    }

    #[test]
    fn a_broken_rule_names_the_line_that_wrote_the_record() {
        use std::sync::atomic::{AtomicU32, Ordering};
        // The line each row records on: the one after the store.
        static LINE: AtomicU32 = AtomicU32::new(0);
        let rows: [(&str, fn()); 2] = [
            ("through observe's macros", || {
                LINE.store(line!() + 1, Ordering::Relaxed);
                crate::info!("connection.not_in_the_catalogue")
            }),
            ("in the layer", || {
                record_with(Level::Info, || {
                    LINE.store(line!() + 1, Ordering::Relaxed);
                    tracing::info!(event = "connection.not_in_the_catalogue")
                });
            }),
        ];
        for (case, work) in rows {
            let panic = std::panic::catch_unwind(work).expect_err(case);
            let message = panic.downcast_ref::<String>().map(String::as_str);
            let site = format!("; written at {}:{}", file!(), LINE.load(Ordering::Relaxed));
            assert!(
                message.is_some_and(|message| message.ends_with(&site)),
                "{case}: {message:?}; want the site {site:?}"
            );
        }
    }

    #[test]
    fn a_failure_record_agrees_with_its_codes_cause_declaration() {
        // A case; the record through observe's macros and as a release
        // build's layer gets it; what fails the test; and the event and
        // `error.message` of the record a release build then writes.
        type Row = (
            &'static str,
            fn(),
            fn(),
            Option<&'static str>,
            &'static str,
            Option<&'static str>,
        );
        const LINE: &str = "bwrap: setting up uid map: Permission denied";
        let rows: [Row; 4] = [
            (
                "declared with a cause and carrying one",
                || {
                    crate::warn!(
                        "repository.git_launch_failed",
                        error.code = Code("git_launch_failed"),
                        error.message = Cause::system(LINE)
                    )
                },
                || {
                    tracing::warn!(
                        event = "repository.git_launch_failed",
                        error.code = "git_launch_failed",
                        error.message = LINE
                    )
                },
                None,
                "repository.git_launch_failed",
                Some(LINE),
            ),
            (
                "declared with a cause and carrying none",
                || {
                    crate::warn!(
                        "repository.git_launch_failed",
                        error.code = Code("git_launch_failed")
                    )
                },
                || {
                    tracing::warn!(
                        event = "repository.git_launch_failed",
                        error.code = "git_launch_failed"
                    )
                },
                Some(
                    r#"event "repository.git_launch_failed", error code "git_launch_failed": the code is declared with a cause and was written without one; written at "#,
                ),
                "repository.git_launch_failed",
                None,
            ),
            (
                "declared without a cause and carrying one",
                || {
                    crate::error!(
                        "report.dropped",
                        error.code = Code("rate_limited"),
                        error.message = Cause::system("Engine said CANARY")
                    )
                },
                || {
                    tracing::error!(
                        event = "report.dropped",
                        error.code = "rate_limited",
                        error.message = "Engine said CANARY"
                    )
                },
                Some(
                    r#"event "report.dropped", error code "rate_limited": the code is declared without a cause and was written with one; written at "#,
                ),
                "report.dropped",
                None,
            ),
            (
                "declared without a cause and carrying none",
                || crate::error!("report.dropped", error.code = Code("rate_limited")),
                || tracing::error!(event = "report.dropped", error.code = "rate_limited"),
                None,
                "report.dropped",
                None,
            ),
        ];
        for (case, through_macros, in_release, fails, event, message) in rows {
            let recorded = std::panic::catch_unwind(|| record_with(Level::Info, through_macros));
            match (fails, recorded) {
                (None, Ok((stderr, _))) => {
                    let records = stderr.records();
                    assert_eq!(records.len(), 1, "{case}");
                    assert_eq!(records[0]["event"], event, "{case}");
                    assert_eq!(
                        records[0].get("error.message").and_then(Value::as_str),
                        message,
                        "{case}"
                    );
                }
                (Some(what), Err(panic)) => {
                    let text = panic.downcast_ref::<String>().map(String::as_str);
                    assert!(
                        text.is_some_and(|text| text.contains(what)),
                        "{case}: {text:?}"
                    );
                }
                (fails, recorded) => {
                    panic!("{case}: fails {fails:?}, recorded {}", recorded.is_ok())
                }
            }
            let (stderr, _) = record_without_debug_panics(Level::Info, in_release);
            let records = stderr.records();
            assert_eq!(records.len(), 1, "{case}");
            assert_eq!(records[0]["event"], event, "{case}");
            assert_eq!(
                records[0].get("error.message").and_then(Value::as_str),
                message,
                "{case}"
            );
            assert!(!stderr.text().contains("CANARY"), "{case}");
        }
    }

    #[test]
    fn without_debug_panics_each_undeclared_record_becomes_diagnostic_invalid_event() {
        // A case, the work that records, and the expected `event.original`.
        type Row = (&'static str, fn(), Option<&'static str>);
        let rows: [Row; 5] = [
            (
                "an event name the catalogue lacks",
                || tracing::info!(event = "connection.not_in_the_catalogue", attempt = 1u64),
                Some("connection.not_in_the_catalogue"),
            ),
            (
                "an event name longer than the catalogue allows",
                || tracing::info!(event = "cold_start.phase_started".repeat(6).as_str()),
                None,
            ),
            (
                "an event that is not an event name",
                || tracing::info!(event = "copied /home/someone/notes"),
                None,
            ),
            (
                "a declared event at another level",
                || {
                    tracing::warn!(
                        event = "cold_start.phase_started",
                        cold_start.kind = "cli",
                        cold_start.phase = "config"
                    )
                },
                Some("cold_start.phase_started"),
            ),
            (
                "an error code the event does not declare",
                || {
                    tracing::info!(
                        event = "cold_start.phase_started",
                        cold_start.kind = "cli",
                        cold_start.phase = "config",
                        error.code = "nope"
                    )
                },
                Some("cold_start.phase_started"),
            ),
        ];
        for (case, work, original) in rows {
            let (stderr, file) = record_without_debug_panics(Level::parse(None).unwrap(), work);
            for records in [stderr.records(), file.records()] {
                assert_eq!(records.len(), 1, "{case}");
                let record = records[0].as_object().unwrap();
                assert_eq!(record["event"], INVALID_EVENT, "{case}");
                assert_eq!(record["level"], "WARN", "{case}");
                assert_eq!(
                    record.get(ORIGINAL_EVENT_FIELD).and_then(Value::as_str),
                    original,
                    "{case}"
                );
                assert!(
                    !record.keys().any(|name| name.starts_with("cold_start.")
                        || name.starts_with("error.")
                        || name == "attempt"),
                    "{case}: {record:?}"
                );
            }
        }

        // The identities of its spans stay; those it carries itself go.
        let (stderr, _) = record_without_debug_panics(Level::parse(None).unwrap(), || {
            let span = correlation_span(&[(Identity::Session, "sess_1")], None);
            let _span = span.enter();
            tracing::info!(
                event = "connection.not_in_the_catalogue",
                task.id = "task_CANARY"
            );
        });
        let record = &stderr.records()[0];
        assert_eq!(record["event"], INVALID_EVENT);
        assert_eq!(record["session.id"], "sess_1");
        assert!(record.get("task.id").is_none(), "{record:?}");
    }

    #[test]
    fn the_level_applies_to_both_sinks() {
        let unset = Level::parse(None).unwrap();
        let (stderr, file) = record_without_debug_panics(unset, || {
            tracing::debug!(
                event = "cold_start.phase_started",
                cold_start.kind = "cli",
                cold_start.phase = "config"
            );
            crate::info!(
                "cold_start.phase_started",
                cold_start.kind = Cli,
                cold_start.phase = Phase::Config
            );
        });
        for records in [stderr.records(), file.records()] {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["level"], "INFO");
        }

        let error = Level::parse(Some(std::ffi::OsStr::new("error"))).unwrap();
        let (stderr, file) = record_without_debug_panics(error, || {
            tracing::error!(event = "connection.not_in_the_catalogue");
        });
        assert_eq!((stderr.text(), file.text()), (String::new(), String::new()));

        let warn = Level::parse(Some(std::ffi::OsStr::new("warn"))).unwrap();
        let (stderr, file) = record_without_debug_panics(warn, || {
            crate::info!(
                "cold_start.phase_started",
                cold_start.kind = Cli,
                cold_start.phase = Phase::Config
            );
        });
        assert_eq!((stderr.text(), file.text()), (String::new(), String::new()));
    }

    #[test]
    fn a_string_longer_than_the_limit_is_replaced_and_control_characters_become_spaces() {
        let sessions = |limit: usize, values: &[&str]| -> Vec<String> {
            let (stderr, _) = record_with_limit(Level::Info, limit, || {
                for value in values {
                    let span = correlation_span(&[(Identity::Session, value)], None);
                    let _span = span.enter();
                    crate::info!(
                        "cold_start.phase_started",
                        cold_start.kind = Cli,
                        cold_start.phase = Phase::Config
                    );
                }
            });
            stderr
                .records()
                .iter()
                .map(|record| record["session.id"].as_str().unwrap().to_owned())
                .collect()
        };
        let (kept, over) = ("a".repeat(1024), "a".repeat(1025));
        assert_eq!(
            sessions(STRING_LIMIT_BYTES, &[&kept, &over, "a\nb\tc\u{7f}"]),
            [kept.clone(), "[TRUNCATED]".to_owned(), "a b c ".to_owned()]
        );
        assert_eq!(
            sessions(8, &["sess_123", "sess_1234"]),
            ["sess_123", "[TRUNCATED]"]
        );
    }

    #[test]
    fn times_are_utc_with_six_fractional_digits() {
        assert_eq!(rfc3339(0, 0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            rfc3339(1_792_829_730, 123_456),
            "2026-10-24T08:15:30.123456Z"
        );
        assert_eq!(rfc3339(951_782_400, 7), "2000-02-29T00:00:00.000007Z");
    }
}
