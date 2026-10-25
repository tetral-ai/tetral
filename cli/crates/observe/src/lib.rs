#![doc = include_str!("../README.md")]
#![deny(clippy::print_stdout, clippy::print_stderr)]

mod catalogue;
mod level;
mod metrics;
mod record;
mod sinks;
mod stopwatch;
mod trace_context;

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing_subscriber::layer::SubscriberExt;

pub use catalogue::{
    Catalogue, CatalogueError, EventSpec, FieldKind, LONG_OPERATION_BUCKETS, MetricKind,
    MetricSpec, REQUEST_BUCKETS, catalogue,
};
pub use level::{InvalidLevel, LEVEL_VARIABLE, Level};
pub use metrics::{Counter, Gauge, Histogram, Registry, registry, render};
pub use record::{Code, Id, Identity, RecordEnum, RecordLayer, RecordValue, correlation_span};
pub use sinks::{
    EXIT_FLUSH_LIMIT, LOG_FILE_LIMIT_BYTES, LogFile, SINK_QUEUE_RECORDS, Sink, Sinks, WriterSlot,
};
pub use stopwatch::{MonotonicClock, Ms, Startup, StartupPhases, Stopwatch};
pub use trace_context::TraceContext;

#[doc(hidden)]
pub mod __private {
    pub use crate::record::{RecordValue, check_declared};
    pub use tracing;
}

/// One per process, since a process makes at most one server start.
pub static STARTUP: Startup = Startup::new();

/// The installed recording of this process.
pub struct Observer {
    sinks: Arc<Sinks>,
    held_log_file: Mutex<Option<WriterSlot>>,
}

/// Installs the record layer for the whole process, writing to stderr at
/// `level` and above.
pub fn install(level: Level) -> Observer {
    let sinks = Arc::new(Sinks::new(Sink::spawn_stderr(
        Box::new(io::stderr()),
        registry(),
    )));
    let subscriber = tracing_subscriber::registry().with(RecordLayer::new(level, sinks.clone()));
    let _ = tracing::subscriber::set_global_default(subscriber);
    Observer {
        sinks,
        held_log_file: Mutex::new(None),
    }
}

impl Observer {
    /// Keeps every record from now on, up to the sink's queue, for the log
    /// file that [`Observer::open_log_file`] opens later, so the records made
    /// before it opens reach it too.
    pub fn hold_records_for_log_file(&self) {
        let (sink, slot) =
            Sink::spawn_held(SINK_QUEUE_RECORDS, sinks::failures(registry(), "file"));
        self.sinks.attach_file(sink);
        *self.held() = Some(slot);
    }

    /// Opens the log file in `directory` and writes to it, after the records
    /// held for it, every later record as well as stderr. When it cannot be
    /// opened, each of those records counts in
    /// `tetral_log_write_failures_total{sink="file"}`.
    pub fn open_log_file(&self, directory: &Path) -> io::Result<()> {
        let slot = self.held().take().unwrap_or_else(|| {
            let (sink, slot) =
                Sink::spawn_held(SINK_QUEUE_RECORDS, sinks::failures(registry(), "file"));
            self.sinks.attach_file(sink);
            slot
        });
        match LogFile::open(directory) {
            Ok(file) => {
                slot.fill(Some(Box::new(file)));
                Ok(())
            }
            Err(error) => {
                slot.fill(None);
                Err(error)
            }
        }
    }

    /// Waits, at most `limit`, until every record so far is written or
    /// dropped; records still held for a log file that never opened are
    /// discarded.
    pub fn flush(&self, limit: Duration) {
        drop(self.held().take());
        self.sinks.flush(limit);
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Option<WriterSlot>> {
        self.held_log_file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Records a declared event at `DEBUG`: `observe::debug!("event.name", field = value, ...)`.
#[macro_export]
macro_rules! debug {
    ($event:literal $(, $($key:ident).+ = $value:expr)* $(,)?) => {
        $crate::__record!($crate::__private::tracing::Level::DEBUG, $event $(, $($key).+ = $value)*)
    };
}

/// Records a declared event at `INFO`.
#[macro_export]
macro_rules! info {
    ($event:literal $(, $($key:ident).+ = $value:expr)* $(,)?) => {
        $crate::__record!($crate::__private::tracing::Level::INFO, $event $(, $($key).+ = $value)*)
    };
}

/// Records a declared event at `WARN`.
#[macro_export]
macro_rules! warn {
    ($event:literal $(, $($key:ident).+ = $value:expr)* $(,)?) => {
        $crate::__record!($crate::__private::tracing::Level::WARN, $event $(, $($key).+ = $value)*)
    };
}

/// Records a declared event at `ERROR`.
#[macro_export]
macro_rules! error {
    ($event:literal $(, $($key:ident).+ = $value:expr)* $(,)?) => {
        $crate::__record!($crate::__private::tracing::Level::ERROR, $event $(, $($key).+ = $value)*)
    };
}

/// Checks an event against the catalogue where it is recorded, in a debug
/// build, so that an undeclared one fails whether or not a subscriber or a
/// sink takes it; then records it. Each value is bound once, so the check and
/// the record read the same value; hygiene keeps each binding apart.
#[doc(hidden)]
#[macro_export]
macro_rules! __record {
    (@bind $level:expr, $event:literal; [$($bound:tt)*]; [$($key:ident).+] $value:expr; $($rest:tt)*) => {{
        let value = &$value;
        $crate::__record!(@bind $level, $event; [$($bound)* [$($key).+] value;]; $($rest)*)
    }};
    (@bind $level:expr, $event:literal; [$([$($key:ident).+] $value:ident;)*];) => {{
        if cfg!(debug_assertions) {
            $crate::__private::check_declared(
                &$level,
                $event,
                &[$((stringify!($($key).+), $crate::__private::RecordValue::code($value))),*],
            );
        }
        $crate::__private::tracing::event!(
            target: module_path!(),
            $level,
            event = $event
            $(, $($key).+ = $crate::__private::RecordValue::record_value($value))*
        )
    }};
    ($level:expr, $event:literal $(, $($key:ident).+ = $value:expr)*) => {
        $crate::__record!(@bind $level, $event; []; $([$($key).+] $value;)*)
    };
}
