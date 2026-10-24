#![doc = include_str!("../README.md")]

mod catalogue;
mod level;
mod metrics;
mod record;
mod sinks;
mod trace_context;

use std::io;
use std::path::Path;
use std::sync::Arc;

use tracing_subscriber::layer::SubscriberExt;

pub use catalogue::{
    Catalogue, CatalogueError, EventSpec, FieldKind, LONG_OPERATION_BUCKETS, MetricKind,
    MetricSpec, REQUEST_BUCKETS, catalogue,
};
pub use level::{InvalidLevel, LEVEL_VARIABLE, Level};
pub use metrics::{Counter, Gauge, Histogram, Registry, registry, render};
pub use record::{Code, Id, Identity, RecordEnum, RecordLayer, RecordValue, correlation_span};
pub use sinks::{LOG_FILE_LIMIT_BYTES, LogFile, SINK_QUEUE_RECORDS, Sink, Sinks};
pub use trace_context::TraceContext;

#[doc(hidden)]
pub mod __private {
    pub use crate::record::{RecordValue, check_declared};
    pub use tracing;
}

/// The installed recording of this process.
pub struct Observer {
    sinks: Arc<Sinks>,
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
    Observer { sinks }
}

impl Observer {
    /// Opens the log file in `directory` and writes every later record to it
    /// as well as to stderr.
    pub fn open_log_file(&self, directory: &Path) -> io::Result<()> {
        let file = LogFile::open(directory)?;
        self.sinks
            .attach_file(Sink::spawn_file(Box::new(file), registry()));
        Ok(())
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
