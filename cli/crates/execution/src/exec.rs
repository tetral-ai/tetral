//! `exec_command` as a task runs it: the defaults and clamps of its waits,
//! the directory and shell it runs in, and the results its reports carry.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use protocol::field::{Count, Maybe, NonEmptyString, Nullable};
use protocol::id::Id;
use protocol::tool::{
    ExecInput, ExecResult, HelperEnvelope, HelperStatus, HelperTool, Metrics, OnWaitExpiry,
    StreamSnapshot, ToolError, ToolResult,
};

/// The initial wait when the call gives none, and its bounds.
pub const DEFAULT_WAIT_MS: u64 = 10_000;
pub const MIN_WAIT_MS: u64 = 250;
pub const MAX_WAIT_MS: u64 = 50_000;

/// A detached command's lifetime when the call gives none, and its bounds.
pub const DEFAULT_LIFETIME_MS: u64 = 1_800_000;
pub const MIN_LIFETIME_MS: u64 = 250;
pub const MAX_LIFETIME_MS: u64 = 3_600_000;

/// What ends the initial wait of a command still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnExpiry {
    /// The command is ended and its report is the error `timeout`.
    Kill,
    /// A `running` report, and the task keeps running.
    Detach,
}

/// One `exec_command` call, resolved without touching the filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecCall {
    pub cmd: String,
    /// The directory the command runs in.
    pub directory: PathBuf,
    pub env: Option<BTreeMap<String, String>>,
    pub wait: Duration,
    pub on_expiry: OnExpiry,
    /// Measured from the actual start; it applies to `detach` only.
    pub lifetime: Duration,
}

impl ExecCall {
    /// The call `input` of a binding whose canonical working directory is
    /// `binding`.
    pub fn new(input: &ExecInput, binding: &Path) -> ExecCall {
        let on_expiry = match input.on_wait_expiry.value() {
            Some(OnWaitExpiry::Detach) => OnExpiry::Detach,
            _ => OnExpiry::Kill,
        };
        ExecCall {
            cmd: input.cmd.as_str().to_owned(),
            directory: directory(&input.cwd, binding),
            env: input.env.value().cloned(),
            wait: clamped(&input.wait_ms, DEFAULT_WAIT_MS, MIN_WAIT_MS, MAX_WAIT_MS),
            on_expiry,
            lifetime: clamped(
                &input.task_lifetime_ms,
                DEFAULT_LIFETIME_MS,
                MIN_LIFETIME_MS,
                MAX_LIFETIME_MS,
            ),
        }
    }
}

fn clamped(value: &Maybe<i64>, default: u64, min: u64, max: u64) -> Duration {
    let millis = match value.value() {
        Some(value) => u64::try_from(*value).unwrap_or(0),
        None => default,
    };
    Duration::from_millis(millis.clamp(min, max))
}

/// The binding's directory for a missing, `null` or empty `cwd`; a relative
/// one joined to it; an absolute one as given.
fn directory(cwd: &Maybe<String>, binding: &Path) -> PathBuf {
    match cwd.value().filter(|cwd| !cwd.is_empty()) {
        Some(cwd) => binding.join(cwd),
        None => binding.to_path_buf(),
    }
}

/// `/bin/bash`, or `/bin/sh` where `/bin/bash` does not exist.
pub fn shell() -> &'static str {
    if Path::new("/bin/bash").exists() {
        "/bin/bash"
    } else {
        "/bin/sh"
    }
}

/// The names of signals 1 to 31 as the Go Helper reports a death by one.
const SIGNAL_NAMES: [&str; 31] = [
    "hangup",
    "interrupt",
    "quit",
    "illegal instruction",
    "trace/breakpoint trap",
    "aborted",
    "bus error",
    "floating point exception",
    "killed",
    "user defined signal 1",
    "segmentation fault",
    "user defined signal 2",
    "broken pipe",
    "alarm clock",
    "terminated",
    "stack fault",
    "child exited",
    "continued",
    "stopped (signal)",
    "stopped",
    "stopped (tty input)",
    "stopped (tty output)",
    "urgent I/O condition",
    "CPU time limit exceeded",
    "file size limit exceeded",
    "virtual timer expired",
    "profiling timer expired",
    "window changed",
    "I/O possible",
    "power failure",
    "bad system call",
];

/// The description of signal `number`: its name, or `signal <n>` for a
/// number without one.
pub fn signal_description(number: i32) -> String {
    usize::try_from(number - 1)
        .ok()
        .and_then(|index| SIGNAL_NAMES.get(index))
        .map_or_else(|| format!("signal {number}"), |name| (*name).to_owned())
}

/// How a task ended, as far as it was observed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExitFacts {
    pub exit_code: Option<i64>,
    pub signal: Option<String>,
}

/// The exit statuses by which the Linux sandbox reports a death by signal
/// `status - 128`: its stages exit with `128 + n` for a command that signal
/// `n` ended, which a command's own exit with such a status cannot be told
/// from.
#[cfg(target_os = "linux")]
const SIGNAL_STATUSES: std::ops::RangeInclusive<i32> = 129..=192;

impl ExitFacts {
    /// The facts of the launched process's wait status `status`.
    pub fn observed(status: std::process::ExitStatus) -> ExitFacts {
        use std::os::unix::process::ExitStatusExt;
        let signal = |number: i32| ExitFacts {
            exit_code: None,
            signal: Some(signal_description(number)),
        };
        if let Some(number) = status.signal() {
            return signal(number);
        }
        match status.code() {
            #[cfg(target_os = "linux")]
            Some(code) if SIGNAL_STATUSES.contains(&code) => signal(code - 128),
            Some(code) => ExitFacts {
                exit_code: Some(i64::from(code)),
                signal: None,
            },
            None => ExitFacts::default(),
        }
    }
}

/// An `exec_command` result envelope.
pub struct Outcome {
    pub task_id: Id,
    pub status: HelperStatus,
    pub error: Option<ToolError>,
    pub exit: ExitFacts,
    pub timed_out: bool,
    pub stdout: StreamSnapshot,
    pub stderr: StreamSnapshot,
    pub duration: Duration,
    pub metrics: Option<Metrics>,
}

impl Outcome {
    pub fn envelope(self) -> HelperEnvelope {
        HelperEnvelope {
            tool: HelperTool::ExecCommand,
            status: self.status,
            truncated: false,
            error: self.error,
            result: ToolResult::Exec(ExecResult {
                task_id: Nullable(Some(self.task_id)),
                exit_code: Nullable(self.exit.exit_code),
                signal: Nullable(self.exit.signal),
                timed_out: self.timed_out,
                cancelled: None,
                was_running: None,
                written_bytes: None,
                stdout: self.stdout,
                stderr: self.stderr,
                duration_ms: millis(self.duration),
            }),
            metrics: self.metrics,
            drive_sync: None,
        }
    }
}

/// A tool error of `kind` with `message`, about `path` when given.
pub fn tool_error(kind: &str, message: &str, path: Option<String>) -> ToolError {
    ToolError {
        kind: NonEmptyString::new(kind).expect("an error kind is not empty"),
        message: message.to_owned(),
        path,
        detail: None,
    }
}

/// An empty stream, as a task that never started reports it.
pub fn empty_stream() -> StreamSnapshot {
    StreamSnapshot {
        text: String::new(),
        total_bytes: Count::saturating(0),
        total_lines: Count::saturating(0),
        returned_bytes: Count::saturating(0),
        truncated: false,
    }
}

pub fn millis(duration: Duration) -> Count {
    Count::saturating(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}
