//! `write_stdin` as a task's supervisor handles it: a `tool.stdin` with empty
//! `chars` reads the task's state and output; one with nonempty `chars`
//! writes them, or for exactly U+0003 interrupts the task, at most once for
//! each `write_seq`, then waits.

use std::collections::HashMap;
use std::time::Duration;

use protocol::tool::{ControlErrorCode, StdinPayload};
use sha2::{Digest, Sha256};

/// A read's wait when the control gives none, and its bounds.
pub const READ_DEFAULT_WAIT_MS: u64 = 5_000;
pub const READ_MIN_WAIT_MS: u64 = 0;
pub const READ_MAX_WAIT_MS: u64 = 50_000;

/// The wait after a write when the control gives none, and its bounds.
pub const WRITE_DEFAULT_WAIT_MS: u64 = 250;
pub const WRITE_MIN_WAIT_MS: u64 = 250;
pub const WRITE_MAX_WAIT_MS: u64 = 30_000;

/// How long a write may take before the task's stdin closes for good.
pub const WRITE_TIMEOUT: Duration = Duration::from_millis(250);

/// The `chars` that interrupt the task instead of being written.
pub const INTERRUPT: &str = "\u{3}";

/// What one `tool.stdin` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Read {
        wait: Duration,
    },
    Write {
        write_seq: i64,
        chars: String,
        digest: [u8; 32],
        wait: Duration,
    },
}

impl Request {
    pub fn of(payload: &StdinPayload) -> Request {
        let wait_ms = payload.wait_ms.value().copied();
        if payload.chars.is_empty() {
            return Request::Read {
                wait: clamped(
                    wait_ms,
                    READ_DEFAULT_WAIT_MS,
                    READ_MIN_WAIT_MS,
                    READ_MAX_WAIT_MS,
                ),
            };
        }
        let write_seq = payload
            .write_seq
            .as_ref()
            .expect("the decoder admits no nonempty tool.stdin without write_seq")
            .get();
        Request::Write {
            write_seq,
            digest: Sha256::digest(payload.chars.as_bytes()).into(),
            chars: payload.chars.clone(),
            wait: clamped(
                wait_ms,
                WRITE_DEFAULT_WAIT_MS,
                WRITE_MIN_WAIT_MS,
                WRITE_MAX_WAIT_MS,
            ),
        }
    }
}

fn clamped(value: Option<i64>, default: u64, min: u64, max: u64) -> Duration {
    let millis = value.map_or(default, |value| u64::try_from(value).unwrap_or(0));
    Duration::from_millis(millis.clamp(min, max))
}

/// What a handled write did, kept for its `write_seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRecord {
    digest: [u8; 32],
    pub written_bytes: u64,
    pub error: Option<ControlErrorCode>,
}

/// How a write's `write_seq` and content stand against the task's records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    /// Recorded with other content: `write_conflict`.
    Conflict,
    /// Recorded with this content: a reply that reuses the record.
    Repeat(WriteRecord),
    /// Unrecorded, at or below the highest recorded: `invalid_input`.
    Stale,
    /// Unrecorded and above every recorded one.
    New,
}

/// A task's handled writes by `write_seq`.
#[derive(Debug, Default)]
pub struct WriteRecords {
    records: HashMap<i64, WriteRecord>,
    highest: i64,
}

impl WriteRecords {
    pub fn check(&self, write_seq: i64, digest: &[u8; 32]) -> Check {
        match self.records.get(&write_seq) {
            Some(record) if record.digest == *digest => Check::Repeat(*record),
            Some(_) => Check::Conflict,
            None if write_seq <= self.highest => Check::Stale,
            None => Check::New,
        }
    }

    pub fn record(
        &mut self,
        write_seq: i64,
        digest: [u8; 32],
        written_bytes: u64,
        error: Option<ControlErrorCode>,
    ) {
        self.highest = self.highest.max(write_seq);
        self.records.insert(
            write_seq,
            WriteRecord {
                digest,
                written_bytes,
                error,
            },
        );
    }
}
