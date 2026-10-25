//! The bounded output of a task's stream. A stream keeps two buffers: the
//! cumulative capture, which no read consumes and from which the `terminal`
//! report and the log are rendered, and the window, which each `running`
//! report and `tool.stdin` reply takes, leaving an empty one. Each buffer
//! keeps the first bytes written to it as its head and the last as its tail;
//! the bytes between are counted and discarded, so a write never waits for
//! space and the reader of the pipes never stops.

use std::collections::VecDeque;

use protocol::field::Count;
use protocol::tool::StreamSnapshot;

use crate::report::StreamOutput;

/// The bytes a buffer keeps of its head, and again of its tail.
pub const STORED_HALF_BYTES: usize = 524_288;

/// One stream's output: both buffers and the stream's cumulative counters.
pub struct StreamCapture {
    cumulative: Retained,
    window: Retained,
    totals: Totals,
}

/// A window taken from a stream, with the stream's counters at that moment.
pub struct Window {
    retained: Retained,
    totals: Totals,
}

#[derive(Clone, Copy, Default)]
struct Totals {
    bytes: u64,
    lines: u64,
}

/// The bytes a buffer keeps of everything written to it.
struct Retained {
    head_capacity: usize,
    tail_capacity: usize,
    head: Vec<u8>,
    tail: VecDeque<u8>,
    written: u64,
}

impl StreamCapture {
    pub fn new() -> StreamCapture {
        StreamCapture::with_halves(STORED_HALF_BYTES, STORED_HALF_BYTES)
    }

    /// A capture whose buffers keep `head` bytes of head and `tail` of tail.
    pub fn with_halves(head: usize, tail: usize) -> StreamCapture {
        StreamCapture {
            cumulative: Retained::new(head, tail),
            window: Retained::new(head, tail),
            totals: Totals::default(),
        }
    }

    pub fn write(&mut self, bytes: &[u8]) {
        self.totals.bytes += bytes.len() as u64;
        self.totals.lines += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        self.cumulative.write(bytes);
        self.window.write(bytes);
    }

    /// The window so far; output written after this goes into a new one.
    pub fn take_window(&mut self) -> Window {
        let empty = Retained::new(self.window.head_capacity, self.window.tail_capacity);
        Window {
            retained: std::mem::replace(&mut self.window, empty),
            totals: self.totals,
        }
    }

    /// Everything the cumulative capture keeps, head, marker and tail, with no
    /// visible limit: the text a task's log holds once the task has ended.
    pub fn stored_text(&self) -> String {
        let capacity = self.cumulative.head_capacity + self.cumulative.tail_capacity;
        self.cumulative
            .snapshot(self.totals, capacity, usize::MAX)
            .text
    }

    /// The bytes both buffers hold.
    pub fn stored_bytes(&self) -> usize {
        self.cumulative.stored_bytes() + self.window.stored_bytes()
    }
}

impl Default for StreamCapture {
    fn default() -> StreamCapture {
        StreamCapture::new()
    }
}

/// The cumulative capture.
impl StreamOutput for StreamCapture {
    fn snapshot(&self, visible_bytes: usize, visible_lines: usize) -> StreamSnapshot {
        self.cumulative
            .snapshot(self.totals, visible_bytes, visible_lines)
    }
}

impl StreamOutput for Window {
    fn snapshot(&self, visible_bytes: usize, visible_lines: usize) -> StreamSnapshot {
        self.retained
            .snapshot(self.totals, visible_bytes, visible_lines)
    }
}

impl Retained {
    fn new(head_capacity: usize, tail_capacity: usize) -> Retained {
        Retained {
            head_capacity,
            tail_capacity,
            head: Vec::new(),
            tail: VecDeque::new(),
            written: 0,
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        self.written += bytes.len() as u64;
        let room = self.head_capacity - self.head.len();
        self.head.extend_from_slice(&bytes[..room.min(bytes.len())]);
        let kept = &bytes[bytes.len() - self.tail_capacity.min(bytes.len())..];
        let excess = (self.tail.len() + kept.len()).saturating_sub(self.tail_capacity);
        self.tail.drain(..excess);
        self.tail.extend(kept);
    }

    fn stored_bytes(&self) -> usize {
        self.head.len() + self.tail.len()
    }

    /// The head part takes at most half the byte budget, rounded down, and
    /// half the line budget, rounded down; the tail part takes the rest of
    /// both. A byte in both parts counts once.
    fn snapshot(
        &self,
        totals: Totals,
        visible_bytes: usize,
        visible_lines: usize,
    ) -> StreamSnapshot {
        let head_bytes = visible_bytes / 2;
        let head_lines = visible_lines / 2;
        let head = head_part(&self.head, head_bytes, head_lines);
        let (tail, tail_start) =
            self.tail_part(visible_bytes - head_bytes, visible_lines - head_lines);
        let overlap = (head.len() as u64).saturating_sub(tail_start);
        let tail = &tail[usize::try_from(overlap)
            .unwrap_or(usize::MAX)
            .min(tail.len())..];
        let returned = head.len() + tail.len();
        let omitted = self.written.saturating_sub(returned as u64);
        let text = if omitted > 0 {
            format!("{head}\n[... {omitted} bytes truncated ...]\n{tail}")
        } else {
            format!("{head}{tail}")
        };
        StreamSnapshot {
            text,
            total_bytes: Count::saturating(totals.bytes),
            total_lines: Count::saturating(totals.lines),
            returned_bytes: Count::saturating(returned as u64),
            truncated: omitted > 0,
        }
    }

    /// The tail part, and the offset in the buffer's bytes where it starts.
    fn tail_part(&self, max_bytes: usize, max_lines: usize) -> (String, u64) {
        let tail_start = self.written - self.tail.len() as u64;
        if max_bytes == 0 || max_lines == 0 || self.tail.is_empty() {
            return (String::new(), self.written);
        }
        let first = self.tail.len() - self.tail.len().min(max_bytes);
        let region: Vec<u8> = self.tail.range(first..).copied().collect();
        let start = after_last_lines(&region, max_lines);
        let text = valid_suffix(&region[start..]);
        let offset = first + region.len() - text.len();
        (text.to_owned(), tail_start + offset as u64)
    }
}

/// The longest prefix of `value` of at most `max_bytes` that ends no later
/// than its `max_lines`-th newline, shortened to a UTF-8 boundary.
fn head_part(value: &[u8], max_bytes: usize, max_lines: usize) -> &str {
    if max_bytes == 0 || max_lines == 0 {
        return "";
    }
    let mut limit = value.len().min(max_bytes);
    if let Some((index, _)) = value[..limit]
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(max_lines - 1)
    {
        limit = index + 1;
    }
    value[..limit]
        .utf8_chunks()
        .next()
        .map_or("", |chunk| chunk.valid())
}

/// Where the suffix of `region` starts that holds its last `max_lines` lines,
/// a final newline not counted; 0 when the region holds fewer.
fn after_last_lines(region: &[u8], max_lines: usize) -> usize {
    let counted = region.strip_suffix(b"\n").unwrap_or(region);
    counted
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(max_lines - 1)
        .map_or(0, |(index, _)| index + 1)
}

/// The longest suffix of `value` that is valid UTF-8.
fn valid_suffix(value: &[u8]) -> &str {
    value
        .utf8_chunks()
        .last()
        .filter(|chunk| chunk.invalid().is_empty())
        .map_or("", |chunk| chunk.valid())
}
