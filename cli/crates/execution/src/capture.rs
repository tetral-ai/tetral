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
    // Only boundary provenance survives discarded bytes, never output text.
    head_split_start: Option<usize>,
    tail_split_end: Option<u64>,
    utf8: Utf8Boundary,
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

    /// Whether output has been written since the last window was taken.
    pub fn window_has_output(&self) -> bool {
        self.window.written > 0
    }

    /// A snapshot with no text that carries the stream's counters, for a
    /// reply that reads no output.
    pub fn counters(&self) -> StreamSnapshot {
        StreamSnapshot {
            text: String::new(),
            total_bytes: Count::saturating(self.totals.bytes),
            total_lines: Count::saturating(self.totals.lines),
            returned_bytes: Count::saturating(0),
            truncated: false,
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
            head_split_start: None,
            tail_split_end: None,
            utf8: Utf8Boundary::default(),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        let before = self.written;
        self.written += bytes.len() as u64;
        let tail_start = self.written.saturating_sub(self.tail_capacity as u64);
        let old_origin = before.saturating_sub(self.tail.len() as u64);
        self.tail_split_end = self.tail_split_end.filter(|end| tail_start < *end);
        if tail_start >= old_origin && tail_start < before {
            let boundary = (tail_start - old_origin) as usize;
            let first = boundary.saturating_sub(3);
            let last = (boundary + 3).min(self.tail.len());
            let mut context = [0; 7];
            for (destination, source) in context.iter_mut().zip(first..last) {
                *destination = self.tail[source];
            }
            if let Some((_, end)) = crossing_character(&context[..last - first], boundary - first) {
                self.tail_split_end = Some(old_origin + first as u64 + end as u64);
            }
        }
        let head_boundary = self.head_capacity as u64;
        self.utf8.scan(bytes, before, |start, text| {
            let end = start + text.len() as u64;
            if start < head_boundary && head_boundary < end {
                let boundary = (head_boundary - start) as usize;
                if !text.is_char_boundary(boundary) {
                    let mut first = boundary;
                    while !text.is_char_boundary(first) {
                        first -= 1;
                    }
                    self.head_split_start = Some((start + first as u64) as usize);
                }
            }
            if start < tail_start && tail_start < end {
                let mut last = (tail_start - start) as usize;
                if !text.is_char_boundary(last) {
                    while !text.is_char_boundary(last) {
                        last += 1;
                    }
                    self.tail_split_end = Some(start + last as u64);
                }
            }
        });
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
        let mut head_end = head_end(&self.head, head_bytes, head_lines);
        let tail: Vec<u8> = self.tail.iter().copied().collect();
        let tail_origin = self.written - tail.len() as u64;
        let mut tail_first = tail_start(
            &tail,
            visible_bytes - head_bytes,
            visible_lines - head_lines,
        );
        let tail_offset = tail_origin + tail_first as u64;
        let (head, tail, retained) = if head_end as u64 >= tail_offset {
            // Decode adjacent or overlapping ranges together: their shared
            // boundary can fall inside an otherwise complete character.
            let overlap = (head_end as u64 - tail_offset) as usize;
            let mut contiguous = self.head[..head_end].to_vec();
            contiguous.extend_from_slice(&tail[(tail_first + overlap).min(tail.len())..]);
            let retained = contiguous.len();
            (valid_text(&contiguous), String::new(), retained)
        } else {
            // A real gap must not turn a split character into visible text.
            // Discard only boundary fragments; invalid bytes elsewhere are
            // ignored without being counted as limit truncation.
            head_end = complete_prefix_end(&self.head, head_end);
            if let Some(first) = self.head_split_start {
                head_end = head_end.min(first);
            }
            tail_first = complete_suffix_start(&tail, tail_first);
            if let Some(end) = self.tail_split_end {
                tail_first = tail_first.max((end - tail_origin) as usize);
            }
            (
                valid_text(&self.head[..head_end]),
                valid_text(&tail[tail_first..]),
                head_end + tail.len() - tail_first,
            )
        };
        let returned = head.len() + tail.len();
        let omitted = self.written.saturating_sub(retained as u64);
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
}

/// The original byte offset after the selected head, before UTF-8 decoding.
fn head_end(value: &[u8], max_bytes: usize, max_lines: usize) -> usize {
    if max_bytes == 0 || max_lines == 0 {
        return 0;
    }
    let limit = value.len().min(max_bytes);
    value[..limit]
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(max_lines - 1)
        .map_or(limit, |(index, _)| index + 1)
}

/// The original byte offset of the selected tail, before UTF-8 decoding.
fn tail_start(value: &[u8], max_bytes: usize, max_lines: usize) -> usize {
    if max_bytes == 0 || max_lines == 0 {
        return value.len();
    }
    let first = value.len() - value.len().min(max_bytes);
    first + after_last_lines(&value[first..], max_lines)
}

/// Keeps every valid fragment; invalid and incomplete input adds no marker.
fn valid_text(value: &[u8]) -> String {
    value.utf8_chunks().map(|chunk| chunk.valid()).collect()
}

/// Removes an incomplete character at a head boundary preceding a real gap.
fn complete_prefix_end(value: &[u8], end: usize) -> usize {
    crossing_character(value, end).map_or(end, |(first, _)| first)
}

fn complete_suffix_start(value: &[u8], first: usize) -> usize {
    crossing_character(value, first).map_or(first, |(_, end)| end)
}

/// Only a complete valid character can turn an edge into limit omission.
fn crossing_character(value: &[u8], boundary: usize) -> Option<(usize, usize)> {
    let mut offset = 0;
    for chunk in value.utf8_chunks() {
        let end = offset + chunk.valid().len();
        if offset < boundary && boundary < end {
            let index = boundary - offset;
            if !chunk.valid().is_char_boundary(index) {
                let (mut first, mut last) = (index, index);
                while !chunk.valid().is_char_boundary(first) {
                    first -= 1;
                }
                while !chunk.valid().is_char_boundary(last) {
                    last += 1;
                }
                return Some((offset + first, offset + last));
            }
        }
        offset = end + chunk.invalid().len();
    }
    None
}

/// A decoder's at-most-three trailing bytes classify storage cuts across
/// write calls. Snapshots never wait for them and never render this state.
#[derive(Default)]
struct Utf8Boundary {
    pending: [u8; 4],
    len: usize,
}

impl Utf8Boundary {
    fn scan(&mut self, bytes: &[u8], origin: u64, mut valid: impl FnMut(u64, &str)) {
        let mut consumed = 0;
        let pending_origin = origin.saturating_sub(self.len as u64);
        while self.len > 0 && consumed < bytes.len() {
            self.pending[self.len] = bytes[consumed];
            match std::str::from_utf8(&self.pending[..self.len + 1]) {
                Ok(text) => {
                    valid(pending_origin, text);
                    self.len = 0;
                    consumed += 1;
                }
                Err(error) if error.error_len().is_none() => {
                    self.len += 1;
                    consumed += 1;
                }
                Err(_) => {
                    // The new byte may start a valid character of its own.
                    self.len = 0;
                }
            }
        }
        let mut offset = origin + consumed as u64;
        for chunk in bytes[consumed..].utf8_chunks() {
            valid(offset, chunk.valid());
            offset += chunk.valid().len() as u64;
            let invalid = chunk.invalid();
            if !invalid.is_empty()
                && offset + invalid.len() as u64 == origin + bytes.len() as u64
                && std::str::from_utf8(invalid).is_err_and(|error| error.error_len().is_none())
            {
                self.pending[..invalid.len()].copy_from_slice(invalid);
                self.len = invalid.len();
            }
            offset += invalid.len() as u64;
        }
    }
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
