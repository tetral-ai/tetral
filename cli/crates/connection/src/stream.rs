//! The reading of one event stream: its bytes split into lines that are
//! bounded before they are kept, the lines parsed into SSE events by
//! `eventsource-stream`, and each event's `data` handed on as one envelope.
//! The SSE `event` and `id` fields and comments carry nothing.
//!
//! The lines are split and bounded here, on bytes, because
//! `eventsource-stream` keeps every byte after an invalid UTF-8 sequence
//! without bound; it gets only whole lines, each decoded as the SSE standard
//! decodes a stream, with every invalid sequence replaced by U+FFFD.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use eventsource_stream::{EventStream, EventStreamError};
use futures_core::Stream;
use http_body::Body as _;

/// The largest `data` of one event until a larger limit is given.
pub const EVENT_MAX_BYTES: usize = protocol::connection::MESSAGE_MAX_BYTES;

/// The largest `data` of one event from the moment the CLI starts submitting
/// a ready that advertises `tool_execution`: a complete `tool.execute`.
pub const TOOL_EVENT_MAX_BYTES: usize = protocol::tool::limits::EXECUTE_SSE_JSON_BYTES;

/// The largest `data` of one event that a stream reads, shared between the
/// stream's reader and the Connection owner, which raises it while the
/// stream runs; each line and each event is checked against its value at
/// that moment.
#[derive(Debug, Clone)]
pub struct EventLimit(Arc<AtomicUsize>);

impl EventLimit {
    pub fn new(max_data_bytes: usize) -> EventLimit {
        EventLimit(Arc::new(AtomicUsize::new(max_data_bytes)))
    }

    pub fn set(&self, max_data_bytes: usize) {
        self.0.store(max_data_bytes, Ordering::SeqCst);
    }

    fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// The length of `data: `, which precedes the data on its line.
const DATA_PREFIX_BYTES: usize = 6;

/// Why a stream was closed before the next event was processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Overflow {
    #[error("a line is longer than the event limit allows")]
    LineTooLong,
    #[error("an event's data is over the event limit")]
    DataTooLarge,
}

/// How a stream ended.
#[derive(Debug)]
pub enum End {
    /// Engine or the network ended it.
    Closed,
    /// The CLI closed it, without processing the event, at a limit.
    Overflow(Overflow),
}

/// Splits a byte stream into lines, each given on with a line feed, and
/// fails it at the first line or event over its limit, before keeping the
/// line. An event's data bytes are the values of its `data` lines joined by
/// line feeds.
struct BoundedLines<S> {
    chunks: S,
    /// A line may hold the limit's data bytes and `data: `.
    limit: EventLimit,
    line: Vec<u8>,
    after_cr: bool,
    first_line: bool,
    /// The data bytes of the event so far; `None` before its first `data`
    /// line.
    data_bytes: Option<usize>,
    lines: std::collections::VecDeque<String>,
    /// The limit a chunk broke, given on after the lines before it.
    failed: Option<Overflow>,
    ended: bool,
}

impl<S> BoundedLines<S> {
    fn new(chunks: S, limit: EventLimit) -> BoundedLines<S> {
        BoundedLines {
            chunks,
            limit,
            line: Vec::new(),
            after_cr: false,
            first_line: true,
            data_bytes: None,
            lines: std::collections::VecDeque::new(),
            failed: None,
            ended: false,
        }
    }

    /// Takes one chunk; a line may end with LF, CR or CR LF, and a CR LF may
    /// span two chunks.
    fn push(&mut self, mut chunk: &[u8]) -> Result<(), Overflow> {
        while !chunk.is_empty() {
            if std::mem::take(&mut self.after_cr) && chunk[0] == b'\n' {
                chunk = &chunk[1..];
                continue;
            }
            match chunk.iter().position(|byte| matches!(byte, b'\n' | b'\r')) {
                Some(end) => {
                    self.extend(&chunk[..end])?;
                    self.after_cr = chunk[end] == b'\r';
                    self.end_line()?;
                    chunk = &chunk[end + 1..];
                }
                None => {
                    self.extend(chunk)?;
                    chunk = &[];
                }
            }
        }
        Ok(())
    }

    fn extend(&mut self, bytes: &[u8]) -> Result<(), Overflow> {
        if self.line.len() + bytes.len() > self.limit.get() + DATA_PREFIX_BYTES {
            return Err(Overflow::LineTooLong);
        }
        self.line.extend_from_slice(bytes);
        Ok(())
    }

    fn end_line(&mut self) -> Result<(), Overflow> {
        let line = std::mem::take(&mut self.line);
        let first = std::mem::take(&mut self.first_line);
        let counted = match line.strip_prefix(UTF8_BOM) {
            Some(rest) if first => rest,
            _ => &line,
        };
        if counted.is_empty() {
            self.data_bytes = None;
        } else if let Some(value) = data_value(counted) {
            let joined = match self.data_bytes {
                Some(bytes) => bytes + 1 + value.len(),
                None => value.len(),
            };
            if joined > self.limit.get() {
                return Err(Overflow::DataTooLarge);
            }
            self.data_bytes = Some(joined);
        }
        let mut line = String::from_utf8_lossy(&line).into_owned();
        line.push('\n');
        self.lines.push_back(line);
        Ok(())
    }
}

/// The byte order mark that may open a stream.
const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";

/// The value of a `data` field line, as SSE reads it.
fn data_value(line: &[u8]) -> Option<&[u8]> {
    let rest = line.strip_prefix(b"data")?;
    if rest.is_empty() {
        return Some(b"");
    }
    let value = rest.strip_prefix(b":")?;
    Some(value.strip_prefix(b" ").unwrap_or(value))
}

impl<S, E> Stream for BoundedLines<S>
where
    S: Stream<Item = Result<Vec<u8>, E>> + Unpin,
{
    type Item = Result<String, Overflow>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(line) = self.lines.pop_front() {
                return Poll::Ready(Some(Ok(line)));
            }
            if self.ended {
                return Poll::Ready(None);
            }
            if let Some(overflow) = self.failed.take() {
                self.ended = true;
                return Poll::Ready(Some(Err(overflow)));
            }
            match Pin::new(&mut self.chunks).poll_next(context) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if let Err(overflow) = self.push(&chunk) {
                        self.failed = Some(overflow);
                    }
                }
                Poll::Ready(Some(Err(_)) | None) => {
                    self.ended = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The data of each event of a byte stream, in order, until the stream ends
/// or a limit closes it.
pub struct Events<S>(EventStream<BoundedLines<S>>);

impl<S, E> Events<S>
where
    S: Stream<Item = Result<Vec<u8>, E>> + Unpin,
{
    pub fn new(chunks: S, limit: EventLimit) -> Events<S> {
        Events(EventStream::new(BoundedLines::new(chunks, limit)))
    }

    /// The next event's data, or how the stream ended.
    pub async fn next(&mut self) -> Result<String, End> {
        match poll_fn(|context| Pin::new(&mut self.0).poll_next(context)).await {
            Some(Ok(event)) => Ok(event.data),
            Some(Err(EventStreamError::Transport(overflow))) => Err(End::Overflow(overflow)),
            Some(Err(_)) | None => Err(End::Closed),
        }
    }
}

/// The body of an event-stream response as chunks of bytes.
pub struct Chunks(reqwest::Body);

impl Chunks {
    pub fn new(response: reqwest::Response) -> Chunks {
        Chunks(response.into())
    }
}

impl Stream for Chunks {
    type Item = Result<Vec<u8>, reqwest::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.0).poll_frame(context) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        return Poll::Ready(Some(Ok(data.to_vec())));
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    /// A byte stream of the given chunks.
    struct Given(std::vec::IntoIter<Vec<u8>>);

    impl Stream for Given {
        type Item = Result<Vec<u8>, Infallible>;

        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.0.next().map(Ok))
        }
    }

    /// The data of every event of `chunks`, then how the stream ended.
    fn read(chunks: Vec<Vec<u8>>, limit: usize) -> (Vec<String>, End) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut events = Events::new(Given(chunks.into_iter()), EventLimit::new(limit));
            let mut data = Vec::new();
            loop {
                match events.next().await {
                    Ok(event) => data.push(event),
                    Err(end) => return (data, end),
                }
            }
        })
    }

    fn event(data: &str) -> Vec<u8> {
        format!("data: {data}\n\n").into_bytes()
    }

    #[test]
    fn an_event_at_the_limit_is_read_and_one_byte_more_closes_the_stream_unread() {
        let at_limit = "x".repeat(EVENT_MAX_BYTES);
        let (data, end) = read(vec![event(&at_limit), event("next")], EVENT_MAX_BYTES);
        assert_eq!(data, [at_limit.as_str(), "next"]);
        assert!(matches!(end, End::Closed));

        // The line of 65,537 data bytes is longer than the limit plus `data: `.
        let over = "x".repeat(EVENT_MAX_BYTES + 1);
        let mut one_chunk = event("before");
        one_chunk.extend(event(&over));
        one_chunk.extend(event("after"));
        let (data, end) = read(vec![one_chunk], EVENT_MAX_BYTES);
        assert_eq!(data, ["before"]);
        assert!(
            matches!(end, End::Overflow(Overflow::LineTooLong)),
            "{end:?}"
        );
    }

    #[test]
    fn data_lines_count_joined_by_line_feeds_and_are_bounded_across_chunks() {
        // Two data lines of 32,767 and 32,768 bytes join to exactly the limit.
        let joined = format!(
            "data: {}\r\ndata:{}\r\n\r\n",
            "a".repeat(32_767),
            "b".repeat(32_768)
        );
        let (data, _) = read(vec![joined.into_bytes()], EVENT_MAX_BYTES);
        assert_eq!(data[0].len(), EVENT_MAX_BYTES);

        let over = format!(
            "data: {}\rdata: {}\r\r",
            "a".repeat(32_768),
            "b".repeat(32_768)
        );
        let (data, end) = read(vec![over.into_bytes()], EVENT_MAX_BYTES);
        assert!(data.is_empty());
        assert!(
            matches!(end, End::Overflow(Overflow::DataTooLarge)),
            "{end:?}"
        );

        // A line split over chunks counts as one line, a CR LF split over two
        // chunks ends one line, and a comment line is bounded too.
        let mut chunks: Vec<Vec<u8>> = format!("data: {}", "c".repeat(EVENT_MAX_BYTES))
            .into_bytes()
            .chunks(1_000)
            .map(<[u8]>::to_vec)
            .collect();
        chunks.push(b"\r".to_vec());
        chunks.push(b"\n\n".to_vec());
        chunks.push(format!(": {}\n", "d".repeat(EVENT_MAX_BYTES + 5)).into_bytes());
        let (data, end) = read(chunks, EVENT_MAX_BYTES);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].len(), EVENT_MAX_BYTES);
        assert!(
            matches!(end, End::Overflow(Overflow::LineTooLong)),
            "{end:?}"
        );
    }

    #[test]
    fn event_and_id_fields_and_comments_carry_nothing_a_raised_limit_applies_and_invalid_utf8_is_replaced()
     {
        let (data, _) = read(
            vec![b": hello\nevent: other\nid: 7\ndata: {\"a\":1}\n\n".to_vec()],
            EVENT_MAX_BYTES,
        );
        assert_eq!(data, [r#"{"a":1}"#]);

        let raised = "y".repeat(EVENT_MAX_BYTES * 2);
        let (data, _) = read(vec![event(&raised)], EVENT_MAX_BYTES * 2);
        assert_eq!(data, [raised.as_str()]);

        // An invalid UTF-8 sequence is replaced, and the stream goes on.
        let (data, _) = read(
            vec![b"data: a\xffb\n\n".to_vec(), event("next")],
            EVENT_MAX_BYTES,
        );
        assert_eq!(data, ["a\u{fffd}b", "next"]);
    }
}
