//! The rmcp stdio transport over a server process's pipes: one JSON-RPC
//! message per line each way.
//!
//! - Every request the CLI writes leaves without `params._meta`: rmcp puts a
//!   progress token there, and the CLI offers none, since it ignores
//!   `notifications/progress`. The manager tags a request it sends by a
//!   number under [`TAG_KEY`] in that `_meta`, which the transport reads and
//!   removes.
//! - The answer to a tagged request reaches the manager as its raw JSON
//!   `result` or `error` ([`TransportEvent::Answer`]), never re-encoded
//!   through a typed form; rmcp gets an empty result in its place. The raw
//!   answer to `initialize` goes to the connection, and rmcp gets it too.
//! - A stdout message over its limit closes the transport without reading
//!   the rest ([`Closed::MessageTooLarge`]); so does the end of stdout.
//!   A line that is not JSON, or not a JSON-RPC message rmcp decodes, is
//!   skipped.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::service::{RoleClient, RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::Transport;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot};

/// The `_meta` member under which the manager tags a request.
pub(crate) const TAG_KEY: &str = "tetral-cli/request";

/// A server's answer to a request, as received.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RawAnswer {
    /// The `result` member.
    Result(Value),
    /// The `error` member.
    Error(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Closed {
    EndOfOutput,
    MessageTooLarge,
}

#[derive(Debug)]
pub(crate) enum TransportEvent {
    /// The answer to the tagged request.
    Answer { tag: u64, answer: RawAnswer },
}

pub(crate) type Events = Arc<dyn Fn(TransportEvent) + Send + Sync>;

/// What the connection keeps of its transport: the raw answer to
/// `initialize` and how the transport closed.
pub(crate) struct Ends {
    pub initialize: oneshot::Receiver<RawAnswer>,
    pub closed: oneshot::Receiver<Closed>,
}

enum Purpose {
    Initialize,
    Tag(u64),
}

/// A request written, by its ID as compact JSON, whose answer the reader
/// takes apart from rmcp.
struct Registration {
    id: String,
    purpose: Purpose,
}

struct Outgoing {
    line: Vec<u8>,
    done: oneshot::Sender<std::io::Result<()>>,
}

pub(crate) struct StdioTransport {
    writer: Option<mpsc::UnboundedSender<Outgoing>>,
    registrations: mpsc::UnboundedSender<Registration>,
    incoming: mpsc::UnboundedReceiver<RxJsonRpcMessage<RoleClient>>,
}

/// The transport over `stdin` and `stdout`, reading messages of at most
/// `limit` bytes.
pub(crate) fn new(
    stdin: ChildStdin,
    stdout: ChildStdout,
    limit: usize,
    events: Events,
) -> (StdioTransport, Ends) {
    let (writer, outgoing) = mpsc::unbounded_channel();
    let (registrations, registered) = mpsc::unbounded_channel();
    let (incoming_sender, incoming) = mpsc::unbounded_channel();
    let (initialize_sender, initialize) = oneshot::channel();
    let (closed_sender, closed) = oneshot::channel();
    tokio::spawn(write(stdin, outgoing));
    tokio::spawn(read(
        Reader {
            stdout: BufReader::with_capacity(65_536, stdout),
            limit,
            registered,
            requests: HashMap::new(),
            incoming: incoming_sender,
            events,
            initialize: Some(initialize_sender),
        },
        closed_sender,
    ));
    (
        StdioTransport {
            writer: Some(writer),
            registrations,
            incoming,
        },
        Ends { initialize, closed },
    )
}

impl Transport<RoleClient> for StdioTransport {
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'static {
        let (done, written) = oneshot::channel();
        let queued = prepare(&item).and_then(|(line, registration)| {
            if let Some(registration) = registration {
                let _ = self.registrations.send(registration);
            }
            self.writer
                .as_ref()
                .ok_or_else(closed)?
                .send(Outgoing { line, done })
                .map_err(|_| closed())
        });
        async move {
            queued?;
            written.await.unwrap_or_else(|_| Err(closed()))
        }
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.incoming.recv()
    }

    async fn close(&mut self) -> Result<(), std::io::Error> {
        self.writer = None;
        Ok(())
    }
}

fn closed() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "the server's stdin is closed",
    )
}

/// The line written for `item`, a request without `params._meta`, and its
/// registration.
fn prepare(
    item: &TxJsonRpcMessage<RoleClient>,
) -> std::io::Result<(Vec<u8>, Option<Registration>)> {
    let mut value = serde_json::to_value(item)?;
    let mut registration = None;
    let mut tag = None;
    if let Value::Object(message) = &mut value
        && let Some(id) = message.get("id").map(Value::to_string)
        && message.contains_key("method")
    {
        if let Some(Value::Object(params)) = message.get_mut("params") {
            if let Some(Value::Object(meta)) = params.remove("_meta") {
                tag = meta.get(TAG_KEY).and_then(Value::as_u64);
            }
            if params.is_empty() {
                message.remove("params");
            }
        }
        let purpose = match message.get("method").and_then(Value::as_str) {
            Some("initialize") => Some(Purpose::Initialize),
            _ => tag.map(Purpose::Tag),
        };
        registration = purpose.map(|purpose| Registration { id, purpose });
    }
    let mut line = serde_json::to_vec(&value)?;
    line.push(b'\n');
    Ok((line, registration))
}

async fn write(mut stdin: ChildStdin, mut outgoing: mpsc::UnboundedReceiver<Outgoing>) {
    while let Some(Outgoing { line, done }) = outgoing.recv().await {
        let result = async {
            stdin.write_all(&line).await?;
            stdin.flush().await
        }
        .await;
        let _ = done.send(result);
    }
}

struct Reader {
    stdout: BufReader<ChildStdout>,
    limit: usize,
    registered: mpsc::UnboundedReceiver<Registration>,
    requests: HashMap<String, Purpose>,
    incoming: mpsc::UnboundedSender<RxJsonRpcMessage<RoleClient>>,
    events: Events,
    initialize: Option<oneshot::Sender<RawAnswer>>,
}

enum Line {
    Message,
    End,
    TooLarge,
}

async fn read(mut reader: Reader, closed: oneshot::Sender<Closed>) {
    let mut line = Vec::new();
    loop {
        match read_line(&mut reader.stdout, &mut line, reader.limit).await {
            Line::Message => {
                // A request is registered before it is written, so before
                // any answer to it can be read.
                while let Ok(Registration { id, purpose }) = reader.registered.try_recv() {
                    reader.requests.insert(id, purpose);
                }
                reader.take(&line);
                line.clear();
            }
            Line::End => {
                let _ = closed.send(Closed::EndOfOutput);
                return;
            }
            Line::TooLarge => {
                let _ = closed.send(Closed::MessageTooLarge);
                return;
            }
        }
    }
}

/// Reads one line into `line`, without its line ending, reading no further
/// once it holds more than `limit` bytes. A last line without a line ending
/// is a message.
async fn read_line(stdout: &mut BufReader<ChildStdout>, line: &mut Vec<u8>, limit: usize) -> Line {
    loop {
        let Ok(available) = stdout.fill_buf().await else {
            return Line::End;
        };
        if available.is_empty() {
            return if line.is_empty() {
                Line::End
            } else {
                Line::Message
            };
        }
        match available.iter().position(|byte| *byte == b'\n') {
            Some(end) => {
                line.extend_from_slice(&available[..end]);
                stdout.consume(end + 1);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return if line.len() > limit {
                    Line::TooLarge
                } else {
                    Line::Message
                };
            }
            None => {
                let read = available.len();
                line.extend_from_slice(available);
                stdout.consume(read);
                // One byte more may still be the `\r` of a line ending.
                if line.len() > limit + 1 {
                    return Line::TooLarge;
                }
            }
        }
    }
}

impl Reader {
    fn take(&mut self, line: &[u8]) {
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            return;
        };
        let value = match value {
            Value::Object(message) if is_answer(&message) => self.answered(message),
            other => other,
        };
        if let Ok(message) = serde_json::from_value(value) {
            let _ = self.incoming.send(message);
        }
    }

    /// Hands a registered request's raw answer on and gives what rmcp gets.
    fn answered(&mut self, mut message: Map<String, Value>) -> Value {
        let id = message.get("id").map(Value::to_string).unwrap_or_default();
        let Some(purpose) = self.requests.remove(&id) else {
            return Value::Object(message);
        };
        match purpose {
            Purpose::Initialize => {
                let answer = raw(message.clone());
                if let Some(initialize) = self.initialize.take() {
                    let _ = initialize.send(answer);
                }
                Value::Object(message)
            }
            Purpose::Tag(tag) => {
                let id = message.remove("id").unwrap_or(Value::Null);
                (self.events)(TransportEvent::Answer {
                    tag,
                    answer: raw(message),
                });
                json!({ "jsonrpc": "2.0", "id": id, "result": {} })
            }
        }
    }
}

/// A message with an `id` and a `result` or an `error`, and no `method`.
fn is_answer(message: &Map<String, Value>) -> bool {
    message.contains_key("id")
        && !message.contains_key("method")
        && (message.contains_key("result") || message.contains_key("error"))
}

fn raw(mut message: Map<String, Value>) -> RawAnswer {
    match message.remove("error") {
        Some(error) => RawAnswer::Error(error),
        None => RawAnswer::Result(message.remove("result").unwrap_or(Value::Null)),
    }
}
